#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import statistics
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from abi import integer
from topics import ZERO_ADDRESS

NATIVE = ZERO_ADDRESS
WETH_RHC = "0x0bd7d308f8e1639fab988df18a8011f41eacad73"


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows = []
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                value = json.loads(line)
                if isinstance(value, dict):
                    rows.append(value)
    return rows


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fields: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for key in row:
            if key not in seen:
                seen.add(key)
                fields.append(key)
    if not fields:
        fields = ["empty"]
    with path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({key: json.dumps(value, ensure_ascii=False, sort_keys=True) if isinstance(value, (dict, list, tuple)) else value for key, value in row.items()})


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def asset_group(token: str | None) -> str | None:
    if token in {NATIVE, WETH_RHC}:
        return "RHC_ETH_EQUIVALENT"
    return token


def load_raw_maps(root: Path) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    txs: dict[str, dict[str, Any]] = {}
    receipts: dict[str, dict[str, Any]] = {}
    for path in root.rglob("transactions.jsonl"):
        for row in read_jsonl(path):
            key = str(row.get("hash") or "").lower()
            if key:
                txs[key] = row
    for path in root.rglob("receipts.jsonl"):
        for row in read_jsonl(path):
            key = str(row.get("transactionHash") or "").lower()
            if key:
                receipts[key] = row
    return txs, receipts


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--core", required=True)
    parser.add_argument("--raw", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    core = Path(args.core)
    raw = Path(args.raw)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    core_validations = list(core.rglob("VALIDATION.json"))
    if len(core_validations) != 1:
        raise SystemExit(f"expected one core VALIDATION.json, got {core_validations}")
    core_validation = json.loads(core_validations[0].read_text(encoding="utf-8"))
    if core_validation.get("status") != "PASS":
        raise SystemExit(json.dumps({"code": "CORE_DATA_NOT_PASS", "validation": core_validation}, sort_keys=True))

    def one(name: str) -> Path:
        matches = list(core.rglob(name))
        if len(matches) != 1:
            raise SystemExit(f"expected one {name}, got {matches}")
        return matches[0]

    transfers = read_csv(one("nft_transfer_events.csv"))
    primary = read_csv(one("primary_mint_events.csv"))
    sales = read_csv(one("seaport_sale_items.csv"))
    candidates = read_csv(one("candidate_wallet_activity.csv"))
    txs, receipts = load_raw_maps(raw)

    # Exact ERC-721 lifecycle rows and ERC-2309 ranges.
    exact_by_token: dict[tuple[str, int], list[dict[str, str]]] = defaultdict(list)
    ranges_by_contract: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in transfers:
        contract = row.get("contract", "").lower()
        if str(row.get("standard")) != "ERC721":
            continue
        if str(row.get("range_transfer", "")).lower() in {"true", "1"}:
            ranges_by_contract[contract].append(row)
        elif row.get("token_id") not in (None, ""):
            exact_by_token[(contract, integer(row.get("token_id")))].append(row)
    for rows in exact_by_token.values():
        rows.sort(key=lambda row: (integer(row.get("block_number")), integer(row.get("transaction_index")), integer(row.get("log_index"))))
    for rows in ranges_by_contract.values():
        rows.sort(key=lambda row: (integer(row.get("block_number")), integer(row.get("transaction_index")), integer(row.get("log_index"))))

    primary_by_tx_contract: dict[tuple[str, str], list[dict[str, str]]] = defaultdict(list)
    primary_tx_quantity: dict[str, int] = defaultdict(int)
    for row in primary:
        key = (row.get("transaction_hash", "").lower(), row.get("contract", "").lower())
        primary_by_tx_contract[key].append(row)
        primary_tx_quantity[key[0]] += integer(row.get("quantity"))

    proven_sales = [row for row in sales if row.get("sale_proof_status") == "PROVEN"]
    sale_by_token: dict[tuple[str, int], list[dict[str, str]]] = defaultdict(list)
    sale_by_tx: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in proven_sales:
        key = (row.get("nft_contract", "").lower(), integer(row.get("token_id")))
        sale_by_token[key].append(row)
        sale_by_tx[row.get("transaction_hash", "").lower()].append(row)
    for rows in sale_by_token.values():
        rows.sort(key=lambda row: (integer(row.get("block_number")), integer(row.get("log_index")), integer(row.get("item_index"))))

    def preceding_transfer(contract: str, token_id: int, seller: str, sale_row: dict[str, str]) -> tuple[dict[str, str] | None, str]:
        sale_position = (integer(sale_row.get("block_number")), integer(sale_row.get("log_index")))
        previous = None
        for row in exact_by_token.get((contract, token_id), []):
            position = (integer(row.get("block_number")), integer(row.get("log_index")))
            if position >= sale_position:
                break
            if row.get("to", "").lower() == seller:
                previous = row
        if previous is not None:
            return previous, "EXACT_TRANSFER"
        for row in ranges_by_contract.get(contract, []):
            position = (integer(row.get("block_number")), integer(row.get("log_index")))
            if position >= sale_position:
                break
            if integer(row.get("from_token_id"), -1) <= token_id <= integer(row.get("to_token_id"), -1) and row.get("to", "").lower() == seller:
                previous = row
        return (previous, "ERC2309_RANGE" if previous is not None else "NOT_FOUND")

    execution_rows: list[dict[str, Any]] = []
    for sale in proven_sales:
        contract = sale.get("nft_contract", "").lower()
        token_id = integer(sale.get("token_id"))
        seller = sale.get("seller", "").lower()
        buyer = sale.get("buyer", "").lower()
        sale_tx = sale.get("transaction_hash", "").lower()
        prior, prior_kind = preceding_transfer(contract, token_id, seller, sale)
        acquisition_status = "UNKNOWN"
        acquisition_payment_asset = None
        acquisition_price_raw = None
        entry_gas_wei = None
        acquisition_tx = prior.get("transaction_hash", "").lower() if prior else None

        if prior is not None and prior.get("from", "").lower() == ZERO_ADDRESS:
            options = [row for row in primary_by_tx_contract.get((acquisition_tx or "", contract), []) if (row.get("recipient") or row.get("minter") or "").lower() == seller]
            if len(options) == 1:
                mint = options[0]
                acquisition_payment_asset = mint.get("payment_asset")
                acquisition_price_raw = float(mint["unit_price_raw"]) if mint.get("unit_price_raw") not in (None, "") else None
                quantity = max(1, integer(mint.get("quantity")))
                tx_quantity = max(1, primary_tx_quantity.get(acquisition_tx or "", quantity))
                if mint.get("tx_from", "").lower() == seller and integer(mint.get("gas_cost_wei")) >= 0:
                    entry_gas_wei = integer(mint.get("gas_cost_wei")) / tx_quantity
                elif mint.get("tx_from", "").lower() != seller:
                    entry_gas_wei = 0
                acquisition_status = "PRIMARY_MINT_EXACT" if acquisition_price_raw is not None else "PRIMARY_MINT_PRICE_UNRESOLVED"
            else:
                acquisition_status = "PRIMARY_MINT_EVENT_AMBIGUOUS"
        elif prior is not None:
            prior_sales = [row for row in sale_by_token.get((contract, token_id), []) if row.get("transaction_hash", "").lower() == acquisition_tx and row.get("buyer", "").lower() == seller]
            if len(prior_sales) == 1 and prior_sales[0].get("allocated_gross_payment_raw") not in (None, ""):
                acquired = prior_sales[0]
                acquisition_payment_asset = acquired.get("payment_token")
                acquisition_price_raw = float(acquired["allocated_gross_payment_raw"]) / max(1, integer(acquired.get("nft_amount")))
                tx = txs.get(acquisition_tx or "", {})
                receipt = receipts.get(acquisition_tx or "", {})
                tx_items = sale_by_tx.get(acquisition_tx or "", [])
                if str(tx.get("from") or "").lower() == seller and tx_items:
                    entry_gas_wei = integer(receipt.get("gasUsed")) * integer(receipt.get("effectiveGasPrice")) / sum(max(1, integer(item.get("nft_amount"))) for item in tx_items)
                elif str(tx.get("from") or "").lower() != seller:
                    entry_gas_wei = 0
                acquisition_status = "PRIOR_SEAPORT_PURCHASE_EXACT"
            else:
                acquisition_status = "TRANSFER_OR_PRIOR_PURCHASE_UNRESOLVED"

        seller_net = float(sale["allocated_seller_net_raw"]) if sale.get("allocated_seller_net_raw") not in (None, "") else None
        sale_asset = sale.get("payment_token")
        sale_tx_obj = txs.get(sale_tx, {})
        sale_receipt = receipts.get(sale_tx, {})
        sale_tx_items = sale_by_tx.get(sale_tx, [])
        exit_gas_wei = None
        if str(sale_tx_obj.get("from") or "").lower() == seller and sale_tx_items:
            sellers = {item.get("seller", "").lower() for item in sale_tx_items}
            if len(sellers) == 1:
                exit_gas_wei = integer(sale_receipt.get("gasUsed")) * integer(sale_receipt.get("effectiveGasPrice")) / sum(max(1, integer(item.get("nft_amount"))) for item in sale_tx_items)
        elif str(sale_tx_obj.get("from") or "").lower() != seller:
            exit_gas_wei = 0

        pnl_status = "NOT_CALCULABLE"
        pnl_raw = None
        if (
            acquisition_price_raw is not None
            and seller_net is not None
            and entry_gas_wei is not None
            and exit_gas_wei is not None
            and asset_group(acquisition_payment_asset) == asset_group(sale_asset) == "RHC_ETH_EQUIVALENT"
        ):
            pnl_raw = seller_net - acquisition_price_raw - entry_gas_wei - exit_gas_wei
            pnl_status = "PROVEN_RHC_ETH_EQUIVALENT"
        execution_rows.append({
            "wallet": seller,
            "nft_contract": contract,
            "token_id": token_id,
            "sale_transaction_hash": sale_tx,
            "sale_block_number": integer(sale.get("block_number")),
            "buyer": buyer,
            "sale_payment_asset": sale_asset,
            "seller_net_raw": seller_net,
            "sale_gas_paid_by_seller_wei": exit_gas_wei,
            "preceding_transfer_status": prior_kind,
            "acquisition_transaction_hash": acquisition_tx,
            "acquisition_status": acquisition_status,
            "acquisition_payment_asset": acquisition_payment_asset,
            "acquisition_price_raw": acquisition_price_raw,
            "entry_gas_paid_by_wallet_wei": entry_gas_wei,
            "realized_pnl_raw": pnl_raw,
            "realized_pnl_status": pnl_status,
            "production_approved": False,
        })

    metrics: list[dict[str, Any]] = []
    by_wallet: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in execution_rows:
        by_wallet[row["wallet"]].append(row)
    for wallet, rows in by_wallet.items():
        proven = [row for row in rows if row["realized_pnl_status"] == "PROVEN_RHC_ETH_EQUIVALENT"]
        values = [float(row["realized_pnl_raw"]) for row in proven]
        metrics.append({
            "wallet": wallet,
            "proven_sale_lots": len(rows),
            "proven_realized_pnl_lots": len(proven),
            "realized_win_rate": sum(value > 0 for value in values) / len(values) if values else None,
            "total_realized_pnl_raw": sum(values) if values else None,
            "median_realized_pnl_raw": statistics.median(values) if values else None,
            "unresolved_sale_lots": len(rows) - len(proven),
            "execution_classification": "RESEARCH_EXECUTION_ALPHA" if proven else "EXECUTION_NOT_EVALUABLE",
            "production_approved": False,
        })

    candidate_wallets = {row.get("minter", "").lower() for row in candidates if row.get("minter")}
    candidate_execution = [row for row in execution_rows if row["wallet"] in candidate_wallets]
    unresolved = [row for row in execution_rows if row["realized_pnl_status"] != "PROVEN_RHC_ETH_EQUIVALENT"]

    write_csv(out / "token_sale_execution_lots.csv", execution_rows)
    write_csv(out / "wallet_execution_alpha.csv", metrics)
    write_csv(out / "candidate_wallet_execution_lots.csv", candidate_execution)
    write_csv(out / "unresolved_execution_lots.csv", unresolved)

    validation = {
        "status": "PASS",
        "sale_lot_rows": len(execution_rows),
        "wallet_metric_rows": len(metrics),
        "proven_realized_pnl_lots": sum(row["realized_pnl_status"] == "PROVEN_RHC_ETH_EQUIVALENT" for row in execution_rows),
        "unresolved_sale_lots": len(unresolved),
        "candidate_wallet_execution_lots": len(candidate_execution),
        "production_approved_wallets": 0,
        "deepseek_handoff_allowed": False,
        "note": "PASS means deterministic classification completed; unresolved lots remain explicitly unresolved.",
    }
    (out / "VALIDATION.json").write_text(json.dumps(validation, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    manifest = []
    for path in sorted(out.iterdir()):
        if path.is_file():
            manifest.append({"path": path.name, "bytes": path.stat().st_size, "sha256": sha256(path)})
    (out / "MANIFEST.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(validation, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
