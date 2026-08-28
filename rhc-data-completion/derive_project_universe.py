#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import statistics
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from canonical_rpc import CanonicalRpc

TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
ZERO = "0x0000000000000000000000000000000000000000"
WETH_RHC = "0x0bd7d308f8e1639fab988df18a8011f41eacad73"
NATIVE = ZERO
PAYMENT_TYPES = {0, 1}
NFT_TYPES = {2, 3, 4, 5}
WINDOWS = {"15m": 15 * 60, "30m": 30 * 60, "2h": 2 * 3600, "24h": 24 * 3600}


def integer(value: Any, default: int = 0) -> int:
    try:
        text = str(value or "0")
        return int(text, 16) if text.startswith("0x") else int(float(text))
    except Exception:
        return default


def address_word(word: str) -> str:
    return "0x" + word[-40:].lower()


def topic_address(topic: str) -> str:
    return "0x" + topic[-40:].lower()


def data_words(data: str) -> list[str]:
    raw = data[2:] if data.startswith("0x") else data
    if len(raw) % 64:
        raise ValueError("ABI data word alignment failure")
    return [raw[index:index + 64] for index in range(0, len(raw), 64)]


def decode_array(words: list[str], offset_bytes: int, tuple_words: int) -> list[list[str]]:
    index = offset_bytes // 32
    if index < 0 or index >= len(words):
        raise ValueError("array offset outside event data")
    length = int(words[index], 16)
    start = index + 1
    end = start + length * tuple_words
    if end > len(words):
        raise ValueError("array payload truncated")
    return [words[start + item * tuple_words:start + (item + 1) * tuple_words] for item in range(length)]


def decode_order_fulfilled(row: dict[str, str]) -> dict[str, Any]:
    topics = json.loads(row["topics_json"])
    words = data_words(row["data"])
    if len(topics) != 4 or len(words) < 3:
        raise ValueError("unexpected OrderFulfilled encoding")
    recipient = address_word(words[0])
    offer_tuples = decode_array(words, int(words[1], 16), 4)
    consideration_tuples = decode_array(words, int(words[2], 16), 5)
    offer = [
        {"item_type": int(item[0], 16), "token": address_word(item[1]), "identifier": int(item[2], 16), "amount": int(item[3], 16)}
        for item in offer_tuples
    ]
    consideration = [
        {"item_type": int(item[0], 16), "token": address_word(item[1]), "identifier": int(item[2], 16), "amount": int(item[3], 16), "recipient": address_word(item[4])}
        for item in consideration_tuples
    ]
    return {
        "order_hash": topics[1].lower(),
        "offerer": topic_address(topics[2]),
        "zone": topic_address(topics[3]),
        "recipient": recipient,
        "offer": offer,
        "consideration": consideration,
    }


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
            writer.writerow({
                key: json.dumps(value, ensure_ascii=False, sort_keys=True)
                if isinstance(value, (dict, list, tuple)) else value
                for key, value in row.items()
            })


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def batch_rpc(rpc: CanonicalRpc, method: str, values: list[str], batch_size: int = 20) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    results: dict[str, Any] = {}
    errors: list[dict[str, Any]] = []
    # Use individual calls through the audited retry/pacing client. This is slower but fail-closed and resumable by output key.
    for index, value in enumerate(values, 1):
        try:
            results[value] = rpc.call(method, [value, False] if method == "eth_getBlockByNumber" else [value])
        except Exception as exc:
            errors.append({"method": method, "value": value, "error": repr(exc)})
        if index % batch_size == 0:
            print({"method": method, "done": index, "total": len(values), "errors": len(errors)}, flush=True)
    return results, errors


def transfer_logs(receipt: dict[str, Any]) -> list[dict[str, Any]]:
    output = []
    for log in receipt.get("logs") or []:
        topics = [str(value).lower() for value in (log.get("topics") or [])]
        if not topics or topics[0] != TRANSFER_TOPIC:
            continue
        if len(topics) == 4:
            output.append({
                "kind": "ERC721",
                "contract": str(log.get("address") or "").lower(),
                "from": topic_address(topics[1]),
                "to": topic_address(topics[2]),
                "token_id": int(topics[3], 16),
                "amount": 1,
                "log_index": integer(log.get("logIndex")),
            })
        elif len(topics) == 3:
            output.append({
                "kind": "ERC20",
                "contract": str(log.get("address") or "").lower(),
                "from": topic_address(topics[1]),
                "to": topic_address(topics[2]),
                "token_id": 0,
                "amount": integer(log.get("data")),
                "log_index": integer(log.get("logIndex")),
            })
    return output


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--canonical-dir", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    canonical = Path(args.canonical_dir)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    events_path = next(iter(canonical.rglob("global_events.csv")), None)
    completeness_path = next(iter(canonical.rglob("DATA_COMPLETENESS.json")), None)
    if events_path is None or completeness_path is None:
        raise SystemExit("canonical package missing global_events.csv or DATA_COMPLETENESS.json")
    canonical_status = json.loads(completeness_path.read_text(encoding="utf-8"))
    rows = read_csv(events_path)
    seadrop_rows = [row for row in rows if row.get("target") == "seadrop"]
    seaport_rows = [row for row in rows if row.get("target") == "seaport"]
    if not seadrop_rows or not seaport_rows:
        raise SystemExit("global canonical event histories are empty")

    unique_blocks = sorted({integer(row.get("block_number")) for row in rows})
    unique_txs = sorted({row.get("transaction_hash", "").lower() for row in rows if row.get("transaction_hash")})
    block_hex = [hex(value) for value in unique_blocks]
    rpc = CanonicalRpc(min_interval=1.35)
    block_results, block_errors = batch_rpc(rpc, "eth_getBlockByNumber", block_hex, batch_size=50)
    receipt_results, receipt_errors = batch_rpc(rpc, "eth_getTransactionReceipt", unique_txs, batch_size=25)

    block_timestamps: dict[int, int] = {}
    block_rows: list[dict[str, Any]] = []
    for block_number, key in zip(unique_blocks, block_hex):
        block = block_results.get(key)
        if isinstance(block, dict):
            timestamp = integer(block.get("timestamp"))
            block_timestamps[block_number] = timestamp
            block_rows.append({
                "block_number": block_number,
                "block_hash": str(block.get("hash") or "").lower(),
                "timestamp_unix": timestamp,
                "timestamp_utc": datetime.fromtimestamp(timestamp, timezone.utc).isoformat().replace("+00:00", "Z"),
            })

    receipt_path = out / "event_receipts.jsonl"
    with receipt_path.open("w", encoding="utf-8", newline="\n") as handle:
        for tx_hash in unique_txs:
            receipt = receipt_results.get(tx_hash)
            if isinstance(receipt, dict):
                handle.write(json.dumps(receipt, sort_keys=True, separators=(",", ":")) + "\n")

    # Primary mint events and opportunity definitions.
    mint_events: list[dict[str, Any]] = []
    project_events: dict[str, list[dict[str, Any]]] = defaultdict(list)
    cumulative_quantity: dict[str, int] = defaultdict(int)
    ordered_seadrop = sorted(seadrop_rows, key=lambda row: (integer(row.get("block_number")), integer(row.get("transaction_index")), integer(row.get("log_index"))))
    for row in ordered_seadrop:
        contract = row.get("nft_contract", "").lower()
        block_number = integer(row.get("block_number"))
        timestamp = block_timestamps.get(block_number)
        quantity = integer(row.get("quantity_minted"))
        before = cumulative_quantity[contract]
        cumulative_quantity[contract] += quantity
        event = {
            "project_id": contract,
            "nft_contract": contract,
            "block_number": block_number,
            "block_hash": row.get("block_hash"),
            "timestamp_unix": timestamp,
            "timestamp_utc": datetime.fromtimestamp(timestamp, timezone.utc).isoformat().replace("+00:00", "Z") if timestamp is not None else None,
            "transaction_hash": row.get("transaction_hash"),
            "log_index": integer(row.get("log_index")),
            "minter": row.get("minter", "").lower(),
            "payer": row.get("payer", "").lower(),
            "fee_recipient": row.get("fee_recipient", "").lower(),
            "quantity": quantity,
            "unit_mint_price_wei": integer(row.get("unit_mint_price_wei")),
            "fee_bps": integer(row.get("fee_bps")),
            "drop_stage_index": integer(row.get("drop_stage_index")),
            "is_public": integer(row.get("drop_stage_index")) == 0,
            "is_paid": integer(row.get("unit_mint_price_wei")) > 0,
            "is_self_funded": row.get("minter", "").lower() == row.get("payer", "").lower(),
            "cumulative_quantity_before": before,
            "cumulative_quantity_after": cumulative_quantity[contract],
        }
        mint_events.append(event)
        project_events[contract].append(event)

    project_opportunities: list[dict[str, Any]] = []
    opportunity_by_project: dict[str, dict[str, Any]] = {}
    for contract, events in project_events.items():
        public_events = [event for event in events if event["is_public"]]
        paid_public = [event for event in public_events if event["is_paid"]]
        free_events = [event for event in events if not event["is_paid"]]
        paid_nonpublic = [event for event in events if event["is_paid"] and not event["is_public"]]
        start = min((event["timestamp_unix"] for event in events if event["timestamp_unix"] is not None), default=None)
        paid_public_start = min((event["timestamp_unix"] for event in paid_public if event["timestamp_unix"] is not None), default=None)
        free_before_paid_public = any(
            event["timestamp_unix"] is not None and paid_public_start is not None and event["timestamp_unix"] <= paid_public_start
            for event in free_events
        )
        prices = sorted({event["unit_mint_price_wei"] for event in paid_public})
        row = {
            "project_id": contract,
            "nft_contract": contract,
            "first_mint_timestamp_unix": start,
            "first_mint_timestamp_utc": datetime.fromtimestamp(start, timezone.utc).isoformat().replace("+00:00", "Z") if start is not None else None,
            "first_paid_public_timestamp_unix": paid_public_start,
            "first_paid_public_timestamp_utc": datetime.fromtimestamp(paid_public_start, timezone.utc).isoformat().replace("+00:00", "Z") if paid_public_start is not None else None,
            "mint_event_rows": len(events),
            "minted_quantity_observed": sum(event["quantity"] for event in events),
            "unique_minters": len({event["minter"] for event in events}),
            "public_event_rows": len(public_events),
            "paid_public_event_rows": len(paid_public),
            "paid_nonpublic_event_rows": len(paid_nonpublic),
            "free_event_rows": len(free_events),
            "free_before_paid_public": free_before_paid_public,
            "paid_public_prices_wei_json": json.dumps(prices),
            "reference_paid_public_price_wei": statistics.median(prices) if prices else None,
            "has_paid_public_opportunity": bool(paid_public),
            "strict_paid_public_from_start": bool(paid_public) and not free_before_paid_public and len(prices) == 1,
            "opportunity_universe": "SEADROP_OBSERVED",
        }
        project_opportunities.append(row)
        opportunity_by_project[contract] = row

    # Decode Seaport orders and prove transfers with canonical receipts.
    sales: list[dict[str, Any]] = []
    order_errors: list[dict[str, Any]] = []
    for row in seaport_rows:
        tx_hash = row.get("transaction_hash", "").lower()
        receipt = receipt_results.get(tx_hash)
        block_number = integer(row.get("block_number"))
        timestamp = block_timestamps.get(block_number)
        try:
            decoded = decode_order_fulfilled(row)
            offer_nfts = [item for item in decoded["offer"] if item["item_type"] in NFT_TYPES]
            consideration_nfts = [item for item in decoded["consideration"] if item["item_type"] in NFT_TYPES]
            direction = "NFT_OFFER_LISTING" if offer_nfts else "PAYMENT_OFFER_BID" if consideration_nfts else "NO_NFT"
            nft_items = offer_nfts or consideration_nfts
            payment_items = [item for item in (decoded["consideration"] if offer_nfts else decoded["offer"]) if item["item_type"] in PAYMENT_TYPES]
            payment_tokens = {item["token"] for item in payment_items}
            payment_token = next(iter(payment_tokens)) if len(payment_tokens) == 1 else None
            order_gross = sum(item["amount"] for item in payment_items) if payment_token is not None else None
            receipt_transfers = transfer_logs(receipt) if isinstance(receipt, dict) else []
            for item in nft_items:
                contract = item["token"]
                amount = max(1, item["amount"])
                seller = decoded["offerer"] if offer_nfts else decoded["recipient"]
                buyer = decoded["recipient"] if offer_nfts else item.get("recipient") or decoded["offerer"]
                matching_nft = [transfer for transfer in receipt_transfers if transfer["kind"] == "ERC721" and transfer["contract"] == contract and transfer["token_id"] == item["identifier"] and transfer["from"] == seller and transfer["to"] == buyer]
                payment_to_seller = sum(transfer["amount"] for transfer in receipt_transfers if transfer["kind"] == "ERC20" and transfer["contract"] == payment_token and transfer["to"] == seller) if payment_token else 0
                gross_per_unit = order_gross / sum(max(1, value["amount"]) for value in nft_items) if order_gross is not None and nft_items else None
                sales.append({
                    "project_id": contract,
                    "nft_contract": contract,
                    "token_id": item["identifier"],
                    "nft_amount": amount,
                    "block_number": block_number,
                    "block_hash": row.get("block_hash"),
                    "timestamp_unix": timestamp,
                    "timestamp_utc": datetime.fromtimestamp(timestamp, timezone.utc).isoformat().replace("+00:00", "Z") if timestamp is not None else None,
                    "transaction_hash": tx_hash,
                    "log_index": integer(row.get("log_index")),
                    "order_hash": decoded["order_hash"],
                    "direction": direction,
                    "seller": seller,
                    "buyer": buyer,
                    "payment_token": payment_token,
                    "order_gross_payment_raw": order_gross,
                    "gross_payment_per_nft_unit_raw": gross_per_unit,
                    "payment_to_seller_raw_receipt": payment_to_seller,
                    "receipt_status": integer(receipt.get("status")) if isinstance(receipt, dict) else None,
                    "matching_nft_transfer_count": len(matching_nft),
                    "sale_proof_status": "PROVEN_ORDER_AND_NFT_TRANSFER" if isinstance(receipt, dict) and integer(receipt.get("status")) == 1 and matching_nft else "ORDER_ONLY_OR_TRANSFER_UNRESOLVED",
                    "offer_json": json.dumps(decoded["offer"], sort_keys=True, separators=(",", ":")),
                    "consideration_json": json.dumps(decoded["consideration"], sort_keys=True, separators=(",", ":")),
                })
        except Exception as exc:
            order_errors.append({"transaction_hash": tx_hash, "log_index": row.get("log_index"), "error": repr(exc)})

    sales_by_project: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for sale in sales:
        if sale["sale_proof_status"] == "PROVEN_ORDER_AND_NFT_TRANSFER":
            sales_by_project[sale["project_id"]].append(sale)

    project_outcomes: list[dict[str, Any]] = []
    for opportunity in project_opportunities:
        project_id = opportunity["project_id"]
        start = opportunity["first_paid_public_timestamp_unix"]
        reference_price = opportunity["reference_paid_public_price_wei"]
        base = {
            "project_id": project_id,
            "nft_contract": project_id,
            "first_paid_public_timestamp_unix": start,
            "reference_paid_public_price_wei": reference_price,
        }
        project_sales = sales_by_project.get(project_id, [])
        for label, seconds in WINDOWS.items():
            eligible = [sale for sale in project_sales if start is not None and sale["timestamp_unix"] is not None and start <= sale["timestamp_unix"] <= start + seconds and sale["payment_token"] in {NATIVE, WETH_RHC} and sale["gross_payment_per_nft_unit_raw"] is not None]
            order_keys = {(sale["transaction_hash"], sale["order_hash"]) for sale in eligible}
            buyers = {sale["buyer"] for sale in eligible if sale["buyer"]}
            prices = [float(sale["gross_payment_per_nft_unit_raw"]) for sale in eligible]
            median_price = statistics.median(prices) if prices else None
            multiple = median_price / float(reference_price) if median_price is not None and reference_price else None
            base[f"orders_{label}"] = len(order_keys)
            base[f"independent_buyers_{label}"] = len(buyers)
            base[f"sale_units_{label}"] = sum(integer(sale["nft_amount"]) for sale in eligible)
            base[f"median_gross_per_unit_raw_{label}"] = median_price
            base[f"median_multiple_vs_mint_{label}"] = multiple
            base[f"success_liquid_100_{label}"] = bool(multiple is not None and multiple >= 1.0 and len(order_keys) >= 3 and len(buyers) >= 3)
            base[f"success_liquid_115_{label}"] = bool(multiple is not None and multiple >= 1.15 and len(order_keys) >= 3 and len(buyers) >= 3)
        project_outcomes.append(base)
    outcome_by_project = {row["project_id"]: row for row in project_outcomes}

    # Build early participation entries before first proven sale and within the first 20% of observed primary quantity.
    first_sale_time = {project: min(sale["timestamp_unix"] for sale in project_sales if sale["timestamp_unix"] is not None) for project, project_sales in sales_by_project.items() if any(sale["timestamp_unix"] is not None for sale in project_sales)}
    wallet_entries: list[dict[str, Any]] = []
    for event in mint_events:
        opportunity = opportunity_by_project[event["project_id"]]
        total_quantity = max(1, integer(opportunity["minted_quantity_observed"]))
        entry_quantile = event["cumulative_quantity_after"] / total_quantity
        before_sale = first_sale_time.get(event["project_id"]) is None or (event["timestamp_unix"] is not None and event["timestamp_unix"] < first_sale_time[event["project_id"]])
        early = entry_quantile <= 0.20 and before_sale
        wallet_entries.append({
            **event,
            "entry_quantity_quantile": entry_quantile,
            "before_first_proven_sale": before_sale,
            "is_early_entry": early,
            "selection_signal_eligible": bool(early and event["is_paid"] and event["is_self_funded"]),
            "signal_type": "PUBLIC_SELF_FUNDED" if event["is_public"] and event["is_paid"] and event["is_self_funded"] else "PRIVILEGED_SELF_FUNDED" if event["is_paid"] and event["is_self_funded"] else "OTHER",
        })

    # Matched baseline Selection Alpha for public self-funded early entries.
    paid_public_opportunities = [row for row in project_opportunities if row["has_paid_public_opportunity"] and row["first_paid_public_timestamp_unix"] is not None and row["reference_paid_public_price_wei"]]
    eligible_entries = [row for row in wallet_entries if row["selection_signal_eligible"] and row["signal_type"] == "PUBLIC_SELF_FUNDED"]
    entry_by_wallet: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
    for row in eligible_entries:
        entry_by_wallet[row["minter"]][row["project_id"]] = row
    wallet_metrics: list[dict[str, Any]] = []
    for wallet, projects in entry_by_wallet.items():
        samples: list[dict[str, Any]] = []
        for project_id, entry in projects.items():
            opportunity = opportunity_by_project[project_id]
            start = integer(opportunity["first_paid_public_timestamp_unix"])
            price = float(opportunity["reference_paid_public_price_wei"])
            comparable = []
            for candidate in paid_public_opportunities:
                candidate_start = integer(candidate["first_paid_public_timestamp_unix"])
                candidate_price = float(candidate["reference_paid_public_price_wei"])
                if abs(candidate_start - start) <= 3 * 86400 and 0.25 <= candidate_price / price <= 4.0:
                    comparable.append(candidate["project_id"])
            outcome = outcome_by_project.get(project_id, {})
            comparison_outcomes = [outcome_by_project.get(value, {}) for value in comparable]
            for metric in ("success_liquid_100_24h", "success_liquid_115_24h"):
                observed = 1.0 if outcome.get(metric) else 0.0
                baseline_values = [1.0 if value.get(metric) else 0.0 for value in comparison_outcomes]
                baseline = statistics.mean(baseline_values) if baseline_values else None
                samples.append({"metric": metric, "observed": observed, "baseline": baseline, "project_id": project_id, "comparable_projects": len(comparable)})
        result: dict[str, Any] = {
            "wallet": wallet,
            "entered_projects": len(projects),
            "median_entry_quantity_quantile": statistics.median(row["entry_quantity_quantile"] for row in projects.values()),
            "production_approved": False,
            "decision_use": "RESEARCH_SELECTION_ALPHA_ONLY",
        }
        for metric in ("success_liquid_100_24h", "success_liquid_115_24h"):
            values = [sample for sample in samples if sample["metric"] == metric and sample["baseline"] is not None]
            result[f"{metric}_sample_count"] = len(values)
            result[f"{metric}_hit_rate"] = statistics.mean(sample["observed"] for sample in values) if values else None
            result[f"{metric}_matched_baseline"] = statistics.mean(sample["baseline"] for sample in values) if values else None
            result[f"{metric}_predictive_lift"] = statistics.mean(sample["observed"] - sample["baseline"] for sample in values) if values else None
        wallet_metrics.append(result)

    write_csv(out / "block_timestamps.csv", block_rows)
    write_csv(out / "primary_mint_events.csv", mint_events)
    write_csv(out / "project_opportunities.csv", project_opportunities)
    write_csv(out / "seaport_sales.csv", sales)
    write_csv(out / "project_outcomes.csv", project_outcomes)
    write_csv(out / "wallet_project_entries.csv", wallet_entries)
    write_csv(out / "wallet_selection_alpha.csv", wallet_metrics)
    write_csv(out / "order_decode_errors.csv", order_errors)
    write_csv(out / "rpc_errors.csv", block_errors + receipt_errors)

    missing_blocks = sorted(set(unique_blocks) - set(block_timestamps))
    missing_receipts = sorted(set(unique_txs) - {key for key, value in receipt_results.items() if isinstance(value, dict)})
    validation = {
        "status": "PASS" if not missing_blocks and not missing_receipts and not order_errors else "PARTIAL_REMEDIATION_REQUIRED",
        "canonical_input_status": canonical_status.get("status"),
        "seadrop_event_rows": len(seadrop_rows),
        "seaport_event_rows": len(seaport_rows),
        "unique_event_blocks": len(unique_blocks),
        "block_timestamps_resolved": len(block_timestamps),
        "missing_block_count": len(missing_blocks),
        "unique_event_transactions": len(unique_txs),
        "receipts_resolved": sum(isinstance(value, dict) for value in receipt_results.values()),
        "missing_receipt_count": len(missing_receipts),
        "project_opportunity_rows": len(project_opportunities),
        "paid_public_opportunities": sum(bool(row["has_paid_public_opportunity"]) for row in project_opportunities),
        "strict_paid_public_from_start": sum(bool(row["strict_paid_public_from_start"]) for row in project_opportunities),
        "proven_sale_rows": sum(row["sale_proof_status"] == "PROVEN_ORDER_AND_NFT_TRANSFER" for row in sales),
        "wallet_project_entry_rows": len(wallet_entries),
        "selection_metric_wallets": len(wallet_metrics),
        "order_decode_errors": len(order_errors),
        "block_rpc_errors": block_errors,
        "receipt_rpc_errors": receipt_errors,
        "missing_blocks": missing_blocks,
        "missing_receipts": missing_receipts,
        "production_approved_wallets": 0,
        "deepseek_handoff_allowed": False,
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
