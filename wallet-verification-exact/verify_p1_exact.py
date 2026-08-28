#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import random
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable

CHAIN_ID = 4663
ZERO = "0x0000000000000000000000000000000000000000"
TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
SEADROP_MINT_TOPIC = "0xe90cf9cc0a552cf52ea6ff74ece0f1c8ae8cc9ad630d3181f55ac43ca076b7d6"
SEAPORT_FULFILLED_TOPIC = "0x9d9af8e38d66c62e2c12f0225249fd9d721c54b83f48d9352c97c6cacdcb6f31"
ADDR_RE = re.compile(r"^0x[a-fA-F0-9]{40}$")
TX_RE = re.compile(r"0x[a-fA-F0-9]{64}")
RPC_URLS = [
    "https://rpc.mainnet.chain.robinhood.com",
]
BLOCKSCOUT_V2 = "https://robinhoodchain.blockscout.com/api/v2"
UA = "RHC-P1-Exact-Evidence/1.0"


def lower_address(value: Any) -> str | None:
    if isinstance(value, dict):
        for key in ("hash", "address", "address_hash", "value"):
            if key in value:
                return lower_address(value[key])
        return None
    if isinstance(value, str) and ADDR_RE.fullmatch(value):
        return value.lower()
    return None


def intish(value: Any, default: int = 0) -> int:
    if value is None:
        return default
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value, 16) if value.startswith("0x") else int(value)
        except ValueError:
            return default
    return default


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fields = sorted({key for row in rows for key in row}) if rows else ["empty"]
    with path.open("w", newline="", encoding="utf-8-sig") as file:
        writer = csv.DictWriter(file, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({
                key: json.dumps(value, ensure_ascii=False, sort_keys=True)
                if isinstance(value, (list, dict, tuple))
                else value
                for key, value in row.items()
            })


def walk(value: Any, path: str = "") -> Iterable[tuple[str, Any]]:
    if isinstance(value, dict):
        for key, item in value.items():
            next_path = f"{path}.{key}" if path else key
            yield from walk(item, next_path)
    elif isinstance(value, list):
        for index, item in enumerate(value):
            yield from walk(item, f"{path}[{index}]")
    else:
        yield path, value


def classify_path(path: str) -> str:
    text = path.lower()
    if any(token in text for token in ("sale", "sell", "sold", "secondary")):
        return "sale"
    if any(token in text for token in ("mint", "primary")):
        return "mint"
    if any(token in text for token in ("buy", "purchase", "acquisition")):
        return "purchase"
    return "other"


def extract_claimed_hashes(row: dict[str, Any]) -> list[dict[str, str]]:
    seen: set[tuple[str, str]] = set()
    out: list[dict[str, str]] = []
    for path, value in walk(row):
        if not isinstance(value, str):
            continue
        for match in TX_RE.findall(value):
            tx_hash = match.lower()
            category = classify_path(path)
            key = (tx_hash, category)
            if key in seen:
                continue
            seen.add(key)
            out.append({"tx_hash": tx_hash, "claim_category": category, "source_path": path})
    return out


class Client:
    def __init__(self) -> None:
        self.rpc_id = 0
        self.last_request = 0.0

    def pace(self, seconds: float = 0.18) -> None:
        wait = seconds - (time.monotonic() - self.last_request)
        if wait > 0:
            time.sleep(wait)

    def request_json(
        self,
        url: str,
        *,
        data: bytes | None = None,
        headers: dict[str, str] | None = None,
        attempts: int = 7,
    ) -> Any:
        last_error: Exception | None = None
        for attempt in range(attempts):
            self.pace()
            request = urllib.request.Request(
                url,
                data=data,
                headers={"User-Agent": UA, "Accept": "application/json", **(headers or {})},
            )
            try:
                with urllib.request.urlopen(request, timeout=60) as response:
                    body = response.read()
                self.last_request = time.monotonic()
                return json.loads(body.decode("utf-8"))
            except urllib.error.HTTPError as exc:
                self.last_request = time.monotonic()
                last_error = RuntimeError(f"HTTP {exc.code}: {exc.read(500).decode('utf-8', 'replace')}")
                if exc.code in (429, 500, 502, 503, 504):
                    time.sleep(min(30, 2 ** attempt + random.random()))
                    continue
                raise last_error
            except Exception as exc:
                last_error = exc
                if attempt + 1 < attempts:
                    time.sleep(min(20, 2 ** attempt + random.random()))
                    continue
        raise RuntimeError(f"request failed: {url}: {last_error}")

    def rpc(self, method: str, params: list[Any]) -> Any:
        errors: list[str] = []
        for url in RPC_URLS:
            self.rpc_id += 1
            payload = json.dumps({
                "jsonrpc": "2.0",
                "id": self.rpc_id,
                "method": method,
                "params": params,
            }).encode("utf-8")
            try:
                result = self.request_json(
                    url,
                    data=payload,
                    headers={"Content-Type": "application/json"},
                )
                if isinstance(result, dict) and result.get("error"):
                    errors.append(f"{url}:{result['error']}")
                    continue
                return result.get("result") if isinstance(result, dict) else None
            except Exception as exc:
                errors.append(f"{url}:{exc!r}")
        raise RuntimeError(" | ".join(errors))

    def internal_transactions(self, tx_hash: str) -> list[dict[str, Any]]:
        url = f"{BLOCKSCOUT_V2}/transactions/{tx_hash}/internal-transactions"
        try:
            payload = self.request_json(url)
        except Exception:
            return []
        rows = payload.get("items") if isinstance(payload, dict) else payload
        return [row for row in (rows or []) if isinstance(row, dict)]


def topic_address(topic: str | None) -> str | None:
    if not isinstance(topic, str) or len(topic) < 42:
        return None
    return "0x" + topic[-40:].lower()


def data_words(data: str | None) -> list[str]:
    if not isinstance(data, str):
        return []
    value = data[2:] if data.startswith("0x") else data
    return [value[index:index + 64] for index in range(0, len(value), 64) if len(value[index:index + 64]) == 64]


def parse_tx_evidence(
    wallet: str,
    tx_hash: str,
    tx: dict[str, Any] | None,
    receipt: dict[str, Any] | None,
    internals: list[dict[str, Any]],
) -> dict[str, Any]:
    wallet = wallet.lower()
    logs = receipt.get("logs") if isinstance(receipt, dict) else []
    logs = logs if isinstance(logs, list) else []

    zero_nft_to_wallet: list[dict[str, Any]] = []
    nft_out_from_wallet: list[dict[str, Any]] = []
    nft_in_to_wallet: list[dict[str, Any]] = []
    erc20_in_to_wallet: list[dict[str, Any]] = []
    erc20_out_from_wallet: list[dict[str, Any]] = []
    seadrop_rows: list[dict[str, Any]] = []
    seaport_count = 0

    for log in logs:
        if not isinstance(log, dict):
            continue
        topics = [str(value).lower() for value in (log.get("topics") or [])]
        if not topics:
            continue
        topic0 = topics[0]
        emitting = lower_address(log.get("address"))
        if topic0 == TRANSFER_TOPIC and len(topics) >= 3:
            source = topic_address(topics[1])
            destination = topic_address(topics[2])
            if len(topics) >= 4:
                token_id = intish(topics[3])
                row = {
                    "contract": emitting,
                    "from": source,
                    "to": destination,
                    "token_id": str(token_id),
                    "log_index": intish(log.get("logIndex")),
                }
                if source == ZERO and destination == wallet:
                    zero_nft_to_wallet.append(row)
                elif source == wallet and destination not in (None, ZERO):
                    nft_out_from_wallet.append(row)
                elif destination == wallet and source not in (None, ZERO):
                    nft_in_to_wallet.append(row)
            else:
                amount = intish(log.get("data"))
                row = {
                    "token": emitting,
                    "from": source,
                    "to": destination,
                    "amount_raw": str(amount),
                    "log_index": intish(log.get("logIndex")),
                }
                if destination == wallet and amount > 0:
                    erc20_in_to_wallet.append(row)
                if source == wallet and amount > 0:
                    erc20_out_from_wallet.append(row)
        elif topic0 == SEADROP_MINT_TOPIC and len(topics) >= 3:
            words = data_words(log.get("data"))
            payer = "0x" + words[0][-40:] if len(words) >= 1 else None
            seadrop_rows.append({
                "nft_contract": topic_address(topics[1]),
                "minter": topic_address(topics[2]),
                "fee_recipient": topic_address(topics[3]) if len(topics) >= 4 else None,
                "payer": payer,
                "quantity": int(words[1], 16) if len(words) >= 2 else None,
                "unit_mint_price_wei": int(words[2], 16) if len(words) >= 3 else None,
                "fee_bps": int(words[3], 16) if len(words) >= 4 else None,
                "drop_stage_index": int(words[4], 16) if len(words) >= 5 else None,
                "log_index": intish(log.get("logIndex")),
            })
        elif topic0 == SEAPORT_FULFILLED_TOPIC:
            seaport_count += 1

    native_in_to_wallet: list[dict[str, Any]] = []
    native_out_from_wallet: list[dict[str, Any]] = []
    for row in internals:
        source = lower_address(row.get("from"))
        destination = lower_address(row.get("to"))
        value = intish(row.get("value"))
        if destination == wallet and value > 0:
            native_in_to_wallet.append({"from": source, "to": destination, "value_wei": str(value)})
        if source == wallet and value > 0:
            native_out_from_wallet.append({"from": source, "to": destination, "value_wei": str(value)})

    tx_from = lower_address(tx.get("from")) if isinstance(tx, dict) else None
    tx_to = lower_address(tx.get("to")) if isinstance(tx, dict) else None
    tx_value = intish(tx.get("value")) if isinstance(tx, dict) else 0
    receipt_status = intish(receipt.get("status"), -1) if isinstance(receipt, dict) else -1

    seadrop_wallet_role = any(
        row.get("minter") == wallet or row.get("payer") == wallet
        for row in seadrop_rows
    )
    mint_proven = receipt_status == 1 and bool(zero_nft_to_wallet) and (tx_from == wallet or seadrop_wallet_role)
    sale_proven = (
        receipt_status == 1
        and bool(nft_out_from_wallet)
        and seaport_count > 0
        and bool(erc20_in_to_wallet or native_in_to_wallet)
    )
    purchase_proven = (
        receipt_status == 1
        and bool(nft_in_to_wallet)
        and bool(erc20_out_from_wallet or native_out_from_wallet or (tx_from == wallet and tx_value > 0))
    )

    return {
        "tx_hash": tx_hash,
        "tx_found": bool(tx),
        "receipt_found": bool(receipt),
        "receipt_status": receipt_status,
        "block_number": intish(receipt.get("blockNumber")) if isinstance(receipt, dict) else None,
        "tx_from": tx_from,
        "tx_to": tx_to,
        "tx_value_wei": str(tx_value),
        "zero_nft_to_wallet": zero_nft_to_wallet,
        "nft_out_from_wallet": nft_out_from_wallet,
        "nft_in_to_wallet": nft_in_to_wallet,
        "erc20_in_to_wallet": erc20_in_to_wallet,
        "erc20_out_from_wallet": erc20_out_from_wallet,
        "native_in_to_wallet": native_in_to_wallet,
        "native_out_from_wallet": native_out_from_wallet,
        "seadrop_rows": seadrop_rows,
        "seaport_orderfulfilled_count": seaport_count,
        "mint_proven": mint_proven,
        "sale_proven": sale_proven,
        "purchase_proven": purchase_proven,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--wallets", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    source_path = Path(args.wallets)
    source_rows = json.loads(source_path.read_text(encoding="utf-8"))
    p1_rows = [row for row in source_rows if str(row.get("priority", "")).upper() == "P1"]

    client = Client()
    evidence_rows: list[dict[str, Any]] = []
    wallet_rows: list[dict[str, Any]] = []
    raw_rows: dict[str, Any] = {}
    rpc_errors: list[dict[str, Any]] = []

    for row in p1_rows:
        wallet = lower_address(row.get("wallet") or row.get("address"))
        claims = extract_claimed_hashes(row)
        counts = defaultdict(int)
        mismatches: list[dict[str, Any]] = []
        if not wallet:
            wallet_rows.append({
                "wallet": row.get("wallet") or row.get("address"),
                "priority": "P1",
                "verification_status": "INVALID_WALLET_INPUT",
                "production_approved": False,
            })
            continue

        for claim in claims:
            tx_hash = claim["tx_hash"]
            try:
                tx = client.rpc("eth_getTransactionByHash", [tx_hash])
                receipt = client.rpc("eth_getTransactionReceipt", [tx_hash])
                internals = client.internal_transactions(tx_hash)
                evidence = parse_tx_evidence(wallet, tx_hash, tx, receipt, internals)
                raw_rows[tx_hash] = {"transaction": tx, "receipt": receipt, "internal_transactions": internals}
            except Exception as exc:
                rpc_errors.append({"wallet": wallet, **claim, "error": repr(exc)})
                evidence = {
                    "tx_hash": tx_hash,
                    "tx_found": False,
                    "receipt_found": False,
                    "mint_proven": False,
                    "sale_proven": False,
                    "purchase_proven": False,
                    "error": repr(exc),
                }

            inferred = []
            for category in ("mint", "sale", "purchase"):
                if evidence.get(f"{category}_proven"):
                    inferred.append(category)
                    counts[f"exact_{category}_proven"] += 1
            if claim["claim_category"] in ("mint", "sale", "purchase") and claim["claim_category"] not in inferred:
                mismatches.append({
                    "tx_hash": tx_hash,
                    "claimed": claim["claim_category"],
                    "inferred": inferred,
                })
                counts["claim_mismatch"] += 1

            evidence_rows.append({
                "wallet": wallet,
                "priority": "P1",
                **claim,
                "inferred_categories": inferred,
                **evidence,
            })

        if not claims:
            status = "SOURCE_ONLY_NO_EXACT_TX_HASH"
        elif rpc_errors and all(error["wallet"] != wallet for error in rpc_errors):
            status = "EXACT_EVIDENCE_RESEARCH_ONLY"
        elif any(error["wallet"] == wallet for error in rpc_errors):
            status = "PROVIDER_ERROR_FAIL_CLOSED"
        elif counts["claim_mismatch"]:
            status = "CLAIM_MISMATCH_REVIEW"
        elif counts["exact_mint_proven"] or counts["exact_sale_proven"] or counts["exact_purchase_proven"]:
            status = "EXACT_EVIDENCE_RESEARCH_ONLY"
        else:
            status = "HASH_PRESENT_BUT_ROLE_NOT_PROVEN"

        wallet_rows.append({
            "wallet": wallet,
            "priority": "P1",
            "source_record": row,
            "claimed_tx_hashes": len(claims),
            "exact_mint_proven": counts["exact_mint_proven"],
            "exact_sale_proven": counts["exact_sale_proven"],
            "exact_purchase_proven": counts["exact_purchase_proven"],
            "claim_mismatches": counts["claim_mismatch"],
            "mismatch_details": mismatches,
            "verification_status": status,
            "strength_status": "NOT_EVALUATED",
            "selection_alpha_status": "NOT_EVALUATED",
            "execution_alpha_status": "NOT_EVALUATED",
            "copy_alpha_status": "NOT_EVALUATED",
            "production_approved": False,
        })

    write_csv(out / "p1_wallet_verification.csv", wallet_rows)
    write_csv(out / "p1_exact_tx_evidence.csv", evidence_rows)
    write_csv(out / "p1_rpc_errors.csv", rpc_errors)
    (out / "raw_rpc_evidence.json").write_text(
        json.dumps(raw_rows, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    (out / "input_p1.json").write_text(
        json.dumps(p1_rows, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    wallet_set = [row.get("wallet") for row in wallet_rows if row.get("wallet")]
    failures: list[dict[str, Any]] = []
    if len(p1_rows) != 19:
        failures.append({"code": "P1_INPUT_COUNT_MISMATCH", "value": len(p1_rows)})
    if len(wallet_set) != len(set(wallet_set)):
        failures.append({"code": "DUPLICATE_P1_WALLET"})
    if len(wallet_rows) != len(p1_rows):
        failures.append({"code": "OUTPUT_COUNT_MISMATCH", "input": len(p1_rows), "output": len(wallet_rows)})
    if rpc_errors:
        failures.append({"code": "RPC_ERRORS_FAIL_CLOSED", "count": len(rpc_errors)})

    validation = {
        "status": "PASS" if not failures else "FAIL",
        "p1_input_wallets": len(p1_rows),
        "p1_output_wallets": len(wallet_rows),
        "wallets_with_exact_tx_hash": sum(int(row.get("claimed_tx_hashes", 0) > 0) for row in wallet_rows),
        "source_only_wallets": sum(row.get("verification_status") == "SOURCE_ONLY_NO_EXACT_TX_HASH" for row in wallet_rows),
        "exact_mint_proven": sum(int(row.get("exact_mint_proven", 0)) for row in wallet_rows),
        "exact_sale_proven": sum(int(row.get("exact_sale_proven", 0)) for row in wallet_rows),
        "exact_purchase_proven": sum(int(row.get("exact_purchase_proven", 0)) for row in wallet_rows),
        "claim_mismatches": sum(int(row.get("claim_mismatches", 0)) for row in wallet_rows),
        "rpc_errors": len(rpc_errors),
        "production_approved_wallets": 0,
        "failures": failures,
        "decision_use": "EVIDENCE_VERIFICATION_ONLY_NOT_STRENGTH_SCORING",
    }
    (out / "VALIDATION.json").write_text(
        json.dumps(validation, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    manifest = []
    for path in sorted(out.iterdir()):
        if path.is_file():
            manifest.append({"path": path.name, "bytes": path.stat().st_size, "sha256": sha256_file(path)})
    (out / "MANIFEST.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    print(json.dumps(validation, ensure_ascii=False, sort_keys=True), flush=True)
    if failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
