#!/usr/bin/env python3
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import random
import time
import urllib.error
import urllib.request
from collections import defaultdict
from pathlib import Path
from typing import Any

RPC = "https://rpc.mainnet.chain.robinhood.com"
BLOCKSCOUTS = [
    "https://robinhoodchain.blockscout.com/api/v2",
    "https://explorer.hoodmarketcap.com/api/v2",
]
UA = "RHC-Seaport-Tx-Enrichment/1.0 read-only"


def request_json(url: str, *, data: bytes | None = None, attempts: int = 12) -> Any:
    last: Exception | None = None
    for attempt in range(attempts):
        try:
            headers = {"accept": "application/json", "user-agent": UA}
            if data is not None:
                headers["content-type"] = "application/json"
            req = urllib.request.Request(url, data=data, headers=headers)
            with urllib.request.urlopen(req, timeout=90) as response:
                body = response.read()
            return json.loads(body.decode("utf-8"))
        except urllib.error.HTTPError as exc:
            last = exc
            if exc.code in {408, 425, 429, 500, 502, 503, 504}:
                time.sleep(min(120, 2 ** min(attempt, 7) + random.random() * 7))
                continue
            raise
        except Exception as exc:
            last = exc
            if attempt + 1 < attempts:
                time.sleep(min(60, 2 ** min(attempt, 6) + random.random() * 5))
                continue
    raise RuntimeError(f"request failed: {url}: {last!r}")


def rpc_batch(calls: list[tuple[str, list[Any]]]) -> dict[int, Any]:
    payload = [{"jsonrpc": "2.0", "id": index, "method": method, "params": params} for index, (method, params) in enumerate(calls)]
    result = request_json(RPC, data=json.dumps(payload).encode())
    if isinstance(result, dict):
        result = [result]
    if not isinstance(result, list):
        raise RuntimeError(f"invalid batch response: {type(result)!r}")
    output: dict[int, Any] = {}
    for row in result:
        if isinstance(row, dict):
            output[int(row.get("id", -1))] = {"__error__": row["error"]} if row.get("error") else row.get("result")
    return output


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


def fetch_internal(tx_hash: str) -> tuple[list[dict[str, Any]], str | None, list[str]]:
    errors: list[str] = []
    for base in BLOCKSCOUTS:
        try:
            payload = request_json(f"{base}/transactions/{tx_hash}/internal-transactions")
            items = payload.get("items") if isinstance(payload, dict) else payload
            if not isinstance(items, list):
                raise RuntimeError(f"invalid internal tx payload: {payload!r}")
            return [row for row in items if isinstance(row, dict)], base, errors
        except Exception as exc:
            errors.append(f"{base}:{exc!r}")
    return [], None, errors


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--index", required=True)
    parser.add_argument("--shard", type=int, required=True)
    parser.add_argument("--shard-count", type=int, default=64)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    by_tx: dict[str, list[dict[str, Any]]] = defaultdict(list)
    with gzip.open(args.index, "rt", encoding="utf-8") as file:
        for line in file:
            if not line.strip():
                continue
            row = json.loads(line)
            tx_hash = str(row.get("transactionHash") or row.get("transaction_hash") or "").lower()
            if tx_hash.startswith("0x") and len(tx_hash) == 66:
                by_tx[tx_hash].append(row)
    selected = sorted(tx_hash for tx_hash in by_tx if int(hashlib.sha256(tx_hash.encode()).hexdigest(), 16) % args.shard_count == args.shard)

    txs: dict[str, Any] = {}
    receipts: dict[str, Any] = {}
    errors: list[dict[str, Any]] = []
    for start in range(0, len(selected), 20):
        chunk = selected[start:start + 20]
        calls: list[tuple[str, list[Any]]] = []
        lookup: list[tuple[str, str]] = []
        for tx_hash in chunk:
            calls.append(("eth_getTransactionByHash", [tx_hash])); lookup.append((tx_hash, "tx"))
            calls.append(("eth_getTransactionReceipt", [tx_hash])); lookup.append((tx_hash, "receipt"))
        try:
            result = rpc_batch(calls)
        except Exception as exc:
            errors.append({"stage": "rpc_batch", "hashes": chunk, "error": repr(exc)})
            continue
        for index, (tx_hash, kind) in enumerate(lookup):
            value = result.get(index)
            if value is None or isinstance(value, dict) and value.get("__error__"):
                errors.append({"stage": kind, "transaction_hash": tx_hash, "error": value})
            else:
                (txs if kind == "tx" else receipts)[tx_hash] = value
        print({"shard": args.shard, "processed": min(start + len(chunk), len(selected)), "total": len(selected)}, flush=True)
        time.sleep(0.35)

    block_numbers = sorted({intish(receipt.get("blockNumber")) for receipt in receipts.values() if isinstance(receipt, dict)})
    blocks: dict[int, Any] = {}
    for start in range(0, len(block_numbers), 40):
        chunk = block_numbers[start:start + 40]
        try:
            result = rpc_batch([("eth_getBlockByNumber", [hex(number), False]) for number in chunk])
        except Exception as exc:
            errors.append({"stage": "block_batch", "blocks": chunk, "error": repr(exc)})
            continue
        for index, number in enumerate(chunk):
            value = result.get(index)
            if isinstance(value, dict):
                blocks[number] = value
            else:
                errors.append({"stage": "block", "block_number": number, "error": value})

    rows: list[dict[str, Any]] = []
    for position, tx_hash in enumerate(selected, 1):
        tx = txs.get(tx_hash)
        receipt = receipts.get(tx_hash)
        if not isinstance(tx, dict) or not isinstance(receipt, dict):
            continue
        block_number = intish(receipt.get("blockNumber"))
        block = blocks.get(block_number) or {}
        internal, internal_source, internal_errors = fetch_internal(tx_hash)
        if internal_source is None:
            errors.append({"stage": "internal_transactions", "transaction_hash": tx_hash, "error": internal_errors})
        gas_used = intish(receipt.get("gasUsed"))
        gas_price = intish(receipt.get("effectiveGasPrice"), intish(tx.get("gasPrice")))
        rows.append({
            "transaction_hash": tx_hash,
            "block_number": block_number,
            "block_hash": str(receipt.get("blockHash") or "").lower(),
            "timestamp_unix": intish(block.get("timestamp")),
            "tx_from": str(tx.get("from") or "").lower(),
            "tx_to": str(tx.get("to") or "").lower(),
            "tx_value_wei": str(intish(tx.get("value"))),
            "input": tx.get("input"),
            "method_selector": str(tx.get("input") or "0x")[:10].lower(),
            "receipt_status": intish(receipt.get("status")),
            "gas_used": gas_used,
            "effective_gas_price_wei": gas_price,
            "gas_cost_wei": str(gas_used * gas_price),
            "order_fulfilled_event_count": len(by_tx[tx_hash]),
            "order_fulfilled_logs": by_tx[tx_hash],
            "receipt_logs": receipt.get("logs") or [],
            "internal_transactions": internal,
            "internal_source": internal_source,
            "internal_source_errors": internal_errors,
        })
        if position % 25 == 0:
            print({"shard": args.shard, "internal_processed": position, "total": len(selected)}, flush=True)
        time.sleep(0.25)

    with gzip.open(out / "seaport_transactions.jsonl.gz", "wt", encoding="utf-8") as file:
        for row in rows:
            file.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
    (out / "errors.json").write_text(json.dumps(errors, ensure_ascii=False, indent=2), encoding="utf-8")
    enriched = {row["transaction_hash"] for row in rows}
    failures: list[dict[str, Any]] = []
    missing = sorted(set(selected) - enriched)
    if missing:
        failures.append({"code": "SALE_TRANSACTION_ENRICHMENT_MISSING", "count": len(missing), "sample": missing[:50]})
    if errors:
        failures.append({"code": "ENRICHMENT_ERRORS_PRESENT", "count": len(errors)})
    if any(row["receipt_status"] != 1 for row in rows):
        failures.append({"code": "NON_SUCCESS_RECEIPT_IN_ORDERFULFILLED_INDEX"})
    report = {
        "status": "PASS" if not failures else "FAIL",
        "shard": args.shard,
        "shard_count": args.shard_count,
        "index_unique_transactions": len(by_tx),
        "selected_transactions": len(selected),
        "enriched_transactions": len(rows),
        "failures": failures,
        "production_approved_wallets": 0,
        "decision_use": "SECONDARY_SALE_ORDER_AND_CASHFLOW_INPUT",
    }
    (out / "VALIDATION.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    manifest = []
    for path in sorted(out.iterdir()):
        if path.is_file():
            manifest.append({"path": path.name, "bytes": path.stat().st_size, "sha256": hashlib.sha256(path.read_bytes()).hexdigest()})
    (out / "MANIFEST.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, sort_keys=True), flush=True)
    if failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
