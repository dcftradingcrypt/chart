#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
from pathlib import Path
from typing import Any

from rpc_fixed import RpcClient


def integer(value: Any, default: int = 0) -> int:
    try:
        text = str(value or "0")
        return int(text, 16) if text.startswith("0x") else int(float(text))
    except Exception:
        return default


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n")


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
        writer.writerows(rows)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def shard_for(tx_hash: str, count: int) -> int:
    return int(tx_hash[2:18], 16) % count


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-root", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--shard", type=int, required=True)
    parser.add_argument("--shard-count", type=int, required=True)
    args = parser.parse_args()

    root = Path(args.input_root)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    tx_hashes: set[str] = set()
    for path in root.rglob("*.csv"):
        if path.name not in {"events.csv", "transfer_logs.csv"}:
            continue
        for row in read_csv(path):
            tx_hash = str(row.get("transaction_hash") or "").lower()
            if tx_hash.startswith("0x") and len(tx_hash) == 66:
                tx_hashes.add(tx_hash)
    selected = sorted(value for value in tx_hashes if shard_for(value, args.shard_count) == args.shard)

    rpc = RpcClient(min_interval=0.95, max_attempts=10)
    calls: list[tuple[str, list[Any], str]] = []
    for tx_hash in selected:
        calls.append(("eth_getTransactionByHash", [tx_hash], f"tx:{tx_hash}"))
        calls.append(("eth_getTransactionReceipt", [tx_hash], f"receipt:{tx_hash}"))
    results, rpc_failures = rpc.batch(calls, batch_size=20)

    transactions: list[dict[str, Any]] = []
    receipts: list[dict[str, Any]] = []
    summary: list[dict[str, Any]] = []
    block_numbers: set[int] = set()
    missing: list[dict[str, Any]] = []
    for tx_hash in selected:
        tx = results.get(f"tx:{tx_hash}")
        receipt = results.get(f"receipt:{tx_hash}")
        if isinstance(tx, dict):
            transactions.append(tx)
        if isinstance(receipt, dict):
            receipts.append(receipt)
            block_number = integer(receipt.get("blockNumber"), -1)
            if block_number >= 0:
                block_numbers.add(block_number)
        if not isinstance(tx, dict) or not isinstance(receipt, dict):
            missing.append({"transaction_hash": tx_hash, "transaction_present": isinstance(tx, dict), "receipt_present": isinstance(receipt, dict)})
        summary.append({
            "transaction_hash": tx_hash,
            "transaction_present": isinstance(tx, dict),
            "receipt_present": isinstance(receipt, dict),
            "receipt_status": integer(receipt.get("status"), -1) if isinstance(receipt, dict) else None,
            "block_number": integer(receipt.get("blockNumber"), -1) if isinstance(receipt, dict) else None,
            "block_hash": str(receipt.get("blockHash") or "").lower() if isinstance(receipt, dict) else None,
            "tx_from": str(tx.get("from") or "").lower() if isinstance(tx, dict) else None,
            "tx_to": str(tx.get("to") or "").lower() if isinstance(tx, dict) else None,
            "tx_value_wei": integer(tx.get("value")) if isinstance(tx, dict) else None,
            "gas": integer(tx.get("gas")) if isinstance(tx, dict) else None,
            "gas_price": integer(tx.get("gasPrice")) if isinstance(tx, dict) else None,
            "gas_used": integer(receipt.get("gasUsed")) if isinstance(receipt, dict) else None,
            "effective_gas_price": integer(receipt.get("effectiveGasPrice")) if isinstance(receipt, dict) else None,
            "contract_address_created": str(receipt.get("contractAddress") or "").lower() if isinstance(receipt, dict) else None,
            "receipt_log_count": len(receipt.get("logs") or []) if isinstance(receipt, dict) else 0,
        })

    block_calls = [("eth_getBlockByNumber", [hex(number), False], f"block:{number}") for number in sorted(block_numbers)]
    block_results, block_failures = rpc.batch(block_calls, batch_size=25)
    blocks: list[dict[str, Any]] = []
    missing_blocks: list[int] = []
    for number in sorted(block_numbers):
        block = block_results.get(f"block:{number}")
        if isinstance(block, dict):
            blocks.append(block)
        else:
            missing_blocks.append(number)

    write_jsonl(out / "transactions.jsonl", transactions)
    write_jsonl(out / "receipts.jsonl", receipts)
    write_jsonl(out / "blocks.jsonl", blocks)
    write_csv(out / "transaction_summary.csv", summary)
    write_csv(out / "missing_transactions.csv", missing)
    write_csv(out / "rpc_failures.csv", rpc_failures + block_failures)

    failures = []
    if missing:
        failures.append({"code": "MISSING_TRANSACTION_OR_RECEIPT", "count": len(missing), "sample": missing[:10]})
    if missing_blocks:
        failures.append({"code": "MISSING_BLOCK_HEADERS", "count": len(missing_blocks), "sample": missing_blocks[:20]})
    if rpc_failures or block_failures:
        failures.append({"code": "RPC_BATCH_FAILURES", "count": len(rpc_failures) + len(block_failures)})
    validation = {
        "status": "PASS" if not failures else "FAIL",
        "chain_id": 4663,
        "shard": args.shard,
        "shard_count": args.shard_count,
        "global_unique_transaction_hashes": len(tx_hashes),
        "selected_transaction_hashes": len(selected),
        "transactions": len(transactions),
        "receipts": len(receipts),
        "blocks": len(blocks),
        "missing_transactions_or_receipts": len(missing),
        "missing_blocks": len(missing_blocks),
        "failures": failures,
        "rpc_stats": rpc.stats,
    }
    (out / "VALIDATION.json").write_text(json.dumps(validation, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    manifest = []
    for path in sorted(out.iterdir()):
        if path.is_file():
            manifest.append({"path": path.name, "bytes": path.stat().st_size, "sha256": sha256(path)})
    (out / "MANIFEST.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(validation, sort_keys=True), flush=True)
    if validation["status"] != "PASS":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
