#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

from canonical_rpc import CanonicalRpc, fetch_logs_recursive, log_key

TRANSFER = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
EXTERNAL_ONLY = {
    "0x2b5b35ac5a2d5c1224337ba86bf3816abee69da3",
    "0x4bc98b9112229ee07d85a6827d3bde713c8e7e24",
    "0xfaeb5d192a7336a6e635905d8d33a46adbba8513",
    "0xfe80a4f2d6456327663c6b76e167e598e1142364",
}


def keccak_topic(signature: str) -> str:
    try:
        from Crypto.Hash import keccak  # type: ignore
    except ImportError:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "--disable-pip-version-check", "pycryptodome==3.23.0"])
        from Crypto.Hash import keccak  # type: ignore
    digest = keccak.new(digest_bits=256)
    digest.update(signature.encode("ascii"))
    return "0x" + digest.hexdigest()


TRANSFER_SINGLE = keccak_topic("TransferSingle(address,address,address,uint256,uint256)")
TRANSFER_BATCH = keccak_topic("TransferBatch(address,address,address,uint256[],uint256[])")


def padded(address: str) -> str:
    return "0x" + "0" * 24 + address.lower()[2:]


def integer(value: Any) -> int:
    if isinstance(value, int):
        return value
    text = str(value or "0")
    return int(text, 16) if text.startswith("0x") else int(float(text))


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--wallets", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--shard", type=int, required=True)
    parser.add_argument("--shard-count", type=int, required=True)
    parser.add_argument("--fixed-head", type=int, required=True)
    args = parser.parse_args()

    all_wallets = json.loads(Path(args.wallets).read_text(encoding="utf-8"))
    selected = [row for index, row in enumerate(all_wallets) if index % args.shard_count == args.shard]
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    rpc = CanonicalRpc(min_interval=1.45)

    event_queries = [
        ("ERC721_IN", TRANSFER, 2, 4),
        ("ERC721_OUT", TRANSFER, 1, 4),
        ("ERC1155_SINGLE_IN", TRANSFER_SINGLE, 3, None),
        ("ERC1155_SINGLE_OUT", TRANSFER_SINGLE, 2, None),
        ("ERC1155_BATCH_IN", TRANSFER_BATCH, 3, None),
        ("ERC1155_BATCH_OUT", TRANSFER_BATCH, 2, None),
    ]

    summaries: list[dict[str, Any]] = []
    logs_out: list[dict[str, Any]] = []
    coverage_out: list[dict[str, Any]] = []
    known_tx_out: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []

    for wallet_row in selected:
        wallet = wallet_row["wallet"].lower()
        wallet_logs: dict[tuple[str, str, int], dict[str, Any]] = {}
        wallet_cov: list[dict[str, Any]] = []
        query_counts: dict[str, int] = {}
        for query_name, topic0, topic_index, required_topic_count in event_queries:
            topics: list[Any] = [topic0]
            while len(topics) <= topic_index:
                topics.append(None)
            topics[topic_index] = padded(wallet)
            rows, coverage = fetch_logs_recursive(
                rpc,
                from_block=0,
                to_block=args.fixed_head,
                topics=topics,
            )
            if required_topic_count is not None:
                rows = [row for row in rows if len(row.get("topics") or []) == required_topic_count]
            query_counts[query_name] = len(rows)
            for coverage_row in coverage:
                record = {"wallet": wallet, "query": query_name, **coverage_row}
                coverage_out.append(record)
                wallet_cov.append(record)
            for log in rows:
                key = log_key(log)
                record = {
                    "wallet": wallet,
                    "priority": wallet_row.get("priority"),
                    "query": query_name,
                    "block_number": integer(log.get("blockNumber")),
                    "block_hash": str(log.get("blockHash") or "").lower(),
                    "transaction_hash": str(log.get("transactionHash") or "").lower(),
                    "transaction_index": integer(log.get("transactionIndex")),
                    "log_index": integer(log.get("logIndex")),
                    "contract": str(log.get("address") or "").lower(),
                    "topics_json": json.dumps(log.get("topics") or [], separators=(",", ":")),
                    "data": str(log.get("data") or "0x").lower(),
                    "removed": bool(log.get("removed", False)),
                }
                wallet_logs[key] = record

        for kind in ("known_mints", "known_sales"):
            for evidence in wallet_row.get(kind) or []:
                tx_hash = str(evidence.get("tx_hash") or "").lower()
                if not tx_hash:
                    continue
                try:
                    tx = rpc.call("eth_getTransactionByHash", [tx_hash])
                    receipt = rpc.call("eth_getTransactionReceipt", [tx_hash])
                    known_tx_out.append({
                        "wallet": wallet,
                        "priority": wallet_row.get("priority"),
                        "evidence_kind": kind,
                        "transaction_hash": tx_hash,
                        "transaction_present": isinstance(tx, dict),
                        "receipt_present": isinstance(receipt, dict),
                        "receipt_status": integer(receipt.get("status")) if isinstance(receipt, dict) and receipt.get("status") is not None else None,
                        "block_number": integer(receipt.get("blockNumber")) if isinstance(receipt, dict) and receipt.get("blockNumber") is not None else None,
                        "block_hash": str(receipt.get("blockHash") or "").lower() if isinstance(receipt, dict) else None,
                        "tx_from": str(tx.get("from") or "").lower() if isinstance(tx, dict) else None,
                        "tx_to": str(tx.get("to") or "").lower() if isinstance(tx, dict) else None,
                        "tx_value_wei": integer(tx.get("value")) if isinstance(tx, dict) and tx.get("value") is not None else None,
                        "receipt_log_count": len(receipt.get("logs") or []) if isinstance(receipt, dict) else 0,
                        "source_evidence_json": json.dumps(evidence, sort_keys=True, separators=(",", ":")),
                        "tx_json": json.dumps(tx, sort_keys=True, separators=(",", ":")) if isinstance(tx, dict) else None,
                        "receipt_json": json.dumps(receipt, sort_keys=True, separators=(",", ":")) if isinstance(receipt, dict) else None,
                    })
                except Exception as exc:
                    failures.append({"wallet": wallet, "stage": "known_tx", "transaction_hash": tx_hash, "error": repr(exc)})

        wallet_rows = sorted(wallet_logs.values(), key=lambda row: (row["block_number"], row["transaction_index"], row["log_index"]))
        logs_out.extend(wallet_rows)
        coverage_failures = [row for row in wallet_cov if row.get("status") != "PASS"]
        removed_rows = [row for row in wallet_rows if row["removed"]]
        expected_activity = wallet not in EXTERNAL_ONLY
        if coverage_failures:
            failures.append({"wallet": wallet, "stage": "coverage", "failure_count": len(coverage_failures), "sample": coverage_failures[:3]})
        if removed_rows:
            failures.append({"wallet": wallet, "stage": "removed_logs", "count": len(removed_rows)})
        if expected_activity and not wallet_rows:
            failures.append({"wallet": wallet, "stage": "expected_activity_missing", "priority": wallet_row.get("priority")})
        summaries.append({
            "wallet": wallet,
            "priority": wallet_row.get("priority"),
            "expected_onchain_activity": expected_activity,
            "canonical_nft_transfer_rows": len(wallet_rows),
            "unique_contracts": len({row["contract"] for row in wallet_rows}),
            "first_block": min((row["block_number"] for row in wallet_rows), default=None),
            "last_block": max((row["block_number"] for row in wallet_rows), default=None),
            "query_counts_json": json.dumps(query_counts, sort_keys=True, separators=(",", ":")),
            "coverage_failures": len(coverage_failures),
            "status": "PASS" if not coverage_failures and not removed_rows and (wallet_rows or not expected_activity) else "FAIL",
        })
        print(summaries[-1], flush=True)

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

    write_csv(out / "wallet_summary.csv", summaries)
    write_csv(out / "canonical_nft_transfers.csv", logs_out)
    write_csv(out / "coverage.csv", coverage_out)
    write_csv(out / "known_transaction_evidence.csv", known_tx_out)
    write_csv(out / "errors.csv", failures)

    validation = {
        "status": "PASS" if not failures else "FAIL",
        "chain_id": 4663,
        "fixed_head_block": args.fixed_head,
        "shard": args.shard,
        "shard_count": args.shard_count,
        "wallet_rows": len(selected),
        "wallets_passed": sum(row["status"] == "PASS" for row in summaries),
        "wallets_failed": sum(row["status"] != "PASS" for row in summaries),
        "canonical_nft_transfer_rows": len(logs_out),
        "known_transaction_evidence_rows": len(known_tx_out),
        "failure_rows": len(failures),
        "failures": failures,
        "rpc_stats": rpc.stats,
        "production_approved_wallets": 0,
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
