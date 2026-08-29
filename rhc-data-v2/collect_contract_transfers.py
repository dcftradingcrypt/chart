#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
from pathlib import Path
from typing import Any

from rpc_fixed import RpcClient, fetch_logs_recursive, log_key
from topics import CONSECUTIVE_TRANSFER, TRANSFER, TRANSFER_BATCH, TRANSFER_SINGLE


def integer(value: Any, default: int = 0) -> int:
    try:
        text = str(value or "0")
        return int(text, 16) if text.startswith("0x") else int(float(text))
    except Exception:
        return default


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


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


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--contracts", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--shard", type=int, required=True)
    parser.add_argument("--shard-count", type=int, required=True)
    args = parser.parse_args()

    contracts = read_csv(Path(args.contracts))
    selected = [row for index, row in enumerate(contracts) if index % args.shard_count == args.shard]
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    rpc = RpcClient(min_interval=1.1, max_attempts=10)

    queries = [
        ("ERC721_TRANSFER", TRANSFER, 4),
        ("ERC2309_CONSECUTIVE_TRANSFER", CONSECUTIVE_TRANSFER, 4),
        ("ERC1155_TRANSFER_SINGLE", TRANSFER_SINGLE, 4),
        ("ERC1155_TRANSFER_BATCH", TRANSFER_BATCH, 4),
    ]
    all_logs: list[dict[str, Any]] = []
    all_coverage: list[dict[str, Any]] = []
    summaries: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []

    for contract_row in selected:
        contract = contract_row["contract"].lower()
        fixed_head = integer(contract_row.get("fixed_head_block"))
        first = integer(contract_row.get("first_observed_block"), 0)
        start = max(0, first - 10_000) if first else 0
        contract_logs: dict[tuple[str, str, int], dict[str, Any]] = {}
        query_counts: dict[str, int] = {}
        contract_failures: list[dict[str, Any]] = []
        for query_name, topic0, required_topics in queries:
            rows, coverage = fetch_logs_recursive(
                rpc,
                from_block=start,
                to_block=fixed_head,
                address=contract,
                topics=[topic0],
            )
            rows = [row for row in rows if len(row.get("topics") or []) == required_topics]
            query_counts[query_name] = len(rows)
            for part in coverage:
                record = {"contract": contract, "query": query_name, **part}
                all_coverage.append(record)
                if part.get("status") != "PASS":
                    contract_failures.append(record)
            for log in rows:
                key = log_key(log)
                contract_logs[key] = {
                    "contract": contract,
                    "query": query_name,
                    "block_number": integer(log.get("blockNumber")),
                    "block_hash": str(log.get("blockHash") or "").lower(),
                    "transaction_hash": str(log.get("transactionHash") or "").lower(),
                    "transaction_index": integer(log.get("transactionIndex")),
                    "log_index": integer(log.get("logIndex")),
                    "topics_json": json.dumps(log.get("topics") or [], separators=(",", ":")),
                    "data": str(log.get("data") or "0x").lower(),
                    "removed": bool(log.get("removed", False)),
                }
        rows = sorted(contract_logs.values(), key=lambda row: (row["block_number"], row["transaction_index"], row["log_index"]))
        all_logs.extend(rows)
        removed = [row for row in rows if row["removed"]]
        if contract_failures:
            failures.append({"contract": contract, "code": "COVERAGE_FAILURE", "count": len(contract_failures), "sample": contract_failures[:3]})
        if removed:
            failures.append({"contract": contract, "code": "REMOVED_CANONICAL_LOG", "count": len(removed)})
        if not rows:
            failures.append({"contract": contract, "code": "DISCOVERED_CONTRACT_WITHOUT_TRANSFER_HISTORY"})
        summaries.append({
            "contract": contract,
            "fixed_head_block": fixed_head,
            "query_start_block": start,
            "transfer_rows": len(rows),
            "first_transfer_block": min((row["block_number"] for row in rows), default=None),
            "last_transfer_block": max((row["block_number"] for row in rows), default=None),
            "erc721_transfer_rows": query_counts.get("ERC721_TRANSFER", 0),
            "erc2309_rows": query_counts.get("ERC2309_CONSECUTIVE_TRANSFER", 0),
            "erc1155_single_rows": query_counts.get("ERC1155_TRANSFER_SINGLE", 0),
            "erc1155_batch_rows": query_counts.get("ERC1155_TRANSFER_BATCH", 0),
            "coverage_failures": len(contract_failures),
            "status": "PASS" if rows and not contract_failures and not removed else "FAIL",
        })
        print(summaries[-1], flush=True)

    write_csv(out / "contract_summary.csv", summaries)
    write_csv(out / "transfer_logs.csv", all_logs)
    write_csv(out / "coverage.csv", all_coverage)
    write_csv(out / "errors.csv", failures)

    validation = {
        "status": "PASS" if not failures else "FAIL",
        "chain_id": 4663,
        "shard": args.shard,
        "shard_count": args.shard_count,
        "contracts": len(selected),
        "contracts_passed": sum(row["status"] == "PASS" for row in summaries),
        "contracts_failed": sum(row["status"] != "PASS" for row in summaries),
        "transfer_rows": len(all_logs),
        "failure_rows": len(failures),
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
