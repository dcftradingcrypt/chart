#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
from pathlib import Path
from typing import Any

from rpc_fixed import RpcClient, fetch_logs_recursive, log_key
from topics import (
    CONSECUTIVE_TRANSFER,
    SEADROP_ADDRESS,
    SEADROP_MINT,
    SEAPORT_ADDRESS,
    SEAPORT_ORDER_FULFILLED,
    TRANSFER,
    TRANSFER_BATCH,
    TRANSFER_SINGLE,
    ZERO_TOPIC,
)

TARGETS: dict[str, dict[str, Any]] = {
    "seadrop": {
        "address": SEADROP_ADDRESS,
        "topics": [SEADROP_MINT],
        "topic_count": 4,
    },
    "seaport": {
        "address": SEAPORT_ADDRESS,
        "topics": [SEAPORT_ORDER_FULFILLED],
        "topic_count": 4,
    },
    "erc721_mint": {
        "address": None,
        "topics": [TRANSFER, ZERO_TOPIC],
        "topic_count": 4,
    },
    "erc1155_single_mint": {
        "address": None,
        "topics": [TRANSFER_SINGLE, None, ZERO_TOPIC],
        "topic_count": 4,
    },
    "erc1155_batch_mint": {
        "address": None,
        "topics": [TRANSFER_BATCH, None, ZERO_TOPIC],
        "topic_count": 4,
    },
    "erc2309_mint": {
        "address": None,
        "topics": [CONSECUTIVE_TRANSFER, None, ZERO_TOPIC],
        "topic_count": 4,
    },
}


def integer(value: Any) -> int:
    if isinstance(value, int):
        return value
    text = str(value or "0")
    return int(text, 16) if text.startswith("0x") else int(float(text))


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", choices=sorted(TARGETS), required=True)
    parser.add_argument("--fixed-head", type=int, required=True)
    parser.add_argument("--fixed-head-hash", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--chunk", type=int, default=4_000_000)
    args = parser.parse_args()

    target = TARGETS[args.target]
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    rpc = RpcClient(min_interval=1.05, max_attempts=10)

    raw_rows: list[dict[str, Any]] = []
    coverage: list[dict[str, Any]] = []
    cursor = 0
    while cursor <= args.fixed_head:
        end = min(args.fixed_head, cursor + args.chunk - 1)
        rows, parts = fetch_logs_recursive(
            rpc,
            from_block=cursor,
            to_block=end,
            address=target["address"],
            topics=target["topics"],
        )
        rows = [row for row in rows if len(row.get("topics") or []) == target["topic_count"]]
        raw_rows.extend(rows)
        coverage.extend({"target": args.target, **part} for part in parts)
        print({"target": args.target, "from": cursor, "to": end, "rows": len(rows), "parts": len(parts)}, flush=True)
        cursor = end + 1

    unique: dict[tuple[str, str, int], dict[str, Any]] = {}
    for row in raw_rows:
        unique[log_key(row)] = row
    ordered = sorted(
        unique.values(),
        key=lambda row: (
            integer(row.get("blockNumber")),
            integer(row.get("transactionIndex")),
            integer(row.get("logIndex")),
        ),
    )

    raw_path = out / "logs.jsonl"
    with raw_path.open("w", encoding="utf-8", newline="\n") as handle:
        for row in ordered:
            handle.write(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n")

    index_rows = [
        {
            "target": args.target,
            "block_number": integer(row.get("blockNumber")),
            "block_hash": str(row.get("blockHash") or "").lower(),
            "transaction_hash": str(row.get("transactionHash") or "").lower(),
            "transaction_index": integer(row.get("transactionIndex")),
            "log_index": integer(row.get("logIndex")),
            "contract": str(row.get("address") or "").lower(),
            "topic_count": len(row.get("topics") or []),
            "topics_json": json.dumps(row.get("topics") or [], separators=(",", ":")),
            "data": str(row.get("data") or "0x").lower(),
            "removed": bool(row.get("removed", False)),
        }
        for row in ordered
    ]
    fields = list(index_rows[0]) if index_rows else ["target"]
    with (out / "events.csv").open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(index_rows)

    coverage_fields = ["target", "from_block", "to_block", "depth", "status", "rows", "error", "split_reason"]
    with (out / "coverage.csv").open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=coverage_fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(coverage)

    coverage_failures = [part for part in coverage if part.get("status") != "PASS"]
    removed_rows = [row for row in ordered if row.get("removed")]
    validation = {
        "status": "PASS" if not coverage_failures and not removed_rows else "FAIL",
        "chain_id": 4663,
        "target": args.target,
        "fixed_head_block": args.fixed_head,
        "fixed_head_hash": args.fixed_head_hash.lower(),
        "raw_rows": len(raw_rows),
        "event_rows": len(ordered),
        "duplicates_removed": len(raw_rows) - len(ordered),
        "coverage_parts": len(coverage),
        "coverage_failures": coverage_failures,
        "removed_rows": len(removed_rows),
        "rpc_stats": rpc.stats,
        "source": "CANONICAL_PUBLIC_RPC_ETH_GETLOGS",
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
