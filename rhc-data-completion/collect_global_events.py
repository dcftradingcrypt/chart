#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
from pathlib import Path
from typing import Any

from canonical_rpc import CanonicalRpc, fetch_logs_recursive, log_key

TARGETS = {
    "seadrop": {
        "address": "0x00005ea00ac477b1030ce78506496e8c2de24bf5",
        "topic0": "0xe90cf9cc0a552cf52ea6ff74ece0f1c8ae8cc9ad630d3181f55ac43ca076b7d6",
    },
    "seaport": {
        "address": "0x0000000000000068f116a894984e2db1123eb395",
        "topic0": "0x9d9af8e38d66c62e2c12f0225249fd9d721c54b83f48d9352c97c6cacdcb6f31",
    },
}


def integer(value: Any) -> int:
    if isinstance(value, int):
        return value
    return int(str(value), 16) if str(value).startswith("0x") else int(str(value))


def address_from_topic(topic: str) -> str:
    return "0x" + topic.lower()[-40:]


def data_words(data: str) -> list[str]:
    raw = data[2:] if data.startswith("0x") else data
    if len(raw) % 64:
        raise ValueError("ABI data length is not a word multiple")
    return [raw[i:i + 64] for i in range(0, len(raw), 64)]


def normalize(log: dict[str, Any], target: str) -> dict[str, Any]:
    topics = [str(x).lower() for x in (log.get("topics") or [])]
    row: dict[str, Any] = {
        "target": target,
        "block_number": integer(log["blockNumber"]),
        "block_hash": str(log.get("blockHash") or "").lower(),
        "transaction_hash": str(log.get("transactionHash") or "").lower(),
        "transaction_index": integer(log.get("transactionIndex") or "0x0"),
        "log_index": integer(log.get("logIndex") or "0x0"),
        "emitter": str(log.get("address") or "").lower(),
        "removed": bool(log.get("removed", False)),
        "topics_json": json.dumps(topics, separators=(",", ":")),
        "data": str(log.get("data") or "0x").lower(),
    }
    if target == "seadrop" and len(topics) == 4:
        words = data_words(row["data"])
        if len(words) >= 5:
            row.update({
                "nft_contract": address_from_topic(topics[1]),
                "minter": address_from_topic(topics[2]),
                "fee_recipient": address_from_topic(topics[3]),
                "payer": "0x" + words[0][-40:],
                "quantity_minted": int(words[1], 16),
                "unit_mint_price_wei": int(words[2], 16),
                "fee_bps": int(words[3], 16),
                "drop_stage_index": int(words[4], 16),
            })
    return row


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", choices=sorted(TARGETS), required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--chunk", type=int, default=2_000_000)
    parser.add_argument("--confirmations", type=int, default=100)
    args = parser.parse_args()

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    config = TARGETS[args.target]
    rpc = CanonicalRpc(min_interval=1.35)
    latest = rpc.block_number()
    fixed_head = max(0, latest - args.confirmations)
    fixed_block = rpc.block(fixed_head)

    all_logs: list[dict[str, Any]] = []
    coverage: list[dict[str, Any]] = []
    cursor = 0
    while cursor <= fixed_head:
        end = min(fixed_head, cursor + args.chunk - 1)
        rows, cov = fetch_logs_recursive(
            rpc,
            from_block=cursor,
            to_block=end,
            address=config["address"],
            topics=[config["topic0"]],
        )
        all_logs.extend(rows)
        coverage.extend(cov)
        print({"target": args.target, "from": cursor, "to": end, "rows": len(rows), "coverage_parts": len(cov)}, flush=True)
        cursor = end + 1

    dedup: dict[tuple[str, str, int], dict[str, Any]] = {}
    for log in all_logs:
        dedup[log_key(log)] = log
    ordered = sorted(dedup.values(), key=lambda x: (integer(x["blockNumber"]), integer(x.get("transactionIndex") or "0x0"), integer(x.get("logIndex") or "0x0")))
    normalized = [normalize(log, args.target) for log in ordered]

    raw_path = out / f"{args.target}_logs.jsonl"
    with raw_path.open("w", encoding="utf-8", newline="\n") as handle:
        for log in ordered:
            handle.write(json.dumps(log, sort_keys=True, separators=(",", ":")) + "\n")

    csv_path = out / f"{args.target}_events.csv"
    fields: list[str] = []
    seen: set[str] = set()
    for row in normalized:
        for key in row:
            if key not in seen:
                seen.add(key)
                fields.append(key)
    with csv_path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(normalized)

    coverage_path = out / "coverage.csv"
    coverage_fields = ["from_block", "to_block", "status", "rows", "depth", "error"]
    with coverage_path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=coverage_fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(coverage)

    failures = [row for row in coverage if row.get("status") != "PASS"]
    removed = [row for row in normalized if row.get("removed")]
    unique_keys = {(row["block_hash"], row["transaction_hash"], row["log_index"]) for row in normalized}
    validation = {
        "status": "PASS" if not failures and not removed and len(unique_keys) == len(normalized) else "FAIL",
        "target": args.target,
        "chain_id": 4663,
        "latest_observed_block": latest,
        "fixed_head_block": fixed_head,
        "fixed_head_hash": str(fixed_block.get("hash") or "").lower(),
        "confirmations_excluded": args.confirmations,
        "event_rows": len(normalized),
        "duplicate_rows_removed": len(all_logs) - len(ordered),
        "coverage_parts": len(coverage),
        "coverage_failures": failures,
        "removed_rows": len(removed),
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
