#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import random
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

CHAIN_ID = 4663
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
EXPLORERS = [
    "https://robinhoodchain.blockscout.com/api",
    "https://explorer.hoodmarketcap.com/api",
]
UA = "RHC-Selection-Alpha-Shard/1.0 (read-only)"


def fetch_json(url: str, attempts: int = 12, timeout: int = 75) -> Any:
    last: Exception | None = None
    for attempt in range(attempts):
        try:
            req = urllib.request.Request(url, headers={"accept": "application/json", "user-agent": UA})
            with urllib.request.urlopen(req, timeout=timeout) as response:
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            last = exc
            if exc.code == 429 or exc.code >= 500:
                time.sleep(min(60, 2 ** min(attempt, 5) + random.random() * 4))
                continue
            raise
        except Exception as exc:
            last = exc
            if attempt + 1 < attempts:
                time.sleep(min(30, 2 ** min(attempt, 4) + random.random() * 3))
                continue
    raise RuntimeError(f"GET failed: {url}: {last!r}")


def integer(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value, 16) if value.startswith("0x") else int(value)
        except ValueError:
            return None
    return None


def query(target: str, start: int, stop: int) -> tuple[str, list[dict[str, Any]], str]:
    cfg = TARGETS[target]
    params = {
        "module": "logs",
        "action": "getLogs",
        "fromBlock": start,
        "toBlock": stop,
        "address": cfg["address"],
        "topic0": cfg["topic0"],
    }
    errors: list[str] = []
    for base in EXPLORERS:
        url = base + "?" + urllib.parse.urlencode(params)
        try:
            data = fetch_json(url)
        except Exception as exc:
            errors.append(f"{base}:{exc!r}")
            continue
        if not isinstance(data, dict):
            errors.append(f"{base}:non-object")
            continue
        result = data.get("result")
        message = str(data.get("message") or "")
        if isinstance(result, list):
            rows = [row for row in result if isinstance(row, dict)]
            if len(rows) >= 1000:
                return "split", rows, f"{base}:cap:{len(rows)}"
            return "ok", rows, f"{base}:{message}:{len(rows)}"
        text = (message + " " + str(result)).lower()
        if any(token in text for token in ("no logs", "no records", "not found")):
            return "ok", [], f"{base}:{text[:300]}"
        if any(token in text for token in ("1000", "too many", "timeout", "range", "response size", "query returned")):
            return "split", [], f"{base}:{text[:300]}"
        errors.append(f"{base}:{text[:300]}")
    return "retry_split", [], " | ".join(errors)


def normalize(row: dict[str, Any], target: str) -> dict[str, Any]:
    topics = row.get("topics") or []
    if isinstance(topics, str):
        try:
            topics = json.loads(topics)
        except Exception:
            topics = [topics]
    topics = [str(value).lower() for value in topics]
    return {
        "chain_id": CHAIN_ID,
        "target": target,
        "address": str(row.get("address") or TARGETS[target]["address"]).lower(),
        "block_number": integer(row.get("blockNumber") or row.get("block_number")),
        "block_hash": str(row.get("blockHash") or row.get("block_hash") or "").lower(),
        "transaction_hash": str(row.get("transactionHash") or row.get("transaction_hash") or "").lower(),
        "transaction_index": integer(row.get("transactionIndex") or row.get("transaction_index")),
        "log_index": integer(row.get("logIndex") or row.get("log_index")),
        "data": str(row.get("data") or "0x").lower(),
        "topics": topics,
        "topic0": topics[0] if topics else None,
        "raw": row,
    }


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def collect(target: str, segment_index: int, segment_count: int, head: int, out: Path, workers: int) -> None:
    start = (head + 1) * segment_index // segment_count
    stop = (head + 1) * (segment_index + 1) // segment_count - 1
    out.mkdir(parents=True, exist_ok=True)
    pending: list[tuple[int, int, int]] = [(start, stop, 0)]
    accepted: list[dict[str, Any]] = []
    ranges: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []

    while pending:
        batch = pending[: workers * 2]
        pending = pending[workers * 2 :]
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(query, target, a, b): (a, b, depth) for a, b, depth in batch}
            for future in as_completed(futures):
                a, b, depth = futures[future]
                try:
                    state, rows, note = future.result()
                except Exception as exc:
                    state, rows, note = "retry_split", [], repr(exc)
                if state == "ok":
                    accepted.extend(normalize(row, target) for row in rows)
                    ranges.append({"from_block": a, "to_block": b, "depth": depth, "status": "ACCEPTED", "row_count": len(rows), "note": note})
                elif a < b:
                    mid = (a + b) // 2
                    pending.extend([(a, mid, depth + 1), (mid + 1, b, depth + 1)])
                    ranges.append({"from_block": a, "to_block": b, "depth": depth, "status": "SPLIT", "row_count": len(rows), "note": note})
                else:
                    item = {"from_block": a, "to_block": b, "depth": depth, "status": "UNRESOLVED_SINGLE_BLOCK", "row_count": len(rows), "note": note}
                    ranges.append(item)
                    errors.append(item)
        pending.sort(key=lambda item: (item[0], item[1], item[2]))

    unique: dict[tuple[str, int], dict[str, Any]] = {}
    for row in accepted:
        key = (row["transaction_hash"], row["log_index"] if row["log_index"] is not None else -1)
        unique[key] = row
    logs = sorted(unique.values(), key=lambda row: (row["block_number"] or -1, row["transaction_index"] or -1, row["log_index"] or -1))
    with (out / "logs.jsonl").open("w", encoding="utf-8") as handle:
        for row in logs:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
    fields = ["chain_id", "target", "address", "block_number", "block_hash", "transaction_hash", "transaction_index", "log_index", "data", "topics", "topic0"]
    with (out / "logs.csv").open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in logs:
            writer.writerow({field: json.dumps(row[field], ensure_ascii=False) if isinstance(row[field], list) else row[field] for field in fields})
    for filename, data in (("ranges.csv", ranges), ("errors.csv", errors)):
        with (out / filename).open("w", newline="", encoding="utf-8-sig") as handle:
            writer = csv.DictWriter(handle, fieldnames=["from_block", "to_block", "depth", "status", "row_count", "note"])
            writer.writeheader()
            writer.writerows(sorted(data, key=lambda item: (item["from_block"], item["to_block"], item["depth"])))
    wrong_topic = sum(row["topic0"] != TARGETS[target]["topic0"] for row in logs)
    outside = sum(row["block_number"] is None or not start <= row["block_number"] <= stop for row in logs)
    validation = {
        "status": "PASS" if not errors and not wrong_topic and not outside else "FAIL",
        "chain_id": CHAIN_ID,
        "target": target,
        "segment_index": segment_index,
        "segment_count": segment_count,
        "head_block": head,
        "from_block": start,
        "to_block": stop,
        "row_count": len(logs),
        "duplicates_removed": len(accepted) - len(logs),
        "range_attempts": len(ranges),
        "unresolved_single_blocks": len(errors),
        "wrong_topic_rows": wrong_topic,
        "out_of_segment_rows": outside,
    }
    (out / "validation.json").write_text(json.dumps(validation, indent=2, sort_keys=True), encoding="utf-8")
    manifest = []
    for path in sorted(out.iterdir()):
        if path.is_file() and path.name != "manifest.json":
            manifest.append({"path": path.name, "bytes": path.stat().st_size, "sha256": sha256(path)})
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(validation, sort_keys=True), flush=True)
    if validation["status"] != "PASS":
        raise SystemExit(2)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", choices=sorted(TARGETS), required=True)
    parser.add_argument("--segment-index", type=int, required=True)
    parser.add_argument("--segment-count", type=int, required=True)
    parser.add_argument("--head", type=int, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--workers", type=int, default=4)
    args = parser.parse_args()
    collect(args.target, args.segment_index, args.segment_count, args.head, args.out, max(1, args.workers))


if __name__ == "__main__":
    main()
