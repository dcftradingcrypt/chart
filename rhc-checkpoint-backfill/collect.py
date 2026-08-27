#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import random
import time
import urllib.error
import urllib.request
from collections import deque
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
RPCS = [
    "https://rpc.mainnet.chain.robinhood.com",
    "https://robinhood-rpc.publicnode.com",
    "https://rpc-robinhood.hoodmarket.io",
]
UA = "DCF-RHC-Checkpoint-History/1.0 (read-only)"


def integer(value: Any, default: int | None = None) -> int | None:
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


def rpc_request(url: str, method: str, params: list[Any], attempts: int = 12) -> Any:
    payload = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode("utf-8")
    last: Exception | None = None
    for attempt in range(attempts):
        try:
            request = urllib.request.Request(
                url,
                data=payload,
                method="POST",
                headers={"content-type": "application/json", "accept": "application/json", "user-agent": UA},
            )
            with urllib.request.urlopen(request, timeout=90) as response:
                data = json.loads(response.read().decode("utf-8"))
            if not isinstance(data, dict):
                raise RuntimeError(f"non-object response: {type(data)}")
            if data.get("error") is not None:
                return {"__rpc_error__": data["error"]}
            return data.get("result")
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
    raise RuntimeError(f"RPC failed at {url}: {last!r}")


def rpc_any(method: str, params: list[Any]) -> tuple[Any, str]:
    errors = []
    for url in RPCS:
        try:
            result = rpc_request(url, method, params)
            return result, url
        except Exception as exc:
            errors.append(f"{url}:{exc!r}")
    raise RuntimeError(" | ".join(errors))


def latest_block() -> tuple[int, str]:
    result, source = rpc_any("eth_blockNumber", [])
    if not isinstance(result, str):
        raise RuntimeError(f"invalid head result: {result!r}")
    return int(result, 16), source


def normalize(row: dict[str, Any], target: str) -> dict[str, Any]:
    topics = [str(value).lower() for value in (row.get("topics") or [])]
    return {
        "chain_id": CHAIN_ID,
        "target": target,
        "address": str(row.get("address") or TARGETS[target]["address"]).lower(),
        "block_number": integer(row.get("blockNumber")),
        "block_hash": str(row.get("blockHash") or "").lower(),
        "transaction_hash": str(row.get("transactionHash") or "").lower(),
        "transaction_index": integer(row.get("transactionIndex")),
        "log_index": integer(row.get("logIndex")),
        "removed": bool(row.get("removed", False)),
        "data": str(row.get("data") or "0x").lower(),
        "topics": topics,
        "topic0": topics[0] if topics else None,
        "raw": row,
    }


def query_range(target: str, start: int, stop: int) -> tuple[str, list[dict[str, Any]], str]:
    config = TARGETS[target]
    errors = []
    for url in RPCS:
        try:
            result = rpc_request(
                url,
                "eth_getLogs",
                [{
                    "fromBlock": hex(start),
                    "toBlock": hex(stop),
                    "address": config["address"],
                    "topics": [config["topic0"]],
                }],
            )
            if isinstance(result, list):
                return "ACCEPT", [normalize(row, target) for row in result if isinstance(row, dict)], url
            if isinstance(result, dict) and result.get("__rpc_error__") is not None:
                text = json.dumps(result["__rpc_error__"], sort_keys=True).lower()
                if any(token in text for token in ("range", "too many", "response", "limit", "timeout", "size", "query returned", "block range")):
                    return "SPLIT", [], url + ":" + text[:500]
                errors.append(url + ":" + text[:500])
                continue
            errors.append(url + ":unexpected:" + repr(result)[:300])
        except Exception as exc:
            errors.append(url + ":" + repr(exc))
    return "RETRY_OR_SPLIT", [], " | ".join(errors)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fields = sorted({key for row in rows for key in row}) if rows else ["empty"]
    with path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def shard_path(shards: Path, start: int, stop: int) -> Path:
    return shards / f"{start:012d}-{stop:012d}.jsonl"


def write_shard(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        for row in sorted(rows, key=lambda r: (r["block_number"] or -1, r["transaction_index"] or -1, r["log_index"] or -1)):
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")


def collect(target: str, out: Path, workers: int, initial_window: int) -> None:
    out.mkdir(parents=True, exist_ok=True)
    shards = out / "shards"
    shards.mkdir(exist_ok=True)
    head, head_rpc = latest_block()
    pending = deque(
        (start, min(start + initial_window - 1, head), 0)
        for start in range(0, head + 1, initial_window)
    )
    range_rows: list[dict[str, Any]] = []
    unresolved: list[dict[str, Any]] = []
    completed_ranges = 0
    accepted_event_rows = 0

    checkpoint = out / "checkpoint.json"
    while pending:
        batch = [pending.popleft() for _ in range(min(len(pending), workers * 2))]
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(query_range, target, start, stop): (start, stop, depth) for start, stop, depth in batch}
            for future in as_completed(futures):
                start, stop, depth = futures[future]
                try:
                    state, rows, note = future.result()
                except Exception as exc:
                    state, rows, note = "RETRY_OR_SPLIT", [], repr(exc)
                if state == "ACCEPT":
                    path = shard_path(shards, start, stop)
                    write_shard(path, rows)
                    completed_ranges += 1
                    accepted_event_rows += len(rows)
                    range_rows.append({"from_block": start, "to_block": stop, "depth": depth, "status": "ACCEPTED", "row_count": len(rows), "source": note, "shard": path.name})
                elif start < stop:
                    middle = (start + stop) // 2
                    pending.append((start, middle, depth + 1))
                    pending.append((middle + 1, stop, depth + 1))
                    range_rows.append({"from_block": start, "to_block": stop, "depth": depth, "status": "SPLIT", "row_count": 0, "source": note, "shard": ""})
                else:
                    item = {"from_block": start, "to_block": stop, "depth": depth, "status": "UNRESOLVED_SINGLE_BLOCK", "row_count": 0, "source": note, "shard": ""}
                    range_rows.append(item)
                    unresolved.append(item)
        pending = deque(sorted(pending, key=lambda item: (item[0], item[1], item[2])))
        write_csv(out / "ranges.csv", range_rows)
        write_csv(out / "errors.csv", unresolved)
        checkpoint.write_text(
            json.dumps({
                "target": target,
                "fixed_head_block": head,
                "head_rpc": head_rpc,
                "pending_ranges": len(pending),
                "accepted_ranges": completed_ranges,
                "accepted_event_rows_before_dedup": accepted_event_rows,
                "unresolved_single_blocks": len(unresolved),
            }, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        print(json.dumps({"target": target, "pending": len(pending), "accepted_ranges": completed_ranges, "rows": accepted_event_rows, "unresolved": len(unresolved)}), flush=True)

    unique: dict[tuple[str, int], dict[str, Any]] = {}
    for path in sorted(shards.glob("*.jsonl")):
        with path.open(encoding="utf-8") as handle:
            for line in handle:
                row = json.loads(line)
                unique[(row["transaction_hash"], row["log_index"] if row["log_index"] is not None else -1)] = row
    logs = sorted(unique.values(), key=lambda row: (row["block_number"] or -1, row["transaction_index"] or -1, row["log_index"] or -1))
    with (out / "logs.jsonl").open("w", encoding="utf-8", newline="\n") as handle:
        for row in logs:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")

    wrong_topic = sum(row["topic0"] != TARGETS[target]["topic0"] for row in logs)
    wrong_address = sum(row["address"] != TARGETS[target]["address"] for row in logs)
    removed_rows = sum(row["removed"] for row in logs)
    validation = {
        "status": "PASS" if not unresolved and not wrong_topic and not wrong_address and not removed_rows else "FAIL",
        "chain_id": CHAIN_ID,
        "target": target,
        "fixed_head_block": head,
        "head_rpc": head_rpc,
        "event_rows": len(logs),
        "accepted_event_rows_before_dedup": accepted_event_rows,
        "duplicates_removed": accepted_event_rows - len(logs),
        "accepted_ranges": completed_ranges,
        "unresolved_single_blocks": len(unresolved),
        "wrong_topic_rows": wrong_topic,
        "wrong_address_rows": wrong_address,
        "removed_rows": removed_rows,
    }
    (out / "validation.json").write_text(json.dumps(validation, indent=2, sort_keys=True), encoding="utf-8")
    manifest = []
    for path in sorted(out.rglob("*")):
        if path.is_file() and path.name != "manifest.json":
            manifest.append({"path": str(path.relative_to(out)), "bytes": path.stat().st_size, "sha256": sha256(path)})
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(validation, sort_keys=True), flush=True)
    if validation["status"] != "PASS":
        raise SystemExit(2)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", choices=TARGETS, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--workers", type=int, default=6)
    parser.add_argument("--initial-window", type=int, default=500_000)
    args = parser.parse_args()
    collect(args.target, args.out, max(1, args.workers), max(1, args.initial_window))


if __name__ == "__main__":
    main()
