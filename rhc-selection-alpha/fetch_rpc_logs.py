#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import queue
import random
import threading
import time
import urllib.error
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
RPCS = [
    "https://rpc.mainnet.chain.robinhood.com",
    "https://robinhood-rpc.publicnode.com",
    "https://rpc-robinhood.hoodmarket.io",
]
UA = "RHC-Selection-Alpha-RPC/1.0 (read-only)"
_lock = threading.Lock()
_rpc_index = 0
_call_count = 0
_backoffs = 0


def rpc_call(method: str, params: list[Any], attempts: int = 12, timeout: int = 90) -> tuple[Any, str]:
    global _rpc_index, _call_count, _backoffs
    last: Any = None
    for attempt in range(attempts):
        with _lock:
            rpc = RPCS[_rpc_index % len(RPCS)]
            _rpc_index += 1
            _call_count += 1
        payload = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode()
        try:
            req = urllib.request.Request(
                rpc,
                data=payload,
                method="POST",
                headers={"content-type": "application/json", "accept": "application/json", "user-agent": UA},
            )
            with urllib.request.urlopen(req, timeout=timeout) as response:
                data = json.loads(response.read().decode("utf-8"))
            if not isinstance(data, dict):
                raise RuntimeError(f"non-object RPC response: {type(data)}")
            if data.get("error") is not None:
                return {"__rpc_error__": data["error"]}, rpc
            return data.get("result"), rpc
        except urllib.error.HTTPError as exc:
            last = exc
            if exc.code == 429 or exc.code >= 500:
                with _lock:
                    _backoffs += 1
                time.sleep(min(60, 2 ** min(attempt, 5) + random.random() * 5))
                continue
            raise
        except Exception as exc:
            last = exc
            if attempt + 1 < attempts:
                time.sleep(min(30, 2 ** min(attempt, 4) + random.random() * 3))
                continue
    raise RuntimeError(f"RPC {method} failed: {last!r}")


def latest_block() -> tuple[int, str]:
    result, rpc = rpc_call("eth_blockNumber", [], attempts=6, timeout=30)
    if not isinstance(result, str) or not result.startswith("0x"):
        raise RuntimeError(f"invalid eth_blockNumber result: {result!r}")
    return int(result, 16), rpc


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
    result, rpc = rpc_call(
        "eth_getLogs",
        [{
            "fromBlock": hex(start),
            "toBlock": hex(stop),
            "address": config["address"],
            "topics": [config["topic0"]],
        }],
    )
    if isinstance(result, list):
        return "ok", [normalize(row, target) for row in result if isinstance(row, dict)], rpc
    if isinstance(result, dict) and result.get("__rpc_error__") is not None:
        text = json.dumps(result["__rpc_error__"], sort_keys=True).lower()
        # Provider range/result limits are resolved by recursive splitting.
        if any(token in text for token in ("range", "too many", "response", "limit", "timeout", "size", "query returned")):
            return "split", [], rpc + ":" + text[:500]
        return "retry_or_split", [], rpc + ":" + text[:500]
    return "retry_or_split", [], rpc + ":unexpected:" + repr(result)[:500]


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def collect(target: str, out: Path, workers: int, initial_window: int) -> None:
    out.mkdir(parents=True, exist_ok=True)
    head, head_rpc = latest_block()
    pending: list[tuple[int, int, int]] = [
        (start, min(start + initial_window - 1, head), 0)
        for start in range(0, head + 1, initial_window)
    ]
    accepted: list[dict[str, Any]] = []
    ranges: list[dict[str, Any]] = []
    unresolved: list[dict[str, Any]] = []

    while pending:
        batch = pending[: workers * 2]
        pending = pending[workers * 2 :]
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(query_range, target, start, stop): (start, stop, depth) for start, stop, depth in batch}
            for future in as_completed(futures):
                start, stop, depth = futures[future]
                try:
                    state, rows, note = future.result()
                except Exception as exc:
                    state, rows, note = "retry_or_split", [], repr(exc)
                if state == "ok":
                    accepted.extend(rows)
                    ranges.append({"from_block": start, "to_block": stop, "depth": depth, "status": "ACCEPTED", "row_count": len(rows), "note": note})
                elif start < stop:
                    middle = (start + stop) // 2
                    pending.append((start, middle, depth + 1))
                    pending.append((middle + 1, stop, depth + 1))
                    ranges.append({"from_block": start, "to_block": stop, "depth": depth, "status": "SPLIT", "row_count": 0, "note": note})
                else:
                    item = {"from_block": start, "to_block": stop, "depth": depth, "status": "UNRESOLVED_SINGLE_BLOCK", "row_count": 0, "note": note}
                    ranges.append(item)
                    unresolved.append(item)
        pending.sort(key=lambda item: (item[0], item[1], item[2]))
        if len(ranges) % 40 < workers * 2:
            print(target, "ranges", len(ranges), "pending", len(pending), "rows", len(accepted), flush=True)

    unique: dict[tuple[str, int], dict[str, Any]] = {}
    for row in accepted:
        key = (row["transaction_hash"], row["log_index"] if row["log_index"] is not None else -1)
        unique[key] = row
    logs = sorted(unique.values(), key=lambda row: (row["block_number"] or -1, row["transaction_index"] or -1, row["log_index"] or -1))

    with (out / "logs.jsonl").open("w", encoding="utf-8") as handle:
        for row in logs:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
    fields = ["chain_id", "target", "address", "block_number", "block_hash", "transaction_hash", "transaction_index", "log_index", "removed", "data", "topics", "topic0"]
    with (out / "logs.csv").open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in logs:
            writer.writerow({field: json.dumps(row[field], ensure_ascii=False) if isinstance(row[field], list) else row[field] for field in fields})
    for filename, data in (("ranges.csv", ranges), ("errors.csv", unresolved)):
        with (out / filename).open("w", newline="", encoding="utf-8-sig") as handle:
            writer = csv.DictWriter(handle, fieldnames=["from_block", "to_block", "depth", "status", "row_count", "note"])
            writer.writeheader()
            writer.writerows(sorted(data, key=lambda item: (item["from_block"], item["to_block"], item["depth"])))

    wrong_topic = sum(row["topic0"] != TARGETS[target]["topic0"] for row in logs)
    removed = sum(row["removed"] for row in logs)
    validation = {
        "status": "PASS" if not unresolved and not wrong_topic and not removed else "FAIL",
        "chain_id": CHAIN_ID,
        "target": target,
        "head_block": head,
        "head_rpc": head_rpc,
        "initial_window_blocks": initial_window,
        "workers": workers,
        "row_count": len(logs),
        "duplicates_removed": len(accepted) - len(logs),
        "range_attempts": len(ranges),
        "accepted_ranges": sum(row["status"] == "ACCEPTED" for row in ranges),
        "split_ranges": sum(row["status"] == "SPLIT" for row in ranges),
        "unresolved_single_blocks": len(unresolved),
        "wrong_topic_rows": wrong_topic,
        "removed_rows": removed,
        "rpc_calls": _call_count,
        "rpc_backoffs": _backoffs,
        "first_block": logs[0]["block_number"] if logs else None,
        "last_block": logs[-1]["block_number"] if logs else None,
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
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--workers", type=int, default=6)
    parser.add_argument("--initial-window", type=int, default=1_000_000)
    args = parser.parse_args()
    collect(args.target, args.out, max(1, args.workers), max(1, args.initial_window))


if __name__ == "__main__":
    main()
