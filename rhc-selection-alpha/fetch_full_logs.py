#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import random
import threading
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
RPCS = [
    "https://rpc.mainnet.chain.robinhood.com",
    "https://robinhood-rpc.publicnode.com",
    "https://rpc-robinhood.hoodmarket.io",
]
UA = "RHC-Selection-Alpha-History/1.0 (read-only)"
_lock = threading.Lock()
_next_request_at = 0.0
_http_calls = 0
_backoffs = 0


def pace(interval: float = 0.18) -> None:
    global _next_request_at
    with _lock:
        now = time.monotonic()
        delay = max(0.0, _next_request_at - now)
        _next_request_at = max(now, _next_request_at) + interval
    if delay:
        time.sleep(delay)


def fetch_json(url: str, *, payload: dict[str, Any] | None = None, attempts: int = 7, timeout: int = 35) -> Any:
    global _http_calls, _backoffs
    body = json.dumps(payload).encode() if payload is not None else None
    headers = {"accept": "application/json", "user-agent": UA}
    if body is not None:
        headers["content-type"] = "application/json"
    last: Exception | None = None
    for attempt in range(attempts):
        pace()
        with _lock:
            _http_calls += 1
        try:
            req = urllib.request.Request(url, data=body, headers=headers, method="POST" if body else "GET")
            with urllib.request.urlopen(req, timeout=timeout) as response:
                raw = response.read()
            return json.loads(raw.decode("utf-8"))
        except urllib.error.HTTPError as exc:
            last = exc
            if exc.code == 429 or exc.code >= 500:
                with _lock:
                    _backoffs += 1
                time.sleep(min(30, 2 ** min(attempt, 5) + random.random() * 2))
                continue
            raise
        except Exception as exc:
            last = exc
            if attempt + 1 < attempts:
                time.sleep(min(20, 2 ** min(attempt, 4) + random.random() * 2))
                continue
    raise RuntimeError(f"request failed: {url}: {last!r}")


def latest_block() -> tuple[int, str]:
    for rpc in RPCS:
        try:
            data = fetch_json(rpc, payload={"jsonrpc": "2.0", "id": 1, "method": "eth_blockNumber", "params": []})
            value = data.get("result") if isinstance(data, dict) else None
            if isinstance(value, str) and value.startswith("0x"):
                return int(value, 16), rpc
        except Exception as exc:
            print("head probe failed", rpc, repr(exc), flush=True)
    raise SystemExit("no Robinhood Chain RPC returned eth_blockNumber")


def number(value: Any) -> int | None:
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


def query_range(target: str, start: int, stop: int) -> tuple[str, list[dict[str, Any]], str]:
    config = TARGETS[target]
    params = {
        "module": "logs",
        "action": "getLogs",
        "fromBlock": start,
        "toBlock": stop,
        "address": config["address"],
        "topic0": config["topic0"],
    }
    notes: list[str] = []
    for base in EXPLORERS:
        url = base + "?" + urllib.parse.urlencode(params)
        try:
            data = fetch_json(url)
        except Exception as exc:
            notes.append(f"{base}:{exc!r}")
            continue
        if not isinstance(data, dict):
            notes.append(f"{base}:non-object")
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
            return "ok", [], f"{base}:{text[:240]}"
        if any(token in text for token in ("1000", "too many", "timeout", "range", "response size", "query timeout")):
            return "split", [], f"{base}:{text[:240]}"
        notes.append(f"{base}:{text[:240]}")
    return "retry_split", [], " | ".join(notes)


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
        "block_number": number(row.get("blockNumber") or row.get("block_number")),
        "block_hash": str(row.get("blockHash") or row.get("block_hash") or "").lower(),
        "transaction_hash": str(row.get("transactionHash") or row.get("transaction_hash") or "").lower(),
        "transaction_index": number(row.get("transactionIndex") or row.get("transaction_index")),
        "log_index": number(row.get("logIndex") or row.get("log_index")),
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


def collect(target: str, end_block: int, out: Path, workers: int, initial_parts: int) -> None:
    out.mkdir(parents=True, exist_ok=True)
    queue: list[tuple[int, int, int]] = []
    for part in range(initial_parts):
        start = (end_block + 1) * part // initial_parts
        stop = (end_block + 1) * (part + 1) // initial_parts - 1
        queue.append((start, stop, 0))

    accepted_rows: list[dict[str, Any]] = []
    ranges: list[dict[str, Any]] = []
    unresolved: list[dict[str, Any]] = []

    while queue:
        batch = queue[: workers * 3]
        queue = queue[workers * 3 :]
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(query_range, target, start, stop): (start, stop, depth) for start, stop, depth in batch}
            for future in as_completed(futures):
                start, stop, depth = futures[future]
                try:
                    state, rows, note = future.result()
                except Exception as exc:
                    state, rows, note = "retry_split", [], repr(exc)
                if state == "ok":
                    accepted_rows.extend(normalize(row, target) for row in rows)
                    ranges.append({"from_block": start, "to_block": stop, "depth": depth, "status": "ACCEPTED", "row_count": len(rows), "note": note})
                elif start < stop:
                    middle = (start + stop) // 2
                    queue.append((start, middle, depth + 1))
                    queue.append((middle + 1, stop, depth + 1))
                    ranges.append({"from_block": start, "to_block": stop, "depth": depth, "status": "SPLIT", "row_count": len(rows), "note": note})
                else:
                    item = {"from_block": start, "to_block": stop, "depth": depth, "status": "UNRESOLVED_SINGLE_BLOCK", "row_count": len(rows), "note": note}
                    ranges.append(item)
                    unresolved.append(item)
        queue.sort(key=lambda item: (item[0], item[1]))
        if len(ranges) % 50 < workers * 3:
            print(target, "ranges", len(ranges), "pending", len(queue), "raw rows", len(accepted_rows), flush=True)

    unique: dict[tuple[str, int], dict[str, Any]] = {}
    for row in accepted_rows:
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
    for filename, data in (("ranges.csv", ranges), ("errors.csv", unresolved)):
        with (out / filename).open("w", newline="", encoding="utf-8-sig") as handle:
            writer = csv.DictWriter(handle, fieldnames=["from_block", "to_block", "depth", "status", "row_count", "note"])
            writer.writeheader()
            writer.writerows(sorted(data, key=lambda item: (item["from_block"], item["to_block"], item["depth"])))

    wrong_topic = sum(row["topic0"] != TARGETS[target]["topic0"] for row in logs)
    out_of_range = sum(row["block_number"] is None or not 0 <= row["block_number"] <= end_block for row in logs)
    validation = {
        "status": "PASS" if not unresolved and not wrong_topic and not out_of_range else "FAIL",
        "chain_id": CHAIN_ID,
        "target": target,
        "from_block": 0,
        "to_block": end_block,
        "accepted_rows": len(logs),
        "duplicates_removed": len(accepted_rows) - len(logs),
        "wrong_topic_rows": wrong_topic,
        "out_of_range_rows": out_of_range,
        "unresolved_single_blocks": len(unresolved),
        "range_attempts": len(ranges),
        "http_calls": _http_calls,
        "rate_limit_backoffs": _backoffs,
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
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--initial-parts", type=int, default=8)
    args = parser.parse_args()
    head, rpc = latest_block()
    (args.out / "head.json").parent.mkdir(parents=True, exist_ok=True)
    (args.out / "head.json").write_text(json.dumps({"chain_id": CHAIN_ID, "head_block": head, "rpc": rpc}, indent=2), encoding="utf-8")
    collect(args.target, head, args.out, max(1, args.workers), max(1, args.initial_parts))


if __name__ == "__main__":
    main()
