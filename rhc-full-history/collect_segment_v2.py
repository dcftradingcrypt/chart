#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
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

BASE = "https://robinhoodchain.blockscout.com/api"
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
USER_AGENT = "RHC-NFT-Full-History/2.0 (read-only research)"
_lock = threading.Lock()
_next_allowed = 0.0
_http_calls = 0
_backoffs = 0


def integer(value: Any, default: int | None = None) -> int | None:
    if value is None:
        return default
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        try:
            return int(value, 16) if value.startswith("0x") else int(value)
        except ValueError:
            return default
    return default


def pace(min_interval: float = 0.35) -> None:
    global _next_allowed
    with _lock:
        now = time.monotonic()
        wait = max(0.0, _next_allowed - now)
        _next_allowed = max(now, _next_allowed) + min_interval
    if wait:
        time.sleep(wait)


def request_json(url: str) -> dict[str, Any]:
    global _http_calls, _backoffs
    last: Exception | None = None
    for attempt in range(10):
        pace()
        with _lock:
            _http_calls += 1
        try:
            req = urllib.request.Request(url, headers={"user-agent": USER_AGENT, "accept": "application/json"})
            with urllib.request.urlopen(req, timeout=45) as response:
                raw = response.read()
            data = json.loads(raw.decode("utf-8"))
            if not isinstance(data, dict):
                raise RuntimeError(f"unexpected response type: {type(data)}")
            return data
        except urllib.error.HTTPError as exc:
            last = exc
            if exc.code == 429:
                with _lock:
                    _backoffs += 1
                retry = exc.headers.get("Retry-After")
                delay = float(retry) if retry and retry.replace(".", "", 1).isdigit() else min(30, 3 + attempt * 3 + random.random())
                time.sleep(delay)
                continue
            # Oversized-range errors must be split immediately rather than retried.
            if exc.code in (408, 413, 425, 500, 502, 503, 504):
                raise RuntimeError(f"range_or_server_http_{exc.code}") from exc
            raise
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            last = exc
            if attempt < 2:
                time.sleep(1 + attempt + random.random())
                continue
            raise RuntimeError(f"network_or_decode_error:{exc}") from exc
    raise RuntimeError(f"request failed after rate-limit retries: {last}")


def get_logs(target: str, start: int, end: int) -> tuple[str, list[dict[str, Any]], str]:
    spec = TARGETS[target]
    params = {
        "module": "logs",
        "action": "getLogs",
        "fromBlock": start,
        "toBlock": end,
        "address": spec["address"],
        "topic0": spec["topic0"],
    }
    url = BASE + "?" + urllib.parse.urlencode(params)
    try:
        data = request_json(url)
    except Exception as exc:
        return "split", [], repr(exc)
    status = str(data.get("status", ""))
    message = str(data.get("message", ""))
    result = data.get("result")
    if isinstance(result, list):
        rows = [row for row in result if isinstance(row, dict)]
        if len(rows) >= 1000:
            return "split", rows, f"cap_or_exact_1000:{len(rows)}"
        return "ok", rows, f"status={status};message={message}"
    text = f"{message} {result}".lower()
    if any(term in text for term in ("no logs", "no records", "not found")):
        return "ok", [], text
    if any(term in text for term in ("1000", "too many", "query timeout", "timeout", "range", "response size")):
        return "split", [], text
    return "split", [], text[:1000]


def normalize(row: dict[str, Any], target: str) -> dict[str, Any]:
    topics = row.get("topics") or []
    if isinstance(topics, str):
        try:
            topics = json.loads(topics)
        except Exception:
            topics = [topics]
    topics = [str(t).lower() for t in topics]
    return {
        "target": target,
        "address": str(row.get("address") or TARGETS[target]["address"]).lower(),
        "block_number": integer(row.get("blockNumber"), integer(row.get("block_number"))),
        "transaction_hash": str(row.get("transactionHash") or row.get("transaction_hash") or "").lower(),
        "transaction_index": integer(row.get("transactionIndex"), integer(row.get("transaction_index"))),
        "log_index": integer(row.get("logIndex"), integer(row.get("log_index"))),
        "data": str(row.get("data") or "0x").lower(),
        "topics": topics,
        "topic0": topics[0] if topics else None,
        "block_hash": str(row.get("blockHash") or row.get("block_hash") or "").lower(),
        "raw": row,
    }


def collect(target: str, segment_index: int, segment_count: int, end_block: int, out: Path, workers: int, initial_window: int) -> None:
    start_block = (end_block + 1) * segment_index // segment_count
    segment_end = ((end_block + 1) * (segment_index + 1) // segment_count) - 1
    out.mkdir(parents=True, exist_ok=True)
    pending: list[tuple[int, int]] = []
    cursor = start_block
    while cursor <= segment_end:
        right = min(segment_end, cursor + initial_window - 1)
        pending.append((cursor, right))
        cursor = right + 1
    accepted: list[dict[str, Any]] = []
    range_rows: list[dict[str, Any]] = []
    hard_errors: list[dict[str, Any]] = []

    while pending:
        batch = pending[: workers * 2]
        pending = pending[workers * 2 :]
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(get_logs, target, a, b): (a, b) for a, b in batch}
            for future in as_completed(futures):
                a, b = futures[future]
                try:
                    state, rows, note = future.result()
                except Exception as exc:
                    state, rows, note = "split", [], repr(exc)
                if state == "ok":
                    accepted.extend(normalize(row, target) for row in rows)
                    range_rows.append({"from_block": a, "to_block": b, "status": "ACCEPTED", "row_count": len(rows), "note": note})
                elif a < b:
                    mid = (a + b) // 2
                    pending.append((a, mid))
                    pending.append((mid + 1, b))
                    range_rows.append({"from_block": a, "to_block": b, "status": "SPLIT", "row_count": len(rows), "note": note})
                else:
                    hard_errors.append({"from_block": a, "to_block": b, "status": "UNRESOLVED_SINGLE_BLOCK", "row_count": len(rows), "note": note})
                    range_rows.append(hard_errors[-1])
        pending.sort()
        if len(range_rows) and len(range_rows) % 50 == 0:
            print(f"{target} segment={segment_index}/{segment_count} ranges={len(range_rows)} pending={len(pending)} rows={len(accepted)}", flush=True)

    dedup: dict[tuple[str, int], dict[str, Any]] = {}
    for row in accepted:
        key = (row["transaction_hash"], row["log_index"] if row["log_index"] is not None else -1)
        dedup[key] = row
    rows = sorted(dedup.values(), key=lambda x: (x["block_number"] or -1, x["transaction_index"] or -1, x["log_index"] or -1))

    with (out / "logs.jsonl").open("w", encoding="utf-8") as file:
        for row in rows:
            file.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
    fields = ["target", "address", "block_number", "transaction_hash", "transaction_index", "log_index", "data", "topics", "topic0", "block_hash"]
    with (out / "logs.csv").open("w", encoding="utf-8-sig", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: json.dumps(row[k], ensure_ascii=False) if isinstance(row[k], list) else row[k] for k in fields})
    with (out / "ranges.csv").open("w", encoding="utf-8-sig", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=["from_block", "to_block", "status", "row_count", "note"])
        writer.writeheader(); writer.writerows(sorted(range_rows, key=lambda x: (x["from_block"], x["to_block"])))
    with (out / "errors.csv").open("w", encoding="utf-8-sig", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=["from_block", "to_block", "status", "row_count", "note"])
        writer.writeheader(); writer.writerows(hard_errors)

    wrong_topic = sum(1 for row in rows if row["topic0"] != TARGETS[target]["topic0"])
    out_of_range = sum(1 for row in rows if row["block_number"] is None or not (start_block <= row["block_number"] <= segment_end))
    validation = {
        "status": "PASS" if not hard_errors and wrong_topic == 0 and out_of_range == 0 else "FAIL",
        "target": target,
        "segment_index": segment_index,
        "segment_count": segment_count,
        "from_block": start_block,
        "to_block": segment_end,
        "accepted_rows": len(rows),
        "duplicates_removed": len(accepted) - len(rows),
        "wrong_topic_rows": wrong_topic,
        "out_of_range_rows": out_of_range,
        "unresolved_single_block_ranges": len(hard_errors),
        "range_requests": len(range_rows),
        "http_calls": _http_calls,
        "rate_limit_backoffs": _backoffs,
        "initial_window": initial_window,
    }
    (out / "validation.json").write_text(json.dumps(validation, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(validation, sort_keys=True), flush=True)
    if validation["status"] != "PASS":
        raise SystemExit(2)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", choices=sorted(TARGETS), required=True)
    parser.add_argument("--segment-index", type=int, required=True)
    parser.add_argument("--segment-count", type=int, required=True)
    parser.add_argument("--end-block", type=int, required=True)
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--initial-window", type=int, default=100000)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    if not 0 <= args.segment_index < args.segment_count:
        raise SystemExit("invalid segment index")
    collect(args.target, args.segment_index, args.segment_count, args.end_block, args.out, max(1, args.workers), max(1, args.initial_window))


if __name__ == "__main__":
    main()
