#!/usr/bin/env python3
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import random
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

FIXED_HEAD = 48_264_433
ENDPOINTS = [
    "https://robinhoodchain.blockscout.com/api",
    "https://explorer.hoodmarketcap.com/api",
]
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
USER_AGENT = "RHC-Canonical-Universe/1.0"


def canonical_json(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


class Client:
    def __init__(self, delay_seconds: float = 1.25):
        self.delay = delay_seconds
        self.last_request = 0.0
        self.stats: dict[str, int] = {}
        self.endpoint_cursor = 0

    def pace(self) -> None:
        wait = self.delay - (time.monotonic() - self.last_request)
        if wait > 0:
            time.sleep(wait)

    def get(self, endpoint: str, params: dict[str, Any], attempts: int = 10) -> dict[str, Any]:
        url = endpoint + "?" + urllib.parse.urlencode(params)
        last_error: Exception | None = None
        for attempt in range(attempts):
            self.pace()
            req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, "Accept": "application/json"})
            try:
                with urllib.request.urlopen(req, timeout=90) as response:
                    body = response.read()
                    status = response.status
                self.last_request = time.monotonic()
                self.stats[f"http_{status}"] = self.stats.get(f"http_{status}", 0) + 1
                payload = json.loads(body.decode("utf-8"))
                if not isinstance(payload, dict):
                    raise RuntimeError(f"non-object response: {type(payload)}")
                return payload
            except urllib.error.HTTPError as exc:
                self.last_request = time.monotonic()
                self.stats[f"http_{exc.code}"] = self.stats.get(f"http_{exc.code}", 0) + 1
                last_error = exc
                if exc.code in (429, 500, 502, 503, 504):
                    sleep = min(180.0, 10.0 * (2 ** min(attempt, 4)) + random.random() * 4)
                    time.sleep(sleep)
                    continue
                body = exc.read(1000).decode("utf-8", "replace")
                raise RuntimeError(f"HTTP {exc.code} {url}: {body}") from exc
            except Exception as exc:
                last_error = exc
                self.stats["network_or_decode_error"] = self.stats.get("network_or_decode_error", 0) + 1
                if attempt + 1 < attempts:
                    time.sleep(min(90.0, 5.0 * (2 ** min(attempt, 4)) + random.random() * 3))
                    continue
        raise RuntimeError(f"request exhausted: {url}: {last_error!r}")

    def query_logs(self, target: str, start: int, end: int) -> tuple[list[dict[str, Any]], str, str]:
        spec = TARGETS[target]
        params = {
            "module": "logs",
            "action": "getLogs",
            "fromBlock": start,
            "toBlock": end,
            "address": spec["address"],
            "topic0": spec["topic0"],
        }
        failures: list[str] = []
        order = ENDPOINTS[self.endpoint_cursor :] + ENDPOINTS[: self.endpoint_cursor]
        for endpoint in order:
            try:
                payload = self.get(endpoint, params)
                result = payload.get("result")
                message = str(payload.get("message", ""))
                status = str(payload.get("status", ""))
                if isinstance(result, list):
                    self.endpoint_cursor = (ENDPOINTS.index(endpoint) + 1) % len(ENDPOINTS)
                    return [row for row in result if isinstance(row, dict)], endpoint, message
                if status == "0" and isinstance(result, str) and "No records" in result:
                    return [], endpoint, message
                raise RuntimeError(f"unexpected payload: {payload!r}")
            except Exception as exc:
                failures.append(f"{endpoint}:{exc!r}")
        raise RuntimeError(" | ".join(failures))


def int_hex(value: Any) -> int:
    if isinstance(value, int):
        return value
    if not isinstance(value, str):
        raise ValueError(value)
    return int(value, 16) if value.startswith("0x") else int(value)


def row_key(row: dict[str, Any]) -> tuple[str, int]:
    tx = str(row.get("transactionHash") or row.get("transaction_hash") or "").lower()
    log_index = int_hex(row.get("logIndex") or row.get("log_index") or "0x0")
    return tx, log_index


def collect(target: str, start: int, end: int, out: Path) -> None:
    if target not in TARGETS:
        raise SystemExit(f"unknown target: {target}")
    if start < 0 or end < start or end > FIXED_HEAD:
        raise SystemExit(f"invalid range {start}-{end}, fixed head {FIXED_HEAD}")

    out.mkdir(parents=True, exist_ok=True)
    client = Client()
    pending: list[tuple[int, int]] = [(start, end)]
    accepted_ranges: list[dict[str, Any]] = []
    rows_by_key: dict[tuple[str, int], dict[str, Any]] = {}
    request_log: list[dict[str, Any]] = []
    single_block_caps: list[dict[str, Any]] = []

    while pending:
        left, right = pending.pop()
        rows, endpoint, message = client.query_logs(target, left, right)
        request_log.append({
            "from_block": left,
            "to_block": right,
            "rows": len(rows),
            "endpoint": endpoint,
            "message": message,
        })
        print(target, left, right, len(rows), endpoint, flush=True)

        if len(rows) >= 1000:
            if left == right:
                single_block_caps.append({"block": left, "rows_returned": len(rows), "endpoint": endpoint})
                continue
            middle = (left + right) // 2
            pending.append((middle + 1, right))
            pending.append((left, middle))
            continue

        normalized_range_keys: list[tuple[str, int]] = []
        for row in rows:
            block = int_hex(row.get("blockNumber") or row.get("block_number"))
            if not left <= block <= right:
                raise RuntimeError(f"row outside requested range: {block} not in {left}-{right}")
            address = str(row.get("address") or "").lower()
            topics = [str(item).lower() for item in row.get("topics") or []]
            if address != TARGETS[target]["address"]:
                raise RuntimeError(f"wrong emitting address: {address}")
            if not topics or topics[0] != TARGETS[target]["topic0"]:
                raise RuntimeError(f"wrong topic0: {topics[:1]}")
            key = row_key(row)
            normalized_range_keys.append(key)
            existing = rows_by_key.get(key)
            if existing is not None and canonical_json(existing) != canonical_json(row):
                raise RuntimeError(f"conflicting duplicate event {key}")
            rows_by_key[key] = row
        accepted_ranges.append({
            "from_block": left,
            "to_block": right,
            "row_count": len(normalized_range_keys),
            "endpoint": endpoint,
        })

    accepted_ranges.sort(key=lambda row: row["from_block"])
    expected = start
    coverage_failures: list[dict[str, Any]] = []
    for item in accepted_ranges:
        if item["from_block"] != expected:
            coverage_failures.append({"expected_from": expected, "actual_from": item["from_block"]})
        expected = item["to_block"] + 1
    if expected != end + 1:
        coverage_failures.append({"expected_final": end + 1, "actual_final": expected})

    sorted_rows = sorted(
        rows_by_key.values(),
        key=lambda row: (
            int_hex(row.get("blockNumber") or row.get("block_number")),
            int_hex(row.get("transactionIndex") or row.get("transaction_index") or "0x0"),
            int_hex(row.get("logIndex") or row.get("log_index") or "0x0"),
        ),
    )
    with gzip.open(out / "events.jsonl.gz", "wt", encoding="utf-8", newline="\n") as handle:
        for row in sorted_rows:
            handle.write(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n")

    validation = {
        "status": "PASS" if not coverage_failures and not single_block_caps else "FAIL",
        "target": target,
        "fixed_head": FIXED_HEAD,
        "requested_from_block": start,
        "requested_to_block": end,
        "event_rows": len(sorted_rows),
        "unique_event_keys": len(rows_by_key),
        "accepted_ranges": len(accepted_ranges),
        "requests": len(request_log),
        "coverage_failures": coverage_failures,
        "single_block_caps": single_block_caps,
        "http_stats": client.stats,
        "first_event_block": int_hex(sorted_rows[0].get("blockNumber")) if sorted_rows else None,
        "last_event_block": int_hex(sorted_rows[-1].get("blockNumber")) if sorted_rows else None,
    }
    (out / "validation.json").write_text(json.dumps(validation, indent=2, sort_keys=True), encoding="utf-8")
    (out / "accepted_ranges.json").write_text(json.dumps(accepted_ranges, indent=2), encoding="utf-8")
    (out / "request_log.json").write_text(json.dumps(request_log, indent=2), encoding="utf-8")

    manifest = []
    for path in sorted(out.iterdir()):
        if path.is_file():
            manifest.append({
                "path": path.name,
                "bytes": path.stat().st_size,
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            })
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(validation, sort_keys=True), flush=True)
    if validation["status"] != "PASS":
        raise SystemExit(1)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", choices=sorted(TARGETS), required=True)
    parser.add_argument("--start", type=int, required=True)
    parser.add_argument("--end", type=int, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    collect(args.target, args.start, args.end, args.out)


if __name__ == "__main__":
    main()
