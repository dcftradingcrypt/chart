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
SEADROP = "0x00005ea00ac477b1030ce78506496e8c2de24bf5"
ENDPOINTS = [
    "https://robinhoodchain.blockscout.com/api",
    "https://explorer.hoodmarketcap.com/api",
]
USER_AGENT = "RHC-SeaDrop-All-Logs/1.0"


def canonical_json(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def intish(value: Any) -> int:
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        return int(value, 16) if value.startswith("0x") else int(value)
    raise ValueError(value)


def event_key(row: dict[str, Any]) -> tuple[str, int]:
    tx_hash = str(row.get("transactionHash") or row.get("transaction_hash") or "").lower()
    log_index = intish(row.get("logIndex") or row.get("log_index") or "0x0")
    return tx_hash, log_index


class Client:
    def __init__(self, delay_seconds: float = 1.35):
        self.delay_seconds = delay_seconds
        self.last_request = 0.0
        self.endpoint_cursor = 0
        self.stats: dict[str, int] = {}

    def pace(self) -> None:
        wait = self.delay_seconds - (time.monotonic() - self.last_request)
        if wait > 0:
            time.sleep(wait)

    def request(self, endpoint: str, params: dict[str, Any], attempts: int = 10) -> dict[str, Any]:
        url = endpoint + "?" + urllib.parse.urlencode(params)
        last_error: Exception | None = None
        for attempt in range(attempts):
            self.pace()
            request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, "Accept": "application/json"})
            try:
                with urllib.request.urlopen(request, timeout=90) as response:
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
                    time.sleep(min(180.0, 10.0 * (2 ** min(attempt, 4)) + random.random() * 5))
                    continue
                body = exc.read(1000).decode("utf-8", "replace")
                raise RuntimeError(f"HTTP {exc.code} {url}: {body}") from exc
            except Exception as exc:
                last_error = exc
                self.stats["network_or_decode_error"] = self.stats.get("network_or_decode_error", 0) + 1
                if attempt + 1 < attempts:
                    time.sleep(min(90.0, 5.0 * (2 ** min(attempt, 4)) + random.random() * 4))
                    continue
        raise RuntimeError(f"request exhausted: {url}: {last_error!r}")

    def query(self, start: int, end: int) -> tuple[list[dict[str, Any]], str, str]:
        params = {
            "module": "logs",
            "action": "getLogs",
            "fromBlock": start,
            "toBlock": end,
            "address": SEADROP,
        }
        errors: list[str] = []
        ordered = ENDPOINTS[self.endpoint_cursor :] + ENDPOINTS[: self.endpoint_cursor]
        for endpoint in ordered:
            try:
                payload = self.request(endpoint, params)
                result = payload.get("result")
                status = str(payload.get("status", ""))
                message = str(payload.get("message", ""))
                if isinstance(result, list):
                    self.endpoint_cursor = (ENDPOINTS.index(endpoint) + 1) % len(ENDPOINTS)
                    return [row for row in result if isinstance(row, dict)], endpoint, message
                if status == "0" and isinstance(result, str) and "No records" in result:
                    return [], endpoint, message
                raise RuntimeError(f"unexpected payload: {payload!r}")
            except Exception as exc:
                errors.append(f"{endpoint}:{exc!r}")
        raise RuntimeError(" | ".join(errors))


def collect(start: int, end: int, out: Path) -> None:
    if start < 0 or end < start or end > FIXED_HEAD:
        raise SystemExit(f"invalid range {start}-{end}; fixed head={FIXED_HEAD}")
    out.mkdir(parents=True, exist_ok=True)
    client = Client()
    pending: list[tuple[int, int]] = [(start, end)]
    accepted_ranges: list[dict[str, Any]] = []
    request_log: list[dict[str, Any]] = []
    events: dict[tuple[str, int], dict[str, Any]] = {}
    single_block_caps: list[dict[str, Any]] = []

    while pending:
        left, right = pending.pop()
        rows, endpoint, message = client.query(left, right)
        request_log.append({
            "from_block": left,
            "to_block": right,
            "rows": len(rows),
            "endpoint": endpoint,
            "message": message,
        })
        print(left, right, len(rows), endpoint, flush=True)
        if len(rows) >= 1000:
            if left == right:
                single_block_caps.append({"block": left, "rows": len(rows), "endpoint": endpoint})
                continue
            middle = (left + right) // 2
            pending.append((middle + 1, right))
            pending.append((left, middle))
            continue

        for row in rows:
            address = str(row.get("address") or "").lower()
            if address != SEADROP:
                raise RuntimeError(f"wrong emitting address: {address}")
            block = intish(row.get("blockNumber") or row.get("block_number"))
            if block < left or block > right:
                raise RuntimeError(f"row block outside range: {block} not {left}-{right}")
            key = event_key(row)
            previous = events.get(key)
            if previous is not None and canonical_json(previous) != canonical_json(row):
                raise RuntimeError(f"conflicting duplicate event: {key}")
            events[key] = row
        accepted_ranges.append({
            "from_block": left,
            "to_block": right,
            "row_count": len(rows),
            "endpoint": endpoint,
        })

    accepted_ranges.sort(key=lambda row: row["from_block"])
    failures: list[dict[str, Any]] = []
    expected = start
    for row in accepted_ranges:
        if row["from_block"] != expected:
            failures.append({"expected_from": expected, "actual_from": row["from_block"]})
        expected = row["to_block"] + 1
    if expected != end + 1:
        failures.append({"expected_final": end + 1, "actual_final": expected})

    ordered = sorted(
        events.values(),
        key=lambda row: (
            intish(row.get("blockNumber") or row.get("block_number")),
            intish(row.get("transactionIndex") or row.get("transaction_index") or "0x0"),
            intish(row.get("logIndex") or row.get("log_index") or "0x0"),
        ),
    )
    with gzip.open(out / "seadrop_all_logs.jsonl.gz", "wt", encoding="utf-8", newline="\n") as handle:
        for row in ordered:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n")

    topic_counts: dict[str, int] = {}
    for row in ordered:
        topics = row.get("topics") or []
        key = str(topics[0]).lower() if topics else "NO_TOPIC"
        topic_counts[key] = topic_counts.get(key, 0) + 1

    validation = {
        "status": "PASS" if not failures and not single_block_caps else "FAIL",
        "fixed_head": FIXED_HEAD,
        "requested_from_block": start,
        "requested_to_block": end,
        "event_rows": len(ordered),
        "accepted_ranges": len(accepted_ranges),
        "request_count": len(request_log),
        "coverage_failures": failures,
        "single_block_caps": single_block_caps,
        "topic0_counts": topic_counts,
        "http_stats": client.stats,
        "first_event_block": intish(ordered[0]["blockNumber"]) if ordered else None,
        "last_event_block": intish(ordered[-1]["blockNumber"]) if ordered else None,
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
    parser.add_argument("--start", type=int, required=True)
    parser.add_argument("--end", type=int, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    collect(args.start, args.end, args.out)


if __name__ == "__main__":
    main()
