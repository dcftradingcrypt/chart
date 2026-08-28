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

from Crypto.Hash import keccak

FIXED_HEAD = 48_264_433
ENDPOINTS = [
    "https://robinhoodchain.blockscout.com/api",
    "https://explorer.hoodmarketcap.com/api",
]
ZERO_TOPIC = "0x" + ("0" * 64)
USER_AGENT = "RHC-Global-NFT-Mint-Universe/1.0"


def topic(signature: str) -> str:
    digest = keccak.new(digest_bits=256)
    digest.update(signature.encode("utf-8"))
    return "0x" + digest.hexdigest()


TARGETS = {
    "erc721": {
        "params": {
            "topic0": topic("Transfer(address,address,uint256)"),
            "topic1": ZERO_TOPIC,
            "topic0_1_opr": "and",
        },
        "expected_topic_count": 4,
    },
    "erc1155_single": {
        "params": {
            "topic0": topic("TransferSingle(address,address,address,uint256,uint256)"),
            "topic2": ZERO_TOPIC,
            "topic0_2_opr": "and",
        },
        "expected_topic_count": 4,
    },
    "erc1155_batch": {
        "params": {
            "topic0": topic("TransferBatch(address,address,address,uint256[],uint256[])"),
            "topic2": ZERO_TOPIC,
            "topic0_2_opr": "and",
        },
        "expected_topic_count": 4,
    },
}

assert TARGETS["erc721"]["params"]["topic0"] == "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"


def canonical_json(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def intish(value: Any) -> int:
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        return int(value, 16) if value.startswith("0x") else int(value)
    raise ValueError(value)


def log_key(row: dict[str, Any]) -> tuple[str, int]:
    tx_hash = str(row.get("transactionHash") or row.get("transaction_hash") or "").lower()
    log_index = intish(row.get("logIndex") or row.get("log_index") or "0x0")
    return tx_hash, log_index


class Client:
    def __init__(self, delay_seconds: float = 1.35):
        self.delay_seconds = delay_seconds
        self.last_request = 0.0
        self.endpoint_cursor = 0
        self.http_stats: dict[str, int] = {}

    def pace(self) -> None:
        wait = self.delay_seconds - (time.monotonic() - self.last_request)
        if wait > 0:
            time.sleep(wait)

    def get(self, endpoint: str, params: dict[str, Any], attempts: int = 10) -> dict[str, Any]:
        url = endpoint + "?" + urllib.parse.urlencode(params)
        last_error: Exception | None = None
        for attempt in range(attempts):
            self.pace()
            request = urllib.request.Request(
                url,
                headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
            )
            try:
                with urllib.request.urlopen(request, timeout=90) as response:
                    body = response.read()
                    status = response.status
                self.last_request = time.monotonic()
                self.http_stats[f"http_{status}"] = self.http_stats.get(f"http_{status}", 0) + 1
                payload = json.loads(body.decode("utf-8"))
                if not isinstance(payload, dict):
                    raise RuntimeError(f"unexpected response type: {type(payload)}")
                return payload
            except urllib.error.HTTPError as exc:
                self.last_request = time.monotonic()
                self.http_stats[f"http_{exc.code}"] = self.http_stats.get(f"http_{exc.code}", 0) + 1
                last_error = exc
                if exc.code in (429, 500, 502, 503, 504):
                    time.sleep(min(180.0, 10.0 * (2 ** min(attempt, 4)) + random.random() * 5))
                    continue
                body = exc.read(1000).decode("utf-8", "replace")
                raise RuntimeError(f"HTTP {exc.code} {url}: {body}") from exc
            except Exception as exc:
                last_error = exc
                self.http_stats["network_or_decode_error"] = self.http_stats.get("network_or_decode_error", 0) + 1
                if attempt + 1 < attempts:
                    time.sleep(min(90.0, 5.0 * (2 ** min(attempt, 4)) + random.random() * 4))
                    continue
        raise RuntimeError(f"request attempts exhausted: {url}: {last_error!r}")

    def query(self, target: str, start: int, end: int) -> tuple[list[dict[str, Any]], str, str]:
        params = {
            "module": "logs",
            "action": "getLogs",
            "fromBlock": start,
            "toBlock": end,
            **TARGETS[target]["params"],
        }
        errors: list[str] = []
        ordered = ENDPOINTS[self.endpoint_cursor :] + ENDPOINTS[: self.endpoint_cursor]
        for endpoint in ordered:
            try:
                payload = self.get(endpoint, params)
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


def collect(target: str, start: int, end: int, out: Path) -> None:
    if target not in TARGETS:
        raise SystemExit(f"unknown target {target}")
    if start < 0 or end < start or end > FIXED_HEAD:
        raise SystemExit(f"invalid range {start}-{end}; fixed head={FIXED_HEAD}")

    out.mkdir(parents=True, exist_ok=True)
    client = Client()
    pending: list[tuple[int, int]] = [(start, end)]
    accepted: list[dict[str, Any]] = []
    request_log: list[dict[str, Any]] = []
    all_rows: dict[tuple[str, int], dict[str, Any]] = {}
    selected_rows: dict[tuple[str, int], dict[str, Any]] = {}
    single_block_caps: list[dict[str, Any]] = []

    while pending:
        left, right = pending.pop()
        rows, endpoint, message = client.query(target, left, right)
        request_log.append({
            "from_block": left,
            "to_block": right,
            "raw_rows": len(rows),
            "endpoint": endpoint,
            "message": message,
        })
        print(target, left, right, len(rows), endpoint, flush=True)

        # The public API truncates to 1000 rows. Split on raw result count,
        # including ERC-20 Transfer logs returned by the ERC-721 topic query.
        if len(rows) >= 1000:
            if left == right:
                single_block_caps.append({"block": left, "raw_rows": len(rows), "endpoint": endpoint})
                continue
            middle = (left + right) // 2
            pending.append((middle + 1, right))
            pending.append((left, middle))
            continue

        selected_in_range = 0
        for row in rows:
            block = intish(row.get("blockNumber") or row.get("block_number"))
            if block < left or block > right:
                raise RuntimeError(f"row block {block} outside {left}-{right}")
            topics = [str(value).lower() for value in row.get("topics") or []]
            if not topics or topics[0] != TARGETS[target]["params"]["topic0"]:
                raise RuntimeError(f"wrong topic0: {topics[:1]}")
            key = log_key(row)
            existing = all_rows.get(key)
            if existing is not None and canonical_json(existing) != canonical_json(row):
                raise RuntimeError(f"conflicting duplicate {key}")
            all_rows[key] = row

            if len(topics) != TARGETS[target]["expected_topic_count"]:
                # ERC-20 Transfer has three topics and is intentionally excluded.
                if target != "erc721" or len(topics) != 3:
                    raise RuntimeError(f"unexpected topic count for {target}: {len(topics)}")
                continue
            if target == "erc721" and topics[1] != ZERO_TOPIC:
                raise RuntimeError("ERC-721 zero-from topic mismatch")
            if target.startswith("erc1155") and topics[2] != ZERO_TOPIC:
                raise RuntimeError("ERC-1155 zero-from topic mismatch")
            selected_rows[key] = row
            selected_in_range += 1

        accepted.append({
            "from_block": left,
            "to_block": right,
            "raw_row_count": len(rows),
            "selected_nft_row_count": selected_in_range,
            "endpoint": endpoint,
        })

    accepted.sort(key=lambda row: row["from_block"])
    coverage_failures: list[dict[str, Any]] = []
    expected = start
    for item in accepted:
        if item["from_block"] != expected:
            coverage_failures.append({"expected_from": expected, "actual_from": item["from_block"]})
        expected = item["to_block"] + 1
    if expected != end + 1:
        coverage_failures.append({"expected_final": end + 1, "actual_final": expected})

    sorted_selected = sorted(
        selected_rows.values(),
        key=lambda row: (
            intish(row.get("blockNumber") or row.get("block_number")),
            intish(row.get("transactionIndex") or row.get("transaction_index") or "0x0"),
            intish(row.get("logIndex") or row.get("log_index") or "0x0"),
        ),
    )
    with gzip.open(out / "nft_mint_events.jsonl.gz", "wt", encoding="utf-8", newline="\n") as handle:
        for row in sorted_selected:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n")

    validation = {
        "status": "PASS" if not coverage_failures and not single_block_caps else "FAIL",
        "target": target,
        "fixed_head": FIXED_HEAD,
        "requested_from_block": start,
        "requested_to_block": end,
        "selected_nft_mint_rows": len(sorted_selected),
        "raw_event_rows_before_standard_filter": len(all_rows),
        "accepted_ranges": len(accepted),
        "request_count": len(request_log),
        "coverage_failures": coverage_failures,
        "single_block_caps": single_block_caps,
        "http_stats": client.http_stats,
        "first_nft_event_block": intish(sorted_selected[0]["blockNumber"]) if sorted_selected else None,
        "last_nft_event_block": intish(sorted_selected[-1]["blockNumber"]) if sorted_selected else None,
    }
    (out / "validation.json").write_text(json.dumps(validation, indent=2, sort_keys=True), encoding="utf-8")
    (out / "accepted_ranges.json").write_text(json.dumps(accepted, indent=2), encoding="utf-8")
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
