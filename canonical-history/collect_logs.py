#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import random
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

SOURCES = [
    "https://robinhoodchain.blockscout.com/api",
    "https://explorer.hoodmarketcap.com/api",
]
RPC = "https://rpc.mainnet.chain.robinhood.com"
TARGETS = {
    "seadrop": {
        "address": "0x00005ea00ac477b1030ce78506496e8c2de24bf5",
        "topic0": "0xe90cf9cc0a552cf52ea6ff74ece0f1c8ae8cc9ad630d3181f55ac43ca076b7d6",
        "event": "SeaDropMint",
    },
    "seaport": {
        "address": "0x0000000000000068f116a894984e2db1123eb395",
        "topic0": "0x9d9af8e38d66c62e2c12f0225249fd9d721c54b83f48d9352c97c6cacdcb6f31",
        "event": "OrderFulfilled",
    },
}
UA = "RHC-Canonical-History/1.0 read-only"
CAP = 1000


def request_json(url: str, *, data: bytes | None = None, attempts: int = 12) -> Any:
    last: Exception | None = None
    for attempt in range(attempts):
        try:
            headers = {"accept": "application/json", "user-agent": UA}
            if data is not None:
                headers["content-type"] = "application/json"
            req = urllib.request.Request(url, data=data, headers=headers)
            with urllib.request.urlopen(req, timeout=90) as response:
                body = response.read()
            return json.loads(body.decode("utf-8"))
        except urllib.error.HTTPError as exc:
            last = exc
            if exc.code in {408, 425, 429, 500, 502, 503, 504}:
                time.sleep(min(120, 2 ** min(attempt, 7) + random.random() * 6))
                continue
            raise
        except Exception as exc:
            last = exc
            if attempt + 1 < attempts:
                time.sleep(min(60, 2 ** min(attempt, 6) + random.random() * 4))
                continue
    raise RuntimeError(f"request failed: {url}: {last!r}")


def rpc(method: str, params: list[Any]) -> Any:
    payload = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode()
    result = request_json(RPC, data=payload)
    if not isinstance(result, dict) or result.get("error"):
        raise RuntimeError(f"RPC {method} failed: {result!r}")
    return result.get("result")


def normalize_log(row: dict[str, Any], source: str) -> dict[str, Any]:
    normalized = {
        "address": str(row.get("address") or "").lower(),
        "topics": [str(value).lower() for value in (row.get("topics") or [])],
        "data": str(row.get("data") or "0x").lower(),
        "blockNumber": str(row.get("blockNumber") or row.get("block_number") or "").lower(),
        "transactionHash": str(row.get("transactionHash") or row.get("transaction_hash") or "").lower(),
        "transactionIndex": str(row.get("transactionIndex") or row.get("transaction_index") or "").lower(),
        "blockHash": str(row.get("blockHash") or row.get("block_hash") or "").lower(),
        "logIndex": str(row.get("logIndex") or row.get("log_index") or "").lower(),
        "removed": bool(row.get("removed", False)),
        "collection_source": source,
    }
    return normalized


def key(row: dict[str, Any]) -> tuple[str, str, str]:
    return (row.get("blockHash", ""), row.get("transactionHash", ""), row.get("logIndex", ""))


def query_api(source: str, target: dict[str, str], start: int, end: int) -> list[dict[str, Any]]:
    params = {
        "module": "logs",
        "action": "getLogs",
        "fromBlock": start,
        "toBlock": end,
        "address": target["address"],
        "topic0": target["topic0"],
    }
    url = source + "?" + urllib.parse.urlencode(params)
    payload = request_json(url)
    if not isinstance(payload, dict):
        raise RuntimeError(f"unexpected API payload: {type(payload)!r}")
    result = payload.get("result")
    message = str(payload.get("message") or "")
    if isinstance(result, list):
        return [normalize_log(row, source) for row in result if isinstance(row, dict)]
    if str(payload.get("status")) == "0" and ("no" in message.lower() or result in (None, "")):
        return []
    raise RuntimeError(f"logs API failure: {source} {start}-{end}: {payload!r}")


def query_rpc_exact_block(target: dict[str, str], block: int) -> list[dict[str, Any]]:
    result = rpc("eth_getLogs", [{
        "fromBlock": hex(block),
        "toBlock": hex(block),
        "address": target["address"],
        "topics": [target["topic0"]],
    }])
    if not isinstance(result, list):
        raise RuntimeError(f"eth_getLogs exact block returned {type(result)!r}")
    return [normalize_log(row, "OFFICIAL_RPC_EXACT_BLOCK") for row in result if isinstance(row, dict)]


def query_with_fallback(target: dict[str, str], start: int, end: int) -> tuple[list[dict[str, Any]], str, list[str]]:
    errors: list[str] = []
    for source in SOURCES:
        try:
            return query_api(source, target, start, end), source, errors
        except Exception as exc:
            errors.append(f"{source}:{exc!r}")
    if start == end:
        try:
            return query_rpc_exact_block(target, start), "OFFICIAL_RPC_EXACT_BLOCK", errors
        except Exception as exc:
            errors.append(f"{RPC}:{exc!r}")
    raise RuntimeError(" | ".join(errors))


def collect_range(target: dict[str, str], shard_start: int, shard_end: int) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    stack = [(shard_start, shard_end)]
    accepted: list[dict[str, Any]] = []
    coverage: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    while stack:
        start, end = stack.pop()
        try:
            rows, source, fallback_errors = query_with_fallback(target, start, end)
        except Exception as exc:
            errors.append({"from_block": start, "to_block": end, "error": repr(exc)})
            continue
        count = len(rows)
        if count >= CAP and start < end:
            midpoint = (start + end) // 2
            stack.append((midpoint + 1, end))
            stack.append((start, midpoint))
            continue
        if count >= CAP and start == end and source != "OFFICIAL_RPC_EXACT_BLOCK":
            try:
                rows = query_rpc_exact_block(target, start)
                source = "OFFICIAL_RPC_EXACT_BLOCK"
                count = len(rows)
            except Exception as exc:
                errors.append({"from_block": start, "to_block": end, "error": repr(exc), "code": "SINGLE_BLOCK_CAP_UNRESOLVED"})
                continue
        for row in rows:
            row["coverage_from_block"] = start
            row["coverage_to_block"] = end
            accepted.append(row)
        coverage.append({
            "from_block": start,
            "to_block": end,
            "log_count": count,
            "source": source,
            "fallback_errors": fallback_errors,
            "saturated": count >= CAP,
            "status": "PASS" if count < CAP or source == "OFFICIAL_RPC_EXACT_BLOCK" else "FAIL",
        })
        time.sleep(0.45)
    return accepted, coverage, errors


def int_hex(value: str) -> int:
    return int(value, 16) if value.startswith("0x") else int(value)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", choices=sorted(TARGETS), required=True)
    parser.add_argument("--shard", type=int, required=True)
    parser.add_argument("--fixed-head", type=int, required=True)
    parser.add_argument("--fixed-head-hash", required=True)
    parser.add_argument("--shard-size", type=int, default=3_000_000)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    target = TARGETS[args.target]
    start = args.shard * args.shard_size
    end = min(args.fixed_head, start + args.shard_size - 1)
    if start > args.fixed_head:
        rows: list[dict[str, Any]] = []
        coverage = [{"from_block": start, "to_block": start - 1, "log_count": 0, "source": "OUTSIDE_FIXED_HEAD", "status": "PASS"}]
        errors: list[dict[str, Any]] = []
    else:
        rows, coverage, errors = collect_range(target, start, end)

    dedup: dict[tuple[str, str, str], dict[str, Any]] = {}
    duplicate_keys: list[tuple[str, str, str]] = []
    for row in rows:
        event_key = key(row)
        if event_key in dedup and dedup[event_key] != row:
            duplicate_keys.append(event_key)
        dedup[event_key] = row
    rows = sorted(dedup.values(), key=lambda row: (int_hex(row["blockNumber"]), int_hex(row["logIndex"])))
    coverage = sorted(coverage, key=lambda row: (row["from_block"], row["to_block"]))

    failures: list[dict[str, Any]] = list(errors)
    if start <= args.fixed_head:
        expected = start
        for leaf in coverage:
            if leaf["from_block"] != expected:
                failures.append({"code": "COVERAGE_GAP_OR_OVERLAP", "expected_from": expected, "actual_from": leaf["from_block"]})
            expected = leaf["to_block"] + 1
            if leaf.get("status") != "PASS":
                failures.append({"code": "LEAF_NOT_PASS", "leaf": leaf})
        if expected != end + 1:
            failures.append({"code": "COVERAGE_END_MISMATCH", "expected": end + 1, "actual": expected})
    if duplicate_keys:
        failures.append({"code": "CONFLICTING_DUPLICATE_LOG_KEYS", "count": len(duplicate_keys), "sample": duplicate_keys[:20]})
    for row in rows:
        if row.get("removed"):
            failures.append({"code": "REMOVED_LOG_IN_FINALIZED_RANGE", "key": key(row)})
        if not row.get("transactionHash") or not row.get("blockHash") or not row.get("logIndex"):
            failures.append({"code": "LOG_IDENTIFIER_MISSING", "row": row})
        if int_hex(row["blockNumber"]) > args.fixed_head:
            failures.append({"code": "LOG_AFTER_FIXED_HEAD", "row": row})

    with (out / "logs.jsonl").open("w", encoding="utf-8") as file:
        for row in rows:
            file.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
    with (out / "coverage.jsonl").open("w", encoding="utf-8") as file:
        for row in coverage:
            file.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
    (out / "errors.json").write_text(json.dumps(errors, ensure_ascii=False, indent=2), encoding="utf-8")
    report = {
        "status": "PASS" if not failures else "FAIL",
        "target": args.target,
        "event": target["event"],
        "shard": args.shard,
        "shard_start": start,
        "shard_end": end,
        "fixed_head": args.fixed_head,
        "fixed_head_hash": args.fixed_head_hash,
        "log_rows": len(rows),
        "coverage_leaf_rows": len(coverage),
        "failures": failures,
    }
    (out / "VALIDATION.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    manifest = []
    for path in sorted(out.iterdir()):
        if path.is_file():
            manifest.append({"path": path.name, "bytes": path.stat().st_size, "sha256": hashlib.sha256(path.read_bytes()).hexdigest()})
    (out / "MANIFEST.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, sort_keys=True), flush=True)
    if failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
