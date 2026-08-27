#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import random
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

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
UA = "DCF-RHC-Sequential-Canonical-Backfill/1.0 (read-only)"
MIN_INTERVAL_SECONDS = float(os.getenv("RHC_MIN_INTERVAL_SECONDS", "16"))
MAX_ATTEMPTS = int(os.getenv("RHC_MAX_ATTEMPTS", "40"))
PAGE_CAP = 1000
last_request_at = 0.0


def now_utc() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


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


def pace() -> None:
    global last_request_at
    wait = MIN_INTERVAL_SECONDS - (time.monotonic() - last_request_at)
    if wait > 0:
        time.sleep(wait)


def request_json(url: str, *, payload: Any = None, timeout: int = 90) -> tuple[int, Any]:
    global last_request_at
    body = None
    headers = {"accept": "application/json", "user-agent": UA}
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        headers["content-type"] = "application/json"
    last_error: Exception | None = None
    for attempt in range(1, MAX_ATTEMPTS + 1):
        pace()
        request = urllib.request.Request(url, data=body, headers=headers, method="POST" if body else "GET")
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                raw = response.read()
                status = response.status
            last_request_at = time.monotonic()
            return status, json.loads(raw.decode("utf-8"))
        except urllib.error.HTTPError as exc:
            last_request_at = time.monotonic()
            last_error = exc
            if exc.code in (429, 408, 425, 500, 502, 503, 504):
                delay = min(300.0, 45.0 + (attempt - 1) * 15.0 + random.random() * 10.0)
                print(json.dumps({"event": "retry_http", "attempt": attempt, "code": exc.code, "delay": delay, "url": url}), flush=True)
                time.sleep(delay)
                continue
            snippet = exc.read(1000).decode("utf-8", "replace")
            raise RuntimeError(f"HTTP {exc.code} {url}: {snippet}") from exc
        except Exception as exc:
            last_request_at = time.monotonic()
            last_error = exc
            delay = min(180.0, 10.0 * attempt + random.random() * 5.0)
            print(json.dumps({"event": "retry_exception", "attempt": attempt, "delay": delay, "error": repr(exc), "url": url}), flush=True)
            time.sleep(delay)
    raise RuntimeError(f"request failed after {MAX_ATTEMPTS} attempts: {url}: {last_error!r}")


def rpc(method: str, params: list[Any]) -> Any:
    errors: list[str] = []
    for url in RPCS:
        try:
            status, data = request_json(url, payload={"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
            if status == 200 and isinstance(data, dict) and "result" in data:
                return data["result"]
            errors.append(f"{url}:{data}")
        except Exception as exc:
            errors.append(f"{url}:{exc!r}")
    raise RuntimeError("RPC unavailable: " + " | ".join(errors))


def get_head_block() -> int:
    return int(rpc("eth_blockNumber", []), 16)


def normalize(row: dict[str, Any], target: str) -> dict[str, Any]:
    topics = row.get("topics") or []
    if isinstance(topics, str):
        try:
            topics = json.loads(topics)
        except Exception:
            topics = [topics]
    topics = [str(x).lower() if x is not None else None for x in topics]
    return {
        "target": target,
        "address": str(row.get("address") or "").lower(),
        "block_number": integer(row.get("blockNumber"), integer(row.get("block_number"))),
        "transaction_hash": str(row.get("transactionHash") or row.get("transaction_hash") or "").lower(),
        "transaction_index": integer(row.get("transactionIndex"), integer(row.get("transaction_index"))),
        "log_index": integer(row.get("logIndex"), integer(row.get("log_index"))),
        "block_hash": str(row.get("blockHash") or row.get("block_hash") or "").lower() or None,
        "timestamp_unix": integer(row.get("timeStamp"), integer(row.get("timestamp"))),
        "gas_price": integer(row.get("gasPrice"), integer(row.get("gas_price"))),
        "gas_used": integer(row.get("gasUsed"), integer(row.get("gas_used"))),
        "topics": topics,
        "data": str(row.get("data") or "0x").lower(),
        "raw": row,
    }


def key(row: dict[str, Any]) -> tuple[str, int]:
    return row["transaction_hash"], row["log_index"] if row["log_index"] is not None else -1


def explorer_page(target: str, from_block: int, to_block: int) -> tuple[list[dict[str, Any]], str, str]:
    spec = TARGETS[target]
    params = {
        "module": "logs",
        "action": "getLogs",
        "fromBlock": from_block,
        "toBlock": to_block,
        "address": spec["address"],
        "topic0": spec["topic0"],
    }
    errors: list[str] = []
    for base in EXPLORERS:
        url = base + "?" + urllib.parse.urlencode(params)
        try:
            status, data = request_json(url)
            if status != 200 or not isinstance(data, dict):
                errors.append(f"{base}:HTTP {status}")
                continue
            result = data.get("result")
            message = str(data.get("message") or "")
            if isinstance(result, list):
                return [normalize(x, target) for x in result if isinstance(x, dict)], base, message
            text = (message + " " + str(result)).lower()
            if any(token in text for token in ("no logs", "no records", "not found")):
                return [], base, text
            errors.append(f"{base}:{text[:500]}")
        except Exception as exc:
            errors.append(f"{base}:{exc!r}")
    raise RuntimeError(" | ".join(errors))


def rpc_exact_block(target: str, block_number: int) -> list[dict[str, Any]]:
    spec = TARGETS[target]
    result = rpc("eth_getLogs", [{
        "fromBlock": hex(block_number),
        "toBlock": hex(block_number),
        "address": spec["address"],
        "topics": [spec["topic0"]],
    }])
    return [normalize(x, target) for x in (result or []) if isinstance(x, dict)]


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_state(out: Path, target: str, rows_by_key: dict[tuple[str, int], dict[str, Any]], pages: list[dict[str, Any]], checkpoint: dict[str, Any], errors: list[dict[str, Any]]) -> None:
    out.mkdir(parents=True, exist_ok=True)
    rows = sorted(rows_by_key.values(), key=lambda r: (r["block_number"] or -1, r["transaction_index"] or -1, r["log_index"] or -1))
    with (out / "events.jsonl").open("w", encoding="utf-8", newline="\n") as file:
        for row in rows:
            file.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
    fields = ["target", "address", "block_number", "transaction_hash", "transaction_index", "log_index", "block_hash", "timestamp_unix", "gas_price", "gas_used", "topics", "data"]
    with (out / "events.csv").open("w", encoding="utf-8-sig", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({name: json.dumps(row[name], ensure_ascii=False) if isinstance(row[name], list) else row[name] for name in fields})
    for name, data in (("pages.csv", pages), ("errors.csv", errors)):
        fields2 = sorted({k for r in data for k in r}) if data else ["empty"]
        with (out / name).open("w", encoding="utf-8-sig", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=fields2, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(data)
    (out / "checkpoint.json").write_text(json.dumps(checkpoint, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def collect(target: str, out: Path) -> None:
    spec = TARGETS[target]
    head = get_head_block()
    cursor = 0
    rows_by_key: dict[tuple[str, int], dict[str, Any]] = {}
    pages: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    complete = False
    page_number = 0
    previous_signature: tuple[int, int, str] | None = None

    checkpoint = {
        "target": target,
        "started_at_utc": now_utc(),
        "fixed_head_block": head,
        "cursor": cursor,
        "complete": False,
        "event_rows": 0,
    }
    write_state(out, target, rows_by_key, pages, checkpoint, errors)

    while cursor <= head:
        page_number += 1
        try:
            rows, source, message = explorer_page(target, cursor, head)
            if not rows:
                complete = True
                pages.append({"page": page_number, "from_block": cursor, "to_block": head, "source": source, "returned_rows": 0, "new_rows": 0, "status": "COMPLETE_EMPTY", "message": message})
                break

            wrong = [r for r in rows if r["address"] != spec["address"] or not r["topics"] or r["topics"][0] != spec["topic0"]]
            if wrong:
                raise RuntimeError(f"wrong address/topic rows: {len(wrong)}")
            block_numbers = [r["block_number"] for r in rows if r["block_number"] is not None]
            if not block_numbers:
                raise RuntimeError("page has no block numbers")
            last_block = max(block_numbers)
            before = len(rows_by_key)
            for row in rows:
                rows_by_key[key(row)] = row
            new_rows = len(rows_by_key) - before
            signature = (cursor, last_block, rows[-1]["transaction_hash"])
            status = "PAGE"

            if len(rows) < PAGE_CAP:
                complete = True
                status = "COMPLETE_SHORT_PAGE"
            elif last_block < cursor:
                raise RuntimeError(f"non-progressing block range: cursor={cursor}, last={last_block}")
            elif last_block == cursor and (new_rows == 0 or previous_signature == signature):
                exact_rows = rpc_exact_block(target, cursor)
                for row in exact_rows:
                    rows_by_key[key(row)] = row
                status = "RPC_EXACT_BLOCK_ESCAPE"
                cursor += 1
            else:
                cursor = last_block

            pages.append({
                "page": page_number,
                "from_block": signature[0],
                "to_block": head,
                "source": source,
                "returned_rows": len(rows),
                "new_rows": new_rows,
                "last_block": last_block,
                "next_cursor": cursor,
                "status": status,
                "message": message,
            })
            previous_signature = signature
            checkpoint.update({"updated_at_utc": now_utc(), "cursor": cursor, "complete": complete, "event_rows": len(rows_by_key), "pages": page_number})
            write_state(out, target, rows_by_key, pages, checkpoint, errors)
            print(json.dumps({"target": target, "page": page_number, "cursor": cursor, "returned": len(rows), "new": new_rows, "events": len(rows_by_key), "complete": complete}), flush=True)
            if complete:
                break
        except Exception as exc:
            error = {"page": page_number, "cursor": cursor, "error": repr(exc), "at_utc": now_utc()}
            errors.append(error)
            checkpoint.update({"updated_at_utc": now_utc(), "cursor": cursor, "complete": False, "event_rows": len(rows_by_key), "pages": page_number, "last_error": repr(exc)})
            write_state(out, target, rows_by_key, pages, checkpoint, errors)
            raise

    rows = list(rows_by_key.values())
    duplicate_count = sum(p.get("returned_rows", 0) for p in pages) - len(rows)
    wrong_topic = sum(1 for r in rows if not r["topics"] or r["topics"][0] != spec["topic0"])
    wrong_address = sum(1 for r in rows if r["address"] != spec["address"])
    out_of_range = sum(1 for r in rows if r["block_number"] is None or r["block_number"] < 0 or r["block_number"] > head)
    validation = {
        "status": "PASS" if complete and not errors and not wrong_topic and not wrong_address and not out_of_range else "FAIL",
        "target": target,
        "fixed_head_block": head,
        "complete": complete,
        "event_rows": len(rows),
        "pages": len(pages),
        "duplicates_removed": max(0, duplicate_count),
        "wrong_topic_rows": wrong_topic,
        "wrong_address_rows": wrong_address,
        "out_of_range_rows": out_of_range,
        "error_count": len(errors),
        "generated_at_utc": now_utc(),
    }
    (out / "validation.json").write_text(json.dumps(validation, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    manifest = []
    for path in sorted(out.iterdir()):
        if path.is_file() and path.name != "manifest.json":
            manifest.append({"path": path.name, "bytes": path.stat().st_size, "sha256": sha256(path)})
    (out / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(validation, sort_keys=True), flush=True)
    if validation["status"] != "PASS":
        raise SystemExit(2)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", choices=TARGETS, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    collect(args.target, args.out)


if __name__ == "__main__":
    main()
