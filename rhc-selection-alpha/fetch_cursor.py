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
UA = "RHC-Selection-Alpha-Cursor/1.0 (read-only)"


def fetch_json(url: str, *, payload: dict[str, Any] | None = None, attempts: int = 7, timeout: int = 120) -> Any:
    body = json.dumps(payload).encode() if payload is not None else None
    headers = {"accept": "application/json", "user-agent": UA}
    if body is not None:
        headers["content-type"] = "application/json"
    last: Exception | None = None
    for attempt in range(attempts):
        try:
            req = urllib.request.Request(url, data=body, headers=headers, method="POST" if body else "GET")
            with urllib.request.urlopen(req, timeout=timeout) as response:
                raw = response.read()
            return json.loads(raw.decode("utf-8"))
        except urllib.error.HTTPError as exc:
            last = exc
            if exc.code == 429 or exc.code >= 500:
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
            data = fetch_json(rpc, payload={"jsonrpc": "2.0", "id": 1, "method": "eth_blockNumber", "params": []}, timeout=30)
            value = data.get("result") if isinstance(data, dict) else None
            if isinstance(value, str) and value.startswith("0x"):
                return int(value, 16), rpc
        except Exception as exc:
            print("head probe failed", rpc, repr(exc), flush=True)
    raise SystemExit("no Robinhood Chain RPC returned eth_blockNumber")


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


def query(target: str, start: int, stop: int | str) -> tuple[list[dict[str, Any]], str, str]:
    config = TARGETS[target]
    params = {
        "module": "logs",
        "action": "getLogs",
        "fromBlock": start,
        "toBlock": stop,
        "address": config["address"],
        "topic0": config["topic0"],
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
            return [row for row in result if isinstance(row, dict)], base, message
        text = (message + " " + str(result)).lower()
        if any(token in text for token in ("no logs", "no records", "not found")):
            return [], base, text[:300]
        errors.append(f"{base}:{text[:300]}")
    raise RuntimeError(" | ".join(errors))


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


def collect(target: str, out: Path) -> None:
    out.mkdir(parents=True, exist_ok=True)
    head, rpc = latest_block()
    cursor = 0
    raw_rows: list[dict[str, Any]] = []
    pages: list[dict[str, Any]] = []
    while cursor <= head:
        rows, source, message = query(target, cursor, head)
        if not rows:
            pages.append({"from_block": cursor, "to_block": head, "returned": 0, "source": source, "message": message, "status": "TAIL_EMPTY_COMPLETE"})
            break
        normalized = [normalize(row, target) for row in rows]
        blocks = [row["block_number"] for row in normalized if row["block_number"] is not None]
        if not blocks:
            raise RuntimeError("page contained no parseable block numbers")
        last_block = max(blocks)
        # Replace all rows from the final page block with a dedicated single-block query.
        # This prevents losing additional logs when the 1000-row cap cuts inside a block.
        block_rows, block_source, block_message = query(target, last_block, last_block)
        if len(block_rows) >= 1000:
            raise RuntimeError(f"single block {last_block} reached the 1000-row cap")
        before_last = [row for row in normalized if row["block_number"] != last_block]
        exact_last = [normalize(row, target) for row in block_rows]
        raw_rows.extend(before_last)
        raw_rows.extend(exact_last)
        pages.append({
            "from_block": cursor,
            "to_block": head,
            "returned": len(rows),
            "last_block": last_block,
            "last_block_rows_from_page": sum(row["block_number"] == last_block for row in normalized),
            "last_block_exact_rows": len(exact_last),
            "source": source,
            "last_block_source": block_source,
            "message": message,
            "last_block_message": block_message,
            "status": "CAP_PAGE" if len(rows) >= 1000 else "FINAL_PAGE",
        })
        print(target, "cursor", cursor, "returned", len(rows), "last_block", last_block, "exact_last", len(exact_last), flush=True)
        if len(rows) < 1000:
            cursor = head + 1
            break
        cursor = last_block + 1

    unique: dict[tuple[str, int], dict[str, Any]] = {}
    for row in raw_rows:
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
    with (out / "pages.csv").open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=sorted({key for row in pages for key in row}))
        writer.writeheader()
        writer.writerows(pages)
    wrong_topic = sum(row["topic0"] != TARGETS[target]["topic0"] for row in logs)
    order_errors = sum(logs[index]["block_number"] > logs[index + 1]["block_number"] for index in range(max(0, len(logs) - 1)))
    validation = {
        "status": "PASS" if not wrong_topic and not order_errors and cursor > head else "FAIL",
        "chain_id": CHAIN_ID,
        "target": target,
        "head_block": head,
        "head_rpc": rpc,
        "row_count": len(logs),
        "duplicates_removed": len(raw_rows) - len(logs),
        "page_count": len(pages),
        "wrong_topic_rows": wrong_topic,
        "block_order_errors": order_errors,
        "completed_through_head": cursor > head,
        "first_block": logs[0]["block_number"] if logs else None,
        "last_block": logs[-1]["block_number"] if logs else None,
    }
    (out / "validation.json").write_text(json.dumps(validation, indent=2, sort_keys=True), encoding="utf-8")
    (out / "head.json").write_text(json.dumps({"chain_id": CHAIN_ID, "head_block": head, "rpc": rpc}, indent=2), encoding="utf-8")
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
    args = parser.parse_args()
    collect(args.target, args.out)


if __name__ == "__main__":
    main()
