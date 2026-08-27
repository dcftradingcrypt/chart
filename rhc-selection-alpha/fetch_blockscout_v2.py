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
BASES = [
    "https://robinhoodchain.blockscout.com/api/v2",
    "https://explorer.hoodmarketcap.com/api/v2",
]
UA = "RHC-Selection-Alpha-BlockscoutV2/1.0 (read-only)"


def fetch_json(url: str, attempts: int = 12, timeout: int = 90) -> tuple[Any, str]:
    last: Exception | None = None
    for attempt in range(attempts):
        try:
            req = urllib.request.Request(url, headers={"accept": "application/json", "user-agent": UA})
            with urllib.request.urlopen(req, timeout=timeout) as response:
                return json.loads(response.read().decode("utf-8")), url
        except urllib.error.HTTPError as exc:
            last = exc
            if exc.code == 429 or exc.code >= 500:
                time.sleep(min(90, 3 * (2 ** min(attempt, 5)) + random.random() * 5))
                continue
            raise
        except Exception as exc:
            last = exc
            if attempt + 1 < attempts:
                time.sleep(min(30, 2 ** min(attempt, 4) + random.random() * 3))
                continue
    raise RuntimeError(f"GET failed: {url}: {last!r}")


def request_page(target: str, params: dict[str, Any]) -> tuple[dict[str, Any], str]:
    address = TARGETS[target]["address"]
    errors: list[str] = []
    for base in BASES:
        url = f"{base}/addresses/{address}/logs"
        if params:
            url += "?" + urllib.parse.urlencode(params, doseq=True)
        try:
            data, used = fetch_json(url)
        except Exception as exc:
            errors.append(f"{base}:{exc!r}")
            continue
        if isinstance(data, dict) and isinstance(data.get("items"), list):
            return data, used
        errors.append(f"{base}:invalid:{repr(data)[:500]}")
    raise RuntimeError(" | ".join(errors))


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
    topics = row.get("topics") or []
    if isinstance(topics, str):
        try:
            topics = json.loads(topics)
        except Exception:
            topics = [topics]
    topics = [str(value).lower() for value in topics]
    contract = row.get("smart_contract") or {}
    tx_hash = row.get("transaction_hash") or row.get("transactionHash")
    return {
        "chain_id": CHAIN_ID,
        "target": target,
        "address": str((contract.get("hash") if isinstance(contract, dict) else None) or TARGETS[target]["address"]).lower(),
        "block_number": integer(row.get("block_number") or row.get("blockNumber")),
        "block_timestamp": row.get("block_timestamp") or row.get("timestamp"),
        "transaction_hash": str(tx_hash or "").lower(),
        "log_index": integer(row.get("index") or row.get("log_index") or row.get("logIndex")),
        "data": str(row.get("data") or "0x").lower(),
        "topics": topics,
        "topic0": topics[0] if topics else None,
        "decoded": row.get("decoded"),
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
    params: dict[str, Any] = {}
    seen_cursors: set[str] = set()
    all_matching: list[dict[str, Any]] = []
    pages: list[dict[str, Any]] = []
    raw_count = 0
    page_number = 0

    while True:
        page_number += 1
        data, url = request_page(target, params)
        items = [item for item in data.get("items", []) if isinstance(item, dict)]
        raw_count += len(items)
        normalized = [normalize(item, target) for item in items]
        matching = [row for row in normalized if row["topic0"] == TARGETS[target]["topic0"]]
        all_matching.extend(matching)
        next_params = data.get("next_page_params")
        pages.append({
            "page": page_number,
            "raw_rows": len(items),
            "matching_rows": len(matching),
            "first_block": normalized[0]["block_number"] if normalized else None,
            "last_block": normalized[-1]["block_number"] if normalized else None,
            "url": url,
            "has_next": bool(next_params),
        })
        if page_number % 25 == 0 or not next_params:
            print(target, "page", page_number, "raw", raw_count, "matching", len(all_matching), "next", bool(next_params), flush=True)
        if not next_params:
            break
        if not isinstance(next_params, dict):
            raise RuntimeError(f"invalid next_page_params: {next_params!r}")
        key = json.dumps(next_params, sort_keys=True, separators=(",", ":"))
        if key in seen_cursors:
            raise RuntimeError(f"repeated next_page_params: {key}")
        seen_cursors.add(key)
        params = next_params
        time.sleep(0.22)

    unique: dict[tuple[str, int], dict[str, Any]] = {}
    for row in all_matching:
        key = (row["transaction_hash"], row["log_index"] if row["log_index"] is not None else -1)
        unique[key] = row
    logs = sorted(unique.values(), key=lambda row: (row["block_number"] or -1, row["log_index"] or -1))
    with (out / "logs.jsonl").open("w", encoding="utf-8") as handle:
        for row in logs:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
    fields = ["chain_id", "target", "address", "block_number", "block_timestamp", "transaction_hash", "log_index", "data", "topics", "topic0", "decoded"]
    with (out / "logs.csv").open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in logs:
            writer.writerow({field: json.dumps(row[field], ensure_ascii=False, sort_keys=True) if isinstance(row[field], (list, dict)) else row[field] for field in fields})
    with (out / "pages.csv").open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(pages[0]) if pages else ["page"])
        writer.writeheader()
        writer.writerows(pages)
    wrong_topic = sum(row["topic0"] != TARGETS[target]["topic0"] for row in logs)
    validation = {
        "status": "PASS" if not wrong_topic and page_number > 0 else "FAIL",
        "chain_id": CHAIN_ID,
        "target": target,
        "address": TARGETS[target]["address"],
        "all_address_log_rows": raw_count,
        "matching_topic_rows": len(logs),
        "duplicates_removed": len(all_matching) - len(logs),
        "page_count": page_number,
        "wrong_topic_rows": wrong_topic,
        "pagination_complete": True,
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
    args = parser.parse_args()
    collect(args.target, args.out)


if __name__ == "__main__":
    main()
