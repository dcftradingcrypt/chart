#!/usr/bin/env python3
from __future__ import annotations

import argparse
import gzip
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

RPC_URL = "https://rpc.mainnet.chain.robinhood.com"
EXPLORERS = (
    "https://robinhoodchain.blockscout.com/api",
    "https://explorer.hoodmarketcap.com/api",
)
EVENTS = {
    "seadrop": {
        "topic0": "0xe90cf9cc0a552cf52ea6ff74ece0f1c8ae8cc9ad630d3181f55ac43ca076b7d6",
        "name": "SeaDropMint",
    },
    "seaport": {
        "topic0": "0x9d9af8e38d66c62e2c12f0225249fd9d721c54b83f48d9352c97c6cacdcb6f31",
        "name": "OrderFulfilled",
    },
}
UA = "RHC-Canonical-History/1.0"


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def normalized_log(row: dict[str, Any], source: str) -> dict[str, Any]:
    return {
        "address": str(row.get("address") or "").lower(),
        "topics": [str(value).lower() for value in (row.get("topics") or [])],
        "data": str(row.get("data") or "0x").lower(),
        "blockNumber": str(row.get("blockNumber") or row.get("block_number") or "").lower(),
        "transactionHash": str(row.get("transactionHash") or row.get("transaction_hash") or "").lower(),
        "transactionIndex": str(row.get("transactionIndex") or row.get("transaction_index") or "").lower(),
        "blockHash": str(row.get("blockHash") or row.get("block_hash") or "").lower(),
        "logIndex": str(row.get("logIndex") or row.get("log_index") or "").lower(),
        "removed": bool(row.get("removed", False)),
        "source": source,
    }


def post_json(url: str, payload: dict[str, Any], attempts: int = 7) -> dict[str, Any]:
    body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    last: Exception | None = None
    for attempt in range(attempts):
        time.sleep(0.12 + random.random() * 0.22)
        request = urllib.request.Request(
            url,
            data=body,
            method="POST",
            headers={"Content-Type": "application/json", "Accept": "application/json", "User-Agent": UA},
        )
        try:
            with urllib.request.urlopen(request, timeout=75) as response:
                data = json.loads(response.read().decode("utf-8"))
            if isinstance(data, dict) and data.get("error"):
                raise RuntimeError(json.dumps(data["error"], sort_keys=True))
            return data
        except urllib.error.HTTPError as exc:
            last = exc
            if exc.code not in (408, 425, 429, 500, 502, 503, 504):
                raise
        except Exception as exc:  # noqa: BLE001
            last = exc
        time.sleep(min(60.0, (2 ** min(attempt, 5)) + random.random() * 4))
    raise RuntimeError(f"RPC failed after {attempts} attempts: {last}")


def get_json(url: str, attempts: int = 6) -> dict[str, Any]:
    last: Exception | None = None
    for attempt in range(attempts):
        time.sleep(0.18 + random.random() * 0.35)
        request = urllib.request.Request(url, headers={"Accept": "application/json", "User-Agent": UA})
        try:
            with urllib.request.urlopen(request, timeout=75) as response:
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            last = exc
            if exc.code not in (408, 425, 429, 500, 502, 503, 504):
                raise
        except Exception as exc:  # noqa: BLE001
            last = exc
        time.sleep(min(60.0, (2 ** min(attempt, 5)) + random.random() * 4))
    raise RuntimeError(f"GET failed after {attempts} attempts: {url}: {last}")


def rpc_logs(topic0: str, start: int, end: int) -> list[dict[str, Any]]:
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_getLogs",
        "params": [{"fromBlock": hex(start), "toBlock": hex(end), "topics": [topic0]}],
    }
    data = post_json(RPC_URL, payload)
    result = data.get("result")
    if not isinstance(result, list):
        raise RuntimeError(f"invalid RPC result type: {type(result)}")
    return [normalized_log(row, "ROBINHOOD_OFFICIAL_RPC") for row in result if isinstance(row, dict)]


def explorer_logs(base: str, topic0: str, start: int, end: int) -> tuple[list[dict[str, Any]], bool]:
    query = urllib.parse.urlencode(
        {
            "module": "logs",
            "action": "getLogs",
            "fromBlock": start,
            "toBlock": end,
            "topic0": topic0,
        }
    )
    data = get_json(f"{base}?{query}")
    result = data.get("result")
    if not isinstance(result, list):
        message = data.get("message") or data.get("result")
        if str(message).lower() in {"no records found", "no logs found"}:
            return [], False
        raise RuntimeError(f"invalid explorer result: {data}")
    rows = [normalized_log(row, base) for row in result if isinstance(row, dict)]
    return rows, len(rows) >= 1000


def collect_range(
    topic0: str,
    start: int,
    end: int,
    rows: dict[tuple[str, str, str], dict[str, Any]],
    coverage: list[dict[str, Any]],
    unresolved: list[dict[str, Any]],
    depth: int = 0,
) -> None:
    if start > end:
        return
    errors: list[str] = []
    try:
        found = rpc_logs(topic0, start, end)
        for row in found:
            key = (row["blockHash"], row["transactionHash"], row["logIndex"])
            rows[key] = row
        coverage.append({"start": start, "end": end, "source": "ROBINHOOD_OFFICIAL_RPC", "rows": len(found), "depth": depth})
        return
    except Exception as exc:  # noqa: BLE001
        errors.append(f"rpc:{exc!r}")

    for explorer in EXPLORERS:
        try:
            found, capped = explorer_logs(explorer, topic0, start, end)
            if capped and start < end:
                errors.append(f"{explorer}:CAP_1000")
                break
            for row in found:
                key = (row["blockHash"], row["transactionHash"], row["logIndex"])
                rows[key] = row
            coverage.append({"start": start, "end": end, "source": explorer, "rows": len(found), "depth": depth})
            return
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{explorer}:{exc!r}")

    if start == end:
        unresolved.append({"start": start, "end": end, "errors": errors, "depth": depth})
        return
    middle = (start + end) // 2
    collect_range(topic0, start, middle, rows, coverage, unresolved, depth + 1)
    collect_range(topic0, middle + 1, end, rows, coverage, unresolved, depth + 1)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--event", choices=sorted(EVENTS), required=True)
    parser.add_argument("--shard", type=int, required=True)
    parser.add_argument("--shards", type=int, default=64)
    parser.add_argument("--head", type=int, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()

    if not (0 <= args.shard < args.shards):
        raise SystemExit("invalid shard")
    args.out.mkdir(parents=True, exist_ok=True)
    start = ((args.head + 1) * args.shard) // args.shards
    end = (((args.head + 1) * (args.shard + 1)) // args.shards) - 1
    event = EVENTS[args.event]

    rows: dict[tuple[str, str, str], dict[str, Any]] = {}
    coverage: list[dict[str, Any]] = []
    unresolved: list[dict[str, Any]] = []
    collect_range(event["topic0"], start, end, rows, coverage, unresolved)

    ordered = sorted(
        rows.values(),
        key=lambda row: (
            int(row["blockNumber"], 16) if row["blockNumber"].startswith("0x") else 0,
            int(row["transactionIndex"], 16) if row["transactionIndex"].startswith("0x") else 0,
            int(row["logIndex"], 16) if row["logIndex"].startswith("0x") else 0,
        ),
    )
    log_path = args.out / "logs.jsonl.gz"
    with gzip.open(log_path, "wt", encoding="utf-8", newline="\n") as handle:
        for row in ordered:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")

    coverage_record = {
        "event": args.event,
        "event_name": event["name"],
        "topic0": event["topic0"],
        "shard": args.shard,
        "shards": args.shards,
        "head": args.head,
        "start": start,
        "end": end,
        "resolved_ranges": sorted(coverage, key=lambda row: (row["start"], row["end"])),
        "unresolved_ranges": unresolved,
        "unique_logs": len(ordered),
    }
    (args.out / "coverage.json").write_text(json.dumps(coverage_record, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    validation = {
        "status": "PASS" if not unresolved else "FAIL",
        "event": args.event,
        "shard": args.shard,
        "range_start": start,
        "range_end": end,
        "unique_logs": len(ordered),
        "unresolved_range_count": len(unresolved),
        "removed_log_count": sum(bool(row.get("removed")) for row in ordered),
        "production_approved_wallets": 0,
        "decision_use": "CANONICAL_HISTORY_ONLY",
    }
    (args.out / "validation.json").write_text(json.dumps(validation, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    manifest = []
    for path in sorted(args.out.iterdir()):
        if path.is_file():
            manifest.append({"path": path.name, "bytes": path.stat().st_size, "sha256": sha256(path)})
    (args.out / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(validation, sort_keys=True), flush=True)
    if unresolved:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
