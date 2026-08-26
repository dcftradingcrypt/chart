#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import os
import random
import time
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

OUT = Path(os.getenv("OUT", "out-global-blockscout"))
OUT.mkdir(parents=True, exist_ok=True)
BLOCKSCOUT = "https://robinhoodchain.blockscout.com/api"
RPC = "https://rpc.mainnet.chain.robinhood.com/rpc"
SEADROP = "0x00005ea00ac477b1030ce78506496e8c2de24bf5"
TOPIC0 = "0xe90cf9cc0a552cf52ea6ff74ece0f1c8ae8cc9ad630d3181f55ac43ca076b7d6"
UA = "RHC-SeaDrop-Global-Research/0.1"


def now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def get_json(url: str, attempts: int = 8) -> Any:
    last = None
    for i in range(attempts):
        try:
            req = urllib.request.Request(url, headers={"user-agent": UA, "accept": "application/json"})
            with urllib.request.urlopen(req, timeout=120) as r:
                raw = r.read()
            time.sleep(0.22)
            return json.loads(raw.decode())
        except Exception as e:
            last = e
            if i + 1 == attempts:
                break
            time.sleep(min(30, 2**i + random.random()))
    raise RuntimeError(f"{url}: {last}")


def rpc(method: str, params: list[Any]) -> Any:
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode()
    req = urllib.request.Request(RPC, data=body, headers={"content-type": "application/json", "user-agent": UA})
    with urllib.request.urlopen(req, timeout=90) as r:
        data = json.loads(r.read().decode())
    if "error" in data:
        raise RuntimeError(data["error"])
    return data["result"]


def words(data: str) -> list[int]:
    s = data[2:] if data.startswith("0x") else data
    return [int(s[i : i + 64], 16) for i in range(0, len(s), 64) if len(s[i : i + 64]) == 64]


def to_addr(topic: str) -> str:
    return "0x" + topic[-40:].lower()


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fields = sorted({k for row in rows for k in row}) if rows else ["empty"]
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({k: json.dumps(v, ensure_ascii=False, sort_keys=True) if isinstance(v, (list, dict)) else v for k, v in row.items()})


calls = 0
ranges: list[dict[str, Any]] = []


def fetch_range(start: int, end: int, depth: int = 0) -> list[dict[str, Any]]:
    global calls
    calls += 1
    params = {
        "module": "logs",
        "action": "getLogs",
        "fromBlock": start,
        "toBlock": end,
        "address": SEADROP,
        "topic0": TOPIC0,
    }
    data = get_json(BLOCKSCOUT + "?" + urllib.parse.urlencode(params))
    result = data.get("result") if isinstance(data, dict) else None
    if isinstance(result, str):
        if "No logs found" in result or data.get("status") == "0":
            ranges.append({"from_block": start, "to_block": end, "depth": depth, "rows": 0, "action": "ACCEPT_EMPTY"})
            return []
        raise RuntimeError(result)
    result = result or []
    if len(result) < 1000:
        ranges.append({"from_block": start, "to_block": end, "depth": depth, "rows": len(result), "action": "ACCEPT"})
        return result
    if start >= end:
        raise RuntimeError(f"Block {start} alone returned 1000 logs; provider truncation cannot be resolved")
    middle = (start + end) // 2
    ranges.append({"from_block": start, "to_block": end, "depth": depth, "rows": len(result), "action": "SPLIT"})
    return fetch_range(start, middle, depth + 1) + fetch_range(middle + 1, end, depth + 1)


def main() -> None:
    latest = int(rpc("eth_blockNumber", []), 16)
    raw = fetch_range(0, latest)
    dedup: dict[tuple[str, str], dict[str, Any]] = {}
    for log in raw:
        dedup[(str(log.get("transactionHash")).lower(), str(log.get("logIndex")))] = log
    raw = sorted(dedup.values(), key=lambda x: (int(str(x.get("blockNumber", "0x0")), 16), int(str(x.get("logIndex", "0x0")), 16)))

    events: list[dict[str, Any]] = []
    for log in raw:
        decoded = words(log.get("data", "0x"))
        topics = log.get("topics") or []
        if len(topics) < 4 or len(decoded) < 5:
            continue
        timestamp = log.get("timeStamp")
        timestamp = int(timestamp, 16) if isinstance(timestamp, str) and timestamp.startswith("0x") else None
        events.append(
            {
                "transaction_hash": str(log.get("transactionHash")).lower(),
                "log_index": int(str(log.get("logIndex", "0x0")), 16),
                "block_number": int(str(log.get("blockNumber", "0x0")), 16),
                "timestamp_utc": datetime.fromtimestamp(timestamp, timezone.utc).isoformat().replace("+00:00", "Z") if timestamp else None,
                "nft_contract": to_addr(topics[1]),
                "minter": to_addr(topics[2]),
                "fee_recipient": to_addr(topics[3]),
                "payer": "0x" + decoded[0].to_bytes(32, "big")[-20:].hex(),
                "quantity": decoded[1],
                "unit_mint_price_wei": decoded[2],
                "unit_mint_price_eth": decoded[2] / 1e18,
                "gross_mint_value_wei": decoded[1] * decoded[2],
                "gross_mint_value_eth": decoded[1] * decoded[2] / 1e18,
                "fee_bps": decoded[3],
                "drop_stage_index": decoded[4],
                "is_free": decoded[2] == 0,
                "is_paid": decoded[2] > 0,
                "source": "BLOCKSCOUT_CANONICAL_SEADROP_GLOBAL",
            }
        )

    by_contract: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for event in events:
        by_contract[event["nft_contract"]].append(event)

    collections: list[dict[str, Any]] = []
    wallet_projects: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for contract, rows in sorted(by_contract.items(), key=lambda kv: min(x["block_number"] for x in kv[1])):
        rows = sorted(rows, key=lambda x: (x["block_number"], x["log_index"]))
        total = sum(x["quantity"] for x in rows)
        free = sum(x["quantity"] for x in rows if x["is_free"])
        paid = total - free
        first = rows[0]
        model = "MIXED_FREE_AND_PAID_OBSERVED" if free and paid else ("FREE_ONLY_OBSERVED" if free else "PAID_ONLY_OBSERVED")
        collections.append(
            {
                "nft_contract": contract,
                "first_mint_timestamp_utc": first["timestamp_utc"],
                "first_mint_block": first["block_number"],
                "first_mint_price_wei": first["unit_mint_price_wei"],
                "first_stage_index": first["drop_stage_index"],
                "last_mint_timestamp_utc": rows[-1]["timestamp_utc"],
                "last_mint_block": rows[-1]["block_number"],
                "event_count": len(rows),
                "minted_quantity": total,
                "free_quantity": free,
                "paid_quantity": paid,
                "unique_minters": len({x["minter"] for x in rows}),
                "unique_payers": len({x["payer"] for x in rows}),
                "observed_stage_indexes": sorted({x["drop_stage_index"] for x in rows}),
                "observed_prices_wei": sorted({x["unit_mint_price_wei"] for x in rows}),
                "observed_model": model,
                "paid_from_first_observed": bool(first["is_paid"] and free == 0),
                "production_approved": False,
            }
        )
        for row in rows:
            wallet_projects[(row["minter"], contract)].append(row)

    wallet_rows: list[dict[str, Any]] = []
    for (wallet, contract), rows in sorted(wallet_projects.items()):
        rows = sorted(rows, key=lambda x: (x["block_number"], x["log_index"]))
        wallet_rows.append(
            {
                "wallet": wallet,
                "nft_contract": contract,
                "first_entry_timestamp_utc": rows[0]["timestamp_utc"],
                "first_entry_block": rows[0]["block_number"],
                "first_entry_price_wei": rows[0]["unit_mint_price_wei"],
                "first_entry_stage_index": rows[0]["drop_stage_index"],
                "mint_event_count": len(rows),
                "minted_quantity": sum(x["quantity"] for x in rows),
                "free_quantity": sum(x["quantity"] for x in rows if x["is_free"]),
                "paid_quantity": sum(x["quantity"] for x in rows if x["is_paid"]),
                "total_primary_cost_wei": sum(x["gross_mint_value_wei"] for x in rows),
                "total_primary_cost_eth": sum(x["gross_mint_value_eth"] for x in rows),
                "production_approved": False,
            }
        )

    write_csv(OUT / "seadrop_global_events.csv", events)
    write_csv(OUT / "seadrop_global_collections.csv", collections)
    write_csv(OUT / "seadrop_global_wallet_project_entries.csv", wallet_rows)
    write_csv(OUT / "scan_ranges.csv", ranges)
    with (OUT / "seadrop_global_events.jsonl").open("w", encoding="utf-8") as f:
        for row in events:
            f.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")

    validation = {
        "status": "PASS" if events and collections and len(raw) == len(dedup) else "FAIL",
        "generated_at_utc": now(),
        "latest_block": latest,
        "api_calls": calls,
        "raw_rows": len(raw),
        "event_rows": len(events),
        "collection_rows": len(collections),
        "wallet_project_rows": len(wallet_rows),
        "paid_only_collections": sum(x["observed_model"] == "PAID_ONLY_OBSERVED" for x in collections),
        "mixed_collections": sum(x["observed_model"] == "MIXED_FREE_AND_PAID_OBSERVED" for x in collections),
        "free_only_collections": sum(x["observed_model"] == "FREE_ONLY_OBSERVED" for x in collections),
        "paid_from_first_observed_collections": sum(bool(x["paid_from_first_observed"]) for x in collections),
        "range_rows": len(ranges),
        "unresolved_1000_row_leaf": 0,
    }
    (OUT / "validation.json").write_text(json.dumps(validation, indent=2), encoding="utf-8")
    print(json.dumps(validation), flush=True)
    if validation["status"] != "PASS":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
