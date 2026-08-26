#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import os
import random
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

OUT = Path(os.getenv("OUT", "out-mcbrokers"))
OUT.mkdir(parents=True, exist_ok=True)
RPC = "https://rpc.mainnet.chain.robinhood.com/rpc"
SEADROP = "0x00005ea00ac477b1030ce78506496e8c2de24bf5"
CONTRACT = "0x444444447657f90a85c99c00c0780e4e1c40c897"
TOPIC0 = "0xe90cf9cc0a552cf52ea6ff74ece0f1c8ae8cc9ad630d3181f55ac43ca076b7d6"
TOPIC1 = "0x" + "0" * 24 + CONTRACT[2:]
UA = "RHC-McBrokers-RPC-Research/0.2"
request_id = 0
rpc_calls = 0
rpc_retries = 0


def rpc(method: str, params: list, attempts: int = 10):
    global request_id, rpc_calls, rpc_retries
    last = None
    for attempt in range(attempts):
        request_id += 1
        rpc_calls += 1
        body = json.dumps({"jsonrpc": "2.0", "id": request_id, "method": method, "params": params}).encode()
        req = urllib.request.Request(RPC, data=body, headers={"content-type": "application/json", "user-agent": UA})
        try:
            with urllib.request.urlopen(req, timeout=120) as response:
                data = json.loads(response.read().decode())
            if "error" in data:
                raise RuntimeError(data["error"])
            return data.get("result")
        except Exception as exc:
            last = exc
            if attempt + 1 == attempts:
                break
            rpc_retries += 1
            time.sleep(min(25, 2**attempt + random.random()))
    raise RuntimeError(f"{method}: {last}")


def words(data: str) -> list[int]:
    raw = data[2:] if data.startswith("0x") else data
    return [int(raw[i : i + 64], 16) for i in range(0, len(raw), 64) if len(raw[i : i + 64]) == 64]


def topic_address(topic: str) -> str:
    return "0x" + topic[-40:].lower()


def write_csv(path: Path, rows: list[dict]) -> None:
    fields = sorted({key for row in rows for key in row}) if rows else ["empty"]
    with path.open("w", encoding="utf-8-sig", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({key: json.dumps(value, ensure_ascii=False, sort_keys=True) if isinstance(value, (list, dict)) else value for key, value in row.items()})


def get_logs(start: int, end: int, minimum_chunk: int = 50) -> list[dict]:
    chunk = 20_000
    cursor = start
    output: list[dict] = []
    ranges: list[dict] = []
    while cursor <= end:
        stop = min(end, cursor + chunk - 1)
        query = {"fromBlock": hex(cursor), "toBlock": hex(stop), "address": SEADROP, "topics": [TOPIC0, TOPIC1]}
        try:
            rows = rpc("eth_getLogs", [query], attempts=5) or []
        except Exception:
            if chunk <= minimum_chunk:
                raise
            chunk = max(minimum_chunk, chunk // 2)
            continue
        output.extend(rows)
        ranges.append({"from_block": cursor, "to_block": stop, "rows": len(rows), "chunk": chunk})
        cursor = stop + 1
        if len(rows) < 150 and chunk < 100_000:
            chunk = min(100_000, chunk * 2)
        elif len(rows) > 5000:
            chunk = max(minimum_chunk, chunk // 2)
        if len(ranges) % 25 == 0:
            print(f"range={len(ranges)} end={stop} rows={len(rows)} total={len(output)} chunk={chunk}", flush=True)
    write_csv(OUT / "ranges.csv", ranges)
    return output


def main() -> None:
    latest = int(rpc("eth_blockNumber", []), 16)
    low, high = 0, latest
    while low < high:
        middle = (low + high) // 2
        code = rpc("eth_getCode", [CONTRACT, hex(middle)])
        if code not in (None, "0x", "0x0"):
            high = middle
        else:
            low = middle + 1
    first_code_block = low
    print(f"first_code_block={first_code_block} latest={latest}", flush=True)

    raw = get_logs(first_code_block, latest)
    dedup = {(row.get("transactionHash"), row.get("logIndex")): row for row in raw}
    raw = sorted(dedup.values(), key=lambda row: (int(row["blockNumber"], 16), int(row["logIndex"], 16)))

    block_numbers = sorted({int(row["blockNumber"], 16) for row in raw})
    timestamps: dict[int, int] = {}
    for index, block_number in enumerate(block_numbers, 1):
        block = rpc("eth_getBlockByNumber", [hex(block_number), False])
        timestamps[block_number] = int(block["timestamp"], 16)
        if index % 100 == 0:
            print(f"timestamps={index}/{len(block_numbers)}", flush=True)

    events: list[dict] = []
    for row in raw:
        decoded = words(row.get("data", "0x"))
        topics = row.get("topics") or []
        if len(decoded) < 5 or len(topics) < 4:
            continue
        block_number = int(row["blockNumber"], 16)
        timestamp = timestamps.get(block_number)
        events.append(
            {
                "collection_name": "McBrokers",
                "nft_contract": CONTRACT,
                "transaction_hash": row["transactionHash"].lower(),
                "log_index": int(row["logIndex"], 16),
                "block_number": block_number,
                "timestamp_utc": datetime.fromtimestamp(timestamp, timezone.utc).isoformat().replace("+00:00", "Z") if timestamp else None,
                "minter": topic_address(topics[2]),
                "fee_recipient": topic_address(topics[3]),
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
            }
        )

    total_quantity = sum(event["quantity"] for event in events)
    free_quantity = sum(event["quantity"] for event in events if event["is_free"])
    paid_quantity = total_quantity - free_quantity
    first = events[0] if events else {}
    model = "NO_EVENT" if not events else ("MIXED_FREE_AND_PAID" if free_quantity and paid_quantity else ("FREE_ONLY" if free_quantity else "PAID_ONLY"))
    summary = [
        {
            "contract_address": CONTRACT,
            "collection_name": "McBrokers",
            "event_count": len(events),
            "minted_quantity": total_quantity,
            "free_quantity": free_quantity,
            "paid_quantity": paid_quantity,
            "unique_minters": len({event["minter"] for event in events}),
            "unique_payers": len({event["payer"] for event in events}),
            "first_mint_timestamp_utc": first.get("timestamp_utc"),
            "first_mint_price_wei": first.get("unit_mint_price_wei"),
            "first_stage_index": first.get("drop_stage_index"),
            "observed_prices_wei": sorted({event["unit_mint_price_wei"] for event in events}),
            "observed_stage_indexes": sorted({event["drop_stage_index"] for event in events}),
            "onchain_model": model,
            "paid_from_first_observed": bool(events and first.get("is_paid") and free_quantity == 0),
            "production_approved": False,
        }
    ]
    write_csv(OUT / "events.csv", events)
    write_csv(OUT / "summary.csv", summary)
    with (OUT / "events.jsonl").open("w", encoding="utf-8") as file:
        for event in events:
            file.write(json.dumps(event, ensure_ascii=False, sort_keys=True) + "\n")
    validation = {
        "status": "PASS" if events else "FAIL",
        "first_code_block": first_code_block,
        "latest_block": latest,
        "event_rows": len(events),
        "minted_quantity": total_quantity,
        "free_quantity": free_quantity,
        "paid_quantity": paid_quantity,
        "rpc_calls": rpc_calls,
        "rpc_retries": rpc_retries,
    }
    (OUT / "validation.json").write_text(json.dumps(validation, indent=2), encoding="utf-8")
    print(json.dumps(validation), flush=True)
    if validation["status"] != "PASS":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
