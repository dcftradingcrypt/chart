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

OUT = Path(os.getenv("OUT", "out-targeted"))
OUT.mkdir(parents=True, exist_ok=True)
TARGETS = Path(os.getenv("TARGETS", "rhc-seadrop-population/targets.csv"))
BLOCKSCOUT = "https://robinhoodchain.blockscout.com/api"
RPC = "https://rpc.mainnet.chain.robinhood.com/rpc"
SEADROP = "0x00005ea00ac477b1030ce78506496e8c2de24bf5"
TOPIC0 = "0xe90cf9cc0a552cf52ea6ff74ece0f1c8ae8cc9ad630d3181f55ac43ca076b7d6"
UA = "RHC-SeaDrop-Targeted-Research/0.3"


def now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def get_json(url: str, attempts: int = 8, delay: float = 0.25) -> Any:
    last = None
    for i in range(attempts):
        try:
            req = urllib.request.Request(url, headers={"user-agent": UA, "accept": "application/json"})
            with urllib.request.urlopen(req, timeout=90) as r:
                raw = r.read()
            time.sleep(delay)
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


def topic_addr(address: str) -> str:
    return "0x" + "0" * 24 + address.lower()[2:]


def words(data: str) -> list[int]:
    s = data[2:] if data.startswith("0x") else data
    return [int(s[i : i + 64], 16) for i in range(0, len(s), 64) if len(s[i : i + 64]) == 64]


def to_addr(topic: str) -> str:
    return "0x" + topic[-40:].lower()


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fields = sorted({k for r in rows for k in r}) if rows else ["empty"]
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {k: json.dumps(v, ensure_ascii=False, sort_keys=True) if isinstance(v, (list, dict)) else v for k, v in row.items()}
            )


def load_targets() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with TARGETS.open(encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            address = row["contract_address"].strip().lower()
            rows.append(
                {
                    "contract_address": address,
                    "name": row.get("collection_name") or address,
                    "sources": sorted(x for x in (row.get("sources") or "").split("|") if x),
                }
            )
    return rows


def fetch_logs(contract: str, start: int, end: int) -> list[dict[str, Any]]:
    params = {
        "module": "logs",
        "action": "getLogs",
        "fromBlock": start,
        "toBlock": end,
        "address": SEADROP,
        "topic0": TOPIC0,
        "topic1": topic_addr(contract),
        "topic0_1_opr": "and",
    }
    url = BLOCKSCOUT + "?" + urllib.parse.urlencode(params)
    data = get_json(url)
    result = data.get("result") if isinstance(data, dict) else None
    if isinstance(result, str):
        if "No logs found" in result or data.get("status") == "0":
            return []
        raise RuntimeError(result)
    result = result or []
    if len(result) < 1000:
        return result
    if start >= end:
        raise RuntimeError(f"1000-log truncation at single block {start}")
    middle = (start + end) // 2
    return fetch_logs(contract, start, middle) + fetch_logs(contract, middle + 1, end)


def main() -> None:
    latest = int(rpc("eth_blockNumber", []), 16)
    target_rows = load_targets()
    write_csv(OUT / "target_contracts.csv", target_rows)

    raw: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    for i, row in enumerate(target_rows, 1):
        address = row["contract_address"]
        print(f"LOGS {i}/{len(target_rows)} {address} {row.get('name') or ''}", flush=True)
        try:
            logs = fetch_logs(address, 0, latest)
        except Exception as e:
            errors.append({"contract_address": address, "error": str(e)})
            continue
        for log in logs:
            log["_target_contract"] = address
        raw.extend(logs)

    dedup: dict[tuple[str, str], dict[str, Any]] = {}
    for log in raw:
        dedup[(str(log.get("transactionHash")).lower(), str(log.get("logIndex")))] = log
    raw = sorted(
        dedup.values(),
        key=lambda x: (int(str(x.get("blockNumber", "0x0")), 16), int(str(x.get("logIndex", "0x0")), 16)),
    )

    events: list[dict[str, Any]] = []
    for log in raw:
        timestamp = log.get("timeStamp")
        timestamp = int(timestamp, 16) if isinstance(timestamp, str) and timestamp.startswith("0x") else None
        decoded = words(log.get("data", "0x"))
        topics = log.get("topics") or []
        if len(topics) < 4 or len(decoded) < 5:
            continue
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
                "source": "BLOCKSCOUT_SEADROP_LOGS",
            }
        )

    by_contract: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for event in events:
        by_contract[event["nft_contract"]].append(event)

    summary: list[dict[str, Any]] = []
    for target in target_rows:
        address = target["contract_address"]
        rows = sorted(by_contract.get(address, []), key=lambda e: (e["block_number"], e["log_index"]))
        quantity = sum(e["quantity"] for e in rows)
        free_quantity = sum(e["quantity"] for e in rows if e["is_free"])
        paid_quantity = quantity - free_quantity
        first = rows[0] if rows else {}
        if not rows:
            model = "NO_SEADROP_MINT_EVENT_OBSERVED"
        elif free_quantity and paid_quantity:
            model = "MIXED_FREE_AND_PAID_OBSERVED"
        elif free_quantity:
            model = "FREE_ONLY_OBSERVED"
        elif paid_quantity:
            model = "PAID_ONLY_OBSERVED"
        else:
            model = "UNRESOLVED"
        summary.append(
            {
                **target,
                "event_count": len(rows),
                "minted_quantity": quantity,
                "free_quantity": free_quantity,
                "paid_quantity": paid_quantity,
                "unique_minters": len({e["minter"] for e in rows}),
                "unique_payers": len({e["payer"] for e in rows}),
                "first_mint_timestamp_utc": first.get("timestamp_utc"),
                "first_mint_price_wei": first.get("unit_mint_price_wei"),
                "first_mint_stage_index": first.get("drop_stage_index"),
                "observed_prices_wei": sorted({e["unit_mint_price_wei"] for e in rows}),
                "observed_stage_indexes": sorted({e["drop_stage_index"] for e in rows}),
                "observed_model": model,
                "paid_from_first_observed": bool(rows and first.get("is_paid") and free_quantity == 0),
                "production_approved": False,
            }
        )

    write_csv(OUT / "seadrop_mint_events.csv", events)
    write_csv(OUT / "seadrop_target_summary.csv", summary)
    write_csv(OUT / "errors.csv", errors)
    with (OUT / "seadrop_mint_events.jsonl").open("w", encoding="utf-8") as f:
        for event in events:
            f.write(json.dumps(event, ensure_ascii=False, sort_keys=True) + "\n")

    validation = {
        "status": "PASS" if not errors and len(target_rows) >= 150 else "PARTIAL",
        "generated_at_utc": now(),
        "latest_block": latest,
        "target_contracts": len(target_rows),
        "event_rows": len(events),
        "contracts_with_events": sum(bool(r["event_count"]) for r in summary),
        "paid_only_contracts": sum(r["observed_model"] == "PAID_ONLY_OBSERVED" for r in summary),
        "mixed_contracts": sum(r["observed_model"] == "MIXED_FREE_AND_PAID_OBSERVED" for r in summary),
        "free_only_contracts": sum(r["observed_model"] == "FREE_ONLY_OBSERVED" for r in summary),
        "no_event_contracts": sum(r["observed_model"] == "NO_SEADROP_MINT_EVENT_OBSERVED" for r in summary),
        "error_count": len(errors),
    }
    (OUT / "validation.json").write_text(json.dumps(validation, indent=2), encoding="utf-8")
    print(json.dumps(validation), flush=True)
    if validation["status"] == "PARTIAL" and len(errors) > 10:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
