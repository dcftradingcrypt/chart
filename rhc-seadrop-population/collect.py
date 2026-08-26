#!/usr/bin/env python3
from __future__ import annotations

import csv
import hashlib
import json
import os
import random
import time
import urllib.error
import urllib.request
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SEADROP = "0x00005ea00ac477b1030ce78506496e8c2de24bf5"
SEADROP_MINT_TOPIC = "0xe90cf9cc0a552cf52ea6ff74ece0f1c8ae8cc9ad630d3181f55ac43ca076b7d6"
RPC_URLS = [
    os.getenv("RPC_URL", "https://rpc.mainnet.chain.robinhood.com/rpc"),
    "https://rpc.mainnet.chain.robinhood.com",
]
OUT = Path(os.getenv("OUT", "out"))
OUT.mkdir(parents=True, exist_ok=True)


def now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


class RPC:
    def __init__(self) -> None:
        self.url = None
        self.ident = 1
        self.calls = 0
        self.retries = 0
        self._select()

    def _http(self, url: str, payload: Any, timeout: int = 90) -> Any:
        body = json.dumps(payload).encode()
        req = urllib.request.Request(
            url,
            data=body,
            method="POST",
            headers={"content-type": "application/json", "user-agent": "RHC-SeaDrop-Research/0.1"},
        )
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode())

    def _select(self) -> None:
        last = None
        for url in dict.fromkeys(RPC_URLS):
            try:
                data = self._http(url, {"jsonrpc": "2.0", "id": 1, "method": "eth_chainId", "params": []}, 30)
                if data.get("result") == hex(4663):
                    self.url = url
                    return
            except Exception as e:
                last = e
        raise RuntimeError(f"No Robinhood Chain RPC available: {last}")

    def call(self, method: str, params: list[Any], attempts: int = 8) -> Any:
        assert self.url
        last = None
        for i in range(attempts):
            self.ident += 1
            self.calls += 1
            try:
                data = self._http(self.url, {"jsonrpc": "2.0", "id": self.ident, "method": method, "params": params})
                if "error" in data:
                    raise RuntimeError(json.dumps(data["error"], ensure_ascii=False))
                return data.get("result")
            except Exception as e:
                last = e
                if i + 1 == attempts:
                    break
                self.retries += 1
                time.sleep(min(20, (2 ** i) * 0.35 + random.random() * 0.5))
        raise RuntimeError(f"RPC {method} failed: {last}")

    def batch(self, calls: list[tuple[str, list[Any]]], attempts: int = 6) -> list[Any]:
        assert self.url
        payload = []
        ids = []
        for method, params in calls:
            self.ident += 1
            ids.append(self.ident)
            payload.append({"jsonrpc": "2.0", "id": self.ident, "method": method, "params": params})
        last = None
        for i in range(attempts):
            self.calls += len(calls)
            try:
                rows = self._http(self.url, payload, 120)
                by_id = {int(x["id"]): x for x in rows}
                out = []
                for ident in ids:
                    x = by_id.get(ident, {})
                    if "error" in x:
                        out.append({"__error__": x["error"]})
                    else:
                        out.append(x.get("result"))
                return out
            except Exception as e:
                last = e
                if i + 1 == attempts:
                    break
                self.retries += 1
                time.sleep(min(20, (2 ** i) * 0.5 + random.random()))
        raise RuntimeError(f"RPC batch failed: {last}")


def to_addr(topic: str) -> str:
    return "0x" + topic[-40:].lower()


def words(data: str) -> list[int]:
    s = data[2:] if data.startswith("0x") else data
    return [int(s[i:i+64], 16) for i in range(0, len(s), 64) if len(s[i:i+64]) == 64]


def abi_string(raw: str | None) -> str | None:
    if not raw or raw == "0x":
        return None
    b = bytes.fromhex(raw[2:])
    try:
        if len(b) >= 64:
            off = int.from_bytes(b[:32], "big")
            if off + 32 <= len(b):
                n = int.from_bytes(b[off:off+32], "big")
                if off + 32 + n <= len(b):
                    return b[off+32:off+32+n].decode("utf-8", "replace").strip("\x00")
        return b.rstrip(b"\x00").decode("utf-8", "replace")
    except Exception:
        return None


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fields = sorted({k for r in rows for k in r}) if rows else ["empty"]
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow({k: json.dumps(v, ensure_ascii=False, sort_keys=True) if isinstance(v, (list, dict)) else v for k, v in r.items()})


def write_json(path: Path, obj: Any) -> None:
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def main() -> None:
    rpc = RPC()
    latest = int(rpc.call("eth_blockNumber", []), 16)
    latest_code = rpc.call("eth_getCode", [SEADROP, "latest"])
    if latest_code in (None, "0x", "0x0"):
        raise RuntimeError("SeaDrop code absent at latest block")

    # Find the first block at which the canonical SeaDrop address has code.
    lo, hi = 0, latest
    archive_search_ok = True
    try:
        while lo < hi:
            mid = (lo + hi) // 2
            code = rpc.call("eth_getCode", [SEADROP, hex(mid)])
            if code not in (None, "0x", "0x0"):
                hi = mid
            else:
                lo = mid + 1
        deployment_block = lo
    except Exception:
        archive_search_ok = False
        deployment_block = 0

    raw_logs: list[dict[str, Any]] = []
    start = deployment_block
    chunk = 200_000
    min_chunk = 500
    max_chunk = 500_000
    ranges = []
    while start <= latest:
        end = min(latest, start + chunk - 1)
        filt = {"fromBlock": hex(start), "toBlock": hex(end), "address": SEADROP, "topics": [SEADROP_MINT_TOPIC]}
        try:
            got = rpc.call("eth_getLogs", [filt], attempts=4) or []
        except Exception as e:
            if chunk <= min_chunk:
                raise RuntimeError(f"eth_getLogs failed at minimum chunk {start}-{end}: {e}")
            chunk = max(min_chunk, chunk // 2)
            continue
        raw_logs.extend(got)
        ranges.append({"from_block": start, "to_block": end, "logs": len(got), "chunk": chunk})
        start = end + 1
        if len(got) < 500 and chunk < max_chunk:
            chunk = min(max_chunk, int(chunk * 1.5))
        elif len(got) > 5000 and chunk > min_chunk:
            chunk = max(min_chunk, chunk // 2)
        if len(ranges) % 25 == 0:
            print(f"scan {end}/{latest}; logs={len(raw_logs)}; chunk={chunk}", flush=True)

    # Deduplicate and decode.
    dedup = {}
    for log in raw_logs:
        key = (log.get("transactionHash"), log.get("logIndex"))
        dedup[key] = log
    raw_logs = sorted(dedup.values(), key=lambda x: (int(x["blockNumber"], 16), int(x["logIndex"], 16)))

    block_nums = sorted({int(x["blockNumber"], 16) for x in raw_logs})
    timestamps: dict[int, int] = {}
    for i in range(0, len(block_nums), 80):
        nums = block_nums[i:i+80]
        results = rpc.batch([("eth_getBlockByNumber", [hex(n), False]) for n in nums])
        for n, b in zip(nums, results):
            if isinstance(b, dict) and b.get("timestamp"):
                timestamps[n] = int(b["timestamp"], 16)

    events = []
    for log in raw_logs:
        ws = words(log.get("data", "0x"))
        if len(log.get("topics", [])) < 4 or len(ws) < 5:
            continue
        bn = int(log["blockNumber"], 16)
        ts = timestamps.get(bn)
        events.append({
            "transaction_hash": log["transactionHash"].lower(),
            "log_index": int(log["logIndex"], 16),
            "block_number": bn,
            "timestamp_utc": datetime.fromtimestamp(ts, timezone.utc).isoformat().replace("+00:00", "Z") if ts else None,
            "nft_contract": to_addr(log["topics"][1]),
            "minter": to_addr(log["topics"][2]),
            "fee_recipient": to_addr(log["topics"][3]),
            "payer": "0x" + ws[0].to_bytes(32, "big")[-20:].hex(),
            "quantity": ws[1],
            "unit_mint_price_wei": ws[2],
            "unit_mint_price_eth": ws[2] / 1e18,
            "gross_mint_value_wei": ws[1] * ws[2],
            "gross_mint_value_eth": ws[1] * ws[2] / 1e18,
            "fee_bps": ws[3],
            "drop_stage_index": ws[4],
            "is_free": ws[2] == 0,
            "is_paid": ws[2] > 0,
            "source": "ROBINHOOD_RPC_CANONICAL_SEADROP_EVENT",
        })

    contracts = sorted({e["nft_contract"] for e in events})
    meta: dict[str, dict[str, Any]] = {}
    selectors = {"name": "0x06fdde03", "symbol": "0x95d89b41", "total_supply": "0x18160ddd", "owner": "0x8da5cb5b"}
    for i in range(0, len(contracts), 40):
        cs = contracts[i:i+40]
        calls = []
        keys = []
        for c in cs:
            for k, sel in selectors.items():
                calls.append(("eth_call", [{"to": c, "data": sel}, "latest"]))
                keys.append((c, k))
            calls.append(("eth_getCode", [c, "latest"]))
            keys.append((c, "code"))
        results = rpc.batch(calls)
        for (c, k), result in zip(keys, results):
            meta.setdefault(c, {})
            if isinstance(result, dict) and "__error__" in result:
                meta[c][k] = None
            elif k in ("name", "symbol"):
                meta[c][k] = abi_string(result)
            elif k == "total_supply":
                meta[c][k] = int(result, 16) if isinstance(result, str) and result not in ("0x", "0x0") else None
            elif k == "owner":
                meta[c][k] = ("0x" + result[-40:].lower()) if isinstance(result, str) and len(result) >= 42 else None
            else:
                meta[c]["code_sha256"] = hashlib.sha256(bytes.fromhex(result[2:])).hexdigest() if isinstance(result, str) and result.startswith("0x") else None

    by_contract: dict[str, list[dict[str, Any]]] = defaultdict(list)
    wallet_projects: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for e in events:
        by_contract[e["nft_contract"]].append(e)
        wallet_projects[(e["minter"], e["nft_contract"])].append(e)

    population = []
    for c, rows in sorted(by_contract.items(), key=lambda kv: min(x["block_number"] for x in kv[1])):
        rows = sorted(rows, key=lambda x: (x["block_number"], x["log_index"]))
        prices = sorted({int(x["unit_mint_price_wei"]) for x in rows})
        stages = sorted({int(x["drop_stage_index"]) for x in rows})
        first = rows[0]
        total_qty = sum(x["quantity"] for x in rows)
        free_qty = sum(x["quantity"] for x in rows if x["is_free"])
        paid_qty = total_qty - free_qty
        has_free = free_qty > 0
        has_paid = paid_qty > 0
        if not has_free and has_paid and first["is_paid"]:
            observed_model = "OBSERVED_PAID_FROM_FIRST_SEADROP_MINT_NO_FREE_EVENT"
        elif has_free and has_paid:
            observed_model = "OBSERVED_MIXED_FREE_AND_PAID"
        elif has_free:
            observed_model = "OBSERVED_FREE_ONLY"
        else:
            observed_model = "OBSERVED_PAID_ONLY_OTHER"
        population.append({
            "nft_contract": c,
            "name": meta.get(c, {}).get("name"),
            "symbol": meta.get(c, {}).get("symbol"),
            "owner": meta.get(c, {}).get("owner"),
            "current_total_supply": meta.get(c, {}).get("total_supply"),
            "code_sha256": meta.get(c, {}).get("code_sha256"),
            "first_mint_timestamp_utc": first["timestamp_utc"],
            "first_mint_block": first["block_number"],
            "first_stage_index": first["drop_stage_index"],
            "first_unit_price_wei": first["unit_mint_price_wei"],
            "first_unit_price_eth": first["unit_mint_price_eth"],
            "last_mint_timestamp_utc": rows[-1]["timestamp_utc"],
            "last_mint_block": rows[-1]["block_number"],
            "seadrop_event_count": len(rows),
            "minted_quantity": total_qty,
            "free_minted_quantity": free_qty,
            "paid_minted_quantity": paid_qty,
            "unique_minters": len({x["minter"] for x in rows}),
            "unique_payers": len({x["payer"] for x in rows}),
            "observed_stage_indexes": stages,
            "observed_unit_prices_wei": prices,
            "observed_unit_prices_eth": [p / 1e18 for p in prices],
            "has_observed_free_mint": has_free,
            "has_observed_paid_mint": has_paid,
            "has_paid_public_stage0": any(x["drop_stage_index"] == 0 and x["is_paid"] for x in rows),
            "has_free_public_stage0": any(x["drop_stage_index"] == 0 and x["is_free"] for x in rows),
            "observed_primary_model": observed_model,
            "candidate_paid_from_start": observed_model == "OBSERVED_PAID_FROM_FIRST_SEADROP_MINT_NO_FREE_EVENT",
            "qualification_limit": "Observed mint events only; unminted/free scheduled stages still require NFT Trencher or OpenSea stage verification.",
        })

    wallet_rows = []
    for (wallet, contract), rows in sorted(wallet_projects.items()):
        rows = sorted(rows, key=lambda x: (x["block_number"], x["log_index"]))
        wallet_rows.append({
            "wallet": wallet,
            "nft_contract": contract,
            "name": meta.get(contract, {}).get("name"),
            "first_entry_timestamp_utc": rows[0]["timestamp_utc"],
            "first_entry_block": rows[0]["block_number"],
            "first_entry_price_wei": rows[0]["unit_mint_price_wei"],
            "first_entry_stage_index": rows[0]["drop_stage_index"],
            "mint_event_count": len(rows),
            "minted_quantity": sum(x["quantity"] for x in rows),
            "total_primary_cost_wei": sum(x["gross_mint_value_wei"] for x in rows),
            "total_primary_cost_eth": sum(x["gross_mint_value_eth"] for x in rows),
            "free_quantity": sum(x["quantity"] for x in rows if x["is_free"]),
            "paid_quantity": sum(x["quantity"] for x in rows if x["is_paid"]),
            "research_status": "NOT_SCORED_SECONDARY_AND_COPY_PNL_PENDING",
            "production_approved": False,
        })

    candidates = [r for r in population if r["candidate_paid_from_start"]]

    with (OUT / "seadrop_mint_events.jsonl").open("w", encoding="utf-8") as f:
        for row in events:
            f.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
    write_csv(OUT / "seadrop_mint_events.csv", events)
    write_csv(OUT / "seadrop_collection_population.csv", population)
    write_csv(OUT / "seadrop_wallet_project_entries.csv", wallet_rows)
    write_csv(OUT / "candidate_paid_from_start_observed.csv", candidates)
    write_json(OUT / "scan_ranges.json", ranges)
    write_json(OUT / "raw_log_sample.json", raw_logs[:100])

    known = {
        "0xb433123b8657dacf3b246b3e25f8952a0cd2f121": "HOOD DOLLS",
        "0xf885faf151e3362ad1634b7f2f5c43338746fbba": "SAFEMARS BOARDING PASS",
    }
    missing_known = [a for a in known if a not in by_contract]
    validation = {
        "status": "PASS" if events and population and not missing_known else "FAIL",
        "generated_at_utc": now(),
        "chain_id": 4663,
        "rpc_url": rpc.url,
        "seadrop_address": SEADROP,
        "seadrop_mint_topic": SEADROP_MINT_TOPIC,
        "archive_deployment_search_ok": archive_search_ok,
        "deployment_block": deployment_block,
        "latest_block": latest,
        "event_rows": len(events),
        "collection_rows": len(population),
        "wallet_project_rows": len(wallet_rows),
        "candidate_paid_from_start_rows": len(candidates),
        "missing_known_p0_contracts": missing_known,
        "rpc_calls": rpc.calls,
        "rpc_retries": rpc.retries,
    }
    write_json(OUT / "validation.json", validation)
    write_json(OUT / "summary.json", {
        **validation,
        "models": {k: sum(1 for r in population if r["observed_primary_model"] == k) for k in sorted({r["observed_primary_model"] for r in population})},
        "paid_quantity": sum(r["paid_minted_quantity"] for r in population),
        "free_quantity": sum(r["free_minted_quantity"] for r in population),
    })
    if validation["status"] != "PASS":
        raise SystemExit(1)
    print(json.dumps(validation, ensure_ascii=False))


if __name__ == "__main__":
    main()
