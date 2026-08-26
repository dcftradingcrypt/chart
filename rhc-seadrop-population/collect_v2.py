#!/usr/bin/env python3
from __future__ import annotations

import csv
import gzip
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


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def topic_address(value: str) -> str:
    return "0x" + value[-40:].lower()


def decode_words(value: str) -> list[int]:
    value = value[2:] if value.startswith("0x") else value
    return [int(value[i : i + 64], 16) for i in range(0, len(value), 64) if len(value[i : i + 64]) == 64]


def decode_abi_string(raw: str | None) -> str | None:
    if not raw or raw in ("0x", "0x0"):
        return None
    try:
        data = bytes.fromhex(raw[2:])
        if len(data) >= 64:
            offset = int.from_bytes(data[:32], "big")
            if offset + 32 <= len(data):
                length = int.from_bytes(data[offset : offset + 32], "big")
                end = offset + 32 + length
                if end <= len(data):
                    return data[offset + 32 : end].decode("utf-8", "replace").strip("\x00")
        return data.rstrip(b"\x00").decode("utf-8", "replace")
    except Exception:
        return None


def write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fields = sorted({key for row in rows for key in row}) if rows else ["empty"]
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    key: json.dumps(value, ensure_ascii=False, sort_keys=True)
                    if isinstance(value, (list, dict, tuple))
                    else value
                    for key, value in row.items()
                }
            )


class RateLimitError(RuntimeError):
    pass


class RangeLimitError(RuntimeError):
    pass


class RPC:
    def __init__(self, min_interval: float = 0.65) -> None:
        self.urls = list(dict.fromkeys(RPC_URLS))
        self.url_index = 0
        self.url = self.urls[0]
        self.ident = 1
        self.calls = 0
        self.retries = 0
        self.rate_limits = 0
        self.last_request = 0.0
        self.min_interval = min_interval
        self._select()

    def _pace(self) -> None:
        delay = self.min_interval - (time.monotonic() - self.last_request)
        if delay > 0:
            time.sleep(delay + random.random() * 0.08)

    def _request(self, payload: Any, timeout: int = 120) -> Any:
        self._pace()
        body = json.dumps(payload).encode("utf-8")
        request = urllib.request.Request(
            self.url,
            data=body,
            method="POST",
            headers={
                "content-type": "application/json",
                "accept": "application/json",
                "user-agent": "RHC-SeaDrop-Research/0.2-read-only",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                self.last_request = time.monotonic()
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            self.last_request = time.monotonic()
            if exc.code == 429:
                self.rate_limits += 1
                retry_after = exc.headers.get("Retry-After")
                try:
                    wait = float(retry_after) if retry_after else 0.0
                except Exception:
                    wait = 0.0
                raise RateLimitError(f"HTTP 429 retry_after={wait}") from exc
            body_text = exc.read(1000).decode("utf-8", "replace")
            raise RuntimeError(f"HTTP {exc.code}: {body_text}") from exc

    def _select(self) -> None:
        last: Exception | None = None
        for index, url in enumerate(self.urls):
            self.url_index = index
            self.url = url
            try:
                data = self._request({"jsonrpc": "2.0", "id": 1, "method": "eth_chainId", "params": []}, 30)
                if data.get("result") == hex(4663):
                    return
            except Exception as exc:
                last = exc
        raise RuntimeError(f"No Robinhood Chain RPC available: {last}")

    def rotate(self) -> None:
        if len(self.urls) > 1:
            self.url_index = (self.url_index + 1) % len(self.urls)
            self.url = self.urls[self.url_index]

    @staticmethod
    def _classify_rpc_error(error: Any) -> Exception:
        text = json.dumps(error, ensure_ascii=False).lower()
        if any(token in text for token in ("too many results", "response size", "block range", "query returned more", "limit exceeded", "-32005")):
            return RangeLimitError(text)
        if any(token in text for token in ("rate limit", "too many requests", "429")):
            return RateLimitError(text)
        return RuntimeError(text)

    def call(self, method: str, params: list[Any], attempts: int = 20) -> Any:
        last: Exception | None = None
        for attempt in range(attempts):
            self.ident += 1
            self.calls += 1
            try:
                data = self._request(
                    {"jsonrpc": "2.0", "id": self.ident, "method": method, "params": params}
                )
                if "error" in data:
                    raise self._classify_rpc_error(data["error"])
                return data.get("result")
            except RangeLimitError:
                raise
            except RateLimitError as exc:
                last = exc
                self.retries += 1
                self.rotate()
                wait = min(150.0, 20.0 + attempt * 8.0 + random.random() * 8.0)
                print(f"rate-limit method={method}; waiting={wait:.1f}s; endpoint={self.url}", flush=True)
                time.sleep(wait)
            except Exception as exc:
                last = exc
                self.retries += 1
                self.rotate()
                wait = min(45.0, 2.0 ** min(attempt, 5) + random.random() * 2.0)
                print(f"transient method={method}; waiting={wait:.1f}s; error={exc}", flush=True)
                time.sleep(wait)
        raise RuntimeError(f"RPC {method} failed after {attempts} attempts: {last}")

    def batch(self, calls: list[tuple[str, list[Any]]], attempts: int = 12) -> list[Any]:
        if not calls:
            return []
        last: Exception | None = None
        for attempt in range(attempts):
            payload = []
            ids = []
            for method, params in calls:
                self.ident += 1
                ids.append(self.ident)
                payload.append({"jsonrpc": "2.0", "id": self.ident, "method": method, "params": params})
            self.calls += len(calls)
            try:
                rows = self._request(payload, 180)
                if not isinstance(rows, list):
                    raise RuntimeError(f"batch response is not a list: {type(rows)}")
                by_id = {int(row["id"]): row for row in rows}
                output = []
                for ident in ids:
                    row = by_id.get(ident, {})
                    if "error" in row:
                        output.append({"__error__": row["error"]})
                    else:
                        output.append(row.get("result"))
                return output
            except RateLimitError as exc:
                last = exc
                self.retries += 1
                self.rotate()
                wait = min(150.0, 25.0 + attempt * 10.0 + random.random() * 8.0)
                print(f"batch rate-limit; waiting={wait:.1f}s", flush=True)
                time.sleep(wait)
            except Exception as exc:
                last = exc
                self.retries += 1
                self.rotate()
                time.sleep(min(45.0, 2.0 ** min(attempt, 5) + random.random() * 2.0))
        raise RuntimeError(f"RPC batch failed: {last}")


def decode_event(log: dict[str, Any]) -> dict[str, Any] | None:
    topics = log.get("topics") or []
    values = decode_words(log.get("data") or "0x")
    if len(topics) < 4 or len(values) < 5:
        return None
    block_number = int(log["blockNumber"], 16)
    block_timestamp = log.get("blockTimestamp")
    timestamp_utc = None
    if block_timestamp:
        try:
            timestamp_utc = datetime.fromtimestamp(int(block_timestamp, 16), timezone.utc).isoformat().replace("+00:00", "Z")
        except Exception:
            timestamp_utc = None
    price = values[2]
    quantity = values[1]
    return {
        "transaction_hash": str(log.get("transactionHash") or "").lower(),
        "log_index": int(log.get("logIndex") or "0x0", 16),
        "block_number": block_number,
        "timestamp_utc": timestamp_utc,
        "nft_contract": topic_address(topics[1]),
        "minter": topic_address(topics[2]),
        "fee_recipient": topic_address(topics[3]),
        "payer": "0x" + values[0].to_bytes(32, "big")[-20:].hex(),
        "quantity": quantity,
        "unit_mint_price_wei": price,
        "unit_mint_price_eth": price / 1e18,
        "gross_mint_value_wei": quantity * price,
        "gross_mint_value_eth": quantity * price / 1e18,
        "fee_bps": values[3],
        "drop_stage_index": values[4],
        "is_free": price == 0,
        "is_paid": price > 0,
        "source": "ROBINHOOD_RPC_CANONICAL_SEADROP_EVENT",
    }


def init_contract_agg(event: dict[str, Any]) -> dict[str, Any]:
    return {
        "first": event,
        "last": event,
        "event_count": 0,
        "minted_quantity": 0,
        "free_quantity": 0,
        "paid_quantity": 0,
        "gross_value_wei": 0,
        "minters": set(),
        "payers": set(),
        "prices": set(),
        "stages": set(),
        "paid_public_stage0": False,
        "free_public_stage0": False,
    }


def init_wallet_agg(event: dict[str, Any]) -> dict[str, Any]:
    return {
        "first": event,
        "last": event,
        "event_count": 0,
        "quantity": 0,
        "free_quantity": 0,
        "paid_quantity": 0,
        "gross_value_wei": 0,
        "payers": set(),
        "prices": set(),
        "stages": set(),
    }


def update_aggregate(agg: dict[str, Any], event: dict[str, Any], wallet: bool = False) -> None:
    agg["last"] = event
    agg["event_count"] += 1
    quantity_key = "quantity" if wallet else "minted_quantity"
    agg[quantity_key] += event["quantity"]
    agg["free_quantity"] += event["quantity"] if event["is_free"] else 0
    agg["paid_quantity"] += event["quantity"] if event["is_paid"] else 0
    agg["gross_value_wei"] += event["gross_mint_value_wei"]
    agg["payers"].add(event["payer"])
    agg["prices"].add(event["unit_mint_price_wei"])
    agg["stages"].add(event["drop_stage_index"])
    if not wallet:
        agg["minters"].add(event["minter"])
        if event["drop_stage_index"] == 0 and event["is_paid"]:
            agg["paid_public_stage0"] = True
        if event["drop_stage_index"] == 0 and event["is_free"]:
            agg["free_public_stage0"] = True


def fetch_metadata(rpc: RPC, contracts: list[str]) -> dict[str, dict[str, Any]]:
    selectors = {
        "name": "0x06fdde03",
        "symbol": "0x95d89b41",
        "total_supply": "0x18160ddd",
        "owner": "0x8da5cb5b",
    }
    metadata: dict[str, dict[str, Any]] = {contract: {} for contract in contracts}
    for start in range(0, len(contracts), 15):
        subset = contracts[start : start + 15]
        calls: list[tuple[str, list[Any]]] = []
        keys: list[tuple[str, str]] = []
        for contract in subset:
            for key, selector in selectors.items():
                calls.append(("eth_call", [{"to": contract, "data": selector}, "latest"]))
                keys.append((contract, key))
            calls.append(("eth_getCode", [contract, "latest"]))
            keys.append((contract, "code"))
        results = rpc.batch(calls)
        for (contract, key), result in zip(keys, results):
            if isinstance(result, dict) and "__error__" in result:
                metadata[contract][key] = None
            elif key in ("name", "symbol"):
                metadata[contract][key] = decode_abi_string(result)
            elif key == "total_supply":
                metadata[contract][key] = int(result, 16) if isinstance(result, str) and result not in ("0x", "0x0") else None
            elif key == "owner":
                metadata[contract][key] = "0x" + result[-40:].lower() if isinstance(result, str) and len(result) >= 42 else None
            elif key == "code":
                metadata[contract]["code_sha256"] = (
                    hashlib.sha256(bytes.fromhex(result[2:])).hexdigest()
                    if isinstance(result, str) and result.startswith("0x")
                    else None
                )
        print(f"metadata {min(start + len(subset), len(contracts))}/{len(contracts)}", flush=True)
    return metadata


def main() -> None:
    rpc = RPC()
    latest = int(rpc.call("eth_blockNumber", []), 16)
    code = rpc.call("eth_getCode", [SEADROP, "latest"])
    if code in (None, "0x", "0x0"):
        raise RuntimeError("canonical SeaDrop code is absent")

    state_path = OUT / "scan_state.json"
    event_path = OUT / "seadrop_mint_events.jsonl.gz"
    range_path = OUT / "scan_ranges.jsonl"
    start = 0
    chunk = 40_000
    min_chunk = 1_000
    max_chunk = 120_000
    if state_path.exists():
        state = json.loads(state_path.read_text(encoding="utf-8"))
        start = int(state.get("next_block", 0))
        chunk = int(state.get("chunk", chunk))

    mode = "at" if event_path.exists() else "wt"
    range_mode = "a" if range_path.exists() else "w"
    contract_agg: dict[str, dict[str, Any]] = {}
    wallet_agg: dict[tuple[str, str], dict[str, Any]] = {}
    total_events = 0
    successful_ranges = 0

    # If resuming inside the same workspace, rebuild aggregates from prior compact events.
    if event_path.exists() and event_path.stat().st_size > 0:
        with gzip.open(event_path, "rt", encoding="utf-8") as handle:
            for line in handle:
                event = json.loads(line)
                total_events += 1
                ca = contract_agg.setdefault(event["nft_contract"], init_contract_agg(event))
                update_aggregate(ca, event, wallet=False)
                key = (event["minter"], event["nft_contract"])
                wa = wallet_agg.setdefault(key, init_wallet_agg(event))
                update_aggregate(wa, event, wallet=True)

    with gzip.open(event_path, mode, encoding="utf-8") as event_handle, range_path.open(range_mode, encoding="utf-8") as range_handle:
        while start <= latest:
            end = min(latest, start + chunk - 1)
            request_filter = {
                "fromBlock": hex(start),
                "toBlock": hex(end),
                "address": SEADROP,
                "topics": [SEADROP_MINT_TOPIC],
            }
            try:
                logs = rpc.call("eth_getLogs", [request_filter], attempts=25) or []
            except RangeLimitError as exc:
                if chunk <= min_chunk:
                    raise RuntimeError(f"range limit at minimum chunk {start}-{end}: {exc}")
                chunk = max(min_chunk, chunk // 2)
                print(f"range-limit {start}-{end}; shrink chunk={chunk}", flush=True)
                continue
            except Exception as exc:
                # Preserve the current block; the next retry uses the same range.
                print(f"range transient {start}-{end}; waiting 60s; error={exc}", flush=True)
                time.sleep(60 + random.random() * 10)
                continue

            decoded_count = 0
            for log in logs:
                event = decode_event(log)
                if event is None:
                    continue
                event_handle.write(json.dumps(event, ensure_ascii=False, sort_keys=True) + "\n")
                total_events += 1
                decoded_count += 1
                ca = contract_agg.setdefault(event["nft_contract"], init_contract_agg(event))
                update_aggregate(ca, event, wallet=False)
                key = (event["minter"], event["nft_contract"])
                wa = wallet_agg.setdefault(key, init_wallet_agg(event))
                update_aggregate(wa, event, wallet=True)

            event_handle.flush()
            range_row = {
                "from_block": start,
                "to_block": end,
                "requested_chunk": chunk,
                "raw_logs": len(logs),
                "decoded_events": decoded_count,
                "completed_at_utc": utc_now(),
            }
            range_handle.write(json.dumps(range_row, sort_keys=True) + "\n")
            range_handle.flush()
            successful_ranges += 1
            start = end + 1
            if len(logs) > 4_000 and chunk > min_chunk:
                chunk = max(min_chunk, chunk // 2)
            elif len(logs) < 400 and chunk < max_chunk:
                chunk = min(max_chunk, int(chunk * 1.25))
            write_json(
                state_path,
                {
                    "next_block": start,
                    "latest_block_at_start": latest,
                    "chunk": chunk,
                    "total_events": total_events,
                    "successful_ranges": successful_ranges,
                    "updated_at_utc": utc_now(),
                },
            )
            if successful_ranges % 25 == 0:
                print(
                    f"scan through={end}/{latest}; events={total_events}; contracts={len(contract_agg)}; chunk={chunk}; rate_limits={rpc.rate_limits}",
                    flush=True,
                )

    contracts = sorted(contract_agg)
    metadata = fetch_metadata(rpc, contracts)

    # Fetch timestamps only for first/last blocks of each collection.
    boundary_blocks = sorted(
        {
            aggregate[boundary]["block_number"]
            for aggregate in contract_agg.values()
            for boundary in ("first", "last")
            if aggregate[boundary].get("timestamp_utc") is None
        }
    )
    block_times: dict[int, str] = {}
    for offset in range(0, len(boundary_blocks), 20):
        numbers = boundary_blocks[offset : offset + 20]
        results = rpc.batch([("eth_getBlockByNumber", [hex(number), False]) for number in numbers])
        for number, block in zip(numbers, results):
            if isinstance(block, dict) and block.get("timestamp"):
                timestamp = int(block["timestamp"], 16)
                block_times[number] = datetime.fromtimestamp(timestamp, timezone.utc).isoformat().replace("+00:00", "Z")

    population: list[dict[str, Any]] = []
    for contract, aggregate in sorted(contract_agg.items(), key=lambda item: item[1]["first"]["block_number"]):
        first = aggregate["first"]
        last = aggregate["last"]
        free_quantity = aggregate["free_quantity"]
        paid_quantity = aggregate["paid_quantity"]
        if free_quantity == 0 and paid_quantity > 0 and first["is_paid"]:
            model = "OBSERVED_PAID_FROM_FIRST_SEADROP_MINT_NO_FREE_EVENT"
        elif free_quantity > 0 and paid_quantity > 0:
            model = "OBSERVED_MIXED_FREE_AND_PAID"
        elif free_quantity > 0:
            model = "OBSERVED_FREE_ONLY"
        else:
            model = "OBSERVED_PAID_ONLY_OTHER"
        population.append(
            {
                "nft_contract": contract,
                "name": metadata.get(contract, {}).get("name"),
                "symbol": metadata.get(contract, {}).get("symbol"),
                "owner": metadata.get(contract, {}).get("owner"),
                "current_total_supply": metadata.get(contract, {}).get("total_supply"),
                "code_sha256": metadata.get(contract, {}).get("code_sha256"),
                "first_mint_block": first["block_number"],
                "first_mint_timestamp_utc": first.get("timestamp_utc") or block_times.get(first["block_number"]),
                "first_stage_index": first["drop_stage_index"],
                "first_unit_price_wei": first["unit_mint_price_wei"],
                "first_unit_price_eth": first["unit_mint_price_eth"],
                "last_mint_block": last["block_number"],
                "last_mint_timestamp_utc": last.get("timestamp_utc") or block_times.get(last["block_number"]),
                "seadrop_event_count": aggregate["event_count"],
                "minted_quantity": aggregate["minted_quantity"],
                "free_minted_quantity": free_quantity,
                "paid_minted_quantity": paid_quantity,
                "gross_mint_value_wei": aggregate["gross_value_wei"],
                "gross_mint_value_eth": aggregate["gross_value_wei"] / 1e18,
                "unique_minters": len(aggregate["minters"]),
                "unique_payers": len(aggregate["payers"]),
                "observed_stage_indexes": sorted(aggregate["stages"]),
                "observed_unit_prices_wei": sorted(aggregate["prices"]),
                "observed_unit_prices_eth": [price / 1e18 for price in sorted(aggregate["prices"])],
                "has_observed_free_mint": free_quantity > 0,
                "has_observed_paid_mint": paid_quantity > 0,
                "has_paid_public_stage0": aggregate["paid_public_stage0"],
                "has_free_public_stage0": aggregate["free_public_stage0"],
                "observed_primary_model": model,
                "source_scope": "ACTUAL_SEADROP_MINT_EVENTS_ONLY",
                "scheduled_unminted_stage_coverage": "NOT_COVERED",
            }
        )

    wallet_rows: list[dict[str, Any]] = []
    for (wallet, contract), aggregate in sorted(wallet_agg.items(), key=lambda item: item[1]["first"]["block_number"]):
        first = aggregate["first"]
        last = aggregate["last"]
        wallet_rows.append(
            {
                "wallet_address": wallet,
                "nft_contract": contract,
                "project_name": metadata.get(contract, {}).get("name"),
                "first_entry_block": first["block_number"],
                "first_entry_timestamp_utc": first.get("timestamp_utc"),
                "last_entry_block": last["block_number"],
                "last_entry_timestamp_utc": last.get("timestamp_utc"),
                "event_count": aggregate["event_count"],
                "minted_quantity": aggregate["quantity"],
                "free_minted_quantity": aggregate["free_quantity"],
                "paid_minted_quantity": aggregate["paid_quantity"],
                "gross_mint_cost_wei": aggregate["gross_value_wei"],
                "gross_mint_cost_eth": aggregate["gross_value_wei"] / 1e18,
                "observed_payers": sorted(aggregate["payers"]),
                "observed_stage_indexes": sorted(aggregate["stages"]),
                "observed_unit_prices_wei": sorted(aggregate["prices"]),
                "public_paid_candidate": aggregate["paid_quantity"] > 0 and 0 in aggregate["stages"],
                "production_approved": False,
                "decision_use": "RESEARCH_ONLY_PENDING_SECONDARY_SALES_AND_COPY_SIMULATION",
            }
        )

    strict_candidates = [
        row
        for row in population
        if row["observed_primary_model"] == "OBSERVED_PAID_FROM_FIRST_SEADROP_MINT_NO_FREE_EVENT"
        and row["has_paid_public_stage0"]
        and not row["has_free_public_stage0"]
    ]

    write_csv(OUT / "seadrop_collection_population.csv", population)
    write_csv(OUT / "seadrop_wallet_project_entries.csv", wallet_rows)
    write_csv(OUT / "observed_paid_from_start_candidates.csv", strict_candidates)
    write_json(
        OUT / "quality_report.json",
        {
            "generated_at_utc": utc_now(),
            "validation_status": "PASS" if total_events > 0 and len(population) > 0 else "FAIL",
            "latest_block": latest,
            "scanned_from_block": 0,
            "scanned_to_block": latest,
            "event_rows": total_events,
            "collection_rows": len(population),
            "wallet_project_rows": len(wallet_rows),
            "observed_paid_from_start_candidate_rows": len(strict_candidates),
            "rpc_url": rpc.url,
            "rpc_calls": rpc.calls,
            "rpc_retries": rpc.retries,
            "rate_limit_events": rpc.rate_limits,
            "known_p0_hood_present": any(row["nft_contract"] == "0xb433123b8657dacf3b246b3e25f8952a0cd2f121" for row in population),
            "known_p0_safemars_present": any(row["nft_contract"] == "0xf885faf151e3362ad1634b7f2f5c43338746fbba" for row in population),
            "limitations": [
                "The population covers actual canonical SeaDropMint events, not unminted scheduled stages.",
                "A no-free-event result is preliminary until NFT Trencher/OpenSea stage evidence is joined.",
                "No wallet is production-approved from primary mint data alone.",
            ],
        },
    )
    print(
        json.dumps(
            {
                "status": "PASS",
                "events": total_events,
                "collections": len(population),
                "wallet_projects": len(wallet_rows),
                "strict_candidates": len(strict_candidates),
                "rate_limits": rpc.rate_limits,
            },
            sort_keys=True,
        ),
        flush=True,
    )


if __name__ == "__main__":
    main()
