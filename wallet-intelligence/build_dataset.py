#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import json
import math
import random
import statistics
import time
import urllib.error
import urllib.request
from collections import defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

RPC_URL = "https://rpc.mainnet.chain.robinhood.com"
ZERO = "0x0000000000000000000000000000000000000000"
NATIVE = "0x0000000000000000000000000000000000000000"
KNOWN_WETH = {"0x0bd7d308f8e1639fab988df18a8011f41eacad73"}
TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
UA = "RHC-Wallet-Intelligence-Builder/1.0"
WINDOWS = {"15m": 15 * 60, "30m": 30 * 60, "2h": 2 * 3600, "24h": 24 * 3600}
LABELS = ("liquid_100_24h", "liquid_115_24h", "liquid_150_24h")


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def intish(value: Any, default: int = 0) -> int:
    try:
        if isinstance(value, str) and value.startswith("0x"):
            return int(value, 16)
        return int(value)
    except Exception:
        return default


def address_word(word: bytes) -> str:
    return "0x" + word[-20:].hex()


def word(data: bytes, index: int) -> bytes:
    start = index * 32
    end = start + 32
    if end > len(data):
        raise ValueError(f"word {index} out of range")
    return data[start:end]


def decode_static_tuple_array(data: bytes, offset: int, width: int) -> list[list[bytes]]:
    if offset < 0 or offset + 32 > len(data):
        raise ValueError("array offset out of bounds")
    count = int.from_bytes(data[offset : offset + 32], "big")
    cursor = offset + 32
    end = cursor + count * width * 32
    if end > len(data):
        raise ValueError("array payload truncated")
    return [[data[cursor + (i * width + j) * 32 : cursor + (i * width + j + 1) * 32] for j in range(width)] for i in range(count)]


def read_jsonl_gz(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                rows.append(json.loads(line))
    return rows


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def write_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> int:
    count = 0
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
            count += 1
    return count


def write_csv(path: Path, rows: list[dict[str, Any]]) -> int:
    fields = sorted({key for row in rows for key in row}) if rows else ["empty"]
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({key: json.dumps(value, ensure_ascii=False, sort_keys=True) if isinstance(value, (list, dict, tuple)) else value for key, value in row.items()})
    return len(rows)


def rpc_request(payload: Any, attempts: int = 7) -> Any:
    body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    last: Exception | None = None
    for attempt in range(attempts):
        time.sleep(0.09 + random.random() * 0.18)
        request = urllib.request.Request(RPC_URL, data=body, method="POST", headers={"Content-Type": "application/json", "Accept": "application/json", "User-Agent": UA})
        try:
            with urllib.request.urlopen(request, timeout=90) as response:
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            last = exc
            if exc.code not in (408, 425, 429, 500, 502, 503, 504):
                raise
        except Exception as exc:  # noqa: BLE001
            last = exc
        time.sleep(min(60.0, 2 ** min(attempt, 5) + random.random() * 4))
    raise RuntimeError(f"RPC request failed: {last}")


def rpc_batch(calls: list[tuple[str, list[Any]]]) -> dict[int, Any]:
    payload = [{"jsonrpc": "2.0", "id": i, "method": method, "params": params} for i, (method, params) in enumerate(calls)]
    data = rpc_request(payload)
    if not isinstance(data, list):
        data = [data]
    result: dict[int, Any] = {}
    for row in data:
        if isinstance(row, dict) and "id" in row:
            result[int(row["id"])] = row.get("result") if "error" not in row else {"__error__": row["error"]}
    return result


def fetch_chain_context(log_rows: list[dict[str, Any]]) -> tuple[dict[int, dict[str, Any]], dict[str, dict[str, Any]], dict[str, dict[str, Any]], list[dict[str, Any]]]:
    block_numbers = sorted({intish(row.get("blockNumber") or row.get("block_number")) for row in log_rows})
    tx_hashes = sorted({str(row.get("transactionHash") or row.get("transaction_hash") or "").lower() for row in log_rows if row.get("transactionHash") or row.get("transaction_hash")})
    blocks: dict[int, dict[str, Any]] = {}
    txs: dict[str, dict[str, Any]] = {}
    receipts: dict[str, dict[str, Any]] = {}
    errors: list[dict[str, Any]] = []

    for start in range(0, len(block_numbers), 80):
        chunk = block_numbers[start : start + 80]
        try:
            response = rpc_batch([("eth_getBlockByNumber", [hex(number), False]) for number in chunk])
            for index, number in enumerate(chunk):
                value = response.get(index)
                if isinstance(value, dict) and "__error__" not in value:
                    blocks[number] = value
                else:
                    errors.append({"kind": "block", "block_number": number, "value": value})
        except Exception as exc:  # noqa: BLE001
            errors.append({"kind": "block_batch", "start": start, "error": repr(exc)})

    for start in range(0, len(tx_hashes), 32):
        chunk = tx_hashes[start : start + 32]
        calls: list[tuple[str, list[Any]]] = []
        keys: list[tuple[str, str]] = []
        for tx_hash in chunk:
            calls.append(("eth_getTransactionByHash", [tx_hash])); keys.append((tx_hash, "tx"))
            calls.append(("eth_getTransactionReceipt", [tx_hash])); keys.append((tx_hash, "receipt"))
        try:
            response = rpc_batch(calls)
            for index, (tx_hash, kind) in enumerate(keys):
                value = response.get(index)
                if not isinstance(value, dict) or "__error__" in value:
                    errors.append({"kind": kind, "transaction_hash": tx_hash, "value": value})
                elif kind == "tx":
                    txs[tx_hash] = value
                else:
                    receipts[tx_hash] = value
        except Exception as exc:  # noqa: BLE001
            errors.append({"kind": "tx_receipt_batch", "start": start, "error": repr(exc)})
    return blocks, txs, receipts, errors


def decode_seadrop(row: dict[str, Any], blocks: dict[int, dict[str, Any]], txs: dict[str, dict[str, Any]], receipts: dict[str, dict[str, Any]]) -> dict[str, Any]:
    topics = row["topics"]
    data = bytes.fromhex(row["data"][2:])
    if len(topics) < 4 or len(data) < 160:
        raise ValueError("invalid SeaDropMint event")
    block_number = int(row["blockNumber"], 16)
    tx_hash = row["transactionHash"].lower()
    tx = txs.get(tx_hash) or {}
    receipt = receipts.get(tx_hash) or {}
    return {
        "nft_contract": "0x" + topics[1][-40:],
        "minter": "0x" + topics[2][-40:],
        "fee_recipient": "0x" + topics[3][-40:],
        "payer": address_word(word(data, 0)),
        "quantity": int.from_bytes(word(data, 1), "big"),
        "unit_mint_price_wei": int.from_bytes(word(data, 2), "big"),
        "fee_bps": int.from_bytes(word(data, 3), "big"),
        "drop_stage_index": int.from_bytes(word(data, 4), "big"),
        "block_number": block_number,
        "block_hash": row["blockHash"].lower(),
        "transaction_hash": tx_hash,
        "transaction_index": int(row.get("transactionIndex") or "0x0", 16),
        "log_index": int(row["logIndex"], 16),
        "timestamp_unix": intish((blocks.get(block_number) or {}).get("timestamp")),
        "tx_from": str(tx.get("from") or "").lower(),
        "tx_to": str(tx.get("to") or "").lower(),
        "tx_value_wei": intish(tx.get("value")),
        "receipt_status": intish(receipt.get("status")),
        "entry_gas_cost_wei": intish(receipt.get("gasUsed")) * intish(receipt.get("effectiveGasPrice") or tx.get("gasPrice")),
    }


def parse_item(words: list[bytes], received: bool) -> dict[str, Any]:
    item = {
        "item_type": int.from_bytes(words[0], "big"),
        "token": address_word(words[1]),
        "identifier": int.from_bytes(words[2], "big"),
        "amount": int.from_bytes(words[3], "big"),
    }
    if received:
        item["recipient"] = address_word(words[4])
    return item


def decode_order_fulfilled(row: dict[str, Any], blocks: dict[int, dict[str, Any]], txs: dict[str, dict[str, Any]], receipts: dict[str, dict[str, Any]]) -> dict[str, Any]:
    topics = row["topics"]
    data = bytes.fromhex(row["data"][2:])
    if len(topics) == 3:
        if len(data) < 128:
            raise ValueError("OrderFulfilled data too short")
        order_hash = "0x" + word(data, 0).hex()
        offerer = "0x" + topics[1][-40:]
        zone = "0x" + topics[2][-40:]
        recipient = address_word(word(data, 1))
        offer_offset = int.from_bytes(word(data, 2), "big")
        consideration_offset = int.from_bytes(word(data, 3), "big")
    elif len(topics) >= 4:
        if len(data) < 96:
            raise ValueError("indexed OrderFulfilled data too short")
        order_hash = topics[1].lower()
        offerer = "0x" + topics[2][-40:]
        zone = "0x" + topics[3][-40:]
        recipient = address_word(word(data, 0))
        offer_offset = int.from_bytes(word(data, 1), "big")
        consideration_offset = int.from_bytes(word(data, 2), "big")
    else:
        raise ValueError("OrderFulfilled topics invalid")
    offers = [parse_item(words, False) for words in decode_static_tuple_array(data, offer_offset, 4)]
    consideration = [parse_item(words, True) for words in decode_static_tuple_array(data, consideration_offset, 5)]
    block_number = int(row["blockNumber"], 16)
    tx_hash = row["transactionHash"].lower()
    tx = txs.get(tx_hash) or {}
    receipt = receipts.get(tx_hash) or {}
    return {
        "order_hash": order_hash,
        "offerer": offerer,
        "zone": zone,
        "recipient": recipient,
        "offer": offers,
        "consideration": consideration,
        "block_number": block_number,
        "block_hash": row["blockHash"].lower(),
        "transaction_hash": tx_hash,
        "transaction_index": int(row.get("transactionIndex") or "0x0", 16),
        "log_index": int(row["logIndex"], 16),
        "timestamp_unix": intish((blocks.get(block_number) or {}).get("timestamp")),
        "tx_from": str(tx.get("from") or "").lower(),
        "receipt_status": intish(receipt.get("status")),
        "gas_used": intish(receipt.get("gasUsed")),
        "effective_gas_price_wei": intish(receipt.get("effectiveGasPrice") or tx.get("gasPrice")),
    }


def sale_rows(events: list[dict[str, Any]], transfer_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    transfers_by_tx: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in transfer_rows:
        transfers_by_tx[row["transaction_hash"]].append(row)
    sales: list[dict[str, Any]] = []
    for event in events:
        offer_nfts = [item for item in event["offer"] if item["item_type"] in (2, 3, 4, 5)]
        consideration_nfts = [item for item in event["consideration"] if item["item_type"] in (2, 3, 4, 5)]
        if offer_nfts and consideration_nfts:
            direction = "NFT_FOR_NFT_OR_COMPLEX"
        elif offer_nfts:
            direction = "LISTING"
        elif consideration_nfts:
            direction = "OFFER"
        else:
            continue
        nfts = offer_nfts or consideration_nfts
        contract_units: dict[str, int] = defaultdict(int)
        token_ids: dict[str, set[str]] = defaultdict(set)
        for item in nfts:
            contract = item["token"].lower()
            amount = item["amount"] if item["item_type"] in (3, 5) else 1
            contract_units[contract] += amount
            token_ids[contract].add(str(item["identifier"]))
        if len(contract_units) != 1:
            bundle_status = "MULTI_CONTRACT_BUNDLE"
        else:
            bundle_status = "SINGLE_CONTRACT"

        currency_offer = [item for item in event["offer"] if item["item_type"] in (0, 1)]
        currency_consideration = [item for item in event["consideration"] if item["item_type"] in (0, 1)]
        currency_items = currency_consideration if direction == "LISTING" else currency_offer if direction == "OFFER" else []
        currency_tokens = {NATIVE if item["item_type"] == 0 else item["token"].lower() for item in currency_items}
        payment_token = next(iter(currency_tokens)) if len(currency_tokens) == 1 else None
        gross = sum(item["amount"] for item in currency_items) if payment_token else None

        tx_transfers = transfers_by_tx.get(event["transaction_hash"], [])
        for contract, units in contract_units.items():
            relevant = [row for row in tx_transfers if row["contract_address"] == contract and row["from_address"] != ZERO and row["to_address"] != ZERO]
            sellers = {row["from_address"] for row in relevant}
            buyers = {row["to_address"] for row in relevant}
            seller = next(iter(sellers)) if len(sellers) == 1 else (event["offerer"] if direction == "LISTING" else None)
            buyer = next(iter(buyers)) if len(buyers) == 1 else (event["offerer"] if direction == "OFFER" else event["recipient"] or event["tx_from"])
            seller_net = None
            if seller and direction == "LISTING" and payment_token:
                seller_items = [item for item in currency_consideration if item.get("recipient", "").lower() == seller and (NATIVE if item["item_type"] == 0 else item["token"].lower()) == payment_token]
                if seller_items:
                    seller_net = sum(item["amount"] for item in seller_items)
            price_per_unit = (gross / units) if gross is not None and units else None
            sales.append(
                {
                    **{key: event[key] for key in ("order_hash", "block_number", "block_hash", "transaction_hash", "log_index", "timestamp_unix", "tx_from")},
                    "direction": direction,
                    "bundle_status": bundle_status,
                    "nft_contract": contract,
                    "nft_units": units,
                    "token_ids": sorted(token_ids[contract], key=lambda value: int(value)),
                    "seller": seller,
                    "buyer": buyer,
                    "payment_token": payment_token,
                    "gross_payment_raw": gross,
                    "seller_net_raw": seller_net,
                    "price_per_unit_raw": price_per_unit,
                    "is_native_or_weth": payment_token == NATIVE or payment_token in KNOWN_WETH,
                    "independent_party": bool(seller and buyer and seller != buyer),
                    "transfer_rows_matched": len(relevant),
                    "offer_json": event["offer"],
                    "consideration_json": event["consideration"],
                }
            )
    return sales


def project_tables(seadrop: list[dict[str, Any]], transfers: list[dict[str, Any]], sales: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    by_contract: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in seadrop:
        by_contract[row["nft_contract"]].append(row)
    zero_mints_by_contract: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in transfers:
        if row["from_address"] == ZERO:
            zero_mints_by_contract[row["contract_address"]].append(row)
    sales_by_contract: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in sales:
        if row["is_native_or_weth"] and row["independent_party"] and row["gross_payment_raw"] is not None:
            sales_by_contract[row["nft_contract"]].append(row)

    opportunities: list[dict[str, Any]] = []
    outcomes: list[dict[str, Any]] = []
    entries: list[dict[str, Any]] = []
    milestones: list[dict[str, Any]] = []
    for contract, events in sorted(by_contract.items()):
        public_paid = sorted([row for row in events if row["drop_stage_index"] == 0 and row["unit_mint_price_wei"] > 0 and row["receipt_status"] == 1], key=lambda row: (row["block_number"], row["log_index"]))
        if not public_paid:
            continue
        first = public_paid[0]
        start_time = first["timestamp_unix"]
        prices = sorted({row["unit_mint_price_wei"] for row in public_paid})
        zero_seadrop_before = [row for row in events if row["unit_mint_price_wei"] == 0 and (row["block_number"], row["log_index"]) < (first["block_number"], first["log_index"])]
        zero_transfer_before = [row for row in zero_mints_by_contract.get(contract, []) if (row["block_number"], row["log_index"]) < (first["block_number"], first["log_index"])]
        total_paid_quantity = sum(row["quantity"] for row in public_paid)
        project_status = "STRICT_PAID_PUBLIC_FROM_START" if not zero_seadrop_before and not zero_transfer_before and len(prices) == 1 else "PAID_PUBLIC_WITH_COST_BASIS_DIVERGENCE"
        opportunity = {
            "project_entity_id": f"contract:{contract}",
            "nft_contract": contract,
            "primary_route": "SEADROP_PUBLIC",
            "project_status": project_status,
            "first_paid_public_block": first["block_number"],
            "first_paid_public_time": start_time,
            "initial_mint_price_wei": first["unit_mint_price_wei"],
            "observed_paid_public_prices_wei": prices,
            "paid_public_event_count": len(public_paid),
            "paid_public_quantity": total_paid_quantity,
            "zero_seadrop_events_before_paid": len(zero_seadrop_before),
            "zero_transfer_items_before_paid": len(zero_transfer_before),
        }
        opportunities.append(opportunity)

        project_sales = sorted([row for row in sales_by_contract.get(contract, []) if row["timestamp_unix"] >= start_time], key=lambda row: (row["timestamp_unix"], row["block_number"], row["log_index"]))
        milestone = {"project_entity_id": opportunity["project_entity_id"], "nft_contract": contract}
        for multiple in (1.0, 1.15, 1.5):
            qualifying = [row for row in project_sales if row["price_per_unit_raw"] is not None and row["price_per_unit_raw"] >= first["unit_mint_price_wei"] * multiple]
            milestone[f"first_{str(multiple).replace('.', '_')}x_sale_time"] = qualifying[0]["timestamp_unix"] if qualifying else None
        milestones.append(milestone)

        outcome = {**opportunity}
        for window_name, seconds in WINDOWS.items():
            window_sales = [row for row in project_sales if row["timestamp_unix"] <= start_time + seconds and row["bundle_status"] == "SINGLE_CONTRACT"]
            order_count = len({(row["transaction_hash"], row["log_index"], row["order_hash"]) for row in window_sales})
            buyers = {row["buyer"] for row in window_sales if row["buyer"]}
            prices_per_unit = [float(row["price_per_unit_raw"]) for row in window_sales if row["price_per_unit_raw"] is not None]
            outcome[f"orders_{window_name}"] = order_count
            outcome[f"buyers_{window_name}"] = len(buyers)
            outcome[f"sale_units_{window_name}"] = sum(row["nft_units"] for row in window_sales)
            outcome[f"max_multiple_{window_name}"] = (max(prices_per_unit) / first["unit_mint_price_wei"]) if prices_per_unit else None
            outcome[f"median_multiple_{window_name}"] = (statistics.median(prices_per_unit) / first["unit_mint_price_wei"]) if prices_per_unit else None
        outcome["liquid_100_24h"] = int((outcome["max_multiple_24h"] or 0) >= 1.0 and outcome["orders_24h"] >= 2 and outcome["buyers_24h"] >= 2)
        outcome["liquid_115_24h"] = int((outcome["max_multiple_24h"] or 0) >= 1.15 and outcome["orders_24h"] >= 2 and outcome["buyers_24h"] >= 2)
        outcome["liquid_150_24h"] = int((outcome["max_multiple_24h"] or 0) >= 1.5 and outcome["orders_24h"] >= 2 and outcome["buyers_24h"] >= 2)
        outcomes.append(outcome)

        cumulative = 0
        first_profit_time = milestone.get("first_1_0x_sale_time")
        for event in public_paid:
            quantile = cumulative / total_paid_quantity if total_paid_quantity else None
            cumulative += event["quantity"]
            entries.append(
                {
                    "project_entity_id": opportunity["project_entity_id"],
                    "nft_contract": contract,
                    "wallet": event["minter"],
                    "payer": event["payer"],
                    "tx_from": event["tx_from"],
                    "economic_role": "SELF_FUNDED" if event["payer"] == event["minter"] else "ROUTED_OR_SPONSORED",
                    "block_number": event["block_number"],
                    "transaction_hash": event["transaction_hash"],
                    "log_index": event["log_index"],
                    "timestamp_unix": event["timestamp_unix"],
                    "quantity": event["quantity"],
                    "unit_mint_price_wei": event["unit_mint_price_wei"],
                    "entry_quantity_quantile": quantile,
                    "early_20pct": bool(quantile is not None and quantile < 0.20),
                    "before_first_nonloss_sale": bool(first_profit_time is None or event["timestamp_unix"] < first_profit_time),
                    "entry_gas_cost_wei": event["entry_gas_cost_wei"],
                    "project_status": project_status,
                }
            )
    return opportunities, outcomes, entries, milestones


def wallet_selection_metrics(opportunities: list[dict[str, Any]], outcomes: list[dict[str, Any]], entries: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    opportunity_by_id = {row["project_entity_id"]: row for row in opportunities}
    outcome_by_id = {row["project_entity_id"]: row for row in outcomes}
    eligible_entries = [row for row in entries if row["economic_role"] == "SELF_FUNDED" and row["early_20pct"] and row["before_first_nonloss_sale"]]
    wallet_projects: dict[str, set[str]] = defaultdict(set)
    entry_rows: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in eligible_entries:
        wallet_projects[row["wallet"]].add(row["project_entity_id"])
        entry_rows[(row["wallet"], row["project_entity_id"])].append(row)

    matched_pairs: list[dict[str, Any]] = []
    metrics: list[dict[str, Any]] = []
    for wallet, entered in sorted(wallet_projects.items()):
        matched_by_label: dict[str, list[int]] = defaultdict(list)
        entered_labels: dict[str, list[int]] = defaultdict(list)
        for project_id in sorted(entered):
            project = opportunity_by_id[project_id]
            outcome = outcome_by_id[project_id]
            price = project["initial_mint_price_wei"]
            start = project["first_paid_public_time"]
            controls = []
            for candidate in opportunities:
                candidate_id = candidate["project_entity_id"]
                if candidate_id in entered:
                    continue
                candidate_price = candidate["initial_mint_price_wei"]
                if not candidate_price or not price:
                    continue
                if abs(candidate["first_paid_public_time"] - start) > 7 * 86400:
                    continue
                ratio = candidate_price / price
                if not (0.5 <= ratio <= 2.0):
                    continue
                if candidate["primary_route"] != project["primary_route"]:
                    continue
                controls.append(candidate_id)
            for label in LABELS:
                entered_labels[label].append(int(outcome[label]))
                control_values = [int(outcome_by_id[candidate_id][label]) for candidate_id in controls]
                matched_by_label[label].extend(control_values)
                matched_pairs.append(
                    {
                        "wallet": wallet,
                        "entered_project": project_id,
                        "label": label,
                        "entered_outcome": int(outcome[label]),
                        "matched_control_projects": controls,
                        "matched_control_count": len(controls),
                        "matched_control_success_rate": (sum(control_values) / len(control_values)) if control_values else None,
                    }
                )
        row: dict[str, Any] = {
            "wallet": wallet,
            "entered_projects": len(entered),
            "earliest_entry_time": min(min(value["timestamp_unix"] for value in entry_rows[(wallet, project_id)]) for project_id in entered),
            "median_entry_quantile": statistics.median(min(value["entry_quantity_quantile"] for value in entry_rows[(wallet, project_id)]) for project_id in entered),
            "production_approved": False,
            "decision_use": "RESEARCH_SELECTION_SIGNAL_ONLY",
        }
        positive_lifts = 0
        for label in LABELS:
            values = entered_labels[label]
            controls = matched_by_label[label]
            hit_rate = sum(values) / len(values) if values else None
            baseline = sum(controls) / len(controls) if controls else None
            lift = hit_rate - baseline if hit_rate is not None and baseline is not None else None
            row[f"{label}_hit_rate"] = hit_rate
            row[f"{label}_matched_baseline"] = baseline
            row[f"{label}_predictive_lift"] = lift
            row[f"{label}_matched_control_observations"] = len(controls)
            if lift is not None and lift > 0:
                positive_lifts += 1
        if len(entered) < 3:
            classification = "LOW_SAMPLE_NOT_EVALUATED"
        elif positive_lifts == len(LABELS):
            classification = "PROVISIONAL_POSITIVE_SELECTION_SIGNAL"
        elif positive_lifts:
            classification = "MIXED_SELECTION_SIGNAL"
        else:
            classification = "NO_POSITIVE_SELECTION_LIFT"
        row["selection_classification"] = classification
        metrics.append(row)
    return metrics, matched_pairs


def copy_proxy(entries: list[dict[str, Any]], seadrop: list[dict[str, Any]], sales: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    by_contract = defaultdict(list)
    for row in seadrop:
        if row["drop_stage_index"] == 0 and row["unit_mint_price_wei"] > 0 and row["payer"] == row["minter"] and row["receipt_status"] == 1:
            by_contract[row["nft_contract"]].append(row)
    sales_by_contract = defaultdict(list)
    for sale in sales:
        if sale["is_native_or_weth"] and sale["independent_party"] and sale["price_per_unit_raw"] is not None:
            sales_by_contract[sale["nft_contract"]].append(sale)
    rows: list[dict[str, Any]] = []
    for signal in entries:
        if not (signal["economic_role"] == "SELF_FUNDED" and signal["early_20pct"] and signal["before_first_nonloss_sale"]):
            continue
        candidates = sorted(by_contract.get(signal["nft_contract"], []), key=lambda row: (row["timestamp_unix"], row["block_number"], row["log_index"]))
        for delay_name, min_block_delta, min_seconds in (("1block", 1, 0), ("30s", 0, 30), ("60s", 0, 60)):
            later = [row for row in candidates if row["minter"] != signal["wallet"] and row["unit_mint_price_wei"] == signal["unit_mint_price_wei"] and row["block_number"] >= signal["block_number"] + min_block_delta and row["timestamp_unix"] >= signal["timestamp_unix"] + min_seconds]
            proxy = later[0] if later else None
            exit_sales = []
            if proxy:
                exit_sales = [sale for sale in sales_by_contract.get(signal["nft_contract"], []) if proxy["timestamp_unix"] <= sale["timestamp_unix"] <= proxy["timestamp_unix"] + 24 * 3600 and sale["bundle_status"] == "SINGLE_CONTRACT"]
            best_exit = max((sale["price_per_unit_raw"] for sale in exit_sales), default=None)
            entry_total = None
            if proxy:
                entry_total = proxy["unit_mint_price_wei"] + (proxy["entry_gas_cost_wei"] / max(proxy["quantity"], 1))
            rows.append(
                {
                    "signal_wallet": signal["wallet"],
                    "project_entity_id": signal["project_entity_id"],
                    "nft_contract": signal["nft_contract"],
                    "signal_transaction_hash": signal["transaction_hash"],
                    "delay": delay_name,
                    "proxy_entry_available": bool(proxy),
                    "proxy_entry_wallet": proxy["minter"] if proxy else None,
                    "proxy_entry_transaction_hash": proxy["transaction_hash"] if proxy else None,
                    "proxy_entry_time": proxy["timestamp_unix"] if proxy else None,
                    "proxy_entry_price_wei": proxy["unit_mint_price_wei"] if proxy else None,
                    "proxy_entry_gas_per_item_wei": (proxy["entry_gas_cost_wei"] / max(proxy["quantity"], 1)) if proxy else None,
                    "proxy_total_entry_cost_wei": entry_total,
                    "best_observed_sale_price_per_unit_24h": best_exit,
                    "market_exit_nonloss_proxy": bool(entry_total is not None and best_exit is not None and best_exit >= entry_total),
                    "status": "MARKET_EXIT_PROXY_ONLY_NOT_EXECUTABLE_BID",
                }
            )
    aggregate: list[dict[str, Any]] = []
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[(row["signal_wallet"], row["delay"])].append(row)
    for (wallet, delay), values in sorted(grouped.items()):
        available = [row for row in values if row["proxy_entry_available"]]
        aggregate.append(
            {
                "wallet": wallet,
                "delay": delay,
                "signals": len(values),
                "proxy_entries_available": len(available),
                "proxy_entry_availability_rate": len(available) / len(values) if values else None,
                "market_exit_nonloss_proxy_count": sum(row["market_exit_nonloss_proxy"] for row in available),
                "market_exit_nonloss_proxy_rate": sum(row["market_exit_nonloss_proxy"] for row in available) / len(available) if available else None,
                "status": "MARKET_EXIT_PROXY_ONLY_NOT_COPY_ALPHA",
                "production_approved": False,
            }
        )
    return rows, aggregate


def execution_research(seadrop: list[dict[str, Any]], transfers: list[dict[str, Any]], sales: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    seadrop_by_tx_contract = {(row["transaction_hash"], row["nft_contract"]): row for row in seadrop}
    token_events: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in transfers:
        if row["standard"] == "ERC-721" and int(row["amount"]) == 1:
            token_events[(row["contract_address"], row["token_id"])].append(row)
    sale_by_token: dict[tuple[str, str, str], dict[str, Any]] = {}
    for sale in sales:
        if sale["bundle_status"] != "SINGLE_CONTRACT" or sale["nft_units"] <= 0 or sale["seller_net_raw"] is None:
            continue
        unit_net = sale["seller_net_raw"] / sale["nft_units"]
        unit_gross = sale["gross_payment_raw"] / sale["nft_units"] if sale["gross_payment_raw"] is not None else None
        for token_id in sale["token_ids"]:
            sale_by_token[(sale["nft_contract"], token_id, sale["transaction_hash"])] = {**sale, "unit_seller_net": unit_net, "unit_gross": unit_gross}

    realized: list[dict[str, Any]] = []
    for (contract, token_id), events in token_events.items():
        events.sort(key=lambda row: (row["block_number"], row["transaction_index"], row["log_index"], row.get("batch_item_index", 0)))
        owner = None
        cost_basis = None
        acquisition_type = None
        acquisition_tx = None
        acquisition_gas = None
        for event in events:
            frm = event["from_address"]
            to = event["to_address"]
            if frm == ZERO:
                owner = to
                mint = seadrop_by_tx_contract.get((event["transaction_hash"], contract))
                if mint and mint["minter"] == to and mint["payer"] == to and mint["quantity"] > 0:
                    cost_basis = mint["unit_mint_price_wei"]
                    acquisition_gas = mint["entry_gas_cost_wei"] / mint["quantity"]
                    acquisition_type = "SELF_FUNDED_SEADROP_MINT"
                    acquisition_tx = event["transaction_hash"]
                else:
                    cost_basis = None
                    acquisition_gas = None
                    acquisition_type = "MINT_COST_UNRESOLVED"
                    acquisition_tx = event["transaction_hash"]
                continue
            sale = sale_by_token.get((contract, token_id, event["transaction_hash"]))
            if sale and owner == frm and sale["seller"] == frm and sale["buyer"] == to:
                exact = cost_basis is not None and acquisition_gas is not None
                net_pnl = sale["unit_seller_net"] - cost_basis - acquisition_gas if exact else None
                realized.append(
                    {
                        "wallet": frm,
                        "nft_contract": contract,
                        "token_id": token_id,
                        "acquisition_type": acquisition_type,
                        "acquisition_transaction_hash": acquisition_tx,
                        "sale_transaction_hash": sale["transaction_hash"],
                        "sale_order_hash": sale["order_hash"],
                        "acquisition_cost_wei": cost_basis,
                        "entry_gas_allocated_wei": acquisition_gas,
                        "seller_net_wei": sale["unit_seller_net"],
                        "exit_gas_allocated_wei": 0,
                        "net_realized_pnl_wei": net_pnl,
                        "pnl_status": "PROVEN" if exact else "ACQUISITION_COST_UNRESOLVED",
                        "sale_time": sale["timestamp_unix"],
                    }
                )
                owner = to
                cost_basis = sale["unit_gross"]
                acquisition_gas = None
                acquisition_type = "SECONDARY_PURCHASE_GAS_UNRESOLVED"
                acquisition_tx = sale["transaction_hash"]
            else:
                owner = to
                cost_basis = None
                acquisition_gas = None
                acquisition_type = "TRANSFERRED_COST_UNRESOLVED"
                acquisition_tx = event["transaction_hash"]

    wallet_rows: list[dict[str, Any]] = []
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in realized:
        grouped[row["wallet"]].append(row)
    for wallet, rows in sorted(grouped.items()):
        proven = [row for row in rows if row["pnl_status"] == "PROVEN"]
        pnls = [float(row["net_realized_pnl_wei"]) for row in proven]
        wallet_rows.append(
            {
                "wallet": wallet,
                "observed_sale_lots": len(rows),
                "proven_realized_sale_lots": len(proven),
                "proven_win_rate": sum(value > 0 for value in pnls) / len(pnls) if pnls else None,
                "median_realized_pnl_wei": statistics.median(pnls) if pnls else None,
                "total_realized_pnl_wei": sum(pnls) if pnls else None,
                "execution_classification": "PROVISIONAL_EXECUTION_DATA" if proven else "EXECUTION_NOT_EVALUATED",
                "production_approved": False,
            }
        )
    return realized, wallet_rows


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--chain-history", type=Path, required=True)
    parser.add_argument("--nft-population", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)

    chain_validation = json.loads((args.chain_history / "VALIDATION.json").read_text(encoding="utf-8"))
    nft_validation = json.loads((args.nft_population / "VALIDATION.json").read_text(encoding="utf-8"))
    if chain_validation.get("status") != "PASS" or nft_validation.get("status") != "PASS":
        raise SystemExit("upstream canonical data is not PASS")

    raw_seadrop = read_jsonl_gz(args.chain_history / "seadrop" / "seadrop_logs.jsonl.gz")
    raw_seaport = read_jsonl_gz(args.chain_history / "seaport" / "seaport_logs.jsonl.gz")
    transfers = read_jsonl_gz(args.nft_population / "all_transfers.jsonl.gz")
    raw_context_logs = raw_seadrop + raw_seaport
    blocks, txs, receipts, context_errors = fetch_chain_context(raw_context_logs)

    seadrop: list[dict[str, Any]] = []
    seadrop_errors: list[dict[str, Any]] = []
    for row in raw_seadrop:
        try:
            seadrop.append(decode_seadrop(row, blocks, txs, receipts))
        except Exception as exc:  # noqa: BLE001
            seadrop_errors.append({"transaction_hash": row.get("transactionHash"), "log_index": row.get("logIndex"), "error": repr(exc)})
    orders: list[dict[str, Any]] = []
    order_errors: list[dict[str, Any]] = []
    for row in raw_seaport:
        try:
            orders.append(decode_order_fulfilled(row, blocks, txs, receipts))
        except Exception as exc:  # noqa: BLE001
            order_errors.append({"transaction_hash": row.get("transactionHash"), "log_index": row.get("logIndex"), "error": repr(exc)})

    sales = sale_rows(orders, transfers)
    opportunities, outcomes, entries, milestones = project_tables(seadrop, transfers, sales)
    selection, matched_pairs = wallet_selection_metrics(opportunities, outcomes, entries)
    copy_rows, copy_wallets = copy_proxy(entries, seadrop, sales)
    realized, execution_wallets = execution_research(seadrop, transfers, sales)

    write_jsonl(args.out / "seadrop_mints.jsonl", seadrop)
    write_jsonl(args.out / "seaport_orders.jsonl", orders)
    write_csv(args.out / "seaport_sales.csv", sales)
    write_csv(args.out / "project_opportunities.csv", opportunities)
    write_csv(args.out / "project_outcomes.csv", outcomes)
    write_csv(args.out / "wallet_project_entries.csv", entries)
    write_csv(args.out / "project_sale_milestones.csv", milestones)
    write_csv(args.out / "wallet_selection_alpha.csv", selection)
    write_jsonl(args.out / "matched_baseline_pairs.jsonl", matched_pairs)
    write_csv(args.out / "copy_market_exit_proxy_events.csv", copy_rows)
    write_csv(args.out / "wallet_copy_market_exit_proxy.csv", copy_wallets)
    write_csv(args.out / "realized_token_sales.csv", realized)
    write_csv(args.out / "wallet_execution_alpha.csv", execution_wallets)
    (args.out / "chain_context_errors.json").write_text(json.dumps(context_errors, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    (args.out / "seadrop_decode_errors.json").write_text(json.dumps(seadrop_errors, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    (args.out / "seaport_decode_errors.json").write_text(json.dumps(order_errors, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")

    failures = []
    if context_errors:
        failures.append({"code": "CHAIN_CONTEXT_ERRORS", "count": len(context_errors)})
    if seadrop_errors or len(seadrop) != len(raw_seadrop):
        failures.append({"code": "SEADROP_DECODE_INCOMPLETE", "raw": len(raw_seadrop), "decoded": len(seadrop), "errors": len(seadrop_errors)})
    if order_errors or len(orders) != len(raw_seaport):
        failures.append({"code": "SEAPORT_DECODE_INCOMPLETE", "raw": len(raw_seaport), "decoded": len(orders), "errors": len(order_errors)})
    if not opportunities or len(outcomes) != len(opportunities):
        failures.append({"code": "PROJECT_UNIVERSE_INCOMPLETE", "opportunities": len(opportunities), "outcomes": len(outcomes)})
    validation = {
        "status": "PASS" if not failures else "FAIL",
        "raw_seadrop_logs": len(raw_seadrop),
        "decoded_seadrop_mints": len(seadrop),
        "raw_seaport_logs": len(raw_seaport),
        "decoded_seaport_orders": len(orders),
        "sale_rows": len(sales),
        "project_opportunities": len(opportunities),
        "project_outcomes": len(outcomes),
        "wallet_project_entries": len(entries),
        "wallet_selection_rows": len(selection),
        "copy_proxy_event_rows": len(copy_rows),
        "execution_sale_lots": len(realized),
        "execution_wallet_rows": len(execution_wallets),
        "failures": failures,
        "production_approved_wallets": 0,
        "live_mint_decision_ready": False,
    }
    (args.out / "VALIDATION.json").write_text(json.dumps(validation, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    manifest = []
    for path in sorted(args.out.iterdir()):
        if path.is_file():
            manifest.append({"path": path.name, "bytes": path.stat().st_size, "sha256": sha256(path)})
    (args.out / "MANIFEST.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(validation, sort_keys=True))
    if validation["status"] != "PASS":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
