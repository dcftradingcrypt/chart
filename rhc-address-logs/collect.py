#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import os
import random
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from pathlib import Path
from typing import Any

TARGET = os.environ.get("TARGET", "seadrop").strip().lower()
OUT = Path(os.environ.get("OUT", f"out-{TARGET}"))
OUT.mkdir(parents=True, exist_ok=True)
FIXED_END_BLOCK = int(os.environ.get("FIXED_END_BLOCK", "46840468"))
BASE = "https://robinhoodchain.blockscout.com/api/v2"
SEADROP = "0x00005ea00ac477b1030ce78506496e8c2de24bf5"
SEAPORT = "0x0000000000000068f116a894984e2db1123eb395"
SEADROP_TOPIC = "0xe90cf9cc0a552cf52ea6ff74ece0f1c8ae8cc9ad630d3181f55ac43ca076b7d6"
ORDER_FULFILLED_TOPIC = "0x9d9af8e38d66c62e2c12f0225249fd9d721c54b83f48d9352c97c6cacdcb6f31"
ZERO = "0x0000000000000000000000000000000000000000"
WETH = "0x0bd7d308f8e1639fab988df18a8011f41eacad73"
KNOWN_SEAPORT_TX = "0x3db5fe30892fe0ed96bb04de878a3220f84acd687ef486370d7973d0561914e2"
UA = f"RHC-Address-Log-Research/{TARGET}/0.1"
last_request = 0.0
http_calls = 0
rate_limit_backoffs = 0


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
    global last_request
    remaining = 0.45 - (time.monotonic() - last_request)
    if remaining > 0:
        time.sleep(remaining)


def get_json(url: str, attempts: int = 20) -> dict[str, Any]:
    global last_request, http_calls, rate_limit_backoffs
    last: Exception | None = None
    for attempt in range(attempts):
        pace()
        http_calls += 1
        try:
            request = urllib.request.Request(url, headers={"user-agent": UA, "accept": "application/json"})
            with urllib.request.urlopen(request, timeout=120) as response:
                payload = response.read()
            last_request = time.monotonic()
            data = json.loads(payload.decode())
            if not isinstance(data, dict):
                raise RuntimeError(f"unexpected response type {type(data)}")
            return data
        except urllib.error.HTTPError as exc:
            last = exc
            last_request = time.monotonic()
            if exc.code == 429:
                rate_limit_backoffs += 1
                retry_after = exc.headers.get("Retry-After")
                wait = float(retry_after) if retry_after and retry_after.isdigit() else min(180, 30 + attempt * 10 + random.random() * 10)
                print(f"429 backoff {wait:.1f}s", flush=True)
                time.sleep(wait)
                continue
            if exc.code in (500, 502, 503, 504):
                time.sleep(min(45, 2**attempt + random.random()))
                continue
            raise
        except Exception as exc:
            last = exc
            if attempt + 1 < attempts:
                time.sleep(min(45, 2**attempt + random.random()))
    raise RuntimeError(f"GET {url} failed: {last}")


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fields = sorted({key for row in rows for key in row}) if rows else ["empty"]
    with path.open("w", encoding="utf-8-sig", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fields, extrasaction="ignore")
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


def topic_address(topic: str) -> str:
    return "0x" + topic[-40:].lower()


def data_words(data: str) -> list[int]:
    value = data[2:] if data.startswith("0x") else data
    return [int(value[index : index + 64], 16) for index in range(0, len(value), 64) if len(value[index : index + 64]) == 64]


def first_topic(row: dict[str, Any]) -> str | None:
    topics = row.get("topics") or []
    if not topics:
        return None
    topic = topics[0]
    if isinstance(topic, dict):
        topic = topic.get("value") or topic.get("hash")
    return str(topic).lower() if topic else None


def normalize_topics(row: dict[str, Any]) -> list[str]:
    output: list[str] = []
    for topic in row.get("topics") or []:
        if isinstance(topic, dict):
            topic = topic.get("value") or topic.get("hash")
        if topic:
            output.append(str(topic).lower())
    return output


def fetch_all_address_logs(address: str, topic0: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    endpoint = f"{BASE}/addresses/{address}/logs"
    cursor: dict[str, Any] | None = None
    seen_cursors: set[str] = set()
    matching: list[dict[str, Any]] = []
    pages: list[dict[str, Any]] = []
    total_rows = 0
    for page_number in range(1, 20001):
        url = endpoint
        if cursor:
            url += "?" + urllib.parse.urlencode(cursor)
        data = get_json(url)
        items = data.get("items") or []
        if not isinstance(items, list):
            raise RuntimeError(f"items is not a list on page {page_number}")
        total_rows += len(items)
        page_blocks = [integer(item.get("block_number")) for item in items]
        page_blocks = [value for value in page_blocks if value is not None]
        retained = 0
        for item in items:
            block_number = integer(item.get("block_number"))
            if block_number is None or block_number > FIXED_END_BLOCK:
                continue
            if first_topic(item) == topic0:
                matching.append(item)
                retained += 1
        next_cursor = data.get("next_page_params")
        pages.append(
            {
                "page": page_number,
                "rows": len(items),
                "retained_topic_rows": retained,
                "min_block": min(page_blocks) if page_blocks else None,
                "max_block": max(page_blocks) if page_blocks else None,
                "next_page_params": next_cursor,
            }
        )
        if page_number % 100 == 0:
            print(f"{TARGET} page={page_number} total_rows={total_rows} topic_rows={len(matching)}", flush=True)
            (OUT / "pagination_checkpoint.json").write_text(
                json.dumps({"page": page_number, "total_rows": total_rows, "matching_rows": len(matching), "next_page_params": next_cursor}, indent=2),
                encoding="utf-8",
            )
        if not next_cursor:
            break
        cursor_key = json.dumps(next_cursor, sort_keys=True, separators=(",", ":"))
        if cursor_key in seen_cursors:
            raise RuntimeError(f"repeated pagination cursor on page {page_number}: {cursor_key}")
        seen_cursors.add(cursor_key)
        cursor = next_cursor
    else:
        raise RuntimeError("pagination exceeded 20,000 pages")
    pages.append({"page": "TOTAL", "rows": total_rows, "retained_topic_rows": len(matching)})
    return matching, pages


def decode_seadrop(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    events: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    dedup: dict[tuple[str, int], dict[str, Any]] = {}
    for row in rows:
        tx_hash = str(row.get("transaction_hash") or row.get("transactionHash") or "").lower()
        log_index = integer(row.get("index"), integer(row.get("log_index"), integer(row.get("logIndex"), -1))) or 0
        dedup[(tx_hash, log_index)] = row
    for (tx_hash, log_index), row in sorted(dedup.items(), key=lambda item: (integer(item[1].get("block_number"), 0) or 0, item[0][1])):
        topics = normalize_topics(row)
        decoded = data_words(str(row.get("data") or "0x"))
        if len(topics) < 4 or len(decoded) < 5:
            errors.append({"transaction_hash": tx_hash, "log_index": log_index, "error": "TOPIC_OR_DATA_LENGTH"})
            continue
        events.append(
            {
                "transaction_hash": tx_hash,
                "log_index": log_index,
                "block_number": integer(row.get("block_number")),
                "nft_contract": topic_address(topics[1]),
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
                "source": "BLOCKSCOUT_V2_ADDRESS_LOGS_SEADROP",
            }
        )
    by_contract: dict[str, list[dict[str, Any]]] = defaultdict(list)
    wallet_projects: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for event in events:
        by_contract[event["nft_contract"]].append(event)
        wallet_projects[(event["minter"], event["nft_contract"])].append(event)
    collections: list[dict[str, Any]] = []
    for contract, contract_rows in sorted(by_contract.items(), key=lambda item: min(row["block_number"] or 0 for row in item[1])):
        contract_rows = sorted(contract_rows, key=lambda row: (row["block_number"] or 0, row["log_index"]))
        total = sum(row["quantity"] for row in contract_rows)
        free = sum(row["quantity"] for row in contract_rows if row["is_free"])
        paid = total - free
        first = contract_rows[0]
        model = "MIXED_FREE_AND_PAID_OBSERVED" if free and paid else ("FREE_ONLY_OBSERVED" if free else "PAID_ONLY_OBSERVED")
        collections.append(
            {
                "nft_contract": contract,
                "first_mint_block": first["block_number"],
                "first_mint_price_wei": first["unit_mint_price_wei"],
                "first_stage_index": first["drop_stage_index"],
                "last_mint_block": contract_rows[-1]["block_number"],
                "event_count": len(contract_rows),
                "minted_quantity": total,
                "free_quantity": free,
                "paid_quantity": paid,
                "unique_minters": len({row["minter"] for row in contract_rows}),
                "unique_payers": len({row["payer"] for row in contract_rows}),
                "observed_stage_indexes": sorted({row["drop_stage_index"] for row in contract_rows}),
                "observed_prices_wei": sorted({row["unit_mint_price_wei"] for row in contract_rows}),
                "observed_model": model,
                "paid_from_first_observed": bool(first["is_paid"] and free == 0),
                "production_approved": False,
            }
        )
    wallet_rows: list[dict[str, Any]] = []
    for (wallet, contract), wallet_contract_rows in sorted(wallet_projects.items()):
        wallet_contract_rows = sorted(wallet_contract_rows, key=lambda row: (row["block_number"] or 0, row["log_index"]))
        wallet_rows.append(
            {
                "wallet": wallet,
                "nft_contract": contract,
                "first_entry_block": wallet_contract_rows[0]["block_number"],
                "first_entry_price_wei": wallet_contract_rows[0]["unit_mint_price_wei"],
                "first_entry_stage_index": wallet_contract_rows[0]["drop_stage_index"],
                "mint_event_count": len(wallet_contract_rows),
                "minted_quantity": sum(row["quantity"] for row in wallet_contract_rows),
                "free_quantity": sum(row["quantity"] for row in wallet_contract_rows if row["is_free"]),
                "paid_quantity": sum(row["quantity"] for row in wallet_contract_rows if row["is_paid"]),
                "total_primary_cost_wei": sum(row["gross_mint_value_wei"] for row in wallet_contract_rows),
                "total_primary_cost_eth": sum(row["gross_mint_value_eth"] for row in wallet_contract_rows),
                "production_approved": False,
            }
        )
    return events, collections, wallet_rows + [{"__decode_errors__": errors}]


def word(blob: bytes, index: int) -> int:
    start = index * 32
    end = start + 32
    if end > len(blob):
        raise ValueError("word outside ABI payload")
    return int.from_bytes(blob[start:end], "big")


def int_address(value: int) -> str:
    return "0x" + value.to_bytes(32, "big")[-20:].hex()


def decode_order_payload(data_hex: str) -> tuple[str, str, list[dict[str, Any]], list[dict[str, Any]]]:
    blob = bytes.fromhex(data_hex[2:] if data_hex.startswith("0x") else data_hex)
    order_hash = "0x" + blob[:32].hex()
    recipient = int_address(word(blob, 1))
    offer_start = word(blob, 2) // 32
    consideration_start = word(blob, 3) // 32
    offer_count = word(blob, offer_start)
    consideration_count = word(blob, consideration_start)
    offer = []
    for item_index in range(offer_count):
        position = offer_start + 1 + item_index * 4
        offer.append({"item_type": word(blob, position), "token": int_address(word(blob, position + 1)), "identifier": word(blob, position + 2), "amount": word(blob, position + 3)})
    consideration = []
    for item_index in range(consideration_count):
        position = consideration_start + 1 + item_index * 5
        consideration.append({"item_type": word(blob, position), "token": int_address(word(blob, position + 1)), "identifier": word(blob, position + 2), "amount": word(blob, position + 3), "recipient": int_address(word(blob, position + 4))})
    return order_hash, recipient, offer, consideration


def asset_class(item_type: int) -> str:
    return {0: "NATIVE", 1: "ERC20", 2: "ERC721", 3: "ERC1155", 4: "ERC721", 5: "ERC1155"}.get(item_type, "UNKNOWN")


def add_by_token(target: dict[str, int], rows: list[dict[str, Any]]) -> None:
    for row in rows:
        target[row["token"]] = target.get(row["token"], 0) + int(row["amount"])


def decode_seaport(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    dedup: dict[tuple[str, int], dict[str, Any]] = {}
    for row in rows:
        tx_hash = str(row.get("transaction_hash") or row.get("transactionHash") or "").lower()
        log_index = integer(row.get("index"), integer(row.get("log_index"), integer(row.get("logIndex"), -1))) or 0
        dedup[(tx_hash, log_index)] = row
    orders: list[dict[str, Any]] = []
    sale_items: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    for (tx_hash, log_index), row in sorted(dedup.items(), key=lambda item: (integer(item[1].get("block_number"), 0) or 0, item[0][1])):
        topics = normalize_topics(row)
        if len(topics) < 3:
            errors.append({"transaction_hash": tx_hash, "log_index": log_index, "error": "TOPICS_LT_3"})
            continue
        try:
            order_hash, recipient, offer, consideration = decode_order_payload(str(row.get("data") or "0x"))
        except Exception as exc:
            errors.append({"transaction_hash": tx_hash, "log_index": log_index, "error": repr(exc)})
            continue
        offerer = topic_address(topics[1])
        zone = topic_address(topics[2])
        for item in offer:
            item["asset_class"] = asset_class(int(item["item_type"]))
        for item in consideration:
            item["asset_class"] = asset_class(int(item["item_type"]))
        offer_nfts = [item for item in offer if item["asset_class"] in ("ERC721", "ERC1155")]
        consideration_nfts = [item for item in consideration if item["asset_class"] in ("ERC721", "ERC1155")]
        offer_currency = [item for item in offer if item["asset_class"] in ("NATIVE", "ERC20")]
        consideration_currency = [item for item in consideration if item["asset_class"] in ("NATIVE", "ERC20")]
        buyer_gross: dict[str, int] = {}
        seller_gross: dict[str, int] = {}
        seller_fees: dict[str, int] = {}
        seller_net: dict[str, int] = {}
        if offer_nfts and not consideration_nfts:
            direction = "LISTING_NFT_IN_OFFER"
            seller = offerer
            buyer = recipient
            add_by_token(buyer_gross, consideration_currency)
            for item in consideration_currency:
                destination = seller_gross if item["recipient"] == seller else seller_fees
                destination[item["token"]] = destination.get(item["token"], 0) + int(item["amount"])
            seller_net = dict(seller_gross)
            nft_rows = offer_nfts
            proceeds_status = "EXACT_LISTING_CONSIDERATION_RECIPIENTS"
        elif consideration_nfts and not offer_nfts:
            direction = "OFFER_NFT_IN_CONSIDERATION"
            buyer = offerer
            seller = recipient
            add_by_token(buyer_gross, offer_currency)
            add_by_token(seller_gross, offer_currency)
            add_by_token(seller_fees, consideration_currency)
            seller_net = {token: amount - seller_fees.get(token, 0) for token, amount in seller_gross.items()}
            nft_rows = consideration_nfts
            proceeds_status = "EXACT_OFFER_SPEND_MINUS_SELLER_SIDE_FEES"
        else:
            direction = "COMPLEX_OR_BARTER"
            buyer = None
            seller = None
            nft_rows = offer_nfts + consideration_nfts
            proceeds_status = "UNRESOLVED"
        base = {
            "transaction_hash": tx_hash,
            "log_index": log_index,
            "block_number": integer(row.get("block_number")),
            "order_hash": order_hash,
            "offerer": offerer,
            "zone": zone,
            "recipient": recipient,
            "direction": direction,
            "seller": seller,
            "buyer": buyer,
            "offer_item_count": len(offer),
            "consideration_item_count": len(consideration),
            "nft_item_count": len(nft_rows),
            "buyer_gross_by_asset": buyer_gross,
            "seller_gross_by_asset": seller_gross,
            "seller_fee_by_asset": seller_fees,
            "seller_net_by_asset": seller_net,
            "proceeds_status": proceeds_status,
            "source": "BLOCKSCOUT_V2_ADDRESS_LOGS_SEAPORT",
        }
        orders.append({**base, "offer": offer, "consideration": consideration})
        payment_tokens = set(buyer_gross) | set(seller_net)
        single_asset = len(payment_tokens) == 1
        payment_token = next(iter(payment_tokens)) if single_asset else None
        exact = len(nft_rows) == 1 and single_asset and direction != "COMPLEX_OR_BARTER"
        buyer_gross_raw = buyer_gross.get(payment_token) if payment_token else None
        seller_net_raw = seller_net.get(payment_token) if payment_token else None
        payment_class = "NATIVE" if payment_token == ZERO else ("WETH" if payment_token == WETH else ("ERC20_OTHER" if payment_token else "MULTI_OR_NONE"))
        for nft in nft_rows:
            sale_items.append(
                {
                    **{key: value for key, value in base.items() if not key.endswith("_by_asset")},
                    "nft_contract": nft["token"],
                    "token_id": nft["identifier"],
                    "nft_standard": nft["asset_class"],
                    "nft_amount": max(1, int(nft["amount"])),
                    "payment_token": payment_token,
                    "payment_asset_class": payment_class,
                    "order_buyer_gross_raw": buyer_gross_raw,
                    "order_seller_net_raw": seller_net_raw,
                    "allocated_buyer_gross_raw": buyer_gross_raw if exact else None,
                    "allocated_seller_net_raw": seller_net_raw if exact else None,
                    "allocation_status": "EXACT_SINGLE_NFT_ITEM_SINGLE_PAYMENT_ASSET" if exact else "BUNDLE_OR_MULTI_ASSET_UNALLOCATED",
                    "buyer_gross_eth": buyer_gross_raw / 1e18 if exact and buyer_gross_raw is not None and payment_class in ("NATIVE", "WETH") else None,
                    "seller_net_eth": seller_net_raw / 1e18 if exact and seller_net_raw is not None and payment_class in ("NATIVE", "WETH") else None,
                }
            )
    return orders, sale_items, errors


def main() -> None:
    if TARGET == "seadrop":
        address, topic = SEADROP, SEADROP_TOPIC
    elif TARGET == "seaport":
        address, topic = SEAPORT, ORDER_FULFILLED_TOPIC
    else:
        raise ValueError(f"unsupported TARGET={TARGET}")
    rows, page_rows = fetch_all_address_logs(address, topic)
    write_csv(OUT / "pagination_pages.csv", page_rows)
    if TARGET == "seadrop":
        events, collections, wallet_plus = decode_seadrop(rows)
        decode_errors = wallet_plus[-1]["__decode_errors__"] if wallet_plus and "__decode_errors__" in wallet_plus[-1] else []
        wallet_rows = wallet_plus[:-1] if decode_errors or (wallet_plus and "__decode_errors__" in wallet_plus[-1]) else wallet_plus
        write_csv(OUT / "events.csv", events)
        write_csv(OUT / "collections.csv", collections)
        write_csv(OUT / "wallet_project_entries.csv", wallet_rows)
        write_csv(OUT / "decode_errors.csv", decode_errors)
        known_contracts = {"0xb433123b8657dacf3b246b3e25f8952a0cd2f121", "0xf885faf151e3362ad1634b7f2f5c43338746fbba"}
        observed_contracts = {row["nft_contract"] for row in events}
        validation = {
            "status": "PASS" if events and not decode_errors and known_contracts <= observed_contracts else "FAIL",
            "target": TARGET,
            "fixed_end_block": FIXED_END_BLOCK,
            "matching_log_rows": len(rows),
            "decoded_event_rows": len(events),
            "collection_rows": len(collections),
            "wallet_project_rows": len(wallet_rows),
            "decode_error_rows": len(decode_errors),
            "paid_only_collections": sum(row["observed_model"] == "PAID_ONLY_OBSERVED" for row in collections),
            "mixed_collections": sum(row["observed_model"] == "MIXED_FREE_AND_PAID_OBSERVED" for row in collections),
            "free_only_collections": sum(row["observed_model"] == "FREE_ONLY_OBSERVED" for row in collections),
        }
    else:
        orders, sale_items, decode_errors = decode_seaport(rows)
        write_csv(OUT / "orders.csv", orders)
        write_csv(OUT / "sale_items.csv", sale_items)
        write_csv(OUT / "decode_errors.csv", decode_errors)
        known_rows = [row for row in orders if row["transaction_hash"] == KNOWN_SEAPORT_TX]
        validation = {
            "status": "PASS" if orders and not decode_errors and len(known_rows) == 5 else "FAIL",
            "target": TARGET,
            "fixed_end_block": FIXED_END_BLOCK,
            "matching_log_rows": len(rows),
            "decoded_order_rows": len(orders),
            "sale_item_rows": len(sale_items),
            "decode_error_rows": len(decode_errors),
            "known_probe_order_rows": len(known_rows),
            "exact_single_item_rows": sum(row["allocation_status"] == "EXACT_SINGLE_NFT_ITEM_SINGLE_PAYMENT_ASSET" for row in sale_items),
            "native_or_weth_exact_rows": sum(row["buyer_gross_eth"] is not None for row in sale_items),
        }
    validation.update({"pagination_pages": len(page_rows) - 1, "http_calls": http_calls, "rate_limit_backoffs": rate_limit_backoffs})
    (OUT / "validation.json").write_text(json.dumps(validation, indent=2), encoding="utf-8")
    print(json.dumps(validation), flush=True)
    if validation["status"] != "PASS":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
