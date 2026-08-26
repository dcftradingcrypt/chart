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
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

OUT = Path(os.getenv("OUT", "out-seaport-v2-shard"))
OUT.mkdir(parents=True, exist_ok=True)
SHARD = int(os.getenv("SHARD", "0"))
SHARDS = int(os.getenv("SHARDS", "16"))
FIXED_END_BLOCK = int(os.getenv("GLOBAL_END_BLOCK", "46840468"))
BLOCKSCOUT = "https://robinhoodchain.blockscout.com/api"
SEAPORT = "0x0000000000000068f116a894984e2db1123eb395"
ORDER_FULFILLED = "0x9d9af8e38d66c62e2c12f0225249fd9d721c54b83f48d9352c97c6cacdcb6f31"
ZERO = "0x0000000000000000000000000000000000000000"
WETH = "0x0bd7d308f8e1639fab988df18a8011f41eacad73"
UA = f"RHC-Seaport-Sale-Research-v2/{SHARD}"
last_request = 0.0
api_calls = 0
rate_limit_backoffs = 0
scan_ranges: list[dict[str, Any]] = []


def pace() -> None:
    global last_request
    remaining = 1.05 - (time.monotonic() - last_request)
    if remaining > 0:
        time.sleep(remaining)


def get_json(url: str, attempts: int = 18) -> Any:
    global last_request, api_calls, rate_limit_backoffs
    last = None
    for attempt in range(attempts):
        pace()
        api_calls += 1
        try:
            req = urllib.request.Request(url, headers={"user-agent": UA, "accept": "application/json"})
            with urllib.request.urlopen(req, timeout=120) as response:
                raw = response.read()
            last_request = time.monotonic()
            return json.loads(raw.decode())
        except urllib.error.HTTPError as exc:
            last = exc
            last_request = time.monotonic()
            if exc.code == 429:
                rate_limit_backoffs += 1
                wait = min(180, 35 + attempt * 12 + random.random() * 10)
                print(f"429 backoff {wait:.1f}s", flush=True)
                time.sleep(wait)
                continue
            if exc.code in (500, 502, 503, 504):
                time.sleep(min(45, 2**attempt + random.random()))
                continue
            raise
        except Exception as exc:
            last = exc
            time.sleep(min(45, 2**attempt + random.random()))
    raise RuntimeError(f"{url}: {last}")


def fetch_range(start: int, end: int, depth: int = 0) -> list[dict[str, Any]]:
    params = {
        "module": "logs",
        "action": "getLogs",
        "fromBlock": start,
        "toBlock": end,
        "address": SEAPORT,
        "topic0": ORDER_FULFILLED,
    }
    data = get_json(BLOCKSCOUT + "?" + urllib.parse.urlencode(params))
    result = data.get("result") if isinstance(data, dict) else None
    if isinstance(result, str):
        if data.get("status") == "0" or "No logs found" in result:
            scan_ranges.append({"from_block": start, "to_block": end, "depth": depth, "rows": 0, "action": "ACCEPT_EMPTY"})
            return []
        raise RuntimeError(result)
    result = result or []
    if len(result) < 1000:
        scan_ranges.append({"from_block": start, "to_block": end, "depth": depth, "rows": len(result), "action": "ACCEPT"})
        return result
    if start >= end:
        raise RuntimeError(f"1000-log unresolved leaf at block {start}")
    middle = (start + end) // 2
    scan_ranges.append({"from_block": start, "to_block": end, "depth": depth, "rows": len(result), "action": "SPLIT"})
    return fetch_range(start, middle, depth + 1) + fetch_range(middle + 1, end, depth + 1)


def word(blob: bytes, word_index: int) -> int:
    start = word_index * 32
    end = start + 32
    if end > len(blob):
        raise ValueError(f"word {word_index} outside blob of {len(blob)} bytes")
    return int.from_bytes(blob[start:end], "big")


def int_address(value: int) -> str:
    return "0x" + value.to_bytes(32, "big")[-20:].hex()


def topic_address(topic: str) -> str:
    return "0x" + topic[-40:].lower()


def decode_order_fulfilled(data_hex: str) -> tuple[str, str, list[dict[str, Any]], list[dict[str, Any]]]:
    blob = bytes.fromhex(data_hex[2:] if data_hex.startswith("0x") else data_hex)
    if len(blob) < 128 or len(blob) % 32:
        raise ValueError(f"invalid ABI data length {len(blob)}")
    order_hash = "0x" + blob[:32].hex()
    recipient = int_address(word(blob, 1))
    offer_offset = word(blob, 2)
    consideration_offset = word(blob, 3)
    if offer_offset % 32 or consideration_offset % 32:
        raise ValueError("dynamic array offset is not word aligned")
    offer_start = offer_offset // 32
    consideration_start = consideration_offset // 32
    offer_count = word(blob, offer_start)
    consideration_count = word(blob, consideration_start)
    if offer_count > 10000 or consideration_count > 10000:
        raise ValueError("implausible item count")
    offer: list[dict[str, Any]] = []
    for index in range(offer_count):
        pos = offer_start + 1 + index * 4
        offer.append(
            {
                "item_type": word(blob, pos),
                "token": int_address(word(blob, pos + 1)),
                "identifier": word(blob, pos + 2),
                "amount": word(blob, pos + 3),
            }
        )
    consideration: list[dict[str, Any]] = []
    for index in range(consideration_count):
        pos = consideration_start + 1 + index * 5
        consideration.append(
            {
                "item_type": word(blob, pos),
                "token": int_address(word(blob, pos + 1)),
                "identifier": word(blob, pos + 2),
                "amount": word(blob, pos + 3),
                "recipient": int_address(word(blob, pos + 4)),
            }
        )
    return order_hash, recipient, offer, consideration


def asset_class(item_type: int) -> str:
    if item_type == 0:
        return "NATIVE"
    if item_type == 1:
        return "ERC20"
    if item_type in (2, 4):
        return "ERC721"
    if item_type in (3, 5):
        return "ERC1155"
    return "UNKNOWN"


def add_by_token(target: dict[str, int], rows: list[dict[str, Any]]) -> None:
    for row in rows:
        token = row["token"]
        target[token] = target.get(token, 0) + int(row["amount"])


def subtract_maps(left: dict[str, int], right: dict[str, int]) -> dict[str, int | None]:
    tokens = set(left) | set(right)
    return {token: left.get(token, 0) - right.get(token, 0) for token in sorted(tokens)}


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


def main() -> None:
    span = FIXED_END_BLOCK + 1
    start = (span * SHARD) // SHARDS
    end = (span * (SHARD + 1)) // SHARDS - 1
    print(f"seaport-v2 shard={SHARD}/{SHARDS} range={start}-{end}", flush=True)
    raw = fetch_range(start, end)
    dedup = {(str(row.get("transactionHash")).lower(), str(row.get("logIndex"))): row for row in raw}
    raw = sorted(
        dedup.values(),
        key=lambda row: (int(str(row.get("blockNumber", "0x0")), 16), int(str(row.get("logIndex", "0x0")), 16)),
    )

    orders: list[dict[str, Any]] = []
    sale_items: list[dict[str, Any]] = []
    decode_errors: list[dict[str, Any]] = []
    for log in raw:
        topics = log.get("topics") or []
        tx_hash = str(log.get("transactionHash")).lower()
        log_index = int(str(log.get("logIndex", "0x0")), 16)
        if len(topics) < 3:
            decode_errors.append({"transaction_hash": tx_hash, "log_index": log_index, "error": "TOPICS_LT_3"})
            continue
        try:
            order_hash, recipient, offer, consideration = decode_order_fulfilled(str(log.get("data", "0x")))
        except Exception as exc:
            decode_errors.append({"transaction_hash": tx_hash, "log_index": log_index, "error": repr(exc)})
            continue

        offerer = topic_address(topics[1])
        zone = topic_address(topics[2])
        for row in offer:
            row["asset_class"] = asset_class(int(row["item_type"]))
        for row in consideration:
            row["asset_class"] = asset_class(int(row["item_type"]))
        offer_nfts = [row for row in offer if row["asset_class"] in ("ERC721", "ERC1155")]
        consideration_nfts = [row for row in consideration if row["asset_class"] in ("ERC721", "ERC1155")]
        offer_currency = [row for row in offer if row["asset_class"] in ("NATIVE", "ERC20")]
        consideration_currency = [row for row in consideration if row["asset_class"] in ("NATIVE", "ERC20")]

        buyer_gross_by_asset: dict[str, int] = {}
        seller_gross_by_asset: dict[str, int] = {}
        seller_fee_by_asset: dict[str, int] = {}
        seller_net_by_asset: dict[str, int | None] = {}
        if offer_nfts and not consideration_nfts:
            direction = "LISTING_NFT_IN_OFFER"
            seller = offerer
            buyer = recipient
            add_by_token(buyer_gross_by_asset, consideration_currency)
            for row in consideration_currency:
                if row["recipient"] == seller:
                    seller_gross_by_asset[row["token"]] = seller_gross_by_asset.get(row["token"], 0) + int(row["amount"])
                else:
                    seller_fee_by_asset[row["token"]] = seller_fee_by_asset.get(row["token"], 0) + int(row["amount"])
            seller_net_by_asset = dict(seller_gross_by_asset)
            nft_rows = offer_nfts
            proceeds_status = "EXACT_LISTING_CONSIDERATION_RECIPIENTS"
        elif consideration_nfts and not offer_nfts:
            direction = "OFFER_NFT_IN_CONSIDERATION"
            buyer = offerer
            seller = recipient
            add_by_token(buyer_gross_by_asset, offer_currency)
            add_by_token(seller_gross_by_asset, offer_currency)
            add_by_token(seller_fee_by_asset, consideration_currency)
            seller_net_by_asset = subtract_maps(seller_gross_by_asset, seller_fee_by_asset)
            nft_rows = consideration_nfts
            proceeds_status = "EXACT_OFFER_SPEND_MINUS_SELLER_SIDE_CONSIDERATION_FEES"
        else:
            direction = "COMPLEX_OR_BARTER"
            buyer = None
            seller = None
            add_by_token(buyer_gross_by_asset, offer_currency)
            add_by_token(buyer_gross_by_asset, consideration_currency)
            seller_net_by_asset = {}
            nft_rows = offer_nfts + consideration_nfts
            proceeds_status = "UNRESOLVED"

        timestamp = log.get("timeStamp")
        timestamp = int(timestamp, 16) if isinstance(timestamp, str) and timestamp.startswith("0x") else None
        base = {
            "shard": SHARD,
            "transaction_hash": tx_hash,
            "log_index": log_index,
            "block_number": int(str(log.get("blockNumber", "0x0")), 16),
            "timestamp_utc": datetime.fromtimestamp(timestamp, timezone.utc).isoformat().replace("+00:00", "Z") if timestamp else None,
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
            "buyer_gross_by_asset": buyer_gross_by_asset,
            "seller_gross_by_asset": seller_gross_by_asset,
            "seller_fee_by_asset": seller_fee_by_asset,
            "seller_net_by_asset": seller_net_by_asset,
            "proceeds_status": proceeds_status,
            "source": "BLOCKSCOUT_CANONICAL_SEAPORT_ORDER_FULFILLED_V2",
        }
        orders.append({**base, "offer": offer, "consideration": consideration})

        payment_tokens = set(buyer_gross_by_asset) | set(seller_net_by_asset)
        single_payment_asset = len(payment_tokens) == 1
        payment_token = next(iter(payment_tokens)) if single_payment_asset else None
        exact_allocation = len(nft_rows) == 1 and single_payment_asset and direction != "COMPLEX_OR_BARTER"
        buyer_gross = buyer_gross_by_asset.get(payment_token) if payment_token else None
        seller_gross = seller_gross_by_asset.get(payment_token) if payment_token else None
        seller_fee = seller_fee_by_asset.get(payment_token, 0) if payment_token else None
        seller_net = seller_net_by_asset.get(payment_token) if payment_token else None
        payment_asset_class = (
            "NATIVE"
            if payment_token == ZERO
            else ("WETH" if payment_token == WETH else ("ERC20_OTHER" if payment_token else "MULTI_OR_NONE"))
        )
        for nft in nft_rows:
            sale_items.append(
                {
                    **{
                        key: value
                        for key, value in base.items()
                        if key not in ("buyer_gross_by_asset", "seller_gross_by_asset", "seller_fee_by_asset", "seller_net_by_asset")
                    },
                    "nft_contract": nft["token"],
                    "token_id": nft["identifier"],
                    "nft_standard": nft["asset_class"],
                    "nft_amount": max(1, int(nft["amount"])),
                    "payment_token": payment_token,
                    "payment_asset_class": payment_asset_class,
                    "order_buyer_gross_raw": buyer_gross,
                    "order_seller_gross_raw": seller_gross,
                    "order_seller_fee_raw": seller_fee,
                    "order_seller_net_raw": seller_net,
                    "allocated_buyer_gross_raw": buyer_gross if exact_allocation else None,
                    "allocated_seller_net_raw": seller_net if exact_allocation else None,
                    "allocation_status": "EXACT_SINGLE_NFT_ITEM_SINGLE_PAYMENT_ASSET" if exact_allocation else "BUNDLE_OR_MULTI_ASSET_UNALLOCATED",
                    "buyer_gross_eth": buyer_gross / 1e18 if exact_allocation and buyer_gross is not None and payment_asset_class in ("NATIVE", "WETH") else None,
                    "seller_net_eth": seller_net / 1e18 if exact_allocation and seller_net is not None and payment_asset_class in ("NATIVE", "WETH") else None,
                }
            )

    write_csv(OUT / "orders.csv", orders)
    write_csv(OUT / "sale_items.csv", sale_items)
    write_csv(OUT / "decode_errors.csv", decode_errors)
    write_csv(OUT / "scan_ranges.csv", scan_ranges)
    with (OUT / "orders.jsonl").open("w", encoding="utf-8") as file:
        for row in orders:
            file.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")

    validation = {
        "status": "PASS" if len(orders) + len(decode_errors) == len(raw) and not decode_errors else "PARTIAL",
        "shard": SHARD,
        "shards": SHARDS,
        "from_block": start,
        "to_block": end,
        "fixed_end_block": FIXED_END_BLOCK,
        "raw_log_rows": len(raw),
        "decoded_order_rows": len(orders),
        "sale_item_rows": len(sale_items),
        "decode_error_rows": len(decode_errors),
        "exact_single_item_rows": sum(row["allocation_status"] == "EXACT_SINGLE_NFT_ITEM_SINGLE_PAYMENT_ASSET" for row in sale_items),
        "native_or_weth_exact_rows": sum(row["buyer_gross_eth"] is not None for row in sale_items),
        "api_calls": api_calls,
        "rate_limit_backoffs": rate_limit_backoffs,
        "scan_range_rows": len(scan_ranges),
    }
    (OUT / "validation.json").write_text(json.dumps(validation, indent=2), encoding="utf-8")
    print(json.dumps(validation), flush=True)
    if validation["status"] == "PARTIAL" and decode_errors:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
