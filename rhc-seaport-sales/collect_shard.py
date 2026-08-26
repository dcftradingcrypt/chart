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

from eth_abi import decode

OUT = Path(os.getenv("OUT", "out-seaport-shard"))
OUT.mkdir(parents=True, exist_ok=True)
SHARD = int(os.getenv("SHARD", "0"))
SHARDS = int(os.getenv("SHARDS", "16"))
FIXED_END_BLOCK = int(os.getenv("GLOBAL_END_BLOCK", "46840468"))
BLOCKSCOUT = "https://robinhoodchain.blockscout.com/api"
SEAPORT = "0x0000000000000068f116a894984e2db1123eb395"
ORDER_FULFILLED = "0x9d9af8e38d66c62e2c12f0225249fd9d721c54b83f48d9352c97c6cacdcb6f31"
ZERO = "0x0000000000000000000000000000000000000000"
WETH = "0x0bd7d308f8e1639fab988df18a8011f41eacad73"
UA = f"RHC-Seaport-Sale-Research/{SHARD}"
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
    for i in range(attempts):
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
                wait = min(180, 35 + i * 12 + random.random() * 10)
                print(f"429 backoff {wait:.1f}s", flush=True)
                time.sleep(wait)
                continue
            if exc.code in (500, 502, 503, 504):
                time.sleep(min(45, 2**i + random.random()))
                continue
            raise
        except Exception as exc:
            last = exc
            time.sleep(min(45, 2**i + random.random()))
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


def topic_address(topic: str) -> str:
    return "0x" + topic[-40:].lower()


def normalized_address(value: Any) -> str:
    if isinstance(value, bytes):
        return "0x" + value[-20:].hex()
    return str(value).lower()


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


def main() -> None:
    span = FIXED_END_BLOCK + 1
    start = (span * SHARD) // SHARDS
    end = (span * (SHARD + 1)) // SHARDS - 1
    print(f"seaport shard={SHARD}/{SHARDS} range={start}-{end}", flush=True)
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
        if len(topics) < 3:
            decode_errors.append({"transaction_hash": log.get("transactionHash"), "log_index": log.get("logIndex"), "error": "TOPICS_LT_3"})
            continue
        try:
            order_hash, recipient, offer, consideration = decode(
                ["bytes32", "address", "(uint8,address,uint256,uint256)[]", "(uint8,address,uint256,uint256,address)[]"],
                bytes.fromhex(str(log.get("data", "0x"))[2:]),
            )
        except Exception as exc:
            decode_errors.append(
                {
                    "transaction_hash": str(log.get("transactionHash")).lower(),
                    "log_index": int(str(log.get("logIndex", "0x0")), 16),
                    "error": repr(exc),
                }
            )
            continue

        offerer = topic_address(topics[1])
        zone = topic_address(topics[2])
        recipient_address = normalized_address(recipient)
        offer_rows = [
            {
                "item_type": int(item_type),
                "asset_class": asset_class(int(item_type)),
                "token": normalized_address(token),
                "identifier": int(identifier),
                "amount": int(amount),
            }
            for item_type, token, identifier, amount in offer
        ]
        consideration_rows = [
            {
                "item_type": int(item_type),
                "asset_class": asset_class(int(item_type)),
                "token": normalized_address(token),
                "identifier": int(identifier),
                "amount": int(amount),
                "recipient": normalized_address(item_recipient),
            }
            for item_type, token, identifier, amount, item_recipient in consideration
        ]
        offer_nfts = [row for row in offer_rows if row["asset_class"] in ("ERC721", "ERC1155")]
        consideration_nfts = [row for row in consideration_rows if row["asset_class"] in ("ERC721", "ERC1155")]
        offer_currency = [row for row in offer_rows if row["asset_class"] in ("NATIVE", "ERC20")]
        consideration_currency = [row for row in consideration_rows if row["asset_class"] in ("NATIVE", "ERC20")]

        if offer_nfts and not consideration_nfts:
            direction = "LISTING_NFT_IN_OFFER"
            seller = offerer
            buyer = recipient_address
            payment_rows = consideration_currency
            gross_payment_by_asset: dict[str, int] = {}
            seller_proceeds_by_asset: dict[str, int] = {}
            for row in payment_rows:
                token = row["token"]
                gross_payment_by_asset[token] = gross_payment_by_asset.get(token, 0) + row["amount"]
                if row["recipient"] == seller:
                    seller_proceeds_by_asset[token] = seller_proceeds_by_asset.get(token, 0) + row["amount"]
            net_proceeds_status = "EXACT_FROM_CONSIDERATION_RECIPIENTS"
            nft_rows = offer_nfts
        elif consideration_nfts and not offer_nfts:
            direction = "OFFER_NFT_IN_CONSIDERATION"
            buyer = offerer
            seller = recipient_address
            payment_rows = offer_currency
            gross_payment_by_asset = {}
            for row in payment_rows:
                token = row["token"]
                gross_payment_by_asset[token] = gross_payment_by_asset.get(token, 0) + row["amount"]
            seller_proceeds_by_asset = {}
            net_proceeds_status = "GROSS_ONLY_OFFER_SIDE_FEES_UNRESOLVED"
            nft_rows = consideration_nfts
        else:
            direction = "COMPLEX_OR_BARTER"
            seller = None
            buyer = None
            gross_payment_by_asset = {}
            seller_proceeds_by_asset = {}
            net_proceeds_status = "UNRESOLVED"
            nft_rows = offer_nfts + consideration_nfts
            for row in offer_currency:
                gross_payment_by_asset[row["token"]] = gross_payment_by_asset.get(row["token"], 0) + row["amount"]
            for row in consideration_currency:
                gross_payment_by_asset[row["token"]] = gross_payment_by_asset.get(row["token"], 0) + row["amount"]

        timestamp = log.get("timeStamp")
        timestamp = int(timestamp, 16) if isinstance(timestamp, str) and timestamp.startswith("0x") else None
        base = {
            "shard": SHARD,
            "transaction_hash": str(log.get("transactionHash")).lower(),
            "log_index": int(str(log.get("logIndex", "0x0")), 16),
            "block_number": int(str(log.get("blockNumber", "0x0")), 16),
            "timestamp_utc": datetime.fromtimestamp(timestamp, timezone.utc).isoformat().replace("+00:00", "Z") if timestamp else None,
            "order_hash": "0x" + bytes(order_hash).hex(),
            "offerer": offerer,
            "zone": zone,
            "recipient": recipient_address,
            "direction": direction,
            "seller": seller,
            "buyer": buyer,
            "offer_item_count": len(offer_rows),
            "consideration_item_count": len(consideration_rows),
            "nft_item_count": len(nft_rows),
            "gross_payment_by_asset": gross_payment_by_asset,
            "seller_proceeds_by_asset": seller_proceeds_by_asset,
            "net_proceeds_status": net_proceeds_status,
            "single_nft_item": len(nft_rows) == 1,
            "source": "BLOCKSCOUT_CANONICAL_SEAPORT_ORDER_FULFILLED",
        }
        orders.append({**base, "offer": offer_rows, "consideration": consideration_rows})

        total_nft_units = sum(max(1, int(row["amount"])) for row in nft_rows)
        single_payment_asset = len(gross_payment_by_asset) == 1
        payment_token = next(iter(gross_payment_by_asset)) if single_payment_asset else None
        gross_payment_amount = next(iter(gross_payment_by_asset.values())) if single_payment_asset else None
        seller_proceeds_amount = seller_proceeds_by_asset.get(payment_token) if payment_token and seller_proceeds_by_asset else None
        payment_asset_class = (
            "NATIVE"
            if payment_token == ZERO
            else ("WETH" if payment_token == WETH else ("ERC20_OTHER" if payment_token else "MULTI_OR_NONE"))
        )
        for nft in nft_rows:
            item_amount = max(1, int(nft["amount"]))
            allocation_exact = len(nft_rows) == 1 and single_payment_asset
            sale_items.append(
                {
                    **{key: value for key, value in base.items() if key not in ("gross_payment_by_asset", "seller_proceeds_by_asset")},
                    "nft_contract": nft["token"],
                    "token_id": nft["identifier"],
                    "nft_standard": nft["asset_class"],
                    "nft_amount": item_amount,
                    "payment_token": payment_token,
                    "payment_asset_class": payment_asset_class,
                    "order_gross_payment_raw": gross_payment_amount,
                    "order_seller_proceeds_raw": seller_proceeds_amount,
                    "allocated_gross_payment_raw": gross_payment_amount if allocation_exact else None,
                    "allocated_seller_proceeds_raw": seller_proceeds_amount if allocation_exact else None,
                    "allocation_status": "EXACT_SINGLE_NFT_ITEM_SINGLE_PAYMENT_ASSET" if allocation_exact else "BUNDLE_OR_MULTI_ASSET_UNALLOCATED",
                    "native_or_weth_gross_eth": gross_payment_amount / 1e18 if allocation_exact and payment_asset_class in ("NATIVE", "WETH") else None,
                    "native_or_weth_seller_proceeds_eth": seller_proceeds_amount / 1e18 if allocation_exact and seller_proceeds_amount is not None and payment_asset_class in ("NATIVE", "WETH") else None,
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
        "native_or_weth_exact_rows": sum(row["native_or_weth_gross_eth"] is not None for row in sale_items),
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
