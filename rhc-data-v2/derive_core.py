#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import statistics
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from abi import (
    NFT_ITEM_TYPES,
    PAYMENT_ITEM_TYPES,
    decode_erc1155_batch,
    decode_erc1155_single,
    decode_erc2309,
    decode_erc721_transfer,
    decode_seadrop,
    decode_seaport,
    integer,
    parse_topics,
    topic_address,
    transfer_kind,
)
from topics import (
    CONSECUTIVE_TRANSFER,
    SEADROP_MINT,
    SEAPORT_ORDER_FULFILLED,
    TRANSFER,
    TRANSFER_BATCH,
    TRANSFER_SINGLE,
    ZERO_ADDRESS,
)

WINDOWS = {"15m": 900, "30m": 1800, "2h": 7200, "24h": 86400}
NATIVE = ZERO_ADDRESS
WETH_RHC = "0x0bd7d308f8e1639fab988df18a8011f41eacad73"


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows = []
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                value = json.loads(line)
                if isinstance(value, dict):
                    rows.append(value)
    return rows


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fields: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for key in row:
            if key not in seen:
                seen.add(key)
                fields.append(key)
    if not fields:
        fields = ["empty"]
    with path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({
                key: json.dumps(value, ensure_ascii=False, sort_keys=True)
                if isinstance(value, (dict, list, tuple)) else value
                for key, value in row.items()
            })


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def utc(timestamp: int | None) -> str | None:
    if timestamp is None:
        return None
    return datetime.fromtimestamp(timestamp, timezone.utc).isoformat().replace("+00:00", "Z")


def receipt_transfers(receipt: dict[str, Any]) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for log in receipt.get("logs") or []:
        topics = [str(value).lower() for value in (log.get("topics") or [])]
        if not topics:
            continue
        contract = str(log.get("address") or "").lower()
        log_index = integer(log.get("logIndex"))
        try:
            if topics[0] == TRANSFER and len(topics) == 4:
                value = decode_erc721_transfer(topics)
                output.append({"standard": "ERC721", "contract": contract, "log_index": log_index, **value})
            elif topics[0] == TRANSFER and len(topics) == 3:
                output.append({
                    "standard": "ERC20",
                    "contract": contract,
                    "log_index": log_index,
                    "from": topic_address(topics[1]),
                    "to": topic_address(topics[2]),
                    "token_id": 0,
                    "amount": integer(log.get("data")),
                })
            elif topics[0] == TRANSFER_SINGLE:
                value = decode_erc1155_single(topics, str(log.get("data") or "0x"))
                output.append({"standard": "ERC1155", "contract": contract, "log_index": log_index, **value})
            elif topics[0] == TRANSFER_BATCH:
                for value in decode_erc1155_batch(topics, str(log.get("data") or "0x")):
                    output.append({"standard": "ERC1155", "contract": contract, "log_index": log_index, **value})
        except Exception:
            continue
    return output


def load_inputs(root: Path) -> tuple[
    list[dict[str, str]],
    dict[str, list[dict[str, str]]],
    list[dict[str, str]],
    dict[str, dict[str, Any]],
    dict[str, dict[str, Any]],
    dict[int, dict[str, Any]],
    list[dict[str, Any]],
]:
    validations = []
    for path in root.rglob("VALIDATION.json"):
        try:
            validations.append({"path": str(path.relative_to(root)), **json.loads(path.read_text(encoding="utf-8"))})
        except Exception:
            pass
    failed = [row for row in validations if row.get("status") != "PASS"]
    if failed:
        raise SystemExit(json.dumps({"code": "UPSTREAM_VALIDATION_FAILURE", "failed": failed[:20], "count": len(failed)}, sort_keys=True))

    contract_paths = list(root.rglob("contracts.csv"))
    if len(contract_paths) != 1:
        raise SystemExit(f"expected exactly one contracts.csv, got {contract_paths}")
    contracts = read_csv(contract_paths[0])

    discovery: dict[str, list[dict[str, str]]] = defaultdict(list)
    for path in root.rglob("events.csv"):
        rows = read_csv(path)
        for row in rows:
            discovery[row.get("target") or path.parent.name].append(row)

    transfer_rows: list[dict[str, str]] = []
    for path in root.rglob("transfer_logs.csv"):
        transfer_rows.extend(read_csv(path))

    txs: dict[str, dict[str, Any]] = {}
    receipts: dict[str, dict[str, Any]] = {}
    blocks: dict[int, dict[str, Any]] = {}
    for path in root.rglob("transactions.jsonl"):
        for row in read_jsonl(path):
            tx_hash = str(row.get("hash") or "").lower()
            if tx_hash:
                txs[tx_hash] = row
    for path in root.rglob("receipts.jsonl"):
        for row in read_jsonl(path):
            tx_hash = str(row.get("transactionHash") or "").lower()
            if tx_hash:
                receipts[tx_hash] = row
    for path in root.rglob("blocks.jsonl"):
        for row in read_jsonl(path):
            number = integer(row.get("number"), -1)
            if number >= 0:
                blocks[number] = row

    known_wallet_paths = list(root.rglob("known-wallets.json"))
    known_wallets = json.loads(known_wallet_paths[0].read_text(encoding="utf-8")) if known_wallet_paths else []
    return contracts, discovery, transfer_rows, txs, receipts, blocks, known_wallets


def normalize_transfers(rows: list[dict[str, str]], block_times: dict[int, int]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    output: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    for row in rows:
        query = row.get("query") or ""
        topics = parse_topics(row.get("topics_json") or "[]")
        block = integer(row.get("block_number"))
        base = {
            "contract": row.get("contract", "").lower(),
            "block_number": block,
            "block_hash": row.get("block_hash", "").lower(),
            "timestamp_unix": block_times.get(block),
            "timestamp_utc": utc(block_times.get(block)),
            "transaction_hash": row.get("transaction_hash", "").lower(),
            "transaction_index": integer(row.get("transaction_index")),
            "log_index": integer(row.get("log_index")),
            "source_query": query,
        }
        try:
            if query == "ERC721_TRANSFER":
                value = decode_erc721_transfer(topics)
                output.append({**base, "standard": "ERC721", "range_transfer": False, **value, "event_kind": transfer_kind(value["from"], value["to"])})
            elif query == "ERC1155_TRANSFER_SINGLE":
                value = decode_erc1155_single(topics, row.get("data") or "0x")
                output.append({**base, "standard": "ERC1155", "range_transfer": False, **value, "event_kind": transfer_kind(value["from"], value["to"])})
            elif query == "ERC1155_TRANSFER_BATCH":
                for value in decode_erc1155_batch(topics, row.get("data") or "0x"):
                    output.append({**base, "standard": "ERC1155", "range_transfer": False, **value, "event_kind": transfer_kind(value["from"], value["to"])})
            elif query == "ERC2309_CONSECUTIVE_TRANSFER":
                value = decode_erc2309(topics, row.get("data") or "0x")
                output.append({**base, "standard": "ERC721", "range_transfer": True, "token_id": None, **value, "event_kind": transfer_kind(value["from"], value["to"])})
        except Exception as exc:
            errors.append({**base, "error": repr(exc), "topics_json": row.get("topics_json"), "data": row.get("data")})
    output.sort(key=lambda value: (value["block_number"], value["transaction_index"], value["log_index"], integer(value.get("batch_item_index"))))
    return output, errors


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-root", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    root = Path(args.input_root)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    contracts, discovery, transfer_source, txs, receipts, blocks, known_wallets = load_inputs(root)
    block_times = {number: integer(block.get("timestamp")) for number, block in blocks.items()}
    transfer_events, transfer_errors = normalize_transfers(transfer_source, block_times)

    # Decode canonical SeaDrop events.
    seadrop_events: list[dict[str, Any]] = []
    decode_errors: list[dict[str, Any]] = list(transfer_errors)
    for row in discovery.get("seadrop", []):
        block = integer(row.get("block_number"))
        try:
            decoded = decode_seadrop(parse_topics(row.get("topics_json") or "[]"), row.get("data") or "0x")
            seadrop_events.append({
                "route": "SEADROP",
                "contract": decoded["nft_contract"],
                "block_number": block,
                "block_hash": row.get("block_hash", "").lower(),
                "timestamp_unix": block_times.get(block),
                "timestamp_utc": utc(block_times.get(block)),
                "transaction_hash": row.get("transaction_hash", "").lower(),
                "transaction_index": integer(row.get("transaction_index")),
                "log_index": integer(row.get("log_index")),
                **decoded,
                "is_public": decoded["stage_index"] == 0,
                "is_paid": decoded["unit_price"] > 0,
                "is_self_funded": decoded["payer"] == decoded["minter"],
            })
        except Exception as exc:
            decode_errors.append({"source": "SEADROP", "transaction_hash": row.get("transaction_hash"), "log_index": row.get("log_index"), "error": repr(exc)})
    seadrop_by_tx_contract: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for event in seadrop_events:
        seadrop_by_tx_contract[(event["transaction_hash"], event["contract"])].append(event)

    # Group standard mint transfers by transaction and contract.
    mint_transfers = [row for row in transfer_events if row["event_kind"] == "MINT"]
    mint_groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    tx_mint_contracts: dict[str, set[str]] = defaultdict(set)
    for row in mint_transfers:
        key = (row["transaction_hash"], row["contract"])
        mint_groups[key].append(row)
        tx_mint_contracts[row["transaction_hash"]].add(row["contract"])

    primary_events: list[dict[str, Any]] = []
    covered_mint_groups: set[tuple[str, str]] = set()
    for key, events in seadrop_by_tx_contract.items():
        tx_hash, contract = key
        matching = mint_groups.get(key, [])
        covered_mint_groups.add(key)
        for event in events:
            receipt = receipts.get(tx_hash, {})
            tx = txs.get(tx_hash, {})
            zero_quantity = sum(integer(row.get("amount")) for row in matching if row.get("to") == event["minter"])
            primary_events.append({
                **event,
                "standard": next((row["standard"] for row in matching), "UNKNOWN"),
                "recipient": event["minter"],
                "mint_transfer_quantity": zero_quantity,
                "mint_transfer_match": zero_quantity == event["quantity"],
                "tx_from": str(tx.get("from") or "").lower(),
                "tx_to": str(tx.get("to") or "").lower(),
                "tx_value_wei": integer(tx.get("value")),
                "receipt_status": integer(receipt.get("status"), -1),
                "gas_used": integer(receipt.get("gasUsed")),
                "effective_gas_price": integer(receipt.get("effectiveGasPrice")),
                "gas_cost_wei": integer(receipt.get("gasUsed")) * integer(receipt.get("effectiveGasPrice")),
                "payment_asset": NATIVE,
                "payment_total_raw": event["unit_price"] * event["quantity"],
                "unit_price_raw": event["unit_price"],
                "payment_proof_status": "SEADROP_EVENT_EXACT",
                "access_class": "PUBLIC" if event["is_public"] else "PRIVILEGED_SEADROP_STAGE",
            })

    # Custom/non-SeaDrop primary mint groups.
    for key, rows in mint_groups.items():
        if key in covered_mint_groups:
            continue
        tx_hash, contract = key
        tx = txs.get(tx_hash, {})
        receipt = receipts.get(tx_hash, {})
        tx_from = str(tx.get("from") or "").lower()
        quantity = sum(integer(row.get("amount")) for row in rows)
        recipients = sorted({row.get("to") for row in rows if row.get("to")})
        native_value = integer(tx.get("value"))
        erc20_outflows: dict[str, int] = defaultdict(int)
        for transfer in receipt_transfers(receipt):
            if transfer["standard"] == "ERC20" and transfer["from"] == tx_from and transfer["amount"] > 0:
                erc20_outflows[transfer["contract"]] += transfer["amount"]
        payment_asset = None
        payment_total = None
        proof = "UNRESOLVED_CUSTOM_PAYMENT"
        if len(tx_mint_contracts[tx_hash]) == 1 and quantity > 0:
            if native_value > 0 and not erc20_outflows:
                payment_asset = NATIVE
                payment_total = native_value
                proof = "TOP_LEVEL_NATIVE_VALUE_EXACT"
            elif native_value == 0 and len(erc20_outflows) == 1:
                payment_asset, payment_total = next(iter(erc20_outflows.items()))
                proof = "TX_SENDER_ERC20_OUTFLOW_EXACT"
            elif native_value == 0 and not erc20_outflows:
                payment_asset = NATIVE
                payment_total = 0
                proof = "ZERO_VALUE_CONFIRMED"
        self_recipient = len(recipients) == 1 and recipients[0] == tx_from
        primary_events.append({
            "route": "CUSTOM",
            "contract": contract,
            "standard": rows[0]["standard"],
            "block_number": rows[0]["block_number"],
            "block_hash": rows[0]["block_hash"],
            "timestamp_unix": rows[0]["timestamp_unix"],
            "timestamp_utc": rows[0]["timestamp_utc"],
            "transaction_hash": tx_hash,
            "transaction_index": rows[0]["transaction_index"],
            "log_index": min(row["log_index"] for row in rows),
            "minter": tx_from if self_recipient else None,
            "payer": tx_from,
            "recipient": recipients[0] if len(recipients) == 1 else None,
            "recipients_json": recipients,
            "quantity": quantity,
            "unit_price": payment_total / quantity if payment_total is not None and quantity else None,
            "unit_price_raw": payment_total / quantity if payment_total is not None and quantity else None,
            "fee_bps": None,
            "stage_index": None,
            "is_public": None,
            "is_paid": payment_total is not None and payment_total > 0,
            "is_self_funded": self_recipient,
            "mint_transfer_quantity": quantity,
            "mint_transfer_match": True,
            "tx_from": tx_from,
            "tx_to": str(tx.get("to") or "").lower(),
            "tx_value_wei": native_value,
            "receipt_status": integer(receipt.get("status"), -1),
            "gas_used": integer(receipt.get("gasUsed")),
            "effective_gas_price": integer(receipt.get("effectiveGasPrice")),
            "gas_cost_wei": integer(receipt.get("gasUsed")) * integer(receipt.get("effectiveGasPrice")),
            "payment_asset": payment_asset,
            "payment_total_raw": payment_total,
            "payment_proof_status": proof,
            "access_class": "CUSTOM_ACCESS_UNKNOWN",
        })

    primary_events.sort(key=lambda row: (integer(row.get("block_number")), integer(row.get("transaction_index")), integer(row.get("log_index"))))

    # Decode Seaport orders and prove NFT movements from canonical receipts.
    seaport_orders: list[dict[str, Any]] = []
    seaport_items: list[dict[str, Any]] = []
    for row in discovery.get("seaport", []):
        tx_hash = row.get("transaction_hash", "").lower()
        receipt = receipts.get(tx_hash, {})
        block = integer(row.get("block_number"))
        try:
            decoded = decode_seaport(parse_topics(row.get("topics_json") or "[]"), row.get("data") or "0x")
            nft_offer = [item for item in decoded["offer"] if item["item_type"] in NFT_ITEM_TYPES]
            nft_consideration = [item for item in decoded["consideration"] if item["item_type"] in NFT_ITEM_TYPES]
            payment_offer = [item for item in decoded["offer"] if item["item_type"] in PAYMENT_ITEM_TYPES]
            payment_consideration = [item for item in decoded["consideration"] if item["item_type"] in PAYMENT_ITEM_TYPES]
            nft_side = "OFFER" if nft_offer else "CONSIDERATION" if nft_consideration else "NONE"
            nft_items = nft_offer or nft_consideration
            payment_items = payment_consideration if payment_consideration else payment_offer
            payment_side = "CONSIDERATION" if payment_consideration else "OFFER" if payment_offer else "NONE"
            payment_tokens = {item["token"] for item in payment_items}
            payment_token = next(iter(payment_tokens)) if len(payment_tokens) == 1 else None
            gross = sum(item["amount"] for item in payment_items) if payment_token is not None else None
            receipt_rows = receipt_transfers(receipt)
            item_records = []
            total_nft_units = sum(max(1, item["amount"]) for item in nft_items)
            projects = {item["token"] for item in nft_items}
            allocation_exact = len(projects) == 1 and payment_token is not None and total_nft_units > 0
            for item_index, item in enumerate(nft_items):
                matches = [
                    transfer for transfer in receipt_rows
                    if transfer["standard"] in {"ERC721", "ERC1155"}
                    and transfer["contract"] == item["token"]
                    and transfer["token_id"] == item["identifier"]
                ]
                seller = matches[0]["from"] if len(matches) == 1 else None
                buyer = matches[0]["to"] if len(matches) == 1 else None
                seller_net = None
                seller_net_status = "UNRESOLVED"
                if seller and payment_side == "CONSIDERATION":
                    direct = [value for value in payment_consideration if value.get("recipient") == seller and value["token"] == payment_token]
                    if direct:
                        seller_net = sum(value["amount"] for value in direct)
                        seller_net_status = "EVENT_RECIPIENT_EXACT"
                if seller and seller_net is None and payment_token and payment_token != NATIVE:
                    direct_receipt = [value for value in receipt_rows if value["standard"] == "ERC20" and value["contract"] == payment_token and value["to"] == seller]
                    if direct_receipt:
                        seller_net = sum(value["amount"] for value in direct_receipt)
                        seller_net_status = "RECEIPT_TRANSFER_EXACT"
                allocated_gross = gross * max(1, item["amount"]) / total_nft_units if allocation_exact and gross is not None else None
                allocated_seller_net = seller_net * max(1, item["amount"]) / total_nft_units if allocation_exact and seller_net is not None else None
                record = {
                    "transaction_hash": tx_hash,
                    "log_index": integer(row.get("log_index")),
                    "order_hash": decoded["order_hash"],
                    "block_number": block,
                    "block_hash": row.get("block_hash", "").lower(),
                    "timestamp_unix": block_times.get(block),
                    "timestamp_utc": utc(block_times.get(block)),
                    "offerer": decoded["offerer"],
                    "fulfillment_recipient": decoded["recipient"],
                    "nft_side": nft_side,
                    "payment_side": payment_side,
                    "item_index": item_index,
                    "nft_contract": item["token"],
                    "token_id": item["identifier"],
                    "nft_amount": item["amount"],
                    "seller": seller,
                    "buyer": buyer,
                    "matching_nft_transfer_count": len(matches),
                    "payment_token": payment_token,
                    "order_gross_payment_raw": gross,
                    "allocated_gross_payment_raw": allocated_gross,
                    "seller_net_payment_raw": seller_net,
                    "allocated_seller_net_raw": allocated_seller_net,
                    "seller_net_status": seller_net_status,
                    "single_project_allocation": allocation_exact,
                    "receipt_status": integer(receipt.get("status"), -1),
                    "sale_proof_status": "PROVEN" if integer(receipt.get("status"), -1) == 1 and len(matches) == 1 else "UNRESOLVED",
                }
                item_records.append(record)
                seaport_items.append(record)
            seaport_orders.append({
                "transaction_hash": tx_hash,
                "log_index": integer(row.get("log_index")),
                "order_hash": decoded["order_hash"],
                "block_number": block,
                "block_hash": row.get("block_hash", "").lower(),
                "timestamp_unix": block_times.get(block),
                "timestamp_utc": utc(block_times.get(block)),
                "offerer": decoded["offerer"],
                "zone": decoded["zone"],
                "recipient": decoded["recipient"],
                "nft_side": nft_side,
                "payment_side": payment_side,
                "nft_item_rows": len(nft_items),
                "nft_units": total_nft_units,
                "nft_projects": len(projects),
                "nft_contracts_json": sorted(projects),
                "payment_token": payment_token,
                "gross_payment_raw": gross,
                "allocation_exact": allocation_exact,
                "proven_nft_item_rows": sum(value["sale_proof_status"] == "PROVEN" for value in item_records),
                "receipt_status": integer(receipt.get("status"), -1),
                "offer_json": decoded["offer"],
                "consideration_json": decoded["consideration"],
            })
        except Exception as exc:
            decode_errors.append({"source": "SEAPORT", "transaction_hash": tx_hash, "log_index": row.get("log_index"), "error": repr(exc)})

    # Build project opportunity universe. SeaDrop Public is comparable; market-traded custom projects are retained but not comparable.
    events_by_contract: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for event in primary_events:
        events_by_contract[event["contract"]].append(event)
    traded_contracts = {row["nft_contract"] for row in seaport_items if row["sale_proof_status"] == "PROVEN"}
    contract_rows = {row["contract"].lower(): row for row in contracts}
    project_opportunities: list[dict[str, Any]] = []
    for contract in sorted(set(events_by_contract) | traded_contracts | set(contract_rows)):
        events = sorted(events_by_contract.get(contract, []), key=lambda row: (integer(row.get("block_number")), integer(row.get("log_index"))))
        seadrop = [row for row in events if row["route"] == "SEADROP"]
        public = [row for row in seadrop if row["is_public"]]
        paid_public = [row for row in public if row["is_paid"]]
        free = [row for row in events if row.get("is_paid") is False]
        paid_public_start = min((row["timestamp_unix"] for row in paid_public if row["timestamp_unix"] is not None), default=None)
        free_before = any(row["timestamp_unix"] is not None and paid_public_start is not None and row["timestamp_unix"] <= paid_public_start for row in free)
        prices = sorted({integer(row.get("unit_price_raw")) for row in paid_public if row.get("unit_price_raw") is not None})
        first = min((row["timestamp_unix"] for row in events if row["timestamp_unix"] is not None), default=None)
        project_opportunities.append({
            "project_id": contract,
            "nft_contract": contract,
            "first_primary_timestamp_unix": first,
            "first_primary_timestamp_utc": utc(first),
            "first_paid_public_timestamp_unix": paid_public_start,
            "first_paid_public_timestamp_utc": utc(paid_public_start),
            "primary_event_rows": len(events),
            "observed_primary_quantity": sum(integer(row.get("quantity")) for row in events),
            "unique_primary_recipients": len({row.get("recipient") or row.get("minter") for row in events if row.get("recipient") or row.get("minter")}),
            "seadrop_event_rows": len(seadrop),
            "public_event_rows": len(public),
            "paid_public_event_rows": len(paid_public),
            "free_primary_event_rows": len(free),
            "free_before_paid_public": free_before,
            "paid_public_prices_raw_json": prices,
            "reference_paid_public_price_raw": statistics.median(prices) if prices else None,
            "strict_paid_public_from_start": bool(paid_public) and not free_before and len(prices) == 1,
            "has_proven_seaport_trade": contract in traded_contracts,
            "comparable_selection_universe": bool(paid_public),
            "project_class": "SEADROP_NFT" if seadrop else "CUSTOM_MARKET_TRADED_NFT" if contract in traded_contracts else "UNCLASSIFIED_NFT_CONTRACT",
            "production_approved": False,
        })
    opportunity = {row["project_id"]: row for row in project_opportunities}

    # Project outcomes from independent, exactly allocatable Seaport orders.
    sales_by_project: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in seaport_items:
        if row["sale_proof_status"] == "PROVEN" and row["single_project_allocation"] and row["payment_token"] in {NATIVE, WETH_RHC} and row["allocated_gross_payment_raw"] is not None:
            sales_by_project[row["nft_contract"]].append(row)
    project_outcomes: list[dict[str, Any]] = []
    for project in project_opportunities:
        project_id = project["project_id"]
        start = project["first_paid_public_timestamp_unix"]
        reference = project["reference_paid_public_price_raw"]
        result: dict[str, Any] = {
            "project_id": project_id,
            "nft_contract": project_id,
            "first_paid_public_timestamp_unix": start,
            "reference_paid_public_price_raw": reference,
        }
        project_sales = sales_by_project.get(project_id, [])
        for label, seconds in WINDOWS.items():
            eligible = [row for row in project_sales if start is not None and row["timestamp_unix"] is not None and start <= row["timestamp_unix"] <= start + seconds]
            orders = {(row["transaction_hash"], row["order_hash"]) for row in eligible}
            buyers = {row["buyer"] for row in eligible if row["buyer"]}
            prices = [float(row["allocated_gross_payment_raw"]) / max(1, integer(row["nft_amount"])) for row in eligible]
            median_price = statistics.median(prices) if prices else None
            multiple = median_price / float(reference) if median_price is not None and reference else None
            result[f"orders_{label}"] = len(orders)
            result[f"independent_buyers_{label}"] = len(buyers)
            result[f"sale_units_{label}"] = sum(integer(row["nft_amount"]) for row in eligible)
            result[f"median_gross_per_unit_raw_{label}"] = median_price
            result[f"median_multiple_vs_mint_{label}"] = multiple
            result[f"success_liquid_100_{label}"] = bool(multiple is not None and multiple >= 1.0 and len(orders) >= 3 and len(buyers) >= 3)
            result[f"success_liquid_115_{label}"] = bool(multiple is not None and multiple >= 1.15 and len(orders) >= 3 and len(buyers) >= 3)
        project_outcomes.append(result)
    outcome = {row["project_id"]: row for row in project_outcomes}

    # Build early wallet entries and matched-baseline Selection Alpha.
    first_sale = {
        project_id: min(row["timestamp_unix"] for row in values if row["timestamp_unix"] is not None)
        for project_id, values in sales_by_project.items()
        if any(row["timestamp_unix"] is not None for row in values)
    }
    cumulative: dict[str, int] = defaultdict(int)
    wallet_entries: list[dict[str, Any]] = []
    for event in primary_events:
        project = opportunity[event["contract"]]
        before = cumulative[event["contract"]]
        cumulative[event["contract"]] += integer(event.get("quantity"))
        denominator = max(1, integer(project.get("observed_primary_quantity")))
        quantile = cumulative[event["contract"]] / denominator
        before_sale = first_sale.get(event["contract"]) is None or (event["timestamp_unix"] is not None and event["timestamp_unix"] < first_sale[event["contract"]])
        eligible = bool(
            event["route"] == "SEADROP"
            and event["is_public"]
            and event["is_paid"]
            and event["is_self_funded"]
            and event["receipt_status"] == 1
            and event["mint_transfer_match"]
            and before_sale
            and quantile <= 0.20
        )
        wallet_entries.append({
            **event,
            "cumulative_quantity_before": before,
            "cumulative_quantity_after": cumulative[event["contract"]],
            "entry_quantity_quantile": quantile,
            "before_first_proven_sale": before_sale,
            "selection_signal_eligible": eligible,
            "selection_signal_type": "PUBLIC_SELF_FUNDED" if eligible else "NOT_ELIGIBLE",
        })

    comparable = [row for row in project_opportunities if row["comparable_selection_universe"] and row["first_paid_public_timestamp_unix"] is not None and row["reference_paid_public_price_raw"]]
    entries_by_wallet: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
    for row in wallet_entries:
        if row["selection_signal_eligible"]:
            entries_by_wallet[row["minter"]][row["contract"]] = row
    wallet_metrics: list[dict[str, Any]] = []
    for wallet, entered in entries_by_wallet.items():
        metric_samples: dict[str, list[tuple[float, float, int]]] = defaultdict(list)
        active_start = min(row["timestamp_unix"] for row in entered.values() if row["timestamp_unix"] is not None)
        active_end = max(row["timestamp_unix"] for row in entered.values() if row["timestamp_unix"] is not None)
        active_opportunities = [row for row in comparable if active_start - 7 * 86400 <= row["first_paid_public_timestamp_unix"] <= active_end + 7 * 86400]
        for project_id, entry in entered.items():
            project = opportunity[project_id]
            start = integer(project["first_paid_public_timestamp_unix"])
            price = float(project["reference_paid_public_price_raw"])
            matched = [
                candidate for candidate in comparable
                if abs(integer(candidate["first_paid_public_timestamp_unix"]) - start) <= 3 * 86400
                and 0.25 <= float(candidate["reference_paid_public_price_raw"]) / price <= 4.0
            ]
            for metric in ("success_liquid_100_24h", "success_liquid_115_24h"):
                observed = 1.0 if outcome.get(project_id, {}).get(metric) else 0.0
                baseline_values = [1.0 if outcome.get(candidate["project_id"], {}).get(metric) else 0.0 for candidate in matched]
                if baseline_values:
                    metric_samples[metric].append((observed, statistics.mean(baseline_values), len(matched)))
        result: dict[str, Any] = {
            "wallet": wallet,
            "entered_projects": len(entered),
            "active_window_opportunities": len(active_opportunities),
            "active_window_selectivity": len(entered) / len(active_opportunities) if active_opportunities else None,
            "median_entry_quantity_quantile": statistics.median(row["entry_quantity_quantile"] for row in entered.values()),
            "classification": "RESEARCH_ONLY_NOT_EVALUATED_FOR_PRODUCTION",
            "production_approved": False,
        }
        for metric in ("success_liquid_100_24h", "success_liquid_115_24h"):
            values = metric_samples.get(metric, [])
            result[f"{metric}_sample_count"] = len(values)
            result[f"{metric}_hit_rate"] = statistics.mean(value[0] for value in values) if values else None
            result[f"{metric}_matched_baseline"] = statistics.mean(value[1] for value in values) if values else None
            result[f"{metric}_predictive_lift"] = statistics.mean(value[0] - value[1] for value in values) if values else None
            result[f"{metric}_median_matched_projects"] = statistics.median(value[2] for value in values) if values else None
        wallet_metrics.append(result)

    candidate_wallets = {str(row.get("wallet") or "").lower(): row for row in known_wallets}
    candidate_activity = []
    for row in wallet_entries:
        wallet = str(row.get("minter") or row.get("recipient") or "").lower()
        if wallet in candidate_wallets:
            candidate_activity.append({**row, "candidate_priority": candidate_wallets[wallet].get("priority")})

    write_csv(out / "nft_contract_population.csv", project_opportunities)
    write_csv(out / "nft_transfer_events.csv", transfer_events)
    write_csv(out / "primary_mint_events.csv", primary_events)
    write_csv(out / "seadrop_mint_events.csv", seadrop_events)
    write_csv(out / "seaport_orders.csv", seaport_orders)
    write_csv(out / "seaport_sale_items.csv", seaport_items)
    write_csv(out / "project_opportunities.csv", project_opportunities)
    write_csv(out / "project_outcomes.csv", project_outcomes)
    write_csv(out / "wallet_project_entries.csv", wallet_entries)
    write_csv(out / "wallet_selection_alpha.csv", wallet_metrics)
    write_csv(out / "candidate_wallet_activity.csv", candidate_activity)
    write_csv(out / "decode_errors.csv", decode_errors)

    required_tx_hashes = {row["transaction_hash"] for row in transfer_events} | {row.get("transaction_hash", "") for values in discovery.values() for row in values}
    missing_txs = sorted(value for value in required_tx_hashes if value and value not in txs)
    missing_receipts = sorted(value for value in required_tx_hashes if value and value not in receipts)
    event_blocks = {integer(row.get("block_number")) for row in transfer_events} | {integer(row.get("block_number")) for values in discovery.values() for row in values}
    missing_blocks = sorted(value for value in event_blocks if value not in blocks)
    failures = []
    if transfer_errors:
        failures.append({"code": "TRANSFER_DECODE_ERRORS", "count": len(transfer_errors)})
    if decode_errors:
        failures.append({"code": "EVENT_DECODE_ERRORS", "count": len(decode_errors)})
    if missing_txs:
        failures.append({"code": "TRANSACTIONS_MISSING", "count": len(missing_txs), "sample": missing_txs[:20]})
    if missing_receipts:
        failures.append({"code": "RECEIPTS_MISSING", "count": len(missing_receipts), "sample": missing_receipts[:20]})
    if missing_blocks:
        failures.append({"code": "BLOCKS_MISSING", "count": len(missing_blocks), "sample": missing_blocks[:20]})
    if any(row.get("receipt_status") != 1 for row in primary_events):
        failures.append({"code": "FAILED_PRIMARY_TRANSACTION_INCLUDED", "count": sum(row.get("receipt_status") != 1 for row in primary_events)})
    if any(row.get("mint_transfer_match") is False for row in primary_events if row["route"] == "SEADROP"):
        failures.append({"code": "SEADROP_MINT_TRANSFER_MISMATCH", "count": sum(row.get("mint_transfer_match") is False for row in primary_events if row["route"] == "SEADROP")})

    validation = {
        "status": "PASS" if not failures else "FAIL",
        "chain_id": 4663,
        "contract_population_rows": len(contracts),
        "transfer_event_rows": len(transfer_events),
        "primary_mint_event_rows": len(primary_events),
        "seadrop_event_rows": len(seadrop_events),
        "seaport_order_rows": len(seaport_orders),
        "seaport_item_rows": len(seaport_items),
        "proven_seaport_item_rows": sum(row["sale_proof_status"] == "PROVEN" for row in seaport_items),
        "project_opportunity_rows": len(project_opportunities),
        "comparable_selection_projects": sum(bool(row["comparable_selection_universe"]) for row in project_opportunities),
        "strict_paid_public_projects": sum(bool(row["strict_paid_public_from_start"]) for row in project_opportunities),
        "project_outcome_rows": len(project_outcomes),
        "wallet_project_entry_rows": len(wallet_entries),
        "selection_metric_wallets": len(wallet_metrics),
        "candidate_wallet_activity_rows": len(candidate_activity),
        "missing_transactions": len(missing_txs),
        "missing_receipts": len(missing_receipts),
        "missing_blocks": len(missing_blocks),
        "decode_error_rows": len(decode_errors),
        "failures": failures,
        "production_approved_wallets": 0,
        "deepseek_handoff_allowed": False,
    }
    (out / "VALIDATION.json").write_text(json.dumps(validation, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    (out / "REMEDIATION.json").write_text(json.dumps({
        "missing_transactions": missing_txs,
        "missing_receipts": missing_receipts,
        "missing_blocks": missing_blocks,
        "decode_errors": decode_errors,
    }, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    manifest = []
    for path in sorted(out.iterdir()):
        if path.is_file():
            manifest.append({"path": path.name, "bytes": path.stat().st_size, "sha256": sha256(path)})
    (out / "MANIFEST.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(validation, sort_keys=True), flush=True)
    if validation["status"] != "PASS":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
