#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
import math
import os
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

from common import (
    ZERO_ADDRESS,
    WETH_RHC,
    address,
    block_raw,
    block_timestamp,
    boolish,
    canonical_json,
    decode_contract_transfer,
    fetch_artifact,
    intish,
    internal_items,
    load_jsonl_gz,
    merge_prefer_official,
    normalize_asset,
    only_file,
    parse_json_field,
    payment_flows_for_transaction,
    read_csv,
    receipt_core,
    sha256_file,
    transaction_raw,
    tx_core,
    unix_to_iso,
    write_csv,
    write_jsonl_gz,
)

RUN_ID = int(os.environ["GITHUB_RUN_ID"])
FIXED_HEAD = 48_264_433
OUT = Path("rhc-market-provenance-output")
STATUS_DIR = Path("rhc-market-provenance-stage")

SOURCES = {
    "normalized": {
        "branch": "chatgpt/rhc-normalized-universe-20260829",
        "workflow": "RHC normalize complete NFT opportunity and market universe",
        "artifact": "rhc-normalized-universe",
        "status_file": "NORMALIZATION_STATUS.json",
    },
    "primary_enrichment": {
        "branch": "chatgpt/rhc-enrichment-aggregate-20260829",
        "workflow": "RHC aggregate all transaction and contract enrichment v2",
        "artifact": "rhc-enrichment-aggregate-v2",
        "status_file": "AGGREGATE_STATUS.json",
    },
    "token_transfers": {
        "branch": "chatgpt/rhc-token-transfer-aggregate-20260829",
        "workflow": "RHC aggregate complete token-level NFT transfer histories v2",
        "artifact": "rhc-token-transfer-aggregate-v2",
        "status_file": "AGGREGATE_STATUS.json",
    },
    "secondary_enrichment": {
        "branch": "chatgpt/rhc-secondary-enrichment-aggregate-20260829",
        "workflow": "RHC aggregate all secondary NFT transaction evidence v2",
        "artifact": "rhc-secondary-enrichment-aggregate-v2",
        "status_file": "AGGREGATE_STATUS.json",
    },
}


def normalized_block_row(row: dict[str, Any]) -> dict[str, Any]:
    raw = block_raw(row)
    number = intish(row.get("block_number") or raw.get("number") or raw.get("height"))
    timestamp = block_timestamp(raw)
    return {
        "block_number": number,
        "timestamp_unix": timestamp,
        "timestamp_utc": unix_to_iso(timestamp),
        "block_hash": str(raw.get("hash") or "").lower() or None,
        "source": row.get("source"),
        "raw": raw,
    }


def load_enrichment(root: Path) -> dict[str, Any]:
    tx_rows = [tx_core(row) for row in load_jsonl_gz(only_file(root, "transactions.jsonl.gz"))]
    receipt_rows = [receipt_core(row) for row in load_jsonl_gz(only_file(root, "receipts.jsonl.gz"))]
    block_rows = [normalized_block_row(row) for row in load_jsonl_gz(only_file(root, "blocks.jsonl.gz"))]
    internal_rows = load_jsonl_gz(only_file(root, "internal_transactions.jsonl.gz"))
    contract_paths = sorted(root.rglob("contracts.jsonl.gz"))
    contract_rows = load_jsonl_gz(contract_paths[0]) if len(contract_paths) == 1 else []
    return {
        "transactions": tx_rows,
        "receipts": receipt_rows,
        "blocks": block_rows,
        "internals": internal_rows,
        "contracts": contract_rows,
    }


def parse_order_items(rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    output = []
    for row in rows:
        output.append({
            **row,
            "block_number": intish(row.get("block_number")),
            "log_index": intish(row.get("log_index")),
            "item_index": intish(row.get("item_index")),
            "item_type": intish(row.get("item_type")),
            "amount_int": intish(row.get("amount"), 0) or 0,
            "is_nft_bool": boolish(row.get("is_nft")),
            "is_payment_bool": boolish(row.get("is_payment")),
            "offerer": str(row.get("offerer") or "").lower(),
            "token": str(row.get("token") or "").lower(),
            "item_recipient": str(row.get("item_recipient") or "").lower() or None,
            "transaction_hash": str(row.get("transaction_hash") or "").lower(),
            "event_id": str(row.get("event_id") or ""),
            "side": str(row.get("side") or ""),
            "identifier": str(row.get("identifier") or "0"),
        })
    return output


def payment_asset_from_item(item: dict[str, Any]) -> str:
    return ZERO_ADDRESS if int(item["item_type"]) == 0 else str(item["token"]).lower()


def actual_transfer_id(row: dict[str, Any]) -> str:
    return f"{row['transaction_hash']}:{row['log_index']}:{row['item_index']}"


def classify_seaport(
    orders: list[dict[str, str]],
    order_items: list[dict[str, Any]],
    transfers_by_tx: dict[str, list[dict[str, Any]]],
    flows_by_tx: dict[str, list[dict[str, Any]]],
    tx_by_hash: dict[str, dict[str, Any]],
    timestamp_by_block: dict[int, int],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], set[str], list[dict[str, Any]]]:
    items_by_event: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in order_items:
        items_by_event[item["event_id"]].append(item)

    order_rows = []
    token_rows = []
    consumed_transfer_ids: set[str] = set()
    failures = []

    for raw_order in orders:
        event_id = str(raw_order.get("event_id") or "")
        tx_hash = str(raw_order.get("transaction_hash") or "").lower()
        block = intish(raw_order.get("block_number"))
        log_index = intish(raw_order.get("log_index"))
        items = items_by_event.get(event_id, [])
        nft_items = [item for item in items if item["is_nft_bool"]]
        payment_items = [item for item in items if item["is_payment_bool"]]
        actual = [
            row for row in transfers_by_tx.get(tx_hash, [])
            if row["from_address"] != ZERO_ADDRESS and row["to_address"] != ZERO_ADDRESS
        ]
        matched = []
        used = set()
        for item in nft_items:
            candidates = [
                row for row in actual
                if actual_transfer_id(row) not in used
                and row["nft_contract"] == item["token"]
                and str(row["token_id"]) == str(item["identifier"])
            ]
            if not candidates:
                continue
            candidate = candidates[0]
            used.add(actual_transfer_id(candidate))
            matched.append({"item": item, "transfer": candidate})

        sellers = sorted({entry["transfer"]["from_address"] for entry in matched})
        buyers = sorted({entry["transfer"]["to_address"] for entry in matched})
        payment_assets = sorted({payment_asset_from_item(item) for item in payment_items})
        one_asset = len(payment_assets) == 1
        gross_raw = sum(item["amount_int"] for item in payment_items) if one_asset else None
        seller = sellers[0] if len(sellers) == 1 else None
        buyer = buyers[0] if len(buyers) == 1 else None
        asset = payment_assets[0] if one_asset else None
        seller_net = None
        seller_payment_evidence = []
        if seller and asset:
            direct_items = [
                item for item in payment_items
                if item["side"] == "consideration" and item.get("item_recipient") == seller
                and payment_asset_from_item(item) == asset
            ]
            direct_net = sum(item["amount_int"] for item in direct_items)
            if direct_net > 0:
                seller_net = direct_net
                seller_payment_evidence = [{"source": "SEAPORT_CONSIDERATION", "item": item} for item in direct_items]
            else:
                matching_flows = [
                    flow for flow in flows_by_tx.get(tx_hash, [])
                    if flow["to_address"] == seller and flow["asset"] == asset
                ]
                if matching_flows:
                    seller_net = sum(int(flow["amount_raw"]) for flow in matching_flows)
                    seller_payment_evidence = [{"source": "RECEIPT_FLOW", "flow": flow} for flow in matching_flows]

        unique_nft_transfers = len(matched)
        same_contract = len({entry["transfer"]["nft_contract"] for entry in matched}) == 1 if matched else False
        exact_single = unique_nft_transfers == 1 and seller and buyer and seller != buyer and asset and gross_raw and seller_net
        if exact_single:
            proof = "PROVEN_SEAPORT_SINGLE_NFT_SALE"
        elif unique_nft_transfers > 1 and same_contract and seller and buyer and seller != buyer and asset and gross_raw and seller_net:
            proof = "PROVEN_SEAPORT_SAME_CONTRACT_BUNDLE_ORDER_UNALLOCATED"
        elif matched and seller and buyer and seller == buyer:
            proof = "SELF_TRANSFER_NOT_SALE"
        elif matched and not one_asset:
            proof = "MULTI_ASSET_PAYMENT_UNRESOLVED"
        elif matched and not seller_net:
            proof = "SELLER_PAYMENT_NOT_PROVEN"
        elif not matched:
            proof = "ORDER_NFT_TRANSFER_NOT_MATCHED"
        else:
            proof = "MIXED_OR_MULTI_PARTY_ORDER_UNRESOLVED"

        row = {
            "sale_event_id": event_id,
            "transaction_hash": tx_hash,
            "log_index": log_index,
            "block_number": block,
            "timestamp_unix": timestamp_by_block.get(block),
            "timestamp_utc": unix_to_iso(timestamp_by_block.get(block)),
            "marketplace": "SEAPORT",
            "order_hash": raw_order.get("order_hash"),
            "orientation": raw_order.get("orientation"),
            "offerer": str(raw_order.get("offerer") or "").lower(),
            "zone": str(raw_order.get("zone") or "").lower(),
            "order_recipient": str(raw_order.get("recipient") or "").lower(),
            "seller": seller,
            "buyer": buyer,
            "nft_transfer_count": unique_nft_transfers,
            "nft_contracts": sorted({entry["transfer"]["nft_contract"] for entry in matched}),
            "payment_asset": asset,
            "payment_asset_normalized": normalize_asset(asset),
            "gross_payment_raw": str(gross_raw) if gross_raw is not None else None,
            "seller_net_raw": str(seller_net) if seller_net is not None else None,
            "fee_and_royalty_raw": str(gross_raw - seller_net) if gross_raw is not None and seller_net is not None and gross_raw >= seller_net else None,
            "proof_status": proof,
            "seller_payment_evidence": seller_payment_evidence,
            "matched_transfers": [entry["transfer"] for entry in matched],
            "payment_items": payment_items,
        }
        order_rows.append(row)
        if exact_single:
            entry = matched[0]
            transfer = entry["transfer"]
            consumed_transfer_ids.add(actual_transfer_id(transfer))
            quantity = int(transfer["quantity"])
            token_rows.append({
                "sale_event_id": event_id,
                "transaction_hash": tx_hash,
                "log_index": log_index,
                "block_number": block,
                "timestamp_unix": timestamp_by_block.get(block),
                "timestamp_utc": unix_to_iso(timestamp_by_block.get(block)),
                "marketplace": "SEAPORT",
                "seller": seller,
                "buyer": buyer,
                "nft_contract": transfer["nft_contract"],
                "standard": transfer["standard"],
                "token_id": transfer["token_id"],
                "quantity": quantity,
                "transfer_id": actual_transfer_id(transfer),
                "payment_asset": asset,
                "payment_asset_normalized": normalize_asset(asset),
                "gross_payment_raw": str(gross_raw),
                "seller_net_raw": str(seller_net),
                "gross_per_unit_raw": str(gross_raw // quantity) if quantity and gross_raw % quantity == 0 else None,
                "seller_net_per_unit_raw": str(seller_net // quantity) if quantity and seller_net % quantity == 0 else None,
                "allocation_status": "EXACT_SINGLE_TRANSFER",
                "proof_status": proof,
            })
        elif proof.startswith("PROVEN_SEAPORT"):
            for entry in matched:
                consumed_transfer_ids.add(actual_transfer_id(entry["transfer"]))

        if block is None or timestamp_by_block.get(block) is None:
            failures.append({"code": "SEAPORT_TIMESTAMP_MISSING", "event_id": event_id, "block_number": block})
    return order_rows, token_rows, consumed_transfer_ids, failures


def classify_custom_transfers(
    transfers_by_tx: dict[str, list[dict[str, Any]]],
    consumed_transfer_ids: set[str],
    flows_by_tx: dict[str, list[dict[str, Any]]],
    tx_by_hash: dict[str, dict[str, Any]],
    timestamp_by_block: dict[int, int],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], set[str]]:
    order_rows = []
    token_rows = []
    consumed = set(consumed_transfer_ids)
    for tx_hash, all_transfers in sorted(transfers_by_tx.items()):
        transfers = [
            row for row in all_transfers
            if row["from_address"] != ZERO_ADDRESS
            and row["to_address"] != ZERO_ADDRESS
            and actual_transfer_id(row) not in consumed
        ]
        if not transfers:
            continue
        sellers = sorted({row["from_address"] for row in transfers})
        buyers = sorted({row["to_address"] for row in transfers})
        block = transfers[0]["block_number"]
        tx = tx_by_hash.get(tx_hash)
        proof = "UNPROVEN_TRANSFER"
        asset = None
        seller_net = None
        seller = sellers[0] if len(sellers) == 1 else None
        buyer = buyers[0] if len(buyers) == 1 else None
        evidence = []
        if seller and buyer and seller == buyer:
            proof = "SELF_TRANSFER_NOT_SALE"
        elif seller and buyer:
            direct = [
                flow for flow in flows_by_tx.get(tx_hash, [])
                if flow["from_address"] == buyer and flow["to_address"] == seller
            ]
            routed = [
                flow for flow in flows_by_tx.get(tx_hash, [])
                if flow["to_address"] == seller
            ] if tx and tx.get("from_address") == buyer else []
            candidates = direct if direct else routed
            assets = sorted({flow["asset"] for flow in candidates})
            if len(assets) == 1:
                asset = assets[0]
                seller_net = sum(int(flow["amount_raw"]) for flow in candidates if flow["asset"] == asset)
                if seller_net > 0:
                    proof = "PROVEN_DIRECT_BUYER_TO_SELLER_PAYMENT" if direct else "PROVEN_BUYER_INITIATED_ROUTED_PAYMENT"
                    evidence = candidates
            elif len(assets) > 1:
                proof = "MULTI_ASSET_PAYMENT_UNRESOLVED"

        exact_single = proof.startswith("PROVEN_") and len(transfers) == 1 and seller_net and asset
        sale_event_id = f"CUSTOM:{tx_hash}"
        order_rows.append({
            "sale_event_id": sale_event_id,
            "transaction_hash": tx_hash,
            "log_index": None,
            "block_number": block,
            "timestamp_unix": timestamp_by_block.get(block),
            "timestamp_utc": unix_to_iso(timestamp_by_block.get(block)),
            "marketplace": "CUSTOM_OR_DIRECT",
            "seller": seller,
            "buyer": buyer,
            "nft_transfer_count": len(transfers),
            "nft_contracts": sorted({row["nft_contract"] for row in transfers}),
            "payment_asset": asset,
            "payment_asset_normalized": normalize_asset(asset),
            "gross_payment_raw": str(seller_net) if seller_net is not None else None,
            "seller_net_raw": str(seller_net) if seller_net is not None else None,
            "fee_and_royalty_raw": None,
            "proof_status": proof,
            "seller_payment_evidence": evidence,
            "matched_transfers": transfers,
        })
        if exact_single:
            transfer = transfers[0]
            consumed.add(actual_transfer_id(transfer))
            quantity = int(transfer["quantity"])
            token_rows.append({
                "sale_event_id": sale_event_id,
                "transaction_hash": tx_hash,
                "log_index": None,
                "block_number": block,
                "timestamp_unix": timestamp_by_block.get(block),
                "timestamp_utc": unix_to_iso(timestamp_by_block.get(block)),
                "marketplace": "CUSTOM_OR_DIRECT",
                "seller": seller,
                "buyer": buyer,
                "nft_contract": transfer["nft_contract"],
                "standard": transfer["standard"],
                "token_id": transfer["token_id"],
                "quantity": quantity,
                "transfer_id": actual_transfer_id(transfer),
                "payment_asset": asset,
                "payment_asset_normalized": normalize_asset(asset),
                "gross_payment_raw": str(seller_net),
                "seller_net_raw": str(seller_net),
                "gross_per_unit_raw": str(seller_net // quantity) if quantity and seller_net % quantity == 0 else None,
                "seller_net_per_unit_raw": str(seller_net // quantity) if quantity and seller_net % quantity == 0 else None,
                "allocation_status": "EXACT_SINGLE_TRANSFER",
                "proof_status": proof,
            })
    return order_rows, token_rows, consumed


def gas_cost(receipt: dict[str, Any] | None) -> int | None:
    if not receipt:
        return None
    gas_used = intish(receipt.get("gas_used"))
    gas_price = intish(receipt.get("effective_gas_price_wei"))
    if gas_used is None or gas_price is None:
        return None
    return gas_used * gas_price


def build_primary_lots(
    normalized_root: Path,
    tx_by_hash: dict[str, dict[str, Any]],
    receipt_by_hash: dict[str, dict[str, Any]],
    flows_by_tx: dict[str, list[dict[str, Any]]],
    timestamp_by_block: dict[int, int],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    links = read_csv(only_file(normalized_root, "seadrop_mint_token_links.csv"))
    mints = {row["event_id"]: row for row in read_csv(only_file(normalized_root, "seadrop_mints.csv"))}
    global_mints = read_csv(only_file(normalized_root, "global_nft_mints.csv"))
    lots = []
    failures = []

    linked_global_ids = set()
    for link in links:
        event_id = link["seadrop_event_id"]
        mint = mints.get(event_id)
        if not mint:
            failures.append({"code": "SEADROP_MINT_LINK_WITHOUT_EVENT", "event_id": event_id})
            continue
        linked_global_ids.add(link["global_mint_event_id"])
        tx_hash = mint["transaction_hash"].lower()
        quantity = intish(mint.get("quantity"), 0) or 0
        token_quantity = intish(link.get("token_quantity"), 0) or 0
        unit_price = intish(mint.get("unit_mint_price_wei"), 0) or 0
        entry_gas = gas_cost(receipt_by_hash.get(tx_hash))
        self_funded = mint.get("payer", "").lower() == mint.get("minter", "").lower()
        gas_per_unit = entry_gas // quantity if self_funded and entry_gas is not None and quantity > 0 and entry_gas % quantity == 0 else None
        lot_status = "EXACT_SELF_FUNDED_SEADROP" if self_funded else "SPONSORED_OR_ROUTED_SEADROP"
        lots.append({
            "primary_lot_id": link["global_mint_event_id"],
            "source": "SEADROP",
            "transaction_hash": tx_hash,
            "block_number": intish(mint.get("block_number")),
            "timestamp_unix": timestamp_by_block.get(intish(mint.get("block_number"))),
            "timestamp_utc": unix_to_iso(timestamp_by_block.get(intish(mint.get("block_number")))),
            "nft_contract": link["nft_contract"].lower(),
            "standard": link["standard"],
            "token_id": str(link["token_id"]),
            "quantity": token_quantity,
            "recipient": mint["minter"].lower(),
            "payer": mint["payer"].lower(),
            "stage_index": intish(mint.get("drop_stage_index")),
            "stage_class": mint.get("stage_class"),
            "payment_asset": ZERO_ADDRESS,
            "payment_asset_normalized": "ETH_EQUIVALENT",
            "unit_primary_price_raw": str(unit_price),
            "entry_gas_total_raw": str(entry_gas) if entry_gas is not None else None,
            "entry_gas_per_unit_raw": str(gas_per_unit) if gas_per_unit is not None else None,
            "unit_total_cost_raw": str(unit_price + gas_per_unit) if gas_per_unit is not None else None,
            "cost_status": lot_status,
            "copyable_public_preliminary": intish(mint.get("drop_stage_index")) == 0 and unit_price > 0 and self_funded,
        })

    custom = [row for row in global_mints if row.get("event_id") not in linked_global_ids]
    grouped: dict[tuple[str, str], list[dict[str, str]]] = defaultdict(list)
    for row in custom:
        grouped[(row["transaction_hash"].lower(), row["recipient"].lower())].append(row)
    for (tx_hash, recipient), rows in grouped.items():
        tx = tx_by_hash.get(tx_hash)
        receipt = receipt_by_hash.get(tx_hash)
        total_quantity = sum(intish(row.get("quantity"), 0) or 0 for row in rows)
        payer = tx.get("from_address") if tx else None
        asset = None
        total_payment = 0
        status = "UNRESOLVED_CUSTOM_PRIMARY"
        if tx and payer == recipient:
            native_value = int(tx.get("value_wei") or 0)
            erc20_out = [flow for flow in flows_by_tx.get(tx_hash, []) if flow["flow_kind"] == "ERC20_TRANSFER" and flow["from_address"] == recipient and flow["to_address"] != recipient]
            assets = set()
            if native_value > 0:
                assets.add(ZERO_ADDRESS)
            assets.update(flow["asset"] for flow in erc20_out)
            if len(assets) == 0:
                asset = ZERO_ADDRESS
                total_payment = 0
                status = "EXACT_SELF_FUNDED_CUSTOM_FREE"
            elif len(assets) == 1:
                asset = next(iter(assets))
                total_payment = native_value if asset == ZERO_ADDRESS else sum(int(flow["amount_raw"]) for flow in erc20_out if flow["asset"] == asset)
                status = "EXACT_SELF_FUNDED_CUSTOM_PAID"
            else:
                status = "MULTI_ASSET_CUSTOM_PRIMARY_UNRESOLVED"
        elif payer:
            status = "SPONSORED_OR_ROUTED_CUSTOM_PRIMARY"

        entry_gas = gas_cost(receipt)
        unit_price = total_payment // total_quantity if total_quantity and total_payment % total_quantity == 0 else None
        gas_per_unit = entry_gas // total_quantity if payer == recipient and entry_gas is not None and total_quantity and entry_gas % total_quantity == 0 else None
        for row in rows:
            quantity = intish(row.get("quantity"), 0) or 0
            lots.append({
                "primary_lot_id": row["event_id"],
                "source": "CUSTOM_ZERO_MINT",
                "transaction_hash": tx_hash,
                "block_number": intish(row.get("block_number")),
                "timestamp_unix": timestamp_by_block.get(intish(row.get("block_number"))),
                "timestamp_utc": unix_to_iso(timestamp_by_block.get(intish(row.get("block_number")))),
                "nft_contract": row["nft_contract"].lower(),
                "standard": row["standard"],
                "token_id": str(row["token_id"]),
                "quantity": quantity,
                "recipient": recipient,
                "payer": payer,
                "stage_index": None,
                "stage_class": "CUSTOM",
                "payment_asset": asset,
                "payment_asset_normalized": normalize_asset(asset),
                "unit_primary_price_raw": str(unit_price) if unit_price is not None else None,
                "entry_gas_total_raw": str(entry_gas) if entry_gas is not None else None,
                "entry_gas_per_unit_raw": str(gas_per_unit) if gas_per_unit is not None else None,
                "unit_total_cost_raw": str(unit_price + gas_per_unit) if unit_price is not None and gas_per_unit is not None else None,
                "cost_status": status,
                "copyable_public_preliminary": False,
            })
    if len([row for row in lots if row["source"] == "SEADROP"]) != len(links):
        failures.append({"code": "SEADROP_PRIMARY_LOT_COUNT_MISMATCH", "links": len(links), "lots": len([row for row in lots if row["source"] == "SEADROP"])})
    return lots, failures


def build_erc721_lifecycle(
    transfers: list[dict[str, Any]],
    primary_lots: list[dict[str, Any]],
    exact_sales: list[dict[str, Any]],
    tx_by_hash: dict[str, dict[str, Any]],
    receipt_by_hash: dict[str, dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    primary_by_token = {
        (row["nft_contract"], row["token_id"]): row
        for row in primary_lots if row["standard"] == "ERC721"
    }
    sale_by_transfer = {row["transfer_id"]: row for row in exact_sales if row["standard"] == "ERC721"}
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in transfers:
        if row["standard"] == "ERC721":
            grouped[(row["nft_contract"], row["token_id"])].append(row)

    lifecycle_rows = []
    anomalies = []
    execution_rows = []
    for token_key, events in sorted(grouped.items()):
        events.sort(key=lambda row: (row["block_number"], row["log_index"], row["item_index"]))
        owner = None
        basis = None
        basis_asset = None
        basis_status = "UNKNOWN"
        for row in events:
            transfer_id = actual_transfer_id(row)
            from_address = row["from_address"]
            to_address = row["to_address"]
            before_owner = owner
            if from_address == ZERO_ADDRESS:
                if owner not in (None, ZERO_ADDRESS):
                    anomalies.append({"code": "REMINT_WHILE_LIVE", "nft_contract": token_key[0], "token_id": token_key[1], "transfer_id": transfer_id, "owner_before": owner})
                owner = to_address
                lot = primary_by_token.get(token_key)
                if lot and lot["recipient"] == to_address and lot.get("unit_total_cost_raw") is not None:
                    basis = int(lot["unit_total_cost_raw"])
                    basis_asset = lot.get("payment_asset_normalized")
                    basis_status = lot["cost_status"]
                else:
                    basis = None; basis_asset = None; basis_status = "PRIMARY_COST_UNRESOLVED"
            else:
                if owner is not None and owner != from_address:
                    anomalies.append({"code": "OWNER_MISMATCH", "nft_contract": token_key[0], "token_id": token_key[1], "transfer_id": transfer_id, "owner_before": owner, "event_from": from_address})
                sale = sale_by_transfer.get(transfer_id)
                if sale:
                    seller = sale["seller"]
                    buyer = sale["buyer"]
                    seller_net = intish(sale.get("seller_net_per_unit_raw"))
                    gross = intish(sale.get("gross_per_unit_raw"))
                    sale_asset = sale.get("payment_asset_normalized")
                    tx = tx_by_hash.get(sale["transaction_hash"])
                    receipt = receipt_by_hash.get(sale["transaction_hash"])
                    exit_gas = gas_cost(receipt) if tx and tx.get("from_address") == seller else 0
                    pnl = None
                    pnl_status = "ACQUISITION_COST_UNKNOWN"
                    if basis is not None and seller_net is not None and basis_asset == sale_asset:
                        pnl = seller_net - basis - (exit_gas or 0)
                        pnl_status = "PROVEN_EXACT_SAME_ASSET"
                    execution_rows.append({
                        **sale,
                        "acquisition_cost_raw": str(basis) if basis is not None else None,
                        "acquisition_asset_normalized": basis_asset,
                        "acquisition_status": basis_status,
                        "seller_exit_gas_raw": str(exit_gas) if exit_gas is not None else None,
                        "realized_pnl_raw": str(pnl) if pnl is not None else None,
                        "realized_pnl_status": pnl_status,
                    })
                    owner = buyer
                    buyer_gas = gas_cost(receipt) if tx and tx.get("from_address") == buyer else 0
                    if gross is not None:
                        basis = gross + (buyer_gas or 0)
                        basis_asset = sale_asset
                        basis_status = "SECONDARY_PURCHASE_EXACT"
                    else:
                        basis = None; basis_asset = None; basis_status = "SECONDARY_PURCHASE_ALLOCATION_UNKNOWN"
                else:
                    owner = None if to_address == ZERO_ADDRESS else to_address
                    basis = None; basis_asset = None; basis_status = "UNPROVEN_TRANSFER_RESETS_BASIS"
            lifecycle_rows.append({
                "nft_contract": token_key[0],
                "token_id": token_key[1],
                "transfer_id": transfer_id,
                "transaction_hash": row["transaction_hash"],
                "block_number": row["block_number"],
                "log_index": row["log_index"],
                "from_address": from_address,
                "to_address": to_address,
                "owner_before": before_owner,
                "owner_after": owner,
                "basis_after_raw": str(basis) if basis is not None else None,
                "basis_asset_after": basis_asset,
                "basis_status_after": basis_status,
                "sale_event_id": sale_by_transfer.get(transfer_id, {}).get("sale_event_id"),
            })
    return lifecycle_rows, anomalies, execution_rows


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    STATUS_DIR.mkdir(parents=True, exist_ok=True)
    source_records = {}
    roots = {}
    for name, spec in SOURCES.items():
        record, root = fetch_artifact(name, spec["branch"], spec["workflow"], spec["artifact"], OUT)
        source_records[name] = record
        roots[name] = root
        status = json.loads(only_file(root, spec["status_file"]).read_text(encoding="utf-8"))
        if status.get("status") != "PASS":
            raise RuntimeError(f"source {name} status is not PASS")

    normalized_root = roots["normalized"]
    primary = load_enrichment(roots["primary_enrichment"])
    secondary = load_enrichment(roots["secondary_enrichment"])

    tx_rows = primary["transactions"] + secondary["transactions"]
    receipt_rows = primary["receipts"] + secondary["receipts"]
    block_rows = primary["blocks"] + secondary["blocks"]
    internal_rows = primary["internals"] + secondary["internals"]
    tx_by_hash, tx_conflicts = merge_prefer_official(tx_rows, "transaction_hash")
    receipt_by_hash, receipt_conflicts = merge_prefer_official(receipt_rows, "transaction_hash")
    block_by_number, block_conflicts = merge_prefer_official(
        [{**row, "block_key": str(row["block_number"])} for row in block_rows], "block_key"
    )
    timestamp_by_block = {
        int(key): int(row["timestamp_unix"])
        for key, row in block_by_number.items()
        if row.get("timestamp_unix") is not None
    }
    internals_by_tx: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in internal_rows:
        tx_hash = str(row.get("transaction_hash") or "").lower()
        internals_by_tx[tx_hash].extend(internal_items(row))

    transfer_wrappers = load_jsonl_gz(only_file(roots["token_transfers"], "contract_transfer_events.jsonl.gz"))
    normalized_transfers = []
    transfer_decode_failures = []
    for wrapper in transfer_wrappers:
        try:
            normalized_transfers.extend(decode_contract_transfer(wrapper))
        except Exception as exc:
            transfer_decode_failures.append({"error": repr(exc), "wrapper": wrapper})
    for row in normalized_transfers:
        row["timestamp_unix"] = timestamp_by_block.get(row["block_number"])
        row["timestamp_utc"] = unix_to_iso(row["timestamp_unix"])
        row["transfer_id"] = actual_transfer_id(row)

    transfers_by_tx: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in normalized_transfers:
        transfers_by_tx[row["transaction_hash"]].append(row)

    flows_by_tx = {}
    all_tx_hashes = sorted(set(tx_by_hash) | set(receipt_by_hash) | set(internals_by_tx))
    flow_rows = []
    for tx_hash in all_tx_hashes:
        flows = payment_flows_for_transaction(
            tx_by_hash.get(tx_hash),
            receipt_by_hash.get(tx_hash),
            internals_by_tx.get(tx_hash, []),
        )
        flows_by_tx[tx_hash] = flows
        flow_rows.extend(flows)

    orders = read_csv(only_file(normalized_root, "seaport_orders.csv"))
    order_items = parse_order_items(read_csv(only_file(normalized_root, "seaport_order_items.csv")))
    seaport_orders, seaport_token_sales, consumed, seaport_failures = classify_seaport(
        orders, order_items, transfers_by_tx, flows_by_tx, tx_by_hash, timestamp_by_block
    )
    custom_orders, custom_token_sales, consumed = classify_custom_transfers(
        transfers_by_tx, consumed, flows_by_tx, tx_by_hash, timestamp_by_block
    )
    sale_orders = seaport_orders + custom_orders
    exact_token_sales = seaport_token_sales + custom_token_sales

    unproven = []
    for row in normalized_transfers:
        if row["from_address"] == ZERO_ADDRESS or row["to_address"] == ZERO_ADDRESS:
            continue
        if row["transfer_id"] in consumed:
            continue
        related = [order for order in sale_orders if order["transaction_hash"] == row["transaction_hash"]]
        unproven.append({
            **row,
            "classification": "UNPROVEN_SECONDARY_TRANSFER",
            "related_order_statuses": sorted({order["proof_status"] for order in related}),
        })

    primary_lots, primary_failures = build_primary_lots(
        normalized_root, tx_by_hash, receipt_by_hash, flows_by_tx, timestamp_by_block
    )
    lifecycle_rows, lifecycle_anomalies, execution_rows = build_erc721_lifecycle(
        normalized_transfers, primary_lots, exact_token_sales, tx_by_hash, receipt_by_hash
    )

    write_csv(OUT / "transactions_normalized.csv", tx_by_hash.values())
    write_csv(OUT / "receipts_normalized.csv", receipt_by_hash.values())
    write_csv(OUT / "blocks_normalized.csv", block_by_number.values())
    write_csv(OUT / "payment_flows.csv", flow_rows)
    write_csv(OUT / "nft_transfers_normalized.csv", normalized_transfers)
    write_csv(OUT / "sale_orders_classified.csv", sale_orders)
    write_csv(OUT / "proven_exact_token_sales.csv", exact_token_sales)
    write_csv(OUT / "unproven_secondary_transfers.csv", unproven)
    write_csv(OUT / "primary_token_lots.csv", primary_lots)
    write_csv(OUT / "erc721_token_lifecycle.csv", lifecycle_rows)
    write_csv(OUT / "erc721_lifecycle_anomalies.csv", lifecycle_anomalies)
    write_csv(OUT / "execution_token_sales.csv", execution_rows)
    write_csv(OUT / "source_decode_failures.csv", transfer_decode_failures)

    failures = []
    if tx_conflicts:
        failures.append({"code": "TRANSACTION_SOURCE_CONFLICTS", "count": len(tx_conflicts)})
    if receipt_conflicts:
        failures.append({"code": "RECEIPT_SOURCE_CONFLICTS", "count": len(receipt_conflicts)})
    if block_conflicts:
        failures.append({"code": "BLOCK_SOURCE_CONFLICTS", "count": len(block_conflicts)})
    if transfer_decode_failures:
        failures.append({"code": "TRANSFER_DECODE_FAILURES", "count": len(transfer_decode_failures)})
    if seaport_failures:
        failures.append({"code": "SEAPORT_CLASSIFICATION_FAILURES", "count": len(seaport_failures), "sample": seaport_failures[:25]})
    failures.extend(primary_failures)
    missing_transfer_timestamps = [row for row in normalized_transfers if row.get("timestamp_unix") is None]
    if missing_transfer_timestamps:
        failures.append({"code": "TRANSFER_TIMESTAMPS_MISSING", "count": len(missing_transfer_timestamps)})
    if lifecycle_anomalies:
        failures.append({"code": "ERC721_LIFECYCLE_ANOMALIES", "count": len(lifecycle_anomalies), "sample": lifecycle_anomalies[:25]})
    classified_nonzero = len(exact_token_sales) + len(unproven)
    nonzero_transfers = sum(
        row["from_address"] != ZERO_ADDRESS and row["to_address"] != ZERO_ADDRESS
        for row in normalized_transfers
    )
    # Bundle transfers consumed by proven order rows are not exact token-sale rows.
    consumed_bundle_only = len(consumed) - len({row["transfer_id"] for row in exact_token_sales})
    if classified_nonzero + consumed_bundle_only != nonzero_transfers:
        failures.append({
            "code": "SECONDARY_TRANSFER_CLASSIFICATION_COUNT_MISMATCH",
            "nonzero_transfers": nonzero_transfers,
            "exact_token_sales": len(exact_token_sales),
            "consumed_bundle_only": consumed_bundle_only,
            "unproven": len(unproven),
        })

    status = {
        "status": "PASS" if not failures else "FAIL",
        "generated_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "builder_run_id": RUN_ID,
        "fixed_head": FIXED_HEAD,
        "source_records": source_records,
        "counts": {
            "transactions": len(tx_by_hash),
            "receipts": len(receipt_by_hash),
            "blocks": len(block_by_number),
            "payment_flows": len(flow_rows),
            "nft_transfer_items": len(normalized_transfers),
            "secondary_nonzero_transfer_items": nonzero_transfers,
            "classified_sale_orders": len(sale_orders),
            "proven_exact_token_sales": len(exact_token_sales),
            "unproven_secondary_transfer_items": len(unproven),
            "primary_token_lots": len(primary_lots),
            "erc721_execution_sales": len(execution_rows),
        },
        "failures": failures,
        "selection_alpha_ready": False,
        "execution_alpha_ready": not any(row["code"] == "ERC721_LIFECYCLE_ANOMALIES" for row in failures),
        "copy_alpha_ready": False,
        "deepseek_handoff": "BLOCKED_PROJECT_OUTCOME_AND_ALPHA_DATASET_REQUIRED",
        "production_approved_wallets": 0,
    }
    (OUT / "MARKET_PROVENANCE_STATUS.json").write_text(json.dumps(status, indent=2, sort_keys=True), encoding="utf-8")
    (STATUS_DIR / "MARKET_PROVENANCE_STATUS.json").write_text(json.dumps(status, indent=2, sort_keys=True), encoding="utf-8")
    (STATUS_DIR / "SOURCE_RECORDS.json").write_text(json.dumps(source_records, indent=2, sort_keys=True), encoding="utf-8")
    manifest = []
    for path in sorted(OUT.rglob("*")):
        if path.is_file():
            manifest.append({
                "path": str(path.relative_to(OUT)),
                "bytes": path.stat().st_size,
                "sha256": sha256_file(path),
            })
    (OUT / "MANIFEST.json").write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    (STATUS_DIR / "MANIFEST.json").write_text((OUT / "MANIFEST.json").read_text(), encoding="utf-8")
    print(json.dumps(status, sort_keys=True), flush=True)
    if failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
