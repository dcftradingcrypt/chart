#!/usr/bin/env python3
from __future__ import annotations

import csv
import gzip
import hashlib
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable

from eth_abi import decode
from eth_utils import keccak

REPOSITORY = os.environ["GITHUB_REPOSITORY"]
TOKEN = os.environ["GITHUB_TOKEN"]
RUN_ID = int(os.environ["GITHUB_RUN_ID"])
API = f"https://api.github.com/repos/{REPOSITORY}"
FIXED_HEAD = 48_264_433
OUT = Path("rhc-normalized-universe-output")
STATUS_DIR = Path("rhc-normalized-universe-stage")

SOURCES = {
    "main": {
        "branch": "chatgpt/rhc-data-aggregate-20260829",
        "workflow": "RHC aggregate all wallet and NFT evidence",
        "artifact": "rhc-data-aggregate",
    },
    "seadrop_config": {
        "branch": "chatgpt/rhc-seadrop-config-aggregate-20260829",
        "workflow": "RHC aggregate complete SeaDrop configuration history",
        "artifact": "rhc-seadrop-config-aggregate",
    },
}

ITEM_TYPE = {
    0: "NATIVE",
    1: "ERC20",
    2: "ERC721",
    3: "ERC1155",
    4: "ERC721_WITH_CRITERIA",
    5: "ERC1155_WITH_CRITERIA",
}
NFT_ITEM_TYPES = {2, 3, 4, 5}
PAYMENT_ITEM_TYPES = {0, 1}
ZERO = "0x0000000000000000000000000000000000000000"
SEADROP_ADDRESS = "0x00005ea00ac477b1030ce78506496e8c2de24bf5"
SEAPORT_ADDRESS = "0x0000000000000068f116a894984e2db1123eb395"


def event_topic(signature: str) -> str:
    return "0x" + keccak(text=signature).hex()


TOPICS = {
    "SeaDropMint": event_topic("SeaDropMint(address,address,address,address,uint256,uint256,uint256,uint256)"),
    "PublicDropUpdated": event_topic("PublicDropUpdated(address,(uint80,uint48,uint48,uint16,uint16,bool))"),
    "AllowedFeeRecipientUpdated": event_topic("AllowedFeeRecipientUpdated(address,address,bool)"),
    "CreatorPayoutAddressUpdated": event_topic("CreatorPayoutAddressUpdated(address,address)"),
    "PayerUpdated": event_topic("PayerUpdated(address,address,bool)"),
    "DropURIUpdated": event_topic("DropURIUpdated(address,string)"),
    "AllowListUpdated": event_topic("AllowListUpdated(address,bytes32,bytes32,string[],string)"),
    "SignedMintValidationParamsUpdated": event_topic("SignedMintValidationParamsUpdated(address,address,(uint80,uint24,uint40,uint40,uint40,uint16,uint16))"),
    "OrderFulfilled": event_topic("OrderFulfilled(bytes32,address,address,address,(uint8,address,uint256,uint256)[],(uint8,address,uint256,uint256,address)[])"),
}


def canonical(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def api(path: str, attempts: int = 8) -> Any:
    url = path if path.startswith("https://") else API + path
    last_error = None
    for attempt in range(attempts):
        request = urllib.request.Request(
            url,
            headers={
                "Authorization": f"Bearer {TOKEN}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
                "User-Agent": "RHC-Normalized-Universe/1.0",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=180) as response:
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            last_error = exc
            if exc.code in (403, 429, 500, 502, 503, 504) and attempt + 1 < attempts:
                time.sleep(min(120, 5 * (2 ** attempt)))
                continue
            detail = exc.read(1000).decode("utf-8", "replace")
            raise RuntimeError(f"GitHub API HTTP {exc.code} {url}: {detail}") from exc
        except Exception as exc:
            last_error = exc
            if attempt + 1 < attempts:
                time.sleep(min(90, 5 * (2 ** attempt)))
                continue
    raise RuntimeError(f"GitHub API exhausted: {url}: {last_error!r}")


def wait_run(branch: str, workflow: str, timeout: int = 19_800) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        query = urllib.parse.urlencode({"branch": branch, "event": "pull_request", "per_page": 100})
        payload = api(f"/actions/runs?{query}")
        rows = sorted(
            [row for row in payload.get("workflow_runs", []) if row.get("name") == workflow],
            key=lambda row: int(row["id"]),
            reverse=True,
        )
        if not rows:
            print(f"waiting for {branch}/{workflow}", flush=True)
            time.sleep(30)
            continue
        run = rows[0]
        print(json.dumps({
            "branch": branch,
            "id": run["id"],
            "status": run.get("status"),
            "conclusion": run.get("conclusion"),
            "head_sha": run.get("head_sha"),
        }, sort_keys=True), flush=True)
        if run.get("status") == "completed":
            if run.get("conclusion") != "success":
                raise RuntimeError(f"source run failed: {run.get('html_url')}")
            return run
        time.sleep(45)
    raise TimeoutError(f"source run timeout: {branch}/{workflow}")


def download(url: str, destination: Path, attempts: int = 8) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    last_error = None
    for attempt in range(attempts):
        request = urllib.request.Request(
            url,
            headers={
                "Authorization": f"Bearer {TOKEN}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
                "User-Agent": "RHC-Normalized-Universe/1.0",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=300) as response:
                destination.write_bytes(response.read())
            return
        except Exception as exc:
            last_error = exc
            if attempt + 1 < attempts:
                time.sleep(min(120, 5 * (2 ** attempt)))
                continue
    raise RuntimeError(f"artifact download failed: {url}: {last_error!r}")


def fetch_source(name: str, spec: dict[str, str]) -> tuple[dict[str, Any], Path]:
    run = wait_run(spec["branch"], spec["workflow"])
    payload = api(f"/actions/runs/{run['id']}/artifacts?per_page=100")
    matches = [row for row in payload.get("artifacts", []) if row.get("name") == spec["artifact"]]
    if len(matches) != 1:
        raise RuntimeError(f"artifact mismatch for {name}: {len(matches)}")
    artifact = matches[0]
    zip_path = OUT / "source_artifacts" / f"{name}.zip"
    download(artifact["archive_download_url"], zip_path)
    destination = OUT / "sources" / name
    destination.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path) as archive:
        archive.extractall(destination)
    return {
        "run_id": run["id"],
        "head_sha": run.get("head_sha"),
        "html_url": run.get("html_url"),
        "artifact_id": artifact["id"],
        "artifact_sha256": hashlib.sha256(zip_path.read_bytes()).hexdigest(),
        "artifact_bytes": zip_path.stat().st_size,
    }, destination


def only_file(root: Path, filename: str) -> Path:
    paths = sorted(root.rglob(filename))
    if len(paths) != 1:
        raise RuntimeError(f"expected one {filename} under {root}, found {len(paths)}")
    return paths[0]


def load_jsonl_gz(path: Path) -> list[dict[str, Any]]:
    rows = []
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                value = json.loads(line)
                if not isinstance(value, dict):
                    raise RuntimeError(f"non-object row in {path}")
                rows.append(value)
    return rows


def intish(value: Any) -> int:
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        return int(value, 16) if value.startswith("0x") else int(value)
    raise ValueError(value)


def hex_bytes(value: str) -> bytes:
    value = value[2:] if value.startswith("0x") else value
    return bytes.fromhex(value)


def topic_address(value: str) -> str:
    value = value.lower().removeprefix("0x")
    if len(value) != 64:
        raise ValueError(value)
    return "0x" + value[-40:]


def data_address(value: Any) -> str:
    if isinstance(value, bytes):
        return "0x" + value[-20:].hex()
    if isinstance(value, str):
        value = value.lower().removeprefix("0x")
        return "0x" + value[-40:]
    raise ValueError(value)


def event_key(row: dict[str, Any]) -> tuple[str, int]:
    tx = str(row.get("transactionHash") or row.get("transaction_hash") or "").lower()
    log_index = intish(row.get("logIndex") or row.get("log_index") or "0x0")
    return tx, log_index


def block_number(row: dict[str, Any]) -> int:
    return intish(row.get("blockNumber") or row.get("block_number"))


def write_csv(path: Path, rows: Iterable[dict[str, Any]]) -> None:
    materialized = list(rows)
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = sorted({key for row in materialized for key in row}) if materialized else ["empty"]
    with path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in materialized:
            writer.writerow({
                key: json.dumps(value, ensure_ascii=False, sort_keys=True)
                if isinstance(value, (dict, list, tuple))
                else value
                for key, value in row.items()
            })


def decode_seadrop_mint(row: dict[str, Any]) -> dict[str, Any]:
    topics = [str(value).lower() for value in row.get("topics") or []]
    if len(topics) != 4:
        raise RuntimeError(f"SeaDropMint topics={len(topics)}")
    payer, quantity, unit_price, fee_bps, stage = decode(
        ["address", "uint256", "uint256", "uint256", "uint256"],
        hex_bytes(str(row.get("data") or "0x")),
    )
    return {
        "event_id": f"{event_key(row)[0]}:{event_key(row)[1]}",
        "transaction_hash": event_key(row)[0],
        "log_index": event_key(row)[1],
        "block_number": block_number(row),
        "block_hash": str(row.get("blockHash") or row.get("block_hash") or "").lower(),
        "nft_contract": topic_address(topics[1]),
        "minter": topic_address(topics[2]),
        "fee_recipient": topic_address(topics[3]),
        "payer": data_address(payer),
        "quantity": int(quantity),
        "unit_mint_price_wei": int(unit_price),
        "fee_bps": int(fee_bps),
        "drop_stage_index": int(stage),
        "stage_class": "PUBLIC" if int(stage) == 0 else f"NONPUBLIC_{int(stage)}",
        "is_free": int(unit_price) == 0,
        "is_self_funded": data_address(payer) == topic_address(topics[2]),
    }


def decode_public_drop_update(row: dict[str, Any]) -> dict[str, Any]:
    topics = [str(value).lower() for value in row.get("topics") or []]
    if len(topics) != 2:
        raise RuntimeError(f"PublicDropUpdated topics={len(topics)}")
    decoded = decode(
        ["(uint80,uint48,uint48,uint16,uint16,bool)"],
        hex_bytes(str(row.get("data") or "0x")),
    )[0]
    mint_price, start_time, end_time, max_wallet, fee_bps, restrict_fee = decoded
    return {
        "event_id": f"{event_key(row)[0]}:{event_key(row)[1]}",
        "transaction_hash": event_key(row)[0],
        "log_index": event_key(row)[1],
        "block_number": block_number(row),
        "block_hash": str(row.get("blockHash") or "").lower(),
        "nft_contract": topic_address(topics[1]),
        "mint_price_wei": int(mint_price),
        "start_time_unix": int(start_time),
        "end_time_unix": int(end_time),
        "max_total_mintable_by_wallet": int(max_wallet),
        "fee_bps": int(fee_bps),
        "restrict_fee_recipients": bool(restrict_fee),
        "is_active_config": int(end_time) > int(start_time),
    }


def normalize_global_mints(
    erc721_rows: list[dict[str, Any]],
    single_rows: list[dict[str, Any]],
    batch_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for row in erc721_rows:
        topics = [str(value).lower() for value in row.get("topics") or []]
        if len(topics) != 4:
            raise RuntimeError(f"ERC721 mint topic count {len(topics)}")
        output.append({
            "event_id": f"{event_key(row)[0]}:{event_key(row)[1]}",
            "transaction_hash": event_key(row)[0],
            "log_index": event_key(row)[1],
            "block_number": block_number(row),
            "block_hash": str(row.get("blockHash") or "").lower(),
            "standard": "ERC721",
            "nft_contract": str(row.get("address") or "").lower(),
            "recipient": topic_address(topics[2]),
            "token_id": str(intish(topics[3])),
            "quantity": 1,
            "source_event_type": "Transfer",
        })
    for row in single_rows:
        topics = [str(value).lower() for value in row.get("topics") or []]
        if len(topics) != 4:
            raise RuntimeError(f"ERC1155 single topic count {len(topics)}")
        token_id, quantity = decode(["uint256", "uint256"], hex_bytes(str(row.get("data") or "0x")))
        output.append({
            "event_id": f"{event_key(row)[0]}:{event_key(row)[1]}",
            "transaction_hash": event_key(row)[0],
            "log_index": event_key(row)[1],
            "block_number": block_number(row),
            "block_hash": str(row.get("blockHash") or "").lower(),
            "standard": "ERC1155",
            "nft_contract": str(row.get("address") or "").lower(),
            "recipient": topic_address(topics[3]),
            "operator": topic_address(topics[1]),
            "token_id": str(int(token_id)),
            "quantity": int(quantity),
            "source_event_type": "TransferSingle",
        })
    for row in batch_rows:
        topics = [str(value).lower() for value in row.get("topics") or []]
        if len(topics) != 4:
            raise RuntimeError(f"ERC1155 batch topic count {len(topics)}")
        ids, values = decode(["uint256[]", "uint256[]"], hex_bytes(str(row.get("data") or "0x")))
        if len(ids) != len(values):
            raise RuntimeError("ERC1155 batch id/value mismatch")
        for item_index, (token_id, quantity) in enumerate(zip(ids, values)):
            output.append({
                "event_id": f"{event_key(row)[0]}:{event_key(row)[1]}:{item_index}",
                "transaction_hash": event_key(row)[0],
                "log_index": event_key(row)[1],
                "batch_item_index": item_index,
                "block_number": block_number(row),
                "block_hash": str(row.get("blockHash") or "").lower(),
                "standard": "ERC1155",
                "nft_contract": str(row.get("address") or "").lower(),
                "recipient": topic_address(topics[3]),
                "operator": topic_address(topics[1]),
                "token_id": str(int(token_id)),
                "quantity": int(quantity),
                "source_event_type": "TransferBatch",
            })
    return output


def decode_seaport(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    orders = []
    items = []
    failures = []
    for row in rows:
        try:
            topics = [str(value).lower() for value in row.get("topics") or []]
            if len(topics) != 3:
                raise RuntimeError(f"OrderFulfilled topics={len(topics)}")
            order_hash, recipient, offer, consideration = decode(
                [
                    "bytes32",
                    "address",
                    "(uint8,address,uint256,uint256)[]",
                    "(uint8,address,uint256,uint256,address)[]",
                ],
                hex_bytes(str(row.get("data") or "0x")),
            )
            event_id = f"{event_key(row)[0]}:{event_key(row)[1]}"
            offerer = topic_address(topics[1])
            zone = topic_address(topics[2])
            recipient_address = data_address(recipient)
            order_row = {
                "event_id": event_id,
                "transaction_hash": event_key(row)[0],
                "log_index": event_key(row)[1],
                "block_number": block_number(row),
                "block_hash": str(row.get("blockHash") or "").lower(),
                "order_hash": "0x" + bytes(order_hash).hex(),
                "offerer": offerer,
                "zone": zone,
                "recipient": recipient_address,
                "offer_item_count": len(offer),
                "consideration_item_count": len(consideration),
                "offer_json": [],
                "consideration_json": [],
            }
            for side, values in (("offer", offer), ("consideration", consideration)):
                for index, value in enumerate(values):
                    if side == "offer":
                        item_type, token, identifier, amount = value
                        item_recipient = None
                    else:
                        item_type, token, identifier, amount, item_recipient_raw = value
                        item_recipient = data_address(item_recipient_raw)
                    item = {
                        "event_id": event_id,
                        "transaction_hash": event_key(row)[0],
                        "log_index": event_key(row)[1],
                        "block_number": block_number(row),
                        "order_hash": order_row["order_hash"],
                        "offerer": offerer,
                        "zone": zone,
                        "order_recipient": recipient_address,
                        "side": side,
                        "item_index": index,
                        "item_type": int(item_type),
                        "item_type_name": ITEM_TYPE.get(int(item_type), f"UNKNOWN_{int(item_type)}"),
                        "token": data_address(token),
                        "identifier": str(int(identifier)),
                        "amount": str(int(amount)),
                        "item_recipient": item_recipient,
                        "is_nft": int(item_type) in NFT_ITEM_TYPES,
                        "is_payment": int(item_type) in PAYMENT_ITEM_TYPES,
                    }
                    items.append(item)
                    order_row[f"{side}_json"].append(item)
            offer_nft = sum(1 for item in order_row["offer_json"] if item["is_nft"])
            consideration_nft = sum(1 for item in order_row["consideration_json"] if item["is_nft"])
            offer_payment = sum(1 for item in order_row["offer_json"] if item["is_payment"])
            consideration_payment = sum(1 for item in order_row["consideration_json"] if item["is_payment"])
            if offer_nft and consideration_payment:
                orientation = "LISTING_OR_NFT_OFFER_SIDE"
            elif consideration_nft and offer_payment:
                orientation = "BID_OR_PAYMENT_OFFER_SIDE"
            elif offer_nft or consideration_nft:
                orientation = "NFT_BUNDLE_OR_MIXED"
            else:
                orientation = "NO_NFT_ITEM"
            order_row.update({
                "offer_nft_items": offer_nft,
                "consideration_nft_items": consideration_nft,
                "offer_payment_items": offer_payment,
                "consideration_payment_items": consideration_payment,
                "orientation": orientation,
            })
            orders.append(order_row)
        except Exception as exc:
            failures.append({
                "transaction_hash": event_key(row)[0],
                "log_index": event_key(row)[1],
                "block_number": block_number(row),
                "error": repr(exc),
                "raw_data": row.get("data"),
                "topics": row.get("topics"),
            })
    return orders, items, failures


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    STATUS_DIR.mkdir(parents=True, exist_ok=True)
    source_records = {}
    source_roots = {}
    for name, spec in SOURCES.items():
        record, root = fetch_source(name, spec)
        source_records[name] = record
        source_roots[name] = root

    main_root = source_roots["main"]
    config_root = source_roots["seadrop_config"]
    main_status = json.loads(only_file(main_root, "AGGREGATE_STATUS.json").read_text(encoding="utf-8"))
    config_status = json.loads(only_file(config_root, "AGGREGATE_STATUS.json").read_text(encoding="utf-8"))
    if main_status.get("status") != "PASS" or config_status.get("status") != "PASS":
        raise RuntimeError("upstream aggregate is not PASS")

    canonical_seadrop = load_jsonl_gz(only_file(main_root, "canonical_seadrop_events.jsonl.gz"))
    canonical_seaport = load_jsonl_gz(only_file(main_root, "canonical_seaport_events.jsonl.gz"))
    global_erc721 = load_jsonl_gz(only_file(main_root, "global_erc721_mint_events.jsonl.gz"))
    global_single = load_jsonl_gz(only_file(main_root, "global_erc1155_single_mint_events.jsonl.gz"))
    global_batch = load_jsonl_gz(only_file(main_root, "global_erc1155_batch_mint_events.jsonl.gz"))
    seadrop_all_logs = load_jsonl_gz(only_file(config_root, "seadrop_all_logs.jsonl.gz"))

    decode_failures = []
    seadrop_mints = []
    for row in canonical_seadrop:
        try:
            seadrop_mints.append(decode_seadrop_mint(row))
        except Exception as exc:
            decode_failures.append({"type": "SeaDropMint", "key": event_key(row), "error": repr(exc)})

    public_updates = []
    config_events = []
    all_mint_keys = set()
    for row in seadrop_all_logs:
        topics = [str(value).lower() for value in row.get("topics") or []]
        topic0 = topics[0] if topics else "NO_TOPIC"
        name = next((key for key, value in TOPICS.items() if value == topic0), "UNKNOWN")
        config_events.append({
            "event_id": f"{event_key(row)[0]}:{event_key(row)[1]}",
            "transaction_hash": event_key(row)[0],
            "log_index": event_key(row)[1],
            "block_number": block_number(row),
            "block_hash": str(row.get("blockHash") or "").lower(),
            "topic0": topic0,
            "event_name": name,
            "topics_json": topics,
            "data": row.get("data"),
        })
        if topic0 == TOPICS["SeaDropMint"]:
            all_mint_keys.add(event_key(row))
        elif topic0 == TOPICS["PublicDropUpdated"]:
            try:
                public_updates.append(decode_public_drop_update(row))
            except Exception as exc:
                decode_failures.append({"type": "PublicDropUpdated", "key": event_key(row), "error": repr(exc)})

    normalized_global = normalize_global_mints(global_erc721, global_single, global_batch)
    global_by_route: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in normalized_global:
        global_by_route[(row["transaction_hash"], row["nft_contract"], row["recipient"])].append(row)

    token_links = []
    unmatched_seadrop = []
    matched_global_ids = set()
    for mint in seadrop_mints:
        route = (mint["transaction_hash"], mint["nft_contract"], mint["minter"])
        candidates = global_by_route.get(route, [])
        total_quantity = sum(int(row["quantity"]) for row in candidates)
        if total_quantity != int(mint["quantity"]):
            unmatched_seadrop.append({
                **mint,
                "matched_global_events": len(candidates),
                "matched_global_quantity": total_quantity,
                "mismatch": total_quantity - int(mint["quantity"]),
            })
        for row in candidates:
            matched_global_ids.add(row["event_id"])
            token_links.append({
                "seadrop_event_id": mint["event_id"],
                "global_mint_event_id": row["event_id"],
                "transaction_hash": mint["transaction_hash"],
                "nft_contract": mint["nft_contract"],
                "minter": mint["minter"],
                "payer": mint["payer"],
                "drop_stage_index": mint["drop_stage_index"],
                "unit_mint_price_wei": mint["unit_mint_price_wei"],
                "token_id": row["token_id"],
                "token_quantity": row["quantity"],
                "standard": row["standard"],
            })

    for row in normalized_global:
        row["matched_to_seadrop"] = row["event_id"] in matched_global_ids
        row["primary_route"] = "SEADROP" if row["matched_to_seadrop"] else "NON_SEADROP_ZERO_MINT"

    orders, order_items, seaport_failures = decode_seaport(canonical_seaport)
    decode_failures.extend({"type": "OrderFulfilled", **row} for row in seaport_failures)

    contract_metrics: dict[str, dict[str, Any]] = {}
    def metric(contract: str) -> dict[str, Any]:
        contract = contract.lower()
        return contract_metrics.setdefault(contract, {
            "nft_contract": contract,
            "global_mint_event_rows": 0,
            "global_mint_quantity": 0,
            "seadrop_mint_event_rows": 0,
            "seadrop_mint_quantity": 0,
            "paid_public_quantity": 0,
            "free_public_quantity": 0,
            "paid_nonpublic_quantity": 0,
            "free_nonpublic_quantity": 0,
            "non_seadrop_zero_mint_quantity": 0,
            "public_prices_wei": set(),
            "public_update_prices_wei": set(),
            "public_update_count": 0,
            "first_mint_block": None,
            "last_mint_block": None,
        })

    for row in normalized_global:
        value = metric(row["nft_contract"])
        value["global_mint_event_rows"] += 1
        value["global_mint_quantity"] += int(row["quantity"])
        if not row["matched_to_seadrop"]:
            value["non_seadrop_zero_mint_quantity"] += int(row["quantity"])
        block = int(row["block_number"])
        value["first_mint_block"] = block if value["first_mint_block"] is None else min(value["first_mint_block"], block)
        value["last_mint_block"] = block if value["last_mint_block"] is None else max(value["last_mint_block"], block)

    for row in seadrop_mints:
        value = metric(row["nft_contract"])
        quantity = int(row["quantity"])
        value["seadrop_mint_event_rows"] += 1
        value["seadrop_mint_quantity"] += quantity
        if int(row["drop_stage_index"]) == 0:
            value["public_prices_wei"].add(int(row["unit_mint_price_wei"]))
            if int(row["unit_mint_price_wei"]) > 0:
                value["paid_public_quantity"] += quantity
            else:
                value["free_public_quantity"] += quantity
        elif int(row["unit_mint_price_wei"]) > 0:
            value["paid_nonpublic_quantity"] += quantity
        else:
            value["free_nonpublic_quantity"] += quantity

    for row in public_updates:
        value = metric(row["nft_contract"])
        value["public_update_count"] += 1
        value["public_update_prices_wei"].add(int(row["mint_price_wei"]))

    project_contracts = []
    for contract, value in sorted(contract_metrics.items()):
        public_prices = sorted(value.pop("public_prices_wei"))
        update_prices = sorted(value.pop("public_update_prices_wei"))
        preliminary_strict = (
            value["paid_public_quantity"] > 0
            and value["free_public_quantity"] == 0
            and value["paid_nonpublic_quantity"] == 0
            and value["free_nonpublic_quantity"] == 0
            and value["non_seadrop_zero_mint_quantity"] == 0
            and len(public_prices) == 1
            and all(price > 0 for price in update_prices)
        )
        project_contracts.append({
            **value,
            "public_prices_wei": public_prices,
            "public_update_prices_wei": update_prices,
            "public_price_changed_in_mints": len(public_prices) > 1,
            "public_price_changed_in_config": len(update_prices) > 1,
            "preliminary_strict_paid_public": preliminary_strict,
            "requires_tx_receipt_enrichment": True,
            "requires_contract_identity_review": True,
        })

    wallet_entries = []
    for row in seadrop_mints:
        wallet_entries.append({
            **row,
            "entry_wallet": row["minter"],
            "economic_payer": row["payer"],
            "entry_route": "SEADROP_PUBLIC" if row["drop_stage_index"] == 0 else "SEADROP_NONPUBLIC",
            "copyable_route_preliminary": row["drop_stage_index"] == 0 and row["unit_mint_price_wei"] > 0,
            "requires_timestamp": True,
            "requires_entity_clustering": True,
        })
    for row in normalized_global:
        if not row["matched_to_seadrop"]:
            wallet_entries.append({
                "event_id": row["event_id"],
                "transaction_hash": row["transaction_hash"],
                "log_index": row["log_index"],
                "block_number": row["block_number"],
                "block_hash": row["block_hash"],
                "nft_contract": row["nft_contract"],
                "entry_wallet": row["recipient"],
                "economic_payer": None,
                "quantity": row["quantity"],
                "token_id": row["token_id"],
                "unit_mint_price_wei": None,
                "drop_stage_index": None,
                "entry_route": "NON_SEADROP_ZERO_MINT",
                "copyable_route_preliminary": False,
                "requires_timestamp": True,
                "requires_entity_clustering": True,
                "requires_payment_reconstruction": True,
            })

    nft_items = [row for row in order_items if row["is_nft"]]
    payment_items = [row for row in order_items if row["is_payment"]]
    sale_candidates = []
    items_by_event: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in order_items:
        items_by_event[item["event_id"]].append(item)
    for order in orders:
        event_items = items_by_event[order["event_id"]]
        nft = [item for item in event_items if item["is_nft"]]
        payment = [item for item in event_items if item["is_payment"]]
        for item in nft:
            sale_candidates.append({
                "event_id": order["event_id"],
                "transaction_hash": order["transaction_hash"],
                "log_index": order["log_index"],
                "block_number": order["block_number"],
                "order_hash": order["order_hash"],
                "orientation": order["orientation"],
                "offerer": order["offerer"],
                "order_recipient": order["recipient"],
                "nft_side": item["side"],
                "nft_contract": item["token"],
                "token_id": item["identifier"],
                "nft_amount": item["amount"],
                "payment_items_json": payment,
                "nft_item_count_in_order": len(nft),
                "payment_item_count_in_order": len(payment),
                "bundle_or_multi_item": len(nft) != 1,
                "requires_receipt_transfer_enrichment": True,
                "requires_timestamp": True,
            })

    write_csv(OUT / "seadrop_mints.csv", seadrop_mints)
    write_csv(OUT / "seadrop_public_drop_updates.csv", public_updates)
    write_csv(OUT / "seadrop_config_events.csv", config_events)
    write_csv(OUT / "global_nft_mints.csv", normalized_global)
    write_csv(OUT / "seadrop_mint_token_links.csv", token_links)
    write_csv(OUT / "unmatched_seadrop_mints.csv", unmatched_seadrop)
    write_csv(OUT / "project_contracts_pre_enrichment.csv", project_contracts)
    write_csv(OUT / "wallet_primary_entries_pre_enrichment.csv", wallet_entries)
    write_csv(OUT / "seaport_orders.csv", orders)
    write_csv(OUT / "seaport_order_items.csv", order_items)
    write_csv(OUT / "seaport_nft_items.csv", nft_items)
    write_csv(OUT / "seaport_payment_items.csv", payment_items)
    write_csv(OUT / "seaport_sale_candidates.csv", sale_candidates)
    write_csv(OUT / "decode_failures.csv", decode_failures)

    primary_txs = sorted({row["transaction_hash"] for row in wallet_entries})
    market_txs = sorted({row["transaction_hash"] for row in orders})
    block_numbers = sorted({int(row["block_number"]) for row in wallet_entries} | {int(row["block_number"]) for row in orders})
    contracts = sorted({row["nft_contract"] for row in project_contracts})
    (OUT / "primary_transaction_hashes.txt").write_text("\n".join(primary_txs) + "\n", encoding="utf-8")
    (OUT / "market_transaction_hashes.txt").write_text("\n".join(market_txs) + "\n", encoding="utf-8")
    (OUT / "event_block_numbers.txt").write_text("\n".join(map(str, block_numbers)) + "\n", encoding="utf-8")
    (OUT / "nft_contracts.txt").write_text("\n".join(contracts) + "\n", encoding="utf-8")

    canonical_mint_keys = {event_key(row) for row in canonical_seadrop}
    failures = []
    if canonical_mint_keys != all_mint_keys:
        failures.append({
            "code": "SEADROP_MINT_SET_MISMATCH",
            "canonical_only": len(canonical_mint_keys - all_mint_keys),
            "all_logs_only": len(all_mint_keys - canonical_mint_keys),
        })
    if decode_failures:
        failures.append({"code": "DECODE_FAILURES", "count": len(decode_failures)})
    if unmatched_seadrop:
        failures.append({"code": "SEADROP_TRANSFER_LINK_MISMATCH", "count": len(unmatched_seadrop)})
    if not project_contracts:
        failures.append({"code": "EMPTY_PROJECT_CONTRACTS"})
    if not orders:
        failures.append({"code": "EMPTY_SEAPORT_ORDERS"})

    status = {
        "status": "PASS" if not failures else "FAIL",
        "generated_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "builder_run_id": RUN_ID,
        "fixed_head": FIXED_HEAD,
        "source_records": source_records,
        "raw_counts": {
            "canonical_seadrop_logs": len(canonical_seadrop),
            "seadrop_all_logs": len(seadrop_all_logs),
            "canonical_seaport_logs": len(canonical_seaport),
            "global_erc721_mint_logs": len(global_erc721),
            "global_erc1155_single_logs": len(global_single),
            "global_erc1155_batch_logs": len(global_batch),
        },
        "normalized_counts": {
            "seadrop_mints": len(seadrop_mints),
            "public_drop_updates": len(public_updates),
            "global_nft_mint_items": len(normalized_global),
            "seadrop_token_links": len(token_links),
            "project_contracts": len(project_contracts),
            "wallet_primary_entries": len(wallet_entries),
            "seaport_orders": len(orders),
            "seaport_order_items": len(order_items),
            "seaport_sale_candidates": len(sale_candidates),
        },
        "decode_failure_count": len(decode_failures),
        "unmatched_seadrop_count": len(unmatched_seadrop),
        "failures": failures,
        "selection_alpha_ready": False,
        "execution_alpha_ready": False,
        "copy_alpha_ready": False,
        "deepseek_handoff": "BLOCKED_TX_RECEIPT_AND_TIMESTAMP_ENRICHMENT_REQUIRED",
        "production_approved_wallets": 0,
    }
    (OUT / "NORMALIZATION_STATUS.json").write_text(json.dumps(status, indent=2, sort_keys=True), encoding="utf-8")
    (STATUS_DIR / "NORMALIZATION_STATUS.json").write_text(json.dumps(status, indent=2, sort_keys=True), encoding="utf-8")
    (STATUS_DIR / "SOURCE_RECORDS.json").write_text(json.dumps(source_records, indent=2, sort_keys=True), encoding="utf-8")

    manifest = []
    for path in sorted(OUT.rglob("*")):
        if path.is_file():
            manifest.append({
                "path": str(path.relative_to(OUT)),
                "bytes": path.stat().st_size,
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            })
    (OUT / "MANIFEST.json").write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    (STATUS_DIR / "MANIFEST.json").write_text((OUT / "MANIFEST.json").read_text(), encoding="utf-8")
    print(json.dumps(status, sort_keys=True), flush=True)
    if failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
