#!/usr/bin/env python3
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import random
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from pathlib import Path
from typing import Any

from Crypto.Hash import keccak

RPC_URL = "https://rpc.mainnet.chain.robinhood.com"
EXPLORERS = (
    "https://robinhoodchain.blockscout.com/api",
    "https://explorer.hoodmarketcap.com/api",
)
ZERO = "0x0000000000000000000000000000000000000000"
ZERO_TOPIC = "0x" + "0" * 64
UA = "RHC-NFT-Transfer-History/1.0"


def topic(signature: str) -> str:
    digest = keccak.new(digest_bits=256)
    digest.update(signature.encode("utf-8"))
    return "0x" + digest.hexdigest()


TRANSFER = topic("Transfer(address,address,uint256)")
TRANSFER_SINGLE = topic("TransferSingle(address,address,address,uint256,uint256)")
TRANSFER_BATCH = topic("TransferBatch(address,address,address,uint256[],uint256[])")


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def intish(value: Any, default: int = 0) -> int:
    try:
        if isinstance(value, str) and value.startswith("0x"):
            return int(value, 16)
        return int(value)
    except Exception:
        return default


def addr_topic(value: str) -> str:
    return "0x" + value[-40:].lower()


def request_json(url: str, *, payload: dict[str, Any] | list[dict[str, Any]] | None = None, attempts: int = 7) -> Any:
    body = json.dumps(payload, separators=(",", ":")).encode("utf-8") if payload is not None else None
    last: Exception | None = None
    for attempt in range(attempts):
        time.sleep(0.10 + random.random() * 0.22)
        request = urllib.request.Request(
            url,
            data=body,
            method="POST" if body is not None else "GET",
            headers={"Content-Type": "application/json", "Accept": "application/json", "User-Agent": UA},
        )
        try:
            with urllib.request.urlopen(request, timeout=80) as response:
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            last = exc
            if exc.code not in (408, 425, 429, 500, 502, 503, 504):
                raise
        except Exception as exc:  # noqa: BLE001
            last = exc
        time.sleep(min(60.0, 2 ** min(attempt, 5) + random.random() * 4))
    raise RuntimeError(f"request failed: {url}: {last}")


def normalize_log(row: dict[str, Any], source: str, standard: str, event_name: str) -> dict[str, Any]:
    return {
        "contract_address": str(row.get("address") or "").lower(),
        "standard": standard,
        "event_name": event_name,
        "topics": [str(value).lower() for value in (row.get("topics") or [])],
        "data": str(row.get("data") or "0x").lower(),
        "block_number": intish(row.get("blockNumber") or row.get("block_number")),
        "block_hash": str(row.get("blockHash") or row.get("block_hash") or "").lower(),
        "transaction_hash": str(row.get("transactionHash") or row.get("transaction_hash") or "").lower(),
        "transaction_index": intish(row.get("transactionIndex") or row.get("transaction_index")),
        "log_index": intish(row.get("logIndex") or row.get("log_index")),
        "removed": bool(row.get("removed", False)),
        "source": source,
    }


def rpc_logs(address: str, topic0: str, start: int, end: int, standard: str, event_name: str) -> list[dict[str, Any]]:
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_getLogs",
        "params": [{"fromBlock": hex(start), "toBlock": hex(end), "address": address, "topics": [topic0]}],
    }
    data = request_json(RPC_URL, payload=payload)
    if isinstance(data, dict) and data.get("error"):
        raise RuntimeError(json.dumps(data["error"], sort_keys=True))
    result = data.get("result") if isinstance(data, dict) else None
    if not isinstance(result, list):
        raise RuntimeError(f"invalid RPC log result: {data}")
    return [normalize_log(row, "ROBINHOOD_OFFICIAL_RPC", standard, event_name) for row in result if isinstance(row, dict)]


def explorer_logs(base: str, address: str, topic0: str, start: int, end: int, standard: str, event_name: str) -> tuple[list[dict[str, Any]], bool]:
    query = urllib.parse.urlencode(
        {
            "module": "logs",
            "action": "getLogs",
            "fromBlock": start,
            "toBlock": end,
            "address": address,
            "topic0": topic0,
        }
    )
    data = request_json(f"{base}?{query}")
    result = data.get("result") if isinstance(data, dict) else None
    if not isinstance(result, list):
        message = str(data.get("message") or data.get("result") or "") if isinstance(data, dict) else ""
        if "no record" in message.lower() or "no log" in message.lower():
            return [], False
        raise RuntimeError(f"invalid explorer logs: {data}")
    rows = [normalize_log(row, base, standard, event_name) for row in result if isinstance(row, dict)]
    return rows, len(rows) >= 1000


def collect_range(
    address: str,
    standard: str,
    event_name: str,
    topic0: str,
    start: int,
    end: int,
    rows: dict[tuple[str, str, int], dict[str, Any]],
    unresolved: list[dict[str, Any]],
    depth: int = 0,
) -> None:
    errors: list[str] = []
    try:
        found = rpc_logs(address, topic0, start, end, standard, event_name)
        for row in found:
            rows[(row["block_hash"], row["transaction_hash"], row["log_index"])] = row
        return
    except Exception as exc:  # noqa: BLE001
        errors.append(f"rpc:{exc!r}")
    for base in EXPLORERS:
        try:
            found, capped = explorer_logs(base, address, topic0, start, end, standard, event_name)
            if capped and start < end:
                errors.append(f"{base}:CAP_1000")
                break
            for row in found:
                rows[(row["block_hash"], row["transaction_hash"], row["log_index"])] = row
            return
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{base}:{exc!r}")
    if start == end:
        unresolved.append({"address": address, "standard": standard, "event_name": event_name, "start": start, "end": end, "errors": errors})
        return
    middle = (start + end) // 2
    collect_range(address, standard, event_name, topic0, start, middle, rows, unresolved, depth + 1)
    collect_range(address, standard, event_name, topic0, middle + 1, end, rows, unresolved, depth + 1)


def decode_uint_array(data: bytes, offset: int) -> list[int]:
    if offset < 0 or offset + 32 > len(data):
        raise ValueError("array offset out of range")
    length = int.from_bytes(data[offset : offset + 32], "big")
    end = offset + 32 + 32 * length
    if end > len(data):
        raise ValueError("array payload truncated")
    return [int.from_bytes(data[offset + 32 + i * 32 : offset + 64 + i * 32], "big") for i in range(length)]


def decode_transfers(row: dict[str, Any]) -> list[dict[str, Any]]:
    topics = row["topics"]
    data_hex = row["data"][2:] if row["data"].startswith("0x") else row["data"]
    data = bytes.fromhex(data_hex) if data_hex else b""
    base = {
        "contract_address": row["contract_address"],
        "standard": row["standard"],
        "event_name": row["event_name"],
        "block_number": row["block_number"],
        "block_hash": row["block_hash"],
        "transaction_hash": row["transaction_hash"],
        "transaction_index": row["transaction_index"],
        "log_index": row["log_index"],
        "source": row["source"],
    }
    if row["event_name"] == "Transfer" and len(topics) >= 4:
        return [{**base, "operator": None, "from_address": addr_topic(topics[1]), "to_address": addr_topic(topics[2]), "token_id": str(int(topics[3], 16)), "amount": "1", "batch_item_index": 0}]
    if row["event_name"] == "TransferSingle" and len(topics) >= 4 and len(data) >= 64:
        return [{**base, "operator": addr_topic(topics[1]), "from_address": addr_topic(topics[2]), "to_address": addr_topic(topics[3]), "token_id": str(int.from_bytes(data[0:32], "big")), "amount": str(int.from_bytes(data[32:64], "big")), "batch_item_index": 0}]
    if row["event_name"] == "TransferBatch" and len(topics) >= 4 and len(data) >= 64:
        ids = decode_uint_array(data, int.from_bytes(data[0:32], "big"))
        values = decode_uint_array(data, int.from_bytes(data[32:64], "big"))
        if len(ids) != len(values):
            raise ValueError("TransferBatch ids/values length mismatch")
        return [{**base, "operator": addr_topic(topics[1]), "from_address": addr_topic(topics[2]), "to_address": addr_topic(topics[3]), "token_id": str(token_id), "amount": str(value), "batch_item_index": index} for index, (token_id, value) in enumerate(zip(ids, values))]
    return []


def rpc_batch(calls: list[tuple[str, list[Any]]]) -> dict[int, Any]:
    payload = [{"jsonrpc": "2.0", "id": index, "method": method, "params": params} for index, (method, params) in enumerate(calls)]
    data = request_json(RPC_URL, payload=payload)
    if not isinstance(data, list):
        data = [data]
    result: dict[int, Any] = {}
    for row in data:
        if isinstance(row, dict) and "id" in row:
            result[int(row["id"])] = row.get("result") if "error" not in row else {"__error__": row["error"]}
    return result


def enrich_mint_transactions(transfer_rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    mint_items = [row for row in transfer_rows if row["from_address"] == ZERO]
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in mint_items:
        grouped[(row["transaction_hash"], row["contract_address"])].append(row)
    tx_hashes = sorted({key[0] for key in grouped})
    txs: dict[str, Any] = {}
    receipts: dict[str, Any] = {}
    blocks: dict[int, Any] = {}
    errors: list[dict[str, Any]] = []
    batch_size = 35
    for start in range(0, len(tx_hashes), batch_size):
        chunk = tx_hashes[start : start + batch_size]
        calls: list[tuple[str, list[Any]]] = []
        keys: list[tuple[str, str]] = []
        for tx_hash in chunk:
            calls.append(("eth_getTransactionByHash", [tx_hash])); keys.append((tx_hash, "tx"))
            calls.append(("eth_getTransactionReceipt", [tx_hash])); keys.append((tx_hash, "receipt"))
        try:
            response = rpc_batch(calls)
        except Exception as exc:  # noqa: BLE001
            errors.append({"stage": "tx_receipt_batch", "start": start, "error": repr(exc)})
            continue
        for index, (tx_hash, kind) in enumerate(keys):
            value = response.get(index)
            if isinstance(value, dict) and value.get("__error__"):
                errors.append({"stage": kind, "transaction_hash": tx_hash, "error": value["__error__"]})
            elif kind == "tx":
                txs[tx_hash] = value
            else:
                receipts[tx_hash] = value
    block_numbers = sorted({intish((receipt or {}).get("blockNumber")) for receipt in receipts.values() if receipt})
    for start in range(0, len(block_numbers), 70):
        chunk = block_numbers[start : start + 70]
        try:
            response = rpc_batch([("eth_getBlockByNumber", [hex(number), False]) for number in chunk])
            for index, number in enumerate(chunk):
                blocks[number] = response.get(index)
        except Exception as exc:  # noqa: BLE001
            errors.append({"stage": "block_batch", "start": start, "error": repr(exc)})

    evidence: list[dict[str, Any]] = []
    for (tx_hash, contract), items in grouped.items():
        tx = txs.get(tx_hash) or {}
        receipt = receipts.get(tx_hash) or {}
        sender = str(tx.get("from") or "").lower()
        native_value = intish(tx.get("value"))
        gas_used = intish(receipt.get("gasUsed"))
        gas_price = intish(receipt.get("effectiveGasPrice") or tx.get("gasPrice"))
        recipients = sorted({row["to_address"] for row in items})
        amount = sum(int(row["amount"]) for row in items)
        erc20_outflows: dict[str, int] = defaultdict(int)
        for log in receipt.get("logs") or []:
            topics = [str(value).lower() for value in (log.get("topics") or [])]
            if len(topics) == 3 and topics[0] == TRANSFER and addr_topic(topics[1]) == sender:
                erc20_outflows[str(log.get("address") or "").lower()] += intish(log.get("data"))
        if native_value > 0 and recipients == [sender]:
            payment_class = "NATIVE_SELF_FUNDED"
        elif native_value > 0:
            payment_class = "NATIVE_THIRD_PARTY_OR_ROUTED"
        elif erc20_outflows and recipients == [sender]:
            payment_class = "ERC20_SELF_FUNDED"
        elif erc20_outflows:
            payment_class = "ERC20_THIRD_PARTY_OR_ROUTED"
        elif recipients == [sender]:
            payment_class = "ZERO_VALUE_SELF_MINT"
        else:
            payment_class = "ZERO_VALUE_THIRD_PARTY_OR_AIRDROP"
        block_number = intish(receipt.get("blockNumber") or items[0]["block_number"])
        block = blocks.get(block_number) or {}
        evidence.append(
            {
                "transaction_hash": tx_hash,
                "contract_address": contract,
                "block_number": block_number,
                "block_hash": str(receipt.get("blockHash") or items[0]["block_hash"] or "").lower(),
                "timestamp_unix": intish(block.get("timestamp")),
                "tx_from": sender,
                "tx_to": str(tx.get("to") or "").lower(),
                "method_selector": str(tx.get("input") or "0x")[:10].lower(),
                "native_value_wei": str(native_value),
                "gas_used": str(gas_used),
                "effective_gas_price_wei": str(gas_price),
                "entry_gas_cost_wei": str(gas_used * gas_price),
                "receipt_status": str(receipt.get("status") or ""),
                "mint_recipients": recipients,
                "mint_item_rows": len(items),
                "mint_quantity": str(amount),
                "token_ids": sorted({row["token_id"] for row in items}, key=lambda value: int(value)),
                "erc20_outflows": dict(sorted(erc20_outflows.items())),
                "payment_class": payment_class,
            }
        )
    return sorted(evidence, key=lambda row: (row["block_number"], row["transaction_hash"])), errors


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--contracts", type=Path, required=True)
    parser.add_argument("--shard", type=int, required=True)
    parser.add_argument("--shards", type=int, default=32)
    parser.add_argument("--head", type=int, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)

    contracts = [json.loads(line) for line in args.contracts.read_text(encoding="utf-8").splitlines() if line.strip()]
    selected = [row for index, row in enumerate(contracts) if index % args.shards == args.shard]
    if not selected:
        raise SystemExit(f"empty contract shard {args.shard}")

    raw_logs: dict[tuple[str, str, int], dict[str, Any]] = {}
    unresolved: list[dict[str, Any]] = []
    processed: list[str] = []
    for index, contract in enumerate(selected, 1):
        address = contract["contract_address"].lower()
        token_types = set(contract.get("token_types") or [])
        event_specs = []
        if "ERC-721" in token_types:
            event_specs.append(("ERC-721", "Transfer", TRANSFER))
        if "ERC-1155" in token_types:
            event_specs.extend((("ERC-1155", "TransferSingle", TRANSFER_SINGLE), ("ERC-1155", "TransferBatch", TRANSFER_BATCH)))
        for standard, event_name, topic0 in event_specs:
            before = len(unresolved)
            collect_range(address, standard, event_name, topic0, 0, args.head, raw_logs, unresolved)
            if len(unresolved) > before:
                continue
        processed.append(address)
        print(json.dumps({"shard": args.shard, "contract": address, "index": index, "total": len(selected), "logs": len(raw_logs), "unresolved": len(unresolved)}), flush=True)

    decoded: list[dict[str, Any]] = []
    decode_errors: list[dict[str, Any]] = []
    for row in sorted(raw_logs.values(), key=lambda value: (value["block_number"], value["transaction_index"], value["log_index"])):
        try:
            decoded.extend(decode_transfers(row))
        except Exception as exc:  # noqa: BLE001
            decode_errors.append({"transaction_hash": row["transaction_hash"], "log_index": row["log_index"], "error": repr(exc)})

    transfer_path = args.out / "transfers.jsonl.gz"
    with gzip.open(transfer_path, "wt", encoding="utf-8", newline="\n") as handle:
        for row in decoded:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
    mint_evidence, enrich_errors = enrich_mint_transactions(decoded)
    with (args.out / "mint_transactions.jsonl").open("w", encoding="utf-8", newline="\n") as handle:
        for row in mint_evidence:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
    (args.out / "selected_contracts.json").write_text(json.dumps(selected, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    (args.out / "unresolved.json").write_text(json.dumps(unresolved, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    (args.out / "decode_errors.json").write_text(json.dumps(decode_errors, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    (args.out / "enrichment_errors.json").write_text(json.dumps(enrich_errors, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")

    validation = {
        "status": "PASS" if not unresolved and not decode_errors and not enrich_errors and len(processed) == len(selected) else "FAIL",
        "shard": args.shard,
        "shards": args.shards,
        "head": args.head,
        "contracts_selected": len(selected),
        "contracts_processed": len(processed),
        "raw_event_logs": len(raw_logs),
        "decoded_transfer_rows": len(decoded),
        "mint_transaction_rows": len(mint_evidence),
        "unresolved_ranges": len(unresolved),
        "decode_errors": len(decode_errors),
        "enrichment_errors": len(enrich_errors),
        "production_approved_wallets": 0,
    }
    (args.out / "VALIDATION.json").write_text(json.dumps(validation, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    manifest = []
    for path in sorted(args.out.iterdir()):
        if path.is_file():
            manifest.append({"path": path.name, "bytes": path.stat().st_size, "sha256": sha256(path)})
    (args.out / "MANIFEST.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(validation, sort_keys=True), flush=True)
    if validation["status"] != "PASS":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
