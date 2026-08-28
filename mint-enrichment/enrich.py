#!/usr/bin/env python3
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import random
import time
import urllib.error
import urllib.request
from collections import defaultdict
from pathlib import Path
from typing import Any

RPC = "https://rpc.mainnet.chain.robinhood.com"
BLOCKSCOUTS = [
    "https://robinhoodchain.blockscout.com/api/v2",
    "https://explorer.hoodmarketcap.com/api/v2",
]
UA = "RHC-Mint-Tx-Enrichment/1.0 read-only"
TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"


def request_json(url: str, *, data: bytes | None = None, attempts: int = 12) -> Any:
    last: Exception | None = None
    for attempt in range(attempts):
        try:
            headers = {"accept": "application/json", "user-agent": UA}
            if data is not None:
                headers["content-type"] = "application/json"
            req = urllib.request.Request(url, data=data, headers=headers)
            with urllib.request.urlopen(req, timeout=90) as response:
                body = response.read()
            return json.loads(body.decode("utf-8"))
        except urllib.error.HTTPError as exc:
            last = exc
            if exc.code in {408, 425, 429, 500, 502, 503, 504}:
                time.sleep(min(120, 2 ** min(attempt, 7) + random.random() * 7))
                continue
            raise
        except Exception as exc:
            last = exc
            if attempt + 1 < attempts:
                time.sleep(min(60, 2 ** min(attempt, 6) + random.random() * 5))
                continue
    raise RuntimeError(f"request failed: {url}: {last!r}")


def rpc_batch(calls: list[tuple[str, list[Any]]]) -> dict[int, Any]:
    payload = [
        {"jsonrpc": "2.0", "id": index, "method": method, "params": params}
        for index, (method, params) in enumerate(calls)
    ]
    result = request_json(RPC, data=json.dumps(payload).encode())
    if isinstance(result, dict):
        result = [result]
    if not isinstance(result, list):
        raise RuntimeError(f"invalid batch response: {type(result)!r}")
    output: dict[int, Any] = {}
    for row in result:
        if not isinstance(row, dict):
            continue
        index = int(row.get("id", -1))
        output[index] = {"__error__": row["error"]} if row.get("error") else row.get("result")
    return output


def intish(value: Any, default: int = 0) -> int:
    if value is None:
        return default
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value, 16) if value.startswith("0x") else int(value)
        except ValueError:
            return default
    return default


def address_from_topic(value: str) -> str:
    return "0x" + value.lower()[-40:]


def fetch_internal(tx_hash: str) -> tuple[list[dict[str, Any]], str | None, list[str]]:
    errors: list[str] = []
    for base in BLOCKSCOUTS:
        try:
            payload = request_json(f"{base}/transactions/{tx_hash}/internal-transactions")
            items = payload.get("items") if isinstance(payload, dict) else payload
            if not isinstance(items, list):
                raise RuntimeError(f"invalid internal tx payload: {payload!r}")
            return [row for row in items if isinstance(row, dict)], base, errors
        except Exception as exc:
            errors.append(f"{base}:{exc!r}")
    return [], None, errors


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--index", required=True)
    parser.add_argument("--shard", type=int, required=True)
    parser.add_argument("--shard-count", type=int, default=32)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    by_tx: dict[str, list[dict[str, Any]]] = defaultdict(list)
    with gzip.open(args.index, "rt", encoding="utf-8") as file:
        for line in file:
            if not line.strip():
                continue
            row = json.loads(line)
            tx_hash = str(row.get("transaction_hash") or "").lower()
            if tx_hash.startswith("0x") and len(tx_hash) == 66:
                by_tx[tx_hash].append(row)
    selected_hashes = sorted(
        tx_hash for tx_hash in by_tx
        if int(hashlib.sha256(tx_hash.encode()).hexdigest(), 16) % args.shard_count == args.shard
    )

    transactions: dict[str, Any] = {}
    receipts: dict[str, Any] = {}
    errors: list[dict[str, Any]] = []
    batch_size = 20
    for start in range(0, len(selected_hashes), batch_size):
        chunk = selected_hashes[start:start + batch_size]
        calls: list[tuple[str, list[Any]]] = []
        lookup: list[tuple[str, str]] = []
        for tx_hash in chunk:
            calls.append(("eth_getTransactionByHash", [tx_hash])); lookup.append((tx_hash, "tx"))
            calls.append(("eth_getTransactionReceipt", [tx_hash])); lookup.append((tx_hash, "receipt"))
        try:
            result = rpc_batch(calls)
        except Exception as exc:
            errors.append({"stage": "rpc_batch", "hashes": chunk, "error": repr(exc)})
            continue
        for index, (tx_hash, kind) in enumerate(lookup):
            value = result.get(index)
            if value is None or isinstance(value, dict) and value.get("__error__"):
                errors.append({"stage": kind, "transaction_hash": tx_hash, "error": value})
                continue
            (transactions if kind == "tx" else receipts)[tx_hash] = value
        print({"shard": args.shard, "processed": min(start + len(chunk), len(selected_hashes)), "total": len(selected_hashes)}, flush=True)
        time.sleep(0.35)

    block_numbers = sorted({intish(row.get("blockNumber")) for row in receipts.values() if isinstance(row, dict)})
    blocks: dict[int, Any] = {}
    for start in range(0, len(block_numbers), 40):
        chunk = block_numbers[start:start + 40]
        try:
            result = rpc_batch([("eth_getBlockByNumber", [hex(number), False]) for number in chunk])
        except Exception as exc:
            errors.append({"stage": "block_batch", "blocks": chunk, "error": repr(exc)})
            continue
        for index, number in enumerate(chunk):
            value = result.get(index)
            if not isinstance(value, dict):
                errors.append({"stage": "block", "block_number": number, "error": value})
            else:
                blocks[number] = value

    transaction_rows: list[dict[str, Any]] = []
    payment_rows: list[dict[str, Any]] = []
    for position, tx_hash in enumerate(selected_hashes, 1):
        tx = transactions.get(tx_hash)
        receipt = receipts.get(tx_hash)
        if not isinstance(tx, dict) or not isinstance(receipt, dict):
            continue
        block_number = intish(receipt.get("blockNumber"))
        block = blocks.get(block_number) or {}
        sender = str(tx.get("from") or "").lower()
        native_value = intish(tx.get("value"))
        gas_used = intish(receipt.get("gasUsed"))
        effective_gas_price = intish(receipt.get("effectiveGasPrice"), intish(tx.get("gasPrice")))
        internal_items, internal_source, internal_errors = fetch_internal(tx_hash)
        if internal_source is None:
            errors.append({"stage": "internal_transactions", "transaction_hash": tx_hash, "error": internal_errors})
        erc20_outflows: dict[str, int] = defaultdict(int)
        erc20_transfers: list[dict[str, Any]] = []
        for log in receipt.get("logs") or []:
            topics = [str(value).lower() for value in (log.get("topics") or [])]
            if len(topics) == 3 and topics[0] == TRANSFER_TOPIC:
                from_address = address_from_topic(topics[1])
                to_address = address_from_topic(topics[2])
                amount = intish(log.get("data"))
                token = str(log.get("address") or "").lower()
                transfer = {"token": token, "from_address": from_address, "to_address": to_address, "amount_raw": str(amount), "log_index": intish(log.get("logIndex"))}
                erc20_transfers.append(transfer)
                if from_address == sender:
                    erc20_outflows[token] += amount
        native_internal = []
        for item in internal_items:
            value = intish(item.get("value") if not isinstance(item.get("value"), dict) else item["value"].get("value"))
            if value > 0:
                native_internal.append({
                    "from_address": str((item.get("from") or {}).get("hash") if isinstance(item.get("from"), dict) else item.get("from") or "").lower(),
                    "to_address": str((item.get("to") or {}).get("hash") if isinstance(item.get("to"), dict) else item.get("to") or "").lower(),
                    "value_wei": str(value),
                    "type": item.get("type") or item.get("call_type"),
                })
        mint_transfers = by_tx[tx_hash]
        row = {
            "transaction_hash": tx_hash,
            "block_number": block_number,
            "block_hash": str(receipt.get("blockHash") or "").lower(),
            "timestamp_unix": intish(block.get("timestamp")),
            "tx_from": sender,
            "tx_to": str(tx.get("to") or "").lower(),
            "tx_value_wei": str(native_value),
            "input": tx.get("input"),
            "method_selector": str(tx.get("input") or "0x")[:10].lower(),
            "receipt_status": intish(receipt.get("status")),
            "gas_used": gas_used,
            "effective_gas_price_wei": effective_gas_price,
            "gas_cost_wei": str(gas_used * effective_gas_price),
            "mint_transfer_count": len(mint_transfers),
            "mint_contracts": sorted({row.get("contract_address") for row in mint_transfers if row.get("contract_address")}),
            "mint_recipients": sorted({row.get("to_address") for row in mint_transfers if row.get("to_address")}),
            "erc20_transfers": erc20_transfers,
            "erc20_outflows_from_sender": {token: str(value) for token, value in erc20_outflows.items()},
            "internal_value_transfers": native_internal,
            "internal_source": internal_source,
            "internal_source_errors": internal_errors,
        }
        transaction_rows.append(row)
        payment_rows.append({
            "transaction_hash": tx_hash,
            "tx_from": sender,
            "native_tx_value_wei": str(native_value),
            "sender_erc20_outflows": {token: str(value) for token, value in erc20_outflows.items()},
            "native_internal_value_transfers": native_internal,
            "payment_route_status": "NATIVE_VALUE" if native_value > 0 else "ERC20_OUTFLOW" if erc20_outflows else "INTERNAL_VALUE" if native_internal else "ZERO_OR_UNRESOLVED",
        })
        if position % 25 == 0:
            print({"shard": args.shard, "internal_processed": position, "total": len(selected_hashes)}, flush=True)
        time.sleep(0.25)

    with gzip.open(out / "mint_transactions.jsonl.gz", "wt", encoding="utf-8") as file:
        for row in transaction_rows:
            file.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
    with gzip.open(out / "mint_payment_flows.jsonl.gz", "wt", encoding="utf-8") as file:
        for row in payment_rows:
            file.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
    (out / "errors.json").write_text(json.dumps(errors, ensure_ascii=False, indent=2), encoding="utf-8")

    enriched_hashes = {row["transaction_hash"] for row in transaction_rows}
    failures: list[dict[str, Any]] = []
    missing = sorted(set(selected_hashes) - enriched_hashes)
    if missing:
        failures.append({"code": "MINT_TRANSACTION_ENRICHMENT_MISSING", "count": len(missing), "sample": missing[:50]})
    if errors:
        failures.append({"code": "ENRICHMENT_ERRORS_PRESENT", "count": len(errors)})
    if any(row["receipt_status"] != 1 for row in transaction_rows):
        failures.append({"code": "NON_SUCCESS_RECEIPT_IN_MINT_INDEX"})
    report = {
        "status": "PASS" if not failures else "FAIL",
        "shard": args.shard,
        "shard_count": args.shard_count,
        "index_unique_transactions": len(by_tx),
        "selected_transactions": len(selected_hashes),
        "enriched_transactions": len(transaction_rows),
        "payment_rows": len(payment_rows),
        "failures": failures,
        "production_approved_wallets": 0,
        "decision_use": "PRIMARY_MINT_COST_AND_ROUTE_INPUT",
    }
    (out / "VALIDATION.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    manifest = []
    for path in sorted(out.iterdir()):
        if path.is_file():
            manifest.append({"path": path.name, "bytes": path.stat().st_size, "sha256": hashlib.sha256(path.read_bytes()).hexdigest()})
    (out / "MANIFEST.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, sort_keys=True), flush=True)
    if failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
