#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import random
import time
import urllib.error
import urllib.request
from collections import defaultdict, deque
from pathlib import Path
from typing import Any

RPC_URL = "https://rpc.mainnet.chain.robinhood.com"
CHAIN_ID = 4663
USER_AGENT = "RHC-Canonical-Completion/1.0"

TARGETS = {
    "seadrop": {
        "address": "0x00005ea00ac477b1030ce78506496e8c2de24bf5",
        "topic0": "0xe90cf9cc0a552cf52ea6ff74ece0f1c8ae8cc9ad630d3181f55ac43ca076b7d6",
    },
    "seaport": {
        "address": "0x0000000000000068f116a894984e2db1123eb395",
        "topic0": "0x9d9af8e38d66c62e2c12f0225249fd9d721c54b83f48d9352c97c6cacdcb6f31",
    },
}


def hex_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        return int(value, 16) if value.startswith("0x") else int(value)
    return int(value)


def address_word(value: str) -> str:
    value = value.removeprefix("0x")
    return "0x" + value[-40:].lower()


def words(data: str) -> list[str]:
    raw = data.removeprefix("0x")
    if len(raw) % 64:
        raise ValueError(f"ABI data length is not word-aligned: {len(raw)}")
    return [raw[i : i + 64] for i in range(0, len(raw), 64)]


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fields = sorted({key for row in rows for key in row}) if rows else ["empty"]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    key: json.dumps(value, ensure_ascii=False, sort_keys=True)
                    if isinstance(value, (dict, list, tuple))
                    else value
                    for key, value in row.items()
                }
            )


class RpcClient:
    def __init__(self, url: str = RPC_URL, min_interval: float = 0.72):
        self.url = url
        self.min_interval = min_interval
        self.last_request = 0.0
        self.request_count = 0
        self.retry_count = 0

    def pace(self) -> None:
        wait = self.min_interval - (time.monotonic() - self.last_request)
        if wait > 0:
            time.sleep(wait)

    def request(self, payload: Any, attempts: int = 10) -> Any:
        last_error: Exception | None = None
        for attempt in range(attempts):
            self.pace()
            request = urllib.request.Request(
                self.url,
                data=json.dumps(payload).encode("utf-8"),
                method="POST",
                headers={
                    "content-type": "application/json",
                    "accept": "application/json",
                    "user-agent": USER_AGENT,
                },
            )
            try:
                with urllib.request.urlopen(request, timeout=120) as response:
                    body = response.read()
                self.last_request = time.monotonic()
                self.request_count += 1
                result = json.loads(body.decode("utf-8"))
                return result
            except urllib.error.HTTPError as exc:
                self.last_request = time.monotonic()
                body = exc.read(3000).decode("utf-8", "replace")
                last_error = RuntimeError(f"HTTP {exc.code}: {body}")
                if exc.code == 429 or exc.code >= 500:
                    self.retry_count += 1
                    time.sleep(min(90, 3 * (2 ** min(attempt, 5)) + random.random() * 4))
                    continue
                raise last_error
            except Exception as exc:
                self.last_request = time.monotonic()
                last_error = exc
                self.retry_count += 1
                if attempt + 1 < attempts:
                    time.sleep(min(60, 2 ** min(attempt, 5) + random.random() * 3))
                    continue
                break
        raise RuntimeError(f"RPC request failed after {attempts} attempts: {last_error}")

    def call(self, method: str, params: list[Any]) -> Any:
        payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
        response = self.request(payload)
        if not isinstance(response, dict):
            raise RuntimeError(f"Unexpected RPC response type: {type(response)}")
        if response.get("error") is not None:
            raise RuntimeError(json.dumps(response["error"], sort_keys=True))
        return response.get("result")

    def batch(self, calls: list[tuple[str, list[Any]]]) -> list[Any]:
        payload = [
            {"jsonrpc": "2.0", "id": index, "method": method, "params": params}
            for index, (method, params) in enumerate(calls)
        ]
        response = self.request(payload)
        if not isinstance(response, list):
            # Some gateways reject batch requests. Fall back to individual calls.
            return [self.call(method, params) for method, params in calls]
        indexed = {int(item.get("id")): item for item in response if isinstance(item, dict)}
        output: list[Any] = []
        for index, (method, params) in enumerate(calls):
            item = indexed.get(index)
            if item is None or item.get("error") is not None:
                output.append(self.call(method, params))
            else:
                output.append(item.get("result"))
        return output


def get_logs(client: RpcClient, target: dict[str, str], start: int, end: int) -> list[dict[str, Any]]:
    result = client.call(
        "eth_getLogs",
        [
            {
                "fromBlock": hex(start),
                "toBlock": hex(end),
                "address": target["address"],
                "topics": [target["topic0"]],
            }
        ],
    )
    if not isinstance(result, list):
        raise RuntimeError(f"eth_getLogs returned non-list: {type(result)}")
    return [row for row in result if isinstance(row, dict)]


def collect_ranges(
    client: RpcClient,
    target: dict[str, str],
    head: int,
    nominal_chunk: int = 250_000,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    queue: deque[tuple[int, int, int]] = deque()
    for start in range(0, head + 1, nominal_chunk):
        queue.append((start, min(head, start + nominal_chunk - 1), 0))
    rows: list[dict[str, Any]] = []
    completed: list[dict[str, Any]] = []
    unresolved: list[dict[str, Any]] = []
    while queue:
        start, end, depth = queue.popleft()
        try:
            batch = get_logs(client, target, start, end)
            rows.extend(batch)
            completed.append({"from_block": start, "to_block": end, "rows": len(batch), "depth": depth})
            if len(completed) % 20 == 0:
                print({"completed_ranges": len(completed), "queued_ranges": len(queue), "events": len(rows)}, flush=True)
        except Exception as exc:
            if start < end and depth < 28:
                midpoint = (start + end) // 2
                queue.appendleft((midpoint + 1, end, depth + 1))
                queue.appendleft((start, midpoint, depth + 1))
            else:
                unresolved.append({"from_block": start, "to_block": end, "depth": depth, "error": repr(exc)})
    return rows, completed, unresolved


def event_key(row: dict[str, Any]) -> tuple[str, str, str]:
    return (
        str(row.get("blockHash") or "").lower(),
        str(row.get("transactionHash") or "").lower(),
        str(row.get("logIndex") or "").lower(),
    )


def decode_seadrop(row: dict[str, Any]) -> dict[str, Any]:
    topics = row.get("topics") or []
    data_words = words(str(row.get("data") or "0x"))
    if len(topics) != 4 or len(data_words) != 5:
        raise ValueError(f"Unexpected SeaDropMint layout topics={len(topics)} words={len(data_words)}")
    return {
        "chain_id": CHAIN_ID,
        "block_number": hex_int(row.get("blockNumber")),
        "block_hash": str(row.get("blockHash") or "").lower(),
        "transaction_hash": str(row.get("transactionHash") or "").lower(),
        "transaction_index": hex_int(row.get("transactionIndex")),
        "log_index": hex_int(row.get("logIndex")),
        "removed": bool(row.get("removed")),
        "seadrop_contract": str(row.get("address") or "").lower(),
        "nft_contract": address_word(topics[1]),
        "minter": address_word(topics[2]),
        "fee_recipient": address_word(topics[3]),
        "payer": address_word(data_words[0]),
        "quantity_minted": int(data_words[1], 16),
        "unit_mint_price_wei": int(data_words[2], 16),
        "fee_bps": int(data_words[3], 16),
        "drop_stage_index": int(data_words[4], 16),
    }


def decode_spent_items(data_words: list[str], offset_bytes: int) -> list[dict[str, Any]]:
    start = offset_bytes // 32
    length = int(data_words[start], 16)
    output = []
    cursor = start + 1
    for _ in range(length):
        output.append(
            {
                "item_type": int(data_words[cursor], 16),
                "token": address_word(data_words[cursor + 1]),
                "identifier": str(int(data_words[cursor + 2], 16)),
                "amount": str(int(data_words[cursor + 3], 16)),
            }
        )
        cursor += 4
    return output


def decode_received_items(data_words: list[str], offset_bytes: int) -> list[dict[str, Any]]:
    start = offset_bytes // 32
    length = int(data_words[start], 16)
    output = []
    cursor = start + 1
    for _ in range(length):
        output.append(
            {
                "item_type": int(data_words[cursor], 16),
                "token": address_word(data_words[cursor + 1]),
                "identifier": str(int(data_words[cursor + 2], 16)),
                "amount": str(int(data_words[cursor + 3], 16)),
                "recipient": address_word(data_words[cursor + 4]),
            }
        )
        cursor += 5
    return output


def decode_seaport(row: dict[str, Any]) -> dict[str, Any]:
    topics = row.get("topics") or []
    data_words = words(str(row.get("data") or "0x"))
    if len(topics) != 3 or len(data_words) < 4:
        raise ValueError(f"Unexpected OrderFulfilled layout topics={len(topics)} words={len(data_words)}")
    order_hash = "0x" + data_words[0]
    recipient = address_word(data_words[1])
    offer_offset = int(data_words[2], 16)
    consideration_offset = int(data_words[3], 16)
    offer = decode_spent_items(data_words, offer_offset)
    consideration = decode_received_items(data_words, consideration_offset)
    offerer = address_word(topics[1])
    zone = address_word(topics[2])
    offer_has_nft = any(item["item_type"] in (2, 3) for item in offer)
    consideration_has_nft = any(item["item_type"] in (2, 3) for item in consideration)
    if offer_has_nft and not consideration_has_nft:
        direction = "LISTING_OR_NFT_OFFER"
        seller, buyer = offerer, recipient
    elif consideration_has_nft and not offer_has_nft:
        direction = "BID_OR_CURRENCY_OFFER"
        seller, buyer = recipient, offerer
    else:
        direction = "MIXED_OR_BUNDLE"
        seller = buyer = None
    payment_totals: dict[str, int] = defaultdict(int)
    payment_source = consideration if offer_has_nft else offer
    for item in payment_source:
        if item["item_type"] in (0, 1):
            token = "NATIVE" if item["item_type"] == 0 else item["token"]
            payment_totals[token] += int(item["amount"])
    nft_items = [item for item in offer + consideration if item["item_type"] in (2, 3)]
    return {
        "chain_id": CHAIN_ID,
        "block_number": hex_int(row.get("blockNumber")),
        "block_hash": str(row.get("blockHash") or "").lower(),
        "transaction_hash": str(row.get("transactionHash") or "").lower(),
        "transaction_index": hex_int(row.get("transactionIndex")),
        "log_index": hex_int(row.get("logIndex")),
        "removed": bool(row.get("removed")),
        "seaport_contract": str(row.get("address") or "").lower(),
        "order_hash": order_hash.lower(),
        "offerer": offerer,
        "zone": zone,
        "recipient": recipient,
        "order_direction": direction,
        "seller": seller,
        "buyer": buyer,
        "offer": offer,
        "consideration": consideration,
        "nft_items": nft_items,
        "payment_totals_raw": dict(payment_totals),
    }


def batch_fetch(client: RpcClient, method: str, values: list[Any], batch_size: int = 20) -> dict[Any, Any]:
    output: dict[Any, Any] = {}
    for start in range(0, len(values), batch_size):
        chunk = values[start : start + batch_size]
        if method == "eth_getBlockByNumber":
            calls = [(method, [hex(int(value)), False]) for value in chunk]
        else:
            calls = [(method, [value]) for value in chunk]
        results = client.batch(calls)
        output.update(zip(chunk, results))
        if start % (batch_size * 10) == 0:
            print({"method": method, "fetched": min(start + len(chunk), len(values)), "total": len(values)}, flush=True)
    return output


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", choices=sorted(TARGETS), required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    client = RpcClient()
    chain_id = hex_int(client.call("eth_chainId", []))
    if chain_id != CHAIN_ID:
        raise RuntimeError(f"Wrong chain id: {chain_id}")
    head = hex_int(client.call("eth_blockNumber", []))
    assert head is not None
    raw_rows, ranges, unresolved = collect_ranges(client, TARGETS[args.target], head)
    unique: dict[tuple[str, str, str], dict[str, Any]] = {}
    duplicates = 0
    for row in raw_rows:
        key = event_key(row)
        if key in unique:
            duplicates += 1
        unique[key] = row
    logs = sorted(
        unique.values(),
        key=lambda row: (hex_int(row.get("blockNumber")) or -1, hex_int(row.get("logIndex")) or -1),
    )
    decoder = decode_seadrop if args.target == "seadrop" else decode_seaport
    decoded: list[dict[str, Any]] = []
    decode_errors: list[dict[str, Any]] = []
    for row in logs:
        try:
            decoded.append(decoder(row))
        except Exception as exc:
            decode_errors.append({"event_key": event_key(row), "error": repr(exc), "raw": row})
    blocks = sorted({hex_int(row.get("blockNumber")) for row in logs if hex_int(row.get("blockNumber")) is not None})
    tx_hashes = sorted({str(row.get("transactionHash") or "").lower() for row in logs if row.get("transactionHash")})
    block_data = batch_fetch(client, "eth_getBlockByNumber", blocks, batch_size=30)
    tx_data = batch_fetch(client, "eth_getTransactionByHash", tx_hashes, batch_size=20)
    receipt_data = batch_fetch(client, "eth_getTransactionReceipt", tx_hashes, batch_size=15)
    timestamps = {
        block: hex_int(value.get("timestamp")) if isinstance(value, dict) else None
        for block, value in block_data.items()
    }
    for row in decoded:
        timestamp = timestamps.get(row["block_number"])
        row["block_timestamp_unix"] = timestamp
        row["block_timestamp_utc"] = (
            time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(timestamp)) if timestamp is not None else None
        )
    write_jsonl(out / "raw_logs.jsonl", logs)
    write_jsonl(out / "decoded_events.jsonl", decoded)
    write_csv(out / "decoded_events.csv", decoded)
    write_jsonl(out / "transactions.jsonl", [{"transaction_hash": key, "value": value} for key, value in tx_data.items()])
    write_jsonl(out / "receipts.jsonl", [{"transaction_hash": key, "value": value} for key, value in receipt_data.items()])
    write_jsonl(out / "blocks.jsonl", [{"block_number": key, "value": value} for key, value in block_data.items()])
    write_json(out / "completed_ranges.json", ranges)
    write_json(out / "unresolved_ranges.json", unresolved)
    write_json(out / "decode_errors.json", decode_errors)
    failures: list[dict[str, Any]] = []
    if unresolved:
        failures.append({"code": "UNRESOLVED_BLOCK_RANGES", "count": len(unresolved)})
    if decode_errors:
        failures.append({"code": "EVENT_DECODE_ERRORS", "count": len(decode_errors)})
    if len(decoded) != len(logs):
        failures.append({"code": "DECODED_COUNT_MISMATCH", "raw": len(logs), "decoded": len(decoded)})
    if any(row.get("removed") for row in decoded):
        failures.append({"code": "REMOVED_LOG_PRESENT"})
    missing_txs = [key for key, value in tx_data.items() if not isinstance(value, dict)]
    missing_receipts = [key for key, value in receipt_data.items() if not isinstance(value, dict)]
    missing_blocks = [key for key, value in block_data.items() if not isinstance(value, dict)]
    if missing_txs:
        failures.append({"code": "MISSING_TRANSACTIONS", "count": len(missing_txs)})
    if missing_receipts:
        failures.append({"code": "MISSING_RECEIPTS", "count": len(missing_receipts)})
    if missing_blocks:
        failures.append({"code": "MISSING_BLOCKS", "count": len(missing_blocks)})
    validation = {
        "status": "PASS" if not failures else "FAIL",
        "target": args.target,
        "chain_id": chain_id,
        "fixed_head_block": head,
        "completed_ranges": len(ranges),
        "unresolved_ranges": len(unresolved),
        "raw_rows_before_dedup": len(raw_rows),
        "duplicate_rows_removed": duplicates,
        "canonical_event_rows": len(logs),
        "decoded_event_rows": len(decoded),
        "unique_transactions": len(tx_hashes),
        "unique_blocks": len(blocks),
        "rpc_request_count": client.request_count,
        "rpc_retry_count": client.retry_count,
        "failures": failures,
    }
    write_json(out / "VALIDATION.json", validation)
    manifest = []
    for path in sorted(out.iterdir()):
        if path.is_file() and path.name != "MANIFEST.json":
            manifest.append(
                {
                    "path": path.name,
                    "bytes": path.stat().st_size,
                    "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                }
            )
    write_json(out / "MANIFEST.json", manifest)
    print(json.dumps(validation, sort_keys=True), flush=True)
    if failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
