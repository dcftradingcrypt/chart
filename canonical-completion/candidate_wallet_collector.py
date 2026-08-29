#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import csv
import gzip
import hashlib
import json
import random
import re
import time
import urllib.error
import urllib.request
from collections import deque
from pathlib import Path
from typing import Any

RPC_URL = "https://rpc.mainnet.chain.robinhood.com"
CHAIN_ID = 4663
USER_AGENT = "RHC-Candidate-Wallet-Completion/1.0"
ZERO = "0x0000000000000000000000000000000000000000"
ERC721_TRANSFER = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
# ERC-1155 event signatures are computed at runtime with pycryptodome.


def keccak256(text: str) -> str:
    from Crypto.Hash import keccak
    digest = keccak.new(digest_bits=256)
    digest.update(text.encode("utf-8"))
    return "0x" + digest.hexdigest()


ERC1155_SINGLE = keccak256("TransferSingle(address,address,address,uint256,uint256)")
ERC1155_BATCH = keccak256("TransferBatch(address,address,address,uint256[],uint256[])")
ADDR = re.compile(r"^0x[a-f0-9]{40}$")


def h2i(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        return int(value, 16) if value.startswith("0x") else int(value)
    return int(value)


def padded(address: str) -> str:
    return "0x" + "0" * 24 + address.lower().removeprefix("0x")


def topic_address(value: str) -> str:
    return "0x" + value.removeprefix("0x")[-40:].lower()


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
            writer.writerow({
                key: json.dumps(value, ensure_ascii=False, sort_keys=True)
                if isinstance(value, (dict, list, tuple)) else value
                for key, value in row.items()
            })


class RpcClient:
    def __init__(self, min_interval: float = 1.15):
        self.min_interval = min_interval
        self.last_request = 0.0
        self.request_count = 0
        self.retry_count = 0

    def pace(self) -> None:
        wait = self.min_interval - (time.monotonic() - self.last_request)
        if wait > 0:
            time.sleep(wait)

    def request(self, payload: Any, attempts: int = 12) -> Any:
        last_error: Exception | None = None
        for attempt in range(attempts):
            self.pace()
            request = urllib.request.Request(
                RPC_URL,
                data=json.dumps(payload).encode("utf-8"),
                method="POST",
                headers={"content-type": "application/json", "accept": "application/json", "user-agent": USER_AGENT},
            )
            try:
                with urllib.request.urlopen(request, timeout=150) as response:
                    body = response.read()
                self.last_request = time.monotonic()
                self.request_count += 1
                return json.loads(body.decode("utf-8"))
            except urllib.error.HTTPError as exc:
                self.last_request = time.monotonic()
                body = exc.read(3000).decode("utf-8", "replace")
                last_error = RuntimeError(f"HTTP {exc.code}: {body}")
                if exc.code == 429 or exc.code >= 500:
                    self.retry_count += 1
                    time.sleep(min(120, 4 * (2 ** min(attempt, 5)) + random.random() * 7))
                    continue
                raise last_error
            except Exception as exc:
                self.last_request = time.monotonic()
                last_error = exc
                self.retry_count += 1
                if attempt + 1 < attempts:
                    time.sleep(min(90, 3 * (2 ** min(attempt, 5)) + random.random() * 5))
                    continue
                break
        raise RuntimeError(f"RPC failed after {attempts} attempts: {last_error}")

    def call(self, method: str, params: list[Any]) -> Any:
        response = self.request({"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
        if not isinstance(response, dict):
            raise RuntimeError(f"Unexpected response type: {type(response)}")
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
            return [self.call(method, params) for method, params in calls]
        indexed = {int(item.get("id")): item for item in response if isinstance(item, dict)}
        output = []
        for index, (method, params) in enumerate(calls):
            item = indexed.get(index)
            if item is None or item.get("error") is not None:
                output.append(self.call(method, params))
            else:
                output.append(item.get("result"))
        return output


def recursive_logs(
    client: RpcClient,
    filter_base: dict[str, Any],
    start: int,
    end: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    queue: deque[tuple[int, int, int]] = deque([(start, end, 0)])
    rows: list[dict[str, Any]] = []
    unresolved: list[dict[str, Any]] = []
    while queue:
        left, right, depth = queue.popleft()
        payload = dict(filter_base)
        payload.update({"fromBlock": hex(left), "toBlock": hex(right)})
        try:
            result = client.call("eth_getLogs", [payload])
            if not isinstance(result, list):
                raise RuntimeError(f"eth_getLogs non-list {type(result)}")
            rows.extend(row for row in result if isinstance(row, dict))
        except Exception as exc:
            if left < right and depth < 28:
                mid = (left + right) // 2
                queue.appendleft((mid + 1, right, depth + 1))
                queue.appendleft((left, mid, depth + 1))
            else:
                unresolved.append({"from_block": left, "to_block": right, "depth": depth, "error": repr(exc)})
    return rows, unresolved


def event_key(row: dict[str, Any]) -> tuple[str, str, str]:
    return (
        str(row.get("blockHash") or "").lower(),
        str(row.get("transactionHash") or "").lower(),
        str(row.get("logIndex") or "").lower(),
    )


def decode_uint_array(data_words: list[str], offset_word: int) -> list[int]:
    length = int(data_words[offset_word], 16)
    return [int(data_words[offset_word + 1 + index], 16) for index in range(length)]


def normalize_log(wallet: str, direction: str, standard: str, row: dict[str, Any]) -> dict[str, Any]:
    topics = [str(value).lower() for value in row.get("topics") or []]
    data = str(row.get("data") or "0x").removeprefix("0x")
    output: dict[str, Any] = {
        "wallet": wallet,
        "direction": direction,
        "token_standard": standard,
        "contract_address": str(row.get("address") or "").lower(),
        "block_number": h2i(row.get("blockNumber")),
        "block_hash": str(row.get("blockHash") or "").lower(),
        "transaction_hash": str(row.get("transactionHash") or "").lower(),
        "transaction_index": h2i(row.get("transactionIndex")),
        "log_index": h2i(row.get("logIndex")),
        "removed": bool(row.get("removed")),
        "from_address": None,
        "to_address": None,
        "operator": None,
        "token_id": None,
        "amount": None,
        "token_ids": None,
        "amounts": None,
    }
    if standard == "ERC721":
        if len(topics) != 4:
            raise ValueError("ERC721 Transfer must have four topics")
        output.update({
            "from_address": topic_address(topics[1]),
            "to_address": topic_address(topics[2]),
            "token_id": str(int(topics[3], 16)),
            "amount": "1",
        })
    elif standard == "ERC1155_SINGLE":
        if len(topics) != 4 or len(data) != 128:
            raise ValueError("ERC1155 TransferSingle layout mismatch")
        output.update({
            "operator": topic_address(topics[1]),
            "from_address": topic_address(topics[2]),
            "to_address": topic_address(topics[3]),
            "token_id": str(int(data[:64], 16)),
            "amount": str(int(data[64:128], 16)),
        })
    elif standard == "ERC1155_BATCH":
        if len(topics) != 4 or len(data) % 64:
            raise ValueError("ERC1155 TransferBatch layout mismatch")
        data_words = [data[index:index + 64] for index in range(0, len(data), 64)]
        ids_offset = int(data_words[0], 16) // 32
        amounts_offset = int(data_words[1], 16) // 32
        ids = decode_uint_array(data_words, ids_offset)
        amounts = decode_uint_array(data_words, amounts_offset)
        if len(ids) != len(amounts):
            raise ValueError("ERC1155 ids/amounts length mismatch")
        output.update({
            "operator": topic_address(topics[1]),
            "from_address": topic_address(topics[2]),
            "to_address": topic_address(topics[3]),
            "token_ids": [str(value) for value in ids],
            "amounts": [str(value) for value in amounts],
        })
    return output


def batch_fetch(client: RpcClient, method: str, values: list[Any], size: int) -> dict[Any, Any]:
    output: dict[Any, Any] = {}
    for start in range(0, len(values), size):
        chunk = values[start:start + size]
        if method == "eth_getBlockByNumber":
            calls = [(method, [hex(int(value)), False]) for value in chunk]
        else:
            calls = [(method, [value]) for value in chunk]
        output.update(zip(chunk, client.batch(calls)))
    return output


def load_all_candidates() -> list[dict[str, Any]]:
    encoded = Path("wallet-verification/wallets.json.gz.b64").read_text().strip()
    p01 = json.loads(gzip.decompress(base64.b64decode(encoded)))
    p2_addresses = [
        line.strip().lower()
        for line in Path("wallet-verification/p2_addresses.txt").read_text().splitlines()
        if line.strip()
    ]
    rows: dict[str, dict[str, Any]] = {}
    for row in p01:
        wallet = row["wallet"].lower()
        rows[wallet] = row
    for wallet in p2_addresses:
        rows.setdefault(wallet, {
            "wallet": wallet,
            "priority": "P2",
            "verification_reasons": ["FIXED_P2_VERIFICATION_QUEUE"],
            "known_mints": [],
            "known_sales": [],
        })
    return [rows[key] for key in sorted(rows)]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--shard", type=int, required=True)
    parser.add_argument("--shard-count", type=int, default=16)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    all_rows = load_all_candidates()
    selected = [row for index, row in enumerate(all_rows) if index % args.shard_count == args.shard]
    client = RpcClient()
    if h2i(client.call("eth_chainId", [])) != CHAIN_ID:
        raise RuntimeError("Wrong chain")
    head = h2i(client.call("eth_blockNumber", []))
    assert head is not None
    raw_logs: list[dict[str, Any]] = []
    normalized: list[dict[str, Any]] = []
    unresolved: list[dict[str, Any]] = []
    summaries: list[dict[str, Any]] = []
    decode_errors: list[dict[str, Any]] = []
    query_specs = [
        ("ERC721", "incoming", {"topics": [ERC721_TRANSFER, None, None]}),
        ("ERC721", "outgoing", {"topics": [ERC721_TRANSFER, None, None]}),
        ("ERC1155_SINGLE", "incoming", {"topics": [ERC1155_SINGLE, None, None, None]}),
        ("ERC1155_SINGLE", "outgoing", {"topics": [ERC1155_SINGLE, None, None, None]}),
        ("ERC1155_BATCH", "incoming", {"topics": [ERC1155_BATCH, None, None, None]}),
        ("ERC1155_BATCH", "outgoing", {"topics": [ERC1155_BATCH, None, None, None]}),
    ]
    for item in selected:
        wallet = item["wallet"].lower()
        wallet_raw: dict[tuple[str, str, str], dict[str, Any]] = {}
        wallet_unresolved: list[dict[str, Any]] = []
        for standard, direction, base in query_specs:
            current = json.loads(json.dumps(base))
            wallet_topic = padded(wallet)
            if standard == "ERC721":
                current["topics"][2 if direction == "incoming" else 1] = wallet_topic
            else:
                current["topics"][3 if direction == "incoming" else 2] = wallet_topic
            rows, errors = recursive_logs(client, current, 0, head)
            for row in rows:
                wallet_raw[event_key(row)] = row
                try:
                    normalized.append(normalize_log(wallet, direction, standard, row))
                except Exception as exc:
                    decode_errors.append({"wallet": wallet, "direction": direction, "standard": standard, "error": repr(exc), "raw": row})
            for error in errors:
                wallet_unresolved.append({"wallet": wallet, "direction": direction, "standard": standard, **error})
        raw_logs.extend({"wallet": wallet, "raw": row} for row in wallet_raw.values())
        unresolved.extend(wallet_unresolved)
        wallet_rows = [row for row in normalized if row["wallet"] == wallet]
        summaries.append({
            "wallet": wallet,
            "priority": item.get("priority"),
            "verification_reasons": item.get("verification_reasons") or [],
            "fixed_head_block": head,
            "canonical_transfer_rows": len(wallet_rows),
            "erc721_rows": sum(row["token_standard"] == "ERC721" for row in wallet_rows),
            "erc1155_single_rows": sum(row["token_standard"] == "ERC1155_SINGLE" for row in wallet_rows),
            "erc1155_batch_rows": sum(row["token_standard"] == "ERC1155_BATCH" for row in wallet_rows),
            "incoming_rows": sum(row["direction"] == "incoming" for row in wallet_rows),
            "outgoing_rows": sum(row["direction"] == "outgoing" for row in wallet_rows),
            "unique_contracts": len({row["contract_address"] for row in wallet_rows}),
            "unique_transactions": len({row["transaction_hash"] for row in wallet_rows}),
            "zero_address_mint_rows": sum(row.get("from_address") == ZERO and row["direction"] == "incoming" for row in wallet_rows),
            "burn_rows": sum(row.get("to_address") == ZERO and row["direction"] == "outgoing" for row in wallet_rows),
            "unresolved_ranges": len(wallet_unresolved),
            "activity_status": "RHC_NFT_ACTIVITY_PRESENT" if wallet_rows else "NO_RHC_NFT_TRANSFER_AT_FIXED_HEAD",
            "strength_status": "NOT_EVALUATED",
            "production_approved": False,
        })
        print({"wallet": wallet, "rows": len(wallet_rows), "unresolved": len(wallet_unresolved)}, flush=True)
    unique_txs = sorted({row["transaction_hash"] for row in normalized if row.get("transaction_hash")})
    unique_blocks = sorted({row["block_number"] for row in normalized if row.get("block_number") is not None})
    transactions = batch_fetch(client, "eth_getTransactionByHash", unique_txs, 15)
    receipts = batch_fetch(client, "eth_getTransactionReceipt", unique_txs, 12)
    blocks = batch_fetch(client, "eth_getBlockByNumber", unique_blocks, 25)
    timestamp_map = {
        block: h2i(value.get("timestamp")) if isinstance(value, dict) else None
        for block, value in blocks.items()
    }
    for row in normalized:
        timestamp = timestamp_map.get(row["block_number"])
        row["block_timestamp_unix"] = timestamp
        row["block_timestamp_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(timestamp)) if timestamp else None
    write_jsonl(out / "raw_wallet_logs.jsonl", raw_logs)
    write_jsonl(out / "normalized_wallet_transfers.jsonl", normalized)
    write_csv(out / "normalized_wallet_transfers.csv", normalized)
    write_csv(out / "wallet_summary.csv", summaries)
    write_json(out / "unresolved_ranges.json", unresolved)
    write_json(out / "decode_errors.json", decode_errors)
    write_jsonl(out / "transactions.jsonl", [{"transaction_hash": key, "value": value} for key, value in transactions.items()])
    write_jsonl(out / "receipts.jsonl", [{"transaction_hash": key, "value": value} for key, value in receipts.items()])
    write_jsonl(out / "blocks.jsonl", [{"block_number": key, "value": value} for key, value in blocks.items()])
    failures = []
    if unresolved:
        failures.append({"code": "UNRESOLVED_LOG_RANGES", "count": len(unresolved)})
    if decode_errors:
        failures.append({"code": "LOG_DECODE_ERRORS", "count": len(decode_errors)})
    missing_txs = [key for key, value in transactions.items() if not isinstance(value, dict)]
    missing_receipts = [key for key, value in receipts.items() if not isinstance(value, dict)]
    missing_blocks = [key for key, value in blocks.items() if not isinstance(value, dict)]
    if missing_txs:
        failures.append({"code": "MISSING_TRANSACTIONS", "count": len(missing_txs)})
    if missing_receipts:
        failures.append({"code": "MISSING_RECEIPTS", "count": len(missing_receipts)})
    if missing_blocks:
        failures.append({"code": "MISSING_BLOCKS", "count": len(missing_blocks)})
    validation = {
        "status": "PASS" if not failures else "FAIL",
        "chain_id": CHAIN_ID,
        "fixed_head_block": head,
        "shard": args.shard,
        "shard_count": args.shard_count,
        "wallet_count": len(selected),
        "wallets_with_activity": sum(row["canonical_transfer_rows"] > 0 for row in summaries),
        "wallets_without_activity": sum(row["canonical_transfer_rows"] == 0 for row in summaries),
        "canonical_transfer_rows": len(normalized),
        "unique_transactions": len(unique_txs),
        "unique_blocks": len(unique_blocks),
        "rpc_request_count": client.request_count,
        "rpc_retry_count": client.retry_count,
        "failures": failures,
        "production_approved_wallets": 0,
    }
    write_json(out / "VALIDATION.json", validation)
    write_json(out / "input_wallets.json", selected)
    manifest = []
    for path in sorted(out.iterdir()):
        if path.is_file() and path.name != "MANIFEST.json":
            manifest.append({"path": path.name, "bytes": path.stat().st_size, "sha256": hashlib.sha256(path.read_bytes()).hexdigest()})
    write_json(out / "MANIFEST.json", manifest)
    print(json.dumps(validation, sort_keys=True), flush=True)
    if failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
