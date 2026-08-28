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
from collections import deque
from pathlib import Path
from typing import Any

RPC_URL = "https://rpc.mainnet.chain.robinhood.com"
CHAIN_ID = 4663
USER_AGENT = "RHC-Zero-Mint-Universe/1.0"
ZERO_TOPIC = "0x" + "0" * 64
ERC721_TRANSFER = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"


def keccak256(text: str) -> str:
    from Crypto.Hash import keccak
    digest = keccak.new(digest_bits=256)
    digest.update(text.encode("utf-8"))
    return "0x" + digest.hexdigest()


TARGETS = {
    "erc721": {"topic0": ERC721_TRANSFER, "topics": [ERC721_TRANSFER, ZERO_TOPIC]},
    "erc1155_single": {
        "topic0": keccak256("TransferSingle(address,address,address,uint256,uint256)"),
        "topics": [keccak256("TransferSingle(address,address,address,uint256,uint256)"), None, ZERO_TOPIC],
    },
    "erc1155_batch": {
        "topic0": keccak256("TransferBatch(address,address,address,uint256[],uint256[])"),
        "topics": [keccak256("TransferBatch(address,address,address,uint256[],uint256[])"), None, ZERO_TOPIC],
    },
}


def h2i(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        return int(value, 16) if value.startswith("0x") else int(value)
    return int(value)


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
    def __init__(self, min_interval: float = 0.9):
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
                with urllib.request.urlopen(request, timeout=180) as response:
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
            raise RuntimeError(f"Unexpected RPC response: {type(response)}")
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


def collect(client: RpcClient, topics: list[Any], head: int, chunk: int = 200_000) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    queue: deque[tuple[int, int, int]] = deque(
        (start, min(head, start + chunk - 1), 0) for start in range(0, head + 1, chunk)
    )
    rows: list[dict[str, Any]] = []
    unresolved: list[dict[str, Any]] = []
    completed = 0
    while queue:
        left, right, depth = queue.popleft()
        try:
            result = client.call("eth_getLogs", [{"fromBlock": hex(left), "toBlock": hex(right), "topics": topics}])
            if not isinstance(result, list):
                raise RuntimeError(f"eth_getLogs non-list {type(result)}")
            rows.extend(row for row in result if isinstance(row, dict))
            completed += 1
            if completed % 25 == 0:
                print({"completed_ranges": completed, "queued": len(queue), "events": len(rows)}, flush=True)
        except Exception as exc:
            if left < right and depth < 28:
                middle = (left + right) // 2
                queue.appendleft((middle + 1, right, depth + 1))
                queue.appendleft((left, middle, depth + 1))
            else:
                unresolved.append({"from_block": left, "to_block": right, "depth": depth, "error": repr(exc)})
    return rows, unresolved


def event_key(row: dict[str, Any]) -> tuple[str, str, str]:
    return (
        str(row.get("blockHash") or "").lower(),
        str(row.get("transactionHash") or "").lower(),
        str(row.get("logIndex") or "").lower(),
    )


def uint_array(words: list[str], offset: int) -> list[int]:
    index = offset // 32
    count = int(words[index], 16)
    return [int(words[index + 1 + item], 16) for item in range(count)]


def decode(target: str, row: dict[str, Any]) -> dict[str, Any]:
    topics = [str(value).lower() for value in row.get("topics") or []]
    data = str(row.get("data") or "0x").removeprefix("0x")
    base = {
        "chain_id": CHAIN_ID,
        "token_standard": target.upper(),
        "contract_address": str(row.get("address") or "").lower(),
        "block_number": h2i(row.get("blockNumber")),
        "block_hash": str(row.get("blockHash") or "").lower(),
        "transaction_hash": str(row.get("transactionHash") or "").lower(),
        "transaction_index": h2i(row.get("transactionIndex")),
        "log_index": h2i(row.get("logIndex")),
        "removed": bool(row.get("removed")),
        "operator": None,
        "recipient": None,
        "token_id": None,
        "amount": None,
        "token_ids": None,
        "amounts": None,
    }
    if target == "erc721":
        if len(topics) != 4:
            raise ValueError("ERC721 mint layout mismatch")
        base.update({"recipient": topic_address(topics[2]), "token_id": str(int(topics[3], 16)), "amount": "1"})
    elif target == "erc1155_single":
        if len(topics) != 4 or len(data) != 128:
            raise ValueError("ERC1155 single layout mismatch")
        base.update({
            "operator": topic_address(topics[1]),
            "recipient": topic_address(topics[3]),
            "token_id": str(int(data[:64], 16)),
            "amount": str(int(data[64:128], 16)),
        })
    else:
        if len(topics) != 4 or len(data) % 64:
            raise ValueError("ERC1155 batch layout mismatch")
        words = [data[index:index + 64] for index in range(0, len(data), 64)]
        ids = uint_array(words, int(words[0], 16))
        amounts = uint_array(words, int(words[1], 16))
        if len(ids) != len(amounts):
            raise ValueError("ERC1155 batch ids/amounts mismatch")
        base.update({
            "operator": topic_address(topics[1]),
            "recipient": topic_address(topics[3]),
            "token_ids": [str(value) for value in ids],
            "amounts": [str(value) for value in amounts],
        })
    return base


def batch_fetch(client: RpcClient, method: str, values: list[Any], size: int) -> dict[Any, Any]:
    output: dict[Any, Any] = {}
    for start in range(0, len(values), size):
        chunk = values[start:start + size]
        calls = [(method, [hex(int(value)), False] if method == "eth_getBlockByNumber" else [value]) for value in chunk]
        output.update(zip(chunk, client.batch(calls)))
    return output


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", choices=sorted(TARGETS), required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    client = RpcClient()
    if h2i(client.call("eth_chainId", [])) != CHAIN_ID:
        raise RuntimeError("Wrong chain")
    head = h2i(client.call("eth_blockNumber", []))
    assert head is not None
    raw, unresolved = collect(client, TARGETS[args.target]["topics"], head)
    unique: dict[tuple[str, str, str], dict[str, Any]] = {}
    duplicate_count = 0
    for row in raw:
        key = event_key(row)
        if key in unique:
            duplicate_count += 1
        unique[key] = row
    logs = sorted(unique.values(), key=lambda row: (h2i(row.get("blockNumber")) or -1, h2i(row.get("logIndex")) or -1))
    decoded = []
    decode_errors = []
    for row in logs:
        try:
            decoded.append(decode(args.target, row))
        except Exception as exc:
            decode_errors.append({"event_key": event_key(row), "error": repr(exc), "raw": row})
    tx_hashes = sorted({row["transaction_hash"] for row in decoded})
    block_numbers = sorted({row["block_number"] for row in decoded if row["block_number"] is not None})
    transactions = batch_fetch(client, "eth_getTransactionByHash", tx_hashes, 15)
    receipts = batch_fetch(client, "eth_getTransactionReceipt", tx_hashes, 12)
    blocks = batch_fetch(client, "eth_getBlockByNumber", block_numbers, 25)
    timestamp_map = {block: h2i(value.get("timestamp")) if isinstance(value, dict) else None for block, value in blocks.items()}
    for row in decoded:
        timestamp = timestamp_map.get(row["block_number"])
        row["block_timestamp_unix"] = timestamp
        row["block_timestamp_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(timestamp)) if timestamp else None
    write_jsonl(out / "raw_zero_mint_logs.jsonl", logs)
    write_jsonl(out / "decoded_zero_mints.jsonl", decoded)
    write_csv(out / "decoded_zero_mints.csv", decoded)
    write_jsonl(out / "transactions.jsonl", [{"transaction_hash": key, "value": value} for key, value in transactions.items()])
    write_jsonl(out / "receipts.jsonl", [{"transaction_hash": key, "value": value} for key, value in receipts.items()])
    write_jsonl(out / "blocks.jsonl", [{"block_number": key, "value": value} for key, value in blocks.items()])
    write_json(out / "unresolved_ranges.json", unresolved)
    write_json(out / "decode_errors.json", decode_errors)
    projects: dict[str, dict[str, Any]] = {}
    for row in decoded:
        project = projects.setdefault(row["contract_address"], {
            "contract_address": row["contract_address"],
            "token_standard": row["token_standard"],
            "first_mint_block": row["block_number"],
            "last_mint_block": row["block_number"],
            "mint_event_rows": 0,
            "minted_quantity": 0,
            "unique_recipients": set(),
            "unique_transactions": set(),
        })
        project["first_mint_block"] = min(project["first_mint_block"], row["block_number"])
        project["last_mint_block"] = max(project["last_mint_block"], row["block_number"])
        project["mint_event_rows"] += 1
        if row.get("amount") is not None:
            project["minted_quantity"] += int(row["amount"])
        elif row.get("amounts"):
            project["minted_quantity"] += sum(int(value) for value in row["amounts"])
        project["unique_recipients"].add(row["recipient"])
        project["unique_transactions"].add(row["transaction_hash"])
    project_rows = []
    for value in projects.values():
        project_rows.append({
            **{key: item for key, item in value.items() if key not in ("unique_recipients", "unique_transactions")},
            "unique_recipients": len(value["unique_recipients"]),
            "unique_transactions": len(value["unique_transactions"]),
        })
    write_csv(out / "project_mint_population.csv", project_rows)
    failures = []
    if unresolved:
        failures.append({"code": "UNRESOLVED_BLOCK_RANGES", "count": len(unresolved)})
    if decode_errors:
        failures.append({"code": "DECODE_ERRORS", "count": len(decode_errors)})
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
        "target": args.target,
        "chain_id": CHAIN_ID,
        "fixed_head_block": head,
        "raw_rows_before_dedup": len(raw),
        "duplicate_rows_removed": duplicate_count,
        "canonical_mint_event_rows": len(decoded),
        "unique_contracts": len(project_rows),
        "unique_transactions": len(tx_hashes),
        "unresolved_ranges": len(unresolved),
        "rpc_request_count": client.request_count,
        "rpc_retry_count": client.retry_count,
        "failures": failures,
    }
    write_json(out / "VALIDATION.json", validation)
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
