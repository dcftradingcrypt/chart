#!/usr/bin/env python3
from __future__ import annotations

import argparse
import concurrent.futures
import csv
import hashlib
import json
import random
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

BASE = "https://robinhoodchain.blockscout.com/api"
TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
ZERO = "0x0000000000000000000000000000000000000000"
UA = "RHC-Strict-NFT-Token-Provenance/1.0"
MAX_RESULT_HINT = 1000

_thread = threading.local()

def session_state() -> dict[str, Any]:
    if not hasattr(_thread, "state"):
        _thread.state = {"last": 0.0, "requests": 0, "backoffs": 0}
    return _thread.state


def pace(delay: float = 0.20) -> None:
    state = session_state()
    wait = delay - (time.monotonic() - state["last"])
    if wait > 0:
        time.sleep(wait)


def get_json(params: dict[str, Any], attempts: int = 12) -> dict[str, Any]:
    query = urllib.parse.urlencode(params)
    url = BASE + "?" + query
    state = session_state()
    last_error: Exception | None = None
    for attempt in range(attempts):
        pace()
        state["requests"] += 1
        try:
            req = urllib.request.Request(url, headers={"accept": "application/json", "user-agent": UA})
            with urllib.request.urlopen(req, timeout=120) as response:
                payload = response.read()
            state["last"] = time.monotonic()
            data = json.loads(payload.decode("utf-8"))
            if not isinstance(data, dict):
                raise RuntimeError(f"Unexpected response type: {type(data)}")
            return data
        except urllib.error.HTTPError as exc:
            state["last"] = time.monotonic()
            last_error = exc
            if exc.code == 429 or exc.code >= 500:
                state["backoffs"] += 1
                time.sleep(min(90, 2 ** min(attempt, 6) + random.random() * 5))
                continue
            body = exc.read(1000).decode("utf-8", "replace")
            raise RuntimeError(f"HTTP {exc.code}: {body}") from exc
        except Exception as exc:
            last_error = exc
            if attempt + 1 < attempts:
                time.sleep(min(45, 2 ** min(attempt, 5) + random.random() * 3))
                continue
    raise RuntimeError(f"GET failed: {last_error}")


def parse_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        value = value.strip()
        try:
            return int(value, 16) if value.startswith("0x") else int(value)
        except ValueError:
            return None
    return None


def topic_address(topic: str) -> str:
    return "0x" + topic.lower()[-40:]


def result_rows(data: dict[str, Any]) -> list[dict[str, Any]]:
    result = data.get("result")
    status = str(data.get("status", ""))
    message = str(data.get("message", ""))
    if isinstance(result, list):
        return [row for row in result if isinstance(row, dict)]
    text = str(result or "")
    if status == "0" and ("no logs" in text.lower() or "no records" in text.lower() or "no logs" in message.lower()):
        return []
    raise RuntimeError(f"Blockscout error status={status} message={message} result={text[:500]}")


def fetch_range(contract: str, start: int, end: int, depth: int = 0) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    params = {
        "module": "logs",
        "action": "getLogs",
        "fromBlock": start,
        "toBlock": end,
        "address": contract,
        "topic0": TRANSFER_TOPIC,
    }
    try:
        rows = result_rows(get_json(params))
    except Exception as exc:
        if start >= end:
            return [], [{"contract": contract, "from_block": start, "to_block": end, "error": repr(exc)}]
        middle = (start + end) // 2
        left, left_errors = fetch_range(contract, start, middle, depth + 1)
        right, right_errors = fetch_range(contract, middle + 1, end, depth + 1)
        return left + right, left_errors + right_errors

    if len(rows) >= MAX_RESULT_HINT:
        if start >= end:
            return rows, [{"contract": contract, "from_block": start, "to_block": end, "error": "RESULT_CAP_AT_SINGLE_BLOCK", "rows": len(rows)}]
        middle = (start + end) // 2
        left, left_errors = fetch_range(contract, start, middle, depth + 1)
        right, right_errors = fetch_range(contract, middle + 1, end, depth + 1)
        return left + right, left_errors + right_errors
    return rows, []


def collect_contract(row: dict[str, str], end_block: int) -> dict[str, Any]:
    contract = row["nft_contract"].lower()
    started = time.time()
    rows, errors = fetch_range(contract, 0, end_block)
    dedup: dict[tuple[str, int], dict[str, Any]] = {}
    decode_errors: list[dict[str, Any]] = []
    decoded: list[dict[str, Any]] = []
    for raw in rows:
        tx_hash = str(raw.get("transactionHash") or raw.get("transaction_hash") or "").lower()
        log_index = parse_int(raw.get("logIndex") or raw.get("log_index"))
        if not tx_hash or log_index is None:
            decode_errors.append({"contract": contract, "error": "MISSING_TX_OR_LOG_INDEX", "raw": raw})
            continue
        dedup[(tx_hash, log_index)] = raw
    for (tx_hash, log_index), raw in sorted(dedup.items(), key=lambda item: (parse_int(item[1].get("blockNumber") or item[1].get("block_number")) or 0, item[0][1])):
        topics = [str(value).lower() for value in (raw.get("topics") or [])]
        if len(topics) < 4 or topics[0] != TRANSFER_TOPIC:
            decode_errors.append({"contract": contract, "transaction_hash": tx_hash, "log_index": log_index, "error": "INVALID_TRANSFER_TOPICS"})
            continue
        from_address = topic_address(topics[1])
        to_address = topic_address(topics[2])
        token_id = int(topics[3], 16)
        decoded.append({
            "nft_contract": contract,
            "transaction_hash": tx_hash,
            "log_index": log_index,
            "block_number": parse_int(raw.get("blockNumber") or raw.get("block_number")),
            "transaction_index": parse_int(raw.get("transactionIndex") or raw.get("transaction_index")),
            "from_address": from_address,
            "to_address": to_address,
            "token_id": str(token_id),
            "event_type": "MINT" if from_address == ZERO else ("BURN" if to_address == ZERO else "TRANSFER"),
            "source": "BLOCKSCOUT_LEGACY_EXACT_ADDRESS_TOPIC_RANGE",
        })
    return {
        "contract": contract,
        "source_row": row,
        "raw_rows": list(dedup.values()),
        "decoded": decoded,
        "errors": errors + decode_errors,
        "elapsed_seconds": round(time.time() - started, 3),
        "request_stats": dict(session_state()),
    }


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fields = sorted({key for row in rows for key in row}) if rows else ["empty"]
    with path.open("w", encoding="utf-8-sig", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({key: json.dumps(value, ensure_ascii=False, sort_keys=True) if isinstance(value, (dict, list, tuple)) else value for key, value in row.items()})


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--contracts", default="token-provenance/strict_sale_contracts.csv")
    parser.add_argument("--end-block", type=int, default=46840468)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--out", default="out-token-provenance")
    args = parser.parse_args()

    output = Path(args.out)
    output.mkdir(parents=True, exist_ok=True)
    with Path(args.contracts).open(newline="", encoding="utf-8-sig") as file:
        contracts = list(csv.DictReader(file))

    results: list[dict[str, Any]] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {executor.submit(collect_contract, row, args.end_block): row["nft_contract"] for row in contracts}
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            results.append(result)
            print(json.dumps({"contract": result["contract"], "raw_rows": len(result["raw_rows"]), "decoded": len(result["decoded"]), "errors": len(result["errors"]), "elapsed_seconds": result["elapsed_seconds"]}), flush=True)

    results.sort(key=lambda item: item["contract"])
    decoded = [row for result in results for row in result["decoded"]]
    errors = [row for result in results for row in result["errors"]]
    raw = [row for result in results for row in result["raw_rows"]]
    summary = []
    for result in results:
        transfers = result["decoded"]
        summary.append({
            **result["source_row"],
            "transfer_log_rows": len(transfers),
            "mint_rows": sum(row["event_type"] == "MINT" for row in transfers),
            "burn_rows": sum(row["event_type"] == "BURN" for row in transfers),
            "secondary_transfer_rows": sum(row["event_type"] == "TRANSFER" for row in transfers),
            "unique_token_ids": len({row["token_id"] for row in transfers}),
            "min_block": min((row["block_number"] for row in transfers if row["block_number"] is not None), default=None),
            "max_block": max((row["block_number"] for row in transfers if row["block_number"] is not None), default=None),
            "error_rows": len(result["errors"]),
            "elapsed_seconds": result["elapsed_seconds"],
        })

    with (output / "raw_logs.jsonl").open("w", encoding="utf-8") as file:
        for row in raw:
            file.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
    write_csv(output / "transfers.csv", decoded)
    write_csv(output / "contract_summary.csv", summary)
    write_csv(output / "errors.csv", errors)

    identities = [(row["nft_contract"], row["transaction_hash"], row["log_index"]) for row in decoded]
    validation = {
        "status": "PASS" if not errors and len(results) == len(contracts) and len(identities) == len(set(identities)) else "FAIL",
        "fixed_end_block": args.end_block,
        "requested_contracts": len(contracts),
        "completed_contracts": len(results),
        "raw_log_rows": len(raw),
        "decoded_transfer_rows": len(decoded),
        "mint_rows": sum(row["event_type"] == "MINT" for row in decoded),
        "burn_rows": sum(row["event_type"] == "BURN" for row in decoded),
        "secondary_transfer_rows": sum(row["event_type"] == "TRANSFER" for row in decoded),
        "unique_identity_rows": len(set(identities)),
        "duplicate_identity_rows": len(identities) - len(set(identities)),
        "error_rows": len(errors),
        "production_approved_wallets": 0,
    }
    (output / "validation.json").write_text(json.dumps(validation, indent=2, sort_keys=True), encoding="utf-8")
    manifest = []
    for path in sorted(output.iterdir()):
        if path.is_file():
            manifest.append({"path": path.name, "bytes": path.stat().st_size, "sha256": sha256(path)})
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(validation, sort_keys=True), flush=True)
    if validation["status"] != "PASS":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
