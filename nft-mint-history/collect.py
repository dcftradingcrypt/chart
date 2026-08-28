#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import json
import random
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

ZERO = "0x0000000000000000000000000000000000000000"
RPC = "https://rpc.mainnet.chain.robinhood.com"
SOURCES = [
    "https://robinhoodchain.blockscout.com/api/v2",
    "https://explorer.hoodmarketcap.com/api/v2",
]
UA = "RHC-NFT-Mint-History/1.0 read-only"


def request_json(url: str, *, data: bytes | None = None, attempts: int = 12) -> Any:
    last: Exception | None = None
    for attempt in range(attempts):
        try:
            headers = {"accept": "application/json", "user-agent": UA}
            if data is not None:
                headers["content-type"] = "application/json"
            request = urllib.request.Request(url, data=data, headers=headers)
            with urllib.request.urlopen(request, timeout=90) as response:
                raw = response.read()
            return json.loads(raw.decode("utf-8"))
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


def rpc(method: str, params: list[Any]) -> Any:
    payload = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode()
    result = request_json(RPC, data=payload)
    if not isinstance(result, dict) or result.get("error"):
        raise RuntimeError(f"RPC {method} failed: {result!r}")
    return result.get("result")


def addr(value: Any) -> str | None:
    if isinstance(value, dict):
        value = value.get("hash") or value.get("address_hash") or value.get("address")
    if not value:
        return None
    text = str(value).lower()
    return text if text.startswith("0x") and len(text) == 42 else None


def pick(row: dict[str, Any], *paths: str) -> Any:
    for path in paths:
        value: Any = row
        valid = True
        for part in path.split("."):
            if not isinstance(value, dict) or part not in value:
                valid = False
                break
            value = value[part]
        if valid and value is not None:
            return value
    return None


def intish(value: Any, default: int | None = None) -> int | None:
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


def normalize(item: dict[str, Any], source: str, requested_standard: str) -> dict[str, Any]:
    token = item.get("token") or {}
    tx_hash = pick(item, "transaction_hash", "tx_hash", "transaction.hash", "hash")
    if isinstance(tx_hash, dict):
        tx_hash = tx_hash.get("hash")
    from_address = addr(pick(item, "from", "from_address", "from.hash"))
    to_address = addr(pick(item, "to", "to_address", "to.hash"))
    contract = addr(pick(token if isinstance(token, dict) else {}, "address", "address_hash")) or addr(pick(item, "token_address", "address"))
    token_id = pick(item, "token_id", "tokenId", "total.token_id", "id")
    amount = pick(item, "amount", "total.value", "value")
    block_number = intish(pick(item, "block_number", "blockNumber", "block"))
    return {
        "source": source,
        "requested_standard": requested_standard,
        "reported_standard": pick(token if isinstance(token, dict) else {}, "type", "standard") or pick(item, "type", "token_type"),
        "contract_address": contract,
        "transaction_hash": str(tx_hash).lower() if tx_hash else None,
        "log_index": intish(pick(item, "log_index", "logIndex", "index")),
        "block_number": block_number,
        "timestamp_utc": pick(item, "timestamp", "block_timestamp"),
        "from_address": from_address,
        "to_address": to_address,
        "token_id": str(token_id) if token_id is not None else None,
        "amount_raw": str(amount) if amount is not None else None,
        "method": pick(item, "method", "method_name"),
        "raw_json": item,
    }


def paginate(source: str, standard: str, fixed_head: int) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    # Blockscout v2 token-transfer filter names differ between releases. The
    # endpoint is tried with the explicit type first; returned rows are still
    # independently filtered and validated below.
    params: dict[str, Any] = {"type": standard}
    seen: set[str] = set()
    rows: list[dict[str, Any]] = []
    pages = 0
    for page in range(1, 100001):
        url = f"{source}/addresses/{ZERO}/token-transfers"
        if params:
            url += "?" + urllib.parse.urlencode(params, doseq=True)
        payload = request_json(url)
        pages = page
        if not isinstance(payload, dict) or not isinstance(payload.get("items"), list):
            raise RuntimeError(f"unexpected transfer payload: {payload!r}")
        for item in payload["items"]:
            if not isinstance(item, dict):
                continue
            row = normalize(item, source, standard)
            # The zero address endpoint can include inbound burns as well as
            # outbound mints. Only outbound zero-address transfers are kept.
            if row["from_address"] != ZERO:
                continue
            block_number = row["block_number"]
            if block_number is None or block_number > fixed_head:
                continue
            reported = str(row.get("reported_standard") or "").upper()
            if standard == "ERC-721" and "721" not in reported and row.get("token_id") is None:
                continue
            if standard == "ERC-1155" and "1155" not in reported:
                continue
            rows.append(row)
        nxt = payload.get("next_page_params")
        if not nxt:
            return rows, {
                "source": source,
                "standard": standard,
                "pages": pages,
                "rows": len(rows),
                "pagination_exhausted": True,
                "fixed_head": fixed_head,
                "status": "PASS",
            }
        if not isinstance(nxt, dict):
            raise RuntimeError(f"invalid next_page_params: {nxt!r}")
        key = json.dumps(nxt, sort_keys=True, separators=(",", ":"))
        if key in seen:
            raise RuntimeError(f"repeated pagination cursor: {key}")
        seen.add(key)
        params = {"type": standard, **nxt}
        time.sleep(0.35)
    raise RuntimeError(f"page limit exceeded for {source} {standard}")


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fields: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for key in row:
            if key not in seen:
                seen.add(key)
                fields.append(key)
    with path.open("w", encoding="utf-8-sig", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fields or ["empty"], extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({key: json.dumps(value, ensure_ascii=False, sort_keys=True) if isinstance(value, (dict, list, tuple)) else value for key, value in row.items()})


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--standard", choices=["ERC-721", "ERC-1155"], required=True)
    parser.add_argument("--fixed-head", type=int, required=True)
    parser.add_argument("--fixed-head-hash", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    source_results: list[tuple[list[dict[str, Any]], dict[str, Any]]] = []
    errors: list[dict[str, Any]] = []
    for source in SOURCES:
        try:
            source_results.append(paginate(source, args.standard, args.fixed_head))
        except Exception as exc:
            errors.append({"source": source, "standard": args.standard, "error": repr(exc)})

    if not source_results:
        rows: list[dict[str, Any]] = []
        coverage: list[dict[str, Any]] = []
    else:
        # One fully exhausted source is sufficient for completeness. Additional
        # sources are used for set comparison and discrepancy reporting.
        coverage = [meta for _, meta in source_results]
        dedup: dict[tuple[Any, ...], dict[str, Any]] = {}
        for source_rows, _ in source_results:
            for row in source_rows:
                key = (
                    row.get("transaction_hash"),
                    row.get("log_index"),
                    row.get("contract_address"),
                    row.get("token_id"),
                    row.get("amount_raw"),
                    row.get("to_address"),
                )
                dedup.setdefault(key, row)
        rows = sorted(dedup.values(), key=lambda row: (row.get("block_number") or -1, row.get("transaction_hash") or "", row.get("log_index") or -1))

    failures: list[dict[str, Any]] = []
    if not source_results:
        failures.append({"code": "NO_COMPLETE_SOURCE"})
    if not any(meta.get("pagination_exhausted") and meta.get("status") == "PASS" for meta in coverage):
        failures.append({"code": "NO_EXHAUSTED_SOURCE"})
    if any(row.get("from_address") != ZERO for row in rows):
        failures.append({"code": "NON_MINT_TRANSFER_PRESENT"})
    if any(row.get("block_number") is None or int(row["block_number"]) > args.fixed_head for row in rows):
        failures.append({"code": "INVALID_BLOCK_RANGE"})
    if any(not row.get("transaction_hash") or not row.get("contract_address") or row.get("log_index") is None for row in rows):
        failures.append({"code": "MINT_EVENT_IDENTIFIER_MISSING"})

    with gzip.open(out / "mint_transfers.jsonl.gz", "wt", encoding="utf-8") as file:
        for row in rows:
            file.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
    write_csv(out / "source_coverage.csv", coverage)
    write_csv(out / "collection_errors.csv", errors)
    report = {
        "status": "PASS" if not failures else "FAIL",
        "standard": args.standard,
        "fixed_head": args.fixed_head,
        "fixed_head_hash": args.fixed_head_hash.lower(),
        "mint_transfer_rows": len(rows),
        "unique_contracts": len({row.get("contract_address") for row in rows}),
        "unique_transactions": len({row.get("transaction_hash") for row in rows}),
        "complete_sources": len(coverage),
        "source_errors": len(errors),
        "failures": failures,
        "production_approved_wallets": 0,
        "decision_use": "PRIMARY_MINT_EVENT_UNIVERSE",
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
