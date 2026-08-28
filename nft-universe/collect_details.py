#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import json
import random
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Iterator

SOURCES = [
    "https://robinhoodchain.blockscout.com/api/v2",
    "https://explorer.hoodmarketcap.com/api/v2",
]
UA = "RHC-NFT-Contract-Details/1.0 read-only"
ZERO = "0x0000000000000000000000000000000000000000"
ADDRESS_RE = re.compile(r"^0x[a-f0-9]{40}$")


def get_json(url: str, params: dict[str, Any] | None = None, attempts: int = 12) -> Any:
    if params:
        url += ("&" if "?" in url else "?") + urllib.parse.urlencode(params, doseq=True)
    last: Exception | None = None
    for attempt in range(attempts):
        try:
            req = urllib.request.Request(url, headers={"accept": "application/json", "user-agent": UA})
            with urllib.request.urlopen(req, timeout=90) as response:
                data = response.read()
            return json.loads(data.decode("utf-8"))
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
    raise RuntimeError(f"GET failed: {url}: {last!r}")


def addr(value: Any) -> str | None:
    if isinstance(value, dict):
        value = value.get("hash") or value.get("address_hash") or value.get("address")
    if value is None:
        return None
    text = str(value).lower()
    return text if ADDRESS_RE.fullmatch(text) else None


def pick(row: dict[str, Any], *paths: str) -> Any:
    for path in paths:
        value: Any = row
        ok = True
        for part in path.split("."):
            if not isinstance(value, dict) or part not in value:
                ok = False
                break
            value = value[part]
        if ok and value is not None:
            return value
    return None


def paginate(source: str, path: str) -> Iterator[tuple[int, list[dict[str, Any]], dict[str, Any] | None]]:
    params: dict[str, Any] = {}
    seen: set[str] = set()
    for page in range(1, 100001):
        payload = get_json(f"{source}/{path.lstrip('/')}", params or None)
        if not isinstance(payload, dict) or not isinstance(payload.get("items"), list):
            raise RuntimeError(f"unexpected payload for {source}/{path}: {payload!r}")
        items = [row for row in payload["items"] if isinstance(row, dict)]
        nxt = payload.get("next_page_params")
        yield page, items, nxt if isinstance(nxt, dict) else None
        if not nxt:
            return
        if not isinstance(nxt, dict):
            raise RuntimeError(f"invalid next_page_params: {nxt!r}")
        key = json.dumps(nxt, sort_keys=True, separators=(",", ":"))
        if key in seen:
            raise RuntimeError(f"repeated next_page_params: {key}")
        seen.add(key)
        params = nxt
        time.sleep(0.25)
    raise RuntimeError(f"page limit exceeded: {source}/{path}")


def fetch_optional(path: str) -> tuple[Any, str | None, list[str]]:
    errors: list[str] = []
    for source in SOURCES:
        try:
            return get_json(f"{source}/{path.lstrip('/')}"), source, errors
        except Exception as exc:
            errors.append(f"{source}:{exc!r}")
    return None, None, errors


def transfer_row(contract: str, item: dict[str, Any], source: str) -> dict[str, Any]:
    from_address = addr(pick(item, "from", "from_address", "from.hash"))
    to_address = addr(pick(item, "to", "to_address", "to.hash"))
    token = pick(item, "token") or {}
    token_id = pick(item, "token_id", "tokenId", "total.token_id", "id")
    amount = pick(item, "amount", "total.value", "value")
    transaction_hash = pick(item, "transaction_hash", "tx_hash", "transaction.hash", "hash")
    if isinstance(transaction_hash, dict):
        transaction_hash = transaction_hash.get("hash")
    event_kind = "TRANSFER"
    if from_address == ZERO:
        event_kind = "MINT"
    elif to_address == ZERO:
        event_kind = "BURN"
    return {
        "contract_address": contract,
        "transaction_hash": str(transaction_hash).lower() if transaction_hash else None,
        "log_index": pick(item, "log_index", "logIndex", "index"),
        "block_number": pick(item, "block_number", "blockNumber", "block"),
        "timestamp_utc": pick(item, "timestamp", "block_timestamp"),
        "from_address": from_address,
        "to_address": to_address,
        "token_id": str(token_id) if token_id is not None else None,
        "amount_raw": str(amount) if amount is not None else None,
        "method": pick(item, "method", "method_name"),
        "reported_token_type": pick(token if isinstance(token, dict) else {}, "type", "standard"),
        "event_kind": event_kind,
        "source": source,
        "raw_json": item,
    }


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
    parser.add_argument("--universe", required=True)
    parser.add_argument("--shard", type=int, required=True)
    parser.add_argument("--shard-count", type=int, default=16)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    with Path(args.universe).open(newline="", encoding="utf-8-sig") as file:
        universe = list(csv.DictReader(file))
    selected = [row for index, row in enumerate(universe) if index % args.shard_count == args.shard]

    metadata_rows: list[dict[str, Any]] = []
    summary_rows: list[dict[str, Any]] = []
    error_rows: list[dict[str, Any]] = []
    transfer_path = out / "all_transfers.jsonl.gz"
    mint_path = out / "mint_transfers.jsonl.gz"
    burn_path = out / "burn_transfers.jsonl.gz"
    all_count = mint_count = burn_count = 0

    with gzip.open(transfer_path, "wt", encoding="utf-8") as all_file, gzip.open(mint_path, "wt", encoding="utf-8") as mint_file, gzip.open(burn_path, "wt", encoding="utf-8") as burn_file:
        for position, source_row in enumerate(selected, 1):
            contract = str(source_row.get("contract_address") or "").lower()
            print({"shard": args.shard, "position": position, "total": len(selected), "contract": contract}, flush=True)
            if not ADDRESS_RE.fullmatch(contract):
                error_rows.append({"contract_address": contract, "stage": "input", "error": "INVALID_ADDRESS"})
                continue
            address_data, address_source, address_errors = fetch_optional(f"addresses/{contract}")
            contract_data, contract_source, contract_errors = fetch_optional(f"smart-contracts/{contract}")
            metadata_rows.append({
                "contract_address": contract,
                "universe_reported_standards": source_row.get("reported_standards"),
                "universe_names": source_row.get("names"),
                "universe_symbols": source_row.get("symbols"),
                "address_source": address_source,
                "address_errors": address_errors,
                "address_json": address_data,
                "smart_contract_source": contract_source,
                "smart_contract_errors": contract_errors,
                "smart_contract_json": contract_data,
            })

            history_source = None
            history_errors: list[str] = []
            pages = contract_all = contract_mints = contract_burns = 0
            exhausted = False
            for candidate_source in SOURCES:
                try:
                    # Write only after one complete source has exhausted pagination.
                    buffered: list[dict[str, Any]] = []
                    candidate_pages = 0
                    for page, items, nxt in paginate(candidate_source, f"tokens/{contract}/transfers"):
                        candidate_pages = page
                        buffered.extend(transfer_row(contract, item, candidate_source) for item in items)
                    history_source = candidate_source
                    pages = candidate_pages
                    exhausted = True
                    for row in buffered:
                        encoded = json.dumps(row, ensure_ascii=False, sort_keys=True)
                        all_file.write(encoded + "\n")
                        all_count += 1
                        contract_all += 1
                        if row["event_kind"] == "MINT":
                            mint_file.write(encoded + "\n")
                            mint_count += 1
                            contract_mints += 1
                        elif row["event_kind"] == "BURN":
                            burn_file.write(encoded + "\n")
                            burn_count += 1
                            contract_burns += 1
                    break
                except Exception as exc:
                    history_errors.append(f"{candidate_source}:{exc!r}")
            if not exhausted:
                error_rows.append({"contract_address": contract, "stage": "transfers", "error": " | ".join(history_errors)})
            summary_rows.append({
                "contract_address": contract,
                "history_source": history_source,
                "history_errors": history_errors,
                "pagination_exhausted": exhausted,
                "pages": pages,
                "transfer_rows": contract_all,
                "mint_rows": contract_mints,
                "burn_rows": contract_burns,
                "decision_use": "PROJECT_AND_TOKEN_LIFECYCLE_INPUT",
            })

    failures: list[dict[str, Any]] = []
    if len(summary_rows) != len(selected):
        failures.append({"code": "SUMMARY_COUNT_MISMATCH", "selected": len(selected), "summaries": len(summary_rows)})
    for row in summary_rows:
        if not row["pagination_exhausted"]:
            failures.append({"code": "TRANSFER_HISTORY_INCOMPLETE", "contract_address": row["contract_address"], "errors": row["history_errors"]})
    if error_rows:
        failures.append({"code": "COLLECTION_ERRORS_PRESENT", "count": len(error_rows)})

    write_csv(out / "contract_metadata.csv", metadata_rows)
    write_csv(out / "contract_transfer_summary.csv", summary_rows)
    write_csv(out / "collection_errors.csv", error_rows)
    report = {
        "status": "PASS" if not failures else "FAIL",
        "shard": args.shard,
        "shard_count": args.shard_count,
        "universe_contracts": len(universe),
        "selected_contracts": len(selected),
        "complete_contracts": sum(bool(row["pagination_exhausted"]) for row in summary_rows),
        "all_transfer_rows": all_count,
        "mint_transfer_rows": mint_count,
        "burn_transfer_rows": burn_count,
        "failures": failures,
        "production_approved_wallets": 0,
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
