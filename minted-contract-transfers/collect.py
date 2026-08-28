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
UA = "RHC-Minted-Contract-Transfers/1.0 read-only"
ZERO = "0x0000000000000000000000000000000000000000"
ADDRESS_RE = re.compile(r"^0x[a-f0-9]{40}$")


def request_json(url: str, params: dict[str, Any] | None = None, attempts: int = 12) -> Any:
    if params:
        url += ("&" if "?" in url else "?") + urllib.parse.urlencode(params, doseq=True)
    last: Exception | None = None
    for attempt in range(attempts):
        try:
            req = urllib.request.Request(url, headers={"accept": "application/json", "user-agent": UA})
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


def addr(value: Any) -> str | None:
    if isinstance(value, dict):
        value = value.get("hash") or value.get("address_hash") or value.get("address")
    if not value:
        return None
    text = str(value).lower()
    return text if ADDRESS_RE.fullmatch(text) else None


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


def paginate(source: str, contract: str) -> Iterator[tuple[int, list[dict[str, Any]], dict[str, Any] | None]]:
    params: dict[str, Any] = {}
    seen: set[str] = set()
    for page in range(1, 100001):
        payload = request_json(f"{source}/tokens/{contract}/transfers", params or None)
        if not isinstance(payload, dict) or not isinstance(payload.get("items"), list):
            raise RuntimeError(f"unexpected transfer payload: {payload!r}")
        items = [row for row in payload["items"] if isinstance(row, dict)]
        nxt = payload.get("next_page_params")
        yield page, items, nxt if isinstance(nxt, dict) else None
        if not nxt:
            return
        if not isinstance(nxt, dict):
            raise RuntimeError(f"invalid next_page_params: {nxt!r}")
        key = json.dumps(nxt, sort_keys=True, separators=(",", ":"))
        if key in seen:
            raise RuntimeError(f"repeated pagination cursor: {key}")
        seen.add(key)
        params = nxt
        time.sleep(0.3)
    raise RuntimeError(f"page limit exceeded for {contract}")


def normalize(contract: str, item: dict[str, Any], source: str) -> dict[str, Any]:
    token = item.get("token") or {}
    tx_hash = pick(item, "transaction_hash", "tx_hash", "transaction.hash", "hash")
    if isinstance(tx_hash, dict):
        tx_hash = tx_hash.get("hash")
    from_address = addr(pick(item, "from", "from_address", "from.hash"))
    to_address = addr(pick(item, "to", "to_address", "to.hash"))
    token_id = pick(item, "token_id", "tokenId", "total.token_id", "id")
    amount = pick(item, "amount", "total.value", "value")
    event_kind = "TRANSFER"
    if from_address == ZERO:
        event_kind = "MINT"
    elif to_address == ZERO:
        event_kind = "BURN"
    return {
        "source": source,
        "contract_address": contract,
        "reported_standard": pick(token if isinstance(token, dict) else {}, "type", "standard") or pick(item, "type", "token_type"),
        "transaction_hash": str(tx_hash).lower() if tx_hash else None,
        "log_index": intish(pick(item, "log_index", "logIndex", "index")),
        "block_number": intish(pick(item, "block_number", "blockNumber", "block")),
        "timestamp_utc": pick(item, "timestamp", "block_timestamp"),
        "from_address": from_address,
        "to_address": to_address,
        "token_id": str(token_id) if token_id is not None else None,
        "amount_raw": str(amount) if amount is not None else None,
        "method": pick(item, "method", "method_name"),
        "event_kind": event_kind,
        "raw_json": item,
    }


def load_contracts(path: Path) -> list[str]:
    if path.suffix.lower() == ".json":
        data = json.loads(path.read_text(encoding="utf-8"))
        values = data if isinstance(data, list) else data.get("contracts", [])
        contracts = [str(value.get("contract_address") if isinstance(value, dict) else value).lower() for value in values]
    else:
        with path.open(newline="", encoding="utf-8-sig") as file:
            rows = list(csv.DictReader(file))
        contracts = [str(row.get("contract_address") or row.get("address") or "").lower() for row in rows]
    return sorted({value for value in contracts if ADDRESS_RE.fullmatch(value)})


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
    parser.add_argument("--contracts", required=True)
    parser.add_argument("--shard", type=int, required=True)
    parser.add_argument("--shard-count", type=int, default=32)
    parser.add_argument("--fixed-head", type=int, required=True)
    parser.add_argument("--fixed-head-hash", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    all_contracts = load_contracts(Path(args.contracts))
    selected = [contract for index, contract in enumerate(all_contracts) if index % args.shard_count == args.shard]

    summaries: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    all_path = out / "all_transfers.jsonl.gz"
    secondary_path = out / "secondary_transfers.jsonl.gz"
    rows_total = secondary_total = 0
    with gzip.open(all_path, "wt", encoding="utf-8") as all_file, gzip.open(secondary_path, "wt", encoding="utf-8") as secondary_file:
        for position, contract in enumerate(selected, 1):
            print({"shard": args.shard, "position": position, "total": len(selected), "contract": contract}, flush=True)
            complete_rows: list[dict[str, Any]] | None = None
            complete_source: str | None = None
            source_errors: list[str] = []
            pages = 0
            for source in SOURCES:
                try:
                    buffered: list[dict[str, Any]] = []
                    candidate_pages = 0
                    for page, items, _ in paginate(source, contract):
                        candidate_pages = page
                        for item in items:
                            row = normalize(contract, item, source)
                            block_number = row.get("block_number")
                            if block_number is None or int(block_number) > args.fixed_head:
                                continue
                            buffered.append(row)
                    complete_rows = buffered
                    complete_source = source
                    pages = candidate_pages
                    break
                except Exception as exc:
                    source_errors.append(f"{source}:{exc!r}")
            if complete_rows is None:
                errors.append({"contract_address": contract, "error": " | ".join(source_errors)})
                summaries.append({"contract_address": contract, "pagination_exhausted": False, "source": None, "source_errors": source_errors, "pages": 0, "transfer_rows": 0, "secondary_transfer_rows": 0})
                continue
            dedup: dict[tuple[Any, ...], dict[str, Any]] = {}
            for row in complete_rows:
                event_key = (
                    row.get("transaction_hash"), row.get("log_index"), row.get("contract_address"),
                    row.get("token_id"), row.get("amount_raw"), row.get("from_address"), row.get("to_address"),
                )
                if event_key in dedup and dedup[event_key] != row:
                    errors.append({"contract_address": contract, "error": "CONFLICTING_DUPLICATE_TRANSFER", "key": event_key})
                dedup[event_key] = row
            ordered = sorted(dedup.values(), key=lambda row: (row.get("block_number") or -1, row.get("transaction_hash") or "", row.get("log_index") or -1))
            secondary_count = 0
            for row in ordered:
                encoded = json.dumps(row, ensure_ascii=False, sort_keys=True)
                all_file.write(encoded + "\n")
                rows_total += 1
                if row.get("event_kind") == "TRANSFER":
                    secondary_file.write(encoded + "\n")
                    secondary_total += 1
                    secondary_count += 1
            summaries.append({
                "contract_address": contract,
                "pagination_exhausted": True,
                "source": complete_source,
                "source_errors": source_errors,
                "pages": pages,
                "transfer_rows": len(ordered),
                "secondary_transfer_rows": secondary_count,
            })

    failures: list[dict[str, Any]] = []
    if len(summaries) != len(selected):
        failures.append({"code": "SUMMARY_COUNT_MISMATCH", "selected": len(selected), "summaries": len(summaries)})
    if errors:
        failures.append({"code": "COLLECTION_ERRORS_PRESENT", "count": len(errors), "sample": errors[:50]})
    if any(not row.get("pagination_exhausted") for row in summaries):
        failures.append({"code": "INCOMPLETE_CONTRACT_HISTORY"})
    write_csv(out / "contract_transfer_summary.csv", summaries)
    write_csv(out / "collection_errors.csv", errors)
    report = {
        "status": "PASS" if not failures else "FAIL",
        "shard": args.shard,
        "shard_count": args.shard_count,
        "fixed_head": args.fixed_head,
        "fixed_head_hash": args.fixed_head_hash.lower(),
        "all_minted_contracts": len(all_contracts),
        "selected_contracts": len(selected),
        "complete_contracts": sum(bool(row.get("pagination_exhausted")) for row in summaries),
        "all_transfer_rows": rows_total,
        "secondary_transfer_rows": secondary_total,
        "failure_count": len(failures),
        "failures": failures,
        "production_approved_wallets": 0,
        "decision_use": "TOKEN_PROVENANCE_AND_NON_SEAPORT_SALE_DISCOVERY",
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
