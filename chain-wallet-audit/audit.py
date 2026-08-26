#!/usr/bin/env python3
from __future__ import annotations

import csv
import hashlib
import json
import random
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

BASE = "https://robinscan.io"
UA = "RHC-Chain-Derived-Wallet-Audit/1.0"
ZERO = "0x0000000000000000000000000000000000000000"
OUT = Path("out-chain-wallet-audit")
OUT.mkdir(parents=True, exist_ok=True)
last_request = 0.0


def pace() -> None:
    global last_request
    wait = 0.25 - (time.monotonic() - last_request)
    if wait > 0:
        time.sleep(wait)


def get_json(path: str, params: dict[str, Any] | None = None, attempts: int = 10) -> tuple[int, Any]:
    global last_request
    url = BASE + path
    if params:
        url += "?" + urllib.parse.urlencode({key: value for key, value in params.items() if value is not None})
    last_error: Exception | None = None
    for attempt in range(attempts):
        pace()
        try:
            request = urllib.request.Request(url, headers={"accept": "application/json", "user-agent": UA})
            with urllib.request.urlopen(request, timeout=60) as response:
                payload = response.read()
                status = response.status
            last_request = time.monotonic()
            return status, json.loads(payload.decode("utf-8"))
        except urllib.error.HTTPError as exc:
            last_request = time.monotonic()
            last_error = exc
            body = exc.read(1000).decode("utf-8", "replace")
            if exc.code in (401, 403, 404):
                return exc.code, {"error": body}
            if exc.code == 429 or exc.code >= 500:
                time.sleep(min(60, 2 ** min(attempt, 6) + random.random() * 4))
                continue
            raise
        except Exception as exc:
            last_error = exc
            if attempt + 1 < attempts:
                time.sleep(min(30, 2 ** min(attempt, 5) + random.random() * 3))
                continue
    raise RuntimeError(f"GET {url} failed: {last_error}")


def paginate(path: str) -> tuple[list[dict[str, Any]], int]:
    rows: list[dict[str, Any]] = []
    cursor: str | None = None
    seen: set[str] = set()
    for page in range(1, 10001):
        status, payload = get_json(path, {"cursor": cursor} if cursor else None)
        if status != 200 or not isinstance(payload, dict):
            raise RuntimeError(f"{path} returned HTTP {status}: {payload}")
        items = payload.get("items") or []
        if not isinstance(items, list):
            raise RuntimeError(f"{path}: items is not a list")
        rows.extend(item for item in items if isinstance(item, dict))
        next_value = payload.get("next") or payload.get("next_cursor") or payload.get("next_page_params")
        if not next_value:
            return rows, page
        if isinstance(next_value, dict):
            cursor = str(next_value.get("cursor") or json.dumps(next_value, separators=(",", ":"), sort_keys=True))
        else:
            cursor = str(next_value)
        if cursor in seen:
            raise RuntimeError(f"{path}: repeated cursor {cursor}")
        seen.add(cursor)
    raise RuntimeError(f"{path}: page limit exceeded")


def address(value: Any) -> str | None:
    if isinstance(value, dict):
        value = value.get("hash") or value.get("address") or value.get("address_hash")
    return str(value).lower() if value else None


def token_address(row: dict[str, Any]) -> str | None:
    token = row.get("token") or {}
    return address(token.get("address_hash") or token.get("address"))


def is_nft_transfer(row: dict[str, Any]) -> bool:
    token_id = row.get("token_id") or row.get("tokenId") or row.get("id")
    token = row.get("token") or {}
    decimals = row.get("decimals") or token.get("decimals")
    text = f"{token.get('symbol') or ''} {token.get('name') or ''}".lower()
    return token_id is not None or str(decimals) in ("0", "None", "") or any(value in text for value in ("nft", "erc721", "erc1155"))


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fields = sorted({key for row in rows for key in row}) if rows else ["empty"]
    with path.open("w", encoding="utf-8-sig", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({key: json.dumps(value, ensure_ascii=False, sort_keys=True) if isinstance(value, (list, dict, tuple)) else value for key, value in row.items()})


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main() -> None:
    with Path("chain-wallet-audit/candidates.csv").open(newline="", encoding="utf-8-sig") as file:
        candidates = list(csv.DictReader(file))

    summaries: list[dict[str, Any]] = []
    tx_rows: list[dict[str, Any]] = []
    transfer_rows: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []

    for candidate in candidates:
        wallet = candidate["wallet"].lower()
        print(wallet, flush=True)
        try:
            counter_status, counters = get_json(f"/api/address/{wallet}/counters")
            if counter_status != 200 or not isinstance(counters, dict):
                raise RuntimeError(f"counters HTTP {counter_status}: {counters}")
            transactions, tx_pages = paginate(f"/api/address/{wallet}/txs")
            transfers, transfer_pages = paginate(f"/api/address/{wallet}/transfers")
            for row in transactions:
                tx_rows.append({"audit_wallet": wallet, **row})
            for row in transfers:
                transfer_rows.append({"audit_wallet": wallet, **row})

            nft_rows = [row for row in transfers if is_nft_transfer(row)]
            mint_receipts = [row for row in nft_rows if address(row.get("from")) == ZERO and address(row.get("to")) == wallet]
            outgoing = [row for row in nft_rows if address(row.get("from")) == wallet and address(row.get("to")) not in (None, ZERO)]
            inbound_secondary = [row for row in nft_rows if address(row.get("to")) == wallet and address(row.get("from")) not in (None, ZERO)]
            contracts = sorted({value for value in (token_address(row) for row in nft_rows) if value})
            summaries.append({
                **candidate,
                "counter_transactions": counters.get("transactions"),
                "counter_token_transfers": counters.get("tokenTransfers"),
                "transaction_rows_fetched": len(transactions),
                "transaction_pages": tx_pages,
                "transfer_rows_fetched": len(transfers),
                "transfer_pages": transfer_pages,
                "nft_like_transfer_rows": len(nft_rows),
                "zero_address_nft_receives": len(mint_receipts),
                "secondary_nft_receives": len(inbound_secondary),
                "outgoing_nft_transfers": len(outgoing),
                "unique_nft_contracts": len(contracts),
                "nft_contracts_json": contracts,
                "audit_status": "PASS",
                "production_approved": False,
                "decision_use": "CHAIN_DERIVED_CANDIDATE_AUDIT_ONLY",
            })
        except Exception as exc:
            errors.append({"wallet": wallet, "error": repr(exc)})
            summaries.append({**candidate, "audit_status": "FAIL", "error": repr(exc), "production_approved": False, "decision_use": "CHAIN_DERIVED_CANDIDATE_AUDIT_ONLY"})

    write_csv(OUT / "wallet_summary.csv", summaries)
    write_csv(OUT / "transactions.csv", tx_rows)
    write_csv(OUT / "transfers.csv", transfer_rows)
    write_csv(OUT / "errors.csv", errors)
    validation = {
        "status": "PASS" if not errors else "PARTIAL",
        "candidate_count": len(candidates),
        "wallets_passed": sum(row.get("audit_status") == "PASS" for row in summaries),
        "wallets_failed": len(errors),
        "transaction_rows": len(tx_rows),
        "transfer_rows": len(transfer_rows),
        "candidate_source": "COMPLETE_CANONICAL_SEADROP_STRICT_PAID_PUBLIC_REPEAT_HISTORY",
        "production_approved_wallets": 0,
    }
    (OUT / "validation.json").write_text(json.dumps(validation, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    manifest = []
    for path in sorted(OUT.iterdir()):
        if path.is_file():
            manifest.append({"path": path.name, "bytes": path.stat().st_size, "sha256": sha256(path)})
    (OUT / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(validation, sort_keys=True), flush=True)
    if validation["status"] != "PASS":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
