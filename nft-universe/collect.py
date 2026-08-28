#!/usr/bin/env python3
from __future__ import annotations

import csv
import hashlib
import json
import random
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

OUT = Path("out-nft-universe")
OUT.mkdir(parents=True, exist_ok=True)
UA = "RHC-NFT-Universe/1.0 read-only"
ADDRESS_RE = re.compile(r"^0x[a-f0-9]{40}$")
SOURCES = [
    "https://robinhoodchain.blockscout.com/api/v2",
    "https://explorer.hoodmarketcap.com/api/v2",
]
STANDARDS = ["ERC-721", "ERC-1155"]


def get_json(url: str, params: dict[str, Any] | None = None, attempts: int = 10) -> Any:
    if params:
        url += ("&" if "?" in url else "?") + urllib.parse.urlencode(params, doseq=True)
    last: Exception | None = None
    for attempt in range(attempts):
        try:
            req = urllib.request.Request(url, headers={"accept": "application/json", "user-agent": UA})
            with urllib.request.urlopen(req, timeout=60) as response:
                body = response.read()
            return json.loads(body.decode("utf-8"))
        except urllib.error.HTTPError as exc:
            last = exc
            if exc.code in {429, 500, 502, 503, 504}:
                time.sleep(min(90, 2 ** min(attempt, 6) + random.random() * 4))
                continue
            raise
        except Exception as exc:
            last = exc
            if attempt + 1 < attempts:
                time.sleep(min(45, 2 ** min(attempt, 5) + random.random() * 3))
                continue
    raise RuntimeError(f"GET failed: {url}: {last!r}")


def address(value: Any) -> str | None:
    if isinstance(value, dict):
        value = value.get("hash") or value.get("address_hash") or value.get("address")
    if value is None:
        return None
    text = str(value).lower()
    return text if ADDRESS_RE.fullmatch(text) else None


def paginate_tokens(source: str, standard: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    params: dict[str, Any] = {"type": standard}
    seen: set[str] = set()
    rows: list[dict[str, Any]] = []
    pages = 0
    while True:
        payload = get_json(f"{source}/tokens", params)
        pages += 1
        if not isinstance(payload, dict) or not isinstance(payload.get("items"), list):
            raise RuntimeError(f"unexpected token payload from {source}: {type(payload)!r}")
        for item in payload["items"]:
            if not isinstance(item, dict):
                continue
            token_address = address(item.get("address") or item.get("address_hash"))
            rows.append({
                "source": source,
                "requested_standard": standard,
                "contract_address": token_address,
                "reported_type": item.get("type") or item.get("token_type"),
                "name": item.get("name"),
                "symbol": item.get("symbol"),
                "total_supply_raw": item.get("total_supply"),
                "decimals": item.get("decimals"),
                "holders_count": item.get("holders_count") or item.get("holder_count"),
                "exchange_rate": item.get("exchange_rate"),
                "circulating_market_cap": item.get("circulating_market_cap"),
                "icon_url": item.get("icon_url"),
                "raw_json": item,
            })
        nxt = payload.get("next_page_params")
        if not nxt:
            return rows, {
                "source": source,
                "standard": standard,
                "status": "PASS",
                "pages": pages,
                "rows": len(rows),
                "pagination_exhausted": True,
            }
        if not isinstance(nxt, dict):
            raise RuntimeError(f"invalid next_page_params from {source}: {nxt!r}")
        key = json.dumps(nxt, sort_keys=True, separators=(",", ":"))
        if key in seen:
            raise RuntimeError(f"repeated pagination cursor from {source}: {key}")
        seen.add(key)
        params = {"type": standard, **nxt}
        if pages >= 10000:
            raise RuntimeError(f"page limit exceeded for {source} {standard}")
        time.sleep(0.35)


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
            writer.writerow({
                key: json.dumps(value, ensure_ascii=False, sort_keys=True)
                if isinstance(value, (dict, list, tuple)) else value
                for key, value in row.items()
            })


def main() -> None:
    source_rows: list[dict[str, Any]] = []
    coverage: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    for source in SOURCES:
        for standard in STANDARDS:
            try:
                rows, meta = paginate_tokens(source, standard)
                source_rows.extend(rows)
                coverage.append(meta)
                print(meta, flush=True)
            except Exception as exc:
                error = {"source": source, "standard": standard, "status": "FAIL", "error": repr(exc)}
                errors.append(error)
                coverage.append(error)
                print(error, flush=True)

    valid_rows = [row for row in source_rows if row.get("contract_address")]
    by_address: dict[str, dict[str, Any]] = {}
    source_sets: dict[tuple[str, str], set[str]] = {}
    for row in valid_rows:
        key = (row["source"], row["requested_standard"])
        source_sets.setdefault(key, set()).add(row["contract_address"])
        current = by_address.setdefault(row["contract_address"], {
            "contract_address": row["contract_address"],
            "reported_standards": set(),
            "sources": set(),
            "names": set(),
            "symbols": set(),
            "max_holders_count": 0,
            "total_supply_values": set(),
        })
        current["reported_standards"].add(str(row.get("reported_type") or row["requested_standard"]))
        current["sources"].add(row["source"])
        if row.get("name"):
            current["names"].add(str(row["name"]))
        if row.get("symbol"):
            current["symbols"].add(str(row["symbol"]))
        try:
            current["max_holders_count"] = max(current["max_holders_count"], int(row.get("holders_count") or 0))
        except Exception:
            pass
        if row.get("total_supply_raw") is not None:
            current["total_supply_values"].add(str(row["total_supply_raw"]))

    universe: list[dict[str, Any]] = []
    for value in by_address.values():
        universe.append({
            "contract_address": value["contract_address"],
            "reported_standards": sorted(value["reported_standards"]),
            "sources": sorted(value["sources"]),
            "source_count": len(value["sources"]),
            "names": sorted(value["names"]),
            "symbols": sorted(value["symbols"]),
            "max_holders_count": value["max_holders_count"],
            "total_supply_values": sorted(value["total_supply_values"]),
            "classification_status": "NOT_CLASSIFIED",
            "decision_use": "CONTRACT_UNIVERSE_ONLY",
        })
    universe.sort(key=lambda row: row["contract_address"])

    comparisons: list[dict[str, Any]] = []
    for standard in STANDARDS:
        a = source_sets.get((SOURCES[0], standard), set())
        b = source_sets.get((SOURCES[1], standard), set())
        comparisons.append({
            "standard": standard,
            "source_a_count": len(a),
            "source_b_count": len(b),
            "intersection_count": len(a & b),
            "only_source_a_count": len(a - b),
            "only_source_b_count": len(b - a),
            "sets_equal": a == b,
            "only_source_a": sorted(a - b),
            "only_source_b": sorted(b - a),
        })

    failures: list[dict[str, Any]] = []
    passed_coverage = [row for row in coverage if row.get("status") == "PASS"]
    if not universe:
        failures.append({"code": "EMPTY_NFT_CONTRACT_UNIVERSE"})
    if not any("721" in "|".join(row["reported_standards"]).upper() for row in universe):
        failures.append({"code": "NO_ERC721_CONTRACTS"})
    if not any("1155" in "|".join(row["reported_standards"]).upper() for row in universe):
        failures.append({"code": "NO_ERC1155_CONTRACTS"})
    for standard in STANDARDS:
        if not any(row.get("status") == "PASS" and row.get("standard") == standard for row in coverage):
            failures.append({"code": "NO_COMPLETE_SOURCE_FOR_STANDARD", "standard": standard})
    if len({row["contract_address"] for row in universe}) != len(universe):
        failures.append({"code": "DUPLICATE_CONTRACT_IN_UNIVERSE"})
    if any(not ADDRESS_RE.fullmatch(row["contract_address"]) for row in universe):
        failures.append({"code": "INVALID_CONTRACT_ADDRESS"})

    write_csv(OUT / "source_token_rows.csv", source_rows)
    write_csv(OUT / "nft_contract_universe.csv", universe)
    write_csv(OUT / "source_coverage.csv", coverage)
    write_csv(OUT / "source_comparison.csv", comparisons)
    write_csv(OUT / "collection_errors.csv", errors)
    report = {
        "status": "PASS" if not failures else "FAIL",
        "unique_nft_contracts": len(universe),
        "source_rows": len(source_rows),
        "complete_source_standard_pairs": len(passed_coverage),
        "source_errors": len(errors),
        "failures": failures,
        "production_approved_wallets": 0,
        "decision_use": "PROJECT_OPPORTUNITY_UNIVERSE_INPUT",
    }
    (OUT / "VALIDATION.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    manifest = []
    for path in sorted(OUT.iterdir()):
        if path.is_file():
            manifest.append({"path": path.name, "bytes": path.stat().st_size, "sha256": hashlib.sha256(path.read_bytes()).hexdigest()})
    (OUT / "MANIFEST.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, sort_keys=True), flush=True)
    if failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
