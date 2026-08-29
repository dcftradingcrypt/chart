#!/usr/bin/env python3
from __future__ import annotations

import argparse
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

BASES = (
    "https://robinhoodchain.blockscout.com/api/v2",
    "https://explorer.hoodmarketcap.com/api/v2",
)
TOKEN_TYPES = ("ERC-721", "ERC-1155")
UA = "RHC-NFT-Population/1.0"


def request_json(url: str, attempts: int = 7) -> dict[str, Any]:
    last: Exception | None = None
    for attempt in range(attempts):
        time.sleep(0.15 + random.random() * 0.25)
        req = urllib.request.Request(url, headers={"Accept": "application/json", "User-Agent": UA})
        try:
            with urllib.request.urlopen(req, timeout=75) as response:
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            last = exc
            if exc.code not in (408, 425, 429, 500, 502, 503, 504):
                raise
        except Exception as exc:  # noqa: BLE001
            last = exc
        time.sleep(min(60.0, 2 ** min(attempt, 5) + random.random() * 3))
    raise RuntimeError(f"request failed: {url}: {last}")


def address_of(row: dict[str, Any]) -> str | None:
    for value in (
        row.get("address_hash"),
        row.get("address"),
        (row.get("address") or {}).get("hash") if isinstance(row.get("address"), dict) else None,
        (row.get("token") or {}).get("address_hash") if isinstance(row.get("token"), dict) else None,
    ):
        if isinstance(value, str) and value.startswith("0x") and len(value) == 42:
            return value.lower()
    return None


def paginate(base: str, token_type: str) -> tuple[list[dict[str, Any]], int]:
    rows: list[dict[str, Any]] = []
    params: dict[str, Any] = {"type": token_type}
    seen: set[str] = set()
    for page in range(1, 10001):
        query = urllib.parse.urlencode(params, doseq=True)
        data = request_json(f"{base}/tokens?{query}")
        items = data.get("items")
        if not isinstance(items, list):
            raise RuntimeError(f"invalid token page from {base}: {data}")
        rows.extend(item for item in items if isinstance(item, dict))
        nxt = data.get("next_page_params")
        if not nxt:
            return rows, page
        if not isinstance(nxt, dict):
            raise RuntimeError(f"invalid next_page_params from {base}: {nxt!r}")
        params = {"type": token_type, **nxt}
        key = json.dumps(params, sort_keys=True, separators=(",", ":"))
        if key in seen:
            raise RuntimeError(f"repeated pagination key: {base}: {key}")
        seen.add(key)
    raise RuntimeError(f"pagination limit exceeded: {base}: {token_type}")


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)

    observations: list[dict[str, Any]] = []
    status: list[dict[str, Any]] = []
    successful_types: set[str] = set()
    for token_type in TOKEN_TYPES:
        type_success = False
        for base in BASES:
            try:
                rows, pages = paginate(base, token_type)
                type_success = True
                successful_types.add(token_type)
                status.append({"base": base, "token_type": token_type, "status": "PASS", "pages": pages, "rows": len(rows)})
                for row in rows:
                    address = address_of(row)
                    if not address:
                        continue
                    observations.append(
                        {
                            "contract_address": address,
                            "token_type": token_type,
                            "source": base,
                            "name": row.get("name"),
                            "symbol": row.get("symbol"),
                            "total_supply": row.get("total_supply"),
                            "holders": row.get("holders") or row.get("holders_count"),
                            "decimals": row.get("decimals"),
                            "exchange_rate": row.get("exchange_rate"),
                            "raw": row,
                        }
                    )
            except Exception as exc:  # noqa: BLE001
                status.append({"base": base, "token_type": token_type, "status": "FAIL", "error": repr(exc)})
        if not type_success:
            raise SystemExit(f"no complete source for token type {token_type}")

    contracts: dict[str, dict[str, Any]] = {}
    source_sets: dict[str, set[str]] = {}
    for row in observations:
        address = row["contract_address"]
        source_sets.setdefault(address, set()).add(row["source"])
        current = contracts.setdefault(
            address,
            {
                "contract_address": address,
                "token_types": set(),
                "names": set(),
                "symbols": set(),
                "total_supply_values": set(),
                "holders_values": set(),
            },
        )
        current["token_types"].add(row["token_type"])
        if row.get("name"):
            current["names"].add(str(row["name"]))
        if row.get("symbol"):
            current["symbols"].add(str(row["symbol"]))
        if row.get("total_supply") is not None:
            current["total_supply_values"].add(str(row["total_supply"]))
        if row.get("holders") is not None:
            current["holders_values"].add(str(row["holders"]))

    normalized = []
    for address, row in sorted(contracts.items()):
        normalized.append(
            {
                "contract_address": address,
                "token_types": sorted(row["token_types"]),
                "names": sorted(row["names"]),
                "symbols": sorted(row["symbols"]),
                "total_supply_values": sorted(row["total_supply_values"]),
                "holders_values": sorted(row["holders_values"]),
                "sources": sorted(source_sets[address]),
            }
        )

    with (args.out / "contracts.jsonl").open("w", encoding="utf-8", newline="\n") as handle:
        for row in normalized:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
    with (args.out / "contracts.csv").open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["contract_address", "token_types", "names", "symbols", "total_supply_values", "holders_values", "sources"])
        writer.writeheader()
        for row in normalized:
            writer.writerow({key: json.dumps(value, ensure_ascii=False, sort_keys=True) if isinstance(value, list) else value for key, value in row.items()})
    (args.out / "source_observations.jsonl").write_text(
        "".join(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n" for row in observations),
        encoding="utf-8",
    )
    (args.out / "source_status.json").write_text(json.dumps(status, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    validation = {
        "status": "PASS" if normalized and successful_types == set(TOKEN_TYPES) else "FAIL",
        "contract_count": len(normalized),
        "erc721_observations": sum(row["token_type"] == "ERC-721" for row in observations),
        "erc1155_observations": sum(row["token_type"] == "ERC-1155" for row in observations),
        "token_types_with_complete_source": sorted(successful_types),
        "production_approved_wallets": 0,
    }
    (args.out / "VALIDATION.json").write_text(json.dumps(validation, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    manifest = []
    for path in sorted(args.out.iterdir()):
        if path.is_file():
            manifest.append({"path": path.name, "bytes": path.stat().st_size, "sha256": sha256(path)})
    (args.out / "MANIFEST.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(validation, sort_keys=True))
    if validation["status"] != "PASS":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
