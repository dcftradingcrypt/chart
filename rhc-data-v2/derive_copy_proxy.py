#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import statistics
from collections import defaultdict
from pathlib import Path
from typing import Any

from abi import integer
from topics import ZERO_ADDRESS

NATIVE = ZERO_ADDRESS
WETH_RHC = "0x0bd7d308f8e1639fab988df18a8011f41eacad73"
DELAYS = {
    "NEXT_BLOCK": {"blocks": 1, "seconds": 0},
    "30S": {"blocks": 0, "seconds": 30},
    "60S": {"blocks": 0, "seconds": 60},
}
WINDOWS = {"2H": 7200, "24H": 86400}


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fields: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for key in row:
            if key not in seen:
                seen.add(key)
                fields.append(key)
    if not fields:
        fields = ["empty"]
    with path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({key: json.dumps(value, ensure_ascii=False, sort_keys=True) if isinstance(value, (dict, list, tuple)) else value for key, value in row.items()})


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def one(root: Path, name: str) -> Path:
    matches = list(root.rglob(name))
    if len(matches) != 1:
        raise SystemExit(f"expected one {name}, got {matches}")
    return matches[0]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--core", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    core = Path(args.core)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    validation = json.loads(one(core, "VALIDATION.json").read_text(encoding="utf-8"))
    if validation.get("status") != "PASS":
        raise SystemExit(json.dumps({"code": "CORE_DATA_NOT_PASS", "validation": validation}, sort_keys=True))

    primary = read_csv(one(core, "primary_mint_events.csv"))
    entries = read_csv(one(core, "wallet_project_entries.csv"))
    sales = read_csv(one(core, "seaport_sale_items.csv"))

    public_by_project: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in primary:
        if (
            row.get("route") == "SEADROP"
            and str(row.get("is_public", "")).lower() in {"true", "1"}
            and str(row.get("is_paid", "")).lower() in {"true", "1"}
            and str(row.get("is_self_funded", "")).lower() in {"true", "1"}
            and integer(row.get("receipt_status"), -1) == 1
            and str(row.get("mint_transfer_match", "")).lower() in {"true", "1"}
        ):
            public_by_project[row.get("contract", "").lower()].append(row)
    for rows in public_by_project.values():
        rows.sort(key=lambda row: (integer(row.get("block_number")), integer(row.get("transaction_index")), integer(row.get("log_index"))))

    sales_by_project: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in sales:
        if (
            row.get("sale_proof_status") == "PROVEN"
            and str(row.get("single_project_allocation", "")).lower() in {"true", "1"}
            and row.get("payment_token") in {NATIVE, WETH_RHC}
            and row.get("allocated_seller_net_raw") not in (None, "")
        ):
            sales_by_project[row.get("nft_contract", "").lower()].append(row)
    for rows in sales_by_project.values():
        rows.sort(key=lambda row: (integer(row.get("timestamp_unix")), integer(row.get("block_number")), integer(row.get("log_index"))))

    signals = [row for row in entries if str(row.get("selection_signal_eligible", "")).lower() in {"true", "1"}]
    proxy_events: list[dict[str, Any]] = []
    for signal in signals:
        project = signal.get("contract", "").lower()
        signal_block = integer(signal.get("block_number"))
        signal_time = integer(signal.get("timestamp_unix"))
        signal_log = integer(signal.get("log_index"))
        signal_price = float(signal.get("unit_price_raw") or signal.get("unit_price") or 0)
        for delay_name, delay in DELAYS.items():
            candidates = []
            for event in public_by_project.get(project, []):
                event_position = (integer(event.get("block_number")), integer(event.get("log_index")))
                signal_position = (signal_block, signal_log)
                if event_position <= signal_position:
                    continue
                if integer(event.get("block_number")) < signal_block + delay["blocks"]:
                    continue
                if integer(event.get("timestamp_unix")) < signal_time + delay["seconds"]:
                    continue
                if float(event.get("unit_price_raw") or event.get("unit_price") or 0) != signal_price:
                    continue
                candidates.append(event)
            copy_entry = candidates[0] if candidates else None
            row: dict[str, Any] = {
                "signal_wallet": signal.get("minter", "").lower(),
                "project_id": project,
                "signal_transaction_hash": signal.get("transaction_hash", "").lower(),
                "signal_block_number": signal_block,
                "signal_timestamp_unix": signal_time,
                "signal_unit_price_raw": signal_price,
                "delay_mode": delay_name,
                "copy_entry_proven_available": copy_entry is not None,
                "copy_entry_transaction_hash": copy_entry.get("transaction_hash", "").lower() if copy_entry else None,
                "copy_entry_wallet": copy_entry.get("minter", "").lower() if copy_entry else None,
                "copy_entry_block_number": integer(copy_entry.get("block_number")) if copy_entry else None,
                "copy_entry_timestamp_unix": integer(copy_entry.get("timestamp_unix")) if copy_entry else None,
                "copy_entry_unit_price_raw": float(copy_entry.get("unit_price_raw") or copy_entry.get("unit_price") or 0) if copy_entry else None,
                "copy_entry_gas_per_unit_wei": integer(copy_entry.get("gas_cost_wei")) / max(1, integer(copy_entry.get("quantity"))) if copy_entry else None,
                "copy_assessment_status": "MARKET_EXIT_PROXY_ONLY_NOT_COPY_ALPHA",
                "production_approved": False,
            }
            for window_name, seconds in WINDOWS.items():
                if copy_entry is None:
                    row[f"independent_orders_{window_name}"] = 0
                    row[f"independent_buyers_{window_name}"] = 0
                    row[f"median_seller_net_per_unit_raw_{window_name}"] = None
                    row[f"market_exit_nonloss_proxy_{window_name}"] = None
                    continue
                entry_time = integer(copy_entry.get("timestamp_unix"))
                eligible = [
                    sale for sale in sales_by_project.get(project, [])
                    if entry_time <= integer(sale.get("timestamp_unix")) <= entry_time + seconds
                ]
                orders = {(sale.get("transaction_hash"), sale.get("order_hash")) for sale in eligible}
                buyers = {sale.get("buyer") for sale in eligible if sale.get("buyer")}
                net_values = [float(sale["allocated_seller_net_raw"]) / max(1, integer(sale.get("nft_amount"))) for sale in eligible]
                median_net = statistics.median(net_values) if net_values else None
                total_entry = float(row["copy_entry_unit_price_raw"]) + float(row["copy_entry_gas_per_unit_wei"])
                row[f"independent_orders_{window_name}"] = len(orders)
                row[f"independent_buyers_{window_name}"] = len(buyers)
                row[f"median_seller_net_per_unit_raw_{window_name}"] = median_net
                row[f"market_exit_nonloss_proxy_{window_name}"] = bool(median_net is not None and median_net >= total_entry and len(orders) >= 3 and len(buyers) >= 3)
            proxy_events.append(row)

    metrics: list[dict[str, Any]] = []
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in proxy_events:
        grouped[(row["signal_wallet"], row["delay_mode"])].append(row)
    for (wallet, delay), rows in grouped.items():
        available = [row for row in rows if row["copy_entry_proven_available"]]
        metrics.append({
            "wallet": wallet,
            "delay_mode": delay,
            "signals": len(rows),
            "proven_copy_entry_events": len(available),
            "proven_copy_entry_rate": len(available) / len(rows) if rows else None,
            "market_exit_nonloss_proxy_rate_2H": sum(row.get("market_exit_nonloss_proxy_2H") is True for row in available) / len(available) if available else None,
            "market_exit_nonloss_proxy_rate_24H": sum(row.get("market_exit_nonloss_proxy_24H") is True for row in available) / len(available) if available else None,
            "classification": "MARKET_EXIT_PROXY_ONLY_NOT_COPY_ALPHA",
            "historical_executable_bid_data_available": False,
            "production_approved": False,
        })

    write_csv(out / "copy_market_exit_proxy_events.csv", proxy_events)
    write_csv(out / "wallet_copy_market_exit_proxy.csv", metrics)
    validation_out = {
        "status": "PASS",
        "selection_signal_rows": len(signals),
        "proxy_event_rows": len(proxy_events),
        "wallet_delay_metric_rows": len(metrics),
        "proven_copy_entry_events": sum(row["copy_entry_proven_available"] for row in proxy_events),
        "historical_executable_bid_data_available": False,
        "copy_alpha_status": "NOT_AVAILABLE_MARKET_EXIT_PROXY_ONLY",
        "production_approved_wallets": 0,
        "deepseek_handoff_allowed": False,
    }
    (out / "VALIDATION.json").write_text(json.dumps(validation_out, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    manifest = []
    for path in sorted(out.iterdir()):
        if path.is_file():
            manifest.append({"path": path.name, "bytes": path.stat().st_size, "sha256": sha256(path)})
    (out / "MANIFEST.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(validation_out, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
