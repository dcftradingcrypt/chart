#!/usr/bin/env python3
from __future__ import annotations

import csv
import hashlib
import json
import shutil
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

ROOT = Path("downloaded")
OUT = Path("out-consolidated")
OUT.mkdir(parents=True, exist_ok=True)


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


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
            writer.writerow({
                key: json.dumps(value, ensure_ascii=False, sort_keys=True)
                if isinstance(value, (dict, list, tuple)) else value
                for key, value in row.items()
            })


def integer(value: Any) -> int:
    try:
        return int(float(value or 0))
    except Exception:
        return 0


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def locate(artifact: str, basename: str) -> Path | None:
    base = ROOT / artifact
    matches = list(base.rglob(basename)) if base.exists() else []
    return matches[0] if matches else None


def main() -> None:
    expected_artifacts = ["rhc-global-seadrop", "rhc-global-seaport"] + [f"rhc-canonical-wallet-{i}" for i in range(16)]
    missing_artifacts = [name for name in expected_artifacts if not (ROOT / name).exists()]

    global_validations: dict[str, Any] = {}
    global_events: list[dict[str, Any]] = []
    for target in ("seadrop", "seaport"):
        artifact = f"rhc-global-{target}"
        validation_path = locate(artifact, "VALIDATION.json")
        events_path = locate(artifact, f"{target}_events.csv")
        if validation_path:
            global_validations[target] = read_json(validation_path)
        if events_path:
            for row in read_csv(events_path):
                row["_artifact"] = artifact
                global_events.append(row)

    wallet_summaries: list[dict[str, Any]] = []
    wallet_logs: list[dict[str, Any]] = []
    wallet_coverage: list[dict[str, Any]] = []
    known_transactions: list[dict[str, Any]] = []
    collector_errors: list[dict[str, Any]] = []
    shard_validations: list[dict[str, Any]] = []
    for shard in range(16):
        artifact = f"rhc-canonical-wallet-{shard}"
        for basename, target in (
            ("wallet_summary.csv", wallet_summaries),
            ("canonical_nft_transfers.csv", wallet_logs),
            ("coverage.csv", wallet_coverage),
            ("known_transaction_evidence.csv", known_transactions),
            ("errors.csv", collector_errors),
        ):
            path = locate(artifact, basename)
            if path:
                for row in read_csv(path):
                    row["_artifact"] = artifact
                    target.append(row)
        validation_path = locate(artifact, "VALIDATION.json")
        if validation_path:
            value = read_json(validation_path)
            value["_artifact"] = artifact
            shard_validations.append(value)

    # De-duplicate canonical logs. A wallet can appear on both incoming and outgoing paths only in self-transfers.
    dedup_logs: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    for row in wallet_logs:
        key = (
            row.get("wallet", "").lower(),
            row.get("block_hash", "").lower(),
            row.get("transaction_hash", "").lower(),
            str(row.get("log_index", "")),
        )
        dedup_logs[key] = row
    wallet_logs = sorted(dedup_logs.values(), key=lambda row: (row.get("wallet", ""), integer(row.get("block_number")), integer(row.get("log_index"))))

    # Wallet-level completeness classifications.
    remediation: list[dict[str, Any]] = []
    unique_wallets: dict[str, dict[str, Any]] = {}
    for row in wallet_summaries:
        wallet = row.get("wallet", "").lower()
        if wallet:
            if wallet in unique_wallets:
                remediation.append({"wallet": wallet, "priority": row.get("priority"), "reason": "DUPLICATE_WALLET_SUMMARY", "action": "DEDUP_AND_AUDIT"})
            unique_wallets[wallet] = row
        expected = str(row.get("expected_onchain_activity", "")).lower() in {"true", "1", "yes"}
        transfer_rows = integer(row.get("canonical_nft_transfer_rows"))
        coverage_failures = integer(row.get("coverage_failures"))
        if coverage_failures:
            remediation.append({"wallet": wallet, "priority": row.get("priority"), "reason": "RPC_COVERAGE_FAILURE", "action": "RETRY_FAILED_BLOCK_RANGES_ONLY"})
        elif expected and transfer_rows == 0:
            remediation.append({"wallet": wallet, "priority": row.get("priority"), "reason": "EXPECTED_ACTIVITY_NOT_FOUND_BY_CANONICAL_RPC", "action": "VERIFY_SOURCE_OBSERVATION_TX_OR_RECLASSIFY_SOURCE_ONLY"})

    for row in collector_errors:
        if row.get("empty"):
            continue
        remediation.append({
            "wallet": row.get("wallet"),
            "priority": None,
            "reason": row.get("stage") or "COLLECTOR_ERROR",
            "action": "RETRY_OR_MANUAL_CANONICAL_RECONCILIATION",
            "detail": row.get("error") or row,
        })

    # Known transaction evidence must be present and successful.
    for row in known_transactions:
        if str(row.get("transaction_present", "")).lower() not in {"true", "1"} or str(row.get("receipt_present", "")).lower() not in {"true", "1"} or integer(row.get("receipt_status")) != 1:
            remediation.append({
                "wallet": row.get("wallet"),
                "priority": row.get("priority"),
                "reason": "KNOWN_TRANSACTION_NOT_CANONICAL_SUCCESS",
                "action": "REQUERY_EXACT_TX_AND_RECEIPT",
                "detail": row.get("transaction_hash"),
            })

    priority_counts = Counter((row.get("priority") or "UNKNOWN") for row in unique_wallets.values())
    status_counts = Counter((row.get("status") or "UNKNOWN") for row in unique_wallets.values())
    global_ok = all(global_validations.get(target, {}).get("status") == "PASS" for target in ("seadrop", "seaport"))
    fixed_heads = {target: global_validations.get(target, {}).get("fixed_head_block") for target in ("seadrop", "seaport")}
    expected_wallet_count = 232
    wallet_set_ok = len(unique_wallets) == expected_wallet_count and priority_counts.get("P0", 0) == 1 and priority_counts.get("P1", 0) == 19 and priority_counts.get("P2", 0) == 212
    complete = not missing_artifacts and global_ok and wallet_set_ok and not remediation

    write_csv(OUT / "global_events.csv", global_events)
    write_csv(OUT / "wallet_summary.csv", list(unique_wallets.values()))
    write_csv(OUT / "canonical_nft_transfers.csv", wallet_logs)
    write_csv(OUT / "wallet_coverage.csv", wallet_coverage)
    write_csv(OUT / "known_transaction_evidence.csv", known_transactions)
    write_csv(OUT / "collector_errors.csv", collector_errors)
    write_csv(OUT / "REMEDIATION_QUEUE.csv", remediation)
    (OUT / "shard_validations.json").write_text(json.dumps(shard_validations, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")

    report = {
        "status": "PASS" if complete else "PARTIAL_REMEDIATION_REQUIRED",
        "expected_artifacts": len(expected_artifacts),
        "missing_artifacts": missing_artifacts,
        "global_event_validations": global_validations,
        "global_event_rows": len(global_events),
        "fixed_heads": fixed_heads,
        "wallets_expected": expected_wallet_count,
        "wallets_observed": len(unique_wallets),
        "priority_counts": dict(priority_counts),
        "wallet_status_counts": dict(status_counts),
        "canonical_nft_transfer_rows": len(wallet_logs),
        "known_transaction_evidence_rows": len(known_transactions),
        "collector_error_rows": len([row for row in collector_errors if not row.get("empty")]),
        "remediation_rows": len(remediation),
        "wallet_set_ok": wallet_set_ok,
        "global_history_complete": global_ok,
        "data_collection_complete_for_wallet_verification": complete,
        "deepseek_handoff_allowed": False,
        "production_approved_wallets": 0,
    }
    (OUT / "DATA_COMPLETENESS.json").write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")

    manifest = []
    for path in sorted(OUT.iterdir()):
        if path.is_file():
            manifest.append({"path": path.name, "bytes": path.stat().st_size, "sha256": sha256(path)})
    (OUT / "MANIFEST.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(report, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
