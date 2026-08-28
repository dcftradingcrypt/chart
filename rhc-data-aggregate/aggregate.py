#!/usr/bin/env python3
from __future__ import annotations

import csv
import gzip
import hashlib
import io
import json
import os
import shutil
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from pathlib import Path
from typing import Any, Iterable

REPOSITORY = os.environ["GITHUB_REPOSITORY"]
TOKEN = os.environ["GITHUB_TOKEN"]
CURRENT_RUN_ID = int(os.environ["GITHUB_RUN_ID"])
API = f"https://api.github.com/repos/{REPOSITORY}"
FIXED_HEAD = 48_264_433
OUT = Path("rhc-data-stage")
BUNDLE = Path("rhc-data-aggregate-output")
DOWNLOADS = BUNDLE / "source_artifacts"
EXTRACTED = BUNDLE / "extracted"
MERGED = BUNDLE / "merged"

SOURCE_SPECS = {
    "p1_recovery": {
        "branch": "chatgpt/rhc-p1c-missing-20260829",
        "workflow": "RHC P1c missing wallet recovery",
        "artifact_prefix": "rhc-wallet-verification-p1c-recovered",
        "expected_artifacts": 1,
    },
    "p2": {
        "branch": "chatgpt/rhc-wallet-verification-p2-20260829",
        "workflow": "RHC wallet verification P2",
        "artifact_prefix": "rhc-wallet-verification-p2-",
        "expected_artifacts": 16,
    },
    "canonical": {
        "branch": "chatgpt/rhc-canonical-universe-20260829",
        "workflow": "RHC canonical SeaDrop and Seaport universe",
        "artifact_prefix": "rhc-canonical-",
        "expected_artifacts": 32,
    },
    "global_mints": {
        "branch": "chatgpt/rhc-global-mint-universe-20260829",
        "workflow": "RHC complete global NFT mint universe",
        "artifact_prefix": "rhc-global-mint-",
        "expected_artifacts": 48,
    },
}

P1_FIXED_RUN_ID = 33167365485
P1_FIXED_ARTIFACT_NAMES = {
    "rhc-wallet-verification-p0",
    "rhc-wallet-verification-p1a",
    "rhc-wallet-verification-p1b",
    "rhc-wallet-verification-p1c",
    "rhc-wallet-verification-p1d",
}


def canonical_json(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def api_request(path: str, *, attempts: int = 8) -> Any:
    url = path if path.startswith("https://") else API + path
    last_error: Exception | None = None
    for attempt in range(attempts):
        request = urllib.request.Request(
            url,
            headers={
                "Authorization": f"Bearer {TOKEN}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
                "User-Agent": "RHC-Data-Aggregator/1.0",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=120) as response:
                body = response.read()
            return json.loads(body.decode("utf-8"))
        except urllib.error.HTTPError as exc:
            last_error = exc
            if exc.code in (403, 429, 500, 502, 503, 504) and attempt + 1 < attempts:
                time.sleep(min(120, 5 * (2 ** attempt)))
                continue
            detail = exc.read(1000).decode("utf-8", "replace")
            raise RuntimeError(f"GitHub API HTTP {exc.code} {url}: {detail}") from exc
        except Exception as exc:
            last_error = exc
            if attempt + 1 < attempts:
                time.sleep(min(90, 5 * (2 ** attempt)))
                continue
    raise RuntimeError(f"GitHub API exhausted for {url}: {last_error!r}")


def download_binary(url: str, destination: Path, attempts: int = 8) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    last_error: Exception | None = None
    for attempt in range(attempts):
        request = urllib.request.Request(
            url,
            headers={
                "Authorization": f"Bearer {TOKEN}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
                "User-Agent": "RHC-Data-Aggregator/1.0",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=300) as response:
                destination.write_bytes(response.read())
            return
        except Exception as exc:
            last_error = exc
            if attempt + 1 < attempts:
                time.sleep(min(120, 5 * (2 ** attempt)))
                continue
    raise RuntimeError(f"artifact download failed {url}: {last_error!r}")


def list_branch_runs(branch: str, workflow_name: str) -> list[dict[str, Any]]:
    query = urllib.parse.urlencode({"branch": branch, "event": "pull_request", "per_page": 100})
    payload = api_request(f"/actions/runs?{query}")
    rows = [
        row for row in payload.get("workflow_runs", [])
        if row.get("name") == workflow_name
    ]
    return sorted(rows, key=lambda row: int(row["id"]), reverse=True)


def wait_for_successful_run(branch: str, workflow_name: str, timeout_seconds: int = 19_800) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    last_snapshot: dict[str, Any] | None = None
    while time.monotonic() < deadline:
        rows = list_branch_runs(branch, workflow_name)
        if not rows:
            print(f"waiting for workflow creation: {branch} / {workflow_name}", flush=True)
            time.sleep(30)
            continue
        run = rows[0]
        last_snapshot = {
            "id": run["id"],
            "status": run.get("status"),
            "conclusion": run.get("conclusion"),
            "head_sha": run.get("head_sha"),
            "html_url": run.get("html_url"),
        }
        print(json.dumps({"source": branch, **last_snapshot}, sort_keys=True), flush=True)
        if run.get("status") == "completed":
            if run.get("conclusion") != "success":
                raise RuntimeError(f"latest source run did not succeed: {branch}: {last_snapshot}")
            return run
        time.sleep(45)
    raise TimeoutError(f"source run timeout: {branch}/{workflow_name}; last={last_snapshot}")


def list_artifacts(run_id: int) -> list[dict[str, Any]]:
    payload = api_request(f"/actions/runs/{run_id}/artifacts?per_page=100")
    return payload.get("artifacts", [])


def download_and_extract_artifacts(
    source_name: str,
    run_id: int,
    *,
    allowed_names: set[str] | None = None,
    prefix: str | None = None,
    expected_count: int | None = None,
) -> list[dict[str, Any]]:
    artifacts = list_artifacts(run_id)
    selected = []
    for artifact in artifacts:
        name = artifact.get("name", "")
        if allowed_names is not None and name not in allowed_names:
            continue
        if prefix is not None and not name.startswith(prefix):
            continue
        selected.append(artifact)
    selected.sort(key=lambda row: row["name"])
    if expected_count is not None and len(selected) != expected_count:
        raise RuntimeError(
            f"artifact count mismatch for {source_name}: expected {expected_count}, got {len(selected)}: "
            f"{[row['name'] for row in selected]}"
        )

    records = []
    for artifact in selected:
        name = artifact["name"]
        zip_path = DOWNLOADS / source_name / f"{name}.zip"
        extract_path = EXTRACTED / source_name / name
        download_binary(artifact["archive_download_url"], zip_path)
        extract_path.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(zip_path) as archive:
            archive.extractall(extract_path)
        records.append({
            "source": source_name,
            "run_id": run_id,
            "artifact_id": artifact["id"],
            "name": name,
            "bytes": zip_path.stat().st_size,
            "sha256": hashlib.sha256(zip_path.read_bytes()).hexdigest(),
            "extract_path": str(extract_path),
        })
        print(json.dumps(records[-1], sort_keys=True), flush=True)
    return records


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: Iterable[dict[str, Any]]) -> None:
    materialized = list(rows)
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = sorted({key for row in materialized for key in row}) if materialized else ["empty"]
    with path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in materialized:
            writer.writerow({
                key: json.dumps(value, ensure_ascii=False, sort_keys=True)
                if isinstance(value, (dict, list, tuple))
                else value
                for key, value in row.items()
            })


def json_files(root: Path, filename: str) -> list[Path]:
    return sorted(path for path in root.rglob(filename) if path.is_file())


def merge_wallet_summaries() -> dict[str, Any]:
    summary_paths = sorted(EXTRACTED.rglob("wallet_summary.csv"))
    if not summary_paths:
        raise RuntimeError("no wallet_summary.csv files found")

    rows_by_wallet: dict[str, dict[str, str]] = {}
    source_rows: list[dict[str, Any]] = []
    conflicts: list[dict[str, Any]] = []
    for path in summary_paths:
        for row in read_csv(path):
            wallet = row.get("wallet", "").lower()
            if not wallet:
                continue
            enriched = {**row, "wallet": wallet, "source_file": str(path)}
            source_rows.append(enriched)
            previous = rows_by_wallet.get(wallet)
            if previous is None:
                rows_by_wallet[wallet] = enriched
                continue
            previous_score = sum(bool(previous.get(key)) for key in previous)
            current_score = sum(bool(enriched.get(key)) for key in enriched)
            if previous.get("priority") != enriched.get("priority") and "P0" not in {
                previous.get("priority"), enriched.get("priority")
            }:
                conflicts.append({
                    "wallet": wallet,
                    "previous_priority": previous.get("priority"),
                    "current_priority": enriched.get("priority"),
                    "previous_source": previous.get("source_file"),
                    "current_source": enriched.get("source_file"),
                })
            if current_score > previous_score:
                rows_by_wallet[wallet] = enriched

    unique_rows = sorted(rows_by_wallet.values(), key=lambda row: (row.get("priority", ""), row["wallet"]))
    write_csv(MERGED / "wallet_summary_all.csv", unique_rows)
    write_csv(MERGED / "wallet_summary_source_rows.csv", source_rows)
    write_csv(MERGED / "wallet_summary_conflicts.csv", conflicts)

    counts: dict[str, int] = {}
    for row in unique_rows:
        priority = row.get("priority", "UNKNOWN")
        counts[priority] = counts.get(priority, 0) + 1
    required = {"P0": 1, "P1": 19, "P2": 212}
    failures = []
    for priority, expected in required.items():
        if counts.get(priority, 0) != expected:
            failures.append({
                "code": "WALLET_PRIORITY_COUNT_MISMATCH",
                "priority": priority,
                "expected": expected,
                "actual": counts.get(priority, 0),
            })
    if conflicts:
        failures.append({"code": "WALLET_SUMMARY_CONFLICTS", "count": len(conflicts)})
    return {
        "status": "PASS" if not failures else "FAIL",
        "summary_files": len(summary_paths),
        "source_rows": len(source_rows),
        "unique_wallets": len(unique_rows),
        "priority_counts": counts,
        "failures": failures,
    }


def load_gz_jsonl(path: Path) -> Iterable[dict[str, Any]]:
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                value = json.loads(line)
                if not isinstance(value, dict):
                    raise RuntimeError(f"non-object JSONL row in {path}")
                yield value


def event_key(row: dict[str, Any]) -> tuple[str, int]:
    tx_hash = str(row.get("transactionHash") or row.get("transaction_hash") or "").lower()
    raw_index = row.get("logIndex") or row.get("log_index") or "0x0"
    log_index = int(raw_index, 16) if isinstance(raw_index, str) and raw_index.startswith("0x") else int(raw_index)
    return tx_hash, log_index


def block_number(row: dict[str, Any]) -> int:
    raw = row.get("blockNumber") or row.get("block_number")
    return int(raw, 16) if isinstance(raw, str) and raw.startswith("0x") else int(raw)


def merge_event_family(
    source_name: str,
    artifact_prefix: str,
    input_filename: str,
    output_filename: str,
    targets: list[str],
    expected_shards_per_target: int,
) -> dict[str, Any]:
    source_root = EXTRACTED / source_name
    failures: list[dict[str, Any]] = []
    target_summaries: dict[str, Any] = {}

    for target in targets:
        artifact_dirs = sorted(
            path for path in source_root.iterdir()
            if path.is_dir() and path.name.startswith(f"{artifact_prefix}{target}-")
        )
        if len(artifact_dirs) != expected_shards_per_target:
            failures.append({
                "code": "EVENT_ARTIFACT_COUNT_MISMATCH",
                "source": source_name,
                "target": target,
                "expected": expected_shards_per_target,
                "actual": len(artifact_dirs),
            })
            continue

        validations = []
        event_paths = []
        for directory in artifact_dirs:
            validation_paths = json_files(directory, "validation.json")
            if len(validation_paths) != 1:
                failures.append({
                    "code": "VALIDATION_FILE_COUNT",
                    "target": target,
                    "artifact": directory.name,
                    "count": len(validation_paths),
                })
                continue
            validation = json.loads(validation_paths[0].read_text(encoding="utf-8"))
            validations.append(validation)
            if validation.get("status") != "PASS":
                failures.append({
                    "code": "SOURCE_VALIDATION_NOT_PASS",
                    "target": target,
                    "artifact": directory.name,
                    "validation": validation,
                })
            found = sorted(directory.rglob(input_filename))
            if len(found) != 1:
                failures.append({
                    "code": "EVENT_FILE_COUNT",
                    "target": target,
                    "artifact": directory.name,
                    "filename": input_filename,
                    "count": len(found),
                })
            else:
                event_paths.append(found[0])

        intervals = sorted(
            (
                int(value["requested_from_block"]),
                int(value["requested_to_block"]),
            )
            for value in validations
        )
        expected_start = 0
        for left, right in intervals:
            if left != expected_start:
                failures.append({
                    "code": "SHARD_COVERAGE_GAP",
                    "target": target,
                    "expected_from": expected_start,
                    "actual_from": left,
                })
            expected_start = right + 1
        if expected_start != FIXED_HEAD + 1:
            failures.append({
                "code": "SHARD_COVERAGE_END_MISMATCH",
                "target": target,
                "expected": FIXED_HEAD + 1,
                "actual": expected_start,
            })

        rows_by_key: dict[tuple[str, int], dict[str, Any]] = {}
        duplicate_count = 0
        for path in event_paths:
            for row in load_gz_jsonl(path):
                key = event_key(row)
                previous = rows_by_key.get(key)
                if previous is not None:
                    duplicate_count += 1
                    if canonical_json(previous) != canonical_json(row):
                        failures.append({
                            "code": "CONFLICTING_EVENT_DUPLICATE",
                            "target": target,
                            "transaction_hash": key[0],
                            "log_index": key[1],
                        })
                else:
                    rows_by_key[key] = row

        ordered = sorted(
            rows_by_key.values(),
            key=lambda row: (block_number(row), event_key(row)[0], event_key(row)[1]),
        )
        output_path = MERGED / output_filename.format(target=target)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(output_path, "wt", encoding="utf-8", newline="\n") as handle:
            for row in ordered:
                handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n")
        target_summaries[target] = {
            "artifacts": len(artifact_dirs),
            "source_event_files": len(event_paths),
            "unique_events": len(ordered),
            "duplicate_rows_deduplicated": duplicate_count,
            "first_block": block_number(ordered[0]) if ordered else None,
            "last_block": block_number(ordered[-1]) if ordered else None,
            "output": str(output_path),
            "sha256": hashlib.sha256(output_path.read_bytes()).hexdigest(),
        }

    return {
        "status": "PASS" if not failures else "FAIL",
        "source": source_name,
        "targets": target_summaries,
        "failures": failures,
    }


def write_manifest(root: Path, destination: Path) -> None:
    rows = []
    for path in sorted(root.rglob("*")):
        if path.is_file() and path != destination:
            rows.append({
                "path": str(path.relative_to(root)),
                "bytes": path.stat().st_size,
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            })
    destination.write_text(json.dumps(rows, indent=2, sort_keys=True), encoding="utf-8")


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    DOWNLOADS.mkdir(parents=True, exist_ok=True)
    EXTRACTED.mkdir(parents=True, exist_ok=True)
    MERGED.mkdir(parents=True, exist_ok=True)

    source_runs: dict[str, Any] = {}
    artifact_records: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []

    # P0/P1 source run is fixed and immutable. It was cancelled only after
    # four successful shards and one partial shard had uploaded artifacts.
    p1_run = api_request(f"/actions/runs/{P1_FIXED_RUN_ID}")
    source_runs["p1_fixed"] = {
        "id": p1_run["id"],
        "status": p1_run.get("status"),
        "conclusion": p1_run.get("conclusion"),
        "head_sha": p1_run.get("head_sha"),
        "html_url": p1_run.get("html_url"),
    }
    artifact_records.extend(download_and_extract_artifacts(
        "p1_fixed",
        P1_FIXED_RUN_ID,
        allowed_names=P1_FIXED_ARTIFACT_NAMES,
        expected_count=5,
    ))

    for source_name, spec in SOURCE_SPECS.items():
        run = wait_for_successful_run(spec["branch"], spec["workflow"])
        source_runs[source_name] = {
            "id": run["id"],
            "status": run.get("status"),
            "conclusion": run.get("conclusion"),
            "head_sha": run.get("head_sha"),
            "html_url": run.get("html_url"),
        }
        artifact_records.extend(download_and_extract_artifacts(
            source_name,
            int(run["id"]),
            prefix=spec["artifact_prefix"],
            expected_count=spec["expected_artifacts"],
        ))

    write_csv(BUNDLE / "artifact_inventory.csv", artifact_records)

    # Require all explicit per-artifact gates that are present.
    gate_files = sorted(
        path for path in EXTRACTED.rglob("*.json")
        if path.name in {
            "VALIDATION.json",
            "validation.json",
            "P1C_RECOVERY_GATE.json",
            "P2_COMPLETENESS_GATE.json",
        }
    )
    gate_rows = []
    for path in gate_files:
        value = json.loads(path.read_text(encoding="utf-8"))
        gate_rows.append({
            "path": str(path),
            "status": value.get("status"),
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        })
        if value.get("status") != "PASS":
            failures.append({
                "code": "SOURCE_GATE_NOT_PASS",
                "path": str(path),
                "status": value.get("status"),
            })
    write_csv(BUNDLE / "source_gate_inventory.csv", gate_rows)

    wallet_result = merge_wallet_summaries()
    if wallet_result["status"] != "PASS":
        failures.extend(wallet_result["failures"])

    canonical_result = merge_event_family(
        "canonical",
        "rhc-canonical-",
        "events.jsonl.gz",
        "canonical_{target}_events.jsonl.gz",
        ["seadrop", "seaport"],
        16,
    )
    if canonical_result["status"] != "PASS":
        failures.extend(canonical_result["failures"])

    global_result = merge_event_family(
        "global_mints",
        "rhc-global-mint-",
        "nft_mint_events.jsonl.gz",
        "global_{target}_mint_events.jsonl.gz",
        ["erc721", "erc1155_single", "erc1155_batch"],
        16,
    )
    if global_result["status"] != "PASS":
        failures.extend(global_result["failures"])

    aggregate_status = {
        "status": "PASS" if not failures else "FAIL",
        "generated_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "aggregator_workflow_run_id": CURRENT_RUN_ID,
        "repository": REPOSITORY,
        "fixed_head": FIXED_HEAD,
        "source_runs": source_runs,
        "artifact_count": len(artifact_records),
        "gate_file_count": len(gate_files),
        "wallet_result": wallet_result,
        "canonical_result": canonical_result,
        "global_mint_result": global_result,
        "failures": failures,
        "production_approved_wallets": 0,
        "deepseek_handoff": "BLOCKED_DATA_ANALYSIS_NOT_YET_RUN",
    }
    (BUNDLE / "AGGREGATE_STATUS.json").write_text(
        json.dumps(aggregate_status, indent=2, sort_keys=True), encoding="utf-8"
    )
    (OUT / "AGGREGATE_STATUS.json").write_text(
        json.dumps(aggregate_status, indent=2, sort_keys=True), encoding="utf-8"
    )
    (OUT / "SOURCE_RUNS.json").write_text(
        json.dumps(source_runs, indent=2, sort_keys=True), encoding="utf-8"
    )
    write_manifest(BUNDLE, BUNDLE / "MANIFEST.json")
    shutil.copy2(BUNDLE / "MANIFEST.json", OUT / "BUNDLE_MANIFEST.json")
    print(json.dumps(aggregate_status, sort_keys=True), flush=True)
    if failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
