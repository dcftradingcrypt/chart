#!/usr/bin/env python3
from __future__ import annotations

import csv
import gzip
import hashlib
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from pathlib import Path
from typing import Any, Iterable

REPOSITORY = os.environ["GITHUB_REPOSITORY"]
TOKEN = os.environ["GITHUB_TOKEN"]
RUN_ID = int(os.environ["GITHUB_RUN_ID"])
API = f"https://api.github.com/repos/{REPOSITORY}"
SOURCE_BRANCH = "chatgpt/rhc-token-transfer-universe-20260829"
SOURCE_WORKFLOW = "RHC complete per-contract NFT transfer universe"
ARTIFACT_PREFIX = "rhc-token-transfer-universe-"
OUT = Path("rhc-token-transfer-aggregate-output")
STATUS_DIR = Path("rhc-token-transfer-aggregate-stage")


def api(path: str, attempts: int = 8) -> Any:
    url = path if path.startswith("https://") else API + path
    last_error = None
    for attempt in range(attempts):
        req = urllib.request.Request(
            url,
            headers={
                "Authorization": f"Bearer {TOKEN}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
                "User-Agent": "RHC-Token-Transfer-Aggregator/1.0",
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=180) as response:
                return json.loads(response.read().decode("utf-8"))
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
    raise RuntimeError(f"GitHub API exhausted: {url}: {last_error!r}")


def wait_source(timeout: int = 19_800) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        query = urllib.parse.urlencode({"branch": SOURCE_BRANCH, "event": "pull_request", "per_page": 100})
        payload = api(f"/actions/runs?{query}")
        rows = sorted(
            [row for row in payload.get("workflow_runs", []) if row.get("name") == SOURCE_WORKFLOW],
            key=lambda row: int(row["id"]),
            reverse=True,
        )
        if not rows:
            print("waiting for token-transfer source workflow", flush=True)
            time.sleep(30)
            continue
        run = rows[0]
        print(json.dumps({
            "id": run["id"],
            "status": run.get("status"),
            "conclusion": run.get("conclusion"),
            "head_sha": run.get("head_sha"),
        }, sort_keys=True), flush=True)
        if run.get("status") == "completed":
            if run.get("conclusion") != "success":
                raise RuntimeError(f"token-transfer source failed: {run.get('html_url')}")
            return run
        time.sleep(45)
    raise TimeoutError("token-transfer source did not finish")


def download(url: str, destination: Path, attempts: int = 8) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    last_error = None
    for attempt in range(attempts):
        req = urllib.request.Request(
            url,
            headers={
                "Authorization": f"Bearer {TOKEN}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
                "User-Agent": "RHC-Token-Transfer-Aggregator/1.0",
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=300) as response:
                destination.write_bytes(response.read())
            return
        except Exception as exc:
            last_error = exc
            if attempt + 1 < attempts:
                time.sleep(min(120, 5 * (2 ** attempt)))
                continue
    raise RuntimeError(f"artifact download failed: {url}: {last_error!r}")


def canonical(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def intish(value: Any) -> int:
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        return int(value, 16) if value.startswith("0x") else int(value)
    raise ValueError(value)


def event_key(row: dict[str, Any]) -> str:
    raw = row["raw"]
    tx_hash = str(raw.get("transactionHash") or raw.get("transaction_hash") or "").lower()
    log_index = intish(raw.get("logIndex") or raw.get("log_index") or "0x0")
    return f"{row['contract'].lower()}:{row['event_name']}:{tx_hash}:{log_index}"


def block_number(row: dict[str, Any]) -> int:
    raw = row["raw"]
    return intish(raw.get("blockNumber") or raw.get("block_number"))


def load_jsonl_gz(path: Path) -> Iterable[dict[str, Any]]:
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                value = json.loads(line)
                if not isinstance(value, dict):
                    raise RuntimeError(f"non-object row in {path}")
                yield value


def write_jsonl_gz(path: Path, rows: Iterable[dict[str, Any]]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with gzip.open(path, "wt", encoding="utf-8", newline="\n") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n")
            count += 1
    return count


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


def only_file(root: Path, filename: str) -> Path:
    paths = sorted(root.rglob(filename))
    if len(paths) != 1:
        raise RuntimeError(f"expected one {filename} under {root}; got {len(paths)}")
    return paths[0]


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    STATUS_DIR.mkdir(parents=True, exist_ok=True)
    source = wait_source()
    payload = api(f"/actions/runs/{source['id']}/artifacts?per_page=100")
    artifacts = sorted(
        [row for row in payload.get("artifacts", []) if row.get("name", "").startswith(ARTIFACT_PREFIX)],
        key=lambda row: row["name"],
    )
    failures: list[dict[str, Any]] = []
    if len(artifacts) != 32:
        failures.append({"code": "ARTIFACT_COUNT_MISMATCH", "expected": 32, "actual": len(artifacts)})

    inventory = []
    validations = []
    source_rows = 0
    by_key: dict[str, dict[str, Any]] = {}
    contract_summaries: dict[str, dict[str, Any]] = {}
    accepted_ranges = []
    source_failure_rows = []

    for artifact in artifacts:
        name = artifact["name"]
        zpath = OUT / "source_artifacts" / f"{name}.zip"
        download(artifact["archive_download_url"], zpath)
        root = OUT / "extracted" / name
        root.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(zpath) as archive:
            archive.extractall(root)
        validation = json.loads(only_file(root, "validation.json").read_text(encoding="utf-8"))
        validations.append(validation)
        if validation.get("status") != "PASS":
            failures.append({"code": "SHARD_VALIDATION_NOT_PASS", "artifact": name, "validation": validation})

        for row in load_jsonl_gz(only_file(root, "contract_transfer_events.jsonl.gz")):
            source_rows += 1
            key = event_key(row)
            previous = by_key.get(key)
            if previous is not None and canonical(previous) != canonical(row):
                failures.append({"code": "CONFLICTING_TRANSFER_DUPLICATE", "event_key": key, "artifact": name})
            else:
                by_key[key] = row
        for row in read_csv(only_file(root, "contract_summary.csv")):
            contract = row.get("contract", "").lower()
            if contract in contract_summaries and canonical(contract_summaries[contract]) != canonical(row):
                failures.append({"code": "CONFLICTING_CONTRACT_SUMMARY", "contract": contract})
            contract_summaries[contract] = row
        accepted_ranges.extend(read_csv(only_file(root, "accepted_ranges.csv")))
        source_failure_rows.extend(read_csv(only_file(root, "failures.csv")))
        inventory.append({
            "name": name,
            "artifact_id": artifact["id"],
            "shard": validation.get("shard"),
            "bytes": zpath.stat().st_size,
            "sha256": hashlib.sha256(zpath.read_bytes()).hexdigest(),
        })

    shard_ids = sorted(int(value["shard"]) for value in validations if value.get("shard") is not None)
    if shard_ids != list(range(32)):
        failures.append({"code": "SHARD_SET_MISMATCH", "actual": shard_ids})
    nonempty_source_failures = [
        row for row in source_failure_rows if any(str(value).strip() for value in row.values())
    ]
    if nonempty_source_failures:
        failures.append({"code": "SOURCE_FAILURE_ROWS_PRESENT", "count": len(nonempty_source_failures)})

    ordered = sorted(by_key.values(), key=lambda row: (block_number(row), event_key(row)))
    merged_path = OUT / "merged" / "contract_transfer_events.jsonl.gz"
    unique_count = write_jsonl_gz(merged_path, ordered)
    write_csv(OUT / "merged" / "contract_summary.csv", contract_summaries.values())
    write_csv(OUT / "merged" / "accepted_ranges.csv", accepted_ranges)
    write_csv(OUT / "artifact_inventory.csv", inventory)
    write_csv(OUT / "source_failure_rows.csv", nonempty_source_failures)

    expected_contracts = sum(int(value.get("contract_count", 0)) for value in validations)
    expected_transfer_rows = sum(int(value.get("transfer_event_rows", 0)) for value in validations)
    if len(contract_summaries) != expected_contracts:
        failures.append({
            "code": "CONTRACT_COUNT_MISMATCH",
            "expected": expected_contracts,
            "actual": len(contract_summaries),
        })
    if unique_count != expected_transfer_rows:
        failures.append({
            "code": "TRANSFER_COUNT_MISMATCH",
            "expected": expected_transfer_rows,
            "actual": unique_count,
            "source_rows": source_rows,
        })

    status = {
        "status": "PASS" if not failures else "FAIL",
        "generated_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "aggregator_run_id": RUN_ID,
        "source_run": {
            "id": source["id"],
            "head_sha": source.get("head_sha"),
            "html_url": source.get("html_url"),
        },
        "artifact_count": len(artifacts),
        "contract_count": len(contract_summaries),
        "source_transfer_rows": source_rows,
        "unique_transfer_rows": unique_count,
        "deduplicated_rows": source_rows - unique_count,
        "merged_sha256": hashlib.sha256(merged_path.read_bytes()).hexdigest(),
        "source_failure_rows": len(nonempty_source_failures),
        "failures": failures,
        "production_approved_wallets": 0,
        "deepseek_handoff": "BLOCKED_OUTCOME_AND_ALPHA_BUILD_REQUIRED",
    }
    (OUT / "AGGREGATE_STATUS.json").write_text(json.dumps(status, indent=2, sort_keys=True), encoding="utf-8")
    (STATUS_DIR / "AGGREGATE_STATUS.json").write_text(json.dumps(status, indent=2, sort_keys=True), encoding="utf-8")
    (STATUS_DIR / "SOURCE_RUN.json").write_text(json.dumps(status["source_run"], indent=2, sort_keys=True), encoding="utf-8")
    manifest = []
    for path in sorted(OUT.rglob("*")):
        if path.is_file():
            manifest.append({
                "path": str(path.relative_to(OUT)),
                "bytes": path.stat().st_size,
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            })
    (OUT / "MANIFEST.json").write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    (STATUS_DIR / "MANIFEST.json").write_text((OUT / "MANIFEST.json").read_text(), encoding="utf-8")
    print(json.dumps(status, sort_keys=True), flush=True)
    if failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
