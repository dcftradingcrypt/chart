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
from typing import Any, Callable, Iterable

REPOSITORY = os.environ["GITHUB_REPOSITORY"]
TOKEN = os.environ["GITHUB_TOKEN"]
RUN_ID = int(os.environ["GITHUB_RUN_ID"])
API = f"https://api.github.com/repos/{REPOSITORY}"
SOURCE_BRANCH = "chatgpt/rhc-secondary-tx-enrichment-20260829"
SOURCE_WORKFLOW = "RHC enrich every secondary NFT transfer transaction"
PREFIX = "rhc-secondary-tx-enrichment-"
OUT = Path("rhc-secondary-enrichment-aggregate-output")
STATUS_DIR = Path("rhc-secondary-enrichment-aggregate-stage")


def api(path: str, attempts: int = 8) -> Any:
    url = path if path.startswith("https://") else API + path
    last_error = None
    for attempt in range(attempts):
        req = urllib.request.Request(url, headers={
            "Authorization": f"Bearer {TOKEN}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "RHC-Secondary-Enrichment-Aggregator/1.0",
        })
        try:
            with urllib.request.urlopen(req, timeout=180) as response:
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            last_error = exc
            if exc.code in (403, 429, 500, 502, 503, 504) and attempt + 1 < attempts:
                time.sleep(min(120, 5 * (2 ** attempt))); continue
            detail = exc.read(1000).decode("utf-8", "replace")
            raise RuntimeError(f"GitHub API HTTP {exc.code} {url}: {detail}") from exc
        except Exception as exc:
            last_error = exc
            if attempt + 1 < attempts:
                time.sleep(min(90, 5 * (2 ** attempt))); continue
    raise RuntimeError(f"GitHub API exhausted: {url}: {last_error!r}")


def wait_source(timeout: int = 19_800) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        query = urllib.parse.urlencode({"branch": SOURCE_BRANCH, "event": "pull_request", "per_page": 100})
        rows = sorted([
            row for row in api(f"/actions/runs?{query}").get("workflow_runs", [])
            if row.get("name") == SOURCE_WORKFLOW
        ], key=lambda row: int(row["id"]), reverse=True)
        if not rows:
            print("waiting for secondary enrichment", flush=True); time.sleep(30); continue
        run = rows[0]
        print(json.dumps({"id": run["id"], "status": run.get("status"), "conclusion": run.get("conclusion"), "head_sha": run.get("head_sha")}, sort_keys=True), flush=True)
        if run.get("status") == "completed":
            if run.get("conclusion") != "success":
                raise RuntimeError(f"secondary enrichment failed: {run.get('html_url')}")
            return run
        time.sleep(45)
    raise TimeoutError("secondary enrichment did not finish")


def download(url: str, destination: Path, attempts: int = 8) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    last_error = None
    for attempt in range(attempts):
        req = urllib.request.Request(url, headers={
            "Authorization": f"Bearer {TOKEN}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "RHC-Secondary-Enrichment-Aggregator/1.0",
        })
        try:
            with urllib.request.urlopen(req, timeout=300) as response:
                destination.write_bytes(response.read())
            return
        except Exception as exc:
            last_error = exc
            if attempt + 1 < attempts:
                time.sleep(min(120, 5 * (2 ** attempt))); continue
    raise RuntimeError(f"artifact download failed: {url}: {last_error!r}")


def canonical(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def load_jsonl_gz(path: Path) -> Iterable[dict[str, Any]]:
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                value = json.loads(line)
                if not isinstance(value, dict): raise RuntimeError(f"non-object row in {path}")
                yield value


def write_jsonl_gz(path: Path, rows: Iterable[dict[str, Any]]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True); count = 0
    with gzip.open(path, "wt", encoding="utf-8", newline="\n") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"); count += 1
    return count


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8-sig") as handle: return list(csv.DictReader(handle))


def write_csv(path: Path, rows: Iterable[dict[str, Any]]) -> None:
    materialized = list(rows); path.parent.mkdir(parents=True, exist_ok=True)
    fields = sorted({key for row in materialized for key in row}) if materialized else ["empty"]
    with path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore"); writer.writeheader()
        for row in materialized:
            writer.writerow({key: json.dumps(value, ensure_ascii=False, sort_keys=True) if isinstance(value, (dict, list, tuple)) else value for key, value in row.items()})


def only_file(root: Path, filename: str) -> Path:
    paths = sorted(root.rglob(filename))
    if len(paths) != 1: raise RuntimeError(f"expected one {filename} under {root}, got {len(paths)}")
    return paths[0]


def merge_family(roots: list[Path], filename: str, output_name: str, key_name: str) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    by_key = {}; conflicts = []; source_rows = 0
    for root in roots:
        for row in load_jsonl_gz(only_file(root, filename)):
            source_rows += 1; key = str(row[key_name]).lower()
            previous = by_key.get(key)
            if previous is not None and canonical(previous) != canonical(row): conflicts.append({"filename": filename, "key": key, "root": str(root)})
            else: by_key[key] = row
    ordered = [by_key[key] for key in sorted(by_key)]
    output = OUT / "merged" / output_name; count = write_jsonl_gz(output, ordered)
    return {"filename": filename, "source_rows": source_rows, "unique_rows": count, "deduplicated_rows": source_rows-count, "conflicts": len(conflicts), "output": str(output), "sha256": hashlib.sha256(output.read_bytes()).hexdigest()}, conflicts


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True); STATUS_DIR.mkdir(parents=True, exist_ok=True)
    source = wait_source(); payload = api(f"/actions/runs/{source['id']}/artifacts?per_page=100")
    artifacts = sorted([row for row in payload.get("artifacts", []) if row.get("name", "").startswith(PREFIX)], key=lambda row: row["name"])
    failures = []
    if len(artifacts) != 16: failures.append({"code": "ARTIFACT_COUNT_MISMATCH", "expected": 16, "actual": len(artifacts)})
    roots = []; inventory = []; validations = []; all_errors = []
    expected_tx = expected_blocks = 0
    for artifact in artifacts:
        name = artifact["name"]; zpath = OUT / "source_artifacts" / f"{name}.zip"; download(artifact["archive_download_url"], zpath)
        root = OUT / "extracted" / name; root.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(zpath) as archive: archive.extractall(root)
        roots.append(root); validation = json.loads(only_file(root, "validation.json").read_text(encoding="utf-8")); validations.append(validation)
        if validation.get("status") != "PASS": failures.append({"code": "SHARD_VALIDATION_NOT_PASS", "artifact": name, "validation": validation})
        expected_tx += int(validation.get("expected_transaction_count", 0)); expected_blocks += int(validation.get("expected_block_count", 0))
        for row in read_csv(only_file(root, "errors.csv")):
            if any(str(value).strip() for value in row.values()): all_errors.append({"artifact": name, **row})
        inventory.append({"name": name, "artifact_id": artifact["id"], "shard": validation.get("shard"), "bytes": zpath.stat().st_size, "sha256": hashlib.sha256(zpath.read_bytes()).hexdigest()})
    shard_ids = sorted(int(value["shard"]) for value in validations if value.get("shard") is not None)
    if shard_ids != list(range(16)): failures.append({"code": "SHARD_SET_MISMATCH", "actual": shard_ids})
    if all_errors: failures.append({"code": "ERROR_ROWS_PRESENT", "count": len(all_errors)})
    write_csv(OUT / "artifact_inventory.csv", inventory); write_csv(OUT / "all_errors.csv", all_errors)

    specs = [
        ("transactions.jsonl.gz", "transactions.jsonl.gz", "transaction_hash"),
        ("receipts.jsonl.gz", "receipts.jsonl.gz", "transaction_hash"),
        ("internal_transactions.jsonl.gz", "internal_transactions.jsonl.gz", "transaction_hash"),
        ("blocks.jsonl.gz", "blocks.jsonl.gz", "block_number"),
    ]
    families = []; conflict_rows = []
    for source_name, output_name, key_name in specs:
        family, conflicts = merge_family(roots, source_name, output_name, key_name); families.append(family); conflict_rows.extend(conflicts)
        if conflicts: failures.append({"code": "MERGE_CONFLICTS", "filename": source_name, "count": len(conflicts)})
    write_csv(OUT / "merge_conflicts.csv", conflict_rows)
    actual = {row["filename"]: row["unique_rows"] for row in families}
    expectations = {"transactions.jsonl.gz": expected_tx, "receipts.jsonl.gz": expected_tx, "internal_transactions.jsonl.gz": expected_tx, "blocks.jsonl.gz": expected_blocks}
    for filename, expected in expectations.items():
        if actual.get(filename) != expected: failures.append({"code": "MERGED_COUNT_MISMATCH", "filename": filename, "expected": expected, "actual": actual.get(filename)})

    status = {
        "status": "PASS" if not failures else "FAIL",
        "generated_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "aggregator_run_id": RUN_ID,
        "source_run": {"id": source["id"], "head_sha": source.get("head_sha"), "html_url": source.get("html_url")},
        "artifact_count": len(artifacts),
        "expected_transactions": expected_tx,
        "expected_blocks": expected_blocks,
        "families": families,
        "error_rows": len(all_errors),
        "failures": failures,
        "deepseek_handoff": "BLOCKED_SALE_CLASSIFICATION_AND_ALPHA_BUILD_REQUIRED",
        "production_approved_wallets": 0,
    }
    (OUT / "AGGREGATE_STATUS.json").write_text(json.dumps(status, indent=2, sort_keys=True), encoding="utf-8")
    (STATUS_DIR / "AGGREGATE_STATUS.json").write_text(json.dumps(status, indent=2, sort_keys=True), encoding="utf-8")
    (STATUS_DIR / "SOURCE_RUN.json").write_text(json.dumps(status["source_run"], indent=2, sort_keys=True), encoding="utf-8")
    manifest = [{"path": str(path.relative_to(OUT)), "bytes": path.stat().st_size, "sha256": hashlib.sha256(path.read_bytes()).hexdigest()} for path in sorted(OUT.rglob("*")) if path.is_file()]
    (OUT / "MANIFEST.json").write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    (STATUS_DIR / "MANIFEST.json").write_text((OUT / "MANIFEST.json").read_text(), encoding="utf-8")
    print(json.dumps(status, sort_keys=True), flush=True)
    if failures: raise SystemExit(1)


if __name__ == "__main__": main()
