#!/usr/bin/env python3
from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import re
import shutil
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from pathlib import Path
from typing import Any

REPO = os.environ.get("GITHUB_REPOSITORY", "dcftradingcrypt/chart")
TOKEN = os.environ["GITHUB_TOKEN"]
WORKFLOW_ID = "344763846"
SOURCE_BRANCH = "chatgpt/rhc-wallet-verification-p2-20260829"
P0 = "0x76d387388bea6b60ca6d1e97f446f7e26d39d313"
SHARD_COUNT = 16
OUT = Path("out-p2-recovery")
OUT.mkdir(parents=True, exist_ok=True)
UA = "RHC-P2-Artifact-Recovery/1.0"
ARTIFACT_RE = re.compile(r"^rhc-wallet-verification-p2-(\d+)$")
ADDR_FILE_RE = re.compile(r"(?:^|/)address_(0x[a-f0-9]{40})\.json$")


def request(url: str, *, accept: str = "application/vnd.github+json", attempts: int = 6) -> bytes:
    last: Exception | None = None
    for attempt in range(attempts):
        req = urllib.request.Request(
            url,
            headers={
                "Authorization": f"Bearer {TOKEN}",
                "Accept": accept,
                "X-GitHub-Api-Version": "2022-11-28",
                "User-Agent": UA,
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=120) as resp:
                return resp.read()
        except urllib.error.HTTPError as exc:
            last = exc
            if exc.code in {429, 500, 502, 503, 504} and attempt + 1 < attempts:
                retry_after = exc.headers.get("retry-after") if exc.headers else None
                delay = float(retry_after) if retry_after else min(30, 2 ** attempt)
                time.sleep(delay)
                continue
            body = exc.read(1000).decode("utf-8", "replace")
            raise RuntimeError(f"HTTP {exc.code} {url}: {body}") from exc
        except Exception as exc:
            last = exc
            if attempt + 1 < attempts:
                time.sleep(min(20, 2 ** attempt))
                continue
            raise
    raise RuntimeError(f"request failed: {url}: {last}")


def get_json(url: str) -> dict[str, Any]:
    return json.loads(request(url).decode("utf-8"))


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def list_runs() -> list[dict[str, Any]]:
    runs: list[dict[str, Any]] = []
    page = 1
    encoded_branch = urllib.parse.quote(SOURCE_BRANCH, safe="")
    while True:
        url = (
            f"https://api.github.com/repos/{REPO}/actions/workflows/{WORKFLOW_ID}/runs"
            f"?branch={encoded_branch}&per_page=100&page={page}"
        )
        payload = get_json(url)
        batch = payload.get("workflow_runs") or []
        runs.extend(batch)
        if len(batch) < 100:
            break
        page += 1
    return runs


def list_artifacts(run_id: int) -> list[dict[str, Any]]:
    artifacts: list[dict[str, Any]] = []
    page = 1
    while True:
        url = f"https://api.github.com/repos/{REPO}/actions/runs/{run_id}/artifacts?per_page=100&page={page}"
        payload = get_json(url)
        batch = payload.get("artifacts") or []
        artifacts.extend(batch)
        if len(batch) < 100:
            break
        page += 1
    return artifacts


def load_source_addresses() -> list[str]:
    url = (
        "https://raw.githubusercontent.com/"
        f"{REPO}/{urllib.parse.quote(SOURCE_BRANCH, safe='')}/wallet-verification/p2_addresses.txt"
    )
    # raw URL branch names with slashes require no percent encoding in path.
    url = f"https://raw.githubusercontent.com/{REPO}/{SOURCE_BRANCH}/wallet-verification/p2_addresses.txt"
    raw = request(url, accept="text/plain").decode("utf-8")
    rows = [line.strip().lower() for line in raw.splitlines() if line.strip()]
    if len(rows) != 212 or len(rows) != len(set(rows)):
        raise RuntimeError(f"invalid P2 registry: rows={len(rows)} unique={len(set(rows))}")
    if not all(re.fullmatch(r"0x[a-f0-9]{40}", row) for row in rows):
        raise RuntimeError("invalid address in P2 registry")
    return rows


def csv_count(path: Path, *, exclude_p0: bool = False) -> int:
    if not path.exists() or path.stat().st_size == 0:
        return 0
    try:
        with path.open(newline="", encoding="utf-8-sig") as fh:
            rows = list(csv.DictReader(fh))
        if exclude_p0:
            rows = [r for r in rows if (r.get("wallet") or r.get("audit_wallet") or "").lower() != P0]
        return len(rows)
    except Exception:
        return 0


def inspect_artifact(data: bytes, artifact: dict[str, Any], run: dict[str, Any], expected_by_shard: dict[int, list[str]]) -> dict[str, Any]:
    name = artifact["name"]
    match = ARTIFACT_RE.fullmatch(name)
    if not match:
        raise RuntimeError(f"unexpected artifact name {name}")
    shard = int(match.group(1))
    expected = set(expected_by_shard[shard])
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            zf.extractall(root)
        files = [p for p in root.rglob("*") if p.is_file()]
        processed: set[str] = set()
        for path in files:
            rel = path.relative_to(root).as_posix().lower()
            m = ADDR_FILE_RE.search(rel)
            if m and m.group(1) != P0:
                processed.add(m.group(1))
        summary_candidates = list(root.rglob("wallet_summary.csv"))
        error_candidates = list(root.rglob("errors.csv"))
        validation_candidates = list(root.rglob("validation.json"))
        summary_count = sum(csv_count(p, exclude_p0=True) for p in summary_candidates)
        error_count = sum(csv_count(p, exclude_p0=True) for p in error_candidates)
        validation_statuses: list[str] = []
        for p in validation_candidates:
            try:
                validation_statuses.append(str(json.loads(p.read_text(encoding="utf-8")).get("status")))
            except Exception:
                validation_statuses.append("UNREADABLE")
        unexpected = sorted(processed - expected)
        matched = sorted(processed & expected)
        missing = sorted(expected - processed)
        complete = not missing and not unexpected and summary_count >= len(expected) and error_count == 0 and any(
            status == "PASS" for status in validation_statuses
        )
        return {
            "shard": shard,
            "artifact_id": artifact["id"],
            "artifact_name": name,
            "artifact_size": artifact.get("size_in_bytes"),
            "artifact_digest": artifact.get("digest"),
            "artifact_zip_sha256": sha256_bytes(data),
            "run_id": run["id"],
            "run_number": run.get("run_number"),
            "run_attempt": run.get("run_attempt"),
            "run_status": run.get("status"),
            "run_conclusion": run.get("conclusion"),
            "run_created_at": run.get("created_at"),
            "run_updated_at": run.get("updated_at"),
            "head_sha": run.get("head_sha"),
            "expected_wallets": len(expected),
            "processed_wallets": len(matched),
            "processed_addresses": matched,
            "missing_addresses": missing,
            "unexpected_addresses": unexpected,
            "wallet_summary_rows": summary_count,
            "error_rows": error_count,
            "validation_statuses": validation_statuses,
            "file_count": len(files),
            "uncompressed_bytes": sum(p.stat().st_size for p in files),
            "complete": complete,
        }


def score(row: dict[str, Any]) -> tuple[int, int, int, int, int, str]:
    return (
        1 if row["complete"] else 0,
        int(row["processed_wallets"]),
        int(row["wallet_summary_rows"]),
        -int(row["error_rows"]),
        int(row["uncompressed_bytes"]),
        str(row["run_updated_at"] or ""),
    )


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fields = sorted({key for row in rows for key in row}) if rows else ["empty"]
    with path.open("w", newline="", encoding="utf-8-sig") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({
                key: json.dumps(value, ensure_ascii=False, sort_keys=True)
                if isinstance(value, (list, dict))
                else value
                for key, value in row.items()
            })


def main() -> None:
    source_addresses = load_source_addresses()
    expected_by_shard = {
        shard: [address for index, address in enumerate(source_addresses) if index % SHARD_COUNT == shard]
        for shard in range(SHARD_COUNT)
    }
    runs = list_runs()
    run_rows = [
        {
            key: run.get(key)
            for key in ("id", "run_number", "run_attempt", "status", "conclusion", "created_at", "updated_at", "head_sha", "event")
        }
        for run in runs
    ]
    (OUT / "workflow_runs.json").write_text(json.dumps(run_rows, indent=2), encoding="utf-8")

    observations: list[dict[str, Any]] = []
    original_dir = OUT / "artifact_zips"
    original_dir.mkdir(exist_ok=True)
    for run in runs:
        for artifact in list_artifacts(int(run["id"])):
            match = ARTIFACT_RE.fullmatch(str(artifact.get("name") or ""))
            if not match or artifact.get("expired"):
                continue
            try:
                data = request(artifact["archive_download_url"], accept="application/zip")
                row = inspect_artifact(data, artifact, run, expected_by_shard)
                observations.append(row)
                zip_name = f"run_{run['id']}_artifact_{artifact['id']}_{artifact['name']}.zip"
                (original_dir / zip_name).write_bytes(data)
                print({k: row[k] for k in ("shard", "run_id", "artifact_id", "processed_wallets", "complete")}, flush=True)
            except Exception as exc:
                observations.append({
                    "shard": int(match.group(1)),
                    "artifact_id": artifact.get("id"),
                    "artifact_name": artifact.get("name"),
                    "run_id": run.get("id"),
                    "download_or_inspection_error": repr(exc),
                    "complete": False,
                    "processed_wallets": 0,
                    "wallet_summary_rows": 0,
                    "error_rows": 0,
                    "uncompressed_bytes": 0,
                })

    selected: dict[int, dict[str, Any]] = {}
    for row in observations:
        shard = int(row["shard"])
        if shard not in selected or score(row) > score(selected[shard]):
            selected[shard] = row

    selected_rows: list[dict[str, Any]] = []
    recovered_wallets: set[str] = set()
    for shard in range(SHARD_COUNT):
        row = dict(selected.get(shard) or {
            "shard": shard,
            "complete": False,
            "processed_wallets": 0,
            "wallet_summary_rows": 0,
            "error_rows": 0,
            "missing_addresses": expected_by_shard[shard],
        })
        selected_rows.append(row)
        recovered_wallets.update(row.get("processed_addresses") or [])

    all_expected = set(source_addresses)
    missing_wallets = sorted(all_expected - recovered_wallets)
    complete_shards = [int(row["shard"]) for row in selected_rows if row.get("complete")]
    partial_shards = [
        int(row["shard"])
        for row in selected_rows
        if not row.get("complete") and int(row.get("processed_wallets") or 0) > 0
    ]
    empty_shards = [
        int(row["shard"])
        for row in selected_rows
        if int(row.get("processed_wallets") or 0) == 0
    ]
    summary = {
        "source_branch": SOURCE_BRANCH,
        "workflow_id": int(WORKFLOW_ID),
        "source_p2_wallets": len(source_addresses),
        "workflow_runs_examined": len(runs),
        "artifact_observations": len(observations),
        "recovered_unique_wallets": len(recovered_wallets),
        "missing_wallets": len(missing_wallets),
        "complete_shards": complete_shards,
        "partial_shards": partial_shards,
        "empty_shards": empty_shards,
        "status": "COMPLETE" if not missing_wallets and len(complete_shards) == SHARD_COUNT else "PARTIAL_RECOVERY",
        "production_approved_wallets": 0,
    }
    write_csv(OUT / "artifact_observations.csv", observations)
    write_csv(OUT / "selected_shard_artifacts.csv", selected_rows)
    (OUT / "missing_wallets.txt").write_text("\n".join(missing_wallets) + ("\n" if missing_wallets else ""), encoding="utf-8")
    (OUT / "expected_shards.json").write_text(json.dumps(expected_by_shard, indent=2), encoding="utf-8")
    (OUT / "SUMMARY.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, sort_keys=True), flush=True)

    # Recovery may legitimately be partial. The workflow itself succeeds if
    # it produced an auditable recovery inventory; missing work is explicit.
    if not observations:
        raise SystemExit("no P2 artifacts found")


if __name__ == "__main__":
    main()
