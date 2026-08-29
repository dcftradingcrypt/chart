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
import urllib.error
import urllib.request
import zipfile
from pathlib import Path
from typing import Any

REPO = os.environ.get("GITHUB_REPOSITORY", "dcftradingcrypt/chart")
RUN_ID = int(os.environ.get("SOURCE_RUN_ID", "33167365485"))
TOKEN = os.environ.get("GITHUB_TOKEN", "")
OUT = Path(os.environ.get("OUT_DIR", "out-p0p1-consolidated"))
DOWNLOADS = OUT / "artifacts"
REQUIRED = {
    "rhc-wallet-verification-p0",
    "rhc-wallet-verification-p1a",
    "rhc-wallet-verification-p1b",
    "rhc-wallet-verification-p1c",
    "rhc-wallet-verification-p1d",
}


def request_json(url: str) -> Any:
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "rhc-wallet-verification-consolidator/1.0",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    if TOKEN:
        headers["Authorization"] = f"Bearer {TOKEN}"
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=90) as response:
        return json.loads(response.read().decode("utf-8"))


def request_bytes(url: str) -> bytes:
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "rhc-wallet-verification-consolidator/1.0",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    if TOKEN:
        headers["Authorization"] = f"Bearer {TOKEN}"
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=180) as response:
        return response.read()


def safe_extract(data: bytes, destination: Path) -> None:
    destination.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(io.BytesIO(data)) as archive:
        root = destination.resolve()
        for member in archive.infolist():
            target = (destination / member.filename).resolve()
            if root not in target.parents and target != root:
                raise RuntimeError(f"unsafe zip member: {member.filename}")
        archive.extractall(destination)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def count_csv(path: Path) -> int:
    with path.open(newline="", encoding="utf-8-sig", errors="replace") as handle:
        return sum(1 for _ in csv.DictReader(handle))


def count_jsonl(path: Path) -> int:
    with path.open(encoding="utf-8", errors="replace") as handle:
        return sum(1 for line in handle if line.strip())


def summarize_artifact(name: str, root: Path, metadata: dict[str, Any]) -> dict[str, Any]:
    files = sorted(path for path in root.rglob("*") if path.is_file())
    validations = []
    csv_counts: dict[str, int] = {}
    jsonl_counts: dict[str, int] = {}
    nonempty_error_files = []
    for path in files:
        rel = str(path.relative_to(root)).replace("\\", "/")
        lower = path.name.lower()
        if lower in {"validation.json", "evidence_gate.json", "final_validation.json"}:
            try:
                validations.append({"path": rel, "value": json.loads(path.read_text(encoding="utf-8"))})
            except Exception as exc:
                validations.append({"path": rel, "parse_error": repr(exc)})
        if path.suffix.lower() == ".csv":
            try:
                csv_counts[rel] = count_csv(path)
            except Exception:
                csv_counts[rel] = -1
        elif path.suffix.lower() == ".jsonl":
            try:
                jsonl_counts[rel] = count_jsonl(path)
            except Exception:
                jsonl_counts[rel] = -1
        if "error" in lower and path.stat().st_size > 0:
            if path.suffix.lower() == ".csv":
                rows = csv_counts.get(rel, -1)
                if rows != 0:
                    nonempty_error_files.append({"path": rel, "rows": rows})
            else:
                nonempty_error_files.append({"path": rel, "bytes": path.stat().st_size})
    validation_statuses = []
    for item in validations:
        value = item.get("value")
        if isinstance(value, dict):
            validation_statuses.append(str(value.get("status", "MISSING_STATUS")))
        else:
            validation_statuses.append("UNPARSEABLE")
    return {
        "artifact": name,
        "artifact_id": metadata.get("id"),
        "artifact_size_bytes": metadata.get("size_in_bytes"),
        "artifact_digest": metadata.get("digest"),
        "file_count": len(files),
        "total_uncompressed_bytes": sum(path.stat().st_size for path in files),
        "validations": validations,
        "validation_statuses": validation_statuses,
        "csv_row_counts": csv_counts,
        "jsonl_row_counts": jsonl_counts,
        "nonempty_error_files": nonempty_error_files,
        "file_manifest": [
            {
                "path": str(path.relative_to(root)).replace("\\", "/"),
                "bytes": path.stat().st_size,
                "sha256": sha256(path),
            }
            for path in files
        ],
    }


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    DOWNLOADS.mkdir(parents=True, exist_ok=True)
    url = f"https://api.github.com/repos/{REPO}/actions/runs/{RUN_ID}/artifacts?per_page=100"
    report: dict[str, Any] = {
        "source_repository": REPO,
        "source_run_id": RUN_ID,
        "required_artifacts": sorted(REQUIRED),
        "artifacts": [],
        "missing_artifacts": [],
        "download_errors": [],
        "status": "FAIL",
    }
    try:
        payload = request_json(url)
    except Exception as exc:
        report["metadata_error"] = repr(exc)
        (OUT / "consolidated_status.json").write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
        return 1

    available = {
        item.get("name"): item
        for item in payload.get("artifacts", [])
        if isinstance(item, dict) and not item.get("expired")
    }
    report["available_artifacts"] = sorted(available)
    report["missing_artifacts"] = sorted(REQUIRED - set(available))

    for name in sorted(REQUIRED & set(available)):
        item = available[name]
        try:
            data = request_bytes(item["archive_download_url"])
            zip_path = DOWNLOADS / f"{name}.zip"
            zip_path.write_bytes(data)
            extract_root = DOWNLOADS / name
            safe_extract(data, extract_root)
            summary = summarize_artifact(name, extract_root, item)
            summary["download_zip_sha256"] = hashlib.sha256(data).hexdigest()
            summary["download_zip_bytes"] = len(data)
            report["artifacts"].append(summary)
        except Exception as exc:
            report["download_errors"].append({"artifact": name, "error": repr(exc)})

    artifact_names = {row["artifact"] for row in report["artifacts"]}
    validation_failures = []
    for row in report["artifacts"]:
        if not row["validation_statuses"]:
            validation_failures.append({"artifact": row["artifact"], "reason": "NO_VALIDATION_FILE"})
        for status in row["validation_statuses"]:
            if status not in {"PASS", "PASS_WITH_WARNINGS", "PARTIAL"}:
                validation_failures.append({"artifact": row["artifact"], "reason": f"VALIDATION_{status}"})
    report["validation_failures"] = validation_failures
    report["all_required_downloaded"] = artifact_names == REQUIRED
    report["status"] = (
        "PASS"
        if not report["missing_artifacts"]
        and not report["download_errors"]
        and not validation_failures
        else "FAIL"
    )

    status_path = OUT / "consolidated_status.json"
    status_path.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")

    manifest_rows = []
    for path in sorted(p for p in OUT.rglob("*") if p.is_file() and p.name != "MANIFEST.json"):
        manifest_rows.append({
            "path": str(path.relative_to(OUT)).replace("\\", "/"),
            "bytes": path.stat().st_size,
            "sha256": sha256(path),
        })
    (OUT / "MANIFEST.json").write_text(json.dumps(manifest_rows, indent=2, sort_keys=True), encoding="utf-8")

    archive_path = Path("rhc-wallet-verification-p0p1-consolidated.zip")
    with zipfile.ZipFile(archive_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
        for path in sorted(p for p in OUT.rglob("*") if p.is_file()):
            archive.write(path, arcname=str(path.relative_to(OUT)).replace("\\", "/"))
    print(json.dumps({
        "status": report["status"],
        "downloaded": sorted(artifact_names),
        "missing": report["missing_artifacts"],
        "download_errors": report["download_errors"],
        "validation_failures": validation_failures,
        "archive": str(archive_path),
        "archive_sha256": sha256(archive_path),
    }, sort_keys=True))
    return 0 if report["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
