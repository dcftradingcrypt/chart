#!/usr/bin/env python3
from __future__ import annotations

import hashlib
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
from typing import Any

REPO = os.environ.get("GITHUB_REPOSITORY", "dcftradingcrypt/chart")
BRANCH = os.environ.get("SOURCE_BRANCH", "chatgpt/rhc-wallet-verification-p2-20260829")
TOKEN = os.environ["GITHUB_TOKEN"]
API = "https://api.github.com"
OUT = Path("out-canonical-bundle")
DOWNLOADS = OUT / "downloads"
EXTRACTED = OUT / "extracted"

DEPENDENCIES = {
    "RHC canonical global SeaDrop and Seaport histories": {
        "script": "canonical-completion/global_event_collector.py",
        "artifacts": {"rhc-canonical-seadrop", "rhc-canonical-seaport"},
    },
    "RHC canonical full-chain NFT transfers": {
        "script": "canonical-completion/all_nft_transfer_collector.py",
        "artifacts": {
            "rhc-all-nft-transfers-erc721",
            "rhc-all-nft-transfers-erc1155_single",
            "rhc-all-nft-transfers-erc1155_batch",
        },
    },
}


def request_json(url: str, *, attempts: int = 8) -> Any:
    last: Exception | None = None
    for attempt in range(attempts):
        req = urllib.request.Request(
            url,
            headers={
                "Authorization": f"Bearer {TOKEN}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
                "User-Agent": "RHC-Canonical-Bundler/1.0",
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=90) as response:
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            last = RuntimeError(f"HTTP {exc.code}: {exc.read(2000).decode('utf-8','replace')}")
            if exc.code == 429 or exc.code >= 500:
                time.sleep(min(60, 2 ** attempt + 1))
                continue
            raise last
        except Exception as exc:
            last = exc
            time.sleep(min(30, 2 ** attempt))
    raise RuntimeError(f"GET {url} failed: {last}")


def download(url: str, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    req = urllib.request.Request(
        url,
        headers={
            "Authorization": f"Bearer {TOKEN}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "RHC-Canonical-Bundler/1.0",
        },
    )
    with urllib.request.urlopen(req, timeout=300) as response:
        path.write_bytes(response.read())


def file_sha(ref: str, path: str) -> str:
    encoded = urllib.parse.quote(path, safe="/")
    data = request_json(f"{API}/repos/{REPO}/contents/{encoded}?ref={urllib.parse.quote(ref, safe='')}")
    return str(data["sha"])


def latest_matching_run(workflow_name: str, script_path: str, current_script_sha: str) -> dict[str, Any] | None:
    data = request_json(
        f"{API}/repos/{REPO}/actions/runs?branch={urllib.parse.quote(BRANCH, safe='')}&status=success&per_page=100"
    )
    for run in data.get("workflow_runs", []):
        if run.get("name") != workflow_name or run.get("conclusion") != "success":
            continue
        run_sha = run.get("head_sha")
        if not run_sha:
            continue
        try:
            if file_sha(run_sha, script_path) == current_script_sha:
                return run
        except Exception:
            continue
    return None


def validate_extracted(name: str, root: Path) -> dict[str, Any]:
    matches = list(root.rglob("VALIDATION.json"))
    if len(matches) != 1:
        raise RuntimeError(f"{name}: expected one VALIDATION.json, found {len(matches)}")
    validation = json.loads(matches[0].read_text(encoding="utf-8"))
    if validation.get("status") != "PASS":
        raise RuntimeError(f"{name}: validation not PASS: {validation}")
    if int(validation.get("unresolved_ranges", 0) or 0) != 0:
        raise RuntimeError(f"{name}: unresolved ranges present: {validation}")
    manifest = list(root.rglob("MANIFEST.json"))
    if len(manifest) != 1:
        raise RuntimeError(f"{name}: expected one MANIFEST.json")
    entries = json.loads(manifest[0].read_text(encoding="utf-8"))
    manifest_root = manifest[0].parent
    for entry in entries:
        path = manifest_root / entry["path"]
        if not path.exists():
            raise RuntimeError(f"{name}: manifest file missing: {entry['path']}")
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        if digest != entry["sha256"]:
            raise RuntimeError(f"{name}: hash mismatch: {entry['path']}")
    return validation


def main() -> None:
    if OUT.exists():
        shutil.rmtree(OUT)
    DOWNLOADS.mkdir(parents=True)
    EXTRACTED.mkdir(parents=True)
    current_ref = os.environ.get("GITHUB_SHA") or BRANCH
    current_shas = {
        name: file_sha(current_ref, cfg["script"])
        for name, cfg in DEPENDENCIES.items()
    }
    deadline = time.time() + 5 * 3600
    selected_runs: dict[str, dict[str, Any]] = {}
    while time.time() < deadline:
        for name, cfg in DEPENDENCIES.items():
            if name not in selected_runs:
                run = latest_matching_run(name, cfg["script"], current_shas[name])
                if run:
                    selected_runs[name] = run
        if len(selected_runs) == len(DEPENDENCIES):
            break
        print({
            "waiting_for": sorted(set(DEPENDENCIES) - set(selected_runs)),
            "selected": {name: run["id"] for name, run in selected_runs.items()},
        }, flush=True)
        time.sleep(30)
    if len(selected_runs) != len(DEPENDENCIES):
        raise RuntimeError(f"Timed out waiting for matching successful runs: {selected_runs}")

    artifact_records = []
    expected_all = set().union(*(cfg["artifacts"] for cfg in DEPENDENCIES.values()))
    found: dict[str, dict[str, Any]] = {}
    for workflow_name, run in selected_runs.items():
        artifacts = request_json(f"{API}/repos/{REPO}/actions/runs/{run['id']}/artifacts?per_page=100")
        for artifact in artifacts.get("artifacts", []):
            if artifact.get("name") in expected_all and not artifact.get("expired"):
                found[artifact["name"]] = {**artifact, "source_workflow": workflow_name, "source_run_id": run["id"], "source_head_sha": run["head_sha"]}
    missing = expected_all - set(found)
    if missing:
        raise RuntimeError(f"Missing expected artifacts: {sorted(missing)}")

    validations = {}
    for name in sorted(expected_all):
        artifact = found[name]
        zip_path = DOWNLOADS / f"{name}.zip"
        download(artifact["archive_download_url"], zip_path)
        with zipfile.ZipFile(zip_path) as archive:
            bad = archive.testzip()
            if bad:
                raise RuntimeError(f"{name}: corrupt member {bad}")
            destination = EXTRACTED / name
            destination.mkdir(parents=True)
            archive.extractall(destination)
        validations[name] = validate_extracted(name, EXTRACTED / name)
        artifact_records.append({
            "name": name,
            "artifact_id": artifact["id"],
            "artifact_digest": artifact.get("digest"),
            "artifact_size": artifact.get("size_in_bytes"),
            "source_workflow": artifact["source_workflow"],
            "source_run_id": artifact["source_run_id"],
            "source_head_sha": artifact["source_head_sha"],
            "collector_script_sha": current_shas[artifact["source_workflow"]],
            "download_sha256": hashlib.sha256(zip_path.read_bytes()).hexdigest(),
        })

    bundle_root = OUT / "rhc-canonical-raw-complete"
    bundle_root.mkdir()
    shutil.copytree(EXTRACTED, bundle_root / "artifacts")
    (bundle_root / "SOURCE_ARTIFACTS.json").write_text(
        json.dumps(artifact_records, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8"
    )
    (bundle_root / "INPUT_VALIDATIONS.json").write_text(
        json.dumps(validations, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8"
    )
    summary = {
        "status": "PASS",
        "repository": REPO,
        "source_branch": BRANCH,
        "bundler_sha": current_ref,
        "artifact_count": len(artifact_records),
        "artifacts": sorted(found),
        "all_input_validations_pass": True,
        "all_unresolved_ranges_zero": True,
    }
    (bundle_root / "BUNDLE_VALIDATION.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8"
    )
    manifest = []
    for path in sorted(bundle_root.rglob("*")):
        if path.is_file() and path.name != "MANIFEST.json":
            manifest.append({
                "path": str(path.relative_to(bundle_root)).replace("\\", "/"),
                "bytes": path.stat().st_size,
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            })
    (bundle_root / "MANIFEST.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8"
    )
    archive_path = OUT / "rhc-canonical-raw-complete.zip"
    with zipfile.ZipFile(archive_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
        for path in sorted(bundle_root.rglob("*")):
            if path.is_file():
                archive.write(path, path.relative_to(OUT))
    (OUT / "rhc-canonical-raw-complete.zip.sha256").write_text(
        hashlib.sha256(archive_path.read_bytes()).hexdigest() + "  " + archive_path.name + "\n",
        encoding="utf-8",
    )
    print(json.dumps(summary, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
