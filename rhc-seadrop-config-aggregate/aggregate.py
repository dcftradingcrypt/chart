#!/usr/bin/env python3
from __future__ import annotations

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
from typing import Any

REPOSITORY = os.environ["GITHUB_REPOSITORY"]
TOKEN = os.environ["GITHUB_TOKEN"]
RUN_ID = int(os.environ["GITHUB_RUN_ID"])
API = f"https://api.github.com/repos/{REPOSITORY}"
SOURCE_BRANCH = "chatgpt/rhc-seadrop-config-universe-20260829"
WORKFLOW_NAME = "RHC complete SeaDrop configuration universe"
FIXED_HEAD = 48_264_433
OUT = Path("rhc-seadrop-config-stage")
BUNDLE = Path("rhc-seadrop-config-aggregate-output")


def api(path: str, attempts: int = 8) -> Any:
    url = path if path.startswith("https://") else API + path
    last_error = None
    for attempt in range(attempts):
        request = urllib.request.Request(
            url,
            headers={
                "Authorization": f"Bearer {TOKEN}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
                "User-Agent": "RHC-SeaDrop-Config-Aggregator/1.0",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=180) as response:
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            last_error = exc
            if exc.code in (403, 429, 500, 502, 503, 504) and attempt + 1 < attempts:
                time.sleep(min(120, 5 * (2 ** attempt)))
                continue
            detail = exc.read(1000).decode("utf-8", "replace")
            raise RuntimeError(f"HTTP {exc.code} {url}: {detail}") from exc
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
            [row for row in payload.get("workflow_runs", []) if row.get("name") == WORKFLOW_NAME],
            key=lambda row: int(row["id"]),
            reverse=True,
        )
        if not rows:
            print("waiting for source workflow", flush=True)
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
                raise RuntimeError(f"source workflow failed: {run.get('html_url')}")
            return run
        time.sleep(45)
    raise TimeoutError("SeaDrop configuration source workflow did not finish")


def download(url: str, destination: Path, attempts: int = 8) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    last_error = None
    for attempt in range(attempts):
        request = urllib.request.Request(
            url,
            headers={
                "Authorization": f"Bearer {TOKEN}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
                "User-Agent": "RHC-SeaDrop-Config-Aggregator/1.0",
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
    raise RuntimeError(f"artifact download failed: {url}: {last_error!r}")


def intish(value: Any) -> int:
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        return int(value, 16) if value.startswith("0x") else int(value)
    raise ValueError(value)


def event_key(row: dict[str, Any]) -> tuple[str, int]:
    tx = str(row.get("transactionHash") or row.get("transaction_hash") or "").lower()
    index = intish(row.get("logIndex") or row.get("log_index") or "0x0")
    return tx, index


def canonical(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    BUNDLE.mkdir(parents=True, exist_ok=True)
    source = wait_source()
    artifact_payload = api(f"/actions/runs/{source['id']}/artifacts?per_page=100")
    artifacts = sorted(
        [row for row in artifact_payload.get("artifacts", []) if row.get("name", "").startswith("rhc-seadrop-all-")],
        key=lambda row: row["name"],
    )
    failures: list[dict[str, Any]] = []
    if len(artifacts) != 16:
        failures.append({"code": "ARTIFACT_COUNT_MISMATCH", "expected": 16, "actual": len(artifacts)})

    extracted = BUNDLE / "extracted"
    source_zips = BUNDLE / "source_artifacts"
    validations = []
    rows_by_key: dict[tuple[str, int], dict[str, Any]] = {}
    inventory = []

    for artifact in artifacts:
        name = artifact["name"]
        zpath = source_zips / f"{name}.zip"
        download(artifact["archive_download_url"], zpath)
        destination = extracted / name
        destination.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(zpath) as archive:
            archive.extractall(destination)
        validation_paths = list(destination.rglob("validation.json"))
        event_paths = list(destination.rglob("seadrop_all_logs.jsonl.gz"))
        if len(validation_paths) != 1:
            failures.append({"code": "VALIDATION_COUNT", "artifact": name, "actual": len(validation_paths)})
            continue
        if len(event_paths) != 1:
            failures.append({"code": "EVENT_FILE_COUNT", "artifact": name, "actual": len(event_paths)})
            continue
        validation = json.loads(validation_paths[0].read_text(encoding="utf-8"))
        validations.append(validation)
        if validation.get("status") != "PASS":
            failures.append({"code": "SOURCE_VALIDATION_NOT_PASS", "artifact": name, "validation": validation})
        with gzip.open(event_paths[0], "rt", encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                row = json.loads(line)
                key = event_key(row)
                previous = rows_by_key.get(key)
                if previous is not None and canonical(previous) != canonical(row):
                    failures.append({"code": "CONFLICTING_DUPLICATE", "tx_hash": key[0], "log_index": key[1]})
                else:
                    rows_by_key[key] = row
        inventory.append({
            "name": name,
            "artifact_id": artifact["id"],
            "bytes": zpath.stat().st_size,
            "sha256": hashlib.sha256(zpath.read_bytes()).hexdigest(),
        })

    intervals = sorted((int(v["requested_from_block"]), int(v["requested_to_block"])) for v in validations)
    expected = 0
    for left, right in intervals:
        if left != expected:
            failures.append({"code": "COVERAGE_GAP", "expected_from": expected, "actual_from": left})
        expected = right + 1
    if expected != FIXED_HEAD + 1:
        failures.append({"code": "COVERAGE_END_MISMATCH", "expected": FIXED_HEAD + 1, "actual": expected})

    ordered = sorted(
        rows_by_key.values(),
        key=lambda row: (
            intish(row.get("blockNumber") or row.get("block_number")),
            event_key(row)[0],
            event_key(row)[1],
        ),
    )
    merged = BUNDLE / "seadrop_all_logs.jsonl.gz"
    with gzip.open(merged, "wt", encoding="utf-8", newline="\n") as handle:
        for row in ordered:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n")

    topic_counts: dict[str, int] = {}
    for row in ordered:
        topics = row.get("topics") or []
        topic0 = str(topics[0]).lower() if topics else "NO_TOPIC"
        topic_counts[topic0] = topic_counts.get(topic0, 0) + 1

    status = {
        "status": "PASS" if not failures else "FAIL",
        "generated_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "aggregator_run_id": RUN_ID,
        "source_run_id": source["id"],
        "source_head_sha": source.get("head_sha"),
        "fixed_head": FIXED_HEAD,
        "artifact_count": len(artifacts),
        "event_rows": len(ordered),
        "topic0_counts": topic_counts,
        "merged_sha256": hashlib.sha256(merged.read_bytes()).hexdigest(),
        "failures": failures,
    }
    (BUNDLE / "AGGREGATE_STATUS.json").write_text(json.dumps(status, indent=2, sort_keys=True), encoding="utf-8")
    (BUNDLE / "ARTIFACT_INVENTORY.json").write_text(json.dumps(inventory, indent=2, sort_keys=True), encoding="utf-8")
    (OUT / "AGGREGATE_STATUS.json").write_text(json.dumps(status, indent=2, sort_keys=True), encoding="utf-8")
    (OUT / "SOURCE_RUN.json").write_text(json.dumps({
        "id": source["id"],
        "head_sha": source.get("head_sha"),
        "html_url": source.get("html_url"),
    }, indent=2, sort_keys=True), encoding="utf-8")
    manifest = []
    for path in sorted(BUNDLE.rglob("*")):
        if path.is_file():
            manifest.append({
                "path": str(path.relative_to(BUNDLE)),
                "bytes": path.stat().st_size,
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            })
    (BUNDLE / "MANIFEST.json").write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    (OUT / "BUNDLE_MANIFEST.json").write_text((BUNDLE / "MANIFEST.json").read_text(), encoding="utf-8")
    print(json.dumps(status, sort_keys=True), flush=True)
    if failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
