#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter
from pathlib import Path
from typing import Any

REPO = os.environ.get("GITHUB_REPOSITORY", "dcftradingcrypt/chart")
TOKEN = os.environ["GITHUB_TOKEN"]
SOURCE_BRANCH = "chatgpt/rhc-wallet-verification-p2-20260829"
OUT = Path("out-canonical-inventory")
OUT.mkdir(parents=True, exist_ok=True)
UA = "RHC-Canonical-Artifact-Inventory/1.0"


def request_json(url: str, attempts: int = 6) -> dict[str, Any]:
    last: Exception | None = None
    for attempt in range(attempts):
        req = urllib.request.Request(
            url,
            headers={
                "Authorization": f"Bearer {TOKEN}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
                "User-Agent": UA,
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=120) as response:
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            last = exc
            if exc.code in {429, 500, 502, 503, 504} and attempt + 1 < attempts:
                time.sleep(min(30, 2 ** attempt))
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


def list_runs() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    page = 1
    branch = urllib.parse.quote(SOURCE_BRANCH, safe="")
    while True:
        payload = request_json(
            f"https://api.github.com/repos/{REPO}/actions/runs?branch={branch}&per_page=100&page={page}"
        )
        batch = payload.get("workflow_runs") or []
        rows.extend(batch)
        if len(batch) < 100:
            break
        page += 1
    return rows


def list_artifacts(run_id: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    page = 1
    while True:
        payload = request_json(
            f"https://api.github.com/repos/{REPO}/actions/runs/{run_id}/artifacts?per_page=100&page={page}"
        )
        batch = payload.get("artifacts") or []
        rows.extend(batch)
        if len(batch) < 100:
            break
        page += 1
    return rows


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fields = sorted({key for row in rows for key in row}) if rows else ["empty"]
    with path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    runs = list_runs()
    run_rows: list[dict[str, Any]] = []
    artifact_rows: list[dict[str, Any]] = []
    for index, run in enumerate(runs, 1):
        base = {
            "run_id": run.get("id"),
            "workflow_id": run.get("workflow_id"),
            "workflow_name": run.get("name"),
            "workflow_path": run.get("path"),
            "run_number": run.get("run_number"),
            "run_attempt": run.get("run_attempt"),
            "event": run.get("event"),
            "status": run.get("status"),
            "conclusion": run.get("conclusion"),
            "head_sha": run.get("head_sha"),
            "created_at": run.get("created_at"),
            "updated_at": run.get("updated_at"),
        }
        artifacts = list_artifacts(int(run["id"]))
        run_rows.append({**base, "artifact_count": len(artifacts)})
        for artifact in artifacts:
            artifact_rows.append({
                **base,
                "artifact_id": artifact.get("id"),
                "artifact_name": artifact.get("name"),
                "artifact_size_in_bytes": artifact.get("size_in_bytes"),
                "artifact_digest": artifact.get("digest"),
                "artifact_expired": artifact.get("expired"),
                "artifact_created_at": artifact.get("created_at"),
                "artifact_expires_at": artifact.get("expires_at"),
            })
        if index % 25 == 0:
            print({"runs_processed": index, "artifacts": len(artifact_rows)}, flush=True)

    keywords = (
        "canonical", "zero", "mint", "transfer", "seadrop", "seaport",
        "project", "universe", "candidate", "wallet", "selection", "result", "package",
    )
    relevant = [
        row for row in artifact_rows
        if any(k in (str(row.get("workflow_name", "")) + " " + str(row.get("workflow_path", "")) + " " + str(row.get("artifact_name", ""))).lower() for k in keywords)
    ]
    write_csv(OUT / "workflow_runs.csv", run_rows)
    write_csv(OUT / "artifacts_all.csv", artifact_rows)
    write_csv(OUT / "artifacts_relevant.csv", relevant)
    summary = {
        "source_branch": SOURCE_BRANCH,
        "run_count": len(runs),
        "artifact_count": len(artifact_rows),
        "nonexpired_artifact_count": sum(not bool(row.get("artifact_expired")) for row in artifact_rows),
        "relevant_artifact_count": len(relevant),
        "workflow_name_counts": Counter(str(row.get("workflow_name")) for row in run_rows),
        "artifact_name_counts": Counter(str(row.get("artifact_name")) for row in artifact_rows),
        "status": "PASS",
    }
    (OUT / "SUMMARY.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2, default=dict), encoding="utf-8")
    print(json.dumps({k: summary[k] for k in ("run_count", "artifact_count", "nonexpired_artifact_count", "relevant_artifact_count")}), flush=True)


if __name__ == "__main__":
    main()
