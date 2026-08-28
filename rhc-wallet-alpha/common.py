from __future__ import annotations

import csv
import hashlib
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

REPOSITORY = os.environ["GITHUB_REPOSITORY"]
TOKEN = os.environ["GITHUB_TOKEN"]
API = f"https://api.github.com/repos/{REPOSITORY}"
USER_AGENT = "RHC-Wallet-Alpha-Dataset/1.0"
ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"


def api_request(path: str, attempts: int = 8) -> Any:
    url = path if path.startswith("https://") else API + path
    last_error: Exception | None = None
    for attempt in range(attempts):
        request = urllib.request.Request(
            url,
            headers={
                "Authorization": f"Bearer {TOKEN}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
                "User-Agent": USER_AGENT,
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
            raise RuntimeError(f"GitHub API HTTP {exc.code} {url}: {detail}") from exc
        except Exception as exc:
            last_error = exc
            if attempt + 1 < attempts:
                time.sleep(min(90, 5 * (2 ** attempt)))
                continue
    raise RuntimeError(f"GitHub API exhausted: {url}: {last_error!r}")


def wait_successful_run(branch: str, workflow: str, timeout_seconds: int = 19_800) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        query = urllib.parse.urlencode({"branch": branch, "event": "pull_request", "per_page": 100})
        rows = sorted(
            [
                row for row in api_request(f"/actions/runs?{query}").get("workflow_runs", [])
                if row.get("name") == workflow
            ],
            key=lambda row: int(row["id"]),
            reverse=True,
        )
        if not rows:
            print(f"waiting for {branch}/{workflow}", flush=True)
            time.sleep(30)
            continue
        run = rows[0]
        print(json.dumps({
            "branch": branch,
            "workflow": workflow,
            "id": run["id"],
            "status": run.get("status"),
            "conclusion": run.get("conclusion"),
            "head_sha": run.get("head_sha"),
        }, sort_keys=True), flush=True)
        if run.get("status") == "completed":
            if run.get("conclusion") != "success":
                raise RuntimeError(f"source workflow did not succeed: {run.get('html_url')}")
            return run
        time.sleep(45)
    raise TimeoutError(f"source workflow timeout: {branch}/{workflow}")


def download_file(url: str, destination: Path, attempts: int = 8) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    last_error: Exception | None = None
    for attempt in range(attempts):
        request = urllib.request.Request(
            url,
            headers={
                "Authorization": f"Bearer {TOKEN}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
                "User-Agent": USER_AGENT,
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


def fetch_artifact(
    name: str,
    branch: str,
    workflow: str,
    artifact_name: str,
    destination_root: Path,
) -> tuple[dict[str, Any], Path]:
    run = wait_successful_run(branch, workflow)
    artifacts = api_request(f"/actions/runs/{run['id']}/artifacts?per_page=100").get("artifacts", [])
    matches = [row for row in artifacts if row.get("name") == artifact_name]
    if len(matches) != 1:
        raise RuntimeError(f"artifact count mismatch for {name}: {len(matches)}")
    artifact = matches[0]
    zip_path = destination_root / "source_artifacts" / f"{name}.zip"
    download_file(artifact["archive_download_url"], zip_path)
    extract = destination_root / "sources" / name
    extract.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path) as archive:
        archive.extractall(extract)
    return {
        "run_id": run["id"],
        "head_sha": run.get("head_sha"),
        "html_url": run.get("html_url"),
        "artifact_id": artifact["id"],
        "artifact_bytes": zip_path.stat().st_size,
        "artifact_sha256": hashlib.sha256(zip_path.read_bytes()).hexdigest(),
    }, extract


def only_file(root: Path, filename: str) -> Path:
    paths = sorted(root.rglob(filename))
    if len(paths) != 1:
        raise RuntimeError(f"expected exactly one {filename} under {root}, found {len(paths)}")
    return paths[0]


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: Iterable[dict[str, Any]]) -> int:
    materialized = list(rows)
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = sorted({key for row in materialized for key in row}) if materialized else ["empty"]
    with path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in materialized:
            writer.writerow({
                key: json.dumps(value, ensure_ascii=False, sort_keys=True)
                if isinstance(value, (dict, list, tuple, set))
                else value
                for key, value in row.items()
            })
    return len(materialized)


def intish(value: Any, default: int | None = None) -> int | None:
    if value is None or value == "":
        return default
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        try:
            return int(value, 16) if value.startswith("0x") else int(float(value))
        except Exception:
            return default
    return default


def floatish(value: Any, default: float | None = None) -> float | None:
    if value is None or value == "":
        return default
    try:
        return float(value)
    except Exception:
        return default


def boolish(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "pass"}


def parse_json(value: Any, default: Any = None) -> Any:
    if value is None or value == "":
        return default
    if isinstance(value, (dict, list, int, float, bool)):
        return value
    try:
        return json.loads(str(value))
    except Exception:
        return default


def median_int(values: Iterable[int]) -> int | None:
    ordered = sorted(int(value) for value in values)
    if not ordered:
        return None
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    return (ordered[middle - 1] + ordered[middle]) // 2


def mean(values: Iterable[float]) -> float | None:
    rows = list(values)
    return sum(rows) / len(rows) if rows else None


def wilson_interval(successes: int, total: int, z: float = 1.96) -> tuple[float | None, float | None]:
    if total <= 0:
        return None, None
    p = successes / total
    denominator = 1 + z * z / total
    centre = p + z * z / (2 * total)
    margin = z * ((p * (1 - p) / total + z * z / (4 * total * total)) ** 0.5)
    return (centre - margin) / denominator, (centre + margin) / denominator


def unix_to_iso(value: int | None) -> str | None:
    if value is None:
        return None
    return datetime.fromtimestamp(value, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
