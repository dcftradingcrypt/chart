#!/usr/bin/env python3
from __future__ import annotations

import importlib.util
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

MODULE_PATH = Path(__file__).with_name("recover.py")
spec = importlib.util.spec_from_file_location("p2_recover_base", MODULE_PATH)
if spec is None or spec.loader is None:
    raise RuntimeError(f"cannot load {MODULE_PATH}")
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

_base_request = module.request


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def download_artifact(url: str) -> bytes:
    # Stage 1: authenticated GitHub API request, intentionally without
    # following the cross-host redirect.
    opener = urllib.request.build_opener(NoRedirect())
    request = urllib.request.Request(
        url,
        headers={
            "Authorization": f"Bearer {module.TOKEN}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": module.UA,
        },
    )
    location: str | None = None
    try:
        with opener.open(request, timeout=120) as response:
            # Defensive: GitHub may one day return the archive directly.
            if response.status == 200:
                return response.read()
            location = response.headers.get("Location")
    except urllib.error.HTTPError as exc:
        if exc.code not in {301, 302, 303, 307, 308}:
            body = exc.read(1000).decode("utf-8", "replace")
            raise RuntimeError(f"artifact API HTTP {exc.code}: {body}") from exc
        location = exc.headers.get("Location") if exc.headers else None
    if not location:
        raise RuntimeError("artifact API did not provide a signed download URL")

    # Stage 2: signed object-storage URL. Never forward the GitHub token.
    blob_request = urllib.request.Request(
        location,
        headers={"User-Agent": module.UA, "Accept": "application/octet-stream"},
    )
    with urllib.request.urlopen(blob_request, timeout=180) as response:
        data = response.read()
    if not data.startswith(b"PK"):
        raise RuntimeError(f"artifact response is not a ZIP archive: {data[:80]!r}")
    return data


def github_request(url: str, *, accept: str = "application/vnd.github+json", attempts: int = 6) -> bytes:
    if "/actions/artifacts/" in url and url.rstrip("/").endswith("/zip"):
        return download_artifact(url)
    return _base_request(url, accept=accept, attempts=attempts)


def robust_score(row: dict[str, Any]) -> tuple[int, int, int, int, int, str]:
    return (
        1 if bool(row.get("complete")) else 0,
        int(row.get("processed_wallets") or 0),
        int(row.get("wallet_summary_rows") or 0),
        -int(row.get("error_rows") or 0),
        int(row.get("uncompressed_bytes") or 0),
        str(row.get("run_updated_at") or ""),
    )


module.request = github_request
module.score = robust_score
module.main()
