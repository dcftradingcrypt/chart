#!/usr/bin/env python3
from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from pathlib import Path

BASE = "https://api.alphagate.io"
ORIGIN_ID = "abcdefghijklmnopabcdefghijklmnop"
OUT = Path("out-alphagate-probe")
OUT.mkdir(parents=True, exist_ok=True)


def probe(path: str, method: str = "GET", body: bytes | None = None) -> dict:
    url = BASE + path
    headers = {
        "User-Agent": "AlphaGate-Live-ReadOnly-Probe/1.0",
        "Accept": "application/json,text/plain,*/*",
        "X-origin-ID": ORIGIN_ID,
        "Origin": "chrome-extension://" + ORIGIN_ID,
    }
    if body is not None:
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, method=method, data=body, headers=headers)
    row = {"url": url, "method": method}
    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            raw = response.read(100_000)
            row.update({
                "status": response.status,
                "headers": dict(response.headers.items()),
                "body_prefix": raw[:20_000].decode("utf-8", "replace"),
                "bytes_read": len(raw),
            })
    except urllib.error.HTTPError as exc:
        raw = exc.read(100_000)
        row.update({
            "status": exc.code,
            "headers": dict(exc.headers.items()),
            "body_prefix": raw[:20_000].decode("utf-8", "replace"),
            "bytes_read": len(raw),
        })
    except Exception as exc:
        row.update({"error": repr(exc)})
    return row


probes = [
    probe(f"/ext/socket.io/?EIO=4&transport=polling&t={int(time.time()*1000)}"),
    probe("/api/v1/ext/child/discover"),
    probe("/api/v1/ext/child/trending"),
    probe("/api/v1/ext/tracker/profiles"),
    probe("/api/v1/ext/tracker/subscriptions"),
    probe("/api/v1/ext/scan-profile?username=alphagateio"),
]

socket = probes[0]
body = socket.get("body_prefix", "")
socket_live = socket.get("status") in (200, 400, 401, 403) and ("sid" in body or socket.get("status") in (401, 403))
rest_live = any(p.get("status") in (200, 400, 401, 403, 404, 422) for p in probes[1:])
auth_required = any(p.get("status") in (401, 403) for p in probes)

report = {
    "conclusion": {
        "socket_endpoint_live": socket_live,
        "rest_api_live": rest_live,
        "authentication_or_session_required_observed": auth_required,
        "authenticated_feed_delivery_proven": False,
        "reason": "No user credentials or session cookies were supplied; this probe validates deployment and access boundary only.",
    },
    "probes": probes,
}
(OUT / "report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
print(json.dumps(report["conclusion"], sort_keys=True))
if not socket_live or not rest_live:
    raise SystemExit(2)
