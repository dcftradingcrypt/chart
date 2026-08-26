#!/usr/bin/env python3
from __future__ import annotations

import json
import re
import urllib.parse
import urllib.request
import zipfile
from pathlib import Path

OUT = Path("out-alphagate")
OUT.mkdir(parents=True, exist_ok=True)
UA = "Mozilla/5.0 (AlphaGate capability verification; read-only)"


def fetch(url: str, limit: int = 20_000_000) -> bytes:
    req = urllib.request.Request(url, headers={"user-agent": UA, "accept": "*/*"})
    with urllib.request.urlopen(req, timeout=90) as response:
        return response.read(limit)


report = {
    "tested_urls": [],
    "zip_candidates": [],
    "downloaded_zip": None,
    "domains": [],
    "api_literals": [],
    "websocket_literals": [],
    "auth_literals": [],
    "unauthenticated_probes": [],
}
for url in [
    "https://docs.alphagate.io/features/extension",
    "https://alphagate.io/",
    "https://app.alphagate.io/",
]:
    row = {"url": url}
    try:
        raw = fetch(url)
        parsed = urllib.parse.urlparse(url)
        name = re.sub(r"[^a-zA-Z0-9]+", "_", parsed.netloc + parsed.path).strip("_") + ".html"
        (OUT / name).write_bytes(raw)
        row.update({"status": "OK", "bytes": len(raw), "file": name})
        text = raw.decode("utf-8", "replace")
        for candidate in re.findall(r"https?://[^\"'<>\\\s]+", text):
            candidate = candidate.replace("\\u0026", "&").replace("\\/", "/")
            if ".zip" in candidate.lower() and "alphagate" in candidate.lower():
                report["zip_candidates"].append(candidate.rstrip("),.;"))
    except Exception as exc:
        row.update({"status": "ERROR", "error": repr(exc)})
    report["tested_urls"].append(row)

seen = set()
candidates = []
for url in report["zip_candidates"]:
    if url not in seen:
        seen.add(url)
        candidates.append(url)
report["zip_candidates"] = candidates

zip_path = OUT / "alphagate-extension.zip"
for url in candidates:
    try:
        raw = fetch(url, 50_000_000)
        if raw[:2] == b"PK":
            zip_path.write_bytes(raw)
            report["downloaded_zip"] = {"url": url, "bytes": len(raw)}
            break
    except Exception:
        pass

texts = []
if zip_path.exists():
    extract = OUT / "extension"
    extract.mkdir(exist_ok=True)
    with zipfile.ZipFile(zip_path) as archive:
        archive.extractall(extract)
    for path in extract.rglob("*"):
        if path.is_file() and path.suffix.lower() in {".js", ".json", ".html", ".txt", ".css"} and path.stat().st_size < 8_000_000:
            try:
                texts.append(path.read_text(encoding="utf-8", errors="replace"))
            except Exception:
                pass
else:
    for path in OUT.glob("*.html"):
        texts.append(path.read_text(encoding="utf-8", errors="replace"))

joined = "\n".join(texts)
domains = sorted(set(re.findall(r"(?:https?|wss?)://([a-zA-Z0-9._-]+)", joined)))
report["domains"] = domains
report["websocket_literals"] = sorted(set(re.findall(r"wss?://[^\"'<>\\\s]+", joined)))[:500]
report["api_literals"] = sorted(
    set(
        x
        for x in re.findall(r"https?://[^\"'<>\\\s]+", joined)
        if any(k in x.lower() for k in ("api", "graphql", "socket", "webhook"))
    )
)[:1000]
report["auth_literals"] = sorted(
    set(re.findall(r"(?i).{0,60}(?:authorization|bearer|token|firebase|supabase|auth0|clerk).{0,120}", joined))
)[:300]

probe_urls = []
for literal in report["api_literals"]:
    url = literal.replace("\\/", "/").rstrip("),.;")
    try:
        parsed = urllib.parse.urlparse(url)
        if parsed.scheme in ("http", "https") and parsed.netloc and len(url) < 500:
            root = f"{parsed.scheme}://{parsed.netloc}/"
            if root not in probe_urls:
                probe_urls.append(root)
    except Exception:
        pass
for url in probe_urls[:25]:
    row = {"url": url}
    try:
        req = urllib.request.Request(
            url,
            headers={"user-agent": UA, "accept": "application/json,text/plain,*/*"},
            method="GET",
        )
        with urllib.request.urlopen(req, timeout=30) as response:
            body = response.read(1000)
            row.update(
                {
                    "status": response.status,
                    "content_type": response.headers.get("content-type"),
                    "body_prefix": body.decode("utf-8", "replace"),
                }
            )
    except Exception as exc:
        row.update({"error": repr(exc)})
    report["unauthenticated_probes"].append(row)

report["conclusion"] = (
    "LIVE_PUBLIC_ENDPOINT_OR_EXTENSION_INSPECTED"
    if report["downloaded_zip"] or report["api_literals"]
    else "DOCUMENTATION_ONLY_NO_EXECUTABLE_PUBLIC_ENDPOINT_DISCOVERED"
)
(OUT / "report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
print(
    json.dumps(
        {
            "conclusion": report["conclusion"],
            "zip": report["downloaded_zip"],
            "domains": len(domains),
            "api_literals": len(report["api_literals"]),
        },
        sort_keys=True,
    )
)
