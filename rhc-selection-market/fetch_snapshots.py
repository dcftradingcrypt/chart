#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

OUT = Path("out-market")
OUT.mkdir(parents=True, exist_ok=True)
UA = "RHC-Selection-Alpha-Market/1.0 (read-only)"
URLS = {
    "guap_all": "https://guap.wtf/api/mint/all",
    "guap_market": "https://guap.wtf/api/mint/market",
    "guap_sales_feed": "https://guap.wtf/api/mint/sales-feed",
    "guap_mint_feed": "https://guap.wtf/api/mint/mint-feed",
    "guap_wallet_alpha": "https://guap.wtf/api/wallet-alpha",
    "guap_alpha_feed": "https://guap.wtf/api/alpha-feed",
    "mintgo_bootstrap": "https://mintgo.fun/api/bootstrap",
    "mintgo_seadrop_radar": "https://mintgo.fun/api/seadrop-radar",
    "mintgo_trending": "https://mintgo.fun/api/trending",
}


def fetch(url: str, attempts: int = 5) -> tuple[int, bytes, str]:
    last = ""
    for attempt in range(attempts):
        try:
            req = urllib.request.Request(url, headers={"accept": "application/json", "user-agent": UA})
            with urllib.request.urlopen(req, timeout=60) as response:
                return response.status, response.read(), response.headers.get("content-type", "")
        except Exception as exc:
            last = repr(exc)
            if attempt + 1 < attempts:
                time.sleep(2 ** attempt)
    return 0, b"", last


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main() -> None:
    status: list[dict[str, Any]] = []
    for name, url in URLS.items():
        code, body, content_type = fetch(url)
        path = OUT / f"{name}.json"
        valid_json = False
        error = None
        if body:
            path.write_bytes(body)
            try:
                json.loads(body.decode("utf-8"))
                valid_json = True
            except Exception as exc:
                error = repr(exc)
        status.append({
            "name": name,
            "url": url,
            "http_status": code,
            "content_type": content_type,
            "bytes": len(body),
            "valid_json": valid_json,
            "error": error,
        })
        print(status[-1], flush=True)
    (OUT / "status.json").write_text(json.dumps(status, ensure_ascii=False, indent=2), encoding="utf-8")
    manifest = []
    for path in sorted(OUT.iterdir()):
        if path.is_file() and path.name != "manifest.json":
            manifest.append({"path": path.name, "bytes": path.stat().st_size, "sha256": sha256(path)})
    (OUT / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    required = {"guap_all", "guap_market", "guap_sales_feed", "mintgo_bootstrap"}
    failures = [row for row in status if row["name"] in required and not row["valid_json"]]
    if failures:
        raise SystemExit(f"required market snapshots failed: {failures}")


if __name__ == "__main__":
    main()
