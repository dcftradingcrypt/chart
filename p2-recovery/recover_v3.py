#!/usr/bin/env python3
from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import Any

MODULE_PATH = Path(__file__).with_name("recover.py")
spec = importlib.util.spec_from_file_location("p2_recover_base", MODULE_PATH)
if spec is None or spec.loader is None:
    raise RuntimeError(f"cannot load {MODULE_PATH}")
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

_base_request = module.request


def github_request(url: str, *, accept: str = "application/vnd.github+json", attempts: int = 6) -> bytes:
    # GitHub's artifact archive endpoint requires a GitHub JSON media type;
    # it then redirects to a signed blob URL containing the ZIP bytes.
    if accept == "application/zip":
        accept = "application/vnd.github+json"
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
