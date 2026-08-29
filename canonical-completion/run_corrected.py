#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


def patch_candidate(source: str) -> str:
    old = '''            for row in rows:\n                wallet_raw[event_key(row)] = row\n                try:\n                    normalized.append(normalize_log(wallet, direction, standard, row))\n'''
    new = '''            for row in rows:\n                # ERC-20 Transfer shares the ERC-721 signature but has only\n                # three topics. It is not an NFT decode failure.\n                if standard == "ERC721" and len(row.get("topics") or []) != 4:\n                    continue\n                wallet_raw[event_key(row)] = row\n                try:\n                    normalized.append(normalize_log(wallet, direction, standard, row))\n'''
    if old not in source:
        raise RuntimeError("candidate patch anchor not found")
    return source.replace(old, new, 1)


def patch_zero(source: str) -> str:
    old = '''    logs = sorted(unique.values(), key=lambda row: (h2i(row.get("blockNumber")) or -1, h2i(row.get("logIndex")) or -1))\n    decoded = []\n'''
    new = '''    logs = sorted(unique.values(), key=lambda row: (h2i(row.get("blockNumber")) or -1, h2i(row.get("logIndex")) or -1))\n    # ERC-20 mint events share Transfer(address,address,uint256) and match\n    # topic1=zero. Only four-topic Transfer logs are ERC-721 token mints.\n    if args.target == "erc721":\n        logs = [row for row in logs if len(row.get("topics") or []) == 4]\n    decoded = []\n'''
    if old not in source:
        raise RuntimeError("zero-mint patch anchor not found")
    return source.replace(old, new, 1)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--kind", choices=["candidate", "zero"], required=True)
    parser.add_argument("args", nargs=argparse.REMAINDER)
    options = parser.parse_args()
    original = Path(
        "canonical-completion/candidate_wallet_collector.py"
        if options.kind == "candidate"
        else "canonical-completion/zero_mint_collector.py"
    )
    source = original.read_text(encoding="utf-8")
    patched = patch_candidate(source) if options.kind == "candidate" else patch_zero(source)
    runtime = Path("runtime")
    runtime.mkdir(exist_ok=True)
    target = runtime / original.name
    target.write_text(patched, encoding="utf-8")
    subprocess.run([sys.executable, str(target), *options.args], check=True)


if __name__ == "__main__":
    main()
