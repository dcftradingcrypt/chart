#!/usr/bin/env python3
from __future__ import annotations

import json
import time
import urllib.parse
import urllib.request
from pathlib import Path

BASES = [
    "https://robinhoodchain.blockscout.com/api",
    "https://explorer.hoodmarketcap.com/api",
]
TRANSFER = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
WALLETS = [
    # Known paid-primary wallet whose address index returned zero.
    "0x01bdea1495c737fa416b337a0f4074ed68c730a6",
    # Known P0 control.
    "0x76d387388bea6b60ca6d1e97f446f7e26d39d313",
]
OUT = Path("out-wallet-log-probe")
OUT.mkdir(parents=True, exist_ok=True)


def get(url: str, params: dict[str, object]) -> tuple[int, object, str]:
    full = url + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(full, headers={"accept": "application/json", "user-agent": "RHC-Wallet-Log-Probe/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=90) as response:
            body = response.read()
            status = response.status
    except Exception as exc:
        return 0, {"error": repr(exc)}, full
    try:
        payload = json.loads(body.decode("utf-8"))
    except Exception:
        payload = {"raw": body[:1000].decode("utf-8", "replace")}
    return status, payload, full


def padded(address: str) -> str:
    return "0x" + "0" * 24 + address[2:]


records = []
for wallet in WALLETS:
    topic = padded(wallet)
    for base in BASES:
        for direction, topic_key, op_key in (
            ("incoming", "topic2", "topic0_2_opr"),
            ("outgoing", "topic1", "topic0_1_opr"),
        ):
            status, payload, url = get(base, {
                "module": "logs",
                "action": "getLogs",
                "fromBlock": 0,
                "toBlock": "latest",
                "topic0": TRANSFER,
                topic_key: topic,
                op_key: "and",
            })
            rows = payload.get("result") if isinstance(payload, dict) else None
            if not isinstance(rows, list):
                rows = []
            erc721 = [row for row in rows if len(row.get("topics") or []) == 4]
            record = {
                "wallet": wallet,
                "base": base,
                "direction": direction,
                "http_status": status,
                "message": payload.get("message") if isinstance(payload, dict) else None,
                "result_rows": len(rows),
                "erc721_rows": len(erc721),
                "first_block": rows[0].get("blockNumber") if rows else None,
                "last_block": rows[-1].get("blockNumber") if rows else None,
                "url": url,
            }
            records.append(record)
            print(record, flush=True)
            time.sleep(1.2)

(OUT / "results.json").write_text(json.dumps(records, indent=2, sort_keys=True), encoding="utf-8")
