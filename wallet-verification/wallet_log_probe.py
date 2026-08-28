#!/usr/bin/env python3
from __future__ import annotations

import json
import urllib.error
import urllib.request
from pathlib import Path

RPC = "https://rpc.mainnet.chain.robinhood.com"
TRANSFER = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
ZERO_TOPIC = "0x" + "0" * 64
TARGETS = [
    {
        "wallet": "0x01bdea1495c737fa416b337a0f4074ed68c730a6",
        "known_tx": "0x7b8a716d053e4a62bdf5fe00fbe68c2cae5ab76f284ba03504bffa5befa1d682",
        "known_block": 46771285,
    },
    {
        "wallet": "0x76d387388bea6b60ca6d1e97f446f7e26d39d313",
        "known_tx": "0x7d02ee071fe595521f1d09671f9b8ad9f0d2d7e1e2bfa017de53d27449cddd23",
        "known_block": 46802320,
    },
]
OUT = Path("out-wallet-log-probe")
OUT.mkdir(parents=True, exist_ok=True)


def rpc(method: str, params: list[object]) -> dict[str, object]:
    payload = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode()
    req = urllib.request.Request(
        RPC,
        data=payload,
        method="POST",
        headers={
            "content-type": "application/json",
            "accept": "application/json",
            "user-agent": "RHC-Wallet-Log-Probe/2.0",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as response:
            body = response.read()
            status = response.status
    except urllib.error.HTTPError as exc:
        return {"http_status": exc.code, "error": exc.read().decode("utf-8", "replace")[:3000]}
    except Exception as exc:
        return {"http_status": 0, "error": repr(exc)}
    try:
        decoded = json.loads(body.decode("utf-8"))
    except Exception as exc:
        return {"http_status": status, "error": f"decode:{exc!r}", "raw": body[:1000].decode("utf-8", "replace")}
    return {"http_status": status, **decoded}


def padded(address: str) -> str:
    return "0x" + "0" * 24 + address[2:].lower()


head_response = rpc("eth_blockNumber", [])
head_hex = str(head_response.get("result") or "0x0")
head = int(head_hex, 16)
records: list[dict[str, object]] = [{"probe": "head", "head": head, "response": head_response}]

for target in TARGETS:
    wallet = target["wallet"]
    topic_wallet = padded(wallet)
    receipt = rpc("eth_getTransactionReceipt", [target["known_tx"]])
    records.append({"probe": "known_receipt", **target, "response": receipt})

    filters = [
        (
            "exact_known_mint_window",
            target["known_block"] - 2,
            target["known_block"] + 2,
            [TRANSFER, ZERO_TOPIC, topic_wallet],
        ),
        (
            "incoming_recent_2m_blocks",
            max(0, head - 2_000_000),
            head,
            [TRANSFER, None, topic_wallet],
        ),
        (
            "outgoing_recent_2m_blocks",
            max(0, head - 2_000_000),
            head,
            [TRANSFER, topic_wallet],
        ),
        (
            "incoming_full_range",
            0,
            head,
            [TRANSFER, None, topic_wallet],
        ),
        (
            "outgoing_full_range",
            0,
            head,
            [TRANSFER, topic_wallet],
        ),
    ]
    for name, start, end, topics in filters:
        response = rpc(
            "eth_getLogs",
            [{"fromBlock": hex(start), "toBlock": hex(end), "topics": topics}],
        )
        result = response.get("result")
        rows = result if isinstance(result, list) else []
        record = {
            "probe": name,
            "wallet": wallet,
            "from_block": start,
            "to_block": end,
            "row_count": len(rows),
            "first_block": int(rows[0]["blockNumber"], 16) if rows else None,
            "last_block": int(rows[-1]["blockNumber"], 16) if rows else None,
            "http_status": response.get("http_status"),
            "rpc_error": response.get("error"),
        }
        records.append(record)
        print(record, flush=True)

(OUT / "results.json").write_text(json.dumps(records, indent=2, sort_keys=True), encoding="utf-8")
