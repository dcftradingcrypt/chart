#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import random
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any

DEFAULT_RPC_URLS = [
    "https://rpc.mainnet.chain.robinhood.com",
]


@dataclass
class RpcFailure(RuntimeError):
    method: str
    params: list[Any]
    errors: list[dict[str, Any]]

    def __str__(self) -> str:
        return json.dumps({"method": self.method, "params": self.params, "errors": self.errors}, sort_keys=True)


class CanonicalRpc:
    def __init__(self, min_interval: float = 1.35, max_attempts: int = 12, timeout: int = 90):
        configured = [x.strip() for x in os.getenv("RHC_RPC_URLS", "").split(",") if x.strip()]
        self.urls = configured or DEFAULT_RPC_URLS
        self.min_interval = min_interval
        self.max_attempts = max_attempts
        self.timeout = timeout
        self.last_request = 0.0
        self.stats: dict[str, int] = {}

    def _pace(self) -> None:
        remaining = self.min_interval - (time.monotonic() - self.last_request)
        if remaining > 0:
            time.sleep(remaining)

    def _bump(self, key: str) -> None:
        self.stats[key] = self.stats.get(key, 0) + 1

    def call(self, method: str, params: list[Any]) -> Any:
        errors: list[dict[str, Any]] = []
        for attempt in range(1, self.max_attempts + 1):
            for url in self.urls:
                self._pace()
                payload = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode("utf-8")
                request = urllib.request.Request(
                    url,
                    data=payload,
                    method="POST",
                    headers={
                        "content-type": "application/json",
                        "accept": "application/json",
                        "user-agent": "RHC-Canonical-Data-Completion/1.0",
                    },
                )
                try:
                    with urllib.request.urlopen(request, timeout=self.timeout) as response:
                        body = response.read()
                        status = response.status
                    self.last_request = time.monotonic()
                    self._bump(f"http_{status}")
                    decoded = json.loads(body.decode("utf-8"))
                    if isinstance(decoded, dict) and decoded.get("error") is not None:
                        error = decoded["error"]
                        code = error.get("code") if isinstance(error, dict) else None
                        errors.append({"url": url, "attempt": attempt, "rpc_error": error})
                        self._bump(f"rpc_error_{code}")
                        if code in (429, -32005, -32016, -32603) or "limit" in str(error).lower() or "too many" in str(error).lower():
                            continue
                        raise RpcFailure(method, params, errors)
                    if not isinstance(decoded, dict) or "result" not in decoded:
                        errors.append({"url": url, "attempt": attempt, "invalid_response": decoded})
                        continue
                    return decoded["result"]
                except urllib.error.HTTPError as exc:
                    self.last_request = time.monotonic()
                    body = exc.read(2000).decode("utf-8", "replace")
                    errors.append({"url": url, "attempt": attempt, "http": exc.code, "body": body})
                    self._bump(f"http_{exc.code}")
                    if exc.code not in (408, 425, 429, 500, 502, 503, 504):
                        raise RpcFailure(method, params, errors) from exc
                except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
                    self.last_request = time.monotonic()
                    errors.append({"url": url, "attempt": attempt, "network_or_decode": repr(exc)})
                    self._bump("network_or_decode")
            if attempt < self.max_attempts:
                delay = min(90.0, (2 ** min(attempt, 6)) + random.random() * 3)
                time.sleep(delay)
        raise RpcFailure(method, params, errors)

    def block_number(self) -> int:
        return int(self.call("eth_blockNumber", []), 16)

    def block(self, block_number: int) -> dict[str, Any]:
        value = self.call("eth_getBlockByNumber", [hex(block_number), False])
        if not isinstance(value, dict):
            raise RuntimeError(f"missing block {block_number}")
        return value

    def logs(self, filt: dict[str, Any]) -> list[dict[str, Any]]:
        value = self.call("eth_getLogs", [filt])
        if not isinstance(value, list):
            raise RuntimeError(f"eth_getLogs returned {type(value)!r}")
        return [x for x in value if isinstance(x, dict)]


def log_key(log: dict[str, Any]) -> tuple[str, str, int]:
    return (
        str(log.get("blockHash") or "").lower(),
        str(log.get("transactionHash") or "").lower(),
        int(str(log.get("logIndex") or "0x0"), 16),
    )


def fetch_logs_recursive(
    rpc: CanonicalRpc,
    *,
    from_block: int,
    to_block: int,
    address: str | None = None,
    topics: list[Any] | None = None,
    max_depth: int = 30,
    depth: int = 0,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    filt: dict[str, Any] = {"fromBlock": hex(from_block), "toBlock": hex(to_block)}
    if address:
        filt["address"] = address
    if topics is not None:
        filt["topics"] = topics
    try:
        rows = rpc.logs(filt)
        return rows, [{"from_block": from_block, "to_block": to_block, "status": "PASS", "rows": len(rows), "depth": depth}]
    except Exception as exc:
        if from_block >= to_block or depth >= max_depth:
            return [], [{"from_block": from_block, "to_block": to_block, "status": "FAIL", "rows": 0, "depth": depth, "error": repr(exc)}]
        midpoint = (from_block + to_block) // 2
        left_rows, left_cov = fetch_logs_recursive(
            rpc,
            from_block=from_block,
            to_block=midpoint,
            address=address,
            topics=topics,
            max_depth=max_depth,
            depth=depth + 1,
        )
        right_rows, right_cov = fetch_logs_recursive(
            rpc,
            from_block=midpoint + 1,
            to_block=to_block,
            address=address,
            topics=topics,
            max_depth=max_depth,
            depth=depth + 1,
        )
        return left_rows + right_rows, left_cov + right_cov
