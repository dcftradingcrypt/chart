#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import random
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any, Iterable

DEFAULT_RPC_URLS = ["https://rpc.mainnet.chain.robinhood.com"]
RANGE_ERROR_CODES = {-32005, -32016, -32602}
RETRYABLE_RPC_CODES = {429, -32603}
RETRYABLE_HTTP = {408, 425, 429, 500, 502, 503, 504}


class RangeLimitError(RuntimeError):
    pass


@dataclass
class RpcExhausted(RuntimeError):
    method: str
    params: list[Any]
    errors: list[dict[str, Any]]

    def __str__(self) -> str:
        return json.dumps({"method": self.method, "params": self.params, "errors": self.errors}, sort_keys=True)


class RpcClient:
    def __init__(self, min_interval: float = 1.0, timeout: int = 90, max_attempts: int = 10):
        configured = [value.strip() for value in os.getenv("RHC_RPC_URLS", "").split(",") if value.strip()]
        self.urls = configured or DEFAULT_RPC_URLS
        self.min_interval = min_interval
        self.timeout = timeout
        self.max_attempts = max_attempts
        self.last_request = 0.0
        self.stats: dict[str, int] = {}

    def _pace(self) -> None:
        remaining = self.min_interval - (time.monotonic() - self.last_request)
        if remaining > 0:
            time.sleep(remaining)

    def _count(self, key: str, amount: int = 1) -> None:
        self.stats[key] = self.stats.get(key, 0) + amount

    def _request(self, url: str, payload: Any) -> tuple[int, Any]:
        self._pace()
        body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        request = urllib.request.Request(
            url,
            data=body,
            method="POST",
            headers={
                "content-type": "application/json",
                "accept": "application/json",
                "user-agent": "RHC-Canonical-Data-V2/1.0",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                raw = response.read()
                status = response.status
            self.last_request = time.monotonic()
            self._count(f"http_{status}")
            return status, json.loads(raw.decode("utf-8"))
        except urllib.error.HTTPError as exc:
            self.last_request = time.monotonic()
            raw = exc.read(4000).decode("utf-8", "replace")
            self._count(f"http_{exc.code}")
            try:
                decoded = json.loads(raw)
            except Exception:
                decoded = {"raw": raw}
            return exc.code, decoded

    @staticmethod
    def _is_range_error(error: Any) -> bool:
        if not isinstance(error, dict):
            return False
        code = error.get("code")
        message = str(error.get("message") or "").lower()
        if code in RANGE_ERROR_CODES and any(term in message for term in ("range", "result", "limit", "response", "blocks", "query")):
            return True
        return any(term in message for term in (
            "query returned more than",
            "too many results",
            "block range",
            "response size",
            "range is too wide",
            "please limit",
        ))

    def call(self, method: str, params: list[Any]) -> Any:
        errors: list[dict[str, Any]] = []
        for attempt in range(1, self.max_attempts + 1):
            for url in self.urls:
                try:
                    status, decoded = self._request(url, {"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
                except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
                    errors.append({"url": url, "attempt": attempt, "network_or_decode": repr(exc)})
                    self._count("network_or_decode")
                    continue
                if status in RETRYABLE_HTTP:
                    errors.append({"url": url, "attempt": attempt, "http": status, "body": decoded})
                    continue
                if status != 200:
                    errors.append({"url": url, "attempt": attempt, "http": status, "body": decoded})
                    raise RpcExhausted(method, params, errors)
                if not isinstance(decoded, dict):
                    errors.append({"url": url, "attempt": attempt, "invalid_response": decoded})
                    continue
                if decoded.get("error") is not None:
                    error = decoded["error"]
                    if self._is_range_error(error):
                        self._count("range_limit")
                        raise RangeLimitError(json.dumps(error, sort_keys=True))
                    code = error.get("code") if isinstance(error, dict) else None
                    errors.append({"url": url, "attempt": attempt, "rpc_error": error})
                    self._count(f"rpc_error_{code}")
                    if code in RETRYABLE_RPC_CODES or "rate" in str(error).lower() or "temporar" in str(error).lower():
                        continue
                    raise RpcExhausted(method, params, errors)
                if "result" not in decoded:
                    errors.append({"url": url, "attempt": attempt, "missing_result": decoded})
                    continue
                return decoded["result"]
            if attempt < self.max_attempts:
                time.sleep(min(90.0, 2 ** min(attempt, 6) + random.random() * 3))
        raise RpcExhausted(method, params, errors)

    def batch(self, calls: list[tuple[str, list[Any], str]], batch_size: int = 20) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        output: dict[str, Any] = {}
        failures: list[dict[str, Any]] = []
        for start in range(0, len(calls), batch_size):
            chunk = calls[start:start + batch_size]
            payload = [
                {"jsonrpc": "2.0", "id": index, "method": method, "params": params}
                for index, (method, params, _) in enumerate(chunk)
            ]
            completed = False
            errors: list[dict[str, Any]] = []
            for attempt in range(1, self.max_attempts + 1):
                for url in self.urls:
                    try:
                        status, decoded = self._request(url, payload)
                    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
                        errors.append({"url": url, "attempt": attempt, "network_or_decode": repr(exc)})
                        continue
                    if status in RETRYABLE_HTTP:
                        errors.append({"url": url, "attempt": attempt, "http": status, "body": decoded})
                        continue
                    if status != 200 or not isinstance(decoded, list):
                        errors.append({"url": url, "attempt": attempt, "invalid_batch": decoded, "http": status})
                        continue
                    by_id = {int(row.get("id", -1)): row for row in decoded if isinstance(row, dict)}
                    for index, (_, _, key) in enumerate(chunk):
                        row = by_id.get(index)
                        if row is None or row.get("error") is not None:
                            failures.append({"key": key, "row": row, "batch_start": start})
                        else:
                            output[key] = row.get("result")
                    completed = True
                    break
                if completed:
                    break
                if attempt < self.max_attempts:
                    time.sleep(min(90.0, 2 ** min(attempt, 6) + random.random() * 3))
            if not completed:
                # Fall back to individual calls so one malformed item cannot poison a batch.
                for method, params, key in chunk:
                    try:
                        output[key] = self.call(method, params)
                    except Exception as exc:
                        failures.append({"key": key, "method": method, "params": params, "error": repr(exc), "batch_errors": errors})
            print({"batch_done": min(start + len(chunk), len(calls)), "batch_total": len(calls), "failures": len(failures)}, flush=True)
        return output, failures

    def block_number(self) -> int:
        return int(self.call("eth_blockNumber", []), 16)

    def block(self, block_number: int) -> dict[str, Any]:
        value = self.call("eth_getBlockByNumber", [hex(block_number), False])
        if not isinstance(value, dict):
            raise RuntimeError(f"block not found: {block_number}")
        return value

    def logs(self, filt: dict[str, Any]) -> list[dict[str, Any]]:
        value = self.call("eth_getLogs", [filt])
        if not isinstance(value, list):
            raise RuntimeError(f"eth_getLogs returned {type(value)!r}")
        return [row for row in value if isinstance(row, dict)]


def log_key(log: dict[str, Any]) -> tuple[str, str, int]:
    return (
        str(log.get("blockHash") or "").lower(),
        str(log.get("transactionHash") or "").lower(),
        int(str(log.get("logIndex") or "0x0"), 16),
    )


def fetch_logs_recursive(
    rpc: RpcClient,
    *,
    from_block: int,
    to_block: int,
    address: str | list[str] | None = None,
    topics: list[Any] | None = None,
    depth: int = 0,
    max_depth: int = 40,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    filt: dict[str, Any] = {"fromBlock": hex(from_block), "toBlock": hex(to_block)}
    if address is not None:
        filt["address"] = address
    if topics is not None:
        filt["topics"] = topics
    try:
        rows = rpc.logs(filt)
        return rows, [{"from_block": from_block, "to_block": to_block, "depth": depth, "status": "PASS", "rows": len(rows)}]
    except RangeLimitError as exc:
        split_reason = "RANGE_LIMIT"
    except Exception as exc:
        split_reason = f"TRANSIENT_OR_UNKNOWN:{type(exc).__name__}"
    if from_block >= to_block or depth >= max_depth:
        return [], [{"from_block": from_block, "to_block": to_block, "depth": depth, "status": "FAIL", "rows": 0, "error": repr(exc), "split_reason": split_reason}]
    midpoint = (from_block + to_block) // 2
    left_rows, left_coverage = fetch_logs_recursive(
        rpc,
        from_block=from_block,
        to_block=midpoint,
        address=address,
        topics=topics,
        depth=depth + 1,
        max_depth=max_depth,
    )
    right_rows, right_coverage = fetch_logs_recursive(
        rpc,
        from_block=midpoint + 1,
        to_block=to_block,
        address=address,
        topics=topics,
        depth=depth + 1,
        max_depth=max_depth,
    )
    return left_rows + right_rows, left_coverage + right_coverage
