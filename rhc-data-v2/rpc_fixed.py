#!/usr/bin/env python3
from __future__ import annotations

from typing import Any

from rpc import RangeLimitError, RpcClient, log_key


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
    failure = ""
    split_reason = ""
    try:
        rows = rpc.logs(filt)
        return rows, [{
            "from_block": from_block,
            "to_block": to_block,
            "depth": depth,
            "status": "PASS",
            "rows": len(rows),
        }]
    except RangeLimitError as exc:
        failure = repr(exc)
        split_reason = "RANGE_LIMIT"
    except Exception as exc:
        failure = repr(exc)
        split_reason = f"TRANSIENT_OR_UNKNOWN:{type(exc).__name__}"

    if from_block >= to_block or depth >= max_depth:
        return [], [{
            "from_block": from_block,
            "to_block": to_block,
            "depth": depth,
            "status": "FAIL",
            "rows": 0,
            "error": failure,
            "split_reason": split_reason,
        }]

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


__all__ = ["RpcClient", "RangeLimitError", "log_key", "fetch_logs_recursive"]
