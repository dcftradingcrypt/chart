#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
from pathlib import Path
from typing import Any

from abi import integer, parse_topics, topic_address
from rpc_fixed import RpcClient, fetch_logs_recursive, log_key
from topics import TRANSFER, padded_address

EXTERNAL_SOURCE_ONLY = {
    "0x2b5b35ac5a2d5c1224337ba86bf3816abee69da3",
    "0x4bc98b9112229ee07d85a6827d3bde713c8e7e24",
    "0xfaeb5d192a7336a6e635905d8d33a46adbba8513",
    "0xfe80a4f2d6456327663c6b76e167e598e1142364",
}


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fields: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for key in row:
            if key not in seen:
                seen.add(key)
                fields.append(key)
    if not fields:
        fields = ["empty"]
    with path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--wallets", required=True)
    parser.add_argument("--fixed-head", type=int, required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--shard", type=int, required=True)
    parser.add_argument("--shard-count", type=int, required=True)
    args = parser.parse_args()

    wallets = json.loads(Path(args.wallets).read_text(encoding="utf-8"))
    selected = [row for index, row in enumerate(wallets) if index % args.shard_count == args.shard]
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    rpc = RpcClient(min_interval=1.05, max_attempts=10)

    erc20_events: dict[tuple[str, str, int], dict[str, Any]] = {}
    account_rows: list[dict[str, Any]] = []
    coverage_rows: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []

    for wallet_row in selected:
        wallet = wallet_row["wallet"].lower()
        wallet_failures = []
        query_counts = {}
        for direction, topic_index in (("OUT", 1), ("IN", 2)):
            topics: list[Any] = [TRANSFER]
            while len(topics) <= topic_index:
                topics.append(None)
            topics[topic_index] = padded_address(wallet)
            rows, coverage = fetch_logs_recursive(
                rpc,
                from_block=0,
                to_block=args.fixed_head,
                topics=topics,
            )
            # ERC-20 Transfer has exactly three topics. ERC-721 uses four.
            rows = [row for row in rows if len(row.get("topics") or []) == 3]
            query_counts[direction] = len(rows)
            for part in coverage:
                record = {"wallet": wallet, "direction": direction, **part}
                coverage_rows.append(record)
                if part.get("status") != "PASS":
                    wallet_failures.append(record)
            for log in rows:
                topics_value = parse_topics(log.get("topics") or [])
                record = {
                    "wallet": wallet,
                    "priority": wallet_row.get("priority"),
                    "direction": direction,
                    "block_number": integer(log.get("blockNumber")),
                    "block_hash": str(log.get("blockHash") or "").lower(),
                    "transaction_hash": str(log.get("transactionHash") or "").lower(),
                    "transaction_index": integer(log.get("transactionIndex")),
                    "log_index": integer(log.get("logIndex")),
                    "token_contract": str(log.get("address") or "").lower(),
                    "from_address": topic_address(topics_value[1]),
                    "to_address": topic_address(topics_value[2]),
                    "amount_raw": integer(log.get("data")),
                    "removed": bool(log.get("removed", False)),
                }
                erc20_events[(record["block_hash"], record["transaction_hash"], record["log_index"])] = record

        calls = [
            ("eth_getCode", [wallet, hex(args.fixed_head)], f"code:{wallet}"),
            ("eth_getBalance", [wallet, hex(args.fixed_head)], f"balance:{wallet}"),
            ("eth_getTransactionCount", [wallet, hex(args.fixed_head)], f"nonce:{wallet}"),
        ]
        account_data, account_failures = rpc.batch(calls, batch_size=3)
        code = account_data.get(f"code:{wallet}")
        balance = account_data.get(f"balance:{wallet}")
        nonce = account_data.get(f"nonce:{wallet}")
        if account_failures:
            wallet_failures.extend(account_failures)
        account_rows.append({
            "wallet": wallet,
            "priority": wallet_row.get("priority"),
            "fixed_head_block": args.fixed_head,
            "code": code,
            "account_type": "EOA" if code in ("0x", "0x0", None) else "CONTRACT_OR_SMART_ACCOUNT",
            "native_balance_wei": integer(balance),
            "transaction_count": integer(nonce),
            "erc20_incoming_rows": query_counts.get("IN", 0),
            "erc20_outgoing_rows": query_counts.get("OUT", 0),
            "native_funding_history_status": "NOT_INDEXABLE_FROM_STANDARD_RPC",
            "expected_robinhood_activity": wallet not in EXTERNAL_SOURCE_ONLY,
            "status": "PASS" if not wallet_failures else "FAIL",
        })
        if wallet_failures:
            failures.append({"wallet": wallet, "failures": wallet_failures})
        print(account_rows[-1], flush=True)

    erc20_rows = sorted(erc20_events.values(), key=lambda row: (row["wallet"], row["block_number"], row["transaction_index"], row["log_index"]))
    # Candidate edges only. Shared service, CEX, bridge, router, and airdrop exclusions are unresolved here.
    edge_rows = [
        {
            "candidate_wallet": row["wallet"],
            "counterparty": row["from_address"] if row["direction"] == "IN" else row["to_address"],
            "direction": row["direction"],
            "token_contract": row["token_contract"],
            "amount_raw": row["amount_raw"],
            "block_number": row["block_number"],
            "transaction_hash": row["transaction_hash"],
            "edge_status": "UNCLASSIFIED_ERC20_RELATIONSHIP_NOT_ENTITY_PROOF",
        }
        for row in erc20_rows
    ]

    write_csv(out / "wallet_account_state.csv", account_rows)
    write_csv(out / "wallet_erc20_transfers.csv", erc20_rows)
    write_csv(out / "wallet_relationship_edges.csv", edge_rows)
    write_csv(out / "coverage.csv", coverage_rows)
    write_csv(out / "errors.csv", failures)

    validation = {
        "status": "PASS" if not failures else "FAIL",
        "chain_id": 4663,
        "fixed_head_block": args.fixed_head,
        "shard": args.shard,
        "shard_count": args.shard_count,
        "wallet_rows": len(selected),
        "wallets_passed": sum(row["status"] == "PASS" for row in account_rows),
        "wallets_failed": sum(row["status"] != "PASS" for row in account_rows),
        "erc20_transfer_rows": len(erc20_rows),
        "relationship_edge_rows": len(edge_rows),
        "native_funding_history_complete": False,
        "entity_clusters_proven": 0,
        "production_approved_wallets": 0,
        "failures": failures,
        "rpc_stats": rpc.stats,
    }
    (out / "VALIDATION.json").write_text(json.dumps(validation, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    manifest = []
    for path in sorted(out.iterdir()):
        if path.is_file():
            manifest.append({"path": path.name, "bytes": path.stat().st_size, "sha256": sha256(path)})
    (out / "MANIFEST.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(validation, sort_keys=True), flush=True)
    if validation["status"] != "PASS":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
