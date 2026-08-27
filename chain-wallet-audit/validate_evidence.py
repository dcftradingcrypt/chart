#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import re
import sys
from pathlib import Path

OUT = Path("out-chain-wallet-audit")
CANDIDATES = Path("chain-wallet-audit/candidates.csv")
EVIDENCE = Path("chain-wallet-audit/candidate_event_evidence.csv")
ADDR = re.compile(r"^0x[a-f0-9]{40}$")
TX = re.compile(r"^0x[a-f0-9]{64}$")


def rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8-sig") as file:
        return list(csv.DictReader(file))


def integer(value: object, default: int = 0) -> int:
    try:
        return int(float(str(value)))
    except Exception:
        return default


def main() -> None:
    failures: list[dict[str, object]] = []
    required_outputs = [OUT / "validation.json", OUT / "wallet_summary.csv", OUT / "transfers.csv"]
    for path in required_outputs:
        if not path.exists():
            failures.append({"code": "MISSING_OUTPUT", "path": str(path)})

    if failures:
        report = {"status": "FAIL", "failures": failures}
        (OUT / "evidence_gate.json").write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
        raise SystemExit(1)

    validation = json.loads((OUT / "validation.json").read_text(encoding="utf-8"))
    summaries = rows(OUT / "wallet_summary.csv")
    candidates = rows(CANDIDATES)

    # A provider returning an empty first page is not proof that a known
    # SeaDrop minter has no history. Zero rows must fail closed.
    if integer(validation.get("transaction_rows")) == 0:
        failures.append({"code": "EMPTY_TRANSACTION_HISTORY_FALSE_PASS"})
    if integer(validation.get("transfer_rows")) == 0:
        failures.append({"code": "EMPTY_TRANSFER_HISTORY_FALSE_PASS"})
    if validation.get("status") != "PASS":
        failures.append({"code": "UPSTREAM_AUDIT_NOT_PASS", "value": validation.get("status")})

    candidate_wallets = [row.get("wallet", "").lower() for row in candidates]
    if len(candidate_wallets) != len(set(candidate_wallets)):
        failures.append({"code": "DUPLICATE_CANDIDATE_WALLET"})
    if len(summaries) != len(candidates):
        failures.append({"code": "CANDIDATE_SUMMARY_COUNT_MISMATCH", "candidates": len(candidates), "summaries": len(summaries)})

    for row in summaries:
        wallet = row.get("wallet", "").lower()
        projects = integer(row.get("strict_paid_public_projects"))
        transfers = integer(row.get("transfer_rows_fetched"))
        mint_receipts = integer(row.get("zero_address_nft_receives"))
        contracts = integer(row.get("unique_nft_contracts"))
        if not ADDR.fullmatch(wallet):
            failures.append({"code": "INVALID_WALLET", "wallet": wallet})
        if projects <= 0:
            failures.append({"code": "NO_CLAIMED_PROJECTS", "wallet": wallet})
        if transfers <= 0:
            failures.append({"code": "CANDIDATE_WITHOUT_TRANSFER_EVIDENCE", "wallet": wallet})
        if mint_receipts < projects:
            failures.append({"code": "MINT_RECEIPTS_BELOW_PROJECT_COUNT", "wallet": wallet, "receipts": mint_receipts, "projects": projects})
        if contracts < projects:
            failures.append({"code": "UNIQUE_CONTRACTS_BELOW_PROJECT_COUNT", "wallet": wallet, "contracts": contracts, "projects": projects})

    # Aggregate-only candidates are not auditable. The evidence file is a
    # mandatory, immutable source table generated from canonical logs.
    if not EVIDENCE.exists():
        failures.append({"code": "MISSING_CANDIDATE_EVENT_EVIDENCE", "required_path": str(EVIDENCE)})
    else:
        evidence = rows(EVIDENCE)
        required = {
            "wallet", "nft_contract", "block_number", "transaction_hash", "log_index",
            "minter", "payer", "quantity", "unit_mint_price_wei", "drop_stage_index",
            "receipt_status", "zero_transfer_recipient", "zero_transfer_count"
        }
        if evidence and not required.issubset(evidence[0]):
            failures.append({"code": "EVIDENCE_SCHEMA_MISSING_FIELDS", "missing": sorted(required - set(evidence[0]))})
        seen: set[tuple[str, str]] = set()
        by_wallet_contract: set[tuple[str, str]] = set()
        for row in evidence:
            wallet = row.get("wallet", "").lower()
            contract = row.get("nft_contract", "").lower()
            tx_hash = row.get("transaction_hash", "").lower()
            log_index = row.get("log_index", "")
            key = (tx_hash, log_index)
            if key in seen:
                failures.append({"code": "DUPLICATE_EVIDENCE_EVENT", "tx_hash": tx_hash, "log_index": log_index})
            seen.add(key)
            by_wallet_contract.add((wallet, contract))
            if wallet != row.get("minter", "").lower():
                failures.append({"code": "WALLET_NOT_EVENT_MINTER", "wallet": wallet, "tx_hash": tx_hash})
            if row.get("zero_transfer_recipient", "").lower() != wallet or integer(row.get("zero_transfer_count")) <= 0:
                failures.append({"code": "NO_MATCHING_ZERO_ADDRESS_TRANSFER", "wallet": wallet, "tx_hash": tx_hash})
            if integer(row.get("unit_mint_price_wei")) <= 0 or integer(row.get("drop_stage_index"), -1) != 0:
                failures.append({"code": "NOT_STRICT_PAID_PUBLIC", "wallet": wallet, "tx_hash": tx_hash})
            if row.get("receipt_status", "").lower() not in {"1", "success", "ok"}:
                failures.append({"code": "RECEIPT_NOT_SUCCESS", "wallet": wallet, "tx_hash": tx_hash})
            if not ADDR.fullmatch(wallet) or not ADDR.fullmatch(contract) or not TX.fullmatch(tx_hash):
                failures.append({"code": "INVALID_EVIDENCE_IDENTIFIER", "wallet": wallet, "contract": contract, "tx_hash": tx_hash})

        expected = sum(integer(row.get("strict_paid_public_projects")) for row in candidates)
        if len(by_wallet_contract) != expected:
            failures.append({"code": "AGGREGATE_NOT_REPRODUCIBLE", "expected_wallet_project_pairs": expected, "evidence_wallet_project_pairs": len(by_wallet_contract)})

    report = {
        "status": "PASS" if not failures else "FAIL",
        "candidate_count": len(candidates),
        "summary_count": len(summaries),
        "upstream_transaction_rows": integer(validation.get("transaction_rows")),
        "upstream_transfer_rows": integer(validation.get("transfer_rows")),
        "failure_count": len(failures),
        "failures": failures,
        "production_approved_wallets": 0,
    }
    (OUT / "evidence_gate.json").write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(report, sort_keys=True))
    if failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
