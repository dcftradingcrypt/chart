#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import re
from pathlib import Path

OUT = Path("out-chain-wallet-audit")
CANDIDATES = Path("chain-wallet-audit/candidates.csv")
EVIDENCE = Path("chain-wallet-audit/candidate_event_evidence.csv")
ADDR = re.compile(r"^0x[a-f0-9]{40}$")
HASH32 = re.compile(r"^0x[a-f0-9]{64}$")
SHA256 = re.compile(r"^[a-f0-9]{64}$")
TRUE_VALUES = {"1", "true", "yes", "pass"}
SUCCESS_VALUES = {"1", "success", "ok"}
SELF_FUNDED = {"SELF_FUNDED", "SAME_ENTITY_PROVEN"}


def rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8-sig") as file:
        return list(csv.DictReader(file))


def integer(value: object, default: int = 0) -> int:
    try:
        return int(str(value), 0)
    except Exception:
        try:
            return int(float(str(value)))
        except Exception:
            return default


def boolean(value: object) -> bool:
    return str(value).strip().lower() in TRUE_VALUES


def fail_report(failures: list[dict[str, object]], **extra: object) -> None:
    report = {
        "status": "FAIL" if failures else "PASS",
        "failure_count": len(failures),
        "failures": failures,
        "production_approved_wallets": 0,
        **extra,
    }
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "evidence_gate.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    print(json.dumps(report, sort_keys=True))
    if failures:
        raise SystemExit(1)


def main() -> None:
    failures: list[dict[str, object]] = []
    required_outputs = [OUT / "validation.json", OUT / "wallet_summary.csv", OUT / "transfers.csv"]
    for path in required_outputs:
        if not path.exists():
            failures.append({"code": "MISSING_OUTPUT", "path": str(path)})
    if failures:
        fail_report(failures)

    validation = json.loads((OUT / "validation.json").read_text(encoding="utf-8"))
    summaries = rows(OUT / "wallet_summary.csv")
    candidates = rows(CANDIDATES)

    # Empty indexer responses are not absence proofs for wallets that were
    # supposedly derived from canonical mint events.
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
        failures.append({
            "code": "CANDIDATE_SUMMARY_COUNT_MISMATCH",
            "candidates": len(candidates),
            "summaries": len(summaries),
        })

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
            failures.append({
                "code": "MINT_RECEIPTS_BELOW_PROJECT_COUNT",
                "wallet": wallet,
                "receipts": mint_receipts,
                "projects": projects,
            })
        if contracts < projects:
            failures.append({
                "code": "UNIQUE_CONTRACTS_BELOW_PROJECT_COUNT",
                "wallet": wallet,
                "contracts": contracts,
                "projects": projects,
            })

    # Aggregate-only candidates are not auditable. This table must be
    # generated from canonical receipts/logs, not entered by hand.
    evidence: list[dict[str, str]] = []
    if not EVIDENCE.exists():
        failures.append({
            "code": "MISSING_CANDIDATE_EVENT_EVIDENCE",
            "required_path": str(EVIDENCE),
        })
    else:
        evidence = rows(EVIDENCE)
        required = {
            "chain_id",
            "project_entity_id",
            "wallet",
            "nft_contract",
            "token_standard",
            "primary_route",
            "project_eligibility_status",
            "all_zero_cost_allocations_reconciled",
            "block_number",
            "block_hash",
            "canonicality_status",
            "transaction_hash",
            "log_index",
            "tx_from",
            "minter",
            "payer",
            "economic_entity_status",
            "payer_entity_relation",
            "payer_relation_evidence_id",
            "erc4337_sender",
            "paymaster",
            "quantity",
            "unit_mint_price_wei",
            "drop_stage_index",
            "receipt_status",
            "zero_transfer_recipient",
            "zero_transfer_count",
            "zero_transfer_quantity",
            "source_head_block",
            "source_manifest_sha256",
        }
        fields = set(evidence[0]) if evidence else set()
        if not evidence:
            failures.append({"code": "EMPTY_CANDIDATE_EVENT_EVIDENCE"})
        if not required.issubset(fields):
            failures.append({
                "code": "EVIDENCE_SCHEMA_MISSING_FIELDS",
                "missing": sorted(required - fields),
            })

        seen: set[tuple[str, str, str, str]] = set()
        by_wallet_project: set[tuple[str, str]] = set()
        for row in evidence:
            chain_id = integer(row.get("chain_id"), -1)
            wallet = row.get("wallet", "").lower()
            contract = row.get("nft_contract", "").lower()
            project_entity_id = row.get("project_entity_id", "").strip()
            block_hash = row.get("block_hash", "").lower()
            tx_hash = row.get("transaction_hash", "").lower()
            log_index = row.get("log_index", "")
            key = (str(chain_id), block_hash, tx_hash, log_index)
            if key in seen:
                failures.append({
                    "code": "DUPLICATE_EVIDENCE_EVENT",
                    "block_hash": block_hash,
                    "tx_hash": tx_hash,
                    "log_index": log_index,
                })
            seen.add(key)
            by_wallet_project.add((wallet, project_entity_id))

            if chain_id != 4663:
                failures.append({"code": "WRONG_CHAIN", "wallet": wallet, "chain_id": chain_id})
            if not project_entity_id:
                failures.append({"code": "MISSING_PROJECT_ENTITY_ID", "wallet": wallet, "tx_hash": tx_hash})
            if row.get("project_eligibility_status") != "STRICT_COMPARABLE_PASS":
                failures.append({"code": "PROJECT_NOT_STRICT_COMPARABLE", "wallet": wallet, "tx_hash": tx_hash})
            if not boolean(row.get("all_zero_cost_allocations_reconciled")):
                failures.append({"code": "ZERO_COST_ALLOCATIONS_UNRESOLVED", "wallet": wallet, "tx_hash": tx_hash})
            if row.get("primary_route") != "SEADROP_PUBLIC":
                failures.append({"code": "PRIMARY_ROUTE_NOT_PUBLIC_SEADROP", "wallet": wallet, "tx_hash": tx_hash})
            if row.get("canonicality_status") != "FINALIZED_CANONICAL":
                failures.append({"code": "EVENT_NOT_FINALIZED_CANONICAL", "wallet": wallet, "tx_hash": tx_hash})

            minter = row.get("minter", "").lower()
            payer = row.get("payer", "").lower()
            recipient = row.get("zero_transfer_recipient", "").lower()
            entity_status = row.get("economic_entity_status", "")
            payer_relation = row.get("payer_entity_relation", "")
            if wallet != minter or wallet != recipient:
                failures.append({"code": "WALLET_ROLE_MISMATCH", "wallet": wallet, "minter": minter, "recipient": recipient, "tx_hash": tx_hash})
            if payer == wallet:
                if entity_status not in SELF_FUNDED:
                    failures.append({"code": "SELF_PAYER_WITH_INVALID_ENTITY_STATUS", "wallet": wallet, "tx_hash": tx_hash})
            else:
                if entity_status != "SAME_ENTITY_PROVEN" or payer_relation != "SAME_ENTITY_PROVEN" or not row.get("payer_relation_evidence_id", "").strip():
                    failures.append({"code": "PAYER_MINTER_RELATION_UNPROVEN", "wallet": wallet, "payer": payer, "tx_hash": tx_hash})

            quantity = integer(row.get("quantity"))
            transfer_quantity = integer(row.get("zero_transfer_quantity"))
            if integer(row.get("zero_transfer_count")) <= 0 or quantity <= 0 or transfer_quantity != quantity:
                failures.append({"code": "MINT_TRANSFER_QUANTITY_MISMATCH", "wallet": wallet, "quantity": quantity, "zero_transfer_quantity": transfer_quantity, "tx_hash": tx_hash})
            if integer(row.get("unit_mint_price_wei")) <= 0 or integer(row.get("drop_stage_index"), -1) != 0:
                failures.append({"code": "NOT_STRICT_PAID_PUBLIC", "wallet": wallet, "tx_hash": tx_hash})
            if row.get("receipt_status", "").lower() not in SUCCESS_VALUES:
                failures.append({"code": "RECEIPT_NOT_SUCCESS", "wallet": wallet, "tx_hash": tx_hash})
            if integer(row.get("source_head_block")) < integer(row.get("block_number")):
                failures.append({"code": "SOURCE_HEAD_BEFORE_EVENT", "wallet": wallet, "tx_hash": tx_hash})

            source_manifest = row.get("source_manifest_sha256", "").lower()
            identifiers_ok = (
                ADDR.fullmatch(wallet)
                and ADDR.fullmatch(contract)
                and ADDR.fullmatch(row.get("tx_from", "").lower())
                and ADDR.fullmatch(minter)
                and ADDR.fullmatch(payer)
                and HASH32.fullmatch(block_hash)
                and HASH32.fullmatch(tx_hash)
                and SHA256.fullmatch(source_manifest)
            )
            if not identifiers_ok:
                failures.append({"code": "INVALID_EVIDENCE_IDENTIFIER", "wallet": wallet, "contract": contract, "tx_hash": tx_hash})

        expected = sum(integer(row.get("strict_paid_public_projects")) for row in candidates)
        if len(by_wallet_project) != expected:
            failures.append({
                "code": "AGGREGATE_NOT_REPRODUCIBLE_BY_PROJECT_ENTITY",
                "expected_wallet_project_pairs": expected,
                "evidence_wallet_project_pairs": len(by_wallet_project),
            })

    fail_report(
        failures,
        candidate_count=len(candidates),
        summary_count=len(summaries),
        evidence_rows=len(evidence),
        upstream_transaction_rows=integer(validation.get("transaction_rows")),
        upstream_transfer_rows=integer(validation.get("transfer_rows")),
    )


if __name__ == "__main__":
    main()
