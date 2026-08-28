#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
from collections import defaultdict
from pathlib import Path
from typing import Any


def integer(value: Any) -> int:
    try:
        text = str(value or "0")
        return int(text, 16) if text.startswith("0x") else int(float(text))
    except Exception:
        return 0


def topic_address(topic: str) -> str:
    return "0x" + topic[-40:].lower()


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-root", required=True)
    parser.add_argument("--known-wallets", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    root = Path(args.input_root)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    expected_targets = {
        "seadrop",
        "seaport",
        "erc721_mint",
        "erc1155_single_mint",
        "erc1155_batch_mint",
        "erc2309_mint",
    }
    validations: dict[str, dict[str, Any]] = {}
    events_by_target: dict[str, list[dict[str, str]]] = {}
    for validation_path in root.rglob("VALIDATION.json"):
        try:
            value = json.loads(validation_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        target = value.get("target")
        if target in expected_targets:
            if target in validations:
                raise SystemExit(f"duplicate discovery validation for {target}")
            validations[target] = value
            events_path = validation_path.parent / "events.csv"
            if not events_path.exists():
                raise SystemExit(f"missing events.csv for {target}")
            events_by_target[target] = read_csv(events_path)

    missing = sorted(expected_targets - set(validations))
    failed = {target: value for target, value in validations.items() if value.get("status") != "PASS"}
    fixed_heads = {int(value.get("fixed_head_block")) for value in validations.values()}
    fixed_hashes = {str(value.get("fixed_head_hash") or "").lower() for value in validations.values()}
    if missing or failed or len(fixed_heads) != 1 or len(fixed_hashes) != 1:
        raise SystemExit(json.dumps({
            "code": "DISCOVERY_INPUT_NOT_CANONICAL_COMPLETE",
            "missing": missing,
            "failed": failed,
            "fixed_heads": sorted(fixed_heads),
            "fixed_hashes": sorted(fixed_hashes),
        }, sort_keys=True))

    evidence: dict[str, list[dict[str, Any]]] = defaultdict(list)
    standards: dict[str, set[str]] = defaultdict(set)

    for target, standard in (
        ("erc721_mint", "ERC721"),
        ("erc2309_mint", "ERC721"),
        ("erc1155_single_mint", "ERC1155"),
        ("erc1155_batch_mint", "ERC1155"),
    ):
        for row in events_by_target[target]:
            contract = row.get("contract", "").lower()
            if not contract:
                continue
            standards[contract].add(standard)
            evidence[contract].append({
                "source": target,
                "block_number": integer(row.get("block_number")),
                "transaction_hash": row.get("transaction_hash", "").lower(),
                "log_index": integer(row.get("log_index")),
            })

    # SeaDropMint indexes the NFT contract in topics[1].
    for row in events_by_target["seadrop"]:
        topics = json.loads(row.get("topics_json") or "[]")
        if len(topics) < 2:
            continue
        contract = topic_address(topics[1])
        standards[contract].add("ERC721_OR_1155_SEADROP")
        evidence[contract].append({
            "source": "seadrop",
            "block_number": integer(row.get("block_number")),
            "transaction_hash": row.get("transaction_hash", "").lower(),
            "log_index": integer(row.get("log_index")),
        })

    known_wallet_rows = json.loads(Path(args.known_wallets).read_text(encoding="utf-8"))
    known_contracts: set[str] = set()
    for wallet_row in known_wallet_rows:
        for group in ("known_mints", "known_sales"):
            for item in wallet_row.get(group) or []:
                contract = str(item.get("contract") or item.get("nft_contract") or "").lower()
                if contract.startswith("0x") and len(contract) == 42:
                    known_contracts.add(contract)
                    evidence[contract].append({
                        "source": f"candidate_{group}",
                        "wallet": wallet_row.get("wallet"),
                        "transaction_hash": item.get("tx_hash"),
                        "token_id": item.get("token_id"),
                    })

    rows: list[dict[str, Any]] = []
    for contract in sorted(set(standards) | known_contracts):
        source_names = sorted({item["source"] for item in evidence[contract]})
        rows.append({
            "contract": contract,
            "standards_observed_json": json.dumps(sorted(standards.get(contract) or {"UNKNOWN_FROM_KNOWN_EVIDENCE"})),
            "discovery_sources_json": json.dumps(source_names),
            "discovery_evidence_rows": len(evidence[contract]),
            "first_observed_block": min((integer(item.get("block_number")) for item in evidence[contract] if item.get("block_number") is not None), default=None),
            "has_standard_mint_log": bool(standards.get(contract)),
            "has_candidate_known_evidence": contract in known_contracts,
            "fixed_head_block": next(iter(fixed_heads)),
            "fixed_head_hash": next(iter(fixed_hashes)),
            "classification_status": "UNCLASSIFIED_NFT_CONTRACT",
            "production_use": False,
        })

    fields = list(rows[0]) if rows else ["contract"]
    with (out / "contracts.csv").open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
    (out / "contract_evidence.json").write_text(json.dumps(evidence, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")

    validation = {
        "status": "PASS" if rows and not missing and not failed else "FAIL",
        "chain_id": 4663,
        "fixed_head_block": next(iter(fixed_heads)) if fixed_heads else None,
        "fixed_head_hash": next(iter(fixed_hashes)) if fixed_hashes else None,
        "discovery_targets": sorted(expected_targets),
        "contract_rows": len(rows),
        "erc721_observed": sum("ERC721" in standards.get(row["contract"], set()) for row in rows),
        "erc1155_observed": sum("ERC1155" in standards.get(row["contract"], set()) for row in rows),
        "seadrop_contracts": sum("seadrop" in {item["source"] for item in evidence[row["contract"]]} for row in rows),
        "known_evidence_contracts": len(known_contracts),
        "production_approved_contracts": 0,
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
