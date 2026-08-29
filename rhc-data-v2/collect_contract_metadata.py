#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import random
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

from abi import integer
from rpc_fixed import RpcClient

BLOCKSCOUT = "https://robinhoodchain.blockscout.com/api/v2"
OWNER_SELECTOR = "0x8da5cb5b"
SUPPORTS_INTERFACE_SELECTOR = "0x01ffc9a7"
INTERFACES = {
    "ERC165": "01ffc9a7",
    "ERC721": "80ac58cd",
    "ERC721_METADATA": "5b5e139f",
    "ERC1155": "d9b67a26",
    "ERC1155_METADATA_URI": "0e89341c",
}
EIP1967_IMPLEMENTATION_SLOT = "0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc"
EIP1967_ADMIN_SLOT = "0xb53127684a568b3173ae13b9f8a6016e019ea6e01d6a717850b5d6103"
EIP1967_BEACON_SLOT = "0xa3f0ad74e5423aebfd80d3ef4346578335a9a72aeaee59ff6cb3582b35133d50"


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


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
        for row in rows:
            writer.writerow({
                key: json.dumps(value, ensure_ascii=False, sort_keys=True)
                if isinstance(value, (dict, list, tuple)) else value
                for key, value in row.items()
            })


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def http_json(path: str, attempts: int = 8) -> tuple[int, Any, list[dict[str, Any]]]:
    url = BLOCKSCOUT.rstrip("/") + "/" + path.lstrip("/")
    errors: list[dict[str, Any]] = []
    for attempt in range(1, attempts + 1):
        request = urllib.request.Request(
            url,
            headers={
                "accept": "application/json",
                "user-agent": "RHC-Contract-Metadata/1.0",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=90) as response:
                body = response.read()
                status = response.status
            try:
                decoded = json.loads(body.decode("utf-8"))
            except Exception:
                decoded = {"raw": body.decode("utf-8", "replace")}
            return status, decoded, errors
        except urllib.error.HTTPError as exc:
            body = exc.read(4000).decode("utf-8", "replace")
            if exc.code == 404:
                return 404, {"not_found": True, "url": url}, errors
            errors.append({"attempt": attempt, "http": exc.code, "body": body, "url": url})
            if exc.code not in {408, 425, 429, 500, 502, 503, 504}:
                return exc.code, {"error": body, "url": url}, errors
        except Exception as exc:
            errors.append({"attempt": attempt, "error": repr(exc), "url": url})
        if attempt < attempts:
            time.sleep(min(60.0, 2 ** min(attempt, 5) + random.random() * 3))
    return 0, {"failed": True, "url": url}, errors


def call_data(interface_id: str) -> str:
    # bytes4 is left-aligned in its ABI word.
    return SUPPORTS_INTERFACE_SELECTOR + interface_id.lower() + "0" * 56


def decode_bool(value: Any) -> bool | None:
    if not isinstance(value, str) or not value.startswith("0x"):
        return None
    try:
        return int(value, 16) != 0
    except Exception:
        return None


def decode_address_word(value: Any) -> str | None:
    if not isinstance(value, str) or not value.startswith("0x"):
        return None
    raw = value[2:].rjust(64, "0")
    if len(raw) < 40:
        return None
    address = "0x" + raw[-40:].lower()
    if address == "0x" + "0" * 40:
        return None
    return address


def compact_abi(value: Any) -> str | None:
    if value is None:
        return None
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    except Exception:
        return str(value)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--contracts", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--shard", type=int, required=True)
    parser.add_argument("--shard-count", type=int, required=True)
    args = parser.parse_args()

    contracts = read_csv(Path(args.contracts))
    selected = [row for index, row in enumerate(contracts) if index % args.shard_count == args.shard]
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    raw_dir = out / "raw"
    raw_dir.mkdir(exist_ok=True)
    rpc = RpcClient(min_interval=0.95, max_attempts=10)

    summaries: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    creation_txs: list[dict[str, Any]] = []

    for contract_row in selected:
        contract = contract_row["contract"].lower()
        fixed_head = integer(contract_row.get("fixed_head_block"))
        calls: list[tuple[str, list[Any], str]] = [
            ("eth_getCode", [contract, hex(fixed_head)], "code"),
            ("eth_getStorageAt", [contract, EIP1967_IMPLEMENTATION_SLOT, hex(fixed_head)], "implementation_slot"),
            ("eth_getStorageAt", [contract, EIP1967_ADMIN_SLOT, hex(fixed_head)], "admin_slot"),
            ("eth_getStorageAt", [contract, EIP1967_BEACON_SLOT, hex(fixed_head)], "beacon_slot"),
            ("eth_call", [{"to": contract, "data": OWNER_SELECTOR}, hex(fixed_head)], "owner_call"),
        ]
        for name, interface_id in INTERFACES.items():
            calls.append(("eth_call", [{"to": contract, "data": call_data(interface_id)}, hex(fixed_head)], f"interface:{name}"))
        rpc_values, rpc_failures = rpc.batch(calls, batch_size=len(calls))
        for failure in rpc_failures:
            errors.append({"contract": contract, "stage": "rpc", **failure})

        address_status, address_data, address_errors = http_json(f"addresses/{contract}")
        smart_status, smart_data, smart_errors = http_json(f"smart-contracts/{contract}")
        token_status, token_data, token_errors = http_json(f"tokens/{contract}")
        for stage, values in (("address_api", address_errors), ("smart_contract_api", smart_errors), ("token_api", token_errors)):
            for value in values:
                errors.append({"contract": contract, "stage": stage, **value})
        raw = {
            "contract": contract,
            "fixed_head_block": fixed_head,
            "rpc": rpc_values,
            "blockscout": {
                "address": {"status": address_status, "data": address_data},
                "smart_contract": {"status": smart_status, "data": smart_data},
                "token": {"status": token_status, "data": token_data},
            },
        }
        (raw_dir / f"{contract}.json").write_text(json.dumps(raw, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")

        creator = None
        creation_tx_hash = None
        if isinstance(address_data, dict):
            creator_value = address_data.get("creator_address_hash") or address_data.get("creator_address")
            if isinstance(creator_value, dict):
                creator_value = creator_value.get("hash")
            if isinstance(creator_value, str):
                creator = creator_value.lower()
            tx_value = address_data.get("creation_tx_hash") or address_data.get("creation_transaction_hash")
            if isinstance(tx_value, str):
                creation_tx_hash = tx_value.lower()
        if creation_tx_hash:
            tx_values, tx_failures = rpc.batch([
                ("eth_getTransactionByHash", [creation_tx_hash], "tx"),
                ("eth_getTransactionReceipt", [creation_tx_hash], "receipt"),
            ], batch_size=2)
            for failure in tx_failures:
                errors.append({"contract": contract, "stage": "creation_tx", **failure})
            tx = tx_values.get("tx")
            receipt = tx_values.get("receipt")
            creation_txs.append({
                "contract": contract,
                "creation_transaction_hash": creation_tx_hash,
                "creator_address": creator,
                "transaction_present": isinstance(tx, dict),
                "receipt_present": isinstance(receipt, dict),
                "receipt_status": integer(receipt.get("status"), -1) if isinstance(receipt, dict) else None,
                "block_number": integer(receipt.get("blockNumber"), -1) if isinstance(receipt, dict) else None,
                "tx_from": str(tx.get("from") or "").lower() if isinstance(tx, dict) else None,
                "tx_to": str(tx.get("to") or "").lower() if isinstance(tx, dict) else None,
                "contract_address_created": str(receipt.get("contractAddress") or "").lower() if isinstance(receipt, dict) else None,
            })

        implementation = decode_address_word(rpc_values.get("implementation_slot"))
        admin = decode_address_word(rpc_values.get("admin_slot"))
        beacon = decode_address_word(rpc_values.get("beacon_slot"))
        owner = decode_address_word(rpc_values.get("owner_call"))
        smart = smart_data if isinstance(smart_data, dict) and not smart_data.get("not_found") else {}
        token = token_data if isinstance(token_data, dict) and not token_data.get("not_found") else {}
        implementations = smart.get("implementations") if isinstance(smart, dict) else None
        abi = smart.get("abi") if isinstance(smart, dict) else None
        summaries.append({
            "contract": contract,
            "fixed_head_block": fixed_head,
            "runtime_code_present": isinstance(rpc_values.get("code"), str) and rpc_values.get("code") not in {"0x", "0x0"},
            "runtime_code_sha256": hashlib.sha256(bytes.fromhex(str(rpc_values.get("code") or "0x")[2:])).hexdigest() if isinstance(rpc_values.get("code"), str) and str(rpc_values.get("code")).startswith("0x") else None,
            "supports_erc165": decode_bool(rpc_values.get("interface:ERC165")),
            "supports_erc721": decode_bool(rpc_values.get("interface:ERC721")),
            "supports_erc721_metadata": decode_bool(rpc_values.get("interface:ERC721_METADATA")),
            "supports_erc1155": decode_bool(rpc_values.get("interface:ERC1155")),
            "supports_erc1155_metadata_uri": decode_bool(rpc_values.get("interface:ERC1155_METADATA_URI")),
            "owner_call_address": owner,
            "eip1967_implementation": implementation,
            "eip1967_admin": admin,
            "eip1967_beacon": beacon,
            "proxy_storage_detected": any((implementation, admin, beacon)),
            "creator_address": creator,
            "creation_transaction_hash": creation_tx_hash,
            "blockscout_address_status": address_status,
            "blockscout_smart_contract_status": smart_status,
            "blockscout_token_status": token_status,
            "verified_source": bool(smart.get("is_verified")) if isinstance(smart, dict) else False,
            "is_proxy_blockscout": smart.get("is_proxy") if isinstance(smart, dict) else None,
            "implementations_json": implementations,
            "contract_name": smart.get("name") or smart.get("contract_name") if isinstance(smart, dict) else None,
            "compiler_version": smart.get("compiler_version") if isinstance(smart, dict) else None,
            "optimization_enabled": smart.get("optimization_enabled") if isinstance(smart, dict) else None,
            "abi_json": compact_abi(abi),
            "token_name": token.get("name") if isinstance(token, dict) else None,
            "token_symbol": token.get("symbol") if isinstance(token, dict) else None,
            "token_type": token.get("type") if isinstance(token, dict) else None,
            "token_total_supply": token.get("total_supply") if isinstance(token, dict) else None,
            "holders_count": token.get("holders_count") if isinstance(token, dict) else None,
            "admin_risk_status": "OWNER_OR_PROXY_ADMIN_PRESENT" if owner or admin else "NO_STANDARD_OWNER_OR_EIP1967_ADMIN_OBSERVED",
            "metadata_status": "COMPLETE_AVAILABLE_FIELDS" if address_status in {200, 404} and smart_status in {200, 404} and token_status in {200, 404} and not rpc_failures else "PARTIAL",
            "production_approved": False,
        })
        print({"contract": contract, "verified": summaries[-1]["verified_source"], "owner": owner, "implementation": implementation, "status": summaries[-1]["metadata_status"]}, flush=True)

    write_csv(out / "contract_metadata.csv", summaries)
    write_csv(out / "contract_creation_transactions.csv", creation_txs)
    write_csv(out / "errors.csv", errors)

    partial = [row for row in summaries if row["metadata_status"] != "COMPLETE_AVAILABLE_FIELDS"]
    validation = {
        "status": "PASS" if not partial else "FAIL",
        "chain_id": 4663,
        "shard": args.shard,
        "shard_count": args.shard_count,
        "contract_rows": len(selected),
        "metadata_rows": len(summaries),
        "verified_source_contracts": sum(bool(row["verified_source"]) for row in summaries),
        "proxy_storage_contracts": sum(bool(row["proxy_storage_detected"]) for row in summaries),
        "owner_or_admin_observed": sum(row["admin_risk_status"] == "OWNER_OR_PROXY_ADMIN_PRESENT" for row in summaries),
        "partial_rows": len(partial),
        "partial_contracts": [row["contract"] for row in partial],
        "error_rows": len(errors),
        "rpc_stats": rpc.stats,
        "production_approved_contracts": 0,
    }
    (out / "VALIDATION.json").write_text(json.dumps(validation, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    manifest = []
    for path in sorted(out.iterdir()):
        if path.is_file() and path.parent == out:
            manifest.append({"path": path.name, "bytes": path.stat().st_size, "sha256": sha256(path)})
    for path in sorted(raw_dir.iterdir()):
        if path.is_file():
            manifest.append({"path": str(path.relative_to(out)), "bytes": path.stat().st_size, "sha256": sha256(path)})
    (out / "MANIFEST.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(validation, sort_keys=True), flush=True)
    if validation["status"] != "PASS":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
