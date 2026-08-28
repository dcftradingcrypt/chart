from __future__ import annotations

import csv
import gzip
import hashlib
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from eth_abi import decode
from eth_utils import keccak

REPOSITORY = os.environ["GITHUB_REPOSITORY"]
TOKEN = os.environ["GITHUB_TOKEN"]
API = f"https://api.github.com/repos/{REPOSITORY}"
ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"
ZERO_TOPIC = "0x" + ("0" * 64)
WETH_RHC = "0x0bd7d308f8e1639fab988df18a8011f41eacad73"
TRANSFER_TOPIC = "0x" + keccak(text="Transfer(address,address,uint256)").hex()
USER_AGENT = "RHC-Market-Provenance/1.0"


def canonical_json(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def api_request(path: str, attempts: int = 8) -> Any:
    url = path if path.startswith("https://") else API + path
    last_error: Exception | None = None
    for attempt in range(attempts):
        request = urllib.request.Request(
            url,
            headers={
                "Authorization": f"Bearer {TOKEN}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
                "User-Agent": USER_AGENT,
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=180) as response:
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            last_error = exc
            if exc.code in (403, 429, 500, 502, 503, 504) and attempt + 1 < attempts:
                time.sleep(min(120, 5 * (2 ** attempt)))
                continue
            detail = exc.read(1000).decode("utf-8", "replace")
            raise RuntimeError(f"GitHub API HTTP {exc.code} {url}: {detail}") from exc
        except Exception as exc:
            last_error = exc
            if attempt + 1 < attempts:
                time.sleep(min(90, 5 * (2 ** attempt)))
                continue
    raise RuntimeError(f"GitHub API exhausted: {url}: {last_error!r}")


def wait_successful_run(branch: str, workflow: str, timeout_seconds: int = 19_800) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        query = urllib.parse.urlencode({"branch": branch, "event": "pull_request", "per_page": 100})
        rows = sorted(
            [
                row for row in api_request(f"/actions/runs?{query}").get("workflow_runs", [])
                if row.get("name") == workflow
            ],
            key=lambda row: int(row["id"]),
            reverse=True,
        )
        if not rows:
            print(f"waiting for {branch}/{workflow}", flush=True)
            time.sleep(30)
            continue
        run = rows[0]
        print(json.dumps({
            "branch": branch,
            "workflow": workflow,
            "id": run["id"],
            "status": run.get("status"),
            "conclusion": run.get("conclusion"),
            "head_sha": run.get("head_sha"),
        }, sort_keys=True), flush=True)
        if run.get("status") == "completed":
            if run.get("conclusion") != "success":
                raise RuntimeError(f"source workflow did not succeed: {run.get('html_url')}")
            return run
        time.sleep(45)
    raise TimeoutError(f"source workflow timeout: {branch}/{workflow}")


def download_file(url: str, destination: Path, attempts: int = 8) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    last_error: Exception | None = None
    for attempt in range(attempts):
        request = urllib.request.Request(
            url,
            headers={
                "Authorization": f"Bearer {TOKEN}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
                "User-Agent": USER_AGENT,
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=300) as response:
                destination.write_bytes(response.read())
            return
        except Exception as exc:
            last_error = exc
            if attempt + 1 < attempts:
                time.sleep(min(120, 5 * (2 ** attempt)))
                continue
    raise RuntimeError(f"artifact download failed: {url}: {last_error!r}")


def fetch_artifact(
    name: str,
    branch: str,
    workflow: str,
    artifact_name: str,
    destination_root: Path,
) -> tuple[dict[str, Any], Path]:
    run = wait_successful_run(branch, workflow)
    artifacts = api_request(f"/actions/runs/{run['id']}/artifacts?per_page=100").get("artifacts", [])
    matches = [row for row in artifacts if row.get("name") == artifact_name]
    if len(matches) != 1:
        raise RuntimeError(f"artifact count mismatch for {name}: {len(matches)}")
    artifact = matches[0]
    zip_path = destination_root / "source_artifacts" / f"{name}.zip"
    download_file(artifact["archive_download_url"], zip_path)
    extract = destination_root / "sources" / name
    extract.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path) as archive:
        archive.extractall(extract)
    return {
        "run_id": run["id"],
        "head_sha": run.get("head_sha"),
        "html_url": run.get("html_url"),
        "artifact_id": artifact["id"],
        "artifact_bytes": zip_path.stat().st_size,
        "artifact_sha256": sha256_file(zip_path),
    }, extract


def only_file(root: Path, filename: str) -> Path:
    paths = sorted(root.rglob(filename))
    if len(paths) != 1:
        raise RuntimeError(f"expected exactly one {filename} under {root}, found {len(paths)}")
    return paths[0]


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: Iterable[dict[str, Any]]) -> int:
    materialized = list(rows)
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = sorted({key for row in materialized for key in row}) if materialized else ["empty"]
    with path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in materialized:
            writer.writerow({
                key: json.dumps(value, ensure_ascii=False, sort_keys=True)
                if isinstance(value, (dict, list, tuple, set))
                else value
                for key, value in row.items()
            })
    return len(materialized)


def load_jsonl_gz(path: Path) -> list[dict[str, Any]]:
    rows = []
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                value = json.loads(line)
                if not isinstance(value, dict):
                    raise RuntimeError(f"non-object row in {path}")
                rows.append(value)
    return rows


def write_jsonl_gz(path: Path, rows: Iterable[dict[str, Any]]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with gzip.open(path, "wt", encoding="utf-8", newline="\n") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n")
            count += 1
    return count


def parse_json_field(value: Any, default: Any = None) -> Any:
    if value is None or value == "":
        return default
    if isinstance(value, (dict, list, int, float, bool)):
        return value
    try:
        return json.loads(str(value))
    except Exception:
        return default


def intish(value: Any, default: int | None = None) -> int | None:
    if value is None or value == "":
        return default
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        text = value.strip()
        try:
            return int(text, 16) if text.startswith("0x") else int(float(text))
        except Exception:
            return default
    if isinstance(value, dict):
        for key in ("value", "amount", "token_id"):
            if key in value:
                return intish(value[key], default)
    return default


def boolish(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "pass"}


def address(value: Any) -> str | None:
    if isinstance(value, dict):
        for key in ("hash", "address", "address_hash", "value"):
            if key in value:
                return address(value[key])
        return None
    if isinstance(value, bytes):
        return "0x" + value[-20:].hex()
    if not isinstance(value, str):
        return None
    text = value.strip().lower()
    if text.startswith("0x") and len(text) == 42:
        return text
    if text.startswith("0x") and len(text) == 66:
        return "0x" + text[-40:]
    return None


def topic_address(value: str) -> str:
    result = address(value)
    if result is None:
        raise ValueError(value)
    return result


def normalize_asset(asset: str | None) -> str | None:
    if asset is None:
        return None
    asset = asset.lower()
    if asset in {ZERO_ADDRESS, WETH_RHC}:
        return "ETH_EQUIVALENT"
    return asset


def block_timestamp(raw: dict[str, Any]) -> int | None:
    value = raw.get("timestamp") or raw.get("timeStamp") or raw.get("time")
    numeric = intish(value)
    if numeric is not None:
        return numeric
    if isinstance(value, str):
        try:
            return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp())
        except Exception:
            return None
    return None


def transaction_raw(row: dict[str, Any]) -> dict[str, Any]:
    raw = row.get("raw")
    if isinstance(raw, dict):
        return raw
    tx = row.get("transaction")
    return tx if isinstance(tx, dict) else row


def receipt_raw(row: dict[str, Any]) -> dict[str, Any]:
    raw = row.get("raw")
    if isinstance(raw, dict):
        return raw
    result = {}
    tx = row.get("transaction")
    if isinstance(tx, dict):
        result.update(tx)
    logs = row.get("logs")
    if isinstance(logs, list):
        result["logs"] = logs
    return result


def block_raw(row: dict[str, Any]) -> dict[str, Any]:
    raw = row.get("raw")
    return raw if isinstance(raw, dict) else row


def tx_hash_from_raw(raw: dict[str, Any]) -> str | None:
    value = raw.get("hash") or raw.get("transaction_hash") or raw.get("transactionHash")
    return str(value).lower() if value else None


def tx_core(row: dict[str, Any]) -> dict[str, Any]:
    raw = transaction_raw(row)
    tx_hash = row.get("transaction_hash") or tx_hash_from_raw(raw)
    return {
        "transaction_hash": str(tx_hash).lower() if tx_hash else None,
        "from_address": address(raw.get("from")),
        "to_address": address(raw.get("to")),
        "value_wei": intish(raw.get("value"), 0) or 0,
        "block_number": intish(raw.get("blockNumber") or raw.get("block_number")),
        "input": raw.get("input") or raw.get("raw_input") or raw.get("data"),
        "gas_price_wei": intish(raw.get("gasPrice") or raw.get("gas_price")),
        "source": row.get("source"),
        "raw": raw,
    }


def receipt_core(row: dict[str, Any]) -> dict[str, Any]:
    raw = receipt_raw(row)
    tx_hash = row.get("transaction_hash") or raw.get("transactionHash") or raw.get("transaction_hash")
    logs = raw.get("logs") or []
    status = raw.get("status")
    success = None
    if isinstance(status, bool):
        success = status
    elif status is not None:
        status_text = str(status).lower()
        success = status_text in {"0x1", "1", "success", "ok", "true"}
    return {
        "transaction_hash": str(tx_hash).lower() if tx_hash else None,
        "block_number": intish(raw.get("blockNumber") or raw.get("block_number")),
        "success": success,
        "gas_used": intish(raw.get("gasUsed") or raw.get("gas_used")),
        "effective_gas_price_wei": intish(
            raw.get("effectiveGasPrice") or raw.get("effective_gas_price") or raw.get("gas_price")
        ),
        "logs": logs if isinstance(logs, list) else [],
        "source": row.get("source"),
        "raw": raw,
    }


def internal_items(row: dict[str, Any]) -> list[dict[str, Any]]:
    items = row.get("items")
    return [item for item in items if isinstance(item, dict)] if isinstance(items, list) else []


def merge_prefer_official(rows: Iterable[dict[str, Any]], key_field: str) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]]]:
    by_key: dict[str, dict[str, Any]] = {}
    conflicts = []
    for row in rows:
        key = str(row.get(key_field, "")).lower()
        if not key:
            continue
        previous = by_key.get(key)
        if previous is None:
            by_key[key] = row
            continue
        previous_official = previous.get("source") == "OFFICIAL_RPC"
        current_official = row.get("source") == "OFFICIAL_RPC"
        if current_official and not previous_official:
            by_key[key] = row
        elif canonical_json(previous) != canonical_json(row) and previous_official == current_official:
            conflicts.append({"key": key, "previous": previous, "current": row})
    return by_key, conflicts


def decode_contract_transfer(row: dict[str, Any]) -> list[dict[str, Any]]:
    raw = row["raw"]
    topics = [str(value).lower() for value in raw.get("topics") or []]
    tx_hash = str(raw.get("transactionHash") or raw.get("transaction_hash") or "").lower()
    log_index = intish(raw.get("logIndex") or raw.get("log_index"), 0) or 0
    block_number_value = intish(raw.get("blockNumber") or raw.get("block_number"))
    block_hash = str(raw.get("blockHash") or raw.get("block_hash") or "").lower()
    contract = str(row["contract"]).lower()
    event_name = row["event_name"]
    common = {
        "transaction_hash": tx_hash,
        "log_index": log_index,
        "block_number": block_number_value,
        "block_hash": block_hash,
        "nft_contract": contract,
        "standard": row["standard"],
        "event_name": event_name,
    }
    if event_name == "ERC721_TRANSFER":
        if len(topics) != 4:
            raise RuntimeError(f"ERC721 topic count {len(topics)}")
        return [{
            **common,
            "item_index": 0,
            "from_address": topic_address(topics[1]),
            "to_address": topic_address(topics[2]),
            "token_id": str(intish(topics[3], 0)),
            "quantity": 1,
        }]
    if event_name == "ERC1155_TRANSFER_SINGLE":
        if len(topics) != 4:
            raise RuntimeError(f"ERC1155 single topic count {len(topics)}")
        token_id, quantity = decode(["uint256", "uint256"], bytes.fromhex(str(raw.get("data") or "0x")[2:]))
        return [{
            **common,
            "item_index": 0,
            "operator": topic_address(topics[1]),
            "from_address": topic_address(topics[2]),
            "to_address": topic_address(topics[3]),
            "token_id": str(int(token_id)),
            "quantity": int(quantity),
        }]
    if event_name == "ERC1155_TRANSFER_BATCH":
        if len(topics) != 4:
            raise RuntimeError(f"ERC1155 batch topic count {len(topics)}")
        ids, quantities = decode(["uint256[]", "uint256[]"], bytes.fromhex(str(raw.get("data") or "0x")[2:]))
        if len(ids) != len(quantities):
            raise RuntimeError("ERC1155 batch length mismatch")
        return [{
            **common,
            "item_index": index,
            "operator": topic_address(topics[1]),
            "from_address": topic_address(topics[2]),
            "to_address": topic_address(topics[3]),
            "token_id": str(int(token_id)),
            "quantity": int(quantity),
        } for index, (token_id, quantity) in enumerate(zip(ids, quantities))]
    raise RuntimeError(f"unknown transfer event name: {event_name}")


def payment_flows_for_transaction(
    tx: dict[str, Any] | None,
    receipt: dict[str, Any] | None,
    internals: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    flows = []
    seen = set()
    tx_hash = tx.get("transaction_hash") if tx else receipt.get("transaction_hash") if receipt else None

    def add(kind: str, asset: str, sender: str | None, recipient: str | None, amount: int, evidence: Any) -> None:
        if amount <= 0 or sender is None or recipient is None:
            return
        key = (kind, asset.lower(), sender.lower(), recipient.lower(), int(amount), json.dumps(evidence, sort_keys=True, default=str))
        if key in seen:
            return
        seen.add(key)
        flows.append({
            "transaction_hash": tx_hash,
            "flow_kind": kind,
            "asset": asset.lower(),
            "asset_normalized": normalize_asset(asset),
            "from_address": sender.lower(),
            "to_address": recipient.lower(),
            "amount_raw": str(int(amount)),
            "evidence": evidence,
        })

    if tx and int(tx.get("value_wei") or 0) > 0:
        add("TOP_LEVEL_NATIVE", ZERO_ADDRESS, tx.get("from_address"), tx.get("to_address"), int(tx["value_wei"]), {"source": tx.get("source")})

    for index, item in enumerate(internals):
        amount = intish(item.get("value") or (item.get("value") or {}).get("value") if isinstance(item.get("value"), dict) else item.get("value"), 0) or 0
        sender = address(item.get("from"))
        recipient = address(item.get("to"))
        success = item.get("success")
        if success is False or str(success).lower() in {"false", "0", "reverted", "error"}:
            continue
        add("INTERNAL_NATIVE", ZERO_ADDRESS, sender, recipient, amount, {"index": index, "type": item.get("type") or item.get("call_type")})

    if receipt:
        for log_index, log in enumerate(receipt.get("logs") or []):
            topics = [str(value).lower() for value in log.get("topics") or []]
            if len(topics) != 3 or not topics or topics[0] != TRANSFER_TOPIC:
                continue
            token = address(log.get("address"))
            sender = topic_address(topics[1])
            recipient = topic_address(topics[2])
            amount = intish(log.get("data"), 0) or 0
            if token:
                add("ERC20_TRANSFER", token, sender, recipient, amount, {"log_index": intish(log.get("logIndex"), log_index)})
    return flows


def unix_to_iso(value: int | None) -> str | None:
    if value is None:
        return None
    return datetime.fromtimestamp(value, tz=timezone.utc).isoformat().replace("+00:00", "Z")
