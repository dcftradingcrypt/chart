#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import json
import os
import random
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from pathlib import Path
from typing import Any, Iterable

REPOSITORY = os.environ["GITHUB_REPOSITORY"]
TOKEN = os.environ["GITHUB_TOKEN"]
API = f"https://api.github.com/repos/{REPOSITORY}"
SOURCE_BRANCH = "chatgpt/rhc-normalized-universe-20260829"
SOURCE_WORKFLOW = "RHC normalize complete NFT opportunity and market universe"
SOURCE_ARTIFACT = "rhc-normalized-universe"
RPC = "https://rpc.mainnet.chain.robinhood.com"
BLOCKSCOUT = "https://robinhoodchain.blockscout.com/api/v2"
USER_AGENT = "RHC-Tx-Contract-Enrichment/1.0"
SHARD_COUNT = 16


def api(path: str, attempts: int = 8) -> Any:
    url = path if path.startswith("https://") else API + path
    last_error = None
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


def wait_source(timeout: int = 19_800) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        query = urllib.parse.urlencode({"branch": SOURCE_BRANCH, "event": "pull_request", "per_page": 100})
        payload = api(f"/actions/runs?{query}")
        rows = sorted(
            [row for row in payload.get("workflow_runs", []) if row.get("name") == SOURCE_WORKFLOW],
            key=lambda row: int(row["id"]),
            reverse=True,
        )
        if not rows:
            print("waiting for normalized universe run", flush=True)
            time.sleep(30)
            continue
        run = rows[0]
        print(json.dumps({
            "id": run["id"],
            "status": run.get("status"),
            "conclusion": run.get("conclusion"),
            "head_sha": run.get("head_sha"),
        }, sort_keys=True), flush=True)
        if run.get("status") == "completed":
            if run.get("conclusion") != "success":
                raise RuntimeError(f"normalized source failed: {run.get('html_url')}")
            return run
        time.sleep(45)
    raise TimeoutError("normalized universe source did not finish")


def download(url: str, destination: Path, attempts: int = 8) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    last_error = None
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
    raise RuntimeError(f"download failed: {url}: {last_error!r}")


def fetch_source(out: Path) -> tuple[dict[str, Any], Path]:
    run = wait_source()
    artifacts = api(f"/actions/runs/{run['id']}/artifacts?per_page=100").get("artifacts", [])
    matches = [row for row in artifacts if row.get("name") == SOURCE_ARTIFACT]
    if len(matches) != 1:
        raise RuntimeError(f"normalized artifact count={len(matches)}")
    artifact = matches[0]
    zpath = out / "source" / "normalized.zip"
    download(artifact["archive_download_url"], zpath)
    extract = out / "source" / "normalized"
    extract.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zpath) as archive:
        archive.extractall(extract)
    return {
        "run_id": run["id"],
        "head_sha": run.get("head_sha"),
        "html_url": run.get("html_url"),
        "artifact_id": artifact["id"],
        "artifact_sha256": hashlib.sha256(zpath.read_bytes()).hexdigest(),
    }, extract


def only_file(root: Path, filename: str) -> Path:
    paths = sorted(root.rglob(filename))
    if len(paths) != 1:
        raise RuntimeError(f"expected one {filename}, found {len(paths)}")
    return paths[0]


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: Iterable[dict[str, Any]]) -> None:
    materialized = list(rows)
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = sorted({key for row in materialized for key in row}) if materialized else ["empty"]
    with path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in materialized:
            writer.writerow({
                key: json.dumps(value, ensure_ascii=False, sort_keys=True)
                if isinstance(value, (dict, list, tuple))
                else value
                for key, value in row.items()
            })


def write_jsonl_gz(path: Path, rows: Iterable[dict[str, Any]]) -> int:
    count = 0
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wt", encoding="utf-8", newline="\n") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n")
            count += 1
    return count


def shard_for(value: str) -> int:
    return int(hashlib.sha256(value.lower().encode("utf-8")).hexdigest()[:16], 16) % SHARD_COUNT


class HttpClient:
    def __init__(self, delay: float = 0.20):
        self.delay = delay
        self.last_request = 0.0
        self.stats: dict[str, int] = {}

    def pace(self) -> None:
        wait = self.delay - (time.monotonic() - self.last_request)
        if wait > 0:
            time.sleep(wait)

    def request_json(self, url: str, *, data: bytes | None = None, attempts: int = 9) -> Any:
        last_error = None
        for attempt in range(attempts):
            self.pace()
            headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
            if data is not None:
                headers["Content-Type"] = "application/json"
            request = urllib.request.Request(url, data=data, headers=headers)
            try:
                with urllib.request.urlopen(request, timeout=120) as response:
                    body = response.read()
                    status = response.status
                self.last_request = time.monotonic()
                self.stats[f"http_{status}"] = self.stats.get(f"http_{status}", 0) + 1
                return json.loads(body.decode("utf-8"))
            except urllib.error.HTTPError as exc:
                self.last_request = time.monotonic()
                self.stats[f"http_{exc.code}"] = self.stats.get(f"http_{exc.code}", 0) + 1
                last_error = exc
                if exc.code in (408, 425, 429, 500, 502, 503, 504) and attempt + 1 < attempts:
                    time.sleep(min(120, 4 * (2 ** min(attempt, 5)) + random.random() * 3))
                    continue
                body = exc.read(1000).decode("utf-8", "replace")
                raise RuntimeError(f"HTTP {exc.code} {url}: {body}") from exc
            except Exception as exc:
                last_error = exc
                self.stats["network_or_decode_error"] = self.stats.get("network_or_decode_error", 0) + 1
                if attempt + 1 < attempts:
                    time.sleep(min(90, 4 * (2 ** min(attempt, 5)) + random.random() * 3))
                    continue
        raise RuntimeError(f"request exhausted {url}: {last_error!r}")

    def rpc_batch(self, calls: list[tuple[str, list[Any]]]) -> list[Any]:
        payload = [
            {"jsonrpc": "2.0", "id": index, "method": method, "params": params}
            for index, (method, params) in enumerate(calls)
        ]
        response = self.request_json(RPC, data=json.dumps(payload).encode("utf-8"))
        if isinstance(response, dict):
            response = [response]
        by_id = {int(row["id"]): row for row in response}
        output = []
        for index in range(len(calls)):
            row = by_id.get(index)
            if row is None:
                output.append({"__error__": "missing batch response"})
            elif "error" in row:
                output.append({"__error__": row["error"]})
            else:
                output.append(row.get("result"))
        return output

    def blockscout(self, path: str) -> Any:
        return self.request_json(BLOCKSCOUT.rstrip("/") + "/" + path.lstrip("/"))


def paginate_blockscout(client: HttpClient, path: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    params: dict[str, Any] = {}
    seen = set()
    for _ in range(10_000):
        url = BLOCKSCOUT.rstrip("/") + "/" + path.lstrip("/")
        if params:
            url += "?" + urllib.parse.urlencode(params)
        payload = client.request_json(url)
        if isinstance(payload, list):
            rows.extend(item for item in payload if isinstance(item, dict))
            return rows
        if not isinstance(payload, dict):
            raise RuntimeError(f"invalid paginated response for {path}: {type(payload)}")
        items = payload.get("items") or []
        if not isinstance(items, list):
            raise RuntimeError(f"items not list for {path}")
        rows.extend(item for item in items if isinstance(item, dict))
        nxt = payload.get("next_page_params")
        if not nxt:
            return rows
        if not isinstance(nxt, dict):
            raise RuntimeError(f"invalid next_page_params for {path}: {nxt!r}")
        key = json.dumps(nxt, sort_keys=True, separators=(",", ":"))
        if key in seen:
            raise RuntimeError(f"repeated pagination key for {path}: {key}")
        seen.add(key)
        params = nxt
    raise RuntimeError(f"pagination limit exceeded for {path}")


def chunks(values: list[str], size: int) -> Iterable[list[str]]:
    for start in range(0, len(values), size):
        yield values[start : start + size]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--shard", type=int, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    if args.shard < 0 or args.shard >= SHARD_COUNT:
        raise SystemExit("invalid shard")
    out = args.out
    out.mkdir(parents=True, exist_ok=True)
    source_record, root = fetch_source(out)
    status = json.loads(only_file(root, "NORMALIZATION_STATUS.json").read_text(encoding="utf-8"))
    if status.get("status") != "PASS":
        raise RuntimeError("normalized source is not PASS")

    primary_txs = sorted(set(only_file(root, "primary_transaction_hashes.txt").read_text().split()))
    market_txs = sorted(set(only_file(root, "market_transaction_hashes.txt").read_text().split()))
    block_numbers = sorted(set(only_file(root, "event_block_numbers.txt").read_text().split()), key=int)
    contracts = sorted(set(only_file(root, "nft_contracts.txt").read_text().split()))
    entries = read_csv(only_file(root, "wallet_primary_entries_pre_enrichment.csv"))
    custom_txs = sorted({
        row["transaction_hash"].lower()
        for row in entries
        if row.get("entry_route") == "NON_SEADROP_ZERO_MINT"
    })

    all_txs = sorted(set(primary_txs) | set(market_txs))
    txs = [value for value in all_txs if shard_for(value) == args.shard]
    blocks = [value for value in block_numbers if shard_for(value) == args.shard]
    shard_contracts = [value for value in contracts if shard_for(value) == args.shard]
    internal_required = sorted({value for value in txs if value in set(market_txs) or value in set(custom_txs)})
    input_snapshot = {
        "shard": args.shard,
        "shard_count": SHARD_COUNT,
        "source": source_record,
        "transaction_hashes": txs,
        "block_numbers": blocks,
        "contracts": shard_contracts,
        "internal_required_transaction_hashes": internal_required,
    }
    (out / "input_snapshot.json").write_text(json.dumps(input_snapshot, indent=2, sort_keys=True), encoding="utf-8")

    client = HttpClient()
    tx_rows: list[dict[str, Any]] = []
    receipt_rows: list[dict[str, Any]] = []
    block_rows: list[dict[str, Any]] = []
    internal_rows: list[dict[str, Any]] = []
    contract_rows: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []

    for tx_chunk in chunks(txs, 40):
        calls = []
        keys = []
        for tx_hash in tx_chunk:
            calls.append(("eth_getTransactionByHash", [tx_hash])); keys.append(("tx", tx_hash))
            calls.append(("eth_getTransactionReceipt", [tx_hash])); keys.append(("receipt", tx_hash))
        try:
            results = client.rpc_batch(calls)
        except Exception as exc:
            errors.append({"kind": "rpc_tx_receipt_batch", "values": tx_chunk, "error": repr(exc)})
            continue
        for (kind, tx_hash), result in zip(keys, results):
            if result is None or isinstance(result, dict) and "__error__" in result:
                # Blockscout fallback for transaction details and logs.
                try:
                    fallback = client.blockscout(f"transactions/{tx_hash}")
                    if kind == "tx":
                        tx_rows.append({"transaction_hash": tx_hash, "source": "BLOCKSCOUT_FALLBACK", "raw": fallback})
                    else:
                        logs = paginate_blockscout(client, f"transactions/{tx_hash}/logs")
                        receipt_rows.append({
                            "transaction_hash": tx_hash,
                            "source": "BLOCKSCOUT_FALLBACK",
                            "transaction": fallback,
                            "logs": logs,
                        })
                except Exception as fallback_exc:
                    errors.append({
                        "kind": kind,
                        "value": tx_hash,
                        "rpc_result": result,
                        "fallback_error": repr(fallback_exc),
                    })
            elif kind == "tx":
                tx_rows.append({"transaction_hash": tx_hash, "source": "OFFICIAL_RPC", "raw": result})
            else:
                receipt_rows.append({"transaction_hash": tx_hash, "source": "OFFICIAL_RPC", "raw": result})

    for block_chunk in chunks(blocks, 80):
        calls = [("eth_getBlockByNumber", [hex(int(value)), False]) for value in block_chunk]
        try:
            results = client.rpc_batch(calls)
        except Exception as exc:
            errors.append({"kind": "rpc_block_batch", "values": block_chunk, "error": repr(exc)})
            continue
        for value, result in zip(block_chunk, results):
            if result is None or isinstance(result, dict) and "__error__" in result:
                try:
                    fallback = client.blockscout(f"blocks/{value}")
                    block_rows.append({"block_number": int(value), "source": "BLOCKSCOUT_FALLBACK", "raw": fallback})
                except Exception as fallback_exc:
                    errors.append({"kind": "block", "value": value, "rpc_result": result, "fallback_error": repr(fallback_exc)})
            else:
                block_rows.append({"block_number": int(value), "source": "OFFICIAL_RPC", "raw": result})

    for tx_hash in internal_required:
        try:
            rows = paginate_blockscout(client, f"transactions/{tx_hash}/internal-transactions")
            internal_rows.append({"transaction_hash": tx_hash, "source": "BLOCKSCOUT_V2", "items": rows})
        except Exception as exc:
            errors.append({"kind": "internal_transactions", "value": tx_hash, "error": repr(exc)})

    for contract_chunk in chunks(shard_contracts, 40):
        calls = [("eth_getCode", [address, "latest"]) for address in contract_chunk]
        try:
            codes = client.rpc_batch(calls)
        except Exception as exc:
            errors.append({"kind": "rpc_contract_code_batch", "values": contract_chunk, "error": repr(exc)})
            codes = [None] * len(contract_chunk)
        for address, code in zip(contract_chunk, codes):
            record: dict[str, Any] = {
                "contract_address": address,
                "code_source": "OFFICIAL_RPC" if isinstance(code, str) else "UNAVAILABLE",
                "code": code,
                "code_sha256": hashlib.sha256(bytes.fromhex(code[2:])).hexdigest()
                if isinstance(code, str) and code.startswith("0x") and len(code) > 2
                else None,
            }
            for label, path in (
                ("address", f"addresses/{address}"),
                ("token", f"tokens/{address}"),
                ("smart_contract", f"smart-contracts/{address}"),
            ):
                try:
                    record[f"blockscout_{label}"] = client.blockscout(path)
                except Exception as exc:
                    record[f"blockscout_{label}_error"] = repr(exc)
            if not isinstance(code, str) or code == "0x":
                errors.append({"kind": "contract_code", "value": address, "result": code})
            contract_rows.append(record)

    tx_count = write_jsonl_gz(out / "transactions.jsonl.gz", tx_rows)
    receipt_count = write_jsonl_gz(out / "receipts.jsonl.gz", receipt_rows)
    block_count = write_jsonl_gz(out / "blocks.jsonl.gz", block_rows)
    internal_count = write_jsonl_gz(out / "internal_transactions.jsonl.gz", internal_rows)
    contract_count = write_jsonl_gz(out / "contracts.jsonl.gz", contract_rows)
    write_csv(out / "errors.csv", errors)

    fetched_tx = {row["transaction_hash"] for row in tx_rows}
    fetched_receipts = {row["transaction_hash"] for row in receipt_rows}
    fetched_blocks = {str(row["block_number"]) for row in block_rows}
    fetched_internal = {row["transaction_hash"] for row in internal_rows}
    fetched_contracts = {row["contract_address"] for row in contract_rows}
    validation_failures = []
    for code, expected, actual in (
        ("MISSING_TRANSACTIONS", set(txs), fetched_tx),
        ("MISSING_RECEIPTS", set(txs), fetched_receipts),
        ("MISSING_BLOCKS", set(blocks), fetched_blocks),
        ("MISSING_INTERNAL_TRANSACTIONS", set(internal_required), fetched_internal),
        ("MISSING_CONTRACTS", set(shard_contracts), fetched_contracts),
    ):
        missing = sorted(expected - actual)
        if missing:
            validation_failures.append({"code": code, "count": len(missing), "sample": missing[:25]})
    if errors:
        validation_failures.append({"code": "ENRICHMENT_ERRORS", "count": len(errors), "sample": errors[:25]})

    validation = {
        "status": "PASS" if not validation_failures else "FAIL",
        "shard": args.shard,
        "shard_count": SHARD_COUNT,
        "source": source_record,
        "expected": {
            "transactions": len(txs),
            "receipts": len(txs),
            "blocks": len(blocks),
            "internal_transaction_groups": len(internal_required),
            "contracts": len(shard_contracts),
        },
        "fetched": {
            "transactions": tx_count,
            "receipts": receipt_count,
            "blocks": block_count,
            "internal_transaction_groups": internal_count,
            "contracts": contract_count,
        },
        "http_stats": client.stats,
        "error_rows": len(errors),
        "failures": validation_failures,
    }
    (out / "validation.json").write_text(json.dumps(validation, indent=2, sort_keys=True), encoding="utf-8")
    manifest = []
    for path in sorted(out.rglob("*")):
        if path.is_file():
            manifest.append({
                "path": str(path.relative_to(out)),
                "bytes": path.stat().st_size,
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            })
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(validation, sort_keys=True), flush=True)
    if validation_failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
