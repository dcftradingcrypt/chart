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
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable

from eth_utils import keccak

REPOSITORY = os.environ["GITHUB_REPOSITORY"]
TOKEN = os.environ["GITHUB_TOKEN"]
API = f"https://api.github.com/repos/{REPOSITORY}"
SOURCE_BRANCH = "chatgpt/rhc-normalized-universe-20260829"
SOURCE_WORKFLOW = "RHC normalize complete NFT opportunity and market universe"
SOURCE_ARTIFACT = "rhc-normalized-universe"
FIXED_HEAD = 48_264_433
SHARD_COUNT = 32
ENDPOINTS = [
    "https://robinhoodchain.blockscout.com/api",
    "https://explorer.hoodmarketcap.com/api",
]
USER_AGENT = "RHC-Token-Transfer-Universe/1.0"


def topic(signature: str) -> str:
    return "0x" + keccak(text=signature).hex()


EVENTS = {
    "ERC721_TRANSFER": {
        "topic0": topic("Transfer(address,address,uint256)"),
        "standard": "ERC721",
        "topic_count": 4,
    },
    "ERC1155_TRANSFER_SINGLE": {
        "topic0": topic("TransferSingle(address,address,address,uint256,uint256)"),
        "standard": "ERC1155",
        "topic_count": 4,
    },
    "ERC1155_TRANSFER_BATCH": {
        "topic0": topic("TransferBatch(address,address,address,uint256[],uint256[])"),
        "standard": "ERC1155",
        "topic_count": 4,
    },
}
assert EVENTS["ERC721_TRANSFER"]["topic0"] == "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"


def canonical(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def intish(value: Any) -> int:
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        return int(value, 16) if value.startswith("0x") else int(value)
    raise ValueError(value)


def event_key(row: dict[str, Any]) -> tuple[str, int]:
    tx_hash = str(row.get("transactionHash") or row.get("transaction_hash") or "").lower()
    log_index = intish(row.get("logIndex") or row.get("log_index") or "0x0")
    return tx_hash, log_index


def shard_for(address: str) -> int:
    return int(hashlib.sha256(address.lower().encode("utf-8")).hexdigest()[:16], 16) % SHARD_COUNT


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
            print("waiting for normalized universe", flush=True)
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
    raise RuntimeError(f"artifact download failed: {url}: {last_error!r}")


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


class ExplorerClient:
    def __init__(self, delay_seconds: float = 1.25):
        self.delay_seconds = delay_seconds
        self.last_request = 0.0
        self.endpoint_cursor = 0
        self.stats: dict[str, int] = {}

    def pace(self) -> None:
        wait = self.delay_seconds - (time.monotonic() - self.last_request)
        if wait > 0:
            time.sleep(wait)

    def request(self, endpoint: str, params: dict[str, Any], attempts: int = 10) -> dict[str, Any]:
        url = endpoint + "?" + urllib.parse.urlencode(params)
        last_error = None
        for attempt in range(attempts):
            self.pace()
            request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, "Accept": "application/json"})
            try:
                with urllib.request.urlopen(request, timeout=90) as response:
                    body = response.read()
                    status = response.status
                self.last_request = time.monotonic()
                self.stats[f"http_{status}"] = self.stats.get(f"http_{status}", 0) + 1
                payload = json.loads(body.decode("utf-8"))
                if not isinstance(payload, dict):
                    raise RuntimeError(f"non-object response: {type(payload)}")
                return payload
            except urllib.error.HTTPError as exc:
                self.last_request = time.monotonic()
                self.stats[f"http_{exc.code}"] = self.stats.get(f"http_{exc.code}", 0) + 1
                last_error = exc
                if exc.code in (429, 500, 502, 503, 504):
                    time.sleep(min(180.0, 10.0 * (2 ** min(attempt, 4)) + random.random() * 4))
                    continue
                body = exc.read(1000).decode("utf-8", "replace")
                raise RuntimeError(f"HTTP {exc.code} {url}: {body}") from exc
            except Exception as exc:
                last_error = exc
                self.stats["network_or_decode_error"] = self.stats.get("network_or_decode_error", 0) + 1
                if attempt + 1 < attempts:
                    time.sleep(min(90.0, 5.0 * (2 ** min(attempt, 4)) + random.random() * 3))
                    continue
        raise RuntimeError(f"request exhausted: {url}: {last_error!r}")

    def query(self, contract: str, topic0: str, start: int, end: int) -> tuple[list[dict[str, Any]], str, str]:
        params = {
            "module": "logs",
            "action": "getLogs",
            "fromBlock": start,
            "toBlock": end,
            "address": contract,
            "topic0": topic0,
        }
        errors = []
        ordered = ENDPOINTS[self.endpoint_cursor :] + ENDPOINTS[: self.endpoint_cursor]
        for endpoint in ordered:
            try:
                payload = self.request(endpoint, params)
                result = payload.get("result")
                status = str(payload.get("status", ""))
                message = str(payload.get("message", ""))
                if isinstance(result, list):
                    self.endpoint_cursor = (ENDPOINTS.index(endpoint) + 1) % len(ENDPOINTS)
                    return [row for row in result if isinstance(row, dict)], endpoint, message
                if status == "0" and isinstance(result, str) and "No records" in result:
                    return [], endpoint, message
                raise RuntimeError(f"unexpected payload: {payload!r}")
            except Exception as exc:
                errors.append(f"{endpoint}:{exc!r}")
        raise RuntimeError(" | ".join(errors))


def collect_contract_event(
    client: ExplorerClient,
    contract: str,
    event_name: str,
    start: int,
    end: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    pending = [(start, end)]
    rows_by_key: dict[tuple[str, int], dict[str, Any]] = {}
    accepted = []
    single_block_caps = []
    topic0 = EVENTS[event_name]["topic0"]
    while pending:
        left, right = pending.pop()
        rows, endpoint, message = client.query(contract, topic0, left, right)
        print(contract, event_name, left, right, len(rows), endpoint, flush=True)
        if len(rows) >= 1000:
            if left == right:
                single_block_caps.append({
                    "contract": contract,
                    "event_name": event_name,
                    "block": left,
                    "rows": len(rows),
                    "endpoint": endpoint,
                })
                continue
            middle = (left + right) // 2
            pending.append((middle + 1, right))
            pending.append((left, middle))
            continue
        for row in rows:
            block = intish(row.get("blockNumber") or row.get("block_number"))
            if block < left or block > right:
                raise RuntimeError(f"row outside range: {block} not {left}-{right}")
            address = str(row.get("address") or "").lower()
            topics = [str(value).lower() for value in row.get("topics") or []]
            if address != contract:
                raise RuntimeError(f"wrong contract: {address} != {contract}")
            if not topics or topics[0] != topic0:
                raise RuntimeError(f"wrong topic0: {topics[:1]}")
            if len(topics) != EVENTS[event_name]["topic_count"]:
                raise RuntimeError(f"wrong topic count for {event_name}: {len(topics)}")
            key = event_key(row)
            previous = rows_by_key.get(key)
            if previous is not None and canonical(previous) != canonical(row):
                raise RuntimeError(f"conflicting duplicate: {key}")
            rows_by_key[key] = row
        accepted.append({
            "contract": contract,
            "event_name": event_name,
            "from_block": left,
            "to_block": right,
            "row_count": len(rows),
            "endpoint": endpoint,
            "message": message,
        })
    accepted.sort(key=lambda row: row["from_block"])
    failures = []
    expected = start
    for row in accepted:
        if row["from_block"] != expected:
            failures.append({
                "contract": contract,
                "event_name": event_name,
                "expected_from": expected,
                "actual_from": row["from_block"],
            })
        expected = row["to_block"] + 1
    if expected != end + 1:
        failures.append({
            "contract": contract,
            "event_name": event_name,
            "expected_final": end + 1,
            "actual_final": expected,
        })
    return list(rows_by_key.values()), accepted, single_block_caps + failures


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
    source_status = json.loads(only_file(root, "NORMALIZATION_STATUS.json").read_text(encoding="utf-8"))
    if source_status.get("status") != "PASS":
        raise RuntimeError("normalized universe source is not PASS")

    project_rows = read_csv(only_file(root, "project_contracts_pre_enrichment.csv"))
    mint_rows = read_csv(only_file(root, "global_nft_mints.csv"))
    standards: dict[str, set[str]] = defaultdict(set)
    expected_mint_event_ids: dict[tuple[str, str], set[str]] = defaultdict(set)
    for row in mint_rows:
        contract = row["nft_contract"].lower()
        standard = row["standard"]
        standards[contract].add(standard)
        expected_mint_event_ids[(contract, standard)].add(row["event_id"])

    selected = sorted(
        row for row in project_rows
        if shard_for(row["nft_contract"].lower()) == args.shard
    , key=lambda row: row["nft_contract"])
    input_snapshot = {
        "shard": args.shard,
        "shard_count": SHARD_COUNT,
        "source": source_record,
        "contracts": [row["nft_contract"].lower() for row in selected],
    }
    (out / "input_snapshot.json").write_text(json.dumps(input_snapshot, indent=2, sort_keys=True), encoding="utf-8")

    client = ExplorerClient()
    output_rows = []
    contract_summaries = []
    range_rows = []
    failures = []

    for project in selected:
        contract = project["nft_contract"].lower()
        first_mint_block = int(float(project.get("first_mint_block") or 0))
        contract_standards = standards.get(contract, set())
        event_names = []
        if "ERC721" in contract_standards:
            event_names.append("ERC721_TRANSFER")
        if "ERC1155" in contract_standards:
            event_names.extend(["ERC1155_TRANSFER_SINGLE", "ERC1155_TRANSFER_BATCH"])
        if not event_names:
            failures.append({"code": "NO_STANDARD_FOR_CONTRACT", "contract": contract})
            continue

        before = len(output_rows)
        event_counts = {}
        for event_name in event_names:
            try:
                rows, accepted, event_failures = collect_contract_event(
                    client,
                    contract,
                    event_name,
                    first_mint_block,
                    FIXED_HEAD,
                )
                for row in rows:
                    output_rows.append({
                        "contract": contract,
                        "standard": EVENTS[event_name]["standard"],
                        "event_name": event_name,
                        "raw": row,
                    })
                range_rows.extend(accepted)
                event_counts[event_name] = len(rows)
                failures.extend({"code": "CONTRACT_RANGE_FAILURE", **row} for row in event_failures)
            except Exception as exc:
                failures.append({
                    "code": "CONTRACT_EVENT_COLLECTION_FAILED",
                    "contract": contract,
                    "event_name": event_name,
                    "error": repr(exc),
                })

        actual_mint_keys = set()
        for row in output_rows[before:]:
            raw = row["raw"]
            topics = [str(value).lower() for value in raw.get("topics") or []]
            if row["event_name"] == "ERC721_TRANSFER" and len(topics) == 4 and topics[1] == "0x" + ("0" * 64):
                actual_mint_keys.add(f"{event_key(raw)[0]}:{event_key(raw)[1]}")
            elif row["event_name"].startswith("ERC1155") and len(topics) == 4 and topics[2] == "0x" + ("0" * 64):
                actual_mint_keys.add(f"{event_key(raw)[0]}:{event_key(raw)[1]}")
        expected_keys = set()
        for standard in contract_standards:
            for event_id in expected_mint_event_ids[(contract, standard)]:
                expected_keys.add(event_id.split(":")[0] + ":" + event_id.split(":")[1])
        missing_expected = sorted(expected_keys - actual_mint_keys)
        if missing_expected:
            failures.append({
                "code": "EXPECTED_ZERO_MINT_EVENTS_MISSING_FROM_TRANSFER_HISTORY",
                "contract": contract,
                "count": len(missing_expected),
                "sample": missing_expected[:25],
            })
        contract_summaries.append({
            "contract": contract,
            "standards": sorted(contract_standards),
            "first_mint_block": first_mint_block,
            "event_counts": event_counts,
            "expected_zero_mint_event_count": len(expected_keys),
            "actual_zero_mint_event_count": len(actual_mint_keys),
            "missing_expected_zero_mint_events": len(missing_expected),
        })

    output_rows.sort(key=lambda row: (
        intish(row["raw"].get("blockNumber") or row["raw"].get("block_number")),
        event_key(row["raw"])[0],
        event_key(row["raw"])[1],
    ))
    transfer_count = write_jsonl_gz(out / "contract_transfer_events.jsonl.gz", output_rows)
    write_csv(out / "contract_summary.csv", contract_summaries)
    write_csv(out / "accepted_ranges.csv", range_rows)
    write_csv(out / "failures.csv", failures)

    validation = {
        "status": "PASS" if not failures else "FAIL",
        "shard": args.shard,
        "shard_count": SHARD_COUNT,
        "source": source_record,
        "contract_count": len(selected),
        "transfer_event_rows": transfer_count,
        "accepted_range_rows": len(range_rows),
        "failure_rows": len(failures),
        "http_stats": client.stats,
        "failures": failures[:100],
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
    if failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
