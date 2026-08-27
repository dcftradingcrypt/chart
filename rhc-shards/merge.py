#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
from pathlib import Path
from typing import Any

TARGETS = {
    "seadrop": "0xe90cf9cc0a552cf52ea6ff74ece0f1c8ae8cc9ad630d3181f55ac43ca076b7d6",
    "seaport": "0x9d9af8e38d66c62e2c12f0225249fd9d721c54b83f48d9352c97c6cacdcb6f31",
}


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--shards", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--head", type=int, required=True)
    parser.add_argument("--segments", type=int, required=True)
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)

    all_validation: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    target_rows: dict[str, list[dict[str, Any]]] = {target: [] for target in TARGETS}

    for target, topic0 in TARGETS.items():
        validations: dict[int, dict[str, Any]] = {}
        for path in args.shards.rglob("validation.json"):
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                continue
            if data.get("target") != target:
                continue
            index = int(data.get("segment_index", -1))
            if index in validations:
                failures.append({"code": "DUPLICATE_SEGMENT_VALIDATION", "target": target, "segment": index})
            validations[index] = data
            all_validation.append(data)
            logs_path = path.parent / "logs.jsonl"
            if not logs_path.exists():
                failures.append({"code": "MISSING_SHARD_LOGS", "target": target, "segment": index, "path": str(path.parent)})
                continue
            with logs_path.open(encoding="utf-8") as handle:
                for line in handle:
                    line = line.strip()
                    if line:
                        target_rows[target].append(json.loads(line))
        expected = set(range(args.segments))
        missing = sorted(expected - set(validations))
        extra = sorted(set(validations) - expected)
        if missing:
            failures.append({"code": "MISSING_SEGMENTS", "target": target, "segments": missing})
        if extra:
            failures.append({"code": "EXTRA_SEGMENTS", "target": target, "segments": extra})
        for index in sorted(validations):
            data = validations[index]
            expected_start = (args.head + 1) * index // args.segments
            expected_stop = (args.head + 1) * (index + 1) // args.segments - 1
            if data.get("status") != "PASS":
                failures.append({"code": "SHARD_NOT_PASS", "target": target, "segment": index, "value": data.get("status")})
            if int(data.get("head_block", -1)) != args.head or int(data.get("from_block", -1)) != expected_start or int(data.get("to_block", -1)) != expected_stop:
                failures.append({"code": "SHARD_COVERAGE_MISMATCH", "target": target, "segment": index, "validation": data, "expected_start": expected_start, "expected_stop": expected_stop})

        unique: dict[tuple[str, int], dict[str, Any]] = {}
        duplicates = 0
        for row in target_rows[target]:
            key = (str(row.get("transaction_hash", "")).lower(), int(row.get("log_index", -1)))
            if key in unique:
                duplicates += 1
            unique[key] = row
        rows = sorted(unique.values(), key=lambda row: (int(row.get("block_number") or -1), int(row.get("transaction_index") or -1), int(row.get("log_index") or -1)))
        wrong_topic = [row for row in rows if row.get("topic0") != topic0]
        outside = [row for row in rows if row.get("block_number") is None or not 0 <= int(row["block_number"]) <= args.head]
        if wrong_topic:
            failures.append({"code": "MERGED_WRONG_TOPIC", "target": target, "count": len(wrong_topic)})
        if outside:
            failures.append({"code": "MERGED_OUT_OF_RANGE", "target": target, "count": len(outside)})

        jsonl_path = args.out / f"{target}_logs.jsonl"
        with jsonl_path.open("w", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
        fields = ["chain_id", "target", "address", "block_number", "block_hash", "transaction_hash", "transaction_index", "log_index", "data", "topics", "topic0"]
        with (args.out / f"{target}_logs.csv").open("w", newline="", encoding="utf-8-sig") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields)
            writer.writeheader()
            for row in rows:
                writer.writerow({field: json.dumps(row.get(field), ensure_ascii=False) if isinstance(row.get(field), list) else row.get(field) for field in fields})
        (args.out / f"{target}_summary.json").write_text(json.dumps({
            "target": target,
            "head_block": args.head,
            "segment_count": args.segments,
            "row_count": len(rows),
            "cross_segment_duplicates_removed": duplicates,
            "first_block": rows[0]["block_number"] if rows else None,
            "last_block": rows[-1]["block_number"] if rows else None,
        }, indent=2, sort_keys=True), encoding="utf-8")

    report = {
        "status": "PASS" if not failures else "FAIL",
        "head_block": args.head,
        "segment_count": args.segments,
        "validation_rows": len(all_validation),
        "seadrop_rows": sum(1 for _ in (args.out / "seadrop_logs.jsonl").open(encoding="utf-8")) if (args.out / "seadrop_logs.jsonl").exists() else 0,
        "seaport_rows": sum(1 for _ in (args.out / "seaport_logs.jsonl").open(encoding="utf-8")) if (args.out / "seaport_logs.jsonl").exists() else 0,
        "failures": failures,
    }
    (args.out / "validation.json").write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    (args.out / "shard_validations.json").write_text(json.dumps(all_validation, indent=2, sort_keys=True), encoding="utf-8")
    manifest = []
    for path in sorted(args.out.iterdir()):
        if path.is_file() and path.name != "manifest.json":
            manifest.append({"path": path.name, "bytes": path.stat().st_size, "sha256": sha256(path)})
    (args.out / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(report, sort_keys=True), flush=True)
    if failures:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
