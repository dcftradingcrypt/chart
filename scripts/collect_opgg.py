from __future__ import annotations

import csv
import json
import re
import time
from datetime import datetime
from pathlib import Path

import requests
from bs4 import BeautifulSoup

ROOT = Path("out")
TIER_HTML = ROOT / "opgg" / "op.gg_ja_pokemon-champions_tier.html"
DETAIL_DIR = ROOT / "opgg" / "details"
SOURCE_URL = "https://op.gg/ja/pokemon-champions/tier"
DETAIL_URL = "https://op.gg/ja/pokemon-champions/pokedex/{slug}"


def parse_snapshot_time(soup: BeautifulSoup) -> str:
    text = soup.get_text(" ", strip=True)
    match = re.search(r"更新日\s*(\d{4})年(\d{1,2})月(\d{1,2})日\s*(\d{1,2}):(\d{2})", text)
    if not match:
        raise RuntimeError("Could not locate OP.GG update timestamp")
    year, month, day, hour, minute = map(int, match.groups())
    return datetime(year, month, day, hour, minute).strftime("%Y-%m-%d %H:%M JST")


def parse_top100() -> list[dict[str, object]]:
    soup = BeautifulSoup(TIER_HTML.read_text(encoding="utf-8", errors="ignore"), "lxml")
    updated_jst = parse_snapshot_time(soup)
    by_rank: dict[int, dict[str, object]] = {}
    for anchor in soup.select('a[href^="/ja/pokemon-champions/pokedex/"]'):
        href = anchor.get("href", "")
        rank_node = anchor.select_one("span.tabular-nums")
        if not rank_node:
            continue
        match = re.search(r"\d+", rank_node.get_text(" ", strip=True))
        if not match:
            continue
        rank = int(match.group())
        if rank > 100 or rank in by_rank:
            continue
        slug = href.rstrip("/").rsplit("/", 1)[-1]
        image = anchor.select_one("img[alt]")
        name = image.get("alt", "") if image else ""
        types = [node.get_text(" ", strip=True) for node in anchor.select('span[style*="background-color"]')]
        by_rank[rank] = {
            "rank": rank,
            "slug": slug,
            "name_ja": name,
            "types_ja": types,
            "season": "M-4",
            "format": "single",
            "updated_jst": updated_jst,
            "source_url": SOURCE_URL,
        }
    result = [by_rank[r] for r in sorted(by_rank)]
    if len(result) != 100:
        raise RuntimeError(f"Expected 100 ranked Pokemon, found {len(result)}")
    (ROOT / "snapshot_meta.json").write_text(
        json.dumps({
            "season": "M-4",
            "format": "single",
            "updated_jst": updated_jst,
            "source_url": SOURCE_URL,
            "pokemon_count": len(result),
        }, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return result


def write_snapshot(rows: list[dict[str, object]]) -> None:
    with (ROOT / "top100.json").open("w", encoding="utf-8") as handle:
        json.dump(rows, handle, ensure_ascii=False, indent=2)
    with (ROOT / "top100.csv").open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["rank", "slug", "name_ja", "types_ja", "season", "format", "updated_jst", "source_url"])
        for row in rows:
            writer.writerow([
                row["rank"], row["slug"], row["name_ja"], "/".join(row["types_ja"]),
                row["season"], row["format"], row["updated_jst"], row["source_url"],
            ])


def fetch_details(rows: list[dict[str, object]]) -> None:
    DETAIL_DIR.mkdir(parents=True, exist_ok=True)
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/126 Safari/537.36",
        "Accept-Language": "ja,en;q=0.8",
    })
    failures: list[dict[str, object]] = []
    metadata: list[dict[str, object]] = []
    for index, row in enumerate(rows, start=1):
        slug = str(row["slug"])
        url = DETAIL_URL.format(slug=slug)
        response = None
        error = None
        for attempt in range(4):
            try:
                response = session.get(url, timeout=90)
                if response.status_code == 200 and len(response.content) > 10_000:
                    break
                error = f"HTTP {response.status_code}, {len(response.content)} bytes"
            except Exception as exc:  # noqa: BLE001
                error = repr(exc)
            time.sleep(1.5 * (attempt + 1))
        if response is None or response.status_code != 200 or len(response.content) <= 10_000:
            failures.append({"rank": row["rank"], "slug": slug, "url": url, "error": error})
            continue
        html_path = DETAIL_DIR / f"{int(row['rank']):03d}_{slug}.html"
        html_path.write_bytes(response.content)
        metadata.append({
            "rank": row["rank"],
            "slug": slug,
            "url": url,
            "status": response.status_code,
            "bytes": len(response.content),
            "date": response.headers.get("date"),
            "x_pathname": response.headers.get("x-pathname"),
        })
        print(f"[{index:03d}/100] {slug}: {len(response.content)} bytes", flush=True)
        time.sleep(0.20)
    (ROOT / "detail_fetch_metadata.json").write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")
    (ROOT / "detail_fetch_failures.json").write_text(json.dumps(failures, ensure_ascii=False, indent=2), encoding="utf-8")
    if failures:
        raise RuntimeError(f"Failed to fetch {len(failures)} detail pages")


if __name__ == "__main__":
    top100 = parse_top100()
    write_snapshot(top100)
    fetch_details(top100)
