#!/usr/bin/env python3
from __future__ import annotations

import argparse
import concurrent.futures
import datetime as dt
import hashlib
import json
import re
import sys
import time
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup, Tag

BASE = "https://op.gg"
TIER_URL = f"{BASE}/ja/pokemon-champions/tier"
UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/150 Safari/537.36"

STAT_JA_TO_ID = {
    "HP": "hp",
    "こうげき": "atk",
    "ぼうぎょ": "def",
    "とくこう": "spa",
    "とくぼう": "spd",
    "すばやさ": "spe",
}
NATURE_JA_TO_EN = {
    "がんばりや": "Hardy", "さみしがり": "Lonely", "ゆうかん": "Brave", "いじっぱり": "Adamant", "やんちゃ": "Naughty",
    "ずぶとい": "Bold", "すなお": "Docile", "のんき": "Relaxed", "わんぱく": "Impish", "のうてんき": "Lax",
    "おくびょう": "Timid", "せっかち": "Hasty", "まじめ": "Serious", "ようき": "Jolly", "むじゃき": "Naive",
    "ひかえめ": "Modest", "おっとり": "Mild", "れいせい": "Quiet", "てれや": "Bashful", "うっかりや": "Rash",
    "おだやか": "Calm", "おとなしい": "Gentle", "なまいき": "Sassy", "しんちょう": "Careful", "きまぐれ": "Quirky",
}
MOVE_CATEGORY_JA_TO_EN = {"物理": "Physical", "特殊": "Special", "変化": "Status"}


def norm_text(x: str) -> str:
    return re.sub(r"\s+", " ", x).strip()


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def fetch(session: requests.Session, url: str, attempts: int = 7) -> tuple[bytes, dict[str, str]]:
    last: Exception | None = None
    for i in range(attempts):
        try:
            r = session.get(url, timeout=(20, 60), headers={"User-Agent": UA, "Accept-Language": "ja-JP,ja;q=0.9,en;q=0.5"})
            r.raise_for_status()
            if len(r.content) < 10000:
                raise RuntimeError(f"short response {len(r.content)} bytes")
            return r.content, dict(r.headers)
        except Exception as e:
            last = e
            time.sleep(min(20, 1.5 ** i))
    raise RuntimeError(f"failed to fetch {url}: {last}")


def parse_update_time(soup: BeautifulSoup) -> str:
    text = norm_text(soup.get_text(" ", strip=True))
    m = re.search(r"更新日\s*(\d{4}年\d{1,2}月\d{1,2}日\s*\d{1,2}:\d{2})", text)
    if not m:
        raise ValueError("visible update time not found")
    raw = re.sub(r"\s+", " ", m.group(1))
    mm = re.fullmatch(r"(\d{4})年(\d{1,2})月(\d{1,2})日\s*(\d{1,2}):(\d{2})", raw)
    assert mm
    y, mo, d, h, mi = map(int, mm.groups())
    return f"{y:04d}-{mo:02d}-{d:02d} {h:02d}:{mi:02d} JST"


def parse_tier(html: bytes, source_url: str = TIER_URL) -> dict[str, Any]:
    soup = BeautifulSoup(html, "lxml")
    update = parse_update_time(soup)
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for a in soup.find_all("a", href=True):
        href = a.get("href", "")
        if "/pokemon-champions/pokedex/" not in href:
            continue
        slug = href.rstrip("/").split("/")[-1]
        if slug in seen:
            continue
        rank = None
        for sp in a.find_all("span"):
            txt = norm_text(sp.get_text(" ", strip=True))
            m = re.fullmatch(r"#\s*(\d+)", txt)
            if m:
                rank = int(m.group(1))
                break
        if rank is None:
            m = re.search(r"#\s*(\d+)", norm_text(a.get_text(" ", strip=True)))
            if not m:
                continue
            rank = int(m.group(1))
        image = a.find("img", alt=True)
        name_ja = image.get("alt", "").strip() if image else ""
        if not name_ja:
            candidates = [norm_text(x.get_text(" ", strip=True)) for x in a.find_all("span")]
            name_ja = next((x for x in candidates if x and not x.startswith("#") and x not in {"=", "新着"} and not x.isdigit()), "")
        types_ja: list[str] = []
        types_en: list[str] = []
        for img in a.find_all("img", src=re.compile(r"/type/")):
            src = img.get("src", "")
            m = re.search(r"/type/([a-z-]+)\.svg", src)
            if m:
                types_en.append(m.group(1).title())
                parent_text = norm_text(img.parent.get_text(" ", strip=True)) if img.parent else ""
                if parent_text:
                    types_ja.append(parent_text)
        rows.append({
            "rank": rank,
            "slug": slug,
            "name_ja": name_ja,
            "types_ja": types_ja,
            "types_en": types_en,
            "source_url": urljoin(BASE, href),
        })
        seen.add(slug)
    rows.sort(key=lambda r: r["rank"])
    return {
        "source_url": source_url,
        "update_time_jst": update,
        "page_sha256": sha256_bytes(html),
        "count": len(rows),
        "rows": rows,
    }


def battle_section(soup: BeautifulSoup, label: str) -> Tag | None:
    for sec in soup.find_all("section"):
        direct = [c for c in sec.children if isinstance(c, Tag)]
        if not direct:
            continue
        head = direct[0]
        if head.name == "div" and norm_text(head.get_text(" ", strip=True)) == label:
            return sec
    return None


def direct_cards(section: Tag) -> list[Tag]:
    direct = [c for c in section.children if isinstance(c, Tag)]
    if len(direct) < 2:
        return []
    body = direct[1]
    return [c for c in body.children if isinstance(c, Tag) and c.name == "div"]


def parse_rank_percent(card: Tag) -> tuple[int, float]:
    vals: list[str] = []
    for sp in card.find_all("span"):
        txt = norm_text(sp.get_text(" ", strip=True))
        if txt:
            vals.append(txt)
    rank = next((int(x) for x in vals if re.fullmatch(r"\d+", x)), None)
    pct = next((float(x[:-1]) for x in vals if re.fullmatch(r"\d+(?:\.\d+)?%", x)), None)
    if rank is None or pct is None:
        raise ValueError(f"rank/percentage not found: {norm_text(card.get_text(' ', strip=True))[:200]}")
    return rank, pct


def labeled_value(card: Tag, label: str) -> str | None:
    for sp in card.find_all("span"):
        if norm_text(sp.get_text(" ", strip=True)) == label:
            sib = sp.find_next_sibling("span")
            if sib:
                return norm_text(sib.get_text(" ", strip=True))
    return None


def parse_move_cards(section: Tag | None) -> list[dict[str, Any]]:
    if section is None:
        return []
    out: list[dict[str, Any]] = []
    for card in direct_cards(section):
        rank, pct = parse_rank_percent(card)
        a = card.find("a", href=re.compile(r"/moves/"))
        if not a:
            continue
        slug = a.get("href", "").rstrip("/").split("/")[-1]
        name_span = a.find("span", class_=lambda c: c and "font-semibold" in c and "truncate" in c)
        name_ja = norm_text(name_span.get_text(" ", strip=True)) if name_span else slug
        cat_span = a.find("span", attrs={"aria-label": True})
        category_ja = cat_span.get("aria-label") if cat_span else None
        type_en = None
        img = a.find("img", src=re.compile(r"/type/"))
        if img:
            m = re.search(r"/type/([a-z-]+)\.svg", img.get("src", ""))
            if m:
                type_en = m.group(1).title()
        power_raw = labeled_value(card, "威力")
        accuracy_raw = labeled_value(card, "命中")
        priority_raw = labeled_value(card, "優先")
        out.append({
            "rank": rank,
            "usage_percent": pct,
            "slug": slug,
            "name_ja": name_ja,
            "type_en": type_en,
            "category_ja": category_ja,
            "category_en": MOVE_CATEGORY_JA_TO_EN.get(category_ja or ""),
            "power_display": power_raw,
            "accuracy_display": accuracy_raw,
            "priority": int(priority_raw) if priority_raw and re.fullmatch(r"-?\d+", priority_raw) else None,
            "target_ja": labeled_value(card, "対象"),
        })
    return out


def parse_link_cards(section: Tag | None, kind: str) -> list[dict[str, Any]]:
    if section is None:
        return []
    out: list[dict[str, Any]] = []
    for card in direct_cards(section):
        rank, pct = parse_rank_percent(card)
        a = card.find("a", href=re.compile(fr"/{kind}/"))
        if not a:
            continue
        slug = a.get("href", "").rstrip("/").split("/")[-1]
        name_span = a.find("span", class_=lambda c: c and "font-semibold" in c and "truncate" in c)
        name_ja = norm_text(name_span.get_text(" ", strip=True)) if name_span else slug
        out.append({"rank": rank, "usage_percent": pct, "slug": slug, "name_ja": name_ja})
    return out


def parse_natures(section: Tag | None) -> list[dict[str, Any]]:
    if section is None:
        return []
    out = []
    for card in direct_cards(section):
        rank, pct = parse_rank_percent(card)
        strings = [norm_text(x) for x in card.stripped_strings if norm_text(x)]
        name_ja = next((x for x in strings[2:] if x not in {"+", "-"} and x not in STAT_JA_TO_ID and not re.fullmatch(r"\d+(?:\.\d+)?%", x)), "")
        out.append({"rank": rank, "usage_percent": pct, "name_ja": name_ja, "name_en": NATURE_JA_TO_EN.get(name_ja)})
    return out


def parse_training(section: Tag | None) -> list[dict[str, Any]]:
    if section is None:
        return []
    out = []
    for card in direct_cards(section):
        rank, pct = parse_rank_percent(card)
        spread = {k: 0 for k in ("hp", "atk", "def", "spa", "spd", "spe")}
        for sp in card.find_all("span"):
            label = norm_text(sp.get_text(" ", strip=True))
            if label in STAT_JA_TO_ID:
                parent = sp.parent
                vals = [norm_text(x) for x in parent.stripped_strings if norm_text(x)] if parent else []
                num = next((int(x) for x in vals[1:] if re.fullmatch(r"\d+", x)), None)
                if num is not None:
                    spread[STAT_JA_TO_ID[label]] = num
        out.append({"rank": rank, "usage_percent": pct, "spread": spread, "spread_key": "-".join(f"{spread[k]:02x}" for k in ("hp","atk","def","spa","spd","spe"))})
    return out


def parse_base_stats(soup: BeautifulSoup) -> dict[str, int]:
    for h in soup.find_all("h3"):
        if norm_text(h.get_text(" ", strip=True)) != "ステータス":
            continue
        sec = h.parent
        text = norm_text(sec.get_text(" ", strip=True))
        labels = {"HP":"hp", "Attack":"atk", "Defense":"def", "Sp. Atk":"spa", "Sp. Def":"spd", "Speed":"spe"}
        out = {}
        for label, key in labels.items():
            m = re.search(re.escape(label) + r"\s*(\d+)", text)
            if m:
                out[key] = int(m.group(1))
        if len(out) == 6:
            return out
    return {}


def parse_pokemon_page(html: bytes, row: dict[str, Any]) -> dict[str, Any]:
    soup = BeautifulSoup(html, "lxml")
    updated = parse_update_time(soup)
    moves = parse_move_cards(battle_section(soup, "わざ"))
    win_moves = parse_move_cards(battle_section(soup, "勝ち技"))
    items = parse_link_cards(battle_section(soup, "持ち物"), "items")
    abilities = parse_link_cards(battle_section(soup, "特性"), "abilities")
    natures = parse_natures(battle_section(soup, "性格補正"))
    training = parse_training(battle_section(soup, "努力値"))
    if not moves or not items or not abilities or not natures or not training:
        raise ValueError(f"missing battle section for {row['slug']}: moves={len(moves)} items={len(items)} abilities={len(abilities)} natures={len(natures)} training={len(training)}")
    return {
        **row,
        "update_time_jst": updated,
        "page_sha256": sha256_bytes(html),
        "base_stats": parse_base_stats(soup),
        "moves": moves,
        "win_moves": win_moves,
        "items": items,
        "abilities": abilities,
        "natures": natures,
        "training": training,
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--workers", type=int, default=6)
    ap.add_argument("--top", type=int, default=100)
    ap.add_argument("--tier-html", help="parse a local tier HTML instead of fetching")
    ap.add_argument("--single-html", help="parse one local Pokemon HTML with --single-slug")
    ap.add_argument("--single-slug")
    args = ap.parse_args()
    out = Path(args.out)
    raw = out / "raw"
    raw.mkdir(parents=True, exist_ok=True)

    if args.tier_html:
        tier_html = Path(args.tier_html).read_bytes()
    else:
        tier_html, _ = fetch(requests.Session(), TIER_URL)
    tier_start = parse_tier(tier_html)
    (raw / "tier_start.html").write_bytes(tier_html)
    rows = tier_start["rows"][: args.top]
    if len(rows) != args.top or [r["rank"] for r in rows] != list(range(1, args.top + 1)):
        raise RuntimeError(f"top list invalid: got {len(rows)} rows, ranks={[r['rank'] for r in rows[:10]]}...")

    if args.single_html:
        slug = args.single_slug or rows[0]["slug"]
        row = next((r for r in rows if r["slug"] == slug), {"rank": 0, "slug": slug, "name_ja": slug, "types_ja": [], "types_en": [], "source_url": f"{BASE}/ja/pokemon-champions/pokedex/{slug}"})
        data = parse_pokemon_page(Path(args.single_html).read_bytes(), row)
        (out / f"{slug}.json").write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        print(json.dumps(data, ensure_ascii=False, indent=2))
        return 0

    def one(row: dict[str, Any]) -> tuple[dict[str, Any], bytes]:
        body, _ = fetch(requests.Session(), row["source_url"])
        return parse_pokemon_page(body, row), body

    parsed: list[dict[str, Any]] = []
    errors: list[dict[str, str]] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = {ex.submit(one, row): row for row in rows}
        for fut in concurrent.futures.as_completed(futs):
            row = futs[fut]
            try:
                data, body = fut.result()
                parsed.append(data)
                (raw / f"{row['rank']:03d}_{row['slug']}.html").write_bytes(body)
                print(f"captured {row['rank']:03d} {row['slug']} {data['update_time_jst']}", flush=True)
            except Exception as e:
                errors.append({"slug": row["slug"], "error": repr(e)})
                print(f"ERROR {row['slug']}: {e}", file=sys.stderr, flush=True)
    parsed.sort(key=lambda x: x["rank"])

    tier_end_html, _ = fetch(requests.Session(), TIER_URL)
    tier_end = parse_tier(tier_end_html)
    (raw / "tier_end.html").write_bytes(tier_end_html)

    update_times = sorted({x["update_time_jst"] for x in parsed})
    rank_identity_start = [(r["rank"], r["slug"]) for r in tier_start["rows"][:args.top]]
    rank_identity_end = [(r["rank"], r["slug"]) for r in tier_end["rows"][:args.top]]
    validation = {
        "captured_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "tier_start_update_time_jst": tier_start["update_time_jst"],
        "tier_end_update_time_jst": tier_end["update_time_jst"],
        "pokemon_update_times_jst": update_times,
        "top_count": len(rows),
        "parsed_count": len(parsed),
        "errors": errors,
        "ranks_stable": rank_identity_start == rank_identity_end,
        "same_update_cycle": (
            not errors
            and len(parsed) == args.top
            and len(update_times) == 1
            and update_times[0] == tier_start["update_time_jst"] == tier_end["update_time_jst"]
        ),
    }
    (out / "tier_snapshot.json").write_text(json.dumps(tier_start, ensure_ascii=False, indent=2), encoding="utf-8")
    (out / "opponent_usage.json").write_text(json.dumps({"source": tier_start, "pokemon": parsed}, ensure_ascii=False, indent=2), encoding="utf-8")
    (out / "capture_validation.json").write_text(json.dumps(validation, ensure_ascii=False, indent=2), encoding="utf-8")
    if errors or not validation["ranks_stable"] or not validation["same_update_cycle"]:
        print(json.dumps(validation, ensure_ascii=False, indent=2), file=sys.stderr)
        return 2
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
