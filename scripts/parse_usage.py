from __future__ import annotations

import csv
import json
from pathlib import Path

ROOT = Path("out")
DETAIL_DIR = ROOT / "opgg" / "details"


def raw_object(text: str, key: str):
    decoded = text.replace('\\"', '"')
    needle = f'"{key}":'
    index = decoded.find(needle)
    if index < 0:
        raise KeyError(key)
    return json.JSONDecoder().raw_decode(decoded, index + len(needle))[0]


def parse_spread(value: str) -> dict[str, int]:
    values = [int(part, 16) for part in value.split("-")]
    if len(values) != 6:
        raise ValueError(value)
    return dict(zip(("hp", "atk", "def", "spa", "spd", "spe"), values))


def enrich_entries(entries, lookup_map):
    result = []
    for entry in entries:
        lookup_entry = lookup_map.get(entry["id"], {})
        result.append({**entry, **{k: v for k, v in lookup_entry.items() if k != "id"}})
    return result


def main() -> None:
    top100 = json.loads((ROOT / "top100.json").read_text(encoding="utf-8"))
    updated_jst = top100[0]["updated_jst"]
    records = []
    long_rows = []
    for path in sorted(DETAIL_DIR.glob("*.html")):
        rank = int(path.name.split("_", 1)[0])
        text = path.read_text(encoding="utf-8")
        detail = raw_object(text, "singleDetail")
        lookup = raw_object(text, "lookupData")
        lookup_maps = {
            category: {entry["id"]: entry for entry in lookup[category]}
            for category in ("moves", "abilities", "items")
        }
        nature_lookup = {entry["id"]: entry for entry in lookup["natures"]}

        moves = enrich_entries(detail.get("moves", []), lookup_maps["moves"])
        win_moves = enrich_entries(detail.get("win", {}).get("moves", []), lookup_maps["moves"])
        lose_moves = enrich_entries(detail.get("lose", {}).get("moves", []), lookup_maps["moves"])
        abilities = enrich_entries(detail.get("abilities", []), lookup_maps["abilities"])

        items = []
        for entry in detail.get("items", []):
            lookup_entry = lookup_maps["items"].get(entry["id"], {})
            items.append({
                **entry,
                **{k: v for k, v in lookup_entry.items() if k != "id"},
                "roundedZero": entry["usagePercent"] == 0,
            })

        natures = []
        for entry in detail.get("natures", []):
            lookup_id = entry["id"] + 1
            lookup_entry = nature_lookup.get(lookup_id, {})
            natures.append({
                **entry,
                "lookupId": lookup_id,
                **{k: v for k, v in lookup_entry.items() if k != "id"},
            })

        training = []
        for entry in detail.get("training", []):
            training.append({
                **entry,
                "points": parse_spread(entry["spread"]),
                "roundedZero": entry["usagePercent"] == 0,
            })

        pokemon = detail["pokemon"]
        record = {
            "rank": rank,
            "slug": pokemon["key"],
            "pokedexId": pokemon["id"],
            "form": pokemon.get("form", 0),
            "updatedAt": updated_jst,
            "season": "M-4",
            "format": "single",
            "moves": moves,
            "winMoves": win_moves,
            "loseMoves": lose_moves,
            "abilities": abilities,
            "items": items,
            "natures": natures,
            "training": training,
            "mega": detail.get("mega"),
            "team": detail.get("team"),
        }
        records.append(record)

        categories = (
            ("move", moves),
            ("win_move", win_moves),
            ("lose_move", lose_moves),
            ("ability", abilities),
            ("item", items),
            ("nature", natures),
            ("training", training),
        )
        for category, entries in categories:
            for position, entry in enumerate(entries, start=1):
                long_rows.append({
                    "rank": rank,
                    "slug": pokemon["key"],
                    "category": category,
                    "position": position,
                    "id": entry.get("id", ""),
                    "key": entry.get("key") or entry.get("name") or entry.get("spread"),
                    "name_ja": entry.get("name", ""),
                    "usage_percent": entry.get("usagePercent"),
                    "spread": entry.get("spread", ""),
                    "rounded_zero": entry.get("roundedZero", False),
                    "updated_jst": updated_jst,
                })

        for position, entry in enumerate(detail.get("mega", {}).get("use", []), start=1):
            long_rows.append({
                "rank": rank,
                "slug": pokemon["key"],
                "category": "mega_use",
                "position": position,
                "id": entry.get("id", ""),
                "key": entry.get("key", ""),
                "name_ja": "",
                "usage_percent": entry.get("usagePercent"),
                "spread": "",
                "rounded_zero": False,
                "updated_jst": updated_jst,
            })

    if len(records) != 100:
        raise RuntimeError(f"Expected 100 detail records, found {len(records)}")

    (ROOT / "opgg_usage.json").write_text(
        json.dumps(records, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    columns = [
        "rank", "slug", "category", "position", "id", "key", "name_ja",
        "usage_percent", "spread", "rounded_zero", "updated_jst",
    ]
    with (ROOT / "opgg_usage_long.csv").open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        writer.writerows(long_rows)

    print(f"Parsed {len(records)} Pokemon and {len(long_rows)} usage rows at {updated_jst}")


if __name__ == "__main__":
    main()
