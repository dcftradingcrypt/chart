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


def main() -> None:
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

        moves = []
        for entry in detail.get("moves", []):
            lookup_entry = lookup_maps["moves"].get(entry["id"], {})
            moves.append({**entry, **{k: v for k, v in lookup_entry.items() if k != "id"}})

        abilities = []
        for entry in detail.get("abilities", []):
            lookup_entry = lookup_maps["abilities"].get(entry["id"], {})
            abilities.append({**entry, **{k: v for k, v in lookup_entry.items() if k != "id"}})

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
            # The usage payload stores the PokeAPI nature ID minus one.
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
            "updatedAt": "2026-07-25 00:30 JST",
            "season": "M-4",
            "format": "single",
            "moves": moves,
            "abilities": abilities,
            "items": items,
            "natures": natures,
            "training": training,
            "mega": detail.get("mega"),
        }
        records.append(record)

        for category, entries in (
            ("move", moves), ("ability", abilities), ("item", items),
            ("nature", natures), ("training", training),
        ):
            for position, entry in enumerate(entries, start=1):
                long_rows.append({
                    "rank": rank,
                    "slug": pokemon["key"],
                    "category": category,
                    "position": position,
                    "key": entry.get("key") or entry.get("name") or entry.get("spread"),
                    "name_ja": entry.get("name", ""),
                    "usage_percent": entry.get("usagePercent"),
                    "spread": entry.get("spread", ""),
                    "rounded_zero": entry.get("roundedZero", False),
                })

    if len(records) != 100:
        raise RuntimeError(f"Expected 100 detail records, found {len(records)}")

    (ROOT / "opgg_usage.json").write_text(
        json.dumps(records, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    columns = [
        "rank", "slug", "category", "position", "key", "name_ja",
        "usage_percent", "spread", "rounded_zero",
    ]
    with (ROOT / "opgg_usage_long.csv").open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        writer.writerows(long_rows)

    print(f"Parsed {len(records)} Pokemon and {len(long_rows)} usage rows")


if __name__ == "__main__":
    main()
