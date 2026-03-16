"""
Scrape Merger — compares a fresh scrape against a previous CSV.
Identifies new businesses, removed businesses, and produces a merged output.

Usage:
    python merge_scrapes.py old_data.csv new_data.csv
    python merge_scrapes.py old_data.csv new_data.csv --output merged.csv

The "new_data.csv" can be any CSV produced by scraper.py, scraper_app.py,
or playwright_scraper.py — they all share the same schema.

Output files produced:
    merged.csv          — full current list (save this for next time)
    added.csv           — businesses new since last run
    removed.csv         — businesses that disappeared since last run
    changed.csv         — businesses whose address or website changed
"""

import sys
import csv
import re
import argparse
from pathlib import Path
from datetime import date


# ── Normalisation ─────────────────────────────────────────────────────────────

def normalise_name(name):
    """
    Normalise a business name for comparison.
    Strips punctuation, lowercases, collapses whitespace.
    e.g. "Bull City Burger & Brewery" == "bull city burger and brewery"
    """
    name = name.lower().strip()
    name = name.replace("&", "and").replace("+", "and")
    name = re.sub(r"[^\w\s]", "", name)   # strip punctuation
    name = re.sub(r"\s+", " ", name)       # collapse whitespace
    return name


def normalise_street(street):
    """Normalise street address for comparison."""
    street = street.lower().strip()
    # Expand common abbreviations
    replacements = {
        r"\bst\.?\b": "street",
        r"\bave\.?\b": "avenue",
        r"\bblvd\.?\b": "boulevard",
        r"\brd\.?\b": "road",
        r"\bdr\.?\b": "drive",
        r"\bln\.?\b": "lane",
        r"\bct\.?\b": "court",
        r"\bpl\.?\b": "place",
        r"\bste\.?\b": "suite",
    }
    for pattern, replacement in replacements.items():
        street = re.sub(pattern, replacement, street)
    street = re.sub(r"[^\w\s]", " ", street)
    street = re.sub(r"\s+", " ", street).strip()
    return street


def make_key(row):
    """
    Generate a stable match key for a business row.
    Uses normalised name + city as primary key.
    """
    name = normalise_name(row.get("name", ""))
    city = row.get("city", "").lower().strip()
    return (name, city)


def make_address_key(row):
    """Secondary key using street number for disambiguation."""
    street = row.get("street", "")
    numbers = re.findall(r"\d+", street)
    return numbers[0] if numbers else ""


# ── Load CSV ──────────────────────────────────────────────────────────────────

def load_csv(path):
    """Load a CSV file, return list of dicts."""
    with open(path, encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))
    # Normalise column names (strip BOM, whitespace)
    cleaned = []
    for row in rows:
        cleaned.append({k.strip().lstrip("\ufeff"): v.strip() for k, v in row.items()})
    return cleaned


def save_csv(rows, path, fieldnames=None):
    if not rows:
        print(f"  (empty — not saving {path})")
        return
    if fieldnames is None:
        fieldnames = list(rows[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    print(f"  Saved {len(rows)} rows → {path}")


# ── Merge logic ───────────────────────────────────────────────────────────────

def merge(old_rows, new_rows):
    """
    Compare old and new scrape results.
    Returns: (merged, added, removed, changed)
    """
    # Index old rows by (name, city) key
    old_index = {}
    for row in old_rows:
        key = make_key(row)
        if key[0]:  # skip empty names
            old_index[key] = row

    # Index new rows by (name, city) key
    new_index = {}
    for row in new_rows:
        key = make_key(row)
        if key[0]:
            new_index[key] = row

    # Find added, removed, changed, unchanged
    added = []
    removed = []
    changed = []
    merged = []

    # Check every new row against old
    for key, new_row in new_index.items():
        if key not in old_index:
            added.append({**new_row, "change": "added"})
            merged.append(new_row)
        else:
            old_row = old_index[key]
            changes = []

            # Check for meaningful field changes
            for field in ("street", "phone", "website", "city", "state", "zip"):
                old_val = normalise_street(old_row.get(field, "")) if field == "street" else old_row.get(field, "").lower().strip()
                new_val = normalise_street(new_row.get(field, "")) if field == "street" else new_row.get(field, "").lower().strip()
                if old_val and new_val and old_val != new_val:
                    changes.append(f"{field}: '{old_row.get(field)}' → '{new_row.get(field)}'")

            if changes:
                changed.append({
                    **new_row,
                    "change": "updated",
                    "change_detail": "; ".join(changes)
                })

            # Always use the new data in merged output
            merged.append(new_row)

    # Find removed (in old but not in new)
    for key, old_row in old_index.items():
        if key not in new_index:
            removed.append({**old_row, "change": "removed"})

    return merged, added, removed, changed


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Merge two scrape CSVs and show what changed.")
    parser.add_argument("old_csv", help="Previous scrape CSV")
    parser.add_argument("new_csv", help="Fresh scrape CSV")
    parser.add_argument("--output", default=None, help="Output prefix (default: derived from new_csv)")
    args = parser.parse_args()

    old_path = Path(args.old_csv)
    new_path = Path(args.new_csv)

    if not old_path.exists():
        print(f"Error: {old_path} not found")
        sys.exit(1)
    if not new_path.exists():
        print(f"Error: {new_path} not found")
        sys.exit(1)

    print(f"Loading old data: {old_path}")
    old_rows = load_csv(old_path)
    print(f"  {len(old_rows)} rows")

    print(f"Loading new data: {new_path}")
    new_rows = load_csv(new_path)
    print(f"  {len(new_rows)} rows")

    print("\nMerging...")
    merged, added, removed, changed = merge(old_rows, new_rows)

    # Determine output prefix
    prefix = args.output or new_path.stem
    today = date.today().isoformat()

    print(f"\nResults:")
    print(f"  Total in new scrape:  {len(new_rows)}")
    print(f"  Total in old scrape:  {len(old_rows)}")
    print(f"  Added (new):          {len(added)}")
    print(f"  Removed (gone):       {len(removed)}")
    print(f"  Changed (updated):    {len(changed)}")
    print(f"  Unchanged:            {len(merged) - len(added) - len(changed)}")

    print(f"\nSaving outputs:")

    # Merged (full current list — save this for next time)
    base_fields = ["name", "street", "city", "state", "zip", "phone", "website", "description", "source_url"]
    save_csv(merged, f"{prefix}_merged_{today}.csv", fieldnames=base_fields)

    # Added
    if added:
        save_csv(added, f"{prefix}_added_{today}.csv",
                 fieldnames=base_fields + ["change"])
        print(f"\n  New businesses:")
        for r in added[:10]:
            print(f"    + {r['name']} ({r.get('city', '')})")
        if len(added) > 10:
            print(f"    ... and {len(added)-10} more")

    # Removed
    if removed:
        save_csv(removed, f"{prefix}_removed_{today}.csv",
                 fieldnames=base_fields + ["change"])
        print(f"\n  Removed businesses:")
        for r in removed[:10]:
            print(f"    - {r['name']} ({r.get('city', '')})")
        if len(removed) > 10:
            print(f"    ... and {len(removed)-10} more")

    # Changed
    if changed:
        save_csv(changed, f"{prefix}_changed_{today}.csv",
                 fieldnames=base_fields + ["change", "change_detail"])
        print(f"\n  Changed businesses:")
        for r in changed[:10]:
            print(f"    ~ {r['name']}: {r.get('change_detail', '')}")
        if len(changed) > 10:
            print(f"    ... and {len(changed)-10} more")

    print(f"\nDone. Save '{prefix}_merged_{today}.csv' as your new baseline for next time.")


if __name__ == "__main__":
    main()
