"""
Google Places Scraper
Scrapes business listings using the Google Places API (Text Search + Place Details).

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

HOW IT WORKS
────────────
Google Places has two relevant endpoints:

1. Text Search  (/textsearch)
   Accepts a free-text query like "restaurants in Greenville SC" and returns
   up to 20 results per page, max 3 pages (60 results) per query. Each result
   includes name, address, place_id, types, and rating — but NOT phone or website.

2. Place Details  (/details)
   Accepts a place_id and returns the full record including phone, website,
   opening hours, and more. One API call per place.

STRATEGY
────────
Because each query is capped at 60 results, we run multiple overlapping queries
per location to maximise coverage:
  "restaurants in X"   — broad catch-all
  "bars in X"          — catches bars/lounges the restaurant query misses
  "coffee shops in X"  — cafes often excluded from "restaurants"
  "food trucks in X"   — street food vendors
  ...and so on, configured per scrape.

Results are deduplicated by place_id across all queries. Then Place Details
are fetched for each unique place to get phone + website.

COST
────
  Text Search:   $32 per 1,000 requests  (1 req = 1 page = up to 20 results)
  Place Details: $17 per 1,000 requests  (1 req = 1 place)

Typical cost for a mid-size city with 4 queries:
  4 queries × 3 pages  = 12 Text Search calls  = ~$0.38
  ~150 unique places   = 150 Details calls     = ~$2.55
  Total: ~$3 per city

API KEY SETUP
─────────────
1. Go to https://console.cloud.google.com
2. Create a project → Enable "Places API"
3. Create an API key → restrict it to "Places API" for safety
4. Set environment variable:
     export GOOGLE_PLACES_API_KEY="your_key_here"
   Or pass it as the second argument:
     python google_places_scraper.py "restaurants in Greenville SC" YOUR_KEY

USAGE
─────
# Single query — outputs to ../data/
python google_places_scraper.py "restaurants in Greenville SC"

# Multiple queries for better coverage (comma-separated or use --queries flag)
python google_places_scraper.py "restaurants in Greenville SC" --queries \
    "bars in Greenville SC" \
    "coffee shops in Greenville SC" \
    "food trucks in Greenville SC"

# With explicit API key
python google_places_scraper.py "restaurants in Greenville SC" --key YOUR_KEY

OUTPUT
──────
CSV saved to ../data/ with columns matching all other scrapers:
  name, street, city, state, zip, phone, website, description, source_url

LIMITS & NOTES
──────────────
- Google caps results at 60 per query (3 pages × 20). Running multiple
  queries with different category terms is the main workaround.
- Place Details adds ~$17/1000 but is required for phone + website.
- Rate limit: 10 requests/second. The scraper sleeps between Detail calls.
- Results include chains (McDonald's etc.) — filter by type if needed.
- "source_url" is set to the Google Maps URL for the place so you can
  verify or link to it.
"""

import sys
import os
import csv
import json
import time
import re
import argparse
from datetime import date
from urllib.parse import urlparse, urlencode

try:
    import requests
except ImportError:
    print("Missing dependency: pip install requests")
    sys.exit(1)


# ── Config ────────────────────────────────────────────────────────────────────

OUTPUT_FIELDS = ["name", "street", "city", "state", "zip", "phone",
                 "website", "description", "source_url"]

BASE_URL = "https://maps.googleapis.com/maps/api/place"

# Default category queries appended when using --expand flag
CATEGORY_SUFFIXES = [
    "restaurants",
    "bars and nightlife",
    "coffee shops and cafes",
    "food trucks",
    "breweries and taprooms",
    "wineries",
]


# ── API calls ─────────────────────────────────────────────────────────────────

def text_search(query, api_key, page_token=None):
    """Single Text Search call. Returns raw API response dict."""
    params = {"key": api_key}
    if page_token:
        params["pagetoken"] = page_token
        time.sleep(2)  # Google requires delay before using a page token
    else:
        params["query"] = query
    r = requests.get(f"{BASE_URL}/textsearch/json", params=params, timeout=15)
    r.raise_for_status()
    return r.json()


def get_all_text_results(query, api_key):
    """
    Fetch all pages for a single query.
    Google caps at 3 pages × 20 results = 60 max per query.
    """
    all_results = []
    token = None
    page = 1
    while True:
        print(f"    Page {page}: {query[:60]}")
        data = text_search(query, api_key, page_token=token)
        status = data.get("status")
        if status == "ZERO_RESULTS":
            break
        if status != "OK":
            print(f"    API error: {status} — {data.get('error_message', '')}")
            break
        results = data.get("results", [])
        all_results.extend(results)
        token = data.get("next_page_token")
        if not token:
            break
        page += 1
    return all_results


def get_place_details(place_id, api_key):
    """
    Fetch full details for one place.
    Fields chosen to minimise cost — only what we actually use.
    """
    params = {
        "place_id": place_id,
        "fields": "name,formatted_address,formatted_phone_number,website,url,types",
        "key": api_key,
    }
    r = requests.get(f"{BASE_URL}/details/json", params=params, timeout=15)
    r.raise_for_status()
    return r.json().get("result", {})


# ── Address parsing ───────────────────────────────────────────────────────────

# Full state name → abbreviation (matches playwright_scraper.py)
STATE_ABBR = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR",
    "california": "CA", "colorado": "CO", "connecticut": "CT", "delaware": "DE",
    "florida": "FL", "georgia": "GA", "hawaii": "HI", "idaho": "ID",
    "illinois": "IL", "indiana": "IN", "iowa": "IA", "kansas": "KS",
    "kentucky": "KY", "louisiana": "LA", "maine": "ME", "maryland": "MD",
    "massachusetts": "MA", "michigan": "MI", "minnesota": "MN", "mississippi": "MS",
    "missouri": "MO", "montana": "MT", "nebraska": "NE", "nevada": "NV",
    "new hampshire": "NH", "new jersey": "NJ", "new mexico": "NM", "new york": "NY",
    "north carolina": "NC", "north dakota": "ND", "ohio": "OH", "oklahoma": "OK",
    "oregon": "OR", "pennsylvania": "PA", "rhode island": "RI", "south carolina": "SC",
    "south dakota": "SD", "tennessee": "TN", "texas": "TX", "utah": "UT",
    "vermont": "VT", "virginia": "VA", "washington": "WA", "west virginia": "WV",
    "wisconsin": "WI", "wyoming": "WY", "district of columbia": "DC",
    "puerto rico": "PR", "guam": "GU", "virgin islands": "VI",
}


def parse_address(formatted_address):
    """
    Split Google's formatted_address into components.

    Examples:
      "123 Main St, Chapel Hill, NC 27514, USA"
        → street="123 Main St", city="Chapel Hill", state="NC", zip="27514"

      "Pinehurst, NC 28374, USA"   (no street number)
        → street="", city="Pinehurst", state="NC", zip="28374"
    """
    if not formatted_address:
        return "", "", "", ""

    # Strip trailing ", USA" / ", United States"
    addr = re.sub(r",?\s*(USA|United States)\s*$", "", formatted_address).strip()

    parts = [p.strip() for p in addr.split(",")]

    # Last part is always "ST NNNNN"
    state, zip_code = "", ""
    if parts:
        last = parts[-1].strip()
        m = re.match(r"([A-Z]{2})\s+(\d{5})", last)
        if m:
            state    = m.group(1)
            zip_code = m.group(2)
            parts    = parts[:-1]

    # Second-to-last is city
    city = parts[-1].strip() if parts else ""
    if parts:
        parts = parts[:-1]

    # Everything remaining is street
    street = ", ".join(parts).strip()

    # Normalise state to abbreviation in case it came as full name
    if state:
        state = STATE_ABBR.get(state.lower(), state)

    return street, city, state, zip_code


# ── Core scrape ───────────────────────────────────────────────────────────────

def scrape_places(queries, api_key):
    """
    Run all queries, deduplicate by place_id, fetch details, return records.
    """
    # Phase 1: collect all unique places across all queries
    all_places = {}  # place_id → raw result
    for query in queries:
        print(f"\n  Searching: {query}")
        results = get_all_text_results(query, api_key)
        new = 0
        for result in results:
            pid = result.get("place_id")
            if pid and pid not in all_places:
                all_places[pid] = result
                new += 1
        print(f"    +{new} new places (total unique: {len(all_places)})")

    if not all_places:
        return []

    # Phase 2: fetch details for each unique place
    print(f"\n  Fetching details for {len(all_places)} places...")
    records = []
    for i, (place_id, place) in enumerate(all_places.items(), 1):
        if i % 20 == 0:
            print(f"    {i}/{len(all_places)}...")

        details = get_place_details(place_id, api_key)

        # Parse address — prefer details response, fall back to search result
        raw_addr = details.get("formatted_address") or place.get("formatted_address", "")
        street, city, state, zip_code = parse_address(raw_addr)

        # Google Maps URL for the place (source_url)
        maps_url = details.get("url", f"https://www.google.com/maps/place/?q=place_id:{place_id}")

        records.append({
            "name":        details.get("name") or place.get("name", ""),
            "street":      street,
            "city":        city,
            "state":       state,
            "zip":         zip_code,
            "phone":       details.get("formatted_phone_number", ""),
            "website":     details.get("website", ""),
            "description": "",
            "source_url":  maps_url,
        })

        time.sleep(0.05)  # ~20 req/s, well under the 100 req/s limit

    return records


def enrich_from_csv(csv_path, api_key, location_hint=""):
    """
    Read an existing CSV, look up each business by name on Google Places,
    and fill in missing street/phone/website. Writes back in place.
    Location hint (e.g. "Chapel Hill NC") is appended to each name search
    to improve accuracy.
    """
    with open(csv_path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    fieldnames = list(rows[0].keys()) if rows else OUTPUT_FIELDS

    # Infer location hint from existing city/state if not provided
    if not location_hint:
        cities  = [r.get("city","")  for r in rows if r.get("city")]
        states  = [r.get("state","") for r in rows if r.get("state")]
        if cities and states:
            location_hint = f"{cities[0]} {states[0]}"
        elif cities:
            location_hint = cities[0]

    print(f"Enriching {len(rows)} records from: {csv_path}")
    print(f"Location hint: {location_hint!r}")

    needs_enrich = [r for r in rows
                    if not r.get("street") or not r.get("phone") or not r.get("website")
                    or not r.get("city") or not r.get("zip")]
    print(f"Records needing enrichment: {len(needs_enrich)}/{len(rows)}")

    filled_street = filled_phone = filled_website = 0

    for i, record in enumerate(rows, 1):
        if record.get("street") and record.get("phone") and record.get("website") \
                and record.get("city") and record.get("zip"):
            continue

        name = record.get("name", "").strip()
        if not name:
            continue

        query = f"{name} {location_hint}".strip()
        try:
            results = get_all_text_results(query, api_key)
        except Exception as e:
            print(f"  [{i}/{len(rows)}] ✗ {name}: {e}")
            continue

        if not results:
            print(f"  [{i}/{len(rows)}] – {name}: no results")
            continue

        # Take the first result — most relevant match
        place = results[0]
        place_id = place.get("place_id")
        if not place_id:
            continue

        try:
            details = get_place_details(place_id, api_key)
        except Exception as e:
            print(f"  [{i}/{len(rows)}] ✗ {name} (details): {e}")
            continue

        raw_addr = details.get("formatted_address") or place.get("formatted_address", "")
        street, city, state, zip_code = parse_address(raw_addr)

        updated = []
        if not record.get("street") and street:
            record["street"] = street
            updated.append("street")
        if street and not record.get("city") and city:
            record["city"] = city
            updated.append("city")
        if street and not record.get("state") and state:
            record["state"] = state
        if street and not record.get("zip") and zip_code:
            record["zip"] = zip_code
            updated.append("zip")
        if not record.get("street") and not record.get("city"):
            # Full address from Google even if we had nothing
            if street:
                record["street"] = street
                record["city"]   = city
                record["state"]  = state
                record["zip"]    = zip_code
                filled_street += 1
                updated.append("street")
        elif "street" in updated:
            filled_street += 1
        if not record.get("phone") and details.get("formatted_phone_number"):
            record["phone"] = details["formatted_phone_number"]
            filled_phone += 1
            updated.append("phone")
        if not record.get("website") and details.get("website"):
            record["website"] = details["website"]
            filled_website += 1
            updated.append("website")

        status = f"+{','.join(updated)}" if updated else "no new data"
        print(f"  [{i}/{len(rows)}] {name}: {status}")
        time.sleep(0.05)

    # Write back
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    print(f"\n✅ Enriched {csv_path}")
    print(f"   Street filled:  +{filled_street}")
    print(f"   Phone filled:   +{filled_phone}")
    print(f"   Website filled: +{filled_website}")


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Scrape business listings from Google Places API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("query", nargs="?",
                        help='Primary search query, e.g. "restaurants in Greenville SC"')
    parser.add_argument("--csv", metavar="FILE",
                        help="Enrich an existing CSV — looks up each business by name and fills missing fields")
    parser.add_argument("--location", metavar="LOCATION",
                        help='Location hint for --csv mode, e.g. "Chapel Hill NC" (auto-detected if omitted)')
    parser.add_argument("--queries", nargs="+", metavar="QUERY",
                        help="Additional queries to run (results are merged and deduplicated)")
    parser.add_argument("--expand", action="store_true",
                        help="Auto-generate category queries from the location in the primary query")
    parser.add_argument("--key", metavar="API_KEY",
                        help="Google Places API key (or set GOOGLE_PLACES_API_KEY env var)")
    args = parser.parse_args()

    if not args.query and not args.csv:
        parser.error("provide a search query or --csv FILE")

    # Resolve API key
    api_key = (
        args.key
        or os.environ.get("GOOGLE_PLACES_API_KEY")
        or os.environ.get("PLACES_API_KEY")
    )
    if not api_key:
        api_key = input("Enter Google Places API key: ").strip()
    if not api_key:
        print("Error: no API key provided.")
        sys.exit(1)

    # ── CSV enrichment mode ──
    if args.csv:
        enrich_from_csv(args.csv, api_key, location_hint=args.location or "")
        return

    # ── Normal query mode ──
    # Build query list
    queries = [args.query]
    if args.queries:
        queries.extend(args.queries)
    if args.expand:
        # Extract location from primary query by stripping leading category word(s)
        # e.g. "restaurants in Greenville SC" → "Greenville SC"
        location = re.sub(r"^[\w\s]+ in ", "", args.query, flags=re.I).strip()
        for suffix in CATEGORY_SUFFIXES:
            q = f"{suffix} in {location}"
            if q not in queries:
                queries.append(q)
        print(f"Expanded to {len(queries)} queries for: {location}")

    print(f"\nScraping Google Places")
    print(f"Queries ({len(queries)}):")
    for q in queries:
        print(f"  • {q}")

    records = scrape_places(queries, api_key)

    if not records:
        print("\n❌ No records found.")
        sys.exit(1)

    # Build output filename from primary query
    slug = re.sub(r"[^\w\s-]", "", args.query.lower())
    slug = re.sub(r"\s+", "_", slug.strip())[:60]
    datestamp = date.today().strftime("%Y-%m-%d")
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(script_dir, "..", "data")
    os.makedirs(data_dir, exist_ok=True)
    output_file = os.path.join(data_dir, f"google_places_{slug}_{datestamp}.csv")

    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_FIELDS)
        writer.writeheader()
        writer.writerows(records)

    print(f"\n✅ Done — {len(records)} records saved to {output_file}")
    print(f"   With address: {sum(1 for r in records if r.get('street'))}")
    print(f"   With phone:   {sum(1 for r in records if r.get('phone'))}")
    print(f"   With website: {sum(1 for r in records if r.get('website'))}")


if __name__ == "__main__":
    main()