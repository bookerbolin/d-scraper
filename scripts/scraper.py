"""
Web Scraper — Command Line
The fully-featured scraper with all fixes, extracted from scraper_app.py.

Usage:
    python3 scraper.py https://downtowndurham.com/dine-drink/
    python3 scraper.py  (will prompt for URL)

Requirements:
    pip install requests beautifulsoup4 lxml

Features:
    - Plain HTML scraping (Durham, Chapel Hill, Hillsborough patterns)
    - SimpleView CMS detection with fallback advice
    - Internal URL resolution with parallel workers
    - Address extraction from embedded text
    - Pagination with deduplication
    - Same CSV schema as Streamlit app and Playwright scraper
"""

import sys
import re
import csv
import time
import random
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed

from common import (
    normalize_state, extract_phone, extract_address_from_text,
    clean_address, looks_like_address, _extract_best_description, resolve_detail_page,
    STATE_ABBR, DOMAIN_STATE, HEADERS,
    fetch_soup, detect_city, is_simpleview, scrape_simpleview_api,
    parse_listings, resolve_website, resolve_all, scrape_html,
    MAX_PAGES, OUTPUT_FIELDS,
)


def scrape(url, log=print):
    log("Detecting site type...")
    if is_simpleview(url):
        log("SimpleView CMS detected — trying API (use playwright_scraper.py for best results)...")
        records = scrape_simpleview_api(url, log)
        return records, False
    records, js_detected = scrape_html(url, log)
    return records, js_detected


def main():
    url = sys.argv[1].strip() if len(sys.argv) > 1 else input("Enter URL to scrape: ").strip()
    if not url.startswith("http"):
        url = "https://" + url
    from datetime import date as _date
    import os as _os
    parsed = urlparse(url)
    domain = parsed.netloc.replace("www.", "").replace(".", "_")
    path_slug = parsed.path.strip("/").replace("/", "_") or "listings"
    datestamp = _date.today().strftime("%Y-%m-%d")
    script_dir = _os.path.dirname(_os.path.abspath(__file__))
    data_dir = _os.path.join(script_dir, "..", "data")
    _os.makedirs(data_dir, exist_ok=True)
    output_file = _os.path.join(data_dir, f"{domain}_{path_slug}_{datestamp}.csv")
    print(f"\nScraping: {url}")
    print(f"Output:   {output_file}\n")
    records, js_detected = scrape(url)
    if js_detected:
        print("\n⚠️  JS-rendered page — no listings in raw HTML.")
        print("   Use playwright_scraper.py for this site.")
        sys.exit(1)
    if not records:
        print("\n❌ No records extracted.")
        sys.exit(1)


    # ── Detail URL construction ───────────────────────────────────────────
    # For sites where detail page URLs are derivable from listing names
    # (no link in card HTML), construct them so the resolution pass can fetch
    # address/phone/website/description from the detail page.
    import re as _re2

    def _name_to_slug(name):
        s = name.lower()
        s = _re2.sub(r"[\u2018\u2019\u0027\u0060]", "", s)
        s = _re2.sub(r"[^a-z0-9]+", "-", s)
        return s.strip("-")

    DETAIL_URL_BUILDERS = {}
    # Detail URL builders would go here for sites where detail page URLs
    # cannot be captured from card links and must be constructed from name.
    # Currently empty — all known sites have capturable links.
    _source_netloc2 = urlparse(url).netloc.replace("www.", "")
    _builder = DETAIL_URL_BUILDERS.get(_source_netloc2)
    if _builder:
        _filled = 0
        for r in records:
            if not r.get("website") and r.get("name"):
                r["website"] = _builder(r["name"], url)
                _filled += 1
        if _filled:
            print(f"  Constructed {_filled} detail URLs for resolution pass")

    # Resolution pass — runs after detail URL construction so all URLs are available
    records = resolve_all(records, urlparse(url).netloc)

    # Remove boilerplate descriptions — any description appearing 3+ times
    # is site-wide copy (footer tagline, generic blurb), not a real business description
    from collections import Counter as _Counter
    desc_counts = _Counter(r.get("description", "")[:120] for r in records if r.get("description"))
    boilerplate = {d for d, n in desc_counts.items() if n >= 3}
    if boilerplate:
        cleared = 0
        for r in records:
            if r.get("description", "")[:120] in boilerplate:
                r["description"] = ""
                cleared += 1
        print(f"  Cleared {cleared} boilerplate descriptions")

    # State backfill — infer from source domain if all records are missing state
    source_netloc = urlparse(url).netloc.replace("www.", "")
    inferred_state = DOMAIN_STATE.get(source_netloc, "")
    if inferred_state:
        missing_state = sum(1 for r in records if not r.get("state"))
        if missing_state == len(records):
            for r in records:
                r["state"] = inferred_state
            print(f"  State backfilled: {inferred_state} ({missing_state} records)")
        elif missing_state > 0:
            for r in records:
                if not r.get("state"):
                    r["state"] = inferred_state
            print(f"  State backfilled for {missing_state} blank records: {inferred_state}")

    with open(output_file, "w", newline="", encoding="utf-8") as f:
        _output_fields = ["name", "street", "city", "state", "zip", "phone", "website", "description", "source_url"]
        writer = csv.DictWriter(f, fieldnames=_output_fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(records)
    print(f"\n✅ Done — {len(records)} records saved to {output_file}")
    print(f"   With address:     {sum(1 for r in records if r.get('street'))}")
    print(f"   With phone:       {sum(1 for r in records if r.get('phone'))}")
    print(f"   With website:     {sum(1 for r in records if r.get('website'))}")
    print(f"   With description: {sum(1 for r in records if r.get('description'))}")


if __name__ == "__main__":
    main()