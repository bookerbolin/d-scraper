"""
Playwright scraper for SimpleView CMS tourism sites.
Renders JavaScript fully before scraping, bypassing auth requirements.

Usage:
    python playwright_scraper.py https://www.visitchapelhill.org/things-to-do
    python playwright_scraper.py https://www.visitraleigh.com/foodie/drinks/breweries/

Requirements:
    pip install playwright pandas
    playwright install chromium
"""

import sys
import csv
import json
import time
import re
from urllib.parse import urlparse

from playwright.sync_api import sync_playwright


# ── Config ────────────────────────────────────────────────────────────────────

OUTPUT_FIELDS = ["name", "street", "city", "state", "zip", "phone", "website", "description", "source_url"]


# ── Address helpers ───────────────────────────────────────────────────────────

def extract_address_from_text(text):
    if not text:
        return ""
    street_types = (
        "street|st(?:\\.|\\b)|avenue|ave(?:\\.|\\b)|boulevard|blvd(?:\\.|\\b)|"
        "road|rd(?:\\.|\\b)|drive|lane|ln(?:\\.|\\b)|"
        "circle|cir(?:\\.|\\b)|highway|hwy(?:\\.|\\b)|parkway|pkwy(?:\\.|\\b)|"
        "terrace|ter(?:\\.|\\b)|trail|trl(?:\\.|\\b)|pike|alley|broadway"
    )
    false_pos = re.compile(
        r"\d+\s+(beers?|ingredient|piece|item|year|day|hour|minute|mile|foot|feet|oz|lb)\b",
        re.IGNORECASE
    )
    if false_pos.search(text):
        return ""
    pattern = re.compile(
        r"(?<!\d)(\d{1,5}\s+(?:[A-Za-z][\w\s\.]{0,40}?\s+)?(?:" + street_types + r")\.?(?:\s*,?\s*(?:Suite|Ste|Apt|Unit|#)\s*[\w]+)?)",
        re.IGNORECASE
    )
    match = pattern.search(text)
    if match:
        return match.group(1).strip().rstrip(",")
    return ""


def clean_address(text):
    match = re.search(r"\bAddress\s+([^\n]+)", text, re.IGNORECASE)
    if match:
        text = match.group(1).strip()
    text = re.sub(
        r"\s+(features|phone|website|social info|open late|outdoor|lunch|breakfast|dinner)\b.*$",
        "", text, flags=re.IGNORECASE
    ).strip()
    return text


# ── Scraper ───────────────────────────────────────────────────────────────────

def extract_phone(text):
    """Extract first phone number from text. Returns (XXX) XXX-XXXX format."""
    tel_match = re.search(r'tel:\+?1?(\d{10})', text)
    if tel_match:
        digits = tel_match.group(1)
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    pattern = re.compile(r'(\(?\d{3}\)?[\s\-\.]\d{3}[\s\-\.]\d{4})')
    match = pattern.search(text)
    if match:
        digits = re.sub(r'\D', '', match.group(1))
        if len(digits) == 10:
            return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    return ""


def scrape_simpleview(start_url):
    """
    Scrape a SimpleView CMS site by intercepting its XHR API calls
    as the page renders, rather than parsing the HTML.
    """
    parsed = urlparse(start_url)
    base = f"{parsed.scheme}://{parsed.netloc}"
    city = parsed.netloc.replace("www.", "").replace("visit", "").replace(".org", "").replace(".com", "").title()

    all_records = []
    api_responses = []

    print(f"Launching browser for: {start_url}")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = context.new_page()

        # Intercept API responses from SimpleView's REST endpoint
        def handle_response(response):
            if "rest_v2" in response.url and response.status == 200:
                try:
                    data = response.json()
                    print(f"\n  URL: {response.url}")
                    print(f"  Type: {type(data).__name__}")
                    if isinstance(data, list):
                        print(f"  Items: {len(data)}")
                        if data and isinstance(data[0], dict):
                            print(f"  First item keys: {list(data[0].keys())[:8]}")
                    elif isinstance(data, dict):
                        print(f"  Keys: {list(data.keys())}")
                        for k, v in data.items():
                            if isinstance(v, list):
                                print(f"    {k}: list of {len(v)}")
                                if v and isinstance(v[0], dict):
                                    print(f"      First keys: {list(v[0].keys())[:8]}")
                            else:
                                print(f"    {k}: {str(v)[:100]}")
                    if "plugins_listings_listings/find" in response.url:
                        # SimpleView wraps listings in {"docs": {"count": N, "docs": [...]}}
                        if isinstance(data, dict) and "docs" in data:
                            inner = data["docs"]
                            if isinstance(inner, dict) and "docs" in inner:
                                data = inner["docs"]  # unwrap to the actual list
                            elif isinstance(inner, list):
                                data = inner
                        api_responses.append(data)
                except Exception as e:
                    print(f"  Could not parse {response.url}: {e}")

        page.on("response", handle_response)

        # Load the page and wait for network to settle
        print("Loading page...")
        page.goto(start_url, wait_until="networkidle", timeout=30000)

        # Scroll to trigger lazy-loaded content
        print("Scrolling to load all listings...")
        for _ in range(5):
            page.evaluate("window.scrollBy(0, window.innerHeight)")
            time.sleep(1)

        # If API responses were intercepted, use that data (cleanest)
        if api_responses:
            print(f"\nUsing {len(api_responses)} intercepted API response(s)")
            for response_data in api_responses:
                items = response_data if isinstance(response_data, list) else response_data.get("data", [])
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    addr = item.get("address1", item.get("address", ""))
                    all_records.append({
                        "name":        item.get("title", item.get("name", "")),
                        "street":      addr,
                        "city":        item.get("city", city),
                        "state":       item.get("state", "NC"),
                        "zip":         item.get("zip", item.get("postal_code", "")),
                        "website":     item.get("weburl", item.get("url", item.get("website", ""))),
                        "description": item.get("description", item.get("teaser", "")),
                        "source_url":  start_url,
                    })

        else:
            # Fallback: parse the rendered DOM directly
            print("No API responses intercepted — parsing rendered DOM...")
            html = page.content()

            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html, "lxml")

            # Look for listing cards — SimpleView renders them with consistent classes
            cards = (
                soup.select(".sv-listing-card") or
                soup.select(".listing-card") or
                soup.select("[class*='listing']") or
                soup.select("article")
            )

            print(f"Found {len(cards)} potential listing cards in DOM")

            for card in cards:
                name_el = card.select_one("h2, h3, .listing-title, [class*='title']")
                name = name_el.get_text(strip=True) if name_el else ""
                if not name:
                    continue

                # Try to find address
                text = card.get_text(separator=" ", strip=True)
                street = clean_address(extract_address_from_text(text))

                # Try to find website link
                link = card.select_one("a[href^='http']:not([href*='" + parsed.netloc + "'])")
                website = link["href"] if link else ""

                all_records.append({
                    "name": name, "street": street, "city": city,
                    "state": "NC", "zip": "", "website": website,
                    "description": "", "source_url": start_url,
                })

        browser.close()

    # Deduplicate by name
    seen = set()
    deduped = []
    for r in all_records:
        key = r["name"].lower().strip()
        if key and key not in seen:
            seen.add(key)
            deduped.append(r)

    return deduped


# ── Paginated scrape ──────────────────────────────────────────────────────────

def scrape_all_pages(start_url):
    """
    Scrape all pages of a SimpleView site by:
    1. Loading the page with Playwright to get the session token
    2. Capturing the first API call URL
    3. Replaying it with increasing skip values to get all pages
    """
    parsed = urlparse(start_url)
    base = f"{parsed.scheme}://{parsed.netloc}"
    city = parsed.netloc.replace("www.", "").replace("visit", "").replace(".org", "").replace(".com", "").title()

    all_items = []
    seen_ids = set()
    first_api_url = None   # We'll capture and replay this
    total_count = None

    def parse_items(data):
        """
        Extract listing items from SimpleView's API response.
        Handles all known response structures:
          {"docs": {"count": N, "docs": [...]}}  — most common (v2)
          {"count": N, "docs": [...]}             — older SimpleView
          {"data": [...]}                         — some variants
          [...]                                   — bare list
        """
        nonlocal total_count

        if isinstance(data, list):
            return data

        if not isinstance(data, dict):
            return []

        # Try double-nested docs first (most common modern SimpleView)
        if "docs" in data:
            inner = data["docs"]
            if isinstance(inner, dict):
                if total_count is None:
                    total_count = inner.get("count") or data.get("count")
                    if total_count:
                        print(f"  Total listings reported by API: {total_count}")
                return inner.get("docs", inner.get("data", []))
            elif isinstance(inner, list):
                if total_count is None:
                    total_count = data.get("count")
                    if total_count:
                        print(f"  Total listings reported by API: {total_count}")
                return inner

        # Flat dict with count + docs/data at top level (older SimpleView)
        if total_count is None:
            total_count = data.get("count") or data.get("total")
            if total_count:
                print(f"  Total listings reported by API: {total_count}")

        return (
            data.get("docs") or
            data.get("data") or
            data.get("results") or
            data.get("listings") or
            []
        )

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = context.new_page()

        candidate_urls = []  # track all find URLs with item counts

        def handle_response(response):
            nonlocal first_api_url
            if "plugins_listings_listings/find" in response.url and response.status == 200:
                try:
                    data = response.json()
                    items = parse_items(data)
                    new_count = 0
                    for item in items:
                        if not isinstance(item, dict):
                            continue
                        uid = item.get("recid", item.get("id", item.get("title", "")))
                        if uid not in seen_ids:
                            seen_ids.add(uid)
                            all_items.append(item)
                            new_count += 1
                    print(f"  Page 1: +{new_count} items from API")
                    if new_count > 0:
                        candidate_urls.append((new_count, response.url))
                except Exception as e:
                    print(f"  Error: {e}")

        page.on("response", handle_response)
        print(f"Loading page 1: {start_url}")
        page.goto(start_url, wait_until="networkidle", timeout=30000)
        time.sleep(2)

        # Pick the best template URL — prefer one containing "skip", fallback to most items
        import urllib.parse as _up3
        if candidate_urls:
            # Sort by item count descending, prefer URLs containing skip
            skip_urls = [(c, u) for c, u in candidate_urls if "skip" in _up3.unquote(u).lower()]
            best_count, best_url = (skip_urls[0] if skip_urls else sorted(candidate_urls, reverse=True)[0])
            first_api_url = best_url
            print(f"  Using template URL ({best_count} items, contains skip: {bool(skip_urls)})")
            decoded = _up3.unquote(first_api_url)
            print(f"  Full decoded URL:")
            for _i in range(0, min(len(decoded), 800), 200):
                print(f"    {decoded[_i:_i+200]}")

        # Now replay the captured API URL with increasing skip values
        if first_api_url:
            import math, re as _re
            import urllib.parse as _up4

            # Detect actual limit from the URL so page count is correct
            decoded_for_limit = _up4.unquote(first_api_url)
            limit_match = _re.search(r'"limit"\s*:\s*(\d+)', decoded_for_limit)
            actual_limit = int(limit_match.group(1)) if limit_match else 24
            print(f"  Detected limit per page: {actual_limit}")

            # Bump limit to 24 for efficiency if it's smaller
            fetch_limit = max(actual_limit, 24)
            if fetch_limit != actual_limit:
                print(f"  Bumping limit from {actual_limit} to {fetch_limit} for efficiency")

            if total_count:
                total_pages = math.ceil(total_count / fetch_limit)
                print(f"  API reports {total_count} total — fetching {total_pages} pages...")
            else:
                total_pages = 100  # Safety cap
                print(f"  No total count — fetching until empty page (max {total_pages})...")

            print(f"  Template URL: {first_api_url[:120]}...")

            for page_num in range(2, total_pages + 1):
                skip = (page_num - 1) * fetch_limit

                # Helper to do both encoded and decoded replacement
                def replace_value(url, key_encoded, key_decoded, value):
                    # Try URL-encoded pattern first
                    encoded_key = key_encoded
                    if encoded_key in url:
                        return _re.sub(
                            r'(' + encoded_key + r')\d+',
                            lambda m, v=value: m.group(1) + str(v),
                            url
                        )
                    # Fall back to decoded pattern
                    return _re.sub(
                        r'(' + key_decoded + r'\s*:\s*)\d+',
                        lambda m, v=value: m.group(1) + str(v),
                        url
                    )

                paginated_url = replace_value(first_api_url, r'%22skip%22%3A', r'"skip"', skip)
                paginated_url = replace_value(paginated_url, r'%22limit%22%3A', r'"limit"', fetch_limit)

                print(f"  Fetching page {page_num} (skip={skip})...")
                # Verify the skip was actually substituted
                import urllib.parse as _up
                decoded_check = _up.unquote(paginated_url)
                if f'"skip":{skip}' in decoded_check or f'%3A{skip}' in paginated_url:
                    print(f"    Skip substitution: ✓")
                else:
                    print(f"    Skip substitution: ✗ — skip value not found in URL")
                    print(f"    URL snippet: ...{paginated_url[paginated_url.find('skip')-5:paginated_url.find('skip')+20] if 'skip' in paginated_url.lower() else 'skip not in URL'}...")

                try:
                    resp = page.evaluate("""
                        async (url) => {
                            const r = await fetch(url, {
                                headers: {
                                    'X-Requested-With': 'XMLHttpRequest',
                                    'Accept': 'application/json'
                                }
                            });
                            return await r.json();
                        }
                    """, paginated_url)
                except Exception as e:
                    print(f"    Fetch error: {e}")
                    break

                items = parse_items(resp)
                new_count = 0
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    uid = item.get("recid", item.get("id", item.get("title", "")))
                    if uid not in seen_ids:
                        seen_ids.add(uid)
                        all_items.append(item)
                        new_count += 1
                print(f"    +{new_count} new items (total: {len(all_items)})")

                if not items:
                    print("  Empty page — done")
                    break
                time.sleep(0.5)
        else:
            print("  No API URL captured — only page 1 data available")

        browser.close()

    print(f"\nTotal items collected: {len(all_items)}")

    # Convert to records
    records = []
    for item in all_items:
        if not isinstance(item, dict):
            continue
        addr = item.get("address1", item.get("address", ""))
        records.append({
            "name":        item.get("title", item.get("name", "")),
            "street":      addr,
            "city":        item.get("city", city),
            "state":       item.get("state", "NC"),
            "zip":         item.get("zip", ""),
            "phone":       item.get("phone", item.get("phoneNumber", "")),
            "website":     item.get("weburl", item.get("url", item.get("website", ""))),
            "description": item.get("description", item.get("teaser", "")),
            "source_url":  start_url,
        })

    return records


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    url = sys.argv[1] if len(sys.argv) > 1 else input("Enter URL to scrape: ").strip()

    print(f"\nScraping: {url}")
    print("Using Playwright (full JS rendering) — this may take 30-60 seconds\n")

    # Try the paginated version first (better for sites with multiple pages)
    records = scrape_all_pages(url)

    if not records:
        print("Paginated scrape returned nothing — trying single-page scrape...")
        records = scrape_simpleview(url)

    if not records:
        print("No records found. The site structure may have changed.")
        sys.exit(1)

    # Save output — filename includes domain + path slug for clarity
    parsed = urlparse(url)
    domain = parsed.netloc.replace("www.", "").replace(".", "_")
    path_slug = parsed.path.strip("/").replace("/", "_") or "listings"
    output_file = f"{domain}_{path_slug}_playwright.csv"

    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_FIELDS)
        writer.writeheader()
        writer.writerows(records)

    print(f"\n✅ Done — {len(records)} records saved to {output_file}")
    print(f"   With address: {sum(1 for r in records if r.get('street'))}")
    print(f"   With website: {sum(1 for r in records if r.get('website'))}")
