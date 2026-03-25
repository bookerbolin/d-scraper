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

# Full state name → 2-letter abbreviation (US + territories)
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

def normalize_state(raw):
    """Convert full state name or 2-letter abbreviation to uppercase abbreviation."""
    if not raw:
        return ""
    stripped = raw.strip()
    if re.match(r'^[A-Z]{2}$', stripped):
        return stripped
    return STATE_ABBR.get(stripped.lower(), stripped.upper() if len(stripped) == 2 else stripped)


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


def resolve_detail_page(detail_url, source_domain=None):
    """
    Generic detail page resolver. Fetches a listing detail page and extracts:
    - website (via "Visit Website" / "Official Website" link)
    - phone (via tel: link)
    - address (via Location/Address heading → siblings, or address regex)
    Works for exploregeorgia.org, homeofgolf.com, and similar CMS detail pages.
    """
    import requests as _req
    from bs4 import BeautifulSoup as _BS
    HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
    if source_domain is None:
        source_domain = urlparse(detail_url).netloc

    try:
        r = _req.get(detail_url, headers=HEADERS, timeout=12)
        r.raise_for_status()
        soup = _BS(r.text, "lxml")
        for tag in soup(["nav", "header", "footer"]):
            tag.decompose()

        result = {}

        # Website — "Visit Website" / "Official Website" / "Website" link to external domain
        for a in soup.find_all("a", href=True):
            href = a.get("href", "")
            text = a.get_text(strip=True).lower()
            if (href.startswith("http")
                    and source_domain not in href
                    and not any(s in href for s in ["facebook", "instagram", "twitter", "yelp", "tripadvisor"])
                    and any(w in text for w in ["website", "official", "visit site", "homepage"])):
                result["website"] = href
                break

        # Phone — tel: link
        tel = soup.find("a", href=re.compile(r"^tel:"))
        if tel:
            phone = extract_phone(tel["href"])
            if phone:
                result["phone"] = phone

        # Address — try Location/Address heading → sibling lines first
        addr_heading = soup.find(
            ["h2", "h3", "h4", "strong", "dt"],
            string=re.compile(r"^(location|address)$", re.I)
        )
        if addr_heading:
            lines = []
            for sib in addr_heading.find_next_siblings():
                if getattr(sib, "name", None) in ("h2", "h3", "h4"):
                    break
                text = sib.get_text(strip=True)
                if text:
                    lines.append(text)

            street_lines, city, state, zip_code = [], "", "", ""
            for line in lines:
                if re.match(r"^[A-Z]{2}$", line) or line.strip().lower() in STATE_ABBR:
                    state = normalize_state(line)
                elif re.match(r"^\d{5}$", line):
                    zip_code = line
                elif re.search(r"\d{5}", line):
                    # "City, ST 12345" or "City, North Carolina 12345" on one line
                    m = re.search(r"^(.*?),?\s*([A-Z]{2}|[A-Za-z][a-z]+(?: [A-Za-z][a-z]+)?)\s+(\d{5})", line)
                    if m:
                        if not city:
                            city = m.group(1).strip()
                        state = normalize_state(m.group(2).strip())
                        zip_code = m.group(3)
                    else:
                        street_lines.append(line)
                elif not state and not zip_code:
                    street_lines.append(line)

            if len(street_lines) > 1:
                if not city:
                    city = street_lines[-1]
                result["street"] = ", ".join(street_lines[:-1] if not city else street_lines)
            elif street_lines:
                if not city:
                    city = street_lines[0]
                else:
                    result["street"] = street_lines[0]

            if city:   result["city"]  = city
            if state:  result["state"] = state
            if zip_code: result["zip"] = zip_code

        # Fallback: regex scan full page text for address
        if not result.get("street"):
            page_text = soup.get_text(" ", strip=True)
            addr = extract_address_from_text(page_text)
            if addr:
                result["street"] = addr

        return result
    except Exception:
        return {}


# Keep old name as alias for backward compat



def scrape_drupal_views(start_url):
    """
    Scrape a Drupal Views AJAX pagination site (e.g. visitsavannah.com).
    Intercepts the first AJAX call to capture session tokens, then replays
    with page=0,1,2... until empty.
    """
    import json as _json
    from bs4 import BeautifulSoup as _BS
    from urllib.parse import urlparse as _up, urlencode as _ue, parse_qs as _pqs, urlunparse as _uu

    parsed = urlparse(start_url)
    base = f"{parsed.scheme}://{parsed.netloc}"

    first_ajax_url = None
    all_records = []

    print(f"Launching browser for Drupal Views: {start_url}")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = context.new_page()

        # Intercept the Drupal Views AJAX endpoint
        def handle_response(response):
            nonlocal first_ajax_url
            if "views/ajax" in response.url and "drupal_ajax" in response.url:
                if first_ajax_url is None:
                    first_ajax_url = response.url
                    print(f"  Captured Drupal Views AJAX URL")

        page.on("response", handle_response)

        print("Loading page...")
        page.goto(start_url, wait_until="domcontentloaded", timeout=30000)
        time.sleep(3)

        # Scroll to trigger the first AJAX load
        print("Scrolling to trigger AJAX...")
        for _ in range(5):
            page.evaluate("window.scrollBy(0, window.innerHeight)")
            time.sleep(1)

        # Try clicking "next" to capture the AJAX URL if not yet captured
        if not first_ajax_url:
            next_btn = page.query_selector("a[rel='next'], .pager__item--next a, li.next a")
            if next_btn:
                page.evaluate("btn => btn.click()", next_btn)
                time.sleep(2)

        browser.close()

    if not first_ajax_url:
        print("  Could not capture Drupal Views AJAX URL")
        return []

    # Parse the captured URL to extract base params
    from urllib.parse import urlparse as _urlparse, parse_qs as _parse_qs, urlencode as _urlencode
    parsed_ajax = _urlparse(first_ajax_url)
    params = _parse_qs(parsed_ajax.query, keep_blank_values=True)

    # Flatten params (parse_qs returns lists)
    flat_params = {k: v[0] for k, v in params.items()}

    print(f"  view_name: {flat_params.get('view_name', '?')} | display: {flat_params.get('view_display_id', '?')}")

    _HEADERS_BASE = {"User-Agent": "Mozilla/5.0 (compatible; research-bot/1.0)"}
    HEADERS_AJAX = {
        **_HEADERS_BASE,
        "X-Requested-With": "XMLHttpRequest",
        "Accept": "application/json",
        "Referer": start_url,
    }

    import requests as _req

    page_num = 0
    seen_names = set()

    while True:
        flat_params["page"] = str(page_num)
        ajax_url = f"{parsed_ajax.scheme}://{parsed_ajax.netloc}{parsed_ajax.path}?{_urlencode(flat_params)}"

        try:
            r = _req.get(ajax_url, headers=HEADERS_AJAX, timeout=20)
            r.raise_for_status()
            commands = r.json()
        except Exception as e:
            print(f"  Page {page_num} fetch error: {e}")
            break

        # Find the insert command with listing HTML
        html_content = ""
        for cmd in commands:
            if isinstance(cmd, dict) and cmd.get("command") == "insert" and "data" in cmd:
                html_content = cmd["data"]
                break

        if not html_content or len(html_content) < 100:
            print(f"  Page {page_num}: empty response — done")
            break

        soup = _BS(html_content, "lxml")

        # Parse listing cards from the injected HTML
        # visitsavannah uses article/profile cards with h3 name + address fields
        new_count = 0
        cards = soup.select("article, .profile-card, [class*='listing'], [class*='profile']")
        if not cards:
            # Fallback: any h3 with a link
            cards = [h3.find_parent() for h3 in soup.find_all("h3") if h3.find("a")]

        for card in (cards or []):
            if not card:
                continue
            name_el = card.select_one("h3 a, h2 a, .field-title a, [class*='title'] a")
            if not name_el:
                name_el = card.select_one("h3, h2")
            if not name_el:
                continue
            name = name_el.get_text(strip=True)
            if not name or name in seen_names:
                continue
            seen_names.add(name)
            new_count += 1

            detail_url = ""
            a = name_el if name_el.name == "a" else name_el.find("a")
            if a and a.get("href"):
                href = a["href"]
                detail_url = href if href.startswith("http") else base + href

            text = card.get_text(separator=" ", strip=True)
            street = clean_address(extract_address_from_text(text)) if extract_address_from_text(text) else ""
            phone = extract_phone(text)

            all_records.append({
                "name": name, "street": street, "city": "",
                "state": "", "zip": "", "phone": phone,
                "website": detail_url, "description": "",
                "source_url": start_url,
            })

        print(f"  Page {page_num}: +{new_count} new listings (total: {len(all_records)})")
        if new_count == 0:
            break
        page_num += 1
        time.sleep(0.5)

    return all_records



def scrape_simpleview(start_url):
    """
    Scrape a SimpleView CMS site by intercepting its XHR API calls
    as the page renders, rather than parsing the HTML.
    """
    parsed = urlparse(start_url)
    base = f"{parsed.scheme}://{parsed.netloc}"

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
        page.goto(start_url, wait_until="domcontentloaded", timeout=30000)
        time.sleep(3)  # allow JS to render after DOM load

        # Scroll to trigger lazy-loaded content
        print("Scrolling to load all listings...")
        for _ in range(5):
            page.evaluate("window.scrollBy(0, window.innerHeight)")
            time.sleep(1)

        # Click "load more" / "show more results" buttons until exhausted
        # Handles Algolia InfiniteHits and similar infinite-scroll patterns
        _load_more_clicks = 0
        while True:
            try:
                btn = page.query_selector(
                    "button.ais-InfiniteHits-loadMore:not([disabled]):not(.ais-InfiniteHits-loadMore--disabled),"
                    "button[class*='loadMore']:not([disabled]),"
                    "button[class*='load-more']:not([disabled]),"
                    "a[class*='load-more']"
                )
                if not btn or not btn.is_visible():
                    break
                btn.scroll_into_view_if_needed()
                btn.click()
                time.sleep(2)
                _load_more_clicks += 1
            except Exception:
                break
        if _load_more_clicks:
            print(f"  Clicked 'load more' {_load_more_clicks} times")

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
                        "city":        item.get("city", ""),
                        "state":       item.get("state", ""),
                        "zip":         item.get("zip", item.get("postal_code", "")),
                        "website":     item.get("weburl", item.get("url", item.get("website", ""))),
                        "description": item.get("description", item.get("teaser", "")),
                        "source_url":  start_url,
                    })

        else:
            # Fallback: parse the rendered DOM with pagination support
            print("No API responses intercepted — parsing rendered DOM...")

            from bs4 import BeautifulSoup

            def infer_cards(soup):
                """
                Infer listing cards by structure, not class name.
                A listing card is a repeated sibling element that contains
                at least a name-like heading AND one of: address, phone, or outbound link.
                We find the most common repeating container that meets these criteria.
                """
                import re as _re
                from collections import Counter

                PHONE_RE = _re.compile(r'\(?\d{3}\)?[\s.\-]\d{3}[\s.\-]\d{4}')
                ADDR_RE  = _re.compile(r'\d{1,5}\s+\w[\w\s\.]{2,30}'
                                       r'(?:St|Ave|Rd|Dr|Blvd|Ln|Way|Hwy|Pkwy|Ct|Pl|Cir|Street|Avenue|Road|Drive|Boulevard|Lane|Highway|Court|Place)\b',
                                       _re.I)

                def card_score(el):
                    """Score how much an element looks like a listing card (0-3)."""
                    txt = el.get_text(" ", strip=True)
                    score = 0
                    # Has a heading-like child
                    if el.find(["h2","h3","h4","h5"]) or el.find(
                            class_=_re.compile(r'title|heading|name', _re.I)):
                        score += 1
                    # Has address
                    if ADDR_RE.search(txt):
                        score += 1
                    # Has phone or outbound link
                    if PHONE_RE.search(txt) or el.find("a", href=_re.compile(r'^tel:')):
                        score += 1
                    return score

                # Collect all block-level elements that could be cards
                candidates = soup.find_all(
                    ["article", "li", "div", "section"],
                    recursive=True
                )

                # Filter: must have score >= 2, OR score >= 1 with a directory/listing detail link
                # The latter catches CVB cards that show name+link but no address/phone in the card
                DIR_LINK_RE = _re.compile(r'/(directory|listing|business|place)/', _re.I)
                good = []
                for el in candidates:
                    if el.find_parent(["nav","header","footer"]):
                        continue
                    txt = el.get_text(strip=True)
                    if len(txt) < 20 or len(txt) > 2000:
                        continue
                    sc = card_score(el)
                    has_dir_link = bool(el.find("a", href=DIR_LINK_RE))
                    if sc >= 2 or (sc >= 1 and has_dir_link):
                        good.append(el)

                if not good:
                    return []

                # Among good candidates, prefer the most repeated tag+class combo
                # (the repeating unit is the actual card, not its container)
                def sig(el):
                    cls = tuple(sorted(el.get("class", [])))
                    return (el.name, cls)

                counts = Counter(sig(el) for el in good)
                best_sig, best_count = counts.most_common(1)[0]

                # If the best signature appears >= 3 times, use it
                # Otherwise fall back to all good candidates
                if best_count >= 3:
                    cards = [el for el in good if sig(el) == best_sig]
                else:
                    cards = good

                # Deduplicate — remove any card that is an ancestor of another card
                card_set = set(id(c) for c in cards)
                cards = [c for c in cards
                         if not any(id(p) in card_set
                                    for p in c.parents)]
                return cards

            def parse_page_cards(html, parsed_netloc):
                soup = BeautifulSoup(html, "lxml")
                cards = infer_cards(soup)
                results = []
                for card in cards:
                    # Name: first heading or title-like element in the card
                    import re as _re
                    name_el = card.find(["h2","h3","h4","h5"]) or card.find(
                        class_=_re.compile(r'title|heading|name', _re.I))
                    name = name_el.get_text(strip=True) if name_el else ""
                    # Fallback: first short bold/strong text
                    if not name:
                        for el in card.find_all(["strong","b","p"]):
                            t = el.get_text(strip=True)
                            if 2 < len(t) < 80 and not _re.search(r'\d{3}.*\d{4}', t):
                                name = t
                                break
                    if not name:
                        continue

                    text = card.get_text(separator=" ", strip=True)

                    # First try semantic card fields (e.g. visitcharlottesville.org card__* classes)
                    import re as _re
                    street = city_val = state_val = zip_val = ""

                    def _norm_addr(t):
                        """Normalize whitespace including non-breaking spaces."""
                        return _re.sub(r'[\xa0\u2009\u202f]+', ' ', t).strip()

                    _SUITE_RE = _re.compile(
                        r'^(suite|ste\.?|unit|apt\.?|floor|fl\.?|bldg\.?|building|room|rm\.?|#|\d)',
                        _re.I
                    )

                    def _parse_addr(addr_text):
                        """Parse address into (street, city, state, zip). Returns all empty on failure."""
                        addr_text = _norm_addr(addr_text)
                        # Try 4-part: street, suite/extra, city, state zip
                        m = _re.search(
                            r'(\d+[^,\n]{2,60})[,\n]\s*([^,\n]{2,60})[,\n]\s*([A-Za-z][^,\n]{1,40})[,\n]\s*([A-Za-z ]{2,20})\s+(\d{5})\b',
                            addr_text
                        )
                        if m:
                            street_part = m.group(1).strip()
                            second_line = m.group(2).strip()
                            city_part   = m.group(3).strip()
                            state_part  = normalize_state(m.group(4).strip())
                            zip_part    = m.group(5).strip()
                            # If second line looks like a suite/unit, append to street
                            if _SUITE_RE.match(second_line):
                                street_part = f"{street_part}, {second_line}"
                            else:
                                city_part = second_line  # second line is actually city
                            return street_part, city_part, state_part, zip_part
                        # Try 3-part: street, city, state zip
                        m2 = _re.search(
                            r'(\d+[^,\n]{2,60})[,\n]\s*([A-Za-z][^,\n]{1,40})[,\n]\s*([A-Za-z ]{2,20})\s+(\d{5})\b',
                            addr_text
                        )
                        if m2:
                            city_part = m2.group(2).strip()
                            if _SUITE_RE.match(city_part):
                                # Suite leaked into city slot — keep street only
                                return m2.group(1).strip(), "", normalize_state(m2.group(3).strip()), m2.group(4).strip()
                            return m2.group(1).strip(), city_part, normalize_state(m2.group(3).strip()), m2.group(4).strip()
                        return "", "", "", ""

                    # Try semantic address element first
                    addr_el = card.select_one(".card__address, [class*='address']")
                    if addr_el:
                        addr_text = addr_el.get_text(separator=" ", strip=True)
                        street, city_val, state_val, zip_val = _parse_addr(addr_text)
                        if not street and addr_text:
                            street = clean_address(extract_address_from_text(_norm_addr(addr_text))) or ""

                    # Also check data-* attributes on card or its children for city/zip
                    if not city_val:
                        _data_src = card.find(attrs={"data-city": True}) or card
                        if _data_src.get("data-city"):
                            city_val = _data_src["data-city"]
                    if not zip_val:
                        _data_src = card.find(attrs={"data-zipcode": True}) or card
                        if _data_src.get("data-zipcode"):
                            zip_val = _data_src["data-zipcode"]

                    # Fall back to full card text
                    if not street:
                        street, _cv, _sv, _zv = _parse_addr(text)
                        if not city_val: city_val = _cv
                        if not state_val: state_val = _sv
                        if not zip_val: zip_val = _zv
                        if not street:
                            street = clean_address(extract_address_from_text(_norm_addr(text)))

                    # Capture city-only locations when no street found
                    card_city = city_val
                    if not street and not card_city:
                        for line in text.split("  "):
                            line = line.strip()
                            if (line and not _re.search(r'\d', line)
                                    and 2 <= len(line.split()) <= 5
                                    and len(line) < 40
                                    and line.lower() not in ["visit website", "day spas",
                                        "hotels & motels", "spas", "wellness", "resort",
                                        "learn more", "website"]):
                                card_city = line
                                break

                    # Prefer card__website link, then any external link, then internal detail link
                    website = ""
                    website_el = card.select_one(".card__website a[href], [class*='website'] a[href]")
                    if website_el:
                        website = website_el["href"]
                    detail_url = ""
                    if not website:
                        for _a in card.find_all("a", href=True):
                            _href = _a["href"]
                            _nl = urlparse(_href).netloc
                            _ext = (_nl and _nl != parsed_netloc
                                    and not any(s in _nl for s in
                                                ["facebook","instagram","twitter","google","yelp"]))
                            _int = (not _nl or _nl == parsed_netloc)
                            if _ext:
                                website = _href
                                break
                            elif _int and _href not in ("/", "#", "") and not detail_url:
                                detail_url = _href if _href.startswith("http") else f"https://{parsed_netloc}{_href}"
                                if not website:
                                    website = detail_url
                    else:
                        # External website found — also capture internal detail link separately
                        for _a in card.find_all("a", href=True):
                            _href = _a["href"]
                            _nl = urlparse(_href).netloc
                            if (not _nl or _nl == parsed_netloc) and _href not in ("/", "#", ""):
                                detail_url = _href if _href.startswith("http") else f"https://{parsed_netloc}{_href}"
                                break

                    phone_el = card.select_one("a[href^='tel:']")
                    phone = extract_phone(phone_el["href"]) if phone_el else extract_phone(text)

                    rec = {
                        "name": name, "street": street, "city": city_val,
                        "state": state_val, "zip": zip_val,
                        "card_city": card_city,
                        "website": website, "phone": phone,
                    }
                    if detail_url and detail_url != website:
                        rec["_detail_url"] = detail_url
                    results.append(rec)
                return results, soup

            # Parse first page
            html = page.content()
            page_cards, soup = parse_page_cards(html, parsed.netloc)
            print(f"Found {len(page_cards)} potential listing cards on page 1")
            all_dom_cards = page_cards

            # Dismiss any popup/modal overlays before paginating
            for selector in [
                ".block-popupblock-modal", ".spb_overlay", "[class*='popup']",
                "[class*='modal']", "[class*='overlay']", ".close", "button[aria-label='Close']",
            ]:
                try:
                    el = page.query_selector(selector)
                    if el and el.is_visible():
                        el.click(timeout=2000)
                        time.sleep(0.5)
                        break
                except Exception:
                    pass
            # Also try pressing Escape to dismiss modals
            try:
                page.keyboard.press("Escape")
                time.sleep(0.5)
            except Exception:
                pass

            # Detect pagination style from soup — language-agnostic approach:
            # 1. Look for any "X of Y" or "X-Y of Z" counter pattern regardless of label words
            # 2. Fall back to detecting ?page=N links in the soup
            # 3. If neither, just try page 2 and see if new cards appear (probe approach)
            import re as _re_pg
            use_url_pagination = False
            _total_listings = None
            _total_pages = 50
            _page_size = len(page_cards) or 12

            # Strategy 1: find any counter element with "X of Y" or "X-Y of Z" pattern
            # Matches: "1-12 of 298", "Showing 1 to 12 of 298", "Results 1-12 of 298", etc.
            _COUNT_RE = _re_pg.compile(r'\b(\d+)\s*(?:-|to)\s*(\d+)\s+of\s+(\d+)\b')
            for _el in soup.find_all(["p","div","span","li","small"]):
                if _el.find(["p","div","span"]):  # skip containers
                    continue
                _m = _COUNT_RE.search(_el.get_text())
                if _m:
                    _start = int(_m.group(1))
                    _end   = int(_m.group(2))
                    _total = int(_m.group(3))
                    if _total > _end > _start >= 1:  # sanity check
                        _page_size = _end - _start + 1
                        _total_listings = _total
                        _total_pages = -(-_total // _page_size)
                        print(f"  Detected {_total} total listings, {_page_size}/page → {_total_pages} pages")
                        use_url_pagination = _total_pages > 1
                        break

            # Strategy 2: ?page=N links in soup
            if not use_url_pagination:
                _url_page_links = soup.find_all("a", href=_re_pg.compile(r'[?&]page=\d+'))
                if _url_page_links:
                    use_url_pagination = True
                    _total_pages = 50
                    print(f"  Detected ?page=N pagination ({len(_url_page_links)} links)")

            # Strategy 3: probe page 2 — if it returns new cards, paginate
            if not use_url_pagination and page_cards:
                _probe_url2 = start_url.rstrip("/") + "?page=2" if "?" not in start_url else start_url + "&page=2"
                try:
                    page.goto(_probe_url2, wait_until="domcontentloaded", timeout=10000)
                    time.sleep(1.5)
                    _probe_cards, _ = parse_page_cards(page.content(), parsed.netloc)
                    _probe_new = [c for c in _probe_cards if c["name"] not in {x["name"] for x in page_cards}]
                    if _probe_new:
                        use_url_pagination = True
                        _total_pages = 50
                        print(f"  Detected pagination via page 2 probe ({len(_probe_new)} new cards)")
                        # Go back to page 1
                        page.goto(start_url, wait_until="domcontentloaded", timeout=15000)
                        time.sleep(1.5)
                except Exception:
                    pass

            if use_url_pagination:
                print(f"  Using URL-based ?page=N pagination")

            # Try to paginate
            page_num = 1
            seen_names = {c["name"] for c in all_dom_cards}
            max_pages = _total_pages if use_url_pagination else 50
            while page_num < max_pages:
                # Stop if we've collected enough
                if _total_listings and len(seen_names) >= _total_listings:
                    break
                page_num += 1
                if use_url_pagination:
                    # Navigate directly to ?page=N
                    from urllib.parse import urlparse as _ulp2, urlencode as _ue2, parse_qs as _pqs2, urlunparse as _uu2
                    _pp = _ulp2(start_url)
                    _params2 = _pqs2(_pp.query, keep_blank_values=True)
                    _params2.pop("page", None)
                    _params2["page"] = [str(page_num)]
                    _next_url = _uu2(_pp._replace(query=_ue2(_params2, doseq=True)))
                    print(f"  Fetching page {page_num}: {_next_url}")
                    page.goto(_next_url, wait_until="domcontentloaded", timeout=15000)
                    time.sleep(2)
                else:
                    next_btn = page.query_selector("a[rel='next'], .pager__item--next a, [class*='next'] a, a:has-text('Next'), button:has-text('Next')")
                    if not next_btn:
                        break
                    print(f"  Clicking to page {page_num}...")
                    page.evaluate("btn => btn.scrollIntoView({block: 'center'})", next_btn)
                    time.sleep(0.3)
                    page.evaluate("btn => btn.click()", next_btn)
                    page.wait_for_load_state("domcontentloaded", timeout=15000)
                    time.sleep(2)
                html = page.content()
                page_cards, _ = parse_page_cards(html, parsed.netloc)
                if not page_cards:
                    break
                # Detect infinite loop — stop if no new names appeared
                new_cards = [c for c in page_cards if c["name"] not in seen_names]
                if not new_cards:
                    print(f"  Page {page_num} has no new listings — stopping")
                    break
                seen_names.update(c["name"] for c in new_cards)
                print(f"  +{len(new_cards)} new cards on page {page_num}")
                all_dom_cards.extend(new_cards)

            print(f"Total DOM cards across all pages: {len(all_dom_cards)}")

            for card_data in all_dom_cards:
                name = card_data["name"]
                street = card_data["street"]
                card_city  = card_data.get("city", "") or card_data.get("card_city", "")
                card_state = card_data.get("state", "")
                card_zip   = card_data.get("zip", "")
                phone = card_data.get("phone", "")
                website = card_data.get("website", "")

                all_records.append({
                    "name": name, "street": street,
                    "city": card_city, "state": card_state, "zip": card_zip,
                    "phone": phone, "website": website,
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
        captured_urls = set()  # avoid duplicates
        import json as _json

        # Use response event handler — simpler than route interception
        # We read URL only (not body) to avoid race conditions with browser close
        def handle_response(response):
            url = response.url
            if ("plugins_listings_listings/find" in url
                    and response.status == 200
                    and url not in captured_urls):
                captured_urls.add(url)
                # Decode and check if this looks like a real directory call
                import urllib.parse as _upd
                decoded = _upd.unquote(url)
                has_skip = '"skip"' in decoded
                has_count_true = '"count":true' in decoded
                # Accept any listings/find call — sites use different filter structures
                # ($and filters, filter_tags, etc.)
                score = (2 if has_count_true else 0) + (1 if has_skip else 0)
                candidate_urls.append((score, url))

        page.on("response", handle_response)

        print(f"Loading page 1: {start_url}")
        page.goto(start_url, wait_until="domcontentloaded", timeout=30000)

        # Wait for listing cards — signals all API calls have fired
        try:
            page.wait_for_selector(
                "[class*='listing'], [class*='result'], article, .sv-listing",
                timeout=10000
            )
            print("  Listings detected in DOM — all API calls should be complete")
        except Exception:
            print("  No listing selector matched — waiting 8s for API calls...")
        # Scroll down to trigger lazy-loaded directory grid API calls
        for _ in range(6):
            page.evaluate("window.scrollBy(0, window.innerHeight)")
            time.sleep(1.5)

        # Wait for the count=true URL to appear (it fires when the grid renders)
        deadline = time.time() + 10
        while time.time() < deadline:
            if any('"count":true' in __import__('urllib.parse', fromlist=['unquote']).unquote(u)
                   for _, u in candidate_urls):
                print("  count=true URL captured — ready to paginate")
                break
            time.sleep(0.5)

        # Pick the best template URL — prefer one containing "skip", fallback to most items
        import urllib.parse as _up3
        if candidate_urls:
            # Pick highest-scored URL (score: count=true=2, skip=1)
            best_score, first_api_url = sorted(candidate_urls, reverse=True)[0]
            print(f"  Using template URL (score={best_score})")

        # Fetch page 1 and all subsequent pages via requests (browser already closed)
        if first_api_url:
            import math, re as _re, requests as _req
            import urllib.parse as _up4

            api_headers = {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
                "Referer": start_url,
                "Accept": "application/json",
                "X-Requested-With": "XMLHttpRequest",
            }

            def build_paginated_url(template_url, skip, limit):
                """
                Robustly build a paginated URL by decode → parse JSON → modify → re-encode.
                Falls back to regex substitution if JSON parsing fails.
                """
                from urllib.parse import urlparse as _ulp, parse_qs as _pqs, urlencode as _ue, unquote as _uq, quote as _uqq
                parsed_u = _ulp(template_url)
                params = _pqs(parsed_u.query, keep_blank_values=True)
                modified = False
                for param_key, values in params.items():
                    for i, val in enumerate(values):
                        decoded_val = _uq(val)
                        try:
                            obj = _json.loads(decoded_val)
                            changed = False
                            if "skip" in obj:
                                obj["skip"] = skip
                                changed = True
                            if "limit" in obj:
                                obj["limit"] = limit
                                changed = True
                            if changed:
                                params[param_key][i] = _uqq(_json.dumps(obj, separators=(",", ":")))
                                modified = True
                        except (ValueError, TypeError):
                            pass
                if modified:
                    new_query = _ue(params, doseq=True)
                    return parsed_u._replace(query=new_query).geturl()
                # Fallback: regex substitution on raw URL
                result = _re.sub(r'(%22skip%22%3A)\d+', lambda m: m.group(1) + str(skip), template_url)
                result = _re.sub(r'(%22limit%22%3A)\d+', lambda m: m.group(1) + str(limit), result)
                return result

            # Detect limit from the URL
            decoded_for_limit = _up4.unquote(first_api_url)
            limit_match = _re.search(r'"limit"\s*:\s*(\d+)', decoded_for_limit)
            actual_limit = int(limit_match.group(1)) if limit_match else 24
            fetch_limit = max(actual_limit, 24)
            print(f"  Detected limit: {actual_limit}, using: {fetch_limit}")

            # Fetch page 1 with skip=0 and bumped limit
            page1_url = build_paginated_url(first_api_url, skip=0, limit=fetch_limit)
            try:
                r1 = _req.get(page1_url, headers=api_headers, timeout=20)
                data1 = r1.json()
                # Re-read total_count from this response if not already set
                if not total_count:
                    docs = data1.get("docs", data1)
                    if isinstance(docs, dict):
                        total_count = docs.get("count", 0)
                items1 = parse_items(data1)
                for item in items1:
                    if not isinstance(item, dict):
                        continue
                    uid = item.get("recid", item.get("id", item.get("title", "")))
                    if uid not in seen_ids:
                        seen_ids.add(uid)
                        all_items.append(item)
                print(f"  Page 1 (skip=0, limit={fetch_limit}): +{len(items1)} items")
            except Exception as e:
                print(f"  Page 1 fetch error: {e}")

            if total_count:
                total_pages = math.ceil(total_count / fetch_limit)
                print(f"  API reports {total_count} total — fetching {total_pages} pages...")
            else:
                total_pages = 100
                print(f"  No total count — fetching until empty (max {total_pages})...")

            print(f"  Template URL: {first_api_url[:120]}...")

            for page_num in range(2, total_pages + 1):
                skip = (page_num - 1) * fetch_limit
                paginated_url = build_paginated_url(first_api_url, skip=skip, limit=fetch_limit)
                print(f"  Fetching page {page_num} (skip={skip})...")

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
    _sv_base = f"{urlparse(start_url).scheme}://{urlparse(start_url).netloc}"
    records = []
    for item in all_items:
        if not isinstance(item, dict):
            continue
        addr = item.get("address1", item.get("address", ""))
        name = item.get("title", item.get("name", ""))
        # Use the url field directly — SimpleView returns /listing/slug/recid/
        # Fall back to constructing from recid + name slug if url not present
        sv_url = item.get("url", "")
        if sv_url and not sv_url.startswith("http"):
            detail_url = f"{_sv_base}{sv_url}"
        else:
            recid = item.get("recid", item.get("id", ""))
            if recid and name:
                slug = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")
                detail_url = f"{_sv_base}/listing/{slug}/{recid}/"
            else:
                detail_url = ""
        # weburl is the external business website; url is the CVB detail page
        ext_website = item.get("weburl", item.get("website", ""))
        records.append({
            "name":        name,
            "street":      addr,
            "city":        item.get("city", ""),
            "state":       item.get("state", ""),
            "zip":         item.get("zip", ""),
            "phone":       item.get("phone", item.get("phoneNumber", "")),
            "website":     ext_website,
            "description": item.get("description", item.get("teaser", "")),
            "source_url":  start_url,
            "_detail_url": detail_url,
        })

    return records


# ── Algolia scraper ───────────────────────────────────────────────────────────

def parse_algolia_address(addr_list):
    """Parse Algolia address list ['Street City', 'ST NNNNN'] into components."""
    if not addr_list:
        return "", "", "", ""

    raw0 = str(addr_list[0]).strip() if addr_list[0] else ""
    city, state, zip_code = "", "", ""

    # Second element: extract state + zip, strip unit/suite noise before city
    if len(addr_list) > 1 and addr_list[1]:
        second = str(addr_list[1]).strip()
        m = re.search(r'\b([A-Z]{2}|[A-Za-z][a-z]+(?: [A-Za-z][a-z]+)?)\s+(\d{5})\b', second)
        if m:
            state, zip_code = normalize_state(m.group(1)), m.group(2)
            city_part = second[:m.start()].strip().rstrip(',').strip()
            city_part = re.sub(r'^(?:unit|suite|ste|apt|#)\s*\S+\s*', '', city_part, flags=re.I).strip()
            if city_part and not re.match(r'^\d', city_part):
                city = city_part

    # Parse street
    if ',' in raw0:
        parts = raw0.split(',', 1)
        street = parts[0].strip()
        if not city:
            candidate = parts[1].strip()
            candidate = re.sub(r'^(?:unit|suite|ste|apt|#)\s*\S+\s*', '', candidate, flags=re.I).strip()
            if candidate:
                city = candidate
    else:
        street_type = r'\b(Street|St|Avenue|Ave|Boulevard|Blvd|Road|Rd|Drive|Dr|Lane|Ln|Way|Highway|Hwy|Parkway|Pkwy|Court|Ct|Place|Pl|Circle|Cir|Trail|Pike|Run|Broad)\b'
        matches = list(re.finditer(street_type, raw0, re.I))
        if matches:
            last = matches[-1]
            after = raw0[last.end():].strip().rstrip('.,')
            num_suffix = re.match(r'^[\d\-]+\s*', after)
            city_candidate = after[num_suffix.end():].strip() if num_suffix else after
            if city_candidate and not city:
                city = city_candidate
                street = raw0[:raw0.rfind(city_candidate)].strip().rstrip(',').strip()
            else:
                street = raw0
        else:
            street = raw0

    return street, city, state, zip_code


def detect_algolia(url):
    """Fetch page HTML and extract Algolia config from data-info attribute."""
    import requests as _req
    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
    try:
        r = _req.get(url, headers=headers, timeout=15)
        from bs4 import BeautifulSoup as _BS
        soup = _BS(r.text, "lxml")
        block = soup.find(attrs={"data-info": True, "data-config": True})
        if not block:
            return None
        info = json.loads(block["data-info"])
        config = json.loads(block["data-config"])
        opts = json.loads(block.get("data-options", "{}"))
        if not info.get("appId") or not info.get("apiKey") or not info.get("index"):
            return None
        return {
            "appId":    info["appId"],
            "apiKey":   info["apiKey"],
            "index":    info["index"],
            "filters":  config.get("filters", ""),
            "pageSize": int(opts.get("listingsDisplayNumber", 48)),
        }
    except Exception as e:
        print(f"  Algolia detection error: {e}")
        return None


def scrape_algolia(url):
    """Scrape listings from an Algolia-powered site using the public search API."""
    import requests as _req

    print(f"  Detecting Algolia config...")
    cfg = detect_algolia(url)
    if not cfg:
        # Check if page uses Algolia InstantSearch widgets even without detectable config
        try:
            import requests as _rq
            _r = _rq.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=8)
            if "ais-InfiniteHits" in _r.text or "ais-Hits" in _r.text:
                print("  Algolia InstantSearch widgets detected (config embedded in JS — falling through to DOM scraper)")
            else:
                print("  No Algolia config found.")
        except Exception:
            print("  No Algolia config found.")
        return []

    print(f"  ✓ Algolia index: {cfg['index']}")
    api_url = f"https://{cfg['appId']}-dsn.algolia.net/1/indexes/{cfg['index']}/query"
    headers = {
        "X-Algolia-Application-Id": cfg["appId"],
        "X-Algolia-API-Key":        cfg["apiKey"],
    }

    all_records = []
    page = 0
    while True:
        try:
            r = _req.post(api_url, headers=headers, json={
                "filters":    cfg["filters"],
                "hitsPerPage": cfg["pageSize"],
                "page":        page,
            }, timeout=15)
            data = r.json()
        except Exception as e:
            print(f"  Algolia fetch error (page {page}): {e}")
            break

        hits = data.get("hits", [])
        nb_pages = data.get("nbPages", 1)
        nb_hits = data.get("nbHits", "?")
        if not hits:
            break

        for h in hits:
            addr_list = h.get("address") or []
            if isinstance(addr_list, str):
                addr_list = [addr_list]
            street, city, state, zip_code = parse_algolia_address(addr_list)

            phone_raw = h.get("phone", "") or ""
            # Normalize phone
            digits = re.sub(r'\D', '', phone_raw)
            if len(digits) == 11 and digits[0] == '1':
                digits = digits[1:]
            phone = f"({digits[:3]}) {digits[3:6]}-{digits[6:]}" if len(digits) == 10 else phone_raw

            all_records.append({
                "name":        h.get("title", ""),
                "street":      street,
                "city":        city,
                "state":       state,
                "zip":         zip_code,
                "phone":       phone,
                "website":     h.get("website", h.get("webUrl", "")),
                "description": h.get("content", h.get("snippet", "")),
                "source_url":  url,
            })

        print(f"  Page {page}: +{len(hits)} (total {len(all_records)} / {nb_hits})")
        if page >= nb_pages - 1:
            break
        page += 1

    return all_records


# ── Main ──────────────────────────────────────────────────────────────────────

def _extract_best_description(soup, record=None):
    """
    Extract the best business description from a detail page without relying
    on specific label words. Scores all text blocks and returns the one that
    looks most like a business description.
    """
    import re as _re2

    LABEL_WORDS = _re2.compile(
        r"\b(about|description|overview|details|info|information|story|"
        r"summary|profile|who we are|our story|mission)\b", _re2.I
    )
    NOISE_START = _re2.compile(
        r"^(mon|tue|wed|thu|fri|sat|sun|open|closed|hours|phone|fax|email|"
        r"address|directions|map|parking|admission|price|cost|\$|\u00a9|privacy|"
        r"terms|cookie|follow us|share|tweet|like|subscribe|sign up|newsletter|"
        r"powered by|all rights reserved)", _re2.I
    )
    DATA_HEAVY = _re2.compile(r"(\d{5}|\(\d{3}\)|\d{1,2}:\d{2}\s*(am|pm))", _re2.I)

    name = (record or {}).get("name", "")

    def score_block(txt):
        if len(txt) < 60 or len(txt) > 800:
            return 0
        if NOISE_START.match(txt):
            return 0
        if DATA_HEAVY.search(txt[:40]):
            return 0
        s = 0
        if txt[0].isupper():
            s += 1
        if _re2.search(r"[.!?]\s", txt) or txt[-1] in ".!?":
            s += 1
        if " " in txt[10:40]:
            s += 1
        if 80 < len(txt) < 500:
            s += 2
        if name and txt.strip().lower() == name.strip().lower():
            return 0
        return s

    candidates = []
    for tag in soup.find_all(["p", "dd", "div", "span", "li", "section"]):
        if tag.find(["p", "dd", "div", "section"]):
            continue
        txt = _re2.sub(r"\s+", " ", tag.get_text(separator=" ", strip=True)).strip()
        s = score_block(txt)
        if s > 0:
            prev = tag.find_previous(["h2","h3","h4","button","dt","strong","b"])
            if prev and LABEL_WORDS.search(prev.get_text(strip=True)):
                s += 3
            if tag.find_parent(class_=_re2.compile(r"active|open|expanded", _re2.I)):
                s += 2
            candidates.append((s, txt))

    if not candidates:
        return ""
    candidates.sort(key=lambda x: (x[0], len(x[1])), reverse=True)
    return candidates[0][1][:400]

def resolve_csv_with_playwright(csv_path, detail_url_field="website", source_domain=None,
                                phone_selector="a[href^='tel:']",
                                about_heading=re.compile(r'^about', re.I)):
    """
    Re-open a CSV and fill in missing phone/description by fetching each
    detail page with Playwright (handles JS-rendered detail pages like SimpleView).

    Usage:
        python playwright_scraper.py --resolve path/to/file.csv

    Reads: csv_path
    Writes: csv_path (in-place, overwrites)
    Only fetches pages where phone OR description is missing.
    """
    import os as _os

    with open(csv_path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
        fieldnames = list(rows[0].keys()) if rows else OUTPUT_FIELDS

    needs_resolve = [i for i, r in enumerate(rows)
                     if (r.get("_detail_url") or r.get(detail_url_field))
                     and (not r.get("phone") or not r.get("description"))]

    if not needs_resolve:
        print(f"Nothing to resolve — all {len(rows)} records have phone and description.")
        return

    print(f"Resolving {len(needs_resolve)} records via Playwright...")
    print(f"(Fetching JS-rendered detail pages for phone + description)\n")

    filled_phone = 0
    filled_desc  = 0

    CONCURRENCY = 8  # parallel browser pages

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(user_agent=(
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ))

        # Create a pool of pages
        pages = [context.new_page() for _ in range(CONCURRENCY)]
        page_available = list(range(CONCURRENCY))  # indices of free pages
        import threading
        lock = threading.Lock()
        filled_phone_list = [0]
        filled_desc_list  = [0]
        done_count = [0]

        def resolve_one(slot, i):
            record = rows[i]
            detail_url = record.get("_detail_url", "") or record.get(detail_url_field, "")
            if not detail_url:
                return
            pg = pages[slot]
            try:
                pg.goto(detail_url, wait_until="domcontentloaded", timeout=20000)
                time.sleep(1.0)
                from bs4 import BeautifulSoup as _BS
                soup = _BS(pg.content(), "lxml")
                for tag in soup(["nav", "header", "footer"]):
                    tag.decompose()

                if not record.get("phone"):
                    tel = soup.find("a", href=re.compile(r'^tel:'))
                    if tel:
                        phone = extract_phone(tel["href"])
                        if phone:
                            record["phone"] = phone
                            with lock:
                                filled_phone_list[0] += 1

                if not record.get("description"):
                    desc = _extract_best_description(soup, record)
                    if desc:
                        record["description"] = desc
                        with lock:
                            filled_desc_list[0] += 1
            except Exception:
                pass
            finally:
                with lock:
                    done_count[0] += 1
                    page_available.append(slot)

        from concurrent.futures import ThreadPoolExecutor, as_completed
        futures = []
        slot_idx = 0
        with ThreadPoolExecutor(max_workers=CONCURRENCY) as executor:
            for i in needs_resolve:
                # Wait for a free slot
                while True:
                    with lock:
                        if page_available:
                            slot = page_available.pop(0)
                            break
                    time.sleep(0.1)
                fut = executor.submit(resolve_one, slot, i)
                futures.append(fut)

            # Progress reporting while futures complete
            reported = 0
            for fut in as_completed(futures):
                with lock:
                    done = done_count[0]
                if done - reported >= 20 or done == len(needs_resolve):
                    print(f"  {done}/{len(needs_resolve)} — "
                          f"phone: +{filled_phone_list[0]}  desc: +{filled_desc_list[0]}")
                    reported = done

        filled_phone = filled_phone_list[0]
        filled_desc  = filled_desc_list[0]
        browser.close()

    # Boilerplate dedup
    from collections import Counter as _Counter
    desc_counts = _Counter(r.get("description","")[:120] for r in rows if r.get("description"))
    boilerplate = {d for d, n in desc_counts.items() if n >= 3}
    if boilerplate:
        cleared = sum(1 for r in rows if r.get("description","")[:120] in boilerplate)
        for r in rows:
            if r.get("description","")[:120] in boilerplate:
                r["description"] = ""
        print(f"  Cleared {cleared} boilerplate descriptions")

    # Write back — strip private fields, only write OUTPUT_FIELDS
    _private = {"_detail_url", "_resolve_error"}
    for r in rows:
        for k in _private:
            r.pop(k, None)
    out_fields = [f for f in fieldnames if f not in _private]
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=out_fields)
        writer.writeheader()
        writer.writerows(rows)

    print(f"\n✅ Done — wrote {len(rows)} records back to {csv_path}")
    print(f"   Phone filled:       {filled_phone}")
    print(f"   Description filled: {filled_desc}")
    print(f"   With phone now:     {sum(1 for r in rows if r.get('phone'))}")
    print(f"   With description:   {sum(1 for r in rows if r.get('description'))}")


if __name__ == "__main__":
    # --resolve mode: fill in phone/description from JS-rendered detail pages
    if len(sys.argv) > 1 and sys.argv[1] == "--resolve":
        if len(sys.argv) < 3:
            print("Usage: playwright_scraper.py --resolve path/to/file.csv")
            sys.exit(1)
        resolve_csv_with_playwright(sys.argv[2])
        sys.exit(0)

    url = sys.argv[1] if len(sys.argv) > 1 else input("Enter URL to scrape: ").strip()

    print(f"\nScraping: {url}")
    print("Using Playwright (full JS rendering) — this may take 30-60 seconds\n")

    def is_drupal_views_site(url):
        """Detect Drupal Views AJAX sites from page HTML — no hardcoded list needed."""
        import requests as _req
        try:
            r = _req.get(url, headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}, timeout=10)
            html = r.text.lower()
            return (
                "drupal" in html
                and ("views/ajax" in html or "view-content" in html or "views-row" in html)
            )
        except Exception:
            return False

    # Try Algolia first (no browser needed, fastest)
    print("Checking for Algolia-powered listings...")
    records = scrape_algolia(url)

    if not records:
        # Try SimpleView paginated API
        records = scrape_all_pages(url)

    if not records:
        print("Paginated scrape returned nothing — trying single-page DOM scrape...")
        records = scrape_simpleview(url)

    # Last resort: Drupal Views AJAX (auto-detected or forced)
    if not records:
        print("Checking for Drupal Views AJAX...")
        if is_drupal_views_site(url):
            print("  Drupal Views detected — using AJAX scraper...")
            records = scrape_drupal_views(url)
        else:
            print("  Not a Drupal Views site.")

    if not records:
        print("No records found. The site structure may have changed.")
        sys.exit(1)

    # Generic resolution pass — only runs when records have a _detail_url to fetch
    # and are missing street, phone, or description
    missing_addr = [i for i, r in enumerate(records)
                    if r.get("_detail_url") and (
                        not r.get("street")
                        or not r.get("phone")
                        or not r.get("description")
                    )]
    if missing_addr:
        print(f"\nResolution pass — {len(missing_addr)} records missing phone/address/description...")

        # Detect if detail pages are JS-rendered by probing the first _detail_url
        # Must use _detail_url specifically — external business websites won't be JS-rendered
        import requests as _rq2
        _probe_url = ""
        for _r in (records[i] for i in missing_addr):
            if _r.get("_detail_url"):
                _probe_url = _r["_detail_url"]
                break
        _js_rendered = False
        if _probe_url:
            try:
                from bs4 import BeautifulSoup as _BS2
                _probe = _rq2.get(_probe_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=8)
                _probe_text = _BS2(_probe.text, "lxml").get_text(strip=True)
                _js_rendered = len(_probe_text) < 500 or "browser is not supported" in _probe_text
                print(f"  Probed {_probe_url[:60]}... JS-rendered: {_js_rendered}")
            except Exception as _pe:
                print(f"  Probe failed: {_pe}")

        if _js_rendered:
            print(f"  Detail pages are JS-rendered — using Playwright for resolution...")
            try:
                import tempfile, os as _os2
                _tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False,
                                                   newline="", encoding="utf-8")
                _tmp_fields = OUTPUT_FIELDS + ["_detail_url"]
                _writer = csv.DictWriter(_tmp, fieldnames=_tmp_fields, extrasaction="ignore")
                _writer.writeheader()
                _writer.writerows(records)
                _tmp.close()
                print(f"  Temp CSV written: {_tmp.name} ({len(records)} rows)")
                resolve_csv_with_playwright(_tmp.name)
                with open(_tmp.name, newline="", encoding="utf-8") as _f2:
                    _resolved = list(csv.DictReader(_f2))
                _os2.unlink(_tmp.name)
                _merge_count = 0
                for i, r in enumerate(_resolved):
                    if i < len(records):
                        for field in ("phone", "description", "street", "city", "state", "zip", "website"):
                            if r.get(field) and not records[i].get(field):
                                records[i][field] = r[field]
                                _merge_count += 1
                print(f"  Merged {_merge_count} field updates back into records")
            except Exception as _e:
                print(f"  ERROR in Playwright resolution: {_e}")
        else:
            from concurrent.futures import ThreadPoolExecutor, as_completed

            def resolve_detail(record):
                detail_url = record.get("website", "")
                if not detail_url:
                    return record
                details = resolve_detail_page(detail_url)
                for field in ("street", "city", "state", "zip", "phone"):
                    if details.get(field) and not record.get(field):
                        record[field] = details[field]
                if not record.get("website") and details.get("website"):
                    record["website"] = details["website"]
                if not record.get("description") and details.get("description"):
                    record["description"] = details["description"]
                return record

            with ThreadPoolExecutor(max_workers=8) as pool:
                futures = {pool.submit(resolve_detail, records[i]): i for i in missing_addr}
                done = 0
                for future in as_completed(futures):
                    idx = futures[future]
                    try:
                        records[idx] = future.result(timeout=15)
                    except Exception:
                        pass
                    done += 1
                    if done % 10 == 0:
                        print(f"  Resolved {done}/{len(missing_addr)}...")

        filled_street = sum(1 for i in missing_addr if records[i].get("street"))
        filled_phone  = sum(1 for i in missing_addr if records[i].get("phone"))
        filled_desc   = sum(1 for i in missing_addr if records[i].get("description"))
        print(f"  Resolution complete — address: +{filled_street}  phone: +{filled_phone}  description: +{filled_desc}")

    # ── State backfill ───────────────────────────────────────────────────────
    # If state is blank on every record, infer it from the source domain.
    # This handles SimpleView CVB sites where state never appears in the API response.
    # We don't guess zip — multiple cities in a region means multiple zips.
    DOMAIN_STATE = {
        # Michigan
        "annarbor.org": "MI", "visitannarbor.org": "MI",
        # Wisconsin
        "visitmadison.com": "WI", "visitmilwaukee.org": "WI",
        # Georgia
        "visitathensga.com": "GA", "exploregeorgia.org": "GA",
        "visitsavannah.com": "GA", "gosouthsavannah.com": "GA",
        # North Carolina
        "visitchapelhill.org": "NC", "visitraleigh.com": "NC",
        "discoverdurham.com": "NC", "downtowndurham.com": "NC",
        "downtownchapelhill.com": "NC", "visithillsboroughnc.com": "NC",
        "visitwilmingtonnc.com": "NC", "homeofgolf.com": "NC",
        "discoverburkecounty.com": "NC",
        # South Carolina
        "visitgreenvillesc.com": "SC", "charlestoncvb.com": "SC",
        "charleston.com": "SC",
        # Colorado
        "bouldercoloradousa.com": "CO",
        # New York
        "visitithaca.com": "NY",
        # Iowa
        "thinkiowacity.com": "IA",
        # Vermont
        "helloburlingtonvt.com": "VT",
        # Texas
        "austintexas.org": "TX",
        # Virginia
        "visitcharlottesville.org": "VA",
    }
    source_netloc = urlparse(url).netloc.replace("www.", "")
    inferred_state = DOMAIN_STATE.get(source_netloc, "")
    if inferred_state:
        missing_state = sum(1 for r in records if not r.get("state"))
        if missing_state == len(records):
            # All blank — backfill confidently
            for r in records:
                r["state"] = inferred_state
            print(f"  State backfilled: {inferred_state} ({missing_state} records)")
        elif missing_state > 0:
            # Partial — only fill blanks
            for r in records:
                if not r.get("state"):
                    r["state"] = inferred_state
            print(f"  State backfilled for {missing_state} blank records: {inferred_state}")

    # Save output — filename includes domain + path slug + capture date
    parsed = urlparse(url)
    domain = parsed.netloc.replace("www.", "").replace(".", "_")
    path_slug = parsed.path.strip("/").replace("/", "_") or "listings"
    import os as _os
    from datetime import date as _date
    datestamp = _date.today().strftime("%Y-%m-%d")
    script_dir = _os.path.dirname(_os.path.abspath(__file__))
    data_dir = _os.path.join(script_dir, "..", "data")
    _os.makedirs(data_dir, exist_ok=True)
    output_file = _os.path.join(data_dir, f"{domain}_{path_slug}_{datestamp}.csv")

    # Strip private/internal fields before writing
    _private = {"_detail_url", "_resolve_error"}
    for r in records:
        for k in _private:
            r.pop(k, None)
    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_FIELDS)
        writer.writeheader()
        writer.writerows(records)

    print(f"\n✅ Done — {len(records)} records saved to {output_file}")
    print(f"   With address:     {sum(1 for r in records if r.get('street'))}")
    print(f"   With phone:       {sum(1 for r in records if r.get('phone'))}")
    print(f"   With website:     {sum(1 for r in records if r.get('website'))}")
    print(f"   With description: {sum(1 for r in records if r.get('description'))}")