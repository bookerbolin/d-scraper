"""
common.py — Shared utilities for scraper.py, api.py, and playwright_scraper.py.
All three files import from here to avoid duplication.
"""
import re
import csv
import time
import requests
from urllib.parse import urlparse, urlencode, parse_qs, urlunparse, unquote
from bs4 import BeautifulSoup
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed

HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}

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

DOMAIN_STATE = {
    "annarbor.org": "MI", "visitannarbor.org": "MI",
    "visitmadison.com": "WI", "visitmilwaukee.org": "WI",
    "visitathensga.com": "GA", "exploregeorgia.org": "GA",
    "visitsavannah.com": "GA", "gosouthsavannah.com": "GA",
    "visitchapelhill.org": "NC", "visitraleigh.com": "NC",
    "discoverdurham.com": "NC", "downtowndurham.com": "NC",
    "downtownchapelhill.com": "NC", "visithillsboroughnc.com": "NC",
    "visitwilmingtonnc.com": "NC", "homeofgolf.com": "NC",
    "discoverburkecounty.com": "NC", "charlottesgotalot.com": "NC",
    "visitgreenvillesc.com": "SC", "charlestoncvb.com": "SC",
    "charleston.com": "SC",
    "bouldercoloradousa.com": "CO",
    "visitithaca.com": "NY",
    "thinkiowacity.com": "IA",
    "helloburlingtonvt.com": "VT",
    "austintexas.org": "TX",
    "visitcharlottesville.org": "VA",
    "visitrichmondva.com": "VA",
    "venturerichmond.com": "VA",
    "visitslo.com": "CA",
    "visitberkeley.com": "CA",
    "visitcorvallis.com": "OR",
    "eugenecascadescoast.org": "OR",
    "bellingham.org": "WA",
    "destinationmissoula.org": "MT",
    "santabarbaraca.com": "CA",
}

OUTPUT_FIELDS = ["name", "street", "city", "state", "zip", "phone", "website", "description", "source_url"]
MAX_PAGES = 20



def normalize_state(raw):
    """Convert full state name or 2-letter abbreviation to uppercase abbreviation."""
    if not raw:
        return ""
    stripped = raw.strip()
    if re.match(r'^[A-Z]{2}$', stripped):
        return stripped
    return STATE_ABBR.get(stripped.lower(), stripped.upper() if len(stripped) == 2 else stripped)


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
        r"powered by|all rights reserved|\d+\s+\w+.{0,40}(?:st|ave|rd|dr|blvd|ln|way|street|avenue|road|drive|boulevard)\b)",
        _re2.I
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
    if source_domain is None:
        source_domain = urlparse(detail_url).netloc

    try:
        r = _req.get(detail_url, headers=HEADERS, timeout=12)
        r.raise_for_status()
        soup = _BS(r.text, "lxml")
        for tag in soup(["nav", "header", "footer"]):
            tag.decompose()

        result = {}

        # Website — look for external link labelled as website
        # Strategy 1: anchor text contains website/official keywords
        _SKIP_SITE = ["facebook","instagram","twitter","x.com","yelp","tripadvisor",
                      "google","maps.google","mailto:","tel:"]
        for a in soup.find_all("a", href=True):
            href = a.get("href", "")
            atext = a.get_text(strip=True).lower()
            if (href.startswith("http")
                    and source_domain not in href
                    and not any(s in href for s in _SKIP_SITE)
                    and any(w in atext for w in ["website","official","visit site","homepage"])):
                result["website"] = href
                break

        # Strategy 2: link preceded by a "Website" heading/label
        if not result.get("website"):
            for heading in soup.find_all(["h3","h4","strong","dt","label","p"],
                                          string=re.compile(r"^website", re.I)):
                # Find next <a> after the heading
                for sib in heading.find_next_siblings():
                    a = sib if sib.name == "a" else sib.find("a")
                    if a and a.get("href","").startswith("http"):
                        href = a["href"]
                        if source_domain not in href and not any(s in href for s in _SKIP_SITE):
                            result["website"] = href
                            break
                if result.get("website"):
                    break

        # Strategy 3: any clean external link in the main content area
        if not result.get("website"):
            _SKIP_TEXT = {"doing business","list your business","add a business",
                          "submit event","add an event","privacy","contact","login"}
            for a in soup.find_all("a", href=True):
                href = a.get("href","")
                atext = a.get_text(strip=True).lower()
                if (href.startswith("http")
                        and source_domain not in href
                        and not any(s in href for s in _SKIP_SITE)
                        and atext not in _SKIP_TEXT
                        and len(atext) > 3):
                    result["website"] = href
                    break

        # Phone — tel: link first, then plain text fallback
        tel = soup.find("a", href=re.compile(r"^tel:"))
        if tel:
            phone = extract_phone(tel["href"])
            if phone:
                result["phone"] = phone
        if not result.get("phone"):
            # Check for phone near a "Phone" label heading
            for heading in soup.find_all(["h3","h4","strong","dt","label"],
                                          string=re.compile(r"phone", re.I)):
                sib = heading.find_next_sibling() or heading.parent.find_next_sibling()
                if sib:
                    phone = extract_phone(sib.get_text(strip=True))
                    if phone:
                        result["phone"] = phone
                        break
        if not result.get("phone"):
            # Last resort: scan full page text
            page_text_ph = soup.get_text(" ", strip=True)
            phone = extract_phone(page_text_ph)
            if phone:
                result["phone"] = phone

        # Address — try <address> tag first (semantic HTML)
        addr_tag = soup.find("address")
        if addr_tag:
            addr_text = re.sub(r'\s+', ' ', addr_tag.get_text(separator=" ", strip=True))
            # Try full "Street, City, State ZIP" parse
            m = re.search(r'(\d+[^,]{2,60}),\s*([^,]+),\s*([A-Za-z ]{2,20})\s+(\d{5})\b', addr_text)
            if m:
                result["street"] = m.group(1).strip()
                result["city"]   = m.group(2).strip()
                result["state"]  = normalize_state(m.group(3).strip())
                result["zip"]    = m.group(4).strip()
            elif addr_text:
                result["street"] = clean_address(extract_address_from_text(addr_text)) or addr_text[:80]

        # Address — try Location/Address heading → sibling lines
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

        # Fallback: extract address from Google Maps embed ?q= parameter
        if not result.get("street"):
            from urllib.parse import unquote as _unquote, parse_qs as _pqs, urlparse as _ulp
            for iframe in soup.find_all("iframe", src=True):
                src = iframe["src"]
                if "google.com/maps" in src:
                    # Try ?q= parameter
                    parsed = _ulp(src)
                    params = _pqs(parsed.query)
                    q = params.get("q", params.get("query", [""]))[0]
                    if not q:
                        # Also check for address in path e.g. /maps/place/ADDRESS
                        m_path = re.search(r'/place/([^/@]+)', src)
                        if m_path:
                            q = _unquote(m_path.group(1)).replace("+", " ")
                    if q:
                        q = _unquote(q).replace("+", " ").strip()
                        # Try comma-separated: "332 Webbs Mill Rd, Floyd, VA 24091"
                        m_addr = re.search(r'(\d+[^,]{2,50}),\s*([^,]+),\s*([A-Za-z ]{2,20})\s+(\d{5})\b', q)
                        if m_addr:
                            result["street"] = m_addr.group(1).strip()
                            result["city"]   = m_addr.group(2).strip()
                            result["state"]  = normalize_state(m_addr.group(3).strip())
                            result["zip"]    = m_addr.group(4).strip()
                        else:
                            # Space-separated: anchor on STATE ZIP at end, split city as last word(s) before state
                            # "332 Webbs Mill Road North Floyd VA 24091"
                            m_sz = re.search(r'\b([A-Z]{2})\s+(\d{5})\s*$', q)
                            if m_sz:
                                before = q[:m_sz.start()].strip()
                                # Split at last street-type suffix word
                                # e.g. "332 Webbs Mill Road North Floyd" → street ends at "Road", city = "North Floyd"? 
                                # Better: street ends at last suffix, rest is city
                                SUFFIX_RE = re.compile(
                                    r'\b(st|ave|blvd|rd|dr|ln|way|hwy|pkwy|ct|pl|cir|'
                                    r'street|avenue|boulevard|road|drive|lane|highway|'
                                    r'court|place|circle|trail|trl|pike|loop|row|run)\b\.?',
                                    re.I
                                )
                                last_suffix = None
                                for m_s in SUFFIX_RE.finditer(before):
                                    last_suffix = m_s
                                if last_suffix:
                                    after_suffix = before[last_suffix.end():].strip()
                                    DIR_RE = re.compile(r'^(north|south|east|west|ne|nw|se|sw)\b\s*', re.I)
                                    dir_m = DIR_RE.match(after_suffix)
                                    if dir_m:
                                        street_candidate = (before[:last_suffix.end()].strip()
                                                            + " " + dir_m.group(1)).strip()
                                        city_candidate   = after_suffix[dir_m.end():].strip()
                                    else:
                                        street_candidate = before[:last_suffix.end()].strip()
                                        city_candidate   = after_suffix
                                    if re.match(r'\d+', street_candidate) and city_candidate:
                                        result["street"] = street_candidate
                                        result["city"]   = city_candidate
                                        result["state"]  = m_sz.group(1)
                                        result["zip"]    = m_sz.group(2)
                            if not result.get("street"):
                                result["street"] = clean_address(q) or q[:80]
                        break

        # Description — use the same scoring approach as the Playwright resolver
        result["description"] = _extract_best_description(soup, result)

        return result
    except Exception:
        return {}


def looks_like_address(text):
    if not text or len(text) > 120:
        return False
    has_number = bool(re.search(r"\b\d+\b", text))
    if not has_number:
        return False
    street_types = [
        "street", "st", "avenue", "ave", "boulevard", "blvd",
        "road", "rd", "drive", "dr", "lane", "ln",
        "circle", "cir", "highway", "hwy",
        "parkway", "pkwy", "terrace", "ter",
        "trail", "trl", "pike", "alley", "broadway",
    ]
    text_lower = text.lower()
    has_street_type = any(re.search(r"\b" + re.escape(s) + r"\.?\b", text_lower) for s in street_types)
    noise_words = ["phone", "website", "features", "details", "social", "lunch", "open", "outdoor"]
    has_noise = sum(1 for w in noise_words if w in text_lower) >= 2
    return has_street_type and not has_noise



def fetch_soup(url, timeout=15):
    r = requests.get(url, headers=HEADERS, timeout=timeout)
    r.raise_for_status()
    return BeautifulSoup(r.text, "lxml")


def detect_city(url):
    domain = urlparse(url).netloc.lower()
    for city in ["durham", "chapel-hill", "hillsborough",
                 "raleigh", "asheville", "brevard", "pittsboro"]:
        if city.replace("-", "") in domain.replace("-", ""):
            return city.replace("-", " ").title()
    return ""


def is_simpleview(url):
    """Detect SimpleView CMS by checking page HTML for SimpleView signatures."""
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        text = r.text
        has_sv = ("/includes/public/assets/" in text
                  or "/includes/rest_v2/" in text
                  or "simpleviewinc.com" in text)
        is_wix = "wix.com" in text or "wixstatic.com" in text
        is_sq = "squarespace.com" in text or "sqsp.net" in text
        is_wp = "wp-content/" in text or "wp-includes/" in text
        return has_sv and not (is_wix or is_sq or is_wp)
    except Exception:
        return False


def scrape_simpleview_api(start_url, log=print):
    parsed = urlparse(start_url)
    base = f"{parsed.scheme}://{parsed.netloc}"
    api_url = f"{base}/includes/rest_v2/plugins_listings_listings/find/"
    api_headers = {
        **HEADERS,
        "Referer": start_url,
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "X-Requested-With": "XMLHttpRequest",
        "Origin": base,
    }
    city = detect_city(start_url)
    all_records = []
    page = 1
    limit = 25
    while True:
        params = {"limit": limit, "page": page, "rand": round(random.random(), 6)}
        log(f"  Fetching API page {page}...")
        try:
            r = requests.get(api_url, params=params, headers=api_headers, timeout=15)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            log(f"  API error: {e}")
            log("  Tip: SimpleView sites work better with playwright_scraper.py")
            break
        items = data if isinstance(data, list) else data.get("data", data.get("results", data.get("listings", [])))
        if not items:
            break
        for item in items:
            all_records.append({
                "name":        item.get("title", item.get("name", "")),
                "street":      item.get("address1", item.get("address", "")),
                "city":        item.get("city", ""),
                "state":       item.get("state", ""),
                "zip":         item.get("zip", ""),
                "website":     item.get("weburl", item.get("url", item.get("website", ""))),
                "description": item.get("description", item.get("teaser", "")),
                "source_url":  start_url,
            })
        log(f"  Page {page} — {len(items)} listings (total: {len(all_records)})")
        if len(items) < limit:
            break
        page += 1
        time.sleep(0.5)
    return all_records


def parse_listings(soup, source_domain=""):
    records = []

    # Pattern 1: Durham-style — h2 > a[href*=directory]
    h2_links = [h2 for h2 in soup.select("h2")
                if h2.find("a") and "directory" in (h2.find("a").get("href", ""))]
    if h2_links:
        for h2 in h2_links:
            a = h2.find("a")
            next_ul = h2.find_next_sibling("ul")
            items = next_ul.find_all("li") if next_ul else h2.find_next_siblings("li")
            street = ""
            phone = ""
            for item in items:
                t = item.get_text(separator=" ", strip=True)
                # Check tel: href on the <a> inside this li
                a_tel = item.find("a", href=re.compile(r"^tel:"))
                if a_tel and not phone:
                    phone = extract_phone(a_tel["href"])
                if not street:
                    candidate = clean_address(extract_address_from_text(t)) if extract_address_from_text(t) else ""
                    if candidate:
                        street = candidate
                if not phone:
                    phone = extract_phone(t)
            records.append({"name": a.get_text(strip=True), "street": street,
                            "phone": phone, "website": a["href"], "description": ""})
        return records, "durham-style"

    # Pattern 2: Chapel Hill / Wix-style — h3 + "Visit Website" sibling
    for h3 in soup.select("h3"):
        name = h3.get_text(strip=True)
        if not name or len(name) > 80:
            continue
        link = None
        for sibling in h3.next_siblings:
            if not hasattr(sibling, "name"):
                continue
            a = sibling if sibling.name == "a" else sibling.find("a")
            if a and hasattr(a, "get_text") and re.search(r"Visit Website", a.get_text(strip=True), re.I):
                link = a
                break
        if not link:
            parent = h3.find_parent()
            if parent:
                link = parent.find("a", string=re.compile("Visit Website", re.I))
        if not link:
            continue
        records.append({"name": name, "street": "", "phone": "", "website": link.get("href", ""), "description": ""})
    if records:
        return records, "chapelhill-style"

    # Pattern 3: Hillsborough-style — <a> wrapping <h3>
    # Exclude nav/header/footer to avoid picking up menu links
    for a in soup.select("a"):
        if a.find_parent(["nav", "header", "footer"]):
            continue
        h3 = a.find("h3")
        if not h3:
            continue
        name = h3.get_text(strip=True)
        if not name or len(name) > 80:
            continue
        desc_el = a.find("p")
        # Extract address from card-footer or any div/p with address-like text
        street = city = state = zip_code = ""
        # Prioritise card-footer, then fall back to all divs/p/span
        candidates = (
            a.find_all(class_=lambda c: c and "footer" in " ".join(c) if c else False)
            or a.find_all(["div", "p", "span", "footer"])
        )
        for candidate in candidates:
            # Normalise all whitespace (newlines, multiple spaces) to single space
            raw = candidate.get_text(separator=" ", strip=True)
            text = re.sub(r"\s+", " ", raw).strip()
            # Match "Street, City, ST, ZIP" or "Street, City, ST ZIP"
            m = re.match(
                r"^(.+?),\s*([^,]+?),\s*([A-Z]{2}),?\s*(\d{5})\s*$",
                text
            )
            if m:
                street   = m.group(1).strip()
                city     = m.group(2).strip()
                state    = m.group(3).strip()
                zip_code = m.group(4).strip()
                break
            # Fallback: just try to extract a street address
            if not street:
                addr = extract_address_from_text(text)
                if addr:
                    street = clean_address(addr)
        records.append({
            "name": name, "street": street, "city": city, "state": state,
            "zip": zip_code, "phone": "", "website": a.get("href", ""),
            "description": desc_el.get_text(strip=True) if desc_el else "",
        })
    if records:
        return records, "hillsborough-style"


    # Pattern 4: Chamber of Commerce / GrowthZone-style — h5 > a + ul > li
    # Address is in a Google Maps link, phone in tel: link, website in "Visit Website" link.
    # Used by chamber.org directories powered by GrowthZone/ChamberMaster CMS.
    h5_links = [h5 for h5 in soup.select("h5") if h5.find("a")]
    if h5_links:
        for h5 in h5_links:
            a = h5.find("a")
            name = a.get_text(strip=True)
            if not name:
                continue
            next_ul = h5.find_next_sibling("ul")
            if not next_ul:
                continue
            street = ""
            phone = ""
            website = ""
            description = ""
            for li in next_ul.find_all("li"):
                li_a = li.find("a")
                text = li.get_text(strip=True)
                if li_a:
                    href = li_a.get("href", "")
                    if "google.com/maps" in href:
                        street = text
                    elif href.startswith("tel:"):
                        digits = re.sub(r'\D', '', href.replace("tel:", ""))
                        if len(digits) == 10:
                            phone = f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
                    elif "Visit Website" in text and href.startswith("http"):
                        website = href
                elif text and not re.search(r'\d{5}', text):
                    # Plain text without zip = likely description
                    if len(text) > 5 and not text.startswith("("):
                        description = text
            records.append({
                "name": name, "street": street, "phone": phone,
                "website": website, "description": description,
            })
        if records:
            return records, "chamber-style"



    # Pattern 5: Editorial prose-style — <strong> name + sibling text lines
    # Handles both:
    #   - Wilmington style: single-line "Street, City, ST ZIP" with phone
    #   - DiscoverDurham style: multi-line address blocks, detail page links
    strong_tags = [s for s in soup.select("article strong, .entry-content strong, .post-content strong, main strong, [class*='listing'] strong, p strong")
                   if s.get_text(strip=True) and len(s.get_text(strip=True)) < 80
                   and not s.find_parent("nav") and not s.find_parent("header")
                   and not s.find_parent("footer")]
    if strong_tags:
        for strong in strong_tags:
            name = strong.get_text(strip=True)
            if not name:
                continue
            lines = []
            detail_url = ""
            node = strong.next_sibling
            while node:
                if hasattr(node, "name"):
                    if node.name in ("strong", "b", "h2", "h3", "h4", "h5"):
                        break
                    if node.name == "br":
                        node = node.next_sibling
                        continue
                    if node.name == "a":
                        href = node.get("href", "")
                        text = node.get_text(strip=True)
                        if href.startswith("tel:"):
                            lines.append(("tel", href))
                        elif "google.com/maps" in href or "maps.google" in href:
                            pass  # skip map links
                        elif any(w in text.lower() for w in ["website", "visit", "menu", "order"]):
                            lines.append(("link", href))
                        elif text.lower() == "details" or "/directory/" in href:
                            detail_url = href
                        elif href.startswith("http") and not any(s in href for s in ["facebook", "instagram", "twitter", "yelp"]):
                            lines.append(("link", href))
                        node = node.next_sibling
                        continue
                text = str(node).strip() if not hasattr(node, "name") else node.get_text(strip=True)
                if text:
                    lines.append(("text", text))
                node = node.next_sibling

            street = ""
            city_val = ""
            state_val = ""
            zip_val = ""
            phone = ""
            website = ""

            for item in lines:
                if item[0] == "tel":
                    phone = extract_phone(item[1])
                elif item[0] == "link":
                    website = item[1]
                elif item[0] == "text":
                    t = item[1].strip().strip(",")
                    if not t:
                        continue
                    # City, ST ZIP pattern
                    city_match = re.match(r'^([^,]+),\s*([A-Z]{2})\s+(\d{5})$', t)
                    if city_match:
                        city_val = city_match.group(1).strip()
                        state_val = city_match.group(2)
                        zip_val = city_match.group(3)
                    elif not phone:
                        p = extract_phone(t)
                        if p:
                            phone = p
                    if not street and re.search(r'^\d+\s+\w', t) and not city_match:
                        street = t

            # Use detail_url as website if no outbound link found
            if not website and detail_url:
                website = detail_url

            if name and (street or city_val or phone):
                rec = {
                    "name": name, "street": street, "phone": phone,
                    "website": website, "description": "",
                }
                if city_val:
                    rec["city"] = city_val
                if state_val:
                    rec["state"] = state_val
                if zip_val:
                    rec["zip"] = zip_val
                records.append(rec)

        if records:
            return records, "wilmington-prose-style"


    # Pattern 6: Blog/article listing style — h3 > a (name + website) + p > strong (address) + p (description)
    # Used by DiscoverDurham new-restaurants pages and similar editorial content.
    # Structure: <h3><a href="website">Name</a></h3> <p><strong>Address</strong></p> <p>Description</p>
    h3_links = [h3 for h3 in soup.select("article h3, .entry-content h3, main h3")
                if h3.find("a") and not h3.find_parent("nav") and not h3.find_parent("header")]
    if h3_links:
        for h3 in h3_links:
            a = h3.find("a")
            if not a:
                continue
            name = a.get_text(strip=True)
            if not name or len(name) > 80:
                continue
            website = a.get("href", "")
            # Skip section headers and nav links
            if not website or any(s in website for s in ["#", "facebook", "instagram"]):
                continue
            address = ""
            description = ""
            phone = ""
            sibling = h3.find_next_sibling()
            while sibling:
                if sibling.name in ("h2", "h3"):
                    break
                if sibling.name == "p":
                    strong = sibling.find("strong")
                    if strong and not address:
                        address = strong.get_text(strip=True).rstrip(".")
                    elif not strong and not description:
                        text = sibling.get_text(strip=True)
                        if text and len(text) > 10:
                            p = extract_phone(text)
                            if p and not phone:
                                phone = p
                            else:
                                description = text[:200]
                sibling = sibling.find_next_sibling()
            if name and (address or website):
                records.append({
                    "name": name, "street": address, "phone": phone,
                    "website": website, "description": description,
                })
        if records:
            return records, "blog-article-style"


    # Pattern 7: Card-link style — <a href="/detail/"> wrapping <h2> + <p> address + <p> description
    # Used by ncwine.org and similar WordPress sites with card-grid layouts.
    # The entire card is an <a> tag containing heading + address + description.
    card_links = [a for a in soup.find_all("a", href=True)
                  if a.find("h2") and not a.find_parent("nav") and not a.find_parent("header")]
    if card_links:
        for card in card_links:
            h2 = card.find("h2")
            if not h2:
                continue
            name = h2.get_text(strip=True)
            if not name or len(name) > 100:
                continue
            detail_url = card.get("href", "")
            paragraphs = card.find_all("p")
            raw_address = ""
            description = ""
            for p in paragraphs:
                text = p.get_text(strip=True)
                if not text:
                    continue
                if not raw_address:
                    raw_address = text
                elif not description:
                    description = text[:200]

            # Parse address — handle "Street, City, ST ZIP" and "Street City ST ZIP"
            raw = re.sub(r',?\s*USA\s*$', '', raw_address).strip()
            street = raw_address
            city_val = ""
            state_val = ""
            zip_val = ""
            if ',' in raw:
                parts = [p.strip() for p in raw.split(',')]
                last = parts[-1].strip()
                st_zip = re.match(r'([A-Z]{2})\s+(\d{5})', last)
                if st_zip and len(parts) >= 3:
                    street = parts[0]
                    city_val = parts[-2].strip()
                    state_val = st_zip.group(1)
                    zip_val = st_zip.group(2)
                elif st_zip and len(parts) == 2:
                    city_val = parts[0]
                    state_val = st_zip.group(1)
                    zip_val = st_zip.group(2)
                    street = ""
            else:
                m = re.search(r'\b([A-Z]{2})\s+(\d{5})\s*$', raw)
                if m:
                    street = raw[:m.start()].strip()
                    state_val = m.group(1)
                    zip_val = m.group(2)

            if name and (street or city_val):
                rec = {
                    "name": name, "street": street, "phone": "",
                    "website": detail_url, "description": description,
                }
                if city_val:
                    rec["city"] = city_val
                if state_val:
                    rec["state"] = state_val
                if zip_val:
                    rec["zip"] = zip_val
                records.append(rec)
        if records:
            return records, "card-link-style"


    # Pattern 8: Charleston.com / JBusiness Directory style
    # <a href="*businesses/*"><h3>name</h3></a> + sibling <p> for location
    # Detail pages at charleston.com/businesses/slug have full address/phone/website.
    # Pagination: ?start=20, ?start=40 etc.
    charleston_links = [a for a in soup.find_all("a", href=re.compile(r"businesses/"))
                        if a.find("h3") and not a.find_parent("nav")]
    if charleston_links:
        for a in charleston_links:
            h3 = a.find("h3")
            if not h3:
                continue
            name = h3.get_text(strip=True)
            if not name or len(name) > 100:
                continue
            detail_url = a.get("href", "")
            # Location text in next sibling element
            loc_el = a.find_next_sibling(["p", "div", "span"])
            location = loc_el.get_text(strip=True) if loc_el else ""
            city = location.split(",")[0].strip() if location else ""
            records.append({
                "name": name, "street": "", "phone": "", "city": city,
                "state": "", "zip": "", "website": detail_url, "description": "",
            })
        if records:
            return records, "businesses-directory"


    # Pattern 9: GoSouthSavannah / editorial h3 prose style
    # h3 plain text name (no link inside) + next sibling text line for address + sibling <a> for website
    # Used by gosouthsavannah.com and similar static HTML travel guides
    # Structure: <h3>Name</h3> plain address text <a href="website">Official website</a> <p>description</p>
    body_h3s = [h3 for h3 in soup.find_all("h3")
                if not h3.find("a")  # plain text h3, no link inside
                and h3.get_text(strip=True)
                and len(h3.get_text(strip=True)) < 80
                and not h3.find_parent("nav")
                and not h3.find_parent("header")
                and not h3.find_parent("footer")]
    if body_h3s:
        for h3 in body_h3s:
            name = h3.get_text(strip=True)
            if not name:
                continue
            street = ""
            website = ""
            description = ""
            phone = ""
            node = h3.next_sibling
            while node:
                # NavigableString.name is None — use that to distinguish text from tags
                if getattr(node, "name", None) is None:
                    text = str(node).strip()
                    if text and not street:
                        addr = extract_address_from_text(text)
                        if addr:
                            street = clean_address(addr)
                        elif not phone:
                            phone = extract_phone(text)
                else:
                    if node.name in ("h2", "h3"):
                        break
                    if node.name == "a":
                        href = node.get("href", "")
                        text = node.get_text(strip=True)
                        if href.startswith("tel:"):
                            phone = extract_phone(href)
                        elif href.startswith("http") and not any(
                                s in href for s in ["facebook", "instagram", "twitter"]):
                            website = href
                    elif node.name == "p":
                        text = node.get_text(strip=True)
                        # Try address extraction first before treating as description
                        if not street:
                            addr = extract_address_from_text(text)
                            if addr:
                                street = clean_address(addr)
                                # Rest of text after address is description context
                                after = text[text.find(addr) + len(addr):].strip().lstrip(",").strip()
                                if after and len(after) > 10 and not description:
                                    description = after[:200]
                                continue
                        if not description and len(text) > 20:
                            description = text[:200]
                node = node.next_sibling
            if name:  # accept name-only; resolution pass fills in the rest
                records.append({
                    "name": name, "street": street, "phone": phone,
                    "website": website, "description": description,
                })
        # Only return if at least some records have substance (avoid nav-scraping)
        substantial = [r for r in records if r.get("street") or r.get("website") or r.get("phone")]
        if records and (substantial or len(records) >= 3):
            return records, "prose-h3-style"

    # Pattern 10: Prose-embedded directory links
    # Used by visitcharlottesville.org and similar WordPress CVB sites where
    # listing names appear as inline links within editorial paragraphs.
    # Structure: <p>...text... <a href="/directory/slug/">Name</a> ...text...</p>
    # No card structure — names only. Resolution pass fills address/phone/website.
    DIR_PATTERNS = ["/directory/", "/listing/", "/business/", "/member/", "/place/"]
    prose_links = []
    seen_hrefs = set()
    for a in soup.find_all("a", href=True):
        href = a.get("href", "")
        if not any(p in href for p in DIR_PATTERNS):
            continue
        if a.find_parent(["nav", "header", "footer"]):
            continue
        # Must be inside a paragraph or similar prose container
        if not a.find_parent(["p", "li", "td", "div"]):
            continue
        name = a.get_text(strip=True)
        if not name or len(name) > 80 or len(name) < 2:
            continue
        if href in seen_hrefs:
            continue
        seen_hrefs.add(href)
        # Make absolute URL
        full_href = href if href.startswith("http") else ""
        prose_links.append({"name": name, "street": "", "city": "", "state": "",
                            "zip": "", "phone": "", "website": full_href,
                            "description": ""})
    # Only use this pattern if we found a meaningful number of links
    # and no other pattern matched (records is still empty at this point)
    if len(prose_links) >= 5:
        return prose_links, "prose-directory-links"


    # Pattern 11: Structural card inference
    # Fires when no named pattern matched. Finds repeating block elements
    # that look like listing cards based on content structure (heading + address/phone/link),
    # without relying on specific class names.
    def _infer_cards(soup):
        PHONE_RE = re.compile(r'\(?\d{3}\)?[\s.\-]\d{3}[\s.\-]\d{4}')
        ADDR_RE  = re.compile(
            r'\d{1,5}\s+\w[\w\s\.]{2,30}'
            r'(?:St|Ave|Rd|Dr|Blvd|Ln|Way|Hwy|Pkwy|Ct|Pl|Cir|Street|Avenue|Road|Drive|Boulevard|Lane|Highway|Court|Place)\b',
            re.I
        )

        def score(el):
            txt = el.get_text(" ", strip=True)
            s = 0
            if el.find(["h2","h3","h4","h5"]) or el.find(
                    class_=re.compile(r'title|heading|name', re.I)):
                s += 1
            if ADDR_RE.search(txt):
                s += 1
            if PHONE_RE.search(txt) or el.find("a", href=re.compile(r'^tel:')):
                s += 1
            return s

        from collections import Counter
        candidates = []
        for el in soup.find_all(["article","li","div","section"], recursive=True):
            if el.find_parent(["nav","header","footer"]):
                continue
            txt = el.get_text(strip=True)
            if len(txt) < 20 or len(txt) > 2000:
                continue
            if score(el) >= 2:
                candidates.append(el)

        if not candidates:
            return []

        def sig(el):
            return (el.name, tuple(sorted(el.get("class", []))))

        counts = Counter(sig(el) for el in candidates)
        best_sig, best_count = counts.most_common(1)[0]
        cards = [el for el in candidates if sig(el) == best_sig] if best_count >= 3 else candidates

        # Remove ancestors of other cards
        card_ids = set(id(c) for c in cards)
        cards = [c for c in cards if not any(id(p) in card_ids for p in c.parents)]
        return cards

    inferred = _infer_cards(soup)
    if inferred:
        records = []
        for card in inferred:
            name_el = card.find(["h2","h3","h4","h5"]) or card.find(
                class_=re.compile(r'title|heading|name', re.I))
            name = name_el.get_text(strip=True) if name_el else ""
            if not name:
                for el in card.find_all(["strong","b","p"]):
                    t = el.get_text(strip=True)
                    if 2 < len(t) < 80 and not re.search(r'\d{3}.*\d{4}', t):
                        name = t
                        break
            if not name:
                continue

            text = card.get_text(separator=" ", strip=True)

            # Try <address> tag first — handles <br>-separated street/city lines
            street = city_val = state_val = zip_val = ""
            addr_tag = card.find("address")
            if addr_tag:
                # Join <br>-separated lines with commas
                parts = [s.strip() for s in addr_tag.get_text(separator="\n").split("\n") if s.strip()]
                joined = ", ".join(parts)
                # Try 4-part match (handles neighborhood/suite between street and city)
                m4 = re.search(r'(\d+[^,\n]{2,60}),\s*[^,\n]{2,40},\s*([A-Za-z][^,\n]{1,40}),\s*([A-Za-z ]{2,20})\s+(\d{5})\b', joined)
                m3 = re.search(r'(\d+[^,\n]{2,60}),\s*([A-Za-z][^,\n]{1,40}),\s*([A-Za-z ]{2,20})\s+(\d{5})\b', joined)
                if m4:
                    street    = m4.group(1).strip()
                    city_val  = m4.group(2).strip()
                    state_val = normalize_state(m4.group(3).strip())
                    zip_val   = m4.group(4).strip()
                elif m3:
                    street    = m3.group(1).strip()
                    city_val  = m3.group(2).strip()
                    state_val = normalize_state(m3.group(3).strip())
                    zip_val   = m3.group(4).strip()

            # Try full "Street, City, State ZIP" from card text
            if not street:
                full_addr = re.search(
                    r'(\d+[^,]{3,40}),\s*([^,]+),\s*([A-Za-z ]{2,20})\s+(\d{5})\b',
                    text
                )
                if full_addr:
                    street    = full_addr.group(1).strip()
                    city_val  = full_addr.group(2).strip()
                    state_val = normalize_state(full_addr.group(3).strip())
                    zip_val   = full_addr.group(4).strip()
                else:
                    street = clean_address(extract_address_from_text(text))

            # If city still blank, try to extract from street field
            # (handles "800 W Sheridan Ave Oklahoma City , OK 73016" in street)
            if street and not city_val:
                m_csz = re.search(r'^(.*?\d+\s+\S+.*?)\s+([A-Za-z][^,]{2,30})\s*,?\s*([A-Z]{2})\s+(\d{5})\b', street)
                if m_csz:
                    street    = m_csz.group(1).strip()
                    city_val  = m_csz.group(2).strip()
                    state_val = m_csz.group(3)
                    zip_val   = m_csz.group(4)

            phone_el = card.find("a", href=re.compile(r'^tel:'))
            phone = extract_phone(phone_el["href"]) if phone_el else extract_phone(text)

            # Capture any link — outbound preferred, same-domain detail link accepted
            website = ""
            detail_url = ""
            # Skip social/review sites and links with nav-like anchor text
            _SKIP_DOMAINS = ["facebook","instagram","twitter","x.com","google",
                             "yelp","tripadvisor","simpleviewinc"]
            _SKIP_TEXT = {"doing business","list your business","add a business",
                          "submit","login","sign in","privacy","contact"}
            for _a in card.find_all("a", href=True):
                _href = _a["href"]
                _netloc = urlparse(_href).netloc
                _atext = _a.get_text(strip=True).lower()
                _is_ext = (_netloc and _netloc != source_domain
                           and not any(s in _netloc for s in _SKIP_DOMAINS)
                           and _atext not in _SKIP_TEXT)
                _is_int = (not _netloc or _netloc == source_domain)
                if _is_ext:
                    website = _href
                    break
                elif _is_int and _href not in ("/", "#", "") and not detail_url:
                    detail_url = _href if _href.startswith("http") else f"https://{source_domain}{_href}"
                    # Don't set website to internal link — resolution pass will find the real one

            rec = {
                "name": name, "street": street, "city": city_val,
                "state": state_val, "zip": zip_val,
                "phone": phone, "website": website, "description": "",
            }
            if detail_url and detail_url != website:
                rec["_detail_url"] = detail_url
            records.append(rec)
        if records:
            return records, "inferred-cards"

    return [], None


def resolve_website(record, source_domain):
    # Always prefer _detail_url for fetching — it's the CVB detail page
    # which has phone/description, unlike the external business website
    detail_url = record.get("_detail_url", "")
    website = record.get("website", "")
    fetch_url = detail_url or website
    if not fetch_url:
        return record
    # Skip entirely if we already have everything we need
    if record.get("phone") and record.get("description") and record.get("street"):
        return record
    parsed = urlparse(fetch_url)
    # If fetching external site with no detail URL, skip — nothing to gain
    if not detail_url and parsed.netloc and parsed.netloc != source_domain:
        if record.get("phone") and record.get("description"):
            return record
        if not record.get("street"):
            pass  # still try for address
        else:
            return record
    website = fetch_url
    SKIP = [source_domain, "facebook.com", "instagram.com", "twitter.com",
            "yelp.com", "google.com", "tripadvisor.com",
            "linkedin.com", "tiktok.com", "youtube.com", "pinterest.com"]
    SOCIAL = ["facebook.com", "instagram.com", "twitter.com", "x.com",
              "linkedin.com", "tiktok.com", "youtube.com", "pinterest.com",
              "m.facebook.com"]
    try:
        full_url = website if website.startswith("http") else f"https://{source_domain}{website}"
        r = requests.get(full_url, headers=HEADERS, timeout=20)
        soup = BeautifulSoup(r.text, "lxml")

        # Remove nav, footer, header before scanning
        for tag in soup.find_all(["nav", "footer", "header"]):
            tag.decompose()

        # Extract phone — tel: link, then heading label, then page text
        if not record.get("phone"):
            tel = soup.find("a", href=re.compile(r"^tel:"))
            if tel:
                phone = extract_phone(tel["href"])
                if phone:
                    record["phone"] = phone
        if not record.get("phone"):
            for heading in soup.find_all(["h3","h4","strong","dt","label"],
                                          string=re.compile(r"phone", re.I)):
                sib = heading.find_next_sibling() or heading.parent.find_next_sibling()
                if sib:
                    phone = extract_phone(sib.get_text(strip=True))
                    if phone:
                        record["phone"] = phone
                        break
        if not record.get("phone"):
            record["phone"] = extract_phone(soup.get_text(" ", strip=True)) or ""

        # Extract address from Google Maps embed (iframe ?q= param)
        if not record.get("street"):
            from urllib.parse import parse_qs as _pqs2, urlparse as _ulp2, unquote as _uq2
            for iframe in soup.find_all("iframe", src=True):
                src = iframe["src"]
                if "google.com/maps" in src:
                    _pp = _ulp2(src)
                    _params = _pqs2(_pp.query)
                    q = _params.get("q", _params.get("query", [""]))[0]
                    if not q:
                        import re as _rem
                        mp = _rem.search(r'/place/([^/@]+)', src)
                        if mp: q = _uq2(mp.group(1)).replace("+", " ")
                    if q:
                        q = _uq2(q).replace("+", " ").strip()
                        ma = re.search(r'(\d+[^,]{2,50}),\s*([^,]+),\s*([A-Za-z ]{2,20})\s+(\d{5})\b', q)
                        if ma:
                            record["street"] = ma.group(1).strip()
                            if not record.get("city"):  record["city"]  = ma.group(2).strip()
                            if not record.get("state"): record["state"] = normalize_state(ma.group(3).strip())
                            if not record.get("zip"):   record["zip"]   = ma.group(4).strip()
                        else:
                            # Space-separated: anchor on STATE ZIP, use suffix to split street/city
                            msz = re.search(r'\b([A-Z]{2})\s+(\d{5})\s*$', q)
                            if msz:
                                before = q[:msz.start()].strip()
                                SFXR = re.compile(r'\b(st|ave|blvd|rd|dr|ln|way|hwy|pkwy|ct|pl|cir|street|avenue|boulevard|road|drive|lane|highway|court|place|circle)\b\.?', re.I)
                                last_sfx = None
                                for ms in SFXR.finditer(before): last_sfx = ms
                                if last_sfx:
                                    after = before[last_sfx.end():].strip()
                                    dm = re.match(r'(north|south|east|west|ne|nw|se|sw)\b\s*', after, re.I)
                                    if dm:
                                        street_p = before[:last_sfx.end()].strip() + " " + dm.group(1)
                                        city_p = after[dm.end():].strip()
                                    else:
                                        street_p = before[:last_sfx.end()].strip()
                                        city_p = after
                                    if re.match(r'\d+', street_p) and city_p:
                                        record["street"] = street_p
                                        if not record.get("city"):  record["city"]  = city_p
                                        if not record.get("state"): record["state"] = msz.group(1)
                                        if not record.get("zip"):   record["zip"]   = msz.group(2)
                    break

        # Extract address — try semantic <address> tag first, then text scan
        if not record.get("street"):
            addr_tag = soup.find("address")
            if addr_tag:
                addr_text = re.sub(r'[\xa0\s]+', ' ', addr_tag.get_text(separator=" ", strip=True))
                m = re.search(r'(\d+[^,]{2,60}),\s*([^,]+),\s*([A-Za-z ]{2,20})\s+(\d{5})\b', addr_text)
                if m:
                    record["street"] = m.group(1).strip()
                    if not record.get("city"):  record["city"]  = m.group(2).strip()
                    if not record.get("state"): record["state"] = normalize_state(m.group(3).strip())
                    if not record.get("zip"):   record["zip"]   = m.group(4).strip()
                elif addr_text:
                    record["street"] = clean_address(addr_text) or addr_text[:80]
        if not record.get("street"):
            for tag in soup.find_all(["p", "li", "div", "span"]):
                text = tag.get_text(separator=" ", strip=True)
                if looks_like_address(text):
                    record["street"] = clean_address(text)
                    break
                elif len(text) > 120:
                    extracted = extract_address_from_text(text)
                    if extracted:
                        record["street"] = extracted
                        break

        # Find best outbound website link — labelled first, then any clean link, then social
        if not record.get("website") or urlparse(record.get("website","")).netloc == source_domain:
            found_website = ""
            # First pass: labelled link
            for a in soup.find_all("a", href=True):
                href = a["href"]
                href_domain = urlparse(href).netloc
                if not href_domain or any(s in href_domain for s in SKIP):
                    continue
                label = a.get_text(strip=True).lower()
                if any(w in label for w in ["website", "visit", "www", "menu", "order", "book"]):
                    found_website = href
                    break
            # Second pass: any clean outbound link (skip nav-like anchor text)
            _SKIP_LINK_TEXT = {"doing business","list your business","add a business",
                               "submit event","add an event","privacy policy","contact us",
                               "login","sign in","register"}
            if not found_website:
                for a in soup.find_all("a", href=True):
                    href = a["href"]
                    href_domain = urlparse(href).netloc
                    atext = a.get_text(strip=True).lower()
                    if not href_domain or any(s in href_domain for s in SKIP):
                        continue
                    if href.startswith("http") and atext not in _SKIP_LINK_TEXT:
                        found_website = href
                        break
            # Third pass: social link fallback
            if not found_website:
                for a in soup.find_all("a", href=True):
                    href = a["href"]
                    href_domain = urlparse(href).netloc
                    if href.startswith("http") and any(s in href_domain for s in SOCIAL):
                        found_website = href
                        break
            if found_website:
                record["website"] = found_website

        # Extract description using best-scored prose block
        if not record.get("description"):
            desc = _extract_best_description(soup, record)
            if desc:
                record["description"] = desc

    except Exception as e:
        record["_resolve_error"] = str(e)[:120]
    return record


def resolve_all(records, source_domain, log=print):
    # Resolve if:
    # - website is internal (same domain as source)
    # - _detail_url is set and phone or description missing
    # - street and phone both missing (need full detail fetch)
    internal = [i for i, r in enumerate(records)
                if (
                    (r.get("website") and urlparse(r["website"]).netloc in ("", source_domain))
                    or (r.get("_detail_url") and (not r.get("phone") or not r.get("description")))
                    or (r.get("website") and not r.get("street") and not r.get("phone"))
                )]
    if not internal:
        return records
    log(f"  Resolving {len(internal)} detail pages...")
    with ThreadPoolExecutor(max_workers=10) as pool:
        futures = {pool.submit(resolve_website, records[i], source_domain): i for i in internal}
        done = 0
        for future in as_completed(futures):
            idx = futures[future]
            try:
                records[idx] = future.result(timeout=10)
            except Exception:
                pass
            done += 1
            if done % 20 == 0:
                log(f"  Resolved {done}/{len(internal)}...")
    errors = sum(1 for r in records if r.get("_resolve_error"))
    if errors:
        log(f"  Resolve errors ({errors}): showing first 3...")
        shown = 0
        for r in records:
            if r.get("_resolve_error") and shown < 3:
                log(f"    [{r['name'][:30]}] {r['_resolve_error']}")
                shown += 1
    # Clear any website fields that still point to the source domain — these are
    # internal detail page links that had no outbound business website on them
    for r in records:
        w = r.get("website", "")
        if w:
            netloc = urlparse(w).netloc
            if not netloc or netloc == source_domain:
                r["website"] = ""
    still_internal = sum(1 for r in records if urlparse(r.get("website", "")).netloc == source_domain)
    if errors:
        log(f"  Warning: {errors} resolution(s) failed")
    if still_internal:
        log(f"  Note: {still_internal} URLs still point to {source_domain} (no outbound link on detail page)")
    for r in records:
        r.pop("_resolve_error", None)
        r.pop("_detail_url", None)
    return records


def scrape_html(start_url, log=print):
    city = detect_city(start_url)
    all_records = []
    seen_names = set()
    page = 1
    js_detected = False
    # Detect WordPress /page/N/ style pagination from first page
    wp_page_style = False
    _start_page_size = 0  # auto-detected offset pagination page size
    while page <= MAX_PAGES:
        # Try multiple pagination styles
        if page == 1:
            url = start_url
        elif wp_page_style:
            # WordPress /page/N/ style: insert /page/N/ before query string
            from urllib.parse import urlparse as _ulp, urlunparse as _uu
            _p = _ulp(start_url)
            _path = _p.path.rstrip("/")
            url = _uu(_p._replace(path=f"{_path}/page/{page}/"))
        elif "_start_page_size" in dir() and _start_page_size:
            # Offset-style pagination: ?start=0, ?start=20, ?start=40 etc.
            sep = "&" if "?" in start_url else "?"
            url = f"{start_url}{sep}start={(page-1)*_start_page_size}"
        else:
            # Build paginated URL by injecting page param, preserving existing query params
            from urllib.parse import urlparse as _ulp, parse_qs as _pqs, urlencode as _ue, urlunparse as _uu
            _p = _ulp(start_url)
            _params = _pqs(_p.query, keep_blank_values=True)
            # Remove any existing page/pg param to avoid conflicts
            _params.pop("page", None)
            _params.pop("pg", None)
            # Prepend page param so it appears first (some CMSes require this)
            _new_params = {"page": [str(page)]}
            _new_params.update(_params)
            url = _uu(_p._replace(query=_ue(_new_params, doseq=True)))
        log(f"  Fetching page {page}...")
        try:
            soup = fetch_soup(url)
        except Exception as e:
            log(f"  Error: {e}")
            break
        # Detect pagination style from first page links
        if page == 1:
            import re as _re
            _next_links = soup.find_all("a", href=_re.compile(r'/page/\d+/'))
            if _next_links:
                wp_page_style = True
                log("  Detected WordPress /page/N/ pagination")
            # Detect start=N offset pagination from next/pager links
            _start_links = soup.find_all("a", href=_re.compile(r'[?&]start=\d+'))
            if _start_links:
                import urllib.parse as _ulps
                _start_vals = []
                for _sl in _start_links:
                    _m = _re.search(r'[?&]start=(\d+)', _sl["href"])
                    if _m:
                        _start_vals.append(int(_m.group(1)))
                if _start_vals:
                    _start_page_size = min(_start_vals)
                    log(f"  Detected start=N pagination (page size: {_start_page_size})")
        source_domain = urlparse(url).netloc
        listings, pattern = parse_listings(soup, source_domain)
        if not listings:
            if page == 1:
                js_detected = True
            break
        new_count = 0
        for item in listings:
            name = item.get("name", "").strip()
            if not name or name in seen_names:
                continue
            seen_names.add(name)
            new_count += 1
            _website = item.get("website", "")
            _detail = item.get("_detail_url", "")
            # If website is same-domain, it IS the detail URL — promote it
            if _website and not _detail:
                _parsed_w = urlparse(_website)
                if not _parsed_w.netloc or _parsed_w.netloc == source_domain:
                    _detail = _website if _website.startswith("http") else f"https://{source_domain}{_website}"
            _rec = {
                "name": name, "street": item.get("street", ""), "city": item.get("city", ""),
                "state": item.get("state", ""), "zip": item.get("zip", ""),
                "website": _website,
                "phone": item.get("phone", ""), "description": item.get("description", ""), "source_url": start_url,
            }
            if _detail:
                _rec["_detail_url"] = _detail
            all_records.append(_rec)
        log(f"  Page {page} — {new_count} new listings (pattern: {pattern})")
        if new_count == 0:
            break
        page += 1
        time.sleep(0.8)
    return all_records, js_detected