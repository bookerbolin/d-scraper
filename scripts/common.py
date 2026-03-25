"""
common.py — Shared utilities for scraper.py, api.py, and playwright_scraper.py.
All three files import from here to avoid duplication.
"""
import re
import csv
import time
from urllib.parse import urlparse

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

