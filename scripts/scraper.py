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

HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}


def resolve_charleston_detail(detail_url):
    """Fetch a charleston.com/businesses/ detail page and extract address/phone/website."""
    try:
        r = requests.get(detail_url, headers=HEADERS, timeout=12)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")
        for tag in soup.find_all(["nav", "footer", "header"]):
            tag.decompose()
        result = {}
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if href.startswith("http") and "charleston.com" not in href and                not any(s in href for s in ["facebook", "instagram", "twitter",
                                           "yelp", "google", "tripadvisor"]):
                result["website"] = href
                break
        tel = soup.find("a", href=re.compile(r"^tel:"))
        if tel:
            result["phone"] = extract_phone(tel["href"])
        for tag in soup.find_all(["p", "div", "span", "address"]):
            text = tag.get_text(separator=" ", strip=True)
            if looks_like_address(text):
                result["street"] = clean_address(text)
                break
            elif len(text) > 30:
                extracted = extract_address_from_text(text)
                if extracted:
                    result["street"] = extracted
                    break
        return result
    except Exception:
        return {}



OUTPUT_FIELDS = ["name", "street", "city", "state", "zip", "phone", "website", "description", "source_url"]
MAX_PAGES = 20


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
    return match.group(1).strip().rstrip(",") if match else ""


def clean_address(text):
    match = re.search(r"\bAddress\s+([^\n]+)", text, re.IGNORECASE)
    if match:
        text = match.group(1).strip()
    text = re.sub(
        r"\s+(features|phone|website|social info|open late|outdoor|lunch|breakfast|dinner)\b.*$",
        "", text, flags=re.IGNORECASE
    ).strip()
    return text


def extract_phone(text):
    """Extract first phone number from text. Returns (XXX) XXX-XXXX format."""
    import re as _re
    # Handle tel: links — strip country code if present
    tel_match = _re.search(r'tel:\+?1?(\d{10})', text)
    if tel_match:
        digits = tel_match.group(1)
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    # Standard formatted patterns
    pattern = _re.compile(r'(\(?\d{3}\)?[\s\-\.]\d{3}[\s\-\.]\d{4})')
    match = pattern.search(text)
    if match:
        digits = _re.sub(r'\D', '', match.group(1))
        if len(digits) == 10:
            return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    return ""


def fetch_soup(url, timeout=15):
    r = requests.get(url, headers=HEADERS, timeout=timeout)
    r.raise_for_status()
    return BeautifulSoup(r.text, "lxml")


def detect_city(url):
    domain = urlparse(url).netloc.lower()
    for city in ["durham", "chapelhill", "chapel-hill", "hillsborough",
                 "raleigh", "asheville", "brevard", "pittsboro"]:
        if city.replace("-", "") in domain.replace("-", ""):
            return city.replace("-", " ").title()
    return ""


def is_simpleview(url):
    parsed = urlparse(url)
    domain = parsed.netloc.lower()
    for indicator in ["visitchapelhill", "visitraleigh", "visitdurham", "exploreraleigh", "visitnc", "visitcary"]:
        if indicator in domain:
            return True
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        text = r.text
        has_sv = "/includes/public/assets/" in text
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
                "city":        item.get("city", city),
                "state":       item.get("state", "NC"),
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


def parse_listings(soup):
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
                "state": "SC", "zip": "", "website": detail_url, "description": "",
            })
        if records:
            return records, "charleston-style"


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

    return [], None


def scrape_html(start_url, log=print):
    city = detect_city(start_url)
    all_records = []
    seen_names = set()
    page = 1
    js_detected = False
    while page <= MAX_PAGES:
        # Try multiple pagination styles
        if page == 1:
            url = start_url
        elif "discoverdurham.com" in start_url:
            sep = "&" if "?" in start_url else "?"
            url = f"{start_url}{sep}page={page}&"
        elif "charleston.com" in start_url:
            sep = "&" if "?" in start_url else "?"
            url = f"{start_url}{sep}start={(page-1)*20}"
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
        listings, pattern = parse_listings(soup)
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
            all_records.append({
                "name": name, "street": item.get("street", ""), "city": city,
                "state": "NC", "zip": "", "website": item.get("website", ""),
                "phone": item.get("phone", ""), "description": item.get("description", ""), "source_url": start_url,
            })
        log(f"  Page {page} — {new_count} new listings (pattern: {pattern})")
        if new_count == 0:
            break
        page += 1
        time.sleep(0.8)
    return all_records, js_detected


def resolve_website(record, source_domain):
    website = record.get("website", "")
    if not website:
        return record
    parsed = urlparse(website)
    if parsed.netloc and parsed.netloc != source_domain:
        return record
    SKIP = [source_domain, "facebook.com", "instagram.com", "twitter.com",
            "yelp.com", "google.com", "tripadvisor.com",
            "linkedin.com", "tiktok.com", "youtube.com", "pinterest.com"]
    SOCIAL = ["facebook.com", "instagram.com", "twitter.com", "x.com",
              "linkedin.com", "tiktok.com", "youtube.com", "pinterest.com",
              "m.facebook.com"]
    try:
        full_url = website if website.startswith("http") else f"https://{source_domain}{website}"
        r = requests.get(full_url, headers=HEADERS, timeout=10)
        soup = BeautifulSoup(r.text, "lxml")

        # Remove nav, footer, header before scanning
        for tag in soup.find_all(["nav", "footer", "header"]):
            tag.decompose()

        # Extract phone from tel: link if not already set
        if not record.get("phone"):
            for a in soup.find_all("a", href=True):
                if a["href"].startswith("tel:"):
                    phone = extract_phone(a["href"])
                    if phone:
                        record["phone"] = phone
                        break

        # Extract address from Google Maps query= param if not already set
        if not record.get("street"):
            from urllib.parse import parse_qs as _pqs, urlparse as _ulp, unquote as _uq
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if "google.com/maps" in href and "query=" in href:
                    qs = _pqs(_ulp(href).query)
                    query_val = qs.get("query", [""])[0]
                    if query_val:
                        # query= is "Street, City ST ZIP" — split off street
                        addr = _uq(query_val).split(",")[0].strip()
                        if addr and re.search(r"\d", addr):
                            record["street"] = addr
                    break

        # Fallback address: scan page text
        if not record.get("street"):
            for tag in soup.find_all(["p", "li", "div", "span", "address"]):
                text = tag.get_text(separator=" ", strip=True)
                if looks_like_address(text):
                    record["street"] = clean_address(text)
                    break
                elif len(text) > 120:
                    extracted = extract_address_from_text(text)
                    if extracted:
                        record["street"] = extracted
                        break

        # First pass: prefer labelled outbound links (real website)
        for a in soup.find_all("a", href=True):
            href = a["href"]
            href_domain = urlparse(href).netloc
            if not href_domain or any(s in href_domain for s in SKIP):
                continue
            label = a.get_text(strip=True).lower()
            if any(w in label for w in ["website", "visit", "www", "menu", "order", "book"]):
                record["website"] = href
                return record

        # Second pass: first clean outbound non-social link
        for a in soup.find_all("a", href=True):
            href = a["href"]
            href_domain = urlparse(href).netloc
            if not href_domain or any(s in href_domain for s in SKIP):
                continue
            if href.startswith("http"):
                record["website"] = href
                return record

        # Third pass: accept social link as website if nothing else found
        for a in soup.find_all("a", href=True):
            href = a["href"]
            href_domain = urlparse(href).netloc
            if href.startswith("http") and any(s in href_domain for s in SOCIAL):
                record["website"] = href
                return record

    except Exception as e:
        record["_resolve_error"] = str(e)[:80]
    return record


def resolve_all(records, source_domain, log=print):
    internal = [i for i, r in enumerate(records)
                if r.get("website") and urlparse(r["website"]).netloc in ("", source_domain)]
    if not internal:
        return records
    log(f"  Resolving {len(internal)} internal URLs...")
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
    return records


def scrape(url, log=print):
    log("Detecting site type...")
    if is_simpleview(url):
        log("SimpleView CMS detected — trying API (use playwright_scraper.py for best results)...")
        records = scrape_simpleview_api(url, log)
        return records, False
    records, js_detected = scrape_html(url, log)
    if not js_detected:
        records = resolve_all(records, urlparse(url).netloc, log)
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
    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_FIELDS)
        writer.writeheader()
        writer.writerows(records)
    print(f"\n✅ Done — {len(records)} records saved to {output_file}")
    print(f"   With address: {sum(1 for r in records if r.get('street'))}")
    print(f"   With website: {sum(1 for r in records if r.get('website'))}")


if __name__ == "__main__":
    main()