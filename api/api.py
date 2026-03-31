"""
Scraper API — FastAPI
Wraps the scraper logic and exposes it as an HTTP API.
Deploy to Fly.io and call from your Deno backend.

Endpoints:
    GET  /health          — liveness check
    POST /scrape          — scrape a URL, returns JSON records
"""

import os
import re
import csv
import time
import random
import secrets
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

from fastapi import FastAPI, HTTPException, Security, Depends
from fastapi.security.api_key import APIKeyHeader
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from common import (
    normalize_state, extract_phone, extract_address_from_text,
    clean_address, looks_like_address, _extract_best_description, resolve_detail_page,
    STATE_ABBR, DOMAIN_STATE, HEADERS,
    fetch_soup, detect_city, is_simpleview, scrape_simpleview_api,
    parse_listings, resolve_website, resolve_all, scrape_html,
    MAX_PAGES, OUTPUT_FIELDS,
)

# ── App setup ─────────────────────────────────────────────────────────────────

app = FastAPI(
    title="Scraper API",
    description="Scrapes directory-style listing pages and returns structured JSON.",
    version="1.0.0",
)

# Allow your Deno app's origin — set via env var or allow all for dev
ALLOWED_ORIGINS = os.environ.get("ALLOWED_ORIGINS", "*").split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

# ── API key auth ──────────────────────────────────────────────────────────────
# Set API_KEY env var in Fly.io secrets: fly secrets set API_KEY=your-secret-here
# If not set, auth is disabled (useful for local dev)

API_KEY = os.environ.get("API_KEY")
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def verify_api_key(key: Optional[str] = Security(api_key_header)):
    if API_KEY is None:
        return  # Auth disabled — no key configured
    if key != API_KEY:
        raise HTTPException(status_code=403, detail="Invalid or missing API key")


# ── Request / response models ─────────────────────────────────────────────────

class ScrapeRequest(BaseModel):
    url: str
    timeout: int = 60  # max seconds to spend scraping (not enforced hard, just a hint)


class ScrapeRecord(BaseModel):
    name: str
    street: str = ""
    city: str = ""
    state: str = ""
    zip: str = ""
    phone: str = ""
    website: str = ""
    description: str = ""
    source_url: str = ""


class ScrapeResponse(BaseModel):
    success: bool
    url: str
    count: int
    js_detected: bool
    records: list[ScrapeRecord]
    message: str = ""


# ── Scraper logic (same as scraper.py) ───────────────────────────────────────





def enrich_with_places(records, location_hint, api_key):
    """
    For each record missing street/city/zip/phone/website, look it up
    by name on Google Places and fill in the gaps.
    """
    import time as _time

    BASE_URL = "https://maps.googleapis.com/maps/api/place"

    def _text_search(query):
        r = requests.get(f"{BASE_URL}/textsearch/json",
                         params={"query": query, "key": api_key}, timeout=10)
        data = r.json()
        return data.get("results", [])

    def _place_details(place_id):
        r = requests.get(f"{BASE_URL}/details/json",
                         params={"place_id": place_id,
                                 "fields": "name,formatted_address,formatted_phone_number,website,url,types",
                                 "key": api_key}, timeout=10)
        data = r.json()
        result = data.get("result", {})
        # Log first record's raw response for debugging
        if not hasattr(_place_details, "_logged"):
            _place_details._logged = True
            print(f"[Places debug] status={data.get('status')} keys={list(result.keys())}")
        return result

    def _parse_addr(formatted):
        # "123 Main St, City, ST 12345, USA"
        import re as _re
        m = _re.search(r'^(.+?),\s*([^,]+),\s*([A-Z]{2})\s+(\d{5})', formatted or "")
        if m:
            return m.group(1).strip(), m.group(2).strip(), m.group(3), m.group(4)
        return "", "", "", ""

    enriched = 0
    for record in records:
        needs = (not record.get("street") or not record.get("city") or
                 not record.get("zip") or not record.get("phone") or
                 not record.get("website"))
        if not needs:
            continue
        name = record.get("name", "").strip()
        if not name:
            continue
        try:
            results = _text_search(f"{name} {location_hint}".strip())
            if not results:
                continue
            place_id = results[0].get("place_id")
            if not place_id:
                continue
            details = _place_details(place_id)
            street, city, state, zip_code = _parse_addr(
                details.get("formatted_address") or
                results[0].get("formatted_address", ""))
            if not record.get("street") and street:
                record["street"] = street
            if not record.get("city") and city:
                record["city"] = city
            if not record.get("state") and state:
                record["state"] = state
            if not record.get("zip") and zip_code:
                record["zip"] = zip_code
            if not record.get("phone") and details.get("formatted_phone_number"):
                record["phone"] = details["formatted_phone_number"]
            if not record.get("website") and details.get("website"):
                record["website"] = details["website"]
            enriched += 1
        except Exception:
            pass
        _time.sleep(0.05)

    return records, enriched


def run_scrape(url):
    import re as _re3

    def _name_to_slug(name):
        s = name.lower()
        s = _re3.sub(r"[\u2018\u2019\u0027\u0060]", "", s)
        s = _re3.sub(r"[^a-z0-9]+", "-", s)
        return s.strip("-")

    DETAIL_URL_BUILDERS = {}

    if is_simpleview(url):
        records = scrape_simpleview_api(url)
        return records, False, "SimpleView CMS — used direct API (may be incomplete; use Playwright locally for full results)"
    records, js_detected = scrape_html(url)
    if js_detected:
        return [], True, "JavaScript-rendered page — no listings found in raw HTML"

    _source_netloc = urlparse(url).netloc.replace("www.", "")
    _builder = DETAIL_URL_BUILDERS.get(_source_netloc)
    if _builder:
        for r in records:
            if not r.get("website") and r.get("name"):
                r["website"] = _builder(r["name"], url)

    records = resolve_all(records, urlparse(url).netloc)

    # State backfill
    _inferred_state = DOMAIN_STATE.get(_source_netloc, "")
    if _inferred_state:
        for r in records:
            if not r.get("state"):
                r["state"] = _inferred_state

    # Google Places enrichment — runs automatically when API key is configured
    places_key = os.environ.get("GOOGLE_PLACES_API_KEY")
    if places_key and records:
        # Build location hint from state/city backfill or domain
        cities  = [r.get("city","")  for r in records if r.get("city")]
        states  = [r.get("state","") for r in records if r.get("state")]
        loc = f"{cities[0]} {states[0]}".strip() if cities and states else _source_netloc
        records, enriched = enrich_with_places(records, loc, places_key)

    return records, False, ""


# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/scrape", response_model=ScrapeResponse)
async def scrape(
    body: ScrapeRequest,
    _: None = Depends(verify_api_key),
):
    url = body.url.strip()
    if not url.startswith("http"):
        url = "https://" + url

    try:
        records, js_detected, message = run_scrape(url)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Scrape failed: {str(e)}")


    # Ensure all records have all fields
    clean = []
    for r in records:
        clean.append(ScrapeRecord(
            name=r.get("name", ""),
            street=r.get("street", ""),
            city=r.get("city", ""),
            state=r.get("state", ""),
            zip=r.get("zip", ""),
            website=r.get("website", ""),
            description=r.get("description", ""),
            source_url=r.get("source_url", url),
        ))

    if js_detected:
        message = (
            "This page renders its listings via JavaScript and cannot be scraped "
            "through the API. Use the command-line scraper instead: "
            "`scrape-js <url>` (requires Playwright). "
            "The API only handles server-rendered HTML pages."
        )

    return ScrapeResponse(
        success=not js_detected,
        url=url,
        count=len(clean),
        js_detected=js_detected,
        records=clean,
        message=message,
    )