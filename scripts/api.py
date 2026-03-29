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





def run_scrape(url):
    import re as _re3

    def _name_to_slug(name):
        s = name.lower()
        s = _re3.sub(r"[\u2018\u2019\u0027\u0060]", "", s)
        s = _re3.sub(r"[^a-z0-9]+", "-", s)
        return s.strip("-")

    DETAIL_URL_BUILDERS = {}
    # Detail URL builders would go here for sites where detail page URLs
    # cannot be captured from card links and must be constructed from name.
    # Currently empty — all known sites have capturable links.

    if is_simpleview(url):
        records = scrape_simpleview_api(url)
        return records, False, "SimpleView CMS — used direct API (may be incomplete; use Playwright locally for full results)"
    records, js_detected = scrape_html(url)
    if js_detected:
        return [], True, "JavaScript-rendered page — no listings found in raw HTML"

    # Construct detail URLs for sites where they're derivable from name
    _source_netloc = urlparse(url).netloc.replace("www.", "")
    _builder = DETAIL_URL_BUILDERS.get(_source_netloc)
    if _builder:
        for r in records:
            if not r.get("website") and r.get("name"):
                r["website"] = _builder(r["name"], url)

    records = resolve_all(records, urlparse(url).netloc)

    # State backfill — infer from source domain if records are missing state
    _source_netloc = urlparse(url).netloc.replace("www.", "")
    _inferred_state = DOMAIN_STATE.get(_source_netloc, "")
    if _inferred_state:
        for r in records:
            if not r.get("state"):
                r["state"] = _inferred_state

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