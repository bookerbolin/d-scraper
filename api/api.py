"""
Scraper API — FastAPI
Wraps the scraper logic and exposes it as an HTTP API.
Deploy to Fly.io and call from your Deno backend.

Endpoints:
    GET  /health             — liveness check
    POST /scrape             — start a scrape job, returns job_id immediately
    GET  /job/{job_id}       — poll for job status/results
"""

import os
import re
import time
import uuid
import threading
import requests
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, Literal

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

# ── App setup ──────────────────────────────────────────────────────────────────

app = FastAPI(title="Scraper API", version="2.0.0")

ALLOWED_ORIGINS = os.environ.get("ALLOWED_ORIGINS", "*").split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

# ── Auth ───────────────────────────────────────────────────────────────────────

API_KEY = os.environ.get("API_KEY")
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def verify_api_key(key: Optional[str] = Security(api_key_header)):
    if API_KEY is None:
        return
    if key != API_KEY:
        raise HTTPException(status_code=403, detail="Invalid or missing API key")


# ── Job queue ──────────────────────────────────────────────────────────────────

JOBS: dict = {}
JOB_TTL = 600  # 10 minutes


def _prune_jobs():
    now = time.time()
    expired = [jid for jid, j in list(JOBS.items()) if now - j["created_at"] > JOB_TTL]
    for jid in expired:
        del JOBS[jid]


# ── Models ─────────────────────────────────────────────────────────────────────

class ScrapeRequest(BaseModel):
    url: str
    location: str = ""
    timeout: int = 60


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


class JobStarted(BaseModel):
    job_id: str
    status: str = "pending"
    message: str = ""


class JobStatus(BaseModel):
    job_id: str
    status: Literal["pending", "running", "done", "error"]
    url: str = ""
    count: int = 0
    js_detected: bool = False
    records: list[ScrapeRecord] = []
    message: str = ""


# ── Places enrichment ──────────────────────────────────────────────────────────

def enrich_with_places(records, location_hint, api_key):
    BASE_URL = "https://maps.googleapis.com/maps/api/place"

    def _text_search(query):
        r = requests.get(f"{BASE_URL}/textsearch/json",
                         params={"query": query, "key": api_key}, timeout=10)
        return r.json().get("results", [])

    def _place_details(place_id):
        r = requests.get(f"{BASE_URL}/details/json",
                         params={"place_id": place_id,
                                 "fields": "name,formatted_address,formatted_phone_number,website,url,types",
                                 "key": api_key}, timeout=10)
        data = r.json()
        result = data.get("result", {})
        if not hasattr(_place_details, "_logged"):
            _place_details._logged = True
            print(f"[Places debug] status={data.get('status')} keys={list(result.keys())}")
        return result

    def _parse_addr(formatted):
        m = re.search(r'^(.+?),\s*([^,]+),\s*([A-Z]{2})\s+(\d{5})', formatted or "")
        if m:
            return m.group(1).strip(), m.group(2).strip(), m.group(3), m.group(4)
        return "", "", "", ""

    hint_state = ""
    ms = re.search(r'\b([A-Z]{2})\b', location_hint.upper())
    if ms:
        hint_state = ms.group(1)

    needs_count = sum(1 for r in records if not r.get("street") or not r.get("city") or
                      not r.get("zip") or not r.get("phone") or not r.get("website"))
    print(f"[Places debug] {len(records)} records, {needs_count} need enrichment")

    def _enrich_one(record):
        needs = (not record.get("street") or not record.get("city") or
                 not record.get("zip") or not record.get("phone") or
                 not record.get("website"))
        if not needs:
            return 0
        name = record.get("name", "").strip()
        if not name:
            return 0
        try:
            results = _text_search(f"{name} {location_hint}".strip())
            if not results:
                return 0
            place_id = results[0].get("place_id")
            if not place_id:
                return 0
            details = _place_details(place_id)
            street, city, state, zip_code = _parse_addr(
                details.get("formatted_address") or results[0].get("formatted_address", ""))
            if hint_state and state and state != hint_state:
                return 0
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
            return 1
        except Exception:
            return 0

    with ThreadPoolExecutor(max_workers=10) as pool:
        futures = [pool.submit(_enrich_one, r) for r in records]
        enriched = sum(f.result() for f in futures)

    return records, enriched


# ── Core scrape logic ──────────────────────────────────────────────────────────

def run_scrape(url, location_hint=""):
    DETAIL_URL_BUILDERS = {}

    if is_simpleview(url):
        records = scrape_simpleview_api(url)
        return records, False, "SimpleView CMS — used direct API"

    records, js_detected = scrape_html(url)
    if js_detected:
        return [], True, "JavaScript-rendered page — use scrape-js CLI instead"

    _source_netloc = urlparse(url).netloc.replace("www.", "")
    _builder = DETAIL_URL_BUILDERS.get(_source_netloc)
    if _builder:
        for r in records:
            if not r.get("website") and r.get("name"):
                r["website"] = _builder(r["name"], url)

    records = resolve_all(records, urlparse(url).netloc)

    _inferred_state = DOMAIN_STATE.get(_source_netloc, "")
    if _inferred_state:
        for r in records:
            if not r.get("state"):
                r["state"] = _inferred_state

    places_key = os.environ.get("GOOGLE_PLACES_API_KEY")
    if places_key and records:
        if not location_hint:
            cities = [r.get("city", "") for r in records if r.get("city")]
            states = [r.get("state", "") for r in records if r.get("state")]
            location_hint = f"{cities[0]} {states[0]}".strip() if cities and states else _source_netloc
        records, _ = enrich_with_places(records, location_hint, places_key)

    return records, False, ""


def _make_clean_records(records, url):
    return [ScrapeRecord(
        name=r.get("name", ""),
        street=r.get("street", ""),
        city=r.get("city", ""),
        state=r.get("state", ""),
        zip=r.get("zip", ""),
        phone=r.get("phone", ""),
        website=r.get("website", ""),
        description=r.get("description", ""),
        source_url=r.get("source_url", url),
    ) for r in records]


def _run_job(job_id, url, location_hint):
    """Runs in a background thread. Updates JOBS dict as it progresses."""
    JOBS[job_id]["status"] = "running"
    try:
        records, js_detected, message = run_scrape(url, location_hint=location_hint)
        clean = _make_clean_records(records, url)
        if js_detected:
            message = (
                "This page renders its listings via JavaScript and cannot be scraped "
                "through the API. Use scrape-js CLI instead."
            )
        JOBS[job_id].update({
            "status": "done",
            "records": clean,
            "count": len(clean),
            "js_detected": js_detected,
            "message": message,
        })
    except Exception as e:
        JOBS[job_id].update({
            "status": "error",
            "message": str(e),
        })


# ── Endpoints ──────────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/scrape", response_model=JobStarted)
async def scrape(
    body: ScrapeRequest,
    _: None = Depends(verify_api_key),
):
    """Start a scrape job. Returns job_id immediately — poll /job/{job_id} for results."""
    url = body.url.strip()
    if not url.startswith("http"):
        url = "https://" + url

    _prune_jobs()

    job_id = str(uuid.uuid4())
    JOBS[job_id] = {
        "job_id": job_id,
        "status": "pending",
        "url": url,
        "location": body.location.strip(),
        "records": [],
        "count": 0,
        "js_detected": False,
        "message": "",
        "created_at": time.time(),
    }

    t = threading.Thread(target=_run_job, args=(job_id, url, body.location.strip()), daemon=True)
    t.start()

    return JobStarted(job_id=job_id, status="pending", message="Scrape started")


@app.get("/job/{job_id}", response_model=JobStatus)
async def get_job(
    job_id: str,
    _: None = Depends(verify_api_key),
):
    """Poll for job status. Status progresses: pending → running → done | error."""
    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found or expired")

    return JobStatus(
        job_id=job_id,
        status=job["status"],
        url=job.get("url", ""),
        count=job.get("count", 0),
        js_detected=job.get("js_detected", False),
        records=job.get("records", []),
        message=job.get("message", ""),
    )