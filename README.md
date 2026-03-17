# scraper

A web scraper that extracts structured business listings from tourism and directory websites. Deployed as a FastAPI service on Fly.io and called from a Deno backend via HTTP.

---

## Repo structure

```
scraper/
  api/
    api.py              # FastAPI app — scraping logic + HTTP endpoints
    requirements.txt    # Python dependencies
    Dockerfile          # Uses python:3.12 (full image, not slim)
    fly.toml            # Fly.io deployment config
  scripts/
    scraper.py          # Local CLI tool for one-off scrapes
    playwright_scraper.py  # Playwright-based scraper for JS-rendered sites
    merge_scrapes.py    # Diff/merge tool for comparing two scrape runs
  clients/
    scraper-client.ts   # Deno/TypeScript client for calling the API
                        # Copy this into your Deno app when ready
  data/
    .gitkeep            # CSVs from playwright_scraper.py save here (gitignored)
```

---

## API

### Base URL
```
https://daytrip-web-scraper.fly.dev
```

### Authentication
All requests require an `X-API-Key` header matching the `API_KEY` secret set in Fly.io.

### Endpoints

#### `GET /health`
Liveness check. Returns `{"status": "ok"}`.

#### `POST /scrape`
Scrape a directory URL and return structured business records.

**Request body:**
```json
{ "url": "https://downtowndurham.com/dine-drink/" }
```

**Response:**
```json
{
  "success": true,
  "url": "https://downtowndurham.com/dine-drink/",
  "count": 176,
  "js_detected": false,
  "records": [
    {
      "name": "Alley Twenty Six",
      "street": "320 E Chapel Hill St",
      "city": "Durham",
      "state": "NC",
      "zip": "27701",
      "phone": "(919) 680-0570",
      "website": "https://alleytwentysix.com",
      "description": "",
      "source_url": "https://downtowndurham.com/dine-drink/"
    }
  ],
  "message": ""
}
```

**Fields:**

| Field | Description |
|---|---|
| `success` | `false` if JS rendering was detected and no listings found |
| `js_detected` | `true` if the page requires JavaScript to render listings |
| `count` | Number of records returned |
| `records` | Array of business objects |
| `message` | Human-readable note (e.g. SimpleView API limitation warning) |

---

## Calling from Deno

A reference client lives in `clients/scraper-client.ts`. Copy it into your Deno app and import from there.

```typescript
import { callScraper } from "./scraper-client.ts";

const result = await callScraper("https://downtowndurham.com/dine-drink/");
if (result.success) {
  console.log(`Got ${result.count} records`);
}
```

Set these environment variables in your Deno app:
```
SCRAPER_API_URL=https://daytrip-web-scraper.fly.dev
SCRAPER_API_KEY=your-secret-key
```

---

## Supported URL patterns

The scraper auto-detects the site structure and applies the appropriate pattern. No configuration needed — just pass the URL.

### Pattern 1 — Durham downtown style
**Sites:** `downtowndurham.com`
**Structure:** `<h2><a href="/directory/...">` + next `<ul><li>` siblings
**Extracts:** name, street (from li text), phone (from `tel:` link in li)
**Pagination:** `?pg=N`

### Pattern 2 — Chapel Hill / Wix style
**Sites:** `downtownchapelhill.com`
**Structure:** `<h3>` name + sibling "Visit Website" `<a>` link
**Extracts:** name, website
**Pagination:** `?pg=N`

### Pattern 3 — Hillsborough style
**Sites:** `visithillsboroughnc.com`
**Structure:** `<a>` wrapping `<h3>`
**Extracts:** name, website, description
**Pagination:** `?pg=N`

### Pattern 4 — Chamber of Commerce / GrowthZone style
**Sites:** `business.carolinachamber.org` and other GrowthZone/ChamberMaster directories
**Structure:** `<h5><a>` + next `<ul><li>` with Google Maps link, `tel:` link, Visit Website link
**Extracts:** name, street, phone, website, description
**Pagination:** single page (all results in one HTML response)
**URL pattern:** `business.[chamber].org/local-business-directory/Search/[category]`

### Pattern 5 — Editorial prose style
**Sites:** `visitwilmingtonnc.com`, `discoverdurham.com/food-drink/restaurants/`
**Structure:** `<strong>` name + sibling text lines for address/phone, or multi-line `<br>`-separated blocks
**Extracts:** name, street, city, state, zip (when on separate line), phone, website
**Pagination:** `?page=N` for discoverdurham.com, `?pg=N` for others

### Pattern 6 — Blog/article listing style
**Sites:** `discoverdurham.com/food-drink/restaurants/new-restaurants/` and similar editorial pages
**Structure:** `<h3><a href="website">` name + `<p><strong>` address + `<p>` description
**Extracts:** name, street, phone, website, description
**Pagination:** single page (editorial articles, not paginated databases)

### Pattern 7 — Card-link style
**Sites:** `ncwine.org/wineries/wineries-main/`
**Structure:** `<a href="/detail/">` wrapping `<h2>` name + `<p>` address + `<p>` description
**Extracts:** name, street, city, state, zip, website (detail URL), description
**Pagination:** single page (JS filter operates on pre-loaded HTML)

### Pattern 8 — JBusiness Directory style
**Sites:** `charleston.com/eating-and-drinking/restaurants`
**Structure:** `<a href="*/businesses/*"><h3>` name + sibling `<p>` for location
**Extracts:** name, city, detail URL (resolution pass fetches street/phone/real website)
**Pagination:** `?start=20`, `?start=40` etc. (Joomla pagination)
**Note:** Performs a parallel resolution pass on detail pages to get full address/phone/website

### Pattern 9 — Plain h3 editorial prose style
**Sites:** `gosouthsavannah.com` and similar static HTML travel guides
**Structure:** `<h3>` plain text name (no link inside) + sibling address text + sibling `<a>` for website + `<p>` description
**Extracts:** name, street, phone, website, description
**Pagination:** single page

### SimpleView CMS
**Sites:** `visitchapelhill.org`, `visitraleigh.com`, `visitdurham.com`, `visitcary.org`
**Detection:** Domain name match or `/includes/public/assets/` in HTML
**Method:** Direct API call to `/includes/rest_v2/plugins_listings_listings/find/`
**Limitation:** Returns empty from the hosted API — these sites block server-to-server requests. Use `playwright_scraper.py` locally for full results.

---

## Sites known to work via API

| URL | Pattern | Notes |
|---|---|---|
| `downtowndurham.com/dine-drink/` | 1 | ~176 listings |
| `downtownchapelhill.com/bars-restaurants` | 2 | ~88 listings |
| `visithillsboroughnc.com/places_to_eat_categories/american/` | 3 | |
| `business.carolinachamber.org/local-business-directory/Search/food-beverage-391819` | 4 | 74 listings |
| `visitwilmingtonnc.com/coffee-shops-wilmington-nc/` | 5 | |
| `discoverdurham.com/food-drink/restaurants/` | 5 | 534 listings, 45 pages |
| `discoverdurham.com/food-drink/restaurants/new-restaurants/` | 6 | ~25 listings, no pagination |
| `ncwine.org/wineries/wineries-main/` | 7 | All NC wineries, single page |
| `charleston.com/eating-and-drinking/restaurants` | 8 | 30 listings, resolution pass for address |
| `gosouthsavannah.com/historic-district-and-city/historic-architecture/places-of-worship.html` | 9 | Static HTML travel guide |

## Sites known to work via Playwright (CLI only)

| URL | Method | Notes |
|---|---|---|
| `visitchapelhill.org` | SimpleView XHR intercept | ~180 listings |
| `visitraleigh.com` | SimpleView XHR intercept | ~700+ listings |
| `exploregeorgia.org/spas-wellness` | DOM + detail page resolution | 63 listings |
| `visitsavannah.com/savannahs-best-restaurants` | Drupal Views AJAX | ~244 listings |
| `charlestoncvb.com` | DOM pagination | bot detection may interfere |

## Sites that require Playwright and need further work

| URL | Reason |
|---|---|
| `exploreasheville.com` | JS-rendered, no API endpoint found |
| `explorebrevard.com` | JS-rendered + Load More AJAX |

---

## Output fields

All records use this schema:

| Field | Description |
|---|---|
| `name` | Business name |
| `street` | Street address |
| `city` | City |
| `state` | State (2-letter) |
| `zip` | ZIP code |
| `phone` | Phone number, normalized to `(XXX) XXX-XXXX` |
| `website` | Business website URL |
| `description` | Short description if available |
| `source_url` | The URL that was scraped |

---

## Adding a new URL pattern

1. Fetch the URL and inspect the HTML structure
2. Identify the repeating element that marks each listing (heading tag, wrapper div, etc.)
3. Add a new `# Pattern N` block inside `parse_listings()` in `api.py`, before the final `return [], None`
4. Follow the existing pattern structure — return `records, "pattern-name-style"`
5. Test locally: `python3 -c "from api import run_scrape; r,_,_ = run_scrape('https://...'); print(len(r), r[0])"`
6. Push to GitHub — Fly.io auto-deploys

**Common patterns to look for:**
- Heading tag (`h2`–`h5`) containing or followed by the name
- Address in a Google Maps link, `tel:` link, or plain text
- "Visit Website" anchor with the real URL
- Repeating `<ul><li>` or `<p>` blocks per listing

**For JS-rendered sites:** Open DevTools → Network → XHR/Fetch, interact with the page, and look for an API endpoint firing. If it's a SimpleView, Drupal Views, or other known CMS, add the domain to the appropriate handler in `playwright_scraper.py`.

---

## Local development

### Run the API locally
```bash
cd api
pip install -r requirements.txt
uvicorn api:app --reload --port 8080
```

### CLI scraper (plain HTML sites)
```bash
cd scripts
python3 scraper.py https://downtowndurham.com/dine-drink/
# Outputs CSV to scripts/ folder
```

### Playwright scraper (JS-rendered sites)
```bash
cd scripts
pip install playwright --break-system-packages
playwright install chromium
python3 playwright_scraper.py https://visitchapelhill.org/things-to-do
# Outputs CSV to ../data/ folder
```

### Merge two scrape runs
```bash
cd scripts
python3 merge_scrapes.py old_listings.csv new_listings.csv
# Outputs: _merged_DATE.csv, _added_DATE.csv, _removed_DATE.csv, _changed_DATE.csv
```

---

## Deployment

### First deploy
```bash
cd api
fly auth login
fly launch --no-deploy
fly secrets set API_KEY=your-secret-key-here
fly deploy
```

### Auto-deploy via GitHub
1. Push the repo to GitHub
2. In the Fly.io dashboard → your app → Settings → GitHub Integration
3. Connect the repo, set the app root to `api/`
4. Every push to `main` triggers a new deploy

### Secrets
```bash
fly secrets set API_KEY=your-secret-key -a daytrip-web-scraper
fly secrets set ALLOWED_ORIGINS=https://your-deno-app.com -a daytrip-web-scraper
```

---

## Adding Playwright support to the API (future)

Currently the API cannot scrape JS-rendered sites (SimpleView, etc.) because it runs stateless Python without a browser. To enable this:

1. Switch the Dockerfile base to `mcr.microsoft.com/playwright/python:v1.x.x`
2. Add `playwright` to `requirements.txt`
3. Add an async `/scrape-js` endpoint that spins up Chromium per request
4. Scale the Fly.io machine to at least 1GB RAM (`[[vm]] memory = "1gb"`)
5. Estimated cost: ~$3–8/month with `auto_stop_machines = true`