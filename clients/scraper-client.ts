/**
 * scraper-client.ts
 * Deno/TypeScript client for the scraper API.
 * Import this into your Deno backend and call scrapeUrl() or previewUrl().
 */

const SCRAPER_API_URL = Deno.env.get("SCRAPER_API_URL") ?? "https://scraper-api.fly.dev";
const SCRAPER_API_KEY = Deno.env.get("SCRAPER_API_KEY") ?? "";

// ── Types ─────────────────────────────────────────────────────────────────────

export interface ScrapeRecord {
  name: string;
  street: string;
  city: string;
  state: string;
  zip: string;
  phone: string;
  website: string;
  description: string;
  source_url: string;
}

export interface ScrapeResponse {
  success: boolean;
  url: string;
  count: number;
  records: ScrapeRecord[];
  js_detected: boolean;
  message?: string;
}

export interface PreviewResponse {
  success: boolean;
  url: string;
  is_simpleview: boolean;
  js_detected: boolean;
  sample: ScrapeRecord[];
  available_fields: Record<string, string>; // field -> "85%"
  message?: string;
}

// ── Client ────────────────────────────────────────────────────────────────────

function headers(): HeadersInit {
  const h: HeadersInit = { "Content-Type": "application/json" };
  if (SCRAPER_API_KEY) h["X-API-Key"] = SCRAPER_API_KEY;
  return h;
}

/**
 * Scrape a URL and return all listings.
 *
 * @example
 * const result = await scrapeUrl("https://downtowndurham.com/dine-drink/");
 * if (result.success) {
 *   console.log(`Got ${result.count} listings`);
 *   for (const record of result.records) {
 *     console.log(record.name, record.street, record.website);
 *   }
 * }
 */
export async function scrapeUrl(url: string): Promise<ScrapeResponse> {
  const response = await fetch(`${SCRAPER_API_URL}/scrape`, {
    method: "POST",
    headers: headers(),
    body: JSON.stringify({ url }),
    // Scrapes can take up to 60 seconds
    signal: AbortSignal.timeout(90_000),
  });

  if (!response.ok) {
    const error = await response.text();
    throw new Error(`Scraper API error ${response.status}: ${error}`);
  }

  return response.json() as Promise<ScrapeResponse>;
}

/**
 * Preview a URL — returns a 3-record sample and field coverage stats.
 * Useful for showing users what data is available before a full scrape.
 *
 * @example
 * const preview = await previewUrl("https://downtowndurham.com/dine-drink/");
 * console.log(preview.available_fields);
 * // { street: "100%", website: "100%", description: "0%", ... }
 */
export async function previewUrl(url: string): Promise<PreviewResponse> {
  const response = await fetch(`${SCRAPER_API_URL}/scrape/preview`, {
    method: "POST",
    headers: headers(),
    body: JSON.stringify({ url }),
    signal: AbortSignal.timeout(90_000),
  });

  if (!response.ok) {
    const error = await response.text();
    throw new Error(`Scraper API error ${response.status}: ${error}`);
  }

  return response.json() as Promise<PreviewResponse>;
}

/**
 * Check if the scraper API is reachable.
 */
export async function healthCheck(): Promise<boolean> {
  try {
    const response = await fetch(`${SCRAPER_API_URL}/health`, {
      signal: AbortSignal.timeout(5_000),
    });
    return response.ok;
  } catch {
    return false;
  }
}
