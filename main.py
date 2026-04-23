"""
URL enrichment via DuckDuckGo HTML scraping.

Design for anti-bot resilience:
- Persistent session (cookies preserved, looks like a real browser)
- Rotating User-Agent per request (avoid fingerprinting)
- Keep-Alive connection (DDG requires it)
- Strictly sequential processing (no concurrent bursts)
- Exponential backoff on failures
- Jittered delays between requests
"""

import asyncio
import logging
import random
import time

import requests
from bs4 import BeautifulSoup
from fastapi import FastAPI
from urllib.parse import urljoin, urlparse, parse_qs

app = FastAPI()
_logger = logging.getLogger(__name__)

# ── User-Agent rotation pool ─────────────────────────────────────────────────
_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (X11; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0",
]

# ── Single persistent session (cookies survive across requests) ─────────────
_session = None

def _get_session() -> requests.Session:
    global _session
    if _session is None:
        _session = requests.Session()
        # Base headers – keep-alive is CRITICAL for DDG
        _session.headers.update({
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive",   # ← fixed: persistent connection
            "DNT": "1",
        })
    return _session


def _ddg_fetch_url_for_property(address: str, source: str) -> str | None:
    """Fetch a property URL from DuckDuckGo HTML search.

    Uses a persistent session (cookies accumulate naturally).
    Rotates User-Agent per request.
    Retries with exponential backoff on failure.
    """
    query = f"{address.strip()} {source.strip()}"
    url = f"https://html.duckduckgo.com/html/?q={requests.utils.quote(query)}"
    # Clean source for domain matching (removes spaces, .com, www.)
    target = source.lower().replace(" ", "").replace(".com", "").replace("www.", "")

    sess = _get_session()

    for attempt in range(3):
        # Rotate User-Agent per request (avoid fingerprinting)
        sess.headers["User-Agent"] = random.choice(_USER_AGENTS)

        try:
            resp = sess.get(url, timeout=15)

            # Rate limiting / blocking – exponential backoff
            if resp.status_code in (202, 403):
                wait = random.uniform(3.0 * (attempt + 1), 8.0 * (attempt + 1))
                _logger.warning("DDG %s for '%s' (attempt %d), backoff %.1fs",
                                resp.status_code, address, attempt+1, wait)
                time.sleep(wait)
                continue

            if resp.status_code != 200:
                _logger.debug("DDG status %s for '%s'", resp.status_code, address)
                time.sleep(random.uniform(2.0, 4.0))
                continue

            soup = BeautifulSoup(resp.text, "html.parser")
            results = soup.select("a.result__a")

            if not results:
                _logger.debug("No DDG results for '%s' (attempt %d)", address, attempt+1)
                time.sleep(random.uniform(3.0, 6.0))
                continue

            # Extract actual URL from DDG's redirect wrapper
            for a in results:
                raw = a.get("href")
                if not raw:
                    continue
                full_url = urljoin("https://duckduckgo.com", raw)
                parsed = urlparse(full_url)
                actual_url = parse_qs(parsed.query).get("uddg", [None])[0]
                if not actual_url:
                    continue

                domain = urlparse(actual_url).netloc.lower()
                if target in domain:
                    return actual_url

            # Found results but none match the source – don't retry
            _logger.debug("DDG results but no '%s' match for '%s'", source, address)
            return None

        except requests.exceptions.Timeout:
            _logger.warning("DDG timeout for '%s' (attempt %d)", address, attempt+1)
            time.sleep(random.uniform(2.0, 5.0))
        except Exception as e:
            _logger.warning("DDG error for '%s' (attempt %d): %s", address, attempt+1, e)
            time.sleep(random.uniform(1.5, 4.0))

    return None


async def _run_enrich(data: dict) -> dict:
    """Sequential enrichment with human-like delays (3–8s between items)."""
    items = data.get("clean_sold_comps", []) + data.get("clean_active_listings", [])

    results = []
    for i, item in enumerate(items):
        address = item.get("address")
        source = item.get("source")

        if not address or not source:
            results.append({**item, "url": None})
            continue

        # Human-like delay between lookups (prevents bursts)
        if i > 0:
            delay = random.uniform(3.0, 8.0)
            _logger.debug("Sleeping %.1fs before next lookup", delay)
            await asyncio.sleep(delay)

        url = await asyncio.to_thread(_ddg_fetch_url_for_property, address, source)
        results.append({**item, "url": url})

        if url:
            _logger.info("Enriched: %s → %s", address, url)
        else:
            _logger.debug("No URL found: %s (source=%s)", address, source)

    return {"results": results}


@app.post("/enrich")
async def enrich(data: dict) -> dict:
    return await _run_enrich(data)
