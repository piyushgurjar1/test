"""
URL enrichment via DuckDuckGo HTML scraping.

Design for anti-bot resilience:
- Fresh requests.Session per lookup (no cookie accumulation across queries)
- Rotating User-Agent pool (avoids fingerprinting)
- Strictly sequential processing (no concurrent bursts)
- Exponential backoff on failures
- Jittered delays between requests to look human
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


def _make_session() -> requests.Session:
    """Create a fresh session with randomized headers. No cookie carryover."""
    s = requests.Session()
    s.headers.update({
        "User-Agent": random.choice(_USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": random.choice([
            "en-US,en;q=0.9",
            "en-US,en;q=0.8",
            "en-GB,en;q=0.9,en-US;q=0.8",
        ]),
        "Accept-Encoding": "gzip, deflate",
        "Connection": "close",
        "Cache-Control": "no-cache",
        "DNT": "1",
    })
    return s


def _ddg_fetch_url_for_property(address: str, source: str) -> str | None:
    """Fetch a property URL from DuckDuckGo HTML search.

    Uses a FRESH session per call (no cookie accumulation).
    Retries up to 3 times with exponential backoff.
    """
    query = f"{address.strip()} {source.strip()}"
    url = f"https://html.duckduckgo.com/html/?q={requests.utils.quote(query)}"
    target = source.lower().replace(" ", "").replace(".com", "").replace("www.", "")

    for attempt in range(3):
        sess = _make_session()  # fresh session every attempt
        try:
            res = sess.get(url, timeout=15)

            if res.status_code in (202, 403):
                # DDG rate-limiting or blocking — back off hard
                wait = random.uniform(5.0 * (attempt + 1), 10.0 * (attempt + 1))
                _logger.warning(
                    "DDG returned %s for '%s' (attempt %d), backing off %.1fs",
                    res.status_code, address, attempt + 1, wait,
                )
                time.sleep(wait)
                continue

            if res.status_code != 200:
                _logger.debug("DDG status %s for '%s'", res.status_code, address)
                time.sleep(random.uniform(2.0, 5.0))
                continue

            soup = BeautifulSoup(res.text, "html.parser")
            results = soup.select("a.result__a")

            if not results:
                # Could be a CAPTCHA page or empty results
                _logger.debug("No DDG results for '%s' (attempt %d)", address, attempt + 1)
                time.sleep(random.uniform(3.0, 6.0))
                continue

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

            # Results found but no source match — don't retry
            _logger.debug("DDG results found but no '%s' match for '%s'", source, address)
            return None

        except requests.exceptions.Timeout:
            _logger.warning("DDG timeout for '%s' (attempt %d)", address, attempt + 1)
            time.sleep(random.uniform(3.0, 6.0))
        except Exception as e:
            _logger.warning("DDG error for '%s' (attempt %d): %s", address, attempt + 1, e)
            time.sleep(random.uniform(2.0, 5.0))
        finally:
            sess.close()  # ensure connection is fully closed

    return None


async def _run_enrich(data: dict) -> dict:
    """Core enrich logic — sequential with human-like delays.

    Delay kept at 3-8s per item so total stays well under
    Render's 30s request timeout for typical batch sizes.
    Anti-bot protection comes from fresh sessions + UA rotation,
    not from long delays.
    """
    items = data.get("clean_sold_comps", []) + data.get("clean_active_listings", [])

    results = []
    for i, item in enumerate(items):
        address = item.get("address")
        source  = item.get("source")

        if not address or not source:
            results.append({**item, "url": None})
            continue

        # ✅ 3-8s delay — human-like but safe within Render's timeout
        if i > 0:
            delay = random.uniform(3.0, 8.0)
            _logger.debug("Sleeping %.1fs before next lookup", delay)
            await asyncio.sleep(delay)

        url = await asyncio.to_thread(_ddg_fetch_url_for_property, address, source)
        results.append({**item, "url": url, "source_url": url})

        if url:
            _logger.info("Enriched: %s → %s", address, url)
        else:
            _logger.debug("No URL found: %s (source=%s)", address, source)

    return {"results": results}


@app.post("/enrich")
async def enrich(data: dict) -> dict:
    return await _run_enrich(data)
