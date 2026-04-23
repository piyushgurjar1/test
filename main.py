import asyncio
import random
from fastapi import FastAPI
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, parse_qs

app = FastAPI()

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml",
    "Connection": "keep-alive",
}

session = requests.Session()
session.headers.update(HEADERS)


def _ddg_fetch_url_for_property(address: str, source: str) -> str | None:
    query = f"{address.strip()} {source.strip()}"
    url = f"https://html.duckduckgo.com/html/?q={requests.utils.quote(query)}"

    for attempt in range(3):  # retry
        try:
            res = session.get(url, timeout=10)

            if res.status_code != 200:
                continue

            soup = BeautifulSoup(res.text, "html.parser")
            results = soup.select("a.result__a")

            if not results:
                continue

            target = source.lower().replace(" ", "").replace(".com", "")

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

        except Exception:
            pass

        # random backoff
        time_sleep = random.uniform(1.5, 4)
        asyncio.sleep(time_sleep)

    return None


async def process_item(item):
    address = item.get("address")
    source = item.get("source")

    if not address or not source:
        return {**item, "url": None}

    url = await asyncio.to_thread(
        _ddg_fetch_url_for_property, address, source
    )

    return {**item, "url": url}


@app.post("/enrich")
async def enrich(data: dict):
    items = data.get("clean_sold_comps", []) + data.get(
        "clean_active_listings", []
    )

    # limit concurrency (important for Render)
    semaphore = asyncio.Semaphore(3)

    async def sem_task(item):
        async with semaphore:
            await asyncio.sleep(random.uniform(1, 3))
            return await process_item(item)

    results = await asyncio.gather(*[sem_task(i) for i in items])

    return {"results": results}
