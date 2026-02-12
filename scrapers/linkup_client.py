"""
Shared Linkup API client.

Every other module uses this "phone" to call the Linkup service.
Provides: fetch, search, structured search, and first-URL search.
"""

from __future__ import annotations

import random
import time
import logging
from urllib.parse import urlparse

import requests

from scrapers.config import MAX_RETRIES_PER_REQUEST

log = logging.getLogger(__name__)

# Global 429 counter — if too many in a row, we stop hammering the API
_consecutive_429s = 0
_MAX_CONSECUTIVE_429 = 5


class RateLimitExhausted(Exception):
    """Raised when the API returns 429 too many times in a row."""
    pass


def _post_with_retry(
    session: requests.Session,
    url: str,
    *,
    json: dict,
    headers: dict,
    timeout: int,
    label: str = "",
) -> requests.Response:
    """POST with exponential backoff on 429/5xx/timeouts.

    Returns the response on success.
    Raises requests.RequestException on permanent failure.
    Raises RateLimitExhausted if 429 limit exceeded.
    """
    global _consecutive_429s

    last_exc = None
    for attempt in range(MAX_RETRIES_PER_REQUEST + 1):
        try:
            resp = session.post(url, json=json, headers=headers, timeout=timeout)

            # 429 — rate limited
            if resp.status_code == 429:
                _consecutive_429s += 1
                if _consecutive_429s >= _MAX_CONSECUTIVE_429:
                    raise RateLimitExhausted(
                        f"Linkup API returned 429 {_consecutive_429s} times in a row"
                    )
                wait = int(resp.headers.get("Retry-After", 10))
                wait = min(wait, 60)  # cap at 60s
                log.warning("429 rate-limited (%s). Waiting %ds… [%d/%d]",
                            label, wait, _consecutive_429s, _MAX_CONSECUTIVE_429)
                time.sleep(wait)
                continue

            # 5xx — server error, retry with backoff
            if resp.status_code >= 500:
                backoff = (2 ** attempt) + random.uniform(0, 1)
                log.warning("Server %d (%s). Backoff %.1fs…",
                            resp.status_code, label, backoff)
                time.sleep(backoff)
                continue

            # Success or 4xx (non-429) — return as-is
            _consecutive_429s = 0  # reset on success
            return resp

        except (requests.Timeout, requests.ConnectionError) as exc:
            last_exc = exc
            backoff = (2 ** attempt) + random.uniform(0, 1)
            if attempt < MAX_RETRIES_PER_REQUEST:
                log.warning("Connection error (%s). Backoff %.1fs… [attempt %d/%d]",
                            label, backoff, attempt + 1, MAX_RETRIES_PER_REQUEST + 1)
                time.sleep(backoff)
            else:
                raise

    # Exhausted retries — raise last exception or a generic one
    if last_exc:
        raise last_exc
    resp.raise_for_status()
    return resp

LINKUP_API_URL = "https://api.linkup.so/v1/search"
LINKUP_FETCH_URL = "https://api.linkup.so/v1/fetch"
LINKUP_API_KEY = "618ccb05-0186-4e66-9226-208943cd0126"

# Domains to skip when looking for company websites
SKIP_DOMAINS = {
    "thetruckersreport.com", "indeed.com", "glassdoor.com",
    "linkedin.com", "facebook.com", "twitter.com", "x.com",
    "instagram.com", "youtube.com", "yelp.com", "bbb.org",
    "ziprecruiter.com", "salary.com", "crunchbase.com",
    "dnb.com", "zoominfo.com", "wikipedia.org", "google.com",
    "mapquest.com", "yellowpages.com", "manta.com",
    "trustpilot.com", "tiktok.com",
}


def _is_skip_domain(url: str) -> bool:
    """Return True if the URL belongs to a domain we want to ignore."""
    try:
        host = urlparse(url).netloc.lower()
        for d in SKIP_DOMAINS:
            if host == d or host.endswith(f".{d}"):
                return True
    except Exception:
        return False
    return False


def _headers(api_key: str) -> dict:
    return {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }


def fetch_url_content(
    url: str,
    session: requests.Session | None = None,
    api_key: str = LINKUP_API_KEY,
    render_js: bool = True,
) -> str | None:
    """
    Use Linkup /v1/fetch to get the markdown content of a URL.
    Returns the markdown text, or None on failure.
    """
    sess = session or requests.Session()
    payload = {
        "url": url,
        "outputType": "markdown",
        "renderJs": render_js,
    }
    try:
        resp = _post_with_retry(
            sess, LINKUP_FETCH_URL,
            json=payload, headers=_headers(api_key), timeout=60,
            label=f"fetch {url[:60]}",
        )
        resp.raise_for_status()
        data = resp.json()
        return data.get("content", None)
    except RateLimitExhausted:
        raise  # let pipeline handle this
    except requests.RequestException as exc:
        log.error("Linkup fetch failed for %s: %s", url, exc)
        return None


def search(
    query: str,
    session: requests.Session | None = None,
    api_key: str = LINKUP_API_KEY,
    max_results: int = 5,
    depth: str = "standard",
) -> dict | None:
    """
    Generic Linkup search. Returns the raw JSON response dict.
    """
    sess = session or requests.Session()
    payload = {
        "q": query,
        "depth": depth,
        "outputType": "searchResults",
        "maxResults": max_results,
    }
    try:
        resp = _post_with_retry(
            sess, LINKUP_API_URL,
            json=payload, headers=_headers(api_key), timeout=30,
            label=f"search: {query[:50]}",
        )
        resp.raise_for_status()
        return resp.json()
    except RateLimitExhausted:
        raise
    except requests.RequestException as exc:
        log.error("Linkup search failed: %s", exc)
        return None


def search_structured(
    query: str,
    schema: str,
    session: requests.Session | None = None,
    api_key: str = LINKUP_API_KEY,
    depth: str = "deep",
) -> dict | None:
    """
    Linkup structured search. Returns parsed JSON matching the schema.
    """
    sess = session or requests.Session()
    payload = {
        "q": query,
        "depth": depth,
        "outputType": "structured",
        "structuredOutputSchema": schema,
    }
    try:
        resp = _post_with_retry(
            sess, LINKUP_API_URL,
            json=payload, headers=_headers(api_key), timeout=60,
            label=f"structured: {query[:50]}",
        )
        resp.raise_for_status()
        return resp.json()
    except RateLimitExhausted:
        raise
    except requests.RequestException as exc:
        log.error("Linkup structured search failed: %s", exc)
        return None


def search_first_url(
    query: str,
    session: requests.Session | None = None,
    api_key: str = LINKUP_API_KEY,
    skip_domains: bool = True,
) -> str | None:
    """
    Search and return the first non-skipped result URL.
    """
    data = search(query, session, api_key)
    if not data:
        return None
    for result in data.get("results", []):
        url = result.get("url", "")
        if url and (not skip_domains or not _is_skip_domain(url)):
            return url
    return None
