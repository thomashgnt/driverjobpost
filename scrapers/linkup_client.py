"""
Shared Linkup API client.

Every other module uses this "phone" to call the Linkup service.
Provides: fetch, search, structured search, and first-URL search.
"""

from __future__ import annotations

import time
import logging
from urllib.parse import urlparse

import requests

log = logging.getLogger(__name__)

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
        resp = sess.post(
            LINKUP_FETCH_URL,
            json=payload,
            headers=_headers(api_key),
            timeout=60,
        )
        resp.raise_for_status()
        data = resp.json()
        return data.get("content", None)
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
        resp = sess.post(
            LINKUP_API_URL,
            json=payload,
            headers=_headers(api_key),
            timeout=30,
        )
        if resp.status_code == 429:
            log.warning("Rate-limited. Waiting 10s…")
            time.sleep(10)
            resp = sess.post(
                LINKUP_API_URL,
                json=payload,
                headers=_headers(api_key),
                timeout=30,
            )
        resp.raise_for_status()
        return resp.json()
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
        resp = sess.post(
            LINKUP_API_URL,
            json=payload,
            headers=_headers(api_key),
            timeout=60,
        )
        if resp.status_code == 429:
            log.warning("Rate-limited. Waiting 10s…")
            time.sleep(10)
            resp = sess.post(
                LINKUP_API_URL,
                json=payload,
                headers=_headers(api_key),
                timeout=60,
            )
        resp.raise_for_status()
        return resp.json()
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
