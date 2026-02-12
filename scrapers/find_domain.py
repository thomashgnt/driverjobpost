"""
Step 2: Find a company's official website via Linkup search.

Includes domain validation to avoid returning websites of similarly-named
but unrelated companies (e.g. "Next Trucking" instead of "Next Steps Logistics").
"""

from __future__ import annotations

import re
import logging
from urllib.parse import urlparse

import requests

from scrapers.linkup_client import search_first_url

log = logging.getLogger(__name__)

# Legal suffixes to strip when comparing names
_SUFFIXES_RE = re.compile(
    r"\s*,?\s*\b(LLC|L\.?L\.?C\.?|Inc\.?|Corp\.?|Corporation|Ltd\.?|"
    r"Incorporated|Company|Co\.?|LP|LLP)\b\.?\s*$",
    re.IGNORECASE,
)


def _clean_company_name(name: str) -> str:
    """Remove legal suffixes: 'Next Steps Logistics LLC' → 'Next Steps Logistics'."""
    return _SUFFIXES_RE.sub("", name).strip()


def _meaningful_words(name: str) -> list[str]:
    """Extract meaningful words from a company name (skip tiny/common words)."""
    skip = {"the", "and", "of", "a", "an", "for", "in", "at", "by", "to"}
    return [w.lower() for w in name.split() if len(w) > 1 and w.lower() not in skip]


def _domain_matches_company(url: str, clean_name: str) -> bool:
    """
    Check whether the URL's domain looks like it belongs to the company.

    Strategy: extract the domain name (without TLD), remove separators,
    and check if at least half the meaningful company name words appear
    somewhere in the domain.

    Examples that PASS:
        'Next Steps Logistics' + 'nextstepslogistics.com'  → True
        'Riverside Transport'  + 'riversidetransport.com'   → True
        'AGV Inc'              + 'agvinc.net'               → True

    Examples that FAIL:
        'Next Steps Logistics' + 'nexttrucking.com'         → False
        'Great Lakes Solutions' + 'greatlakestransport.com'  → False
    """
    try:
        host = urlparse(url).netloc.lower()
        # Remove www. prefix and TLD  →  "www.nextstepslogistics.com" → "nextstepslogistics"
        domain_core = host.replace("www.", "").rsplit(".", 1)[0]
        # Remove separators  →  "next-steps-logistics" → "nextstepslogistics"
        domain_flat = re.sub(r"[-_.]", "", domain_core)
    except Exception:
        return False

    words = _meaningful_words(clean_name)
    if not words:
        return True  # can't validate, assume OK

    # Count how many company words appear in the domain
    matches = sum(1 for w in words if w in domain_flat)

    # Require ALL meaningful words to appear in the domain.
    # This is strict but avoids matching "Next Trucking" for "Next Steps Logistics"
    # or "Great Lakes Transport" for "Great Lakes Solutions".
    return matches == len(words)


def find_company_domain(
    company_name: str,
    session: requests.Session | None = None,
) -> str | None:
    """
    Given a company name, return their official website URL.
    Returns None if not found or if no result matches the company name.
    """
    clean_name = _clean_company_name(company_name)
    log.info("Searching for domain: %s (cleaned: %s)", company_name, clean_name)

    # ── Attempt 1: quoted search ──────────────────────────────────────
    query = f'"{clean_name}" official website'
    url = search_first_url(query, session=session)

    if url and _domain_matches_company(url, clean_name):
        log.info("Found domain for '%s': %s", company_name, url)
        return url

    if url:
        log.warning("Domain %s doesn't match '%s', trying again…", url, clean_name)

    # ── Attempt 2: broader search ─────────────────────────────────────
    query2 = f"{clean_name} company homepage"
    url2 = search_first_url(query2, session=session)

    if url2 and _domain_matches_company(url2, clean_name):
        log.info("Found domain for '%s' (2nd attempt): %s", company_name, url2)
        return url2

    if url2:
        log.warning("Domain %s still doesn't match '%s', giving up", url2, clean_name)

    # No valid domain found
    log.warning("No matching domain found for '%s'", company_name)
    return None
