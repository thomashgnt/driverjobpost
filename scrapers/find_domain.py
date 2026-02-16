"""
Step 2: Find a company's official website.

Search order:
  1. Extract URL from the job description text (0 API calls)
  2. Linkup search — quoted company name (1 API call)
  3. Linkup search — broader query (1 API call)

Includes domain validation to avoid returning websites of similarly-named
but unrelated companies (e.g. "Next Trucking" instead of "Next Steps Logistics").
"""

from __future__ import annotations

import re
import logging
from urllib.parse import urlparse

import requests

from scrapers.linkup_client import search_first_url, SKIP_DOMAINS

log = logging.getLogger(__name__)

# Legal suffixes to strip when comparing names
_SUFFIXES_RE = re.compile(
    r"\s*,?\s*\b(LLC|L\.?L\.?C\.?|Inc\.?|Corp\.?|Corporation|Ltd\.?|"
    r"Incorporated|Company|Co\.?|LP|LLP)\b\.?\s*$",
    re.IGNORECASE,
)

# Regex to find URLs in text (http/https links)
_URL_RE = re.compile(
    r'https?://[^\s<>\"\')\],]+',
    re.IGNORECASE,
)

# Regex to find bare domains like "packages.llc" or "963delivery.com"
_BARE_DOMAIN_RE = re.compile(
    r'(?<!\S)(?:www\.)?([a-zA-Z0-9][-a-zA-Z0-9]*\.(?:com|net|org|llc|io|co|us|biz|info|delivery))\b',
    re.IGNORECASE,
)


def _clean_company_name(name: str) -> str:
    """Remove legal suffixes: 'Next Steps Logistics LLC' → 'Next Steps Logistics'."""
    return _SUFFIXES_RE.sub("", name).strip()


def _meaningful_words(name: str) -> list[str]:
    """Extract meaningful words from a company name (skip tiny/common words)."""
    skip = {"the", "and", "of", "a", "an", "for", "in", "at", "by", "to"}
    return [w.lower() for w in name.split() if len(w) > 1 and w.lower() not in skip]


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


def _domain_matches_company(url: str, clean_name: str) -> bool:
    """
    Check whether the URL's domain looks like it belongs to the company.

    Strategy: extract the domain name (without TLD), remove separators,
    and check if the meaningful company name words appear in the domain.

    For short names (1 word), we require the domain core to be very close
    to avoid false positives like "packages.ubuntu.com" for "Packages LLC".

    Examples that PASS:
        'Next Steps Logistics' + 'nextstepslogistics.com'  → True
        '963 Delivery'         + '963delivery.com'         → True
        'Packages'             + 'packages.llc'            → True

    Examples that FAIL:
        'Next Steps Logistics' + 'nexttrucking.com'         → False
        'Great Lakes Solutions' + 'greatlakestransport.com'  → False
        'Packages'             + 'packages.ubuntu.com'      → False
    """
    try:
        host = urlparse(url).netloc.lower()
        if not host:
            # Try adding scheme if missing
            host = urlparse(f"https://{url}").netloc.lower()
        # Remove www. prefix and TLD  →  "www.nextstepslogistics.com" → "nextstepslogistics"
        domain_core = host.replace("www.", "").rsplit(".", 1)[0]
        # Remove separators  →  "next-steps-logistics" → "nextstepslogistics"
        domain_flat = re.sub(r"[-_.]", "", domain_core)
    except Exception:
        return False

    words = _meaningful_words(clean_name)
    if not words:
        return True  # can't validate, assume OK

    if len(words) == 1:
        # For single-word names (e.g. "Packages", "963 Delivery" where "963" is short),
        # the domain must be very close to the company name.
        # "packages" matches "packages.llc" but NOT "packages.ubuntu.com"
        # Concatenate all words (including short ones like "963") for this check
        all_words = [w.lower() for w in clean_name.split()
                     if w.lower() not in {"the", "and", "of", "a", "an", "for", "in", "at", "by", "to"}]
        name_flat = "".join(all_words)
        # Domain must be the name itself, or name + a short suffix (max 3 extra chars)
        # This allows "963delivery" → "963deliveryco" but NOT "packagesubuntu"
        if domain_flat.startswith(name_flat) and len(domain_flat) <= len(name_flat) + 3:
            return True
        # Also check if the domain IS the name (e.g. domain "packages" for "packages.llc")
        if name_flat == domain_flat:
            return True
        log.debug("Single-word rejection: domain '%s' doesn't match name '%s'", domain_flat, name_flat)
        return False

    # For multi-word names: require ALL meaningful words to appear in the domain
    matches = sum(1 for w in words if w in domain_flat)
    return matches == len(words)


def _extract_domain_from_description(
    description: str,
    clean_name: str,
) -> str | None:
    """
    Try to find the company website URL in the job description text.
    Returns the first matching URL/domain, or None.
    """
    if not description:
        return None

    candidates: list[str] = []

    # Find full URLs (https://...)
    for match in _URL_RE.finditer(description):
        url = match.group(0).rstrip(".,;:!?)")
        if not _is_skip_domain(url):
            candidates.append(url)

    # Find bare domains (packages.llc, 963delivery.com, etc.)
    for match in _BARE_DOMAIN_RE.finditer(description):
        domain = match.group(1).lower()
        full_url = f"https://{domain}"
        if not _is_skip_domain(full_url) and full_url not in candidates:
            candidates.append(full_url)

    # Check each candidate against the company name
    for url in candidates:
        if _domain_matches_company(url, clean_name):
            log.info("Found domain in job description: %s", url)
            return url

    return None


def find_company_domain(
    company_name: str,
    job_description: str = "",
    session: requests.Session | None = None,
) -> str | None:
    """
    Given a company name, return their official website URL.
    Returns None if not found or if no result matches the company name.

    Search order:
      1. Look for a URL in the job description (0 API calls)
      2. Linkup quoted search (1 API call)
      3. Linkup broader search (1 API call)
    """
    clean_name = _clean_company_name(company_name)
    log.info("Searching for domain: %s (cleaned: %s)", company_name, clean_name)

    # ── Attempt 0: extract from job description ─────────────────────────
    if job_description:
        domain_from_desc = _extract_domain_from_description(job_description, clean_name)
        if domain_from_desc:
            log.info("Found domain for '%s' in job description: %s", company_name, domain_from_desc)
            return domain_from_desc

    # ── Attempt 1: quoted search ────────────────────────────────────────
    query = f'"{clean_name}" official website'
    url = search_first_url(query, session=session)

    if url and _domain_matches_company(url, clean_name):
        log.info("Found domain for '%s': %s", company_name, url)
        return url

    if url:
        log.warning("Domain %s doesn't match '%s', trying again…", url, clean_name)

    # ── Attempt 2: broader search ───────────────────────────────────────
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
