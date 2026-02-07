"""
Step 2: Find a company's official website via Linkup search.
"""

import logging

import requests

from scrapers.linkup_client import search_first_url

log = logging.getLogger(__name__)


def find_company_domain(
    company_name: str,
    session: requests.Session | None = None,
) -> str | None:
    """
    Given a company name, return their official website URL.
    Returns None if not found.
    """
    query = f"{company_name} trucking company official website"
    log.info("Searching for domain: %s", company_name)

    url = search_first_url(query, session=session)

    if url:
        log.info("Found domain for '%s': %s", company_name, url)
    else:
        log.warning("No domain found for '%s'", company_name)

    return url
