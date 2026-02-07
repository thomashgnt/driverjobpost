"""
Step 4: Find LinkedIn profile URLs for decision makers.

Uses Linkup search to find the LinkedIn profile of each person.
"""

import logging

import requests

from scrapers.linkup_client import search

log = logging.getLogger(__name__)


def find_linkedin_url(
    person_name: str,
    person_title: str,
    company_name: str,
    session: requests.Session | None = None,
) -> str | None:
    """
    Search for a person's LinkedIn profile URL.
    Returns the LinkedIn URL or None if not found.
    """
    query = f"{person_name} {person_title} {company_name} LinkedIn"
    log.info("Searching LinkedIn for: %s", query)

    data = search(query, session=session, max_results=5)
    if not data:
        return None

    for result in data.get("results", []):
        url = result.get("url", "")
        if "linkedin.com/in/" in url.lower():
            log.info("Found LinkedIn for '%s': %s", person_name, url)
            return url

    log.warning("No LinkedIn profile found for '%s'", person_name)
    return None
