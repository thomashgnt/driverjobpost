"""
Step 4: Find LinkedIn profile URLs for decision makers.

Uses Linkup search to find the LinkedIn profile of each person.
Validates that the result is for someone at the right company,
not just a person with a similar name.
"""

import re
import logging

import requests

from scrapers.linkup_client import search

log = logging.getLogger(__name__)


def _company_in_result(company_name: str, result_text: str, person_name: str) -> bool:
    """
    Check that a LinkedIn result is about someone at this company,
    not just someone whose personal name matches the company name.
    """
    text = result_text.lower()

    # Remove person's name parts from the text
    for part in person_name.lower().split():
        if len(part) > 1:
            text = text.replace(part, " ")

    text = re.sub(r'\s+', ' ', text)
    company_lower = company_name.lower().strip()

    # Check full company name
    if company_lower in text:
        return True

    # Try without punctuation
    company_clean = re.sub(r'[^\w\s]', '', company_lower)
    text_clean = re.sub(r'[^\w\s]', '', text)
    if company_clean in text_clean:
        return True

    return False


def find_linkedin_url(
    person_name: str,
    person_title: str,
    company_name: str,
    session: requests.Session | None = None,
) -> str | None:
    """
    Search for a person's LinkedIn profile URL.
    Returns the LinkedIn URL or None if not found.
    Validates that the result mentions the company name.
    """
    query = f"{person_name} {person_title} {company_name} LinkedIn"
    log.info("Searching LinkedIn for: %s", query)

    data = search(query, session=session, max_results=5)
    if not data:
        return None

    for result in data.get("results", []):
        url = result.get("url", "")
        if "linkedin.com/in/" not in url.lower():
            continue

        # Verify the company name appears in the result (not just person name)
        name_field = result.get("name", "")
        content = result.get("content", "")
        full_text = f"{name_field} {content}"

        if not _company_in_result(company_name, full_text, person_name):
            log.info("Skipping LinkedIn result for '%s' — company '%s' not confirmed", person_name, company_name)
            continue

        log.info("Found LinkedIn for '%s': %s", person_name, url)
        return url

    log.warning("No LinkedIn profile found for '%s'", person_name)
    return None
