"""
Step 1: Scrape a job posting URL to extract title, company name, and description.

Strategy A: Linkup /fetch → parse the markdown
Strategy B: Linkup structured search (fallback)
Strategy C: Extract company name from URL (last resort)
"""

import json
import re
import logging
from dataclasses import dataclass

import requests

from scrapers.linkup_client import fetch_url_content, search_structured

log = logging.getLogger(__name__)


@dataclass
class JobInfo:
    title: str
    company_name: str
    description: str
    source_url: str


# JSON schema for structured extraction from Linkup
JOB_SCHEMA = json.dumps({
    "type": "object",
    "properties": {
        "jobTitle": {
            "type": "string",
            "description": "The job title from the posting",
        },
        "companyName": {
            "type": "string",
            "description": "The company name that posted the job",
        },
        "jobDescription": {
            "type": "string",
            "description": "A summary of the job description, requirements, and benefits",
        },
    },
    "required": ["jobTitle", "companyName", "jobDescription"],
})


def _extract_from_markdown(markdown: str, url: str) -> JobInfo | None:
    """
    Parse markdown returned by Linkup /fetch to extract job fields.
    """
    lines = markdown.strip().split("\n")
    title = ""
    company = ""
    desc_lines: list[str] = []

    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue

        # First heading → likely the job title
        if not title and stripped.startswith("#"):
            title = stripped.lstrip("#").strip()
            continue

        # Look for company name patterns
        if not company:
            for pattern in [
                r"[Cc]ompany[:\s]+(.+)",
                r"[Pp]osted by[:\s]+(.+)",
                r"[Ee]mployer[:\s]+(.+)",
                r"[Hh]iring [Cc]ompany[:\s]+(.+)",
            ]:
                m = re.search(pattern, stripped)
                if m:
                    company = m.group(1).strip()
                    break
            # Second heading might be the company
            if not company and stripped.startswith("#") and title:
                company = stripped.lstrip("#").strip()
                continue

        desc_lines.append(stripped)

    description = "\n".join(desc_lines).strip()

    # Try to extract company from the URL for thetruckersreport
    if not company:
        m = re.search(r"/(?:profile|co)/([^/.]+)", url)
        if m:
            company = m.group(1).replace("-", " ").title()

    if title or company:
        return JobInfo(
            title=title or "Unknown Title",
            company_name=company or "Unknown Company",
            description=description[:2000] if description else "No description available",
            source_url=url,
        )
    return None


def scrape_job(url: str, session: requests.Session | None = None) -> JobInfo:
    """
    Main entry point: extract job info from a URL.
    Tries Linkup /fetch first, then falls back to structured search.
    """
    sess = session or requests.Session()

    # --- Strategy A: Linkup /fetch ---
    log.info("Fetching job page via Linkup /fetch: %s", url)
    markdown = fetch_url_content(url, session=sess, render_js=True)

    if markdown and len(markdown) > 100:
        log.info("Got %d chars of markdown content", len(markdown))
        job = _extract_from_markdown(markdown, url)
        if job and job.company_name != "Unknown Company":
            return job
        log.warning("Could not parse company from markdown, trying structured search…")

    # --- Strategy B: Linkup structured search ---
    log.info("Falling back to Linkup structured search for: %s", url)
    query = f"Job posting details from {url}"
    data = search_structured(query, JOB_SCHEMA, session=sess, depth="deep")

    if data:
        return JobInfo(
            title=data.get("jobTitle", "Unknown Title"),
            company_name=data.get("companyName", "Unknown Company"),
            description=data.get("jobDescription", "No description")[:2000],
            source_url=url,
        )

    # --- Strategy C: extract from URL ---
    log.warning("All extraction methods failed. Using URL-based fallback.")
    m = re.search(r"/(?:profile|co)/([^/.]+)", url)
    company_guess = m.group(1).replace("-", " ").title() if m else "Unknown Company"

    return JobInfo(
        title="Unknown Title",
        company_name=company_guess,
        description="Could not extract description",
        source_url=url,
    )
