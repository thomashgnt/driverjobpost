"""
Step 1: Scrape a job posting URL to extract title, company name, description,
and contact person mentioned in the posting.

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
    contact_name: str = ""
    contact_email: str = ""


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

# ---------------------------------------------------------------------------
# Contact extraction from job description
# ---------------------------------------------------------------------------

EMAIL_RE = re.compile(r'[\w.-]+@[\w.-]+\.\w{2,}')
GENERIC_EMAIL_PREFIXES = {
    "info", "careers", "jobs", "hr", "noreply", "apply", "applications",
    "recruiting", "hiring", "support", "admin", "contact", "office",
}

CONTACT_PATTERNS = [
    re.compile(r'[Cc]ontact\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2})'),
    re.compile(r'[Aa]pply\s+(?:with|to)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2})'),
    re.compile(r'[Ss]end\s+(?:your\s+)?(?:resume|CV|application)\s+to\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2})'),
    re.compile(r'[Rr]ecruiter[:\s]+([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2})'),
    re.compile(r'[Hh]iring\s+[Mm]anager[:\s]+([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2})'),
    re.compile(r'[Pp]osted\s+by[:\s]+([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2})'),
    re.compile(r'[Aa]sk\s+for\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2})'),
    re.compile(r'[Rr]each\s+(?:out\s+)?to\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2})'),
    re.compile(r'[Cc]all\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2})'),
    re.compile(r'[Ee]mail\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2})'),
]

# Words that look like names but aren't people
NAME_BLOCKLIST = {
    "our team", "the company", "the office", "our office", "the driver",
    "our driver", "the recruiter", "your resume", "your cv",
}


def _extract_contact_info(text: str) -> tuple[str, str]:
    """
    Extract a contact person name and email from job description text.
    Returns (name, email). Either or both may be empty strings.
    """
    if not text:
        return "", ""

    # --- Email extraction ---
    email = ""
    for match in EMAIL_RE.finditer(text):
        candidate = match.group(0).lower()
        prefix = candidate.split("@")[0]
        if prefix not in GENERIC_EMAIL_PREFIXES:
            email = match.group(0)
            break

    # --- Name extraction from patterns ---
    name = ""
    for pattern in CONTACT_PATTERNS:
        m = pattern.search(text)
        if m:
            candidate = m.group(1).strip()
            if candidate.lower() not in NAME_BLOCKLIST and len(candidate.split()) >= 2:
                name = candidate
                break

    # --- If we have an email but no name, try to derive name from email ---
    if email and not name:
        prefix = email.split("@")[0]
        parts = re.split(r'[._-]', prefix)
        if len(parts) >= 2 and all(p.isalpha() for p in parts):
            name = " ".join(p.capitalize() for p in parts)

    return name, email


# ---------------------------------------------------------------------------
# Markdown parsing
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def scrape_job(url: str, session: requests.Session | None = None) -> JobInfo:
    """
    Main entry point: extract job info from a URL.
    Tries Linkup /fetch first, then falls back to structured search.
    Also extracts contact person info from the description.
    """
    sess = session or requests.Session()
    raw_text = ""

    # --- Strategy A: Linkup /fetch ---
    log.info("Fetching job page via Linkup /fetch: %s", url)
    markdown = fetch_url_content(url, session=sess, render_js=True)

    if markdown and len(markdown) > 100:
        log.info("Got %d chars of markdown content", len(markdown))
        raw_text = markdown
        job = _extract_from_markdown(markdown, url)
        if job and job.company_name != "Unknown Company":
            name, email = _extract_contact_info(raw_text)
            job.contact_name = name
            job.contact_email = email
            return job
        log.warning("Could not parse company from markdown, trying structured search…")

    # --- Strategy B: Linkup structured search ---
    log.info("Falling back to Linkup structured search for: %s", url)
    query = f"Job posting details from {url}"
    data = search_structured(query, JOB_SCHEMA, session=sess, depth="deep")

    if data:
        desc = data.get("jobDescription", "No description")[:2000]
        raw_text = raw_text or desc
        name, email = _extract_contact_info(raw_text)
        return JobInfo(
            title=data.get("jobTitle", "Unknown Title"),
            company_name=data.get("companyName", "Unknown Company"),
            description=desc,
            source_url=url,
            contact_name=name,
            contact_email=email,
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
