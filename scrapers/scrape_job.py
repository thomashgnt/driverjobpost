"""
Step 1: Scrape a job posting URL to extract title, company name, description,
and contact person mentioned in the posting.

Strategy A: Linkup /fetch → parse the markdown
Strategy B: Linkup structured search (fallback)
Strategy C: Extract company name from URL (last resort)

Special handling for Amazon DSP / Fountain pages where the actual employer
is the DSP company (e.g. "Next Steps Logistics LLC"), not "Amazon".
"""

from __future__ import annotations

import json
import re
import logging
from dataclasses import dataclass
from urllib.parse import urlparse

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
            "description": "The job title from the posting (e.g. Delivery Driver, CDL Truck Driver)",
        },
        "companyName": {
            "type": "string",
            "description": (
                "The actual employer company name. "
                "IMPORTANT: For Amazon DSP listings, the employer is the Delivery Service Partner "
                "(the LLC/Inc company like 'Next Steps Logistics LLC'), NOT 'Amazon' or 'Amazon DSP'. "
                "Look for the company entity with LLC, Inc, Corp, or Ltd suffix."
            ),
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
# Company name cleanup — strip aggregator/platform names
# ---------------------------------------------------------------------------

# Regex to find entity suffixes (LLC, Inc, Corp...)
ENTITY_SUFFIX_RE = re.compile(
    r'\b(LLC|Inc\.?|Corp\.?|Corporation|Ltd\.?|L\.?L\.?C\.?|Incorporated)\b',
    re.IGNORECASE,
)

# Suffixes / noise to strip from company names
PLATFORM_NOISE = [
    r'\s*[-–—]\s*Amazon\s*DSP\s*$',
    r'\s*[-–—]\s*Amazon\s*$',
    r'\s*\(\s*Amazon\s*DSP\s*\)\s*$',
    r'\s*[-–—]\s*DSP\s*$',
    r'^\s*Amazon\s*DSP\s*[-–—]\s*',
]
PLATFORM_NOISE_RE = [re.compile(p, re.IGNORECASE) for p in PLATFORM_NOISE]


def _is_fountain_amazon_dsp(url: str) -> bool:
    """Check if this URL is a Fountain Amazon DSP job posting."""
    try:
        host = urlparse(url).netloc.lower()
        path = urlparse(url).path.lower()
    except Exception:
        return False
    return "fountain.com" in host and "delivery-service-partner" in path


def _clean_company_name(name: str) -> str:
    """Remove aggregator/platform noise from company name."""
    cleaned = name.strip()
    for regex in PLATFORM_NOISE_RE:
        cleaned = regex.sub("", cleaned).strip()
    return cleaned if cleaned else name.strip()


def _find_entity_in_text(text: str) -> str | None:
    """
    Find a company entity name (LLC/Inc/Corp) in text by locating the
    suffix and walking backwards to grab the preceding capitalized words.

    "Next Steps Logistics LLC" → "Next Steps Logistics LLC"
    "Bullard Logistics, LLC" → "Bullard Logistics, LLC"
    """
    for match in ENTITY_SUFFIX_RE.finditer(text):
        suffix = match.group(0)
        before = text[:match.start()].rstrip(", ")

        # Walk backwards through words to find the company name
        words = before.split()
        name_parts: list[str] = []
        for word in reversed(words):
            clean = word.strip(" -–—|,")
            if not clean:
                break
            # Stop at separators, prices, station codes like "DLN3"
            if clean[0].isupper() and clean.isalpha():
                name_parts.insert(0, clean)
                if len(name_parts) >= 6:
                    break
            elif clean in ("&", "of", "and", "the"):
                name_parts.insert(0, clean)
            else:
                break

        if name_parts:
            # Reconstruct with original comma if present
            comma = ", " if text[match.start() - 2:match.start()].rstrip() .endswith(",") else " "
            entity = " ".join(name_parts) + comma + suffix
            if "amazon" not in entity.lower():
                return entity
    return None


def _extract_real_company(text: str, raw_company: str) -> str:
    """
    For Amazon DSP pages: find the real employer (the LLC/Inc entity).

    Strategy:
      1. Split raw_company by " - " and find the part with LLC/Inc
      2. Search raw_company for entity pattern
      3. Search full text (title + description) for entity pattern
      4. Fallback: clean the raw name

    Examples:
        "$18/hr - Delivery Helper - Next Steps Logistics LLC - Amazon DSP"
        → "Next Steps Logistics LLC"

        "Bullard Logistics, LLC is an Amazon Delivery Service Partner"
        → "Bullard Logistics, LLC"
    """
    # Strategy 1: Split by " - " and find the LLC/Inc part
    for separator in (" - ", " – ", " — ", " | "):
        for part in raw_company.split(separator):
            part = part.strip()
            if ENTITY_SUFFIX_RE.search(part) and "amazon" not in part.lower():
                return part

    # Strategy 2: Find entity in raw company name
    entity = _find_entity_in_text(raw_company)
    if entity:
        return entity

    # Strategy 3: Find entity in full text
    entity = _find_entity_in_text(text)
    if entity:
        return entity

    # Strategy 4: Just clean the name
    return _clean_company_name(raw_company)


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
        # For Amazon DSP / Fountain pages: extract the real employer
        if _is_fountain_amazon_dsp(url):
            full_text = f"{title}\n{description}"
            company = _extract_real_company(full_text, company or title)
        elif company:
            company = _clean_company_name(company)

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
        raw_company = data.get("companyName", "Unknown Company")
        raw_title = data.get("jobTitle", "Unknown Title")

        # Clean up company name (especially for Amazon DSP)
        if _is_fountain_amazon_dsp(url):
            full_text = f"{raw_title}\n{raw_company}\n{desc}"
            company = _extract_real_company(full_text, raw_company)
        else:
            company = _clean_company_name(raw_company)

        name, email = _extract_contact_info(raw_text)
        return JobInfo(
            title=raw_title,
            company_name=company,
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
