"""
Step 3: Deep search for decision makers at a company.

5-priority search strategy:
  1. Contact mentioned in the job posting (regex, 0 API calls)
  2. Crawl the company website (about/team/careers pages)
  3. Search LinkedIn directly
  4. Web search by title category
  5. Fallback — find anyone at the company
"""

import json
import re
import logging
from dataclasses import dataclass, field
from urllib.parse import urlparse

import requests

from scrapers.linkup_client import search_structured, search, fetch_url_content

log = logging.getLogger(__name__)


@dataclass
class DecisionMaker:
    name: str
    title: str
    category: str                        # "Owners / Boss", "Hiring", "Operations & Fleet Management"
    source: str                          # "Job Posting", "Company Website", "LinkedIn", "Web Search"
    mentioned_in_job_posting: bool = False
    linkedin: str = ""
    status: str = "Invalid"              # "Valid" if linkedin found, else "Invalid"


# ---------------------------------------------------------------------------
# Categories (unchanged)
# ---------------------------------------------------------------------------
CATEGORIES: list[tuple[str, list[str]]] = [
    ("Hiring", [
        "hiring", "recruiter", "recruiting", "talent", "human resources",
        "hr ",
    ]),
    ("Owners / Boss", [
        "ceo", "chief", "owner", "founder", "president", "partner",
        "vp", "vice president", "director", "head of",
    ]),
    ("Operations & Fleet Management", [
        "operations", "fleet", "transportation", "logistics", "dispatch",
        "safety", "manager",
    ]),
]


def _categorize_title(title: str) -> str | None:
    """Return the category for this title, or None if irrelevant."""
    lower = title.lower()
    for category, keywords in CATEGORIES:
        if any(kw in lower for kw in keywords):
            return category
    return None


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------
PEOPLE_SCHEMA = json.dumps({
    "type": "object",
    "properties": {
        "people": {
            "type": "array",
            "description": (
                "List of decision makers at this company. "
                "Include: CEO, Owner, Founder, President, "
                "VP/Director/Head of Operations/Fleet/Safety/Transportation/Logistics, "
                "Hiring Manager, Recruiter, HR Director, Fleet Manager, "
                "and any other senior leadership or management roles."
            ),
            "items": {
                "type": "object",
                "properties": {
                    "name": {"type": "string", "description": "Full name of the person"},
                    "title": {
                        "type": "string",
                        "description": (
                            "Job title or role (e.g. CEO, Owner, VP Operations, "
                            "Head of Safety, Fleet Manager, Hiring Manager, HR Director)"
                        ),
                    },
                },
                "required": ["name", "title"],
            },
        },
    },
    "required": ["people"],
})

PERSON_TITLE_SCHEMA = json.dumps({
    "type": "object",
    "properties": {
        "title": {
            "type": "string",
            "description": "The person's job title or role at the company",
        },
    },
    "required": ["title"],
})


# ---------------------------------------------------------------------------
# Source priority (lower = better)
# ---------------------------------------------------------------------------
SOURCE_PRIORITY = {
    "Job Posting": 0,
    "Company Website": 1,
    "LinkedIn": 2,
    "Web Search": 3,
}


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _add_person(
    all_people: dict[str, "DecisionMaker"],
    name: str,
    title: str,
    source: str,
    mentioned_in_posting: bool = False,
    linkedin: str = "",
) -> None:
    """Add a person to the dedup dict. Higher-priority source wins."""
    name = name.strip()
    title = title.strip()
    if not name or name.lower() in ("unknown", "n/a", ""):
        return

    # For job posting contacts, force Hiring category even if title doesn't match
    if source == "Job Posting" and mentioned_in_posting:
        category = _categorize_title(title) or "Hiring"
    else:
        category = _categorize_title(title)
        if not category:
            log.debug("Skipping '%s' with irrelevant title '%s'", name, title)
            return

    key = name.lower()
    if key in all_people:
        existing = all_people[key]
        if SOURCE_PRIORITY.get(source, 99) < SOURCE_PRIORITY.get(existing.source, 99):
            existing.source = source
            existing.title = title
        if mentioned_in_posting:
            existing.mentioned_in_job_posting = True
        if linkedin and not existing.linkedin:
            existing.linkedin = linkedin
            existing.status = "Valid"
    else:
        all_people[key] = DecisionMaker(
            name=name,
            title=title,
            category=category,
            source=source,
            mentioned_in_job_posting=mentioned_in_posting,
            linkedin=linkedin,
            status="Valid" if linkedin else "Invalid",
        )


def _count_valid(all_people: dict) -> int:
    """Count people with a LinkedIn URL (Valid status)."""
    return sum(1 for p in all_people.values() if p.linkedin)


# ---------------------------------------------------------------------------
# Priority 1 — Contact from job posting
# ---------------------------------------------------------------------------

def _priority_1_job_posting(
    job_description: str,
    contact_name: str,
    contact_email: str,
    company_name: str,
    all_people: dict,
    session: requests.Session,
) -> None:
    """Extract the person mentioned in the job posting."""
    if not contact_name:
        return

    log.info("Priority 1: Found contact in job posting: %s", contact_name)

    # Try to find their title
    title = ""
    data = search_structured(
        f'"{contact_name}" "{company_name}" job title role',
        PERSON_TITLE_SCHEMA,
        session=session,
        depth="standard",
    )
    if data:
        title = data.get("title", "")

    if not title:
        title = "Recruiter / Contact"

    _add_person(all_people, contact_name, title, "Job Posting", mentioned_in_posting=True)


# ---------------------------------------------------------------------------
# Priority 2 — Company website
# ---------------------------------------------------------------------------

TEAM_PAGES = ["/about", "/about-us", "/team", "/our-team", "/leadership", "/careers", "/contact"]

PERSON_LINE_RE = re.compile(
    r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})\s*[,\-\|:]\s*(.+?)(?:\n|$)'
)
BOLD_PERSON_RE = re.compile(
    r'\*\*([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})\*\*\s*[,\-\|:\u2014]\s*(.+?)(?:\n|$)'
)


def _extract_people_from_markdown(markdown: str) -> list[tuple[str, str]]:
    """Extract (name, title) pairs from team/about page markdown."""
    people: list[tuple[str, str]] = []
    seen: set[str] = set()

    for regex in [BOLD_PERSON_RE, PERSON_LINE_RE]:
        for m in regex.finditer(markdown):
            name = m.group(1).strip()
            title = m.group(2).strip()
            if name.lower() not in seen and _categorize_title(title):
                seen.add(name.lower())
                people.append((name, title))

    return people


def _priority_2_company_website(
    company_name: str,
    company_domain: str,
    all_people: dict,
    session: requests.Session,
) -> None:
    """Search the company website for team/leadership info."""
    domain_host = urlparse(company_domain).netloc or urlparse(company_domain).path.strip("/")

    # Step 2a: Structured search scoped to the domain
    query = (
        f"site:{domain_host} "
        f"{company_name} CEO owner president founder VP director "
        f"hiring manager fleet manager operations team leadership"
    )
    data = search_structured(query, PEOPLE_SCHEMA, session=session, depth="deep")
    if data and "people" in data:
        for person in data["people"]:
            name = person.get("name", "").strip()
            title = person.get("title", "").strip()
            if name and title:
                _add_person(all_people, name, title, "Company Website")

    # Step 2b: If we found <2 people, try fetching specific pages
    website_count = sum(1 for p in all_people.values() if p.source == "Company Website")
    if website_count < 2:
        base_url = company_domain.rstrip("/")
        if not base_url.startswith("http"):
            base_url = f"https://{base_url}"
        fetch_attempts = 0
        for path in TEAM_PAGES:
            if fetch_attempts >= 3:
                break
            full_url = f"{base_url}{path}"
            markdown = fetch_url_content(full_url, session=session)
            fetch_attempts += 1
            if markdown and len(markdown) > 200:
                people = _extract_people_from_markdown(markdown)
                for name, title in people:
                    _add_person(all_people, name, title, "Company Website")
                if people:
                    break


# ---------------------------------------------------------------------------
# Priority 3 — LinkedIn direct search
# ---------------------------------------------------------------------------

LINKEDIN_TITLE_QUERIES = [
    "CEO OR owner OR founder",
    "hiring manager OR recruiter",
    "fleet manager OR operations",
]


def _parse_linkedin_result(name_field: str) -> tuple[str, str]:
    """Parse LinkedIn search result title like 'John Doe - CEO - Company | LinkedIn'."""
    cleaned = re.sub(r'\s*\|?\s*LinkedIn.*$', '', name_field, flags=re.IGNORECASE)
    parts = re.split(r'\s*[\-\|]\s*', cleaned)
    name = parts[0].strip() if parts else ""
    title = parts[1].strip() if len(parts) > 1 else ""
    return name, title


def _priority_3_linkedin(
    company_name: str,
    all_people: dict,
    session: requests.Session,
) -> None:
    """Search LinkedIn directly for people at the company."""
    for title_group in LINKEDIN_TITLE_QUERIES:
        query = f'site:linkedin.com/in/ "{company_name}" {title_group}'
        data = search(query, session=session, max_results=5)
        if not data:
            continue
        for result in data.get("results", []):
            url = result.get("url", "")
            name_field = result.get("name", "")
            if "linkedin.com/in/" not in url.lower():
                continue
            name, title = _parse_linkedin_result(name_field)
            if name and len(name.split()) >= 2:
                _add_person(all_people, name, title or "Unknown Role", "LinkedIn", linkedin=url)


# ---------------------------------------------------------------------------
# Priority 4 — Web search by title category
# ---------------------------------------------------------------------------

CATEGORY_QUERIES = {
    "Owners / Boss": "CEO OR owner OR founder OR president",
    "Hiring": "hiring manager OR recruiter OR HR director OR talent acquisition",
    "Operations & Fleet Management": "fleet manager OR operations manager OR safety director OR dispatch manager",
}


def _priority_4_web_search(
    company_name: str,
    company_domain: str | None,
    all_people: dict,
    session: requests.Session,
) -> None:
    """Direct web search for decision makers by title."""
    # Skip if we already have 3+ valid contacts
    if _count_valid(all_people) >= 3:
        log.info("Priority 4: Skipping — already have %d valid contacts", _count_valid(all_people))
        return

    domain_hint = f"site:{company_domain}" if company_domain else ""
    for category, title_terms in CATEGORY_QUERIES.items():
        query = f'"{company_name}" {title_terms} {domain_hint}'.strip()
        data = search_structured(query, PEOPLE_SCHEMA, session=session, depth="deep")
        if data and "people" in data:
            for person in data["people"]:
                name = person.get("name", "").strip()
                title = person.get("title", "").strip()
                if name and title:
                    _add_person(all_people, name, title, "Web Search")


# ---------------------------------------------------------------------------
# Priority 5 — Fallback
# ---------------------------------------------------------------------------

def _priority_5_fallback(
    company_name: str,
    all_people: dict,
    session: requests.Session,
) -> None:
    """Last resort: find anyone associated with the company."""
    if _count_valid(all_people) > 0:
        return

    log.info("Priority 5: No valid contacts yet. Doing broad fallback search…")
    query = f'"{company_name}" trucking office manager OR coordinator OR dispatch OR assistant OR employee'
    data = search_structured(query, PEOPLE_SCHEMA, session=session, depth="deep")
    if data and "people" in data:
        for person in data["people"]:
            name = person.get("name", "").strip()
            title = person.get("title", "").strip()
            if name and name.lower() not in ("unknown", "n/a"):
                category = _categorize_title(title) or "Operations & Fleet Management"
                key = name.lower()
                if key not in all_people:
                    all_people[key] = DecisionMaker(
                        name=name,
                        title=title or "Employee",
                        category=category,
                        source="Web Search",
                    )


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------

def find_decision_makers(
    company_name: str,
    company_domain: str | None = None,
    job_description: str = "",
    contact_name: str = "",
    contact_email: str = "",
    session: requests.Session | None = None,
) -> list[DecisionMaker]:
    """
    Deep search for decision makers using 5 priority levels.
    Returns all found people (both Valid and Invalid status).
    """
    sess = session or requests.Session()
    all_people: dict[str, DecisionMaker] = {}

    # Priority 1: Job posting contact (0-1 API calls)
    log.info("=== Priority 1: Checking job posting for contact info ===")
    _priority_1_job_posting(job_description, contact_name, contact_email, company_name, all_people, sess)

    # Priority 2: Company website (1-4 API calls)
    if company_domain:
        log.info("=== Priority 2: Crawling company website ===")
        _priority_2_company_website(company_name, company_domain, all_people, sess)

    # Priority 3: LinkedIn cross-reference (3 API calls)
    log.info("=== Priority 3: Searching LinkedIn ===")
    _priority_3_linkedin(company_name, all_people, sess)

    # Priority 4: Direct title search (0-3 API calls, skipped if 3+ valid)
    log.info("=== Priority 4: Web search by title ===")
    _priority_4_web_search(company_name, company_domain, all_people, sess)

    # Priority 5: Fallback (0-1 API calls, only if 0 valid)
    log.info("=== Priority 5: Fallback search ===")
    _priority_5_fallback(company_name, all_people, sess)

    result = list(all_people.values())
    valid = _count_valid(all_people)
    log.info("Found %d decision makers (%d valid) for '%s'", len(result), valid, company_name)
    return result
