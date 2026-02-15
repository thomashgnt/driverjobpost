"""
Step 3: Deep search for decision makers at a company.

5-priority search strategy:
  1. Contact mentioned in the job posting (regex, 0 API calls)
  2. Crawl the company website (about/team/careers pages)
  3. Search LinkedIn directly
  4. Web search by title category
  5. Fallback — find anyone at the company
"""

from __future__ import annotations

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
    confidence: str = "Low"              # "High", "Medium", "Low"


# Names that indicate a failed extraction (case-insensitive)
REJECT_NAMES = {
    "unknown", "n/a", "not specified", "not found", "not available",
    "none", "no name", "no contact", "the company", "the owner",
}

# Max contacts to keep per company (if more, it's likely noise)
MAX_CONTACTS_PER_COMPANY = 5


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

def _compute_confidence(
    source: str,
    linkedin: str,
    mentioned_in_posting: bool,
    name: str,
) -> str:
    """
    Compute a confidence score for a decision maker.

    High   = LinkedIn confirmed OR mentioned in the job posting
    Medium = Found on company website or full name from web search
    Low    = Single-word name, fallback result, or unconfirmed
    """
    if linkedin or mentioned_in_posting:
        return "High"
    if source in ("Company Website", "Job Posting"):
        return "Medium"
    # Web Search with full name (2+ words) = Medium
    if source in ("LinkedIn", "Web Search") and len(name.split()) >= 2:
        return "Medium"
    return "Low"


def _is_valid_name(name: str) -> bool:
    """Reject names that are clearly bad extractions."""
    stripped = name.strip()
    if not stripped:
        return False
    if stripped.lower() in REJECT_NAMES:
        return False
    # Reject single-word names (e.g. "Chris", "Angie")
    if len(stripped.split()) < 2:
        log.debug("Rejecting single-word name: '%s'", stripped)
        return False
    return True


def _title_company_matches(title: str, company_name: str) -> bool:
    """
    If a title contains 'at [Company]', verify it's the right company.
    Returns True if no 'at' clause found, or if the company matches.
    Returns False if the title references a DIFFERENT company.
    """
    # Look for "at Company Name" pattern
    match = re.search(r'\bat\s+(.+?)(?:\s*$|\s*,)', title, re.IGNORECASE)
    if not match:
        return True  # no "at" clause, can't check — allow it

    title_company = match.group(1).strip().lower()
    # Remove suffixes for comparison
    from scrapers.find_domain import _clean_company_name
    clean_target = _clean_company_name(company_name).lower()
    clean_title = _clean_company_name(title_company).lower()

    # Check if the title company matches the target company.
    # Strategy: both names must share most of their distinctive words.
    skip = {"the", "and", "of", "a", "an", "for", "in", "at", "by", "to", "company"}
    target_words = set(clean_target.split()) - skip
    title_words = set(clean_title.split()) - skip

    if not target_words:
        return True  # can't compare, allow it

    # Require that the majority of target words appear in the title company
    common = target_words & title_words
    match_ratio = len(common) / len(target_words) if target_words else 0
    if match_ratio < 0.75:
        log.debug("Rejecting '%s' — title company '%s' doesn't match '%s' (%.0f%% overlap)",
                  title, title_company, company_name, match_ratio * 100)
        return False
    return True


def _add_person(
    all_people: dict[str, "DecisionMaker"],
    name: str,
    title: str,
    source: str,
    company_name: str = "",
    mentioned_in_posting: bool = False,
    linkedin: str = "",
) -> None:
    """Add a person to the dedup dict. Higher-priority source wins."""
    name = name.strip()
    title = title.strip()

    # ── Reject bad names ──────────────────────────────────────────────
    if not _is_valid_name(name):
        return

    # ── Reject wrong-company titles ───────────────────────────────────
    if company_name and not _title_company_matches(title, company_name):
        return

    # ── Categorize ────────────────────────────────────────────────────
    # For job posting contacts, force Hiring category even if title doesn't match
    if source == "Job Posting" and mentioned_in_posting:
        category = _categorize_title(title) or "Hiring"
    else:
        category = _categorize_title(title)
        if not category:
            log.debug("Skipping '%s' with irrelevant title '%s'", name, title)
            return

    # ── Compute confidence ────────────────────────────────────────────
    confidence = _compute_confidence(source, linkedin, mentioned_in_posting, name)

    # ── Dedup: merge or create ────────────────────────────────────────
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
        # Upgrade confidence if the new source is stronger
        rank = {"High": 2, "Medium": 1, "Low": 0}
        if rank.get(confidence, 0) > rank.get(existing.confidence, 0):
            existing.confidence = confidence
    else:
        all_people[key] = DecisionMaker(
            name=name,
            title=title,
            category=category,
            source=source,
            mentioned_in_job_posting=mentioned_in_posting,
            linkedin=linkedin,
            confidence=confidence,
        )


def _count_valid(all_people: dict) -> int:
    """Count people with High confidence (LinkedIn or mentioned in posting)."""
    return sum(1 for p in all_people.values() if p.confidence == "High")


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

    _add_person(all_people, contact_name, title, "Job Posting",
                company_name=company_name, mentioned_in_posting=True)


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
                _add_person(all_people, name, title, "Company Website",
                            company_name=company_name)

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
                    _add_person(all_people, name, title, "Company Website",
                                company_name=company_name)


# ---------------------------------------------------------------------------
# Priority 3 — LinkedIn direct search
# ---------------------------------------------------------------------------

LINKEDIN_TITLE_QUERIES = [
    "CEO OR owner OR founder",
    "hiring manager OR recruiter OR HR director OR talent acquisition",
    "fleet manager OR operations OR safety",
]


def _parse_linkedin_result(name_field: str) -> tuple[str, str]:
    """Parse LinkedIn search result title like 'John Doe - CEO - Company | LinkedIn'."""
    cleaned = re.sub(r'\s*\|?\s*LinkedIn.*$', '', name_field, flags=re.IGNORECASE)
    parts = re.split(r'\s*[\-\|]\s*', cleaned)
    name = parts[0].strip() if parts else ""
    title = parts[1].strip() if len(parts) > 1 else ""
    return name, title


def _company_in_result(company_name: str, result_text: str, person_name: str) -> bool:
    """
    Check that a LinkedIn result is about someone who works at this company,
    not just someone whose personal name matches the company name.

    Removes the person's name parts from the result text, then checks
    if the company name still appears (exact match OR word-based match).
    """
    text = result_text.lower()
    company_lower = company_name.lower().strip()

    # Remove person's name parts from the text
    for part in person_name.lower().split():
        if len(part) > 1:  # skip initials like "M"
            text = text.replace(part, " ")

    # Clean up extra spaces
    text = re.sub(r'\s+', ' ', text)

    # --- Check 1: exact match ---
    if company_lower in text:
        return True

    # --- Check 2: without punctuation (e.g. "D.M. Bowman" → "dm bowman") ---
    company_clean = re.sub(r'[^\w\s]', '', company_lower)
    text_clean = re.sub(r'[^\w\s]', '', text)
    if company_clean in text_clean:
        return True

    # --- Check 3: word-based match ---
    # For names like "24 Seven Express Inc", the LinkedIn profile might say
    # "24/7 Express" or "24Seven Express". Check if most meaningful words match.
    from scrapers.find_domain import _clean_company_name
    clean_name = _clean_company_name(company_name).lower()
    # Extract meaningful words (skip very short words and numbers-only)
    skip = {"the", "and", "of", "a", "an", "for", "in", "at", "by", "to"}
    company_words = [w for w in clean_name.split()
                     if len(w) > 2 and w not in skip]
    if company_words:
        found = sum(1 for w in company_words if w in text_clean)
        # Require at least half of the meaningful words to appear
        if found >= max(1, len(company_words) * 0.5):
            return True

    return False


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
            content = result.get("content", "")
            if "linkedin.com/in/" not in url.lower():
                continue
            name, title = _parse_linkedin_result(name_field)
            if not name or len(name.split()) < 2:
                continue

            # Guard: verify the company name appears in the result
            # independently of the person's name (avoid last-name matches)
            full_text = f"{name_field} {content}"
            if not _company_in_result(company_name, full_text, name):
                log.info("Skipping '%s' — company '%s' not confirmed in LinkedIn result", name, company_name)
                continue

            _add_person(all_people, name, title or "Unknown Role", "LinkedIn",
                        company_name=company_name, linkedin=url)


# ---------------------------------------------------------------------------
# Priority 4 — Web search by title category
# ---------------------------------------------------------------------------

CATEGORY_QUERIES = {
    "Owners / Boss": "CEO OR owner OR founder OR president",
    "Hiring": "hiring manager OR recruiter OR HR director OR talent acquisition",
    "Operations & Fleet Management": "fleet manager OR operations manager OR safety director OR dispatch manager",
}


def _count_valid_in_category(all_people: dict, category: str) -> int:
    """Count valid (with LinkedIn) people in a specific category."""
    return sum(1 for p in all_people.values() if p.category == category and p.linkedin)


def _priority_4_web_search(
    company_name: str,
    company_domain: str | None,
    all_people: dict,
    session: requests.Session,
) -> None:
    """Direct web search for decision makers by title — per category."""
    domain_hint = f"site:{company_domain}" if company_domain else ""
    for category, title_terms in CATEGORY_QUERIES.items():
        # Skip this category if we already have 1+ valid person in it
        cat_valid = _count_valid_in_category(all_people, category)
        if cat_valid >= 1:
            log.info("Priority 4: Skipping '%s' — already have %d valid", category, cat_valid)
            continue

        log.info("Priority 4: Searching '%s' (0 valid in this category)", category)
        query = f'"{company_name}" {title_terms} {domain_hint}'.strip()
        data = search_structured(query, PEOPLE_SCHEMA, session=session, depth="deep")
        if data and "people" in data:
            for person in data["people"]:
                name = person.get("name", "").strip()
                title = person.get("title", "").strip()
                if name and title:
                    _add_person(all_people, name, title, "Web Search",
                                company_name=company_name)


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
            if not _is_valid_name(name):
                continue
            if company_name and not _title_company_matches(title, company_name):
                continue
            category = _categorize_title(title) or "Operations & Fleet Management"
            key = name.lower()
            if key not in all_people:
                all_people[key] = DecisionMaker(
                    name=name,
                    title=title or "Employee",
                    category=category,
                    source="Web Search",
                    confidence="Low",  # fallback = low confidence
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
    Returns up to MAX_CONTACTS_PER_COMPANY people, sorted by confidence.
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

    # ── Limit results to top contacts ─────────────────────────────────
    # Sort by confidence (High > Medium > Low), then by source priority
    confidence_rank = {"High": 0, "Medium": 1, "Low": 2}
    result = sorted(
        all_people.values(),
        key=lambda p: (confidence_rank.get(p.confidence, 9), SOURCE_PRIORITY.get(p.source, 9)),
    )
    if len(result) > MAX_CONTACTS_PER_COMPANY:
        log.info("Trimming from %d to %d contacts", len(result), MAX_CONTACTS_PER_COMPANY)
        result = result[:MAX_CONTACTS_PER_COMPANY]

    high = sum(1 for p in result if p.confidence == "High")
    log.info("Found %d decision makers (%d high-confidence) for '%s'",
             len(result), high, company_name)
    return result
