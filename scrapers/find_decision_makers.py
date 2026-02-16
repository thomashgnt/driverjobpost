"""
Step 3: Find decision makers at a company.

3 parallel persona scrapers + fallback:
  - Owners scraper   (CEO, owner, founder)
  - Hiring scraper   (hiring manager, recruiter, HR)
  - Operations scraper (fleet manager, operations, safety)
  - Fallback — find anyone at the company (only if 0 high-confidence)
"""

from __future__ import annotations

import concurrent.futures
import json
import re
import logging
from dataclasses import dataclass

import requests

from scrapers.linkup_client import search_structured, search

log = logging.getLogger(__name__)


@dataclass
class DecisionMaker:
    name: str
    title: str
    category: str                        # "Owners / Boss", "Hiring", "Operations & Fleet Management"
    source: str                          # "LinkedIn", "Web Search"
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
# Categories
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
# Schema for structured search
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


# ---------------------------------------------------------------------------
# Source priority (lower = better)
# ---------------------------------------------------------------------------
SOURCE_PRIORITY = {
    "LinkedIn": 0,
    "Web Search": 1,
}


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _compute_confidence(
    source: str,
    linkedin: str,
    name: str,
) -> str:
    """
    Compute a confidence score for a decision maker.

    High   = LinkedIn profile confirmed
    Medium = Full name (2+ words) from LinkedIn or Web Search
    Low    = Single-word name, fallback result, or unconfirmed
    """
    if linkedin:
        return "High"
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
    category = _categorize_title(title)
    if not category:
        log.debug("Skipping '%s' with irrelevant title '%s'", name, title)
        return

    # ── Compute confidence ────────────────────────────────────────────
    confidence = _compute_confidence(source, linkedin, name)

    # ── Dedup: merge or create ────────────────────────────────────────
    key = name.lower()
    if key in all_people:
        existing = all_people[key]
        if SOURCE_PRIORITY.get(source, 99) < SOURCE_PRIORITY.get(existing.source, 99):
            existing.source = source
            existing.title = title
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
            linkedin=linkedin,
            confidence=confidence,
        )


# ---------------------------------------------------------------------------
# LinkedIn result parsing
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Persona config — one entry per scraper
# ---------------------------------------------------------------------------
PERSONA_CONFIG = {
    "owners": {
        "category": "Owners / Boss",
        "linkedin_query": "CEO OR owner OR founder",
        "web_query": "CEO OR owner OR founder OR president",
    },
    "hiring": {
        "category": "Hiring",
        "linkedin_query": "hiring manager OR recruiter OR HR director OR talent acquisition",
        "web_query": "hiring manager OR recruiter OR HR director OR talent acquisition",
    },
    "operations": {
        "category": "Operations & Fleet Management",
        "linkedin_query": "fleet manager OR operations OR safety",
        "web_query": "fleet manager OR operations manager OR safety director OR dispatch manager",
    },
}


def _search_persona(
    persona_key: str,
    company_name: str,
    company_domain: str | None,
) -> dict[str, DecisionMaker]:
    """
    Search for decision makers of one persona type.
    Runs LinkedIn search first, then web search if LinkedIn found nothing.
    Each call creates its own requests.Session (thread-safe).
    """
    config = PERSONA_CONFIG[persona_key]
    session = requests.Session()
    local_people: dict[str, DecisionMaker] = {}

    # --- LinkedIn search ---
    log.info("[%s] Searching LinkedIn…", persona_key)
    query = f'site:linkedin.com/in/ "{company_name}" {config["linkedin_query"]}'
    data = search(query, session=session, max_results=5)
    if data:
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
            full_text = f"{name_field} {content}"
            if not _company_in_result(company_name, full_text, name):
                log.info("[%s] Skipping '%s' — company '%s' not confirmed in LinkedIn result",
                         persona_key, name, company_name)
                continue

            _add_person(local_people, name, title or "Unknown Role", "LinkedIn",
                        company_name=company_name, linkedin=url)

    # --- Web search (only if LinkedIn found 0 people with LinkedIn URLs) ---
    has_linkedin = any(p.linkedin for p in local_people.values())
    if not has_linkedin:
        log.info("[%s] No LinkedIn results. Trying web search…", persona_key)
        domain_hint = f"site:{company_domain}" if company_domain else ""
        query = f'"{company_name}" {config["web_query"]} {domain_hint}'.strip()
        data = search_structured(query, PEOPLE_SCHEMA, session=session, depth="deep")
        if data and "people" in data:
            for person in data["people"]:
                name = person.get("name", "").strip()
                title = person.get("title", "").strip()
                if name and title:
                    _add_person(local_people, name, title, "Web Search",
                                company_name=company_name)

    log.info("[%s] Found %d people", persona_key, len(local_people))
    return local_people


# ---------------------------------------------------------------------------
# Merge helper
# ---------------------------------------------------------------------------

def _merge_people(
    target: dict[str, DecisionMaker],
    source_dict: dict[str, DecisionMaker],
) -> None:
    """Merge people from source_dict into target, handling duplicates."""
    for key, person in source_dict.items():
        if key in target:
            existing = target[key]
            # Keep the higher-priority source
            if SOURCE_PRIORITY.get(person.source, 99) < SOURCE_PRIORITY.get(existing.source, 99):
                existing.source = person.source
                existing.title = person.title
            # Upgrade LinkedIn URL
            if person.linkedin and not existing.linkedin:
                existing.linkedin = person.linkedin
            # Upgrade confidence
            rank = {"High": 2, "Medium": 1, "Low": 0}
            if rank.get(person.confidence, 0) > rank.get(existing.confidence, 0):
                existing.confidence = person.confidence
        else:
            target[key] = person


# ---------------------------------------------------------------------------
# Fallback — broad search (only if 0 high-confidence people)
# ---------------------------------------------------------------------------

def _fallback_search(
    company_name: str,
    all_people: dict,
    session: requests.Session,
) -> None:
    """Last resort: find anyone associated with the company."""
    log.info("Fallback: Doing broad search for '%s'…", company_name)
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
    Search for decision makers using 3 parallel persona scrapers + fallback.
    Returns up to MAX_CONTACTS_PER_COMPANY people, sorted by confidence.
    """
    all_people: dict[str, DecisionMaker] = {}

    # --- Phase 1: Run 3 persona scrapers in parallel ---
    log.info("=== Searching for decision makers (3 parallel scrapers) ===")
    persona_keys = ["owners", "hiring", "operations"]

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = {
            executor.submit(_search_persona, key, company_name, company_domain): key
            for key in persona_keys
        }
        for future in concurrent.futures.as_completed(futures):
            key = futures[future]
            try:
                local_people = future.result()
                _merge_people(all_people, local_people)
            except Exception as exc:
                log.error("Scraper '%s' failed: %s", key, exc)

    # --- Phase 2: Fallback (only if 0 high-confidence people) ---
    high_count = sum(1 for p in all_people.values() if p.confidence == "High")
    if high_count == 0:
        log.info("=== No high-confidence contacts. Running fallback… ===")
        _fallback_search(company_name, all_people, session or requests.Session())

    # --- Phase 3: Sort and trim ---
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
