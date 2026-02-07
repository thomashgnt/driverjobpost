"""
Step 3: Find decision makers (CEO, VP, hiring managers) for a company.

Uses Linkup structured search to get clean name + title pairs.
"""

import json
import logging
from dataclasses import dataclass

import requests

from scrapers.linkup_client import search_structured

log = logging.getLogger(__name__)


@dataclass
class DecisionMaker:
    name: str
    title: str
    source: str  # the query that found this person


# Titles we want to keep — matched as lowercase substrings
WANTED_TITLE_KEYWORDS = [
    # C-suite / owners
    "ceo", "chief", "owner", "founder", "president", "partner",
    # VP level
    "vp", "vice president",
    # Directors / Heads
    "director", "head of",
    # Operations & fleet
    "operations", "fleet", "transportation", "logistics", "dispatch", "safety",
    # Recruiting / HR
    "hiring", "recruiter", "recruiting", "talent", "human resources", "hr ",
    # Manager level (broad)
    "manager",
]

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
                    "name": {
                        "type": "string",
                        "description": "Full name of the person",
                    },
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


def _is_relevant_title(title: str) -> bool:
    """Return True if the title matches one of the wanted keywords."""
    lower = title.lower()
    return any(kw in lower for kw in WANTED_TITLE_KEYWORDS)


def find_decision_makers(
    company_name: str,
    company_domain: str | None = None,
    session: requests.Session | None = None,
) -> list[DecisionMaker]:
    """
    Search for decision makers at the given company.
    Returns a list of DecisionMaker objects, deduplicated by name,
    filtered to only keep relevant titles (C-suite, VP, directors,
    operations, fleet, safety, HR, recruiting, managers).
    """
    sess = session or requests.Session()
    all_people: dict[str, DecisionMaker] = {}  # keyed by name to dedup

    domain_hint = f"site:{company_domain}" if company_domain else ""
    queries = [
        f"{company_name} CEO owner founder president {domain_hint}".strip(),
        f"{company_name} VP director head of operations fleet safety hiring manager recruiter trucking",
    ]

    for query in queries:
        log.info("Searching decision makers: %s", query)
        data = search_structured(query, PEOPLE_SCHEMA, session=sess, depth="deep")

        if data and "people" in data:
            for person in data["people"]:
                name = person.get("name", "").strip()
                title = person.get("title", "").strip()
                if not name or name.lower() in ("unknown", "n/a", ""):
                    continue
                if not title or not _is_relevant_title(title):
                    log.debug("Skipping '%s' with irrelevant title '%s'", name, title)
                    continue
                if name not in all_people:
                    all_people[name] = DecisionMaker(
                        name=name,
                        title=title,
                        source=query,
                    )

    result = list(all_people.values())
    log.info("Found %d decision makers for '%s'", len(result), company_name)
    return result
