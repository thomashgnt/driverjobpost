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


PEOPLE_SCHEMA = json.dumps({
    "type": "object",
    "properties": {
        "people": {
            "type": "array",
            "description": "List of decision makers found for this company",
            "items": {
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "description": "Full name of the person",
                    },
                    "title": {
                        "type": "string",
                        "description": "Job title or role (e.g. CEO, VP Operations, Hiring Manager)",
                    },
                },
                "required": ["name", "title"],
            },
        },
    },
    "required": ["people"],
})


def find_decision_makers(
    company_name: str,
    company_domain: str | None = None,
    session: requests.Session | None = None,
) -> list[DecisionMaker]:
    """
    Search for decision makers at the given company.
    Returns a list of DecisionMaker objects, deduplicated by name.
    """
    sess = session or requests.Session()
    all_people: dict[str, DecisionMaker] = {}  # keyed by name to dedup

    domain_hint = f"site:{company_domain}" if company_domain else ""
    queries = [
        f"{company_name} CEO owner founder {domain_hint}".strip(),
        f"{company_name} leadership team VP director hiring manager trucking",
    ]

    for query in queries:
        log.info("Searching decision makers: %s", query)
        data = search_structured(query, PEOPLE_SCHEMA, session=sess, depth="deep")

        if data and "people" in data:
            for person in data["people"]:
                name = person.get("name", "").strip()
                title = person.get("title", "").strip()
                if name and name.lower() not in ("unknown", "n/a", ""):
                    if name not in all_people:
                        all_people[name] = DecisionMaker(
                            name=name,
                            title=title or "Unknown Role",
                            source=query,
                        )

    result = list(all_people.values())
    log.info("Found %d decision makers for '%s'", len(result), company_name)
    return result
