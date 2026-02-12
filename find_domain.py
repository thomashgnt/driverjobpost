#!/usr/bin/env python3
"""
Google Search Scraper (Linkup API) - Find company websites from the truckers report CSV.

Reads company names from the CSV file and uses the Linkup search API
to find their official websites. Results are saved to a new CSV file.
"""

import csv
import os
import time
import logging
import argparse
from urllib.parse import urlparse

import requests
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
INPUT_CSV = "Scrapping job offer - thetruckersreport.com.csv"
OUTPUT_CSV = "company_websites.csv"

LINKUP_API_URL = "https://api.linkup.so/v1/search"
LINKUP_API_KEY = os.getenv("LINKUP_API_KEY", "")

# Domains to skip (aggregators / job boards, not company sites)
SKIP_DOMAINS = {
    "thetruckersreport.com",
    "indeed.com",
    "glassdoor.com",
    "linkedin.com",
    "facebook.com",
    "twitter.com",
    "x.com",
    "instagram.com",
    "youtube.com",
    "yelp.com",
    "bbb.org",
    "ziprecruiter.com",
    "salary.com",
    "crunchbase.com",
    "dnb.com",
    "zoominfo.com",
    "wikipedia.org",
    "google.com",
    "mapquest.com",
    "yellowpages.com",
    "manta.com",
    "trustpilot.com",
    "tiktok.com",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _is_skip_domain(url: str) -> bool:
    """Return True if the URL belongs to a domain we want to ignore."""
    try:
        host = urlparse(url).netloc.lower()
        for d in SKIP_DOMAINS:
            if host == d or host.endswith(f".{d}"):
                return True
    except Exception:
        return False
    return False


def search_linkup(query: str, session: requests.Session, api_key: str, retries: int = 3) -> str | None:
    """
    Search via Linkup API and return the first non-skipped result URL.
    Returns None if nothing useful was found.
    """
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    payload = {
        "q": query,
        "depth": "standard",
        "outputType": "searchResults",
        "maxResults": 5,
    }

    for attempt in range(retries):
        try:
            resp = session.post(
                LINKUP_API_URL,
                json=payload,
                headers=headers,
                timeout=30,
            )

            if resp.status_code == 429:
                wait = 10 * (attempt + 1)
                log.warning("Rate-limited (429). Waiting %ds before retry…", wait)
                time.sleep(wait)
                continue

            resp.raise_for_status()
            data = resp.json()

            # Extract results — Linkup returns { "results": [ { "name", "url", "content" }, ... ] }
            results = data.get("results", [])
            for result in results:
                url = result.get("url", "")
                if url and not _is_skip_domain(url):
                    return url

            return None

        except requests.RequestException as exc:
            log.warning("Request error (attempt %d/%d): %s", attempt + 1, retries, exc)
            time.sleep(3 * (attempt + 1))

    return None


def read_company_names(path: str) -> list[str]:
    """Read unique company names from the CSV, preserving order."""
    names: list[str] = []
    seen: set[str] = set()

    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = row.get("Company Name", "").strip()
            if name and name not in seen:
                seen.add(name)
                names.append(name)

    return names


def _write_output(path: str, results: list[dict], ordered_companies: list[str]) -> None:
    """Write results to CSV, maintaining the original company order."""
    lookup = {r["Company Name"]: r for r in results}
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["Company Name", "Website"])
        writer.writeheader()
        for name in ordered_companies:
            if name in lookup:
                writer.writerow(lookup[name])


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Find company websites via Linkup Search API."
    )
    parser.add_argument(
        "-i", "--input",
        default=INPUT_CSV,
        help=f"Input CSV file (default: {INPUT_CSV})",
    )
    parser.add_argument(
        "-o", "--output",
        default=OUTPUT_CSV,
        help=f"Output CSV file (default: {OUTPUT_CSV})",
    )
    parser.add_argument(
        "--api-key",
        default=LINKUP_API_KEY,
        help="Linkup API key (default: from .env file)",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=1.0,
        help="Delay between requests in seconds (default: 1)",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from an existing output file (skip already-processed companies)",
    )
    args = parser.parse_args()

    # Read company names
    companies = read_company_names(args.input)
    log.info("Found %d unique companies in %s", len(companies), args.input)

    # If resuming, load already-processed companies
    already_done: dict[str, str] = {}
    if args.resume:
        try:
            with open(args.output, newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    already_done[row["Company Name"]] = row.get("Website", "")
            log.info("Resuming – %d companies already processed", len(already_done))
        except FileNotFoundError:
            pass

    # Process
    session = requests.Session()
    results: list[dict[str, str]] = []

    # Carry forward already-done results
    for name in companies:
        if name in already_done:
            results.append({"Company Name": name, "Website": already_done[name]})

    remaining = [c for c in companies if c not in already_done]
    log.info("Companies to process: %d", len(remaining))

    for i, company in enumerate(remaining, start=1):
        query = f"{company} trucking company official website"
        log.info("[%d/%d] Searching: %s", i, len(remaining), company)

        website = search_linkup(query, session, args.api_key)

        if website:
            log.info("  -> %s", website)
        else:
            log.warning("  -> No website found")

        results.append({
            "Company Name": company,
            "Website": website or "",
        })

        # Write after every lookup so progress is not lost
        _write_output(args.output, results, companies)

        # Small delay between requests
        if i < len(remaining):
            time.sleep(args.delay)

    log.info("Done! Results written to %s", args.output)


if __name__ == "__main__":
    main()
