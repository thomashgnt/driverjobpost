#!/usr/bin/env python3
"""
Google Search Scraper - Find company websites from the truckers report CSV.

Reads company names from the CSV file and searches Google to find their websites.
Results are saved to a new CSV file with the company name and discovered website URL.
"""

import csv
import re
import time
import random
import logging
import argparse
from urllib.parse import quote_plus, urlparse

import requests

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
INPUT_CSV = "Scrapping job offer - thetruckersreport.com.csv"
OUTPUT_CSV = "company_websites.csv"

GOOGLE_SEARCH_URL = "https://www.google.com/search"

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.4; rv:125.0) Gecko/20100101 Firefox/125.0",
]

# Domains to skip (these are aggregators / job boards, not company sites)
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

def _random_ua() -> str:
    return random.choice(USER_AGENTS)


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


def _extract_urls_from_html(html: str) -> list[str]:
    """
    Extract result URLs from Google search HTML without BeautifulSoup.
    Google embeds result links in <a href="/url?q=ACTUAL_URL&..."> tags.
    """
    urls: list[str] = []

    # Pattern 1: /url?q= redirects (standard Google results)
    for match in re.finditer(r'/url\?q=(https?://[^&"]+)', html):
        url = match.group(1)
        if not _is_skip_domain(url):
            urls.append(url)

    # Pattern 2: Direct href links starting with http (fallback)
    if not urls:
        for match in re.finditer(r'href="(https?://(?!www\.google)[^"]+)"', html):
            url = match.group(1)
            if not _is_skip_domain(url):
                urls.append(url)

    # Deduplicate while keeping order
    seen = set()
    unique: list[str] = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            unique.append(u)
    return unique


def search_google(query: str, session: requests.Session, retries: int = 3) -> str | None:
    """
    Search Google for *query* and return the first non-skipped result URL.
    Returns None if nothing useful was found.
    """
    params = {
        "q": query,
        "num": "10",
        "hl": "en",
    }
    headers = {
        "User-Agent": _random_ua(),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.google.com/",
    }

    for attempt in range(retries):
        try:
            resp = session.get(
                GOOGLE_SEARCH_URL,
                params=params,
                headers=headers,
                timeout=15,
            )

            if resp.status_code == 429:
                wait = 30 * (attempt + 1)
                log.warning("Rate-limited (429). Waiting %ds before retry…", wait)
                time.sleep(wait)
                continue

            resp.raise_for_status()

            urls = _extract_urls_from_html(resp.text)
            return urls[0] if urls else None

        except requests.RequestException as exc:
            log.warning("Request error (attempt %d/%d): %s", attempt + 1, retries, exc)
            time.sleep(5 * (attempt + 1))

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


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Find company websites via Google Search."
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
        "--delay-min",
        type=float,
        default=4.0,
        help="Minimum delay between requests in seconds (default: 4)",
    )
    parser.add_argument(
        "--delay-max",
        type=float,
        default=8.0,
        help="Maximum delay between requests in seconds (default: 8)",
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

        website = search_google(query, session)

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

        # Random delay to avoid rate-limiting
        if i < len(remaining):
            delay = random.uniform(args.delay_min, args.delay_max)
            time.sleep(delay)

    log.info("Done! Results written to %s", args.output)


def _write_output(path: str, results: list[dict], ordered_companies: list[str]) -> None:
    """Write results to CSV, maintaining the original company order."""
    lookup = {r["Company Name"]: r for r in results}
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["Company Name", "Website"])
        writer.writeheader()
        for name in ordered_companies:
            if name in lookup:
                writer.writerow(lookup[name])


if __name__ == "__main__":
    main()
