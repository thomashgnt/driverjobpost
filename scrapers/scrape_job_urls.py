#!/usr/bin/env python3
"""
Scrape Job URLs from a listing page
====================================
Extracts individual job posting URLs from a job board listing page.

Usage:
    # Extract all job URLs from a listing page
    python3 scrapers/scrape_job_urls.py "https://amazon-na.fountain.com/amazon-delivery-service-partner/apply/"

    # Save to a specific file
    python3 scrapers/scrape_job_urls.py -o urls.txt "https://..."

    # Then feed them into the pipeline
    python3 scrapers/pipeline.py -f urls.txt --clay-jobs "..." --clay-contacts "..."

Strategy:
  1. Use Linkup fetch to render the page (handles JavaScript)
  2. Extract all links matching job-posting patterns
  3. Use Linkup search as fallback to find more indexed URLs
  4. Deduplicate and save
"""

import sys
import os
import re
import logging
import argparse
from urllib.parse import urlparse, urljoin

import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scrapers.linkup_client import fetch_url_content, search

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Known job board URL patterns (regex → True if it's a single job URL)
# ---------------------------------------------------------------------------
JOB_URL_PATTERNS = [
    # Fountain: /apply/.../opening/...
    re.compile(r'fountain\.com/apply/.+/opening/.+'),
    # Fountain alternate: /jobs/... (with a slug, not just the brand page)
    re.compile(r'work\..+\.fountain\.com/jobs/[A-Za-z]+-[A-Za-z]'),
    # TheTruckersReport: /jobs/profile/...
    re.compile(r'thetruckersreport\.com/jobs/profile/.+'),
    # Indeed: /viewjob or /rc/clk
    re.compile(r'indeed\.com/(viewjob|rc/clk|jobs)\?'),
    # ZipRecruiter: /c/.../job/...
    re.compile(r'ziprecruiter\.com/c/.+/job/.+'),
    # Glassdoor: /job-listing/...
    re.compile(r'glassdoor\.com/job-listing/.+'),
    # CDL Life: /jobs/...
    re.compile(r'cdllife\.com/jobs/.+'),
    # Generic: anything with /job/ or /opening/ or /position/ in the path
    re.compile(r'/(?:job|opening|position|vacancy|posting)/[^/]+$'),
]


def _is_job_url(url: str) -> bool:
    """Check if a URL looks like an individual job posting."""
    for pattern in JOB_URL_PATTERNS:
        if pattern.search(url):
            return True
    return False


def _extract_urls_from_markdown(markdown: str, base_url: str) -> list[str]:
    """Extract all URLs from markdown content."""
    urls: list[str] = []

    # Find markdown links: [text](url)
    for match in re.finditer(r'\[.*?\]\((https?://[^\s)]+)\)', markdown):
        urls.append(match.group(1))

    # Find bare URLs
    for match in re.finditer(r'(?<!\()(https?://[^\s)\]"<>]+)', markdown):
        url = match.group(0)
        if url not in urls:
            urls.append(url)

    # Find relative paths that look like job links
    for match in re.finditer(r'\(/([^)\s"]+)\)', markdown):
        path = match.group(1)
        full = urljoin(base_url, "/" + path)
        if full not in urls:
            urls.append(full)

    return urls


def scrape_from_page(listing_url: str, session: requests.Session) -> list[str]:
    """Strategy 1: Fetch the listing page and extract job URLs."""
    log.info("Fetching listing page: %s", listing_url)
    markdown = fetch_url_content(listing_url, session=session)

    if not markdown:
        log.warning("Could not fetch listing page")
        return []

    log.info("Page content: %d characters", len(markdown))

    all_urls = _extract_urls_from_markdown(markdown, listing_url)
    job_urls = [u for u in all_urls if _is_job_url(u)]

    log.info("Found %d total links, %d look like job postings", len(all_urls), len(job_urls))
    return job_urls


def scrape_from_search(listing_url: str, session: requests.Session) -> list[str]:
    """Strategy 2: Use Linkup search to find indexed job URLs."""
    parsed = urlparse(listing_url)
    host = parsed.netloc

    # For Fountain, search for the opening pattern
    if "fountain.com" in host:
        site_query = f"site:{host}/apply/ opening"
    else:
        site_query = f"site:{host} job opening"

    log.info("Searching indexed pages: %s", site_query)

    job_urls: list[str] = []
    # Do multiple searches with different terms to find more results
    search_queries = [
        site_query,
        f"site:{host} delivery associate opening",
        f"site:{host} driver opening",
    ]

    for query in search_queries:
        data = search(query, session=session, max_results=10)
        if not data:
            continue
        for result in data.get("results", []):
            url = result.get("url", "")
            if url and _is_job_url(url) and url not in job_urls:
                job_urls.append(url)

    log.info("Search found %d job URLs", len(job_urls))
    return job_urls


def scrape_job_urls(listing_url: str, session: requests.Session | None = None) -> list[str]:
    """
    Main function: extract all job URLs from a listing page.
    Combines page scraping + search indexing, deduplicates.
    """
    sess = session or requests.Session()
    seen: set[str] = set()
    all_urls: list[str] = []

    # Strategy 1: Fetch and parse the listing page
    page_urls = scrape_from_page(listing_url, sess)
    for url in page_urls:
        if url not in seen:
            seen.add(url)
            all_urls.append(url)

    # Strategy 2: Search for indexed job URLs
    search_urls = scrape_from_search(listing_url, sess)
    for url in search_urls:
        if url not in seen:
            seen.add(url)
            all_urls.append(url)

    log.info("Total unique job URLs found: %d", len(all_urls))
    return all_urls


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Extract job posting URLs from a listing page",
    )
    parser.add_argument(
        "url",
        help="The listing page URL (e.g. https://amazon-na.fountain.com/.../apply/)",
    )
    parser.add_argument(
        "-o", "--output",
        default="job_urls.txt",
        help="Output file (default: job_urls.txt)",
    )
    args = parser.parse_args()

    session = requests.Session()
    urls = scrape_job_urls(args.url, session)

    if not urls:
        print("No job URLs found. The page might use dynamic loading.")
        print("Try opening the page in a browser and checking the network tab")
        print("for an API endpoint that returns the job list.")
        sys.exit(1)

    # Save to file
    with open(args.output, "w", encoding="utf-8") as f:
        for url in urls:
            f.write(url + "\n")

    print(f"\n{'=' * 60}")
    print(f"  Found {len(urls)} job URLs")
    print(f"  Saved to: {args.output}")
    print(f"{'=' * 60}")
    print(f"\n  Next step — run the pipeline:")
    print(f"  python3 scrapers/pipeline.py -f {args.output}")
    print()


if __name__ == "__main__":
    main()
