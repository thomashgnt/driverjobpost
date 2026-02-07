#!/usr/bin/env python3
"""
Job Offer Pipeline
==================
One command to rule them all:

    # One URL
    python3 scrapers/pipeline.py "https://www.thetruckersreport.com/jobs/..."

    # Multiple URLs
    python3 scrapers/pipeline.py "url1" "url2" "url3"

    # From a file (one URL per line)
    python3 scrapers/pipeline.py -f urls.txt

    # Resume after interruption
    python3 scrapers/pipeline.py -f urls.txt --resume

This will, for each URL:
  1. Scrape the job posting (title, company, description)
  2. Find the company's official website
  3. Find decision makers (CEO, VP, hiring managers)
  4. Save everything to a CSV
"""

import sys
import os
import csv
import logging
import argparse
import time
from datetime import datetime

import requests

# Allow running as: python3 scrapers/pipeline.py
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scrapers.scrape_job import scrape_job
from scrapers.find_domain import find_company_domain
from scrapers.find_decision_makers import find_decision_makers

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

OUTPUT_CSV = "pipeline_results.csv"
CSV_FIELDS = [
    "Timestamp",
    "Job URL",
    "Job Title",
    "Company Name",
    "Company Website",
    "Decision Maker Name",
    "Decision Maker Title",
]


def _load_already_done(path: str) -> set[str]:
    """Load URLs already processed from an existing output CSV."""
    done: set[str] = set()
    try:
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                done.add(row.get("Job URL", ""))
    except FileNotFoundError:
        pass
    return done


def _ensure_csv_header(path: str) -> None:
    """Create the CSV with header if it doesn't exist yet."""
    if not os.path.exists(path):
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
            writer.writeheader()


def _append_results(path: str, url: str, job_title: str, company: str,
                    website: str, makers: list) -> None:
    """Append results for one URL to the CSV."""
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        if makers:
            for dm in makers:
                writer.writerow({
                    "Timestamp": datetime.now().isoformat(),
                    "Job URL": url,
                    "Job Title": job_title,
                    "Company Name": company,
                    "Company Website": website,
                    "Decision Maker Name": dm.name,
                    "Decision Maker Title": dm.title,
                })
        else:
            writer.writerow({
                "Timestamp": datetime.now().isoformat(),
                "Job URL": url,
                "Job Title": job_title,
                "Company Name": company,
                "Company Website": website,
                "Decision Maker Name": "",
                "Decision Maker Title": "",
            })


def process_one_url(url: str, session: requests.Session, output_path: str) -> None:
    """Run the full pipeline for a single job URL."""

    # -- Step 1: Scrape the job posting --
    print("\n  STEP 1: Scraping job posting…")
    job = scrape_job(url, session=session)
    print(f"    Job Title : {job.title}")
    print(f"    Company   : {job.company_name}")
    print(f"    Desc      : {job.description[:120]}…")

    # -- Step 2: Find company website --
    print("  STEP 2: Finding company website…")
    domain = find_company_domain(job.company_name, session=session)
    print(f"    Website   : {domain or 'NOT FOUND'}")

    # -- Step 3: Find decision makers --
    print("  STEP 3: Finding decision makers…")
    makers = find_decision_makers(job.company_name, domain, session=session)
    if makers:
        for i, dm in enumerate(makers, 1):
            print(f"    {i}. {dm.name} — {dm.title}")
    else:
        print("    No decision makers found.")

    # -- Save immediately --
    _append_results(output_path, url, job.title, job.company_name,
                    domain or "", makers)


def collect_urls(args) -> list[str]:
    """Collect URLs from arguments and/or file, deduplicated."""
    urls: list[str] = []
    seen: set[str] = set()

    # From positional arguments
    for u in (args.urls or []):
        u = u.strip()
        if u and u not in seen:
            seen.add(u)
            urls.append(u)

    # From file
    if args.file:
        with open(args.file, encoding="utf-8") as f:
            for line in f:
                u = line.strip()
                if u and not u.startswith("#") and u not in seen:
                    seen.add(u)
                    urls.append(u)

    return urls


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Pipeline: Job URL -> Company -> Domain -> Decision Makers",
    )
    parser.add_argument(
        "urls",
        nargs="*",
        help="One or more job posting URLs",
    )
    parser.add_argument(
        "-f", "--file",
        help="Text file with one URL per line",
    )
    parser.add_argument(
        "-o", "--output",
        default=OUTPUT_CSV,
        help=f"Output CSV file (default: {OUTPUT_CSV})",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=1.0,
        help="Delay in seconds between URLs (default: 1)",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Skip URLs already present in the output CSV",
    )
    args = parser.parse_args()

    # Collect all URLs
    urls = collect_urls(args)
    if not urls:
        print("No URLs provided. Use: pipeline.py URL [URL ...] or pipeline.py -f urls.txt")
        sys.exit(1)

    # Resume: skip already-done URLs
    if args.resume:
        already_done = _load_already_done(args.output)
        before = len(urls)
        urls = [u for u in urls if u not in already_done]
        log.info("Resume: %d already done, %d remaining", before - len(urls), len(urls))

    _ensure_csv_header(args.output)

    total = len(urls)
    log.info("Processing %d URL(s)…", total)

    session = requests.Session()

    for i, url in enumerate(urls, start=1):
        print(f"\n{'=' * 60}")
        print(f"  [{i}/{total}] {url}")
        print(f"{'=' * 60}")

        try:
            process_one_url(url, session, args.output)
        except Exception as exc:
            log.error("Failed to process %s: %s", url, exc)
            print(f"  ERROR: {exc} — skipping to next URL")

        if i < total:
            time.sleep(args.delay)

    print(f"\n{'=' * 60}")
    print(f"  DONE! {total} URL(s) processed.")
    print(f"  Results saved to: {args.output}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
