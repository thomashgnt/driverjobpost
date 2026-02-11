#!/usr/bin/env python3
"""
Job Offer Pipeline
==================
One command to rule them all:

    # One URL
    python3 scrapers/pipeline.py "https://www.thetruckersreport.com/jobs/..."

    # Multiple URLs
    python3 scrapers/pipeline.py "url1" "url2" "url3"

    # From a file (one URL per line, or CSV with URL,Title columns)
    python3 scrapers/pipeline.py -f urls.txt
    python3 scrapers/pipeline.py -f job_offers.csv

    # Resume after interruption
    python3 scrapers/pipeline.py -f urls.txt --resume

    # Push results to Clay (2 tables)
    python3 scrapers/pipeline.py --clay-jobs "WEBHOOK_URL" --clay-contacts "WEBHOOK_URL" "url1"

This will, for each URL:
  1. Scrape the job posting (title, company, description, contact)
  2. Find the company's official website
  3. Find decision makers (5-priority deep search)
  4. Find LinkedIn profiles (skip if already found in step 3)
  5. Save everything to a CSV + push to Clay (if configured)
"""

import sys
import os
import csv
import logging
import argparse
import re
import time
from datetime import datetime
from urllib.parse import urlparse

import requests

# Allow running as: python3 scrapers/pipeline.py
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scrapers.scrape_job import (
    scrape_job, JobInfo, _extract_real_company, _is_fountain_amazon_dsp,
)
from scrapers.find_domain import find_company_domain
from scrapers.find_decision_makers import find_decision_makers
from scrapers.find_linkedin import find_linkedin_url

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
    "Job Board",
    "Job Title",
    "Company Name",
    "Company Website",
    "Decision Maker Name",
    "Decision Maker Title",
    "Category",
    "Mentioned in Job Posting",
    "Source",
    "LinkedIn",
    "Status",
]

# ---------------------------------------------------------------------------
# Job Board name normalization
# ---------------------------------------------------------------------------

# Known job boards → clean name for email templates
KNOWN_JOB_BOARDS: dict[str, str] = {
    "thetruckersreport.com": "The Truckers Report",
    "fountain.com": "Fountain",
    "indeed.com": "Indeed",
    "glassdoor.com": "Glassdoor",
    "ziprecruiter.com": "ZipRecruiter",
    "linkedin.com": "LinkedIn",
    "cdllife.com": "CDL Life",
    "truckingtruth.com": "Trucking Truth",
    "truckdrivingjobs.com": "Truck Driving Jobs",
    "driverjobsite.com": "Driver Job Site",
    "jobisjob.com": "JobisJob",
    "simplyhired.com": "SimplyHired",
    "careerbuilder.com": "CareerBuilder",
    "monster.com": "Monster",
    "driveforwardtransportation.com": "Drive Forward Transportation",
    "livetrucking.com": "Live Trucking",
    "truckerjobusa.com": "Trucker Job USA",
}

# Subdomain prefixes that hint at a company/region (e.g. "amazon-na" in "amazon-na.fountain.com")
# These get turned into a prefix like "Amazon NA"


def _normalize_job_board(url: str) -> str:
    """
    Derive a clean, human-readable job board name from a URL.

    Examples:
        thetruckersreport.com/jobs/...     → "The Truckers Report"
        amazon-na.fountain.com/...          → "Amazon NA - Fountain"
        indeed.com/viewjob?jk=abc           → "Indeed"
        some-unknown-board.com/jobs/123     → "Some Unknown Board"
    """
    try:
        host = urlparse(url).netloc.lower()
    except Exception:
        return "Unknown"

    if not host:
        return "Unknown"

    # Remove www.
    host = re.sub(r'^www\.', '', host)

    # Check exact match first (e.g. "thetruckersreport.com")
    # Extract base domain (last 2 parts: "fountain.com" from "amazon-na.fountain.com")
    parts = host.split(".")
    if len(parts) >= 2:
        base_domain = ".".join(parts[-2:])
    else:
        base_domain = host

    # Check if base domain is known
    if base_domain in KNOWN_JOB_BOARDS:
        board_name = KNOWN_JOB_BOARDS[base_domain]

        # If there's a subdomain prefix (e.g. "amazon-na"), add it
        subdomain = host.replace(f".{base_domain}", "").replace(base_domain, "")
        skip_subs = {"www", "jobs", "careers", "apply", "hire", "app", "m"}
        if subdomain and subdomain not in skip_subs:
            # "amazon-na" → "Amazon NA"
            prefix = subdomain.replace("-", " ").replace(".", " ").strip().title()
            # Uppercase 2-letter tokens (NA, US, EU...)
            prefix = re.sub(r'\b([A-Za-z]{2})\b', lambda m: m.group(1).upper(), prefix)
            return f"{prefix} - {board_name}"
        return board_name

    # Unknown domain → generate a clean name from the domain
    # "some-trucking-board.com" → "Some Trucking Board"
    domain_name = base_domain.rsplit(".", 1)[0]  # remove TLD
    clean = domain_name.replace("-", " ").replace("_", " ").strip().title()
    return clean or "Unknown"


# ---------------------------------------------------------------------------
# CSV title pre-parsing (extract company name without Linkup API call)
# ---------------------------------------------------------------------------

# Patterns to strip from CSV titles when extracting a clean job title
_PRICE_RE = re.compile(r'\$[\d,.]+(?:/hr)?(?:\s*HR)?\+?\s*[:\-–—]?\s*', re.IGNORECASE)
_STATION_CODE_RE = re.compile(r'\b[A-Z]{1,4}\d{1,2}\b')


def _extract_job_title_from_csv(csv_title: str, company: str) -> str:
    """Extract a clean job title from a CSV title string.

    "$18/hr - Delivery Helper - Next Steps Logistics LLC - Amazon DSP"
    → "Delivery Helper"
    """
    # Split by common separators
    for sep in (" - ", " – ", " — ", " | "):
        parts = csv_title.split(sep)
        if len(parts) >= 2:
            candidates = []
            for part in parts:
                p = part.strip()
                # Skip if it's the company name, "Amazon DSP", or a station code
                if not p:
                    continue
                if company and company.lower() in p.lower():
                    continue
                if "amazon" in p.lower() and "dsp" in p.lower():
                    continue
                if _STATION_CODE_RE.fullmatch(p.strip()):
                    continue
                # Strip leading price ("$18/hr - " or "$22.25 HR+ : ")
                p = _PRICE_RE.sub("", p).strip()
                if not p:
                    continue
                # Check if this looks like a job title (contains job-like words)
                lower = p.lower()
                job_words = ("driver", "delivery", "associate", "helper", "dispatcher",
                             "warehouse", "handler", "loader", "sorter", "cdl", "truck")
                if any(w in lower for w in job_words):
                    candidates.append(p)
            if candidates:
                return candidates[0]
    return ""


def _parse_job_from_csv_title(csv_title: str, url: str) -> JobInfo | None:
    """Try to extract job title and company from a CSV title string.

    Returns a partial JobInfo if company extraction succeeds, None otherwise.
    Only applies to Fountain Amazon DSP URLs.
    """
    if not csv_title:
        return None
    if not _is_fountain_amazon_dsp(url):
        return None

    company = _extract_real_company(csv_title, csv_title)

    # If we got back the original string or something useless, extraction failed
    if not company or company == csv_title or len(company) > 80:
        return None
    if "unknown" in company.lower() or "amazon" in company.lower():
        return None

    # Strip trailing parenthetical noise like "(DGR8" or "(DGR8 - GLAK)"
    company = re.sub(r'\s*\([^)]*$', '', company).strip()  # unclosed paren
    company = re.sub(r'\s*\([A-Z0-9]{2,5}(?:\s*[-–]\s*[A-Z0-9]{2,5})?\)', '', company).strip()

    job_title = _extract_job_title_from_csv(csv_title, company)

    return JobInfo(
        title=job_title or "Delivery Associate",
        company_name=company,
        description="",
        source_url=url,
    )


# ---------------------------------------------------------------------------
# Clay webhook helper
# ---------------------------------------------------------------------------

def _push_to_clay(webhook_url: str, data: dict, session: requests.Session) -> bool:
    """POST JSON to a Clay webhook. Retry 2x on failure."""
    for attempt in range(3):
        try:
            resp = session.post(webhook_url, json=data, timeout=15)
            resp.raise_for_status()
            return True
        except requests.RequestException as exc:
            if attempt < 2:
                time.sleep(2)
            else:
                log.error("Clay push failed: %s", exc)
                return False
    return False


# ---------------------------------------------------------------------------
# CSV helpers
# ---------------------------------------------------------------------------

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
    """Create or migrate the CSV header if columns changed."""
    if not os.path.exists(path):
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
            writer.writeheader()
        return

    # Check if existing header matches current fields
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        existing_header = next(reader, None)

    if existing_header != CSV_FIELDS:
        log.warning("CSV columns changed — migrating %s to new format…", path)
        rows = []
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                rows.append(row)
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
            writer.writeheader()
            for row in rows:
                writer.writerow({field: row.get(field, "") for field in CSV_FIELDS})


def _append_results(path: str, url: str, job_board: str, job_title: str,
                    company: str, website: str, makers: list) -> None:
    """Append results for one URL to the CSV."""
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        if makers:
            for dm in makers:
                writer.writerow({
                    "Timestamp": datetime.now().isoformat(),
                    "Job URL": url,
                    "Job Board": job_board,
                    "Job Title": job_title,
                    "Company Name": company,
                    "Company Website": website,
                    "Decision Maker Name": dm.name,
                    "Decision Maker Title": dm.title,
                    "Category": dm.category,
                    "Mentioned in Job Posting": "Yes" if dm.mentioned_in_job_posting else "No",
                    "Source": dm.source,
                    "LinkedIn": dm.linkedin or "",
                    "Status": "Valid" if dm.linkedin else "Invalid",
                })
        else:
            writer.writerow({
                "Timestamp": datetime.now().isoformat(),
                "Job URL": url,
                "Job Board": job_board,
                "Job Title": job_title,
                "Company Name": company,
                "Company Website": website,
                "Decision Maker Name": "",
                "Decision Maker Title": "",
                "Category": "",
                "Mentioned in Job Posting": "",
                "Source": "",
                "LinkedIn": "",
                "Status": "",
            })


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def process_one_url(
    url: str,
    session: requests.Session,
    output_path: str,
    clay_jobs_url: str | None = None,
    clay_contacts_url: str | None = None,
    csv_title: str = "",
) -> dict:
    """Run the full pipeline for a single job URL. Returns Clay push counts."""
    clay_counts = {"jobs": 0, "contacts": 0}

    # -- Derive job board name from URL --
    job_board = _normalize_job_board(url)

    # -- Pre-parse company from CSV title (saves Linkup credits) --
    csv_job = _parse_job_from_csv_title(csv_title, url) if csv_title else None
    if csv_job:
        print(f"\n  [CSV PRE-PARSE] Company: {csv_job.company_name}")

    # -- Step 1: Scrape the job posting --
    print("\n  STEP 1: Scraping job posting…")
    job = scrape_job(url, session=session)

    # Override company if Linkup gave a bad result but CSV has the real one
    if csv_job:
        linkup_co = job.company_name.lower()
        if (linkup_co in ("unknown company", "unknown", "")
                or "amazon" in linkup_co):
            log.info("Using CSV company '%s' (Linkup gave '%s')",
                     csv_job.company_name, job.company_name)
            job.company_name = csv_job.company_name
            if not job.title or job.title.lower() in ("unknown", "unknown title"):
                job.title = csv_job.title

    print(f"    Job Board : {job_board}")
    print(f"    Job Title : {job.title}")
    print(f"    Company   : {job.company_name}")
    print(f"    Desc      : {job.description[:120]}…")
    if job.contact_name:
        print(f"    Contact   : {job.contact_name} ({job.contact_email or 'no email'})")

    # -- Step 2: Find company website --
    print("  STEP 2: Finding company website…")
    domain = find_company_domain(job.company_name, session=session)
    print(f"    Website   : {domain or 'NOT FOUND'}")

    # -- Step 3: Find decision makers (5-priority deep search) --
    print("  STEP 3: Finding decision makers (deep search)…")
    makers = find_decision_makers(
        company_name=job.company_name,
        company_domain=domain,
        job_description=job.description,
        contact_name=job.contact_name,
        contact_email=job.contact_email,
        session=session,
    )

    # -- Step 4: Find LinkedIn profiles (skip if already found) --
    if makers:
        print("  STEP 4: Finding LinkedIn profiles…")
        for dm in makers:
            if dm.linkedin:
                print(f"    {dm.name} → {dm.linkedin} (already found)")
                continue
            linkedin = find_linkedin_url(
                dm.name, dm.title, job.company_name, session=session,
            )
            dm.linkedin = linkedin or ""
            if linkedin:
                print(f"    {dm.name} → {linkedin}")
            else:
                print(f"    {dm.name} → not found")

    # -- Step 5: Display results with validation --
    if makers:
        print("\n  RESULTS:")
        by_cat: dict[str, list] = {}
        for dm in makers:
            by_cat.setdefault(dm.category, []).append(dm)
        for cat, people in by_cat.items():
            print(f"    [{cat}]")
            for dm in people:
                status = "Valid" if dm.linkedin else "Invalid"
                tag = " [FROM JOB POSTING]" if dm.mentioned_in_job_posting else ""
                print(f"      [{status}] {dm.name} — {dm.title} ({dm.source}){tag}")
                if dm.linkedin:
                    print(f"              {dm.linkedin}")
        valid = sum(1 for dm in makers if dm.linkedin)
        print(f"\n    Total: {len(makers)} decision makers ({valid} valid, {len(makers) - valid} invalid)")
    else:
        print("    No decision makers found.")

    # -- Save to CSV --
    _append_results(output_path, url, job_board, job.title, job.company_name,
                    domain or "", makers)

    # -- Push to Clay --
    if clay_jobs_url:
        print("  CLAY: Pushing job offer…")
        valid_count = sum(1 for dm in makers if dm.linkedin) if makers else 0
        job_data = {
            "Job URL": url,
            "Job Board": job_board,
            "Job Title": job.title,
            "Company Name": job.company_name,
            "Company Website": domain or "",
            "Contact in Posting": job.contact_name or "",
            "Decision Makers Found": len(makers) if makers else 0,
            "Valid Contacts": valid_count,
        }
        if _push_to_clay(clay_jobs_url, job_data, session):
            clay_counts["jobs"] = 1
            print("    Job offer pushed to Clay")
        else:
            print("    Failed to push job offer to Clay")

    if clay_contacts_url and makers:
        print("  CLAY: Pushing contacts…")
        for dm in makers:
            contact_data = {
                "Company Name": job.company_name,
                "Company Website": domain or "",
                "Decision Maker Name": dm.name,
                "Decision Maker Title": dm.title,
                "Category": dm.category,
                "LinkedIn": dm.linkedin or "",
                "Status": "Valid" if dm.linkedin else "Invalid",
                "Source": dm.source,
                "Mentioned in Job Posting": "Yes" if dm.mentioned_in_job_posting else "No",
                "Job Board": job_board,
                "Job URL": url,
            }
            if _push_to_clay(clay_contacts_url, contact_data, session):
                clay_counts["contacts"] += 1
        print(f"    {clay_counts['contacts']}/{len(makers)} contacts pushed to Clay")

    return clay_counts


def collect_urls(args) -> list[tuple[str, str]]:
    """Collect (url, csv_title) pairs from arguments and/or file, deduplicated.

    csv_title is empty string when URL comes from CLI args or plain text file.
    When reading a CSV file (extension .csv), the Title column is extracted.
    """
    urls: list[tuple[str, str]] = []
    seen: set[str] = set()

    # From positional arguments
    for u in (args.urls or []):
        u = u.strip()
        if u and u not in seen:
            seen.add(u)
            urls.append((u, ""))

    # From file
    if args.file:
        if args.file.lower().endswith(".csv"):
            # CSV file with headers (expects at least a "URL" column)
            with open(args.file, encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    u = row.get("URL", "").strip()
                    title = row.get("Title", "").strip()
                    if u and u not in seen:
                        seen.add(u)
                        urls.append((u, title))
        else:
            # Plain text file: one URL per line
            with open(args.file, encoding="utf-8") as f:
                for line in f:
                    u = line.strip()
                    if u and not u.startswith("#") and u not in seen:
                        seen.add(u)
                        urls.append((u, ""))

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
        help="Text file (one URL per line) or CSV file with URL,Title columns",
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
    parser.add_argument(
        "--clay-jobs",
        help="Clay webhook URL for Job Offers table",
    )
    parser.add_argument(
        "--clay-contacts",
        help="Clay webhook URL for Contacts table",
    )
    args = parser.parse_args()

    # Collect all URLs (list of (url, csv_title) tuples)
    urls = collect_urls(args)
    if not urls:
        print("No URLs provided. Use: pipeline.py URL [URL ...] or pipeline.py -f file.csv")
        sys.exit(1)

    # Resume: skip already-done URLs
    if args.resume:
        already_done = _load_already_done(args.output)
        before = len(urls)
        urls = [(u, t) for u, t in urls if u not in already_done]
        log.info("Resume: %d already done, %d remaining", before - len(urls), len(urls))

    _ensure_csv_header(args.output)

    total = len(urls)
    log.info("Processing %d URL(s)…", total)
    if args.clay_jobs or args.clay_contacts:
        log.info("Clay integration enabled")

    session = requests.Session()
    total_clay_jobs = 0
    total_clay_contacts = 0

    for i, (url, csv_title) in enumerate(urls, start=1):
        print(f"\n{'=' * 60}")
        print(f"  [{i}/{total}] {url}")
        print(f"{'=' * 60}")

        try:
            counts = process_one_url(
                url, session, args.output,
                clay_jobs_url=args.clay_jobs,
                clay_contacts_url=args.clay_contacts,
                csv_title=csv_title,
            )
            total_clay_jobs += counts["jobs"]
            total_clay_contacts += counts["contacts"]
        except Exception as exc:
            log.error("Failed to process %s: %s", url, exc)
            print(f"  ERROR: {exc} — skipping to next URL")

        if i < total:
            time.sleep(args.delay)

    print(f"\n{'=' * 60}")
    print(f"  DONE! {total} URL(s) processed.")
    print(f"  Results saved to: {args.output}")
    if args.clay_jobs or args.clay_contacts:
        print(f"  Clay: {total_clay_jobs} jobs + {total_clay_contacts} contacts pushed")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
