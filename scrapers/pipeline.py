#!/usr/bin/env python3
"""
Job Offer Pipeline
==================
Usage:

    # Simplest — drop a CSV in inbox/ and run:
    python3 run.py

    # Or give a file directly:
    python3 run.py my_jobs.csv

    # Or a single URL:
    python3 run.py "https://amazon-na.fountain.com/..."

    # Watch mode — auto-processes new CSV files dropped into inbox/:
    python3 run.py --watch

    # Resume after interruption (skips already-done URLs):
    python3 run.py my_jobs.csv --resume

    # Without Clay (CSV only):
    python3 run.py my_jobs.csv --no-clay

Config: edit scrapers/config.py to change Clay webhook URLs and defaults.

Pipeline steps for each URL:
  1. Scrape the job posting (title, company, description, contact)
  2. Find the company's official website
  3. Find decision makers (5-priority deep search)
  4. Find LinkedIn profiles (skip if already found in step 3)
  5. Save everything to a CSV + push to Clay
"""

from __future__ import annotations

import sys
import os
import csv
import glob as globmod
import logging
import argparse
import re
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

import requests

# Allow running as: python3 scrapers/pipeline.py  OR  python3 run.py
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scrapers.scrape_job import (
    scrape_job, JobInfo, _extract_real_company, _is_fountain_amazon_dsp,
)
from scrapers.find_domain import find_company_domain
from scrapers.find_decision_makers import find_decision_makers
from scrapers.find_linkedin import find_linkedin_url
from scrapers.config import (
    CLAY_JOBS_WEBHOOK, CLAY_CONTACTS_WEBHOOK, CLAY_ENABLED,
    DELAY_BETWEEN_URLS, OUTPUT_CSV as DEFAULT_OUTPUT_CSV, WATCH_FOLDER,
    MAX_CONSECUTIVE_FAILURES, CIRCUIT_BREAKER_PAUSE, RATE_LIMIT_LONG_PAUSE,
)
from scrapers.linkup_client import RateLimitExhausted

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)
CSV_FIELDS = [
    "Timestamp",
    "Job URL",
    "Job Board",
    "Job Title",
    "Company Name",
    "Company Website",
    "Contact Phone",
    "Contact Email",
    "Decision Maker Name",
    "Decision Maker Title",
    "Category",
    "Mentioned in Job Posting",
    "Source",
    "LinkedIn",
    "Confidence",
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
                    company: str, website: str, makers: list,
                    contact_phone: str = "", contact_email: str = "") -> None:
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
                    "Contact Phone": contact_phone,
                    "Contact Email": contact_email,
                    "Decision Maker Name": dm.name,
                    "Decision Maker Title": dm.title,
                    "Category": dm.category,
                    "Mentioned in Job Posting": "Yes" if dm.mentioned_in_job_posting else "No",
                    "Source": dm.source,
                    "LinkedIn": dm.linkedin or "",
                    "Confidence": dm.confidence,
                })
        else:
            writer.writerow({
                "Timestamp": datetime.now().isoformat(),
                "Job URL": url,
                "Job Board": job_board,
                "Job Title": job_title,
                "Company Name": company,
                "Company Website": website,
                "Contact Phone": contact_phone,
                "Contact Email": contact_email,
                "Decision Maker Name": "",
                "Decision Maker Title": "",
                "Category": "",
                "Mentioned in Job Posting": "",
                "Source": "",
                "LinkedIn": "",
                "Confidence": "",
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
    if job.contact_name or job.contact_email or job.contact_phone:
        parts = []
        if job.contact_name:
            parts.append(job.contact_name)
        if job.contact_email:
            parts.append(job.contact_email)
        if job.contact_phone:
            parts.append(job.contact_phone)
        print(f"    Contact   : {' | '.join(parts)}")

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
                tag = " [FROM JOB POSTING]" if dm.mentioned_in_job_posting else ""
                print(f"      [{dm.confidence}] {dm.name} — {dm.title} ({dm.source}){tag}")
                if dm.linkedin:
                    print(f"              {dm.linkedin}")
        high = sum(1 for dm in makers if dm.confidence == "High")
        med = sum(1 for dm in makers if dm.confidence == "Medium")
        print(f"\n    Total: {len(makers)} decision makers ({high} high, {med} medium confidence)")
    else:
        print("    No decision makers found.")

    # -- Save to CSV --
    _append_results(output_path, url, job_board, job.title, job.company_name,
                    domain or "", makers,
                    contact_phone=job.contact_phone or "",
                    contact_email=job.contact_email or "")

    # -- Push to Clay --
    if clay_jobs_url:
        print("  CLAY: Pushing job offer…")
        high_count = sum(1 for dm in makers if dm.confidence == "High") if makers else 0
        job_data = {
            "Job URL": url,
            "Job Board": job_board,
            "Job Title": job.title,
            "Company Name": job.company_name,
            "Company Website": domain or "",
            "Contact in Posting": job.contact_name or "",
            "Contact Phone": job.contact_phone or "",
            "Contact Email": job.contact_email or "",
            "Decision Makers Found": len(makers) if makers else 0,
            "High Confidence Contacts": high_count,
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
                "Confidence": dm.confidence,
                "Source": dm.source,
                "Mentioned in Job Posting": "Yes" if dm.mentioned_in_job_posting else "No",
                "Contact Phone": job.contact_phone or "",
                "Contact Email": job.contact_email or "",
                "Job Board": job_board,
                "Job URL": url,
            }
            if _push_to_clay(clay_contacts_url, contact_data, session):
                clay_counts["contacts"] += 1
        print(f"    {clay_counts['contacts']}/{len(makers)} contacts pushed to Clay")

    return clay_counts



def _auto_find_csv() -> str | None:
    """Look for a CSV file to process automatically.

    Priority: 1) inbox/*.csv  2) *.csv in current dir (excluding pipeline output)
    """
    # Check inbox folder first
    inbox = Path(WATCH_FOLDER)
    if inbox.is_dir():
        csvs = sorted(inbox.glob("*.csv"), key=lambda p: p.stat().st_mtime, reverse=True)
        if csvs:
            return str(csvs[0])

    # Fall back to current directory
    csvs = [
        f for f in globmod.glob("*.csv")
        if f != DEFAULT_OUTPUT_CSV and not f.startswith("test")
    ]
    if len(csvs) == 1:
        return csvs[0]
    return None


def run_batch(
    urls: list[tuple[str, str]],
    output_path: str,
    clay_jobs_url: str | None,
    clay_contacts_url: str | None,
    delay: float,
    resume: bool = False,
) -> None:
    """Process a list of (url, csv_title) pairs through the full pipeline.

    Guard rails for large batches:
    - Retry: if a URL fails, retry once after 30s
    - Circuit breaker: if N URLs fail in a row, pause before continuing
    - Rate limit: if Linkup returns 429 too many times, long pause then resume
    """
    if resume:
        already_done = _load_already_done(output_path)
        before = len(urls)
        urls = [(u, t) for u, t in urls if u not in already_done]
        log.info("Resume: %d already done, %d remaining", before - len(urls), len(urls))

    if not urls:
        print("Nothing to process (all URLs already done).")
        return

    _ensure_csv_header(output_path)

    total = len(urls)
    clay_on = bool(clay_jobs_url or clay_contacts_url)
    log.info("Processing %d URL(s)…", total)
    if clay_on:
        log.info("Clay integration enabled")

    session = requests.Session()
    total_clay_jobs = 0
    total_clay_contacts = 0
    total_errors = 0
    total_retried = 0
    consecutive_failures = 0

    for i, (url, csv_title) in enumerate(urls, start=1):
        print(f"\n{'=' * 60}")
        print(f"  [{i}/{total}] {url}")
        print(f"{'=' * 60}")

        success = False
        for attempt in range(2):  # max 2 attempts (original + 1 retry)
            try:
                counts = process_one_url(
                    url, session, output_path,
                    clay_jobs_url=clay_jobs_url,
                    clay_contacts_url=clay_contacts_url,
                    csv_title=csv_title,
                )
                total_clay_jobs += counts["jobs"]
                total_clay_contacts += counts["contacts"]
                success = True
                consecutive_failures = 0
                break

            except RateLimitExhausted:
                log.warning(
                    "Rate limit exhausted. Pausing %ds before retrying…",
                    RATE_LIMIT_LONG_PAUSE,
                )
                print(f"  RATE LIMITED — pausing {RATE_LIMIT_LONG_PAUSE // 60} minutes…")
                time.sleep(RATE_LIMIT_LONG_PAUSE)
                # Reset the global 429 counter so we can try again
                import scrapers.linkup_client as _lc
                _lc._consecutive_429s = 0
                # This counts as attempt 0 retry, loop will try once more

            except Exception as exc:
                if attempt == 0:
                    total_retried += 1
                    log.warning("Failed [%s]. Retrying in 30s… (%s)", url, exc)
                    print(f"  RETRY in 30s… ({exc})")
                    time.sleep(30)
                else:
                    total_errors += 1
                    log.error("Failed after retry [%s]: %s", url, exc)
                    print(f"  ERROR: {exc} — skipping")

        if not success:
            consecutive_failures += 1
            # Circuit breaker: too many failures in a row → pause
            if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                log.warning(
                    "Circuit breaker: %d failures in a row. Pausing %ds…",
                    consecutive_failures, CIRCUIT_BREAKER_PAUSE,
                )
                print(f"\n  CIRCUIT BREAKER — {consecutive_failures} failures in a row. "
                      f"Pausing {CIRCUIT_BREAKER_PAUSE}s…")
                time.sleep(CIRCUIT_BREAKER_PAUSE)
                consecutive_failures = 0

        if i < total:
            time.sleep(delay)

    print(f"\n{'=' * 60}")
    print(f"  DONE! {total} URL(s) processed.")
    if total_errors or total_retried:
        print(f"  Errors: {total_errors} failed, {total_retried} retried")
    print(f"  Results saved to: {output_path}")
    if clay_on:
        print(f"  Clay: {total_clay_jobs} jobs + {total_clay_contacts} contacts pushed")
    print(f"{'=' * 60}")


def _watch_inbox(
    output_path: str,
    clay_jobs_url: str | None,
    clay_contacts_url: str | None,
    delay: float,
) -> None:
    """Watch the inbox folder for new CSV files and process them automatically."""
    inbox = Path(WATCH_FOLDER)
    inbox.mkdir(exist_ok=True)
    processed_dir = inbox / "done"
    processed_dir.mkdir(exist_ok=True)

    print(f"\n  WATCH MODE — Drop a CSV file into '{inbox}/' and it will be processed automatically.")
    print(f"  Press Ctrl+C to stop.\n")

    seen: set[str] = set()
    # Mark files already present as seen (don't re-process)
    for f in inbox.glob("*.csv"):
        seen.add(f.name)
        log.info("Already in inbox (skipping): %s", f.name)

    try:
        while True:
            for csv_file in sorted(inbox.glob("*.csv")):
                if csv_file.name in seen:
                    continue
                seen.add(csv_file.name)
                print(f"\n  NEW FILE DETECTED: {csv_file.name}")
                print(f"{'=' * 60}")

                # Read URLs from the new CSV
                urls: list[tuple[str, str]] = []
                with open(csv_file, encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        u = row.get("URL", "").strip()
                        title = row.get("Title", "").strip()
                        if u:
                            urls.append((u, title))

                if urls:
                    run_batch(urls, output_path, clay_jobs_url,
                              clay_contacts_url, delay, resume=True)
                    # Move processed file to done/
                    dest = processed_dir / csv_file.name
                    csv_file.rename(dest)
                    log.info("Moved %s → %s", csv_file.name, dest)
                else:
                    log.warning("No URLs found in %s", csv_file.name)

            time.sleep(5)  # Poll every 5 seconds
    except KeyboardInterrupt:
        print("\n  Watch mode stopped.")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Pipeline: Job URL -> Company -> Domain -> Decision Makers -> Clay",
    )
    parser.add_argument(
        "input",
        nargs="*",
        help="URL(s) or a CSV file path. If omitted, auto-detects CSV in inbox/ or current dir.",
    )
    parser.add_argument(
        "-f", "--file",
        help="CSV file with URL,Title columns (or plain text, one URL per line)",
    )
    parser.add_argument(
        "-o", "--output",
        default=DEFAULT_OUTPUT_CSV,
        help=f"Output CSV file (default: {DEFAULT_OUTPUT_CSV})",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=DELAY_BETWEEN_URLS,
        help=f"Delay in seconds between URLs (default: {DELAY_BETWEEN_URLS})",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Skip URLs already present in the output CSV",
    )
    parser.add_argument(
        "--no-clay",
        action="store_true",
        help="Disable Clay push (CSV output only)",
    )
    parser.add_argument(
        "--watch",
        action="store_true",
        help=f"Watch '{WATCH_FOLDER}/' for new CSV files and process them automatically",
    )
    args = parser.parse_args()

    # Resolve Clay webhook URLs from config (unless --no-clay)
    if args.no_clay:
        clay_jobs_url = None
        clay_contacts_url = None
    elif CLAY_ENABLED:
        clay_jobs_url = CLAY_JOBS_WEBHOOK
        clay_contacts_url = CLAY_CONTACTS_WEBHOOK
    else:
        clay_jobs_url = None
        clay_contacts_url = None

    # Watch mode
    if args.watch:
        _watch_inbox(args.output, clay_jobs_url, clay_contacts_url, args.delay)
        return

    # Smart input detection: positional arg can be a file path or URL(s)
    file_path = args.file
    raw_urls: list[str] = []

    for item in (args.input or []):
        item = item.strip()
        if not item:
            continue
        # If it looks like a file path (exists or ends in .csv/.txt), treat as file
        if os.path.isfile(item):
            file_path = item
        else:
            raw_urls.append(item)

    # Collect URLs
    urls: list[tuple[str, str]] = []
    seen: set[str] = set()

    for u in raw_urls:
        if u not in seen:
            seen.add(u)
            urls.append((u, ""))

    if file_path:
        urls.extend(_read_file(file_path, seen))

    # Auto-detect if nothing provided
    if not urls:
        auto = _auto_find_csv()
        if auto:
            print(f"  Auto-detected: {auto}")
            urls.extend(_read_file(auto, seen))

    if not urls:
        print("No input found. Usage:")
        print("  python3 run.py my_jobs.csv           # Process a CSV file")
        print("  python3 run.py \"https://...\"          # Process a single URL")
        print("  python3 run.py --watch                # Watch inbox/ for new files")
        print(f"\nOr drop a CSV file into '{WATCH_FOLDER}/' and run: python3 run.py")
        sys.exit(1)

    run_batch(urls, args.output, clay_jobs_url, clay_contacts_url,
              args.delay, resume=args.resume)


def _read_file(path: str, seen: set[str]) -> list[tuple[str, str]]:
    """Read URLs from a file (CSV or plain text)."""
    urls: list[tuple[str, str]] = []
    if path.lower().endswith(".csv"):
        with open(path, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                u = row.get("URL", "").strip()
                title = row.get("Title", "").strip()
                if u and u not in seen:
                    seen.add(u)
                    urls.append((u, title))
    else:
        with open(path, encoding="utf-8") as f:
            for line in f:
                u = line.strip()
                if u and not u.startswith("#") and u not in seen:
                    seen.add(u)
                    urls.append((u, ""))
    return urls


if __name__ == "__main__":
    main()
