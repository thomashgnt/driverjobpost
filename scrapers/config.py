"""
Pipeline Configuration
======================
Edit the .env file at the project root to set your API keys and webhook URLs.
See .env.example for the required variables.
"""

from __future__ import annotations

import os
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# API keys
# ---------------------------------------------------------------------------

LINKUP_API_KEY = os.getenv("LINKUP_API_KEY", "")

# ---------------------------------------------------------------------------
# Clay webhook URLs
# ---------------------------------------------------------------------------

CLAY_JOBS_WEBHOOK = os.getenv("CLAY_JOBS_WEBHOOK", "")
CLAY_CONTACTS_WEBHOOK = os.getenv("CLAY_CONTACTS_WEBHOOK", "")

# Set to False to disable Clay push (CSV-only mode)
CLAY_ENABLED = True

# ---------------------------------------------------------------------------
# Pipeline defaults
# ---------------------------------------------------------------------------

# Delay between URLs (seconds) — be nice to Linkup API
DELAY_BETWEEN_URLS = 1.0

# Output CSV file
OUTPUT_CSV = "pipeline_results.csv"

# Folder to watch for new CSV files (used by --watch mode)
WATCH_FOLDER = "inbox"

# ---------------------------------------------------------------------------
# Rate limiting & retry (guard rails for large batch runs)
# ---------------------------------------------------------------------------

# Retries per Linkup API call (backoff: 2s, 4s, 8s…)
MAX_RETRIES_PER_REQUEST = 3

# If N URLs fail in a row, pause before continuing
MAX_CONSECUTIVE_FAILURES = 3
CIRCUIT_BREAKER_PAUSE = 60        # seconds

# If Linkup returns 429 repeatedly, long pause
RATE_LIMIT_LONG_PAUSE = 300       # 5 minutes
