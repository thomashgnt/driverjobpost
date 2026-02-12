"""
Pipeline Configuration
======================
Edit this file to set your default Clay webhook URLs and other settings.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Clay webhook URLs (set once, never type again)
# ---------------------------------------------------------------------------

CLAY_JOBS_WEBHOOK = (
    "https://api.clay.com/v3/sources/webhook/"
    "pull-in-data-from-a-webhook-3ff2d610-a4b3-4729-97e2-396f7cf4622e"
)

CLAY_CONTACTS_WEBHOOK = (
    "https://api.clay.com/v3/sources/webhook/"
    "pull-in-data-from-a-webhook-931030cf-46b9-472e-9423-d5a4139b9e99"
)

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
