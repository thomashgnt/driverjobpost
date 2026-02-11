#!/usr/bin/env python3
"""
Simple launcher — just run:

    python3 run.py my_jobs.csv          # Process a CSV file
    python3 run.py "https://..."        # Process a single URL
    python3 run.py --watch              # Auto-process new files in inbox/
    python3 run.py                      # Auto-detect CSV in inbox/ or current dir
    python3 run.py my_jobs.csv --resume # Resume after interruption

Config: edit scrapers/config.py to change Clay webhook URLs.
"""

from __future__ import annotations

from scrapers.pipeline import main

if __name__ == "__main__":
    main()
