"""
Streamlit UI for the Job Scraper Pipeline.

Run locally:  streamlit run app.py
Deploy:       Push to GitHub → connect on share.streamlit.io
"""

import contextlib
import csv
import io
import os
import time
from datetime import datetime

import pandas as pd
import requests
import streamlit as st

# --- Page config (must be first Streamlit call) ---
st.set_page_config(
    page_title="Job Scraper Pipeline",
    page_icon="\U0001F4BC",
    layout="wide",
)

# --- Pipeline imports (after set_page_config so secrets bridge runs first) ---
from scrapers.pipeline import (
    process_one_url,
    _ensure_csv_header,
    _load_already_done,
    CSV_FIELDS,
)
from scrapers.config import (
    CLAY_JOBS_WEBHOOK,
    CLAY_CONTACTS_WEBHOOK,
    OUTPUT_CSV,
)


# =========================================================================
# Session state
# =========================================================================
if "processing" not in st.session_state:
    st.session_state.processing = False
if "run_results" not in st.session_state:
    st.session_state.run_results = []
if "run_errors" not in st.session_state:
    st.session_state.run_errors = []


# =========================================================================
# Sidebar — Settings
# =========================================================================
with st.sidebar:
    st.header("Settings")

    clay_enabled = st.toggle("Push to Clay CRM", value=True)
    delay = st.slider("Delay between URLs (s)", 0.0, 10.0, 1.0, 0.5)
    resume = st.checkbox("Resume (skip already-processed)", value=False)

    st.divider()
    st.subheader("API Status")
    has_linkup = bool(os.getenv("LINKUP_API_KEY"))
    has_clay = bool(os.getenv("CLAY_JOBS_WEBHOOK"))

    if has_linkup:
        st.success("Linkup API key: OK")
    else:
        st.error("Linkup API key: MISSING")

    if clay_enabled:
        if has_clay:
            st.success("Clay webhooks: OK")
        else:
            st.warning("Clay webhooks: not configured")


# =========================================================================
# Main area — Title + Input
# =========================================================================
st.title("Job Scraper Pipeline")
st.write("Upload a CSV or paste job URLs to find decision makers.")

tab_csv, tab_urls = st.tabs(["Upload CSV", "Paste URLs"])

uploaded_file = None
url_text = ""

with tab_csv:
    uploaded_file = st.file_uploader(
        "CSV with a **URL** column (and optional **Title** column)",
        type=["csv"],
    )
    if uploaded_file is not None:
        df_preview = pd.read_csv(uploaded_file)
        st.dataframe(df_preview.head(10), use_container_width=True)
        st.caption(f"{len(df_preview)} rows")
        uploaded_file.seek(0)

with tab_urls:
    url_text = st.text_area(
        "One URL per line",
        height=200,
        placeholder="https://amazon-na.fountain.com/delivery-service-partner/apply/...",
    )


# =========================================================================
# Collect URLs from whichever input was used
# =========================================================================
def _collect_urls() -> list[tuple[str, str]]:
    """Return (url, csv_title) pairs from CSV upload or pasted text."""
    urls = []
    seen: set[str] = set()

    if uploaded_file is not None:
        uploaded_file.seek(0)
        reader = csv.DictReader(io.TextIOWrapper(uploaded_file, encoding="utf-8"))
        for row in reader:
            u = row.get("URL", "").strip()
            title = row.get("Title", "").strip()
            if u and u not in seen:
                seen.add(u)
                urls.append((u, title))

    if url_text:
        for line in url_text.strip().splitlines():
            u = line.strip()
            if u and u not in seen:
                seen.add(u)
                urls.append((u, ""))

    return urls


# =========================================================================
# Launch button + processing loop
# =========================================================================
launch = st.button(
    "Launch Pipeline",
    type="primary",
    disabled=st.session_state.processing,
)

if launch:
    urls = _collect_urls()
    if not urls:
        st.error("No URLs provided. Upload a CSV or paste URLs above.")
        st.stop()

    # Resolve Clay settings
    clay_jobs = CLAY_JOBS_WEBHOOK if clay_enabled else None
    clay_contacts = CLAY_CONTACTS_WEBHOOK if clay_enabled else None

    # Handle resume
    output_path = OUTPUT_CSV
    if resume:
        already_done = _load_already_done(output_path)
        before = len(urls)
        urls = [(u, t) for u, t in urls if u not in already_done]
        if not urls:
            st.warning(f"All {before} URLs already processed. Nothing to do.")
            st.stop()
        st.info(f"Resume: {before - len(urls)} already done, {len(urls)} remaining")

    _ensure_csv_header(output_path)

    st.session_state.processing = True
    st.session_state.run_results = []
    st.session_state.run_errors = []

    progress = st.progress(0, text="Starting pipeline...")
    session = requests.Session()

    for i, (url, csv_title) in enumerate(urls):
        progress.progress(i / len(urls), text=f"[{i + 1}/{len(urls)}] {url[:70]}...")

        with st.status(f"[{i + 1}/{len(urls)}] {url[:60]}...", expanded=(i == len(urls) - 1)) as status:
            log_buf = io.StringIO()
            try:
                with contextlib.redirect_stdout(log_buf):
                    counts = process_one_url(
                        url, session, output_path,
                        clay_jobs_url=clay_jobs,
                        clay_contacts_url=clay_contacts,
                        csv_title=csv_title,
                    )
                st.text(log_buf.getvalue())
                status.update(label=f"[{i + 1}/{len(urls)}] Done", state="complete")
                st.session_state.run_results.append({
                    "url": url,
                    "jobs": counts.get("jobs", 0),
                    "contacts": counts.get("contacts", 0),
                })
            except Exception as exc:
                st.text(log_buf.getvalue())
                st.error(f"Error: {exc}")
                status.update(label=f"[{i + 1}/{len(urls)}] Failed", state="error")
                st.session_state.run_errors.append({"url": url, "error": str(exc)})

        if i < len(urls) - 1:
            time.sleep(delay)

    progress.progress(1.0, text="Pipeline complete!")
    st.session_state.processing = False
    st.balloons()


# =========================================================================
# Results table
# =========================================================================
st.divider()
st.header("Results")

if os.path.exists(OUTPUT_CSV):
    df = pd.read_csv(OUTPUT_CSV)

    if not df.empty:
        # Filters
        col1, col2 = st.columns(2)
        with col1:
            companies = ["All"] + sorted(df["Company Name"].dropna().unique().tolist())
            selected_company = st.selectbox("Filter by Company", companies)
        with col2:
            confidences = ["All"] + sorted(df["Confidence"].dropna().unique().tolist())
            selected_conf = st.selectbox("Filter by Confidence", confidences)

        filtered = df.copy()
        if selected_company != "All":
            filtered = filtered[filtered["Company Name"] == selected_company]
        if selected_conf != "All":
            filtered = filtered[filtered["Confidence"] == selected_conf]

        st.dataframe(filtered, use_container_width=True, hide_index=True)
        st.caption(f"Showing {len(filtered)} of {len(df)} rows")

        # Download
        csv_bytes = filtered.to_csv(index=False).encode("utf-8")
        st.download_button(
            "Download CSV",
            data=csv_bytes,
            file_name=f"results_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
            mime="text/csv",
        )

        # Summary metrics
        st.subheader("Summary")
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total Rows", len(df))
        c2.metric("Companies", df["Company Name"].nunique())
        c3.metric("High Confidence", len(df[df["Confidence"] == "High"]))
        c4.metric("With LinkedIn", len(df[df["LinkedIn"].notna() & (df["LinkedIn"] != "")]))
    else:
        st.info("No results yet. Launch the pipeline above.")
else:
    st.info("No results yet. Launch the pipeline above.")

# Errors from last run
if st.session_state.run_errors:
    with st.expander(f"Errors ({len(st.session_state.run_errors)})"):
        for err in st.session_state.run_errors:
            st.error(f"{err['url']}: {err['error']}")
