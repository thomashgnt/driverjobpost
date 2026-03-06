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

# ── Page config (must be first Streamlit call) ───────────────────────────
st.set_page_config(
    page_title="DriverJobPost",
    page_icon="🚛",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Pipeline imports ─────────────────────────────────────────────────────
from scrapers.pipeline import (
    process_one_url,
    _ensure_csv_header,
    _load_already_done,
    _push_to_clay,
    CSV_FIELDS,
)
from scrapers.config import (
    CLAY_JOBS_WEBHOOK,
    CLAY_CONTACTS_WEBHOOK,
    OUTPUT_CSV,
)


# =========================================================================
# Custom CSS — injected once, styles the entire app
# =========================================================================
st.markdown("""
<style>
/* ── Google Font ──────────────────────────────────────────────────── */
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
html, body, [class*="css"] { font-family: 'Inter', -apple-system, sans-serif; }

/* ── Hide default Streamlit chrome ────────────────────────────────── */
#MainMenu {visibility: hidden;}
footer {visibility: hidden;}
header[data-testid="stHeader"] {
    background: rgba(250, 251, 252, 0.9);
    backdrop-filter: blur(12px);
}

/* ── Main container ──────────────────────────────────────────────── */
.block-container { padding-top: 2rem; padding-bottom: 2rem; max-width: 1200px; }

/* ── Metric cards ────────────────────────────────────────────────── */
.metric-card {
    background: #FFFFFF;
    border: 1px solid #E2E8F0;
    border-radius: 14px;
    padding: 1.25rem 1rem;
    text-align: center;
    transition: box-shadow 0.2s ease, transform 0.15s ease;
}
.metric-card:hover { box-shadow: 0 4px 16px rgba(79,70,229,0.08); transform: translateY(-2px); }
.metric-value { font-size: 2rem; font-weight: 700; color: #1E293B; line-height: 1.2; }
.metric-label {
    font-size: 0.75rem; font-weight: 600; color: #94A3B8;
    text-transform: uppercase; letter-spacing: 0.06em; margin-top: 0.25rem;
}
.metric-icon { font-size: 1.4rem; margin-bottom: 0.3rem; }

/* ── Badges ──────────────────────────────────────────────────────── */
.badge {
    display: inline-flex; align-items: center; gap: 0.35rem;
    padding: 0.3rem 0.75rem; border-radius: 9999px;
    font-size: 0.78rem; font-weight: 600;
}
.badge-green  { background: #ECFDF5; color: #059669; }
.badge-yellow { background: #FFFBEB; color: #D97706; }
.badge-red    { background: #FEF2F2; color: #DC2626; }

/* ── Section headers ─────────────────────────────────────────────── */
.section-header {
    display: flex; align-items: center; gap: 0.6rem;
    margin-bottom: 1rem; padding-bottom: 0.6rem;
    border-bottom: 2px solid #E2E8F0;
}
.section-header h2 { margin: 0; font-size: 1.2rem; font-weight: 600; color: #1E293B; }

/* ── Sidebar ─────────────────────────────────────────────────────── */
[data-testid="stSidebar"] { background: #FFFFFF; border-right: 1px solid #E2E8F0; }
.sidebar-brand {
    display: flex; align-items: center; gap: 0.6rem;
    margin-bottom: 1.5rem; padding-bottom: 1rem;
    border-bottom: 1px solid #F1F5F9;
}
.sidebar-brand-icon {
    background: linear-gradient(135deg, #4F46E5, #7C3AED);
    width: 34px; height: 34px; border-radius: 9px;
    display: flex; align-items: center; justify-content: center;
    font-size: 1.1rem; color: white;
}
.sidebar-brand-name { font-weight: 700; font-size: 1.05rem; color: #1E293B; }
.settings-label {
    font-size: 0.7rem; font-weight: 700; color: #94A3B8;
    text-transform: uppercase; letter-spacing: 0.08em; margin-bottom: 0.6rem;
}

/* ── Tabs ─────────────────────────────────────────────────────────── */
.stTabs [data-baseweb="tab-list"] { gap: 0.5rem; }
.stTabs [data-baseweb="tab"] { border-radius: 8px; padding: 0.5rem 1rem; font-weight: 500; }

/* ── Buttons ─────────────────────────────────────────────────────── */
.stButton > button { border-radius: 8px; font-weight: 600; }

/* ── Data editor ─────────────────────────────────────────────────── */
[data-testid="stDataEditor"] { border-radius: 12px; overflow: hidden; border: 1px solid #E2E8F0; }

/* ── Progress bar ────────────────────────────────────────────────── */
.stProgress > div > div { background-color: #4F46E5; }

/* ── Banners ─────────────────────────────────────────────────────── */
.banner {
    border-radius: 12px; padding: 1rem 1.5rem;
    display: flex; align-items: center; gap: 0.8rem; margin-bottom: 1rem;
}
.banner-running { background: linear-gradient(135deg, #EEF2FF, #E0E7FF); border: 1px solid #C7D2FE; }
.banner-success { background: #ECFDF5; border: 1px solid #A7F3D0; }
.banner-title { font-weight: 600; }
.banner-sub { font-size: 0.85rem; opacity: 0.8; }

/* ── Empty state ─────────────────────────────────────────────────── */
.empty-state {
    text-align: center; padding: 3.5rem 1.5rem; color: #94A3B8;
}
.empty-icon  { font-size: 3rem; margin-bottom: 0.8rem; opacity: 0.5; }
.empty-title { font-size: 1.05rem; font-weight: 600; color: #64748B; margin-bottom: 0.4rem; }
.empty-sub   { font-size: 0.9rem; color: #94A3B8; }

/* ── Error blocks ────────────────────────────────────────────────── */
.error-block {
    background: #FEF2F2; border-left: 3px solid #DC2626;
    border-radius: 0 8px 8px 0; padding: 0.75rem 1rem; margin-bottom: 0.5rem;
}
.error-block-url  { font-size: 0.78rem; color: #DC2626; font-weight: 600; }
.error-block-msg  { font-size: 0.85rem; color: #7F1D1D; margin-top: 0.2rem; }

/* ── Misc spacing ────────────────────────────────────────────────── */
hr { margin: 1.5rem 0; border-color: #E2E8F0; }
</style>
""", unsafe_allow_html=True)


# =========================================================================
# Session state
# =========================================================================
for key, default in [("processing", False), ("run_results", []), ("run_errors", [])]:
    if key not in st.session_state:
        st.session_state[key] = default


# =========================================================================
# Sidebar
# =========================================================================
with st.sidebar:
    # Brand
    st.markdown("""
    <div class="sidebar-brand">
        <div class="sidebar-brand-icon">🚛</div>
        <span class="sidebar-brand-name">DriverJobPost</span>
    </div>
    """, unsafe_allow_html=True)

    # Pipeline settings
    st.markdown('<p class="settings-label">Pipeline Settings</p>', unsafe_allow_html=True)
    with st.container(border=True):
        clay_enabled = st.toggle("Push to Clay CRM", value=True)
        delay = st.slider("Delay between URLs (s)", 0.0, 10.0, 1.0, 0.5)
        resume = st.checkbox("Resume (skip already-processed)", value=False)

    # API status
    st.markdown('<p class="settings-label">API Status</p>', unsafe_allow_html=True)
    with st.container(border=True):
        has_linkup = bool(os.getenv("LINKUP_API_KEY"))
        has_clay = bool(os.getenv("CLAY_JOBS_WEBHOOK"))

        if has_linkup:
            st.markdown('<span class="badge badge-green">✓ Linkup API</span>', unsafe_allow_html=True)
        else:
            st.markdown('<span class="badge badge-red">✗ Linkup API</span>', unsafe_allow_html=True)

        if clay_enabled:
            if has_clay:
                st.markdown('<span class="badge badge-green">✓ Clay Webhooks</span>', unsafe_allow_html=True)
            else:
                st.markdown('<span class="badge badge-yellow">⚠ Clay Webhooks</span>', unsafe_allow_html=True)


# =========================================================================
# Header
# =========================================================================
st.markdown("""
<div style="display:flex; align-items:center; gap:1rem; margin-bottom:1.5rem;">
    <div style="background:linear-gradient(135deg,#4F46E5,#7C3AED);
                width:52px; height:52px; border-radius:14px;
                display:flex; align-items:center; justify-content:center;
                font-size:1.6rem; color:white; flex-shrink:0;">
        🚛
    </div>
    <div>
        <h1 style="margin:0; font-size:1.75rem; font-weight:700; color:#1E293B;">
            DriverJobPost
        </h1>
        <p style="margin:0; font-size:0.92rem; color:#64748B;">
            Find decision makers at trucking companies from job postings.
        </p>
    </div>
</div>
""", unsafe_allow_html=True)


# =========================================================================
# Input section
# =========================================================================
st.markdown("""
<div class="section-header">
    <span style="font-size:1.2rem;">📥</span>
    <h2>Input</h2>
</div>
""", unsafe_allow_html=True)

tab_csv, tab_urls = st.tabs(["📄 Upload CSV", "🔗 Paste URLs"])

uploaded_file = None
url_text = ""

with tab_csv:
    st.markdown("Upload a CSV file with a **URL** column (and optional **Title** column).")
    uploaded_file = st.file_uploader(
        "Drop your CSV here",
        type=["csv"],
        label_visibility="collapsed",
    )
    if uploaded_file is not None:
        df_preview = pd.read_csv(uploaded_file)
        st.dataframe(df_preview.head(10), use_container_width=True, hide_index=True)
        st.caption(f"{len(df_preview)} rows detected")
        uploaded_file.seek(0)

with tab_urls:
    url_text = st.text_area(
        "One URL per line",
        height=180,
        placeholder="https://amazon-na.fountain.com/apply/delivery-service-partner/opening/...",
        label_visibility="collapsed",
    )


# =========================================================================
# Collect URLs
# =========================================================================
def _collect_urls() -> list[tuple[str, str]]:
    urls, seen = [], set()
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
# Launch button
# =========================================================================
st.markdown("")  # spacing
col_btn, _ = st.columns([1, 3])
with col_btn:
    launch = st.button(
        "🚀  Launch Pipeline",
        type="primary",
        disabled=st.session_state.processing,
        use_container_width=True,
    )

if launch:
    urls = _collect_urls()
    if not urls:
        st.markdown("""
        <div class="empty-state">
            <div class="empty-icon">📋</div>
            <div class="empty-title">No URLs provided</div>
            <div class="empty-sub">Upload a CSV or paste URLs in the input section above.</div>
        </div>
        """, unsafe_allow_html=True)
        st.stop()

    clay_jobs = CLAY_JOBS_WEBHOOK if clay_enabled else None
    clay_contacts = CLAY_CONTACTS_WEBHOOK if clay_enabled else None
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

    # Running banner
    st.markdown(f"""
    <div class="banner banner-running">
        <span style="font-size:1.4rem;">⚙️</span>
        <div>
            <div class="banner-title" style="color:#3730A3;">Pipeline Running</div>
            <div class="banner-sub" style="color:#6366F1;">Processing {len(urls)} URL(s)…</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    progress = st.progress(0, text="Starting…")
    session = requests.Session()

    for i, (url, csv_title) in enumerate(urls):
        progress.progress(i / len(urls), text=f"[{i + 1}/{len(urls)}]  {url[:65]}…")

        with st.status(f"[{i + 1}/{len(urls)}] {url[:55]}…", expanded=(i == len(urls) - 1)) as status:
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
                status.update(label=f"✅ [{i + 1}/{len(urls)}] Done", state="complete")
                st.session_state.run_results.append({
                    "url": url,
                    "jobs": counts.get("jobs", 0),
                    "contacts": counts.get("contacts", 0),
                })
            except Exception as exc:
                st.text(log_buf.getvalue())
                st.error(f"Error: {exc}")
                status.update(label=f"❌ [{i + 1}/{len(urls)}] Failed", state="error")
                st.session_state.run_errors.append({"url": url, "error": str(exc)})

        if i < len(urls) - 1:
            time.sleep(delay)

    progress.progress(1.0, text="Done!")
    st.session_state.processing = False

    # Success banner
    total_contacts = sum(r.get("contacts", 0) for r in st.session_state.run_results)
    st.markdown(f"""
    <div class="banner banner-success">
        <span style="font-size:1.4rem;">✅</span>
        <div>
            <div class="banner-title" style="color:#065F46;">Pipeline Complete</div>
            <div class="banner-sub" style="color:#059669;">
                {len(st.session_state.run_results)} URL(s) processed · {total_contacts} contacts found
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)


# =========================================================================
# Results
# =========================================================================
st.markdown("""
<div class="section-header">
    <span style="font-size:1.2rem;">📊</span>
    <h2>Results</h2>
</div>
""", unsafe_allow_html=True)

if os.path.exists(OUTPUT_CSV):
    df = pd.read_csv(OUTPUT_CSV)

    if not df.empty:
        # ── Metric cards ─────────────────────────────────────────────
        def _metric(icon, value, label, color="#1E293B"):
            return f"""
            <div class="metric-card">
                <div class="metric-icon">{icon}</div>
                <div class="metric-value" style="color:{color};">{value}</div>
                <div class="metric-label">{label}</div>
            </div>"""

        high_n = len(df[df["Confidence"] == "High"])
        link_n = len(df[df["LinkedIn"].notna() & (df["LinkedIn"] != "")])

        m1, m2, m3, m4 = st.columns(4)
        with m1:
            st.markdown(_metric("📋", len(df), "Total Rows"), unsafe_allow_html=True)
        with m2:
            st.markdown(_metric("🏢", df["Company Name"].nunique(), "Companies"), unsafe_allow_html=True)
        with m3:
            st.markdown(_metric("🎯", high_n, "High Confidence", "#059669"), unsafe_allow_html=True)
        with m4:
            st.markdown(_metric("🔗", link_n, "With LinkedIn", "#4F46E5"), unsafe_allow_html=True)

        st.markdown("")  # spacing

        # ── Filters ──────────────────────────────────────────────────
        st.markdown('<p class="settings-label">Filters</p>', unsafe_allow_html=True)
        col1, col2 = st.columns(2)
        with col1:
            companies = ["All"] + sorted(df["Company Name"].dropna().unique().tolist())
            selected_company = st.selectbox("Company", companies, label_visibility="collapsed")
        with col2:
            confidences = ["All"] + sorted(df["Confidence"].dropna().unique().tolist())
            selected_conf = st.selectbox("Confidence", confidences, label_visibility="collapsed")

        filtered = df.copy()
        if selected_company != "All":
            filtered = filtered[filtered["Company Name"] == selected_company]
        if selected_conf != "All":
            filtered = filtered[filtered["Confidence"] == selected_conf]

        # ── Data table ───────────────────────────────────────────────
        filtered = filtered.reset_index(drop=True)
        filtered.insert(0, "Push", False)

        edited_df = st.data_editor(
            filtered,
            use_container_width=True,
            hide_index=True,
            height=420,
            column_config={
                "Push": st.column_config.CheckboxColumn(
                    "", width="small", help="Select to push to Clay",
                ),
                "Company Name": st.column_config.TextColumn("Company", width="medium"),
                "Decision Maker Name": st.column_config.TextColumn("Contact", width="medium"),
                "Decision Maker Title": st.column_config.TextColumn("Title", width="medium"),
                "Confidence": st.column_config.TextColumn("Conf.", width="small"),
                "LinkedIn": st.column_config.LinkColumn("LinkedIn", width="small", display_text="Profile"),
                "Category": st.column_config.TextColumn("Category", width="small"),
                "Source": st.column_config.TextColumn("Source", width="small"),
                "Job Board": st.column_config.TextColumn("Board", width="small"),
                "Company Website": st.column_config.LinkColumn("Website", width="small", display_text="Link"),
                "Job URL": st.column_config.LinkColumn("Job URL", width="small", display_text="Link"),
                "Job Title": st.column_config.TextColumn("Job", width="medium"),
                "Mentioned in Job Posting": None,  # hide
                "Timestamp": None,  # hide
                "Contact Phone": None,  # hide (usually empty)
                "Contact Email": None,  # hide (usually empty)
            },
            disabled=[c for c in filtered.columns if c != "Push"],
        )

        st.markdown(f"""
        <p style="font-size:0.8rem; color:#94A3B8; margin-top:-0.3rem;">
            Showing {len(filtered)} of {len(df)} results
        </p>
        """, unsafe_allow_html=True)

        # ── Action buttons ───────────────────────────────────────────
        b1, b2, b3, _ = st.columns([1.1, 1.2, 1, 1.7])
        with b1:
            push_selected = st.button("☁️  Push Selected", type="primary", use_container_width=True)
        with b2:
            push_all_filtered = st.button("☁️  Push All Filtered", use_container_width=True)
        with b3:
            csv_bytes = filtered.drop(columns=["Push"]).to_csv(index=False).encode("utf-8")
            st.download_button(
                "📥  Download CSV",
                data=csv_bytes,
                file_name=f"results_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
                mime="text/csv",
                use_container_width=True,
            )

        # ── Push logic ───────────────────────────────────────────────
        def _safe_str(val) -> str:
            if pd.isna(val):
                return ""
            return str(val).strip()

        def _do_push(rows_to_push: pd.DataFrame):
            if not CLAY_CONTACTS_WEBHOOK:
                st.error("Clay contacts webhook not configured.")
                return

            session = requests.Session()
            total = len(rows_to_push)
            success, errors = 0, []
            bar = st.progress(0, text="Pushing to Clay…")

            for i, (_, row) in enumerate(rows_to_push.iterrows()):
                name = _safe_str(row.get("Decision Maker Name"))
                contact_data = {
                    "Company Name": _safe_str(row.get("Company Name")),
                    "Company Website": _safe_str(row.get("Company Website")),
                    "Decision Maker Name": name,
                    "Decision Maker Title": _safe_str(row.get("Decision Maker Title")),
                    "Category": _safe_str(row.get("Category")),
                    "LinkedIn": _safe_str(row.get("LinkedIn")),
                    "Confidence": _safe_str(row.get("Confidence")),
                    "Source": _safe_str(row.get("Source")),
                    "Mentioned in Job Posting": _safe_str(row.get("Mentioned in Job Posting")) or "No",
                    "Contact Phone": _safe_str(row.get("Contact Phone")),
                    "Contact Email": _safe_str(row.get("Contact Email")),
                    "Job Board": _safe_str(row.get("Job Board")),
                    "Job URL": _safe_str(row.get("Job URL")),
                }
                try:
                    resp = session.post(CLAY_CONTACTS_WEBHOOK, json=contact_data, timeout=15)
                    resp.raise_for_status()
                    success += 1
                except Exception as exc:
                    errors.append(f"{name}: {exc}")
                bar.progress((i + 1) / total, text=f"Pushing {i + 1}/{total}…")

            bar.empty()
            if success == total:
                st.success(f"✅ {success}/{total} contacts pushed to Clay!")
            else:
                st.warning(f"⚠️ {success}/{total} pushed ({total - success} failed)")
                for err in errors:
                    st.error(err)

        if push_selected:
            selected_rows = edited_df[edited_df["Push"] == True]  # noqa: E712
            if selected_rows.empty:
                st.warning("No contacts selected — tick the checkboxes first.")
            else:
                _do_push(selected_rows)

        if push_all_filtered:
            _do_push(edited_df)

    else:
        st.markdown("""
        <div class="empty-state">
            <div class="empty-icon">📭</div>
            <div class="empty-title">No results yet</div>
            <div class="empty-sub">Upload a CSV or paste URLs above, then launch the pipeline.</div>
        </div>
        """, unsafe_allow_html=True)
else:
    st.markdown("""
    <div class="empty-state">
        <div class="empty-icon">📭</div>
        <div class="empty-title">No results yet</div>
        <div class="empty-sub">Upload a CSV or paste URLs above, then launch the pipeline.</div>
    </div>
    """, unsafe_allow_html=True)


# =========================================================================
# Errors from last run
# =========================================================================
if st.session_state.run_errors:
    st.markdown(f"""
    <div class="section-header">
        <span style="font-size:1.2rem;">⚠️</span>
        <h2>Errors</h2>
        <span class="badge badge-red">{len(st.session_state.run_errors)}</span>
    </div>
    """, unsafe_allow_html=True)

    with st.expander(f"View {len(st.session_state.run_errors)} error(s)", expanded=False):
        for err in st.session_state.run_errors:
            st.markdown(f"""
            <div class="error-block">
                <div class="error-block-url">{err['url'][:90]}</div>
                <div class="error-block-msg">{err['error']}</div>
            </div>
            """, unsafe_allow_html=True)
