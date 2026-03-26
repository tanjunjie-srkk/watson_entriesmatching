"""
ui/app.py
---------
Streamlit dashboard for the Shopee Reconciliation pipeline.

Run from the project root:
    streamlit run ui/app.py
"""
from __future__ import annotations

import datetime
import hashlib
import random
import sys
import time
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

# Ensure the project root is on sys.path so `reconciliation` package is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from reconciliation import run_reconciliation_from_paths  # noqa: E402
from reconciliation.excel_export import export_to_excel   # noqa: E402

# ──────────────────────────────────────────────────────────────────────────────
# Colour palette
# ──────────────────────────────────────────────────────────────────────────────

PURPLE = "#BFB2F9"
PINK   = "#FD8FD5"
RED    = "#DB3C36"
TEAL   = "#00A0AF"

PURPLE_DARK = "#8B7AD6"
PINK_DARK   = "#D96FB0"
RED_DARK    = "#A82D28"
TEAL_DARK   = "#007A85"

BG_LIGHT    = "#F8F9FC"
CARD_BG     = "#FFFFFF"
TEXT_PRIMARY = "#1A1A2E"
TEXT_MUTED   = "#6B7280"
BORDER       = "#E5E7EB"
AMBER        = "#F59E0B"
GREEN        = "#10B981"


# ──────────────────────────────────────────────────────────────────────────────
# Mock data for Reconciliation Summary (POC)
# ──────────────────────────────────────────────────────────────────────────────

@st.cache_data
def _generate_mock_runs() -> pd.DataFrame:
    """Generate realistic mock reconciliation run history."""
    rng = random.Random(42)
    runs = []
    base_date = datetime.date(2025, 10, 1)
    for i in range(25):
        run_date = base_date + datetime.timedelta(days=i * 7 + rng.randint(0, 3))
        period_from = run_date - datetime.timedelta(days=7)
        period_to = run_date - datetime.timedelta(days=1)
        income_rows = rng.randint(800, 5000)
        balance_rows = rng.randint(800, 5000)
        sales_rows = rng.randint(200, 1500)
        recon_rows = rng.randint(700, min(income_rows, balance_rows))
        # Every 5th run is fully reconciled (OK status, zero outstanding)
        if i % 5 == 0:
            outstanding_rows = 0
            refund_rows = rng.randint(0, int(recon_rows * 0.08))
            match_rate = 100.0
            total_sales = round(rng.uniform(50_000, 500_000), 2)
            total_fees = round(total_sales * rng.uniform(0.03, 0.12), 2)
            total_payment = round(total_sales - total_fees, 2)
            total_outstanding = 0.0
        else:
            # Positive outstanding rows and amounts
            outstanding_rows = rng.randint(1, max(2, int(recon_rows * 0.15)))
            refund_rows = rng.randint(0, int(recon_rows * 0.08))
            match_rate = round((recon_rows - outstanding_rows) / recon_rows * 100, 2) if recon_rows else 0
            total_sales = round(rng.uniform(50_000, 500_000), 2)
            total_fees = round(total_sales * rng.uniform(0.03, 0.12), 2)
            # Compute payment so that outstanding = sales - payment - fees is always positive
            max_payment = total_sales - total_fees
            total_payment = round(max_payment * rng.uniform(0.85, 0.97), 2)
            total_outstanding = round(total_sales - total_payment - total_fees, 2)
        income_not_balance = rng.randint(0, 30)
        balance_not_income = rng.randint(0, 25)
        duration = round(rng.uniform(3.5, 45.0), 2)
        fees_pct = round(total_fees / total_sales * 100, 2) if total_sales else 0

        # Flag for human review when outstanding is not zero
        needs_review = outstanding_rows != 0 or total_outstanding != 0
        review_reasons = []
        if outstanding_rows != 0:
            review_reasons.append(f"Outstanding orders ({outstanding_rows})")
        if total_outstanding != 0:
            review_reasons.append(f"Outstanding amount (RM {total_outstanding:,.2f})")

        run_id = hashlib.sha256(f"run-{i}-{run_date}".encode()).hexdigest()[:8].upper()
        runs.append({
            "Run ID": f"RUN-{run_id}",
            "Run Date": run_date,
            "Period From": period_from,
            "Period To": period_to,
            "Income Rows": income_rows,
            "Balance Rows": balance_rows,
            "Sales Rows": sales_rows,
            "Recon Rows": recon_rows,
            "Outstanding Orders": outstanding_rows,
            "Refund Orders": refund_rows,
            "Match Rate (%)": match_rate,
            "Total Sales (RM)": total_sales,
            "Total Payment (RM)": total_payment,
            "Total Fees (RM)": total_fees,
            "Total Outstanding (RM)": total_outstanding,
            "Income Not In Balance": income_not_balance,
            "Balance Not In Income": balance_not_income,
            "Duration (s)": duration,
            "Fees % of Sales": fees_pct,
            "Needs Review": needs_review,
            "Review Reasons": "; ".join(review_reasons) if review_reasons else "—",
            "Status": "⚠️ Needs Review" if needs_review else "✅ OK",
        })
    return pd.DataFrame(runs)

# ──────────────────────────────────────────────────────────────────────────────
# Page config & CSS
# ──────────────────────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Shopee Reconciliation",
    page_icon="📊",
    layout="wide",
)

st.markdown(f"""
<style>
    /* ── Global ─────────────────────────────────────────── */
    .stApp {{
        background-color: {BG_LIGHT};
    }}
    section[data-testid="stSidebar"] {{
        background: linear-gradient(180deg, #007A85 0%, #004D55 100%);
    }}
    section[data-testid="stSidebar"] * {{
        color: #E8E0FF !important;
    }}
    section[data-testid="stSidebar"] .stTextInput label,
    section[data-testid="stSidebar"] .stCheckbox label,
    section[data-testid="stSidebar"] .stDateInput label,
    section[data-testid="stSidebar"] .stSelectbox label {{
        color: #C4B5FD !important;
    }}
    section[data-testid="stSidebar"] .stTextInput input,
    section[data-testid="stSidebar"] .stDateInput input {{
        background-color: rgba(0,40,50,0.6) !important;
        border: 1px solid rgba(191,178,249,0.3) !important;
        color: #FFFFFF !important;
        border-radius: 8px;
    }}
    section[data-testid="stSidebar"] .stTextInput input::selection,
    section[data-testid="stSidebar"] .stDateInput input::selection {{
        background-color: rgba(191,178,249,0.4) !important;
        color: #FFFFFF !important;
    }}
    /* Radio buttons in sidebar */
    section[data-testid="stSidebar"] div.stRadio > div {{
        background: rgba(255,255,255,0.08) !important;
        border: 1px solid rgba(191,178,249,0.25) !important;
        border-radius: 10px !important;
        padding: 4px !important;
        display: inline-flex !important;
        flex-direction: column !important;
        width: auto !important;
    }}
    section[data-testid="stSidebar"] div.stRadio > div > label {{
        color: #E8E0FF !important;
        width: 100% !important;
        box-sizing: border-box !important;
    }}
    section[data-testid="stSidebar"] div.stRadio > div > label:hover {{
        background: rgba(255,255,255,0.15) !important;
        color: #FFFFFF !important;
    }}
    section[data-testid="stSidebar"] div.stRadio > div > label[data-checked="true"],
    section[data-testid="stSidebar"] div.stRadio > div > label:has(input:checked) {{
        background: rgba(255,255,255,0.2) !important;
        color: #FFFFFF !important;
    }}
    /* Selectbox in sidebar */
    section[data-testid="stSidebar"] .stSelectbox div[data-baseweb="select"],
    section[data-testid="stSidebar"] .stSelectbox > div > div {{
        background-color: rgba(0,40,50,0.6) !important;
        border: 1px solid rgba(191,178,249,0.3) !important;
        border-radius: 8px !important;
    }}
    section[data-testid="stSidebar"] .stSelectbox div[data-baseweb="select"] *,
    section[data-testid="stSidebar"] .stSelectbox span,
    section[data-testid="stSidebar"] .stSelectbox div {{
        color: #FFFFFF !important;
        -webkit-text-fill-color: #FFFFFF !important;
    }}
    section[data-testid="stSidebar"] .stSelectbox svg {{
        fill: #E8E0FF !important;
    }}
    section[data-testid="stSidebar"] div[data-baseweb="select"] [data-baseweb="tag"] {{
        background-color: rgba(191,178,249,0.25) !important;
    }}
    /* Selectbox dropdown menu (rendered at body level) */
    div[data-baseweb="popover"] ul[role="listbox"] {{
        background-color: #004D55 !important;
    }}
    div[data-baseweb="popover"] li[role="option"] {{
        color: #1A1A2E !important;
        -webkit-text-fill-color: #1A1A2E !important;
    }}
    div[data-baseweb="popover"] li[role="option"]:hover,
    div[data-baseweb="popover"] li[role="option"][aria-selected="true"] {{
        background-color: rgba(191,178,249,0.2) !important;
    }}
    /* Headings in sidebar */
    section[data-testid="stSidebar"] h3,
    section[data-testid="stSidebar"] h4 {{
        color: #FFFFFF !important;
    }}
    section[data-testid="stSidebar"] hr {{
        border-color: rgba(191,178,249,0.2) !important;
    }}

    /* ── Header ─────────────────────────────────────────── */
    .dashboard-header {{
        background: linear-gradient(135deg, {PURPLE} 0%, {PINK} 50%, {TEAL} 100%);
        border-radius: 16px;
        padding: 2rem 2.5rem;
        margin-bottom: 1.5rem;
        color: white;
        position: relative;
        overflow: hidden;
    }}
    .dashboard-header h1 {{
        margin: 0;
        font-size: 2rem;
        font-weight: 700;
        color: white;
        text-shadow: 0 1px 3px rgba(0,0,0,0.15);
    }}
    .dashboard-header p {{
        margin: 0.4rem 0 0 0;
        font-size: 0.95rem;
        opacity: 0.92;
    }}

    /* ── Metric cards ───────────────────────────────────── */
    .metric-card {{
        background: {CARD_BG};
        border-radius: 12px;
        padding: 1.25rem;
        border: 1px solid {BORDER};
        box-shadow: 0 1px 3px rgba(0,0,0,0.04);
        transition: transform 0.15s, box-shadow 0.15s;
        height: 100%;
    }}
    .metric-card:hover {{
        transform: translateY(-2px);
        box-shadow: 0 4px 12px rgba(0,0,0,0.08);
    }}
    .metric-label {{
        font-size: 0.78rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.5px;
        color: {TEXT_MUTED};
        margin-bottom: 0.35rem;
    }}
    .metric-value {{
        font-size: 1.75rem;
        font-weight: 700;
        color: {TEXT_PRIMARY};
        line-height: 1.2;
    }}
    .metric-accent {{
        width: 4px;
        height: 32px;
        border-radius: 2px;
        display: inline-block;
        margin-right: 0.6rem;
        vertical-align: middle;
    }}

    /* ── Section headings ───────────────────────────────── */
    .section-title {{
        font-size: 1.15rem;
        font-weight: 700;
        color: {TEXT_PRIMARY};
        margin: 1.8rem 0 0.8rem 0;
        display: flex;
        align-items: center;
        gap: 0.5rem;
    }}
    .section-dot {{
        width: 10px;
        height: 10px;
        border-radius: 50%;
        display: inline-block;
    }}

    /* ── Timing cards ───────────────────────────────────── */
    .timing-card {{
        background: {CARD_BG};
        border-radius: 10px;
        padding: 1rem 1.25rem;
        border: 1px solid {BORDER};
        text-align: center;
    }}
    .timing-label {{
        font-size: 0.72rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.4px;
        color: {TEXT_MUTED};
        margin-bottom: 0.25rem;
    }}
    .timing-value {{
        font-size: 1.5rem;
        font-weight: 700;
        color: {TEXT_PRIMARY};
    }}
    .timing-total {{
        background: linear-gradient(135deg, {TEAL}, {TEAL_DARK});
        border-radius: 10px;
        padding: 1rem 1.25rem;
        text-align: center;
        border: none;
    }}
    .timing-total .timing-label {{
        color: rgba(255,255,255,0.8);
    }}
    .timing-total .timing-value {{
        color: white;
    }}

    /* ── Date range badge ───────────────────────────────── */
    .date-badge {{
        display: inline-flex;
        align-items: center;
        gap: 0.5rem;
        background: {CARD_BG};
        border: 1px solid {BORDER};
        border-radius: 24px;
        padding: 0.5rem 1.25rem;
        font-size: 0.9rem;
        color: {TEXT_PRIMARY};
        font-weight: 500;
    }}
    .date-arrow {{
        color: {TEAL};
        font-weight: 700;
    }}

    /* ── Chart view selector ───────────────────────────── */
    div[data-testid="stHorizontalBlock"] .chart-view-bar {{
        display: flex;
        justify-content: center;
    }}
    div.stRadio > div {{
        display: flex !important;
        gap: 0 !important;
        background: {CARD_BG};
        border: 1px solid {BORDER};
        border-radius: 12px;
        padding: 4px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.04);
    }}
    div.stRadio > div > label {{
        flex: 1 !important;
        text-align: center !important;
        padding: 0.55rem 1.2rem !important;
        border-radius: 8px !important;
        font-weight: 600 !important;
        font-size: 0.82rem !important;
        cursor: pointer !important;
        transition: all 0.2s ease !important;
        white-space: nowrap !important;
        color: {TEXT_MUTED} !important;
        border: none !important;
    }}
    div.stRadio > div > label[data-checked="true"],
    div.stRadio > div > label:has(input:checked) {{
        background: linear-gradient(135deg, #00A3B2, #006770) !important;
        color: white !important;
        box-shadow: 0 2px 8px rgba(0,163,178,0.3) !important;
    }}
    section[data-testid="stMain"] div.stRadio > div > label:hover:not(:has(input:checked)) {{
        background: #F0FAFB !important;
        color: {TEXT_PRIMARY} !important;
    }}
    div.stRadio > div > label > div:first-child {{
        display: none !important;
    }}

    /* ── Clean up Streamlit defaults ────────────────────── */
    [data-testid="stMetric"] {{
        background: {CARD_BG};
        border: 1px solid {BORDER};
        border-radius: 12px;
        padding: 1rem;
    }}
    div[data-testid="stTabs"] button[data-baseweb="tab"] {{
        font-weight: 600;
        font-size: 0.85rem;
    }}
    .stDownloadButton > button {{
        border-radius: 10px !important;
        font-weight: 600 !important;
    }}

    /* ── Progress bar ───────────────────────────────────── */
    .stProgress > div > div > div {{
        background: linear-gradient(90deg, {TEAL}, #70C9D2) !important;
        border-radius: 8px;
    }}
    .stProgress > div > div {{
        background-color: #E0F2F4 !important;
        border-radius: 8px;
    }}
    .stProgress p {{
        color: {TEAL_DARK} !important;
        font-weight: 600 !important;
        font-size: 0.85rem !important;
    }}

    /* ── Status container ───────────────────────────────── */
    [data-testid="stStatusWidget"],
    details[data-testid="stExpander"],
    div[data-testid="stStatus"] {{
        border: 1px solid #B2E0E6 !important;
        border-radius: 12px !important;
        background: #F0FAFB !important;
    }}
    div[data-testid="stStatus"] summary {{
        color: {TEAL_DARK} !important;
        font-weight: 700 !important;
        font-size: 0.95rem !important;
    }}
    div[data-testid="stStatus"] summary span {{
        color: {TEAL_DARK} !important;
    }}
    div[data-testid="stStatus"] [data-testid="stMarkdown"] p {{
        color: {TEXT_PRIMARY} !important;
        font-size: 0.88rem;
    }}
    div[data-testid="stStatus"] [data-testid="stMarkdown"] strong {{
        color: {TEAL_DARK} !important;
    }}

    /* Hide Streamlit branding */
    #MainMenu {{visibility: hidden;}}
    footer {{visibility: hidden;}}
</style>
""", unsafe_allow_html=True)

# ──────────────────────────────────────────────────────────────────────────────
# Cached pipeline runner
# ──────────────────────────────────────────────────────────────────────────────

@st.cache_data(show_spinner=False)
def _cached_run(
    income_paths: tuple[str, ...],
    balance_paths: tuple[str, ...],
    sales_paths: tuple[str, ...],
    master_recon_path: str | None,
) -> dict:
    return run_reconciliation_from_paths(
        list(income_paths),
        list(balance_paths),
        list(sales_paths),
        master_recon_path=master_recon_path,
    )


def _run_with_progress(
    income_paths: tuple[str, ...],
    balance_paths: tuple[str, ...],
    sales_paths: tuple[str, ...],
    master_recon_path: str | None,
) -> dict:
    """Run the pipeline with a live st.status panel + progress bar."""
    TOTAL_STEPS = 6
    step = 0
    progress_bar = st.progress(0, text="Initialising…")
    overall_start = time.time()

    with st.status("Running reconciliation…", expanded=True) as status:
        def on_progress(msg: str):
            nonlocal step
            step += 1
            pct = min(step / TOTAL_STEPS, 1.0)
            progress_bar.progress(pct, text=msg)
            st.write(f"**Step {step}/{TOTAL_STEPS}** — {msg}")

        result = run_reconciliation_from_paths(
            list(income_paths),
            list(balance_paths),
            list(sales_paths),
            master_recon_path=master_recon_path,
            progress_callback=on_progress,
        )

        t0 = time.time()
        progress_bar.progress(1.0, text="📦 Exporting to Excel…")
        st.write("**Step 6/6** — 📦 Exporting to Excel…")
        excel_bytes = export_to_excel({
            "report":                  result["report"],
            "recon_report":            result["recon_report"],
            "Outstanding":             result["outstanding"],
            "Refund":                  result["refund"],
            "outstanding_with_refund": result["outstanding_with_refund"],
            "income_not_balance":      result["income_not_balance"],
            "balance_not_income":      result["balance_not_income"],
        })
        result["timings"].append(("Export Excel", time.time() - t0))
        result["excel_output"] = excel_bytes

        total_elapsed = time.time() - overall_start
        result["total_elapsed"] = total_elapsed
        status.update(
            label=f"Completed in {total_elapsed:.2f}s",
            state="complete",
        )

    progress_bar.empty()
    return result


def _collect_folder_files(root: Path) -> tuple[list[Path], list[Path], list[Path]]:
    """Scan a scenario folder and group Excel files by report type."""
    xlsx_files = sorted(root.rglob("*.xlsx"))
    income_files = [f for f in xlsx_files if f.name.startswith("Income.released")]
    balance_files = [f for f in xlsx_files if f.name.startswith("my_balance_transaction")]
    sales_files = [f for f in xlsx_files if f.name.startswith("SalesReport")]
    return income_files, balance_files, sales_files


def _find_master_recon_file(root: Path) -> Path | None:
    """Find the Shopee Payment Master List file in scenario or parent folder."""
    candidates = [
        *sorted(root.glob("*Shopee Payment Master List*.xlsx")),
        *sorted(root.parent.glob("*Shopee Payment Master List*.xlsx")),
    ]
    return candidates[0] if candidates else None


# ──────────────────────────────────────────────────────────────────────────────
# Sidebar
# ──────────────────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("# Watson Reconciliation")
    st.caption("Automated Shopee payment matching engine")
    st.markdown("---")

    page = st.radio(
        "Navigation",
        ["📊 Reconciliation Summary", "📋 Reconciliation Run"],
        index=0,
        label_visibility="collapsed",
    )
    st.markdown("---")

# Defaults for Reconciliation Run page variables
run_btn = False
folder_exists = False
all_found = False
force_rerun = False
income_paths: list[Path] = []
balance_paths: list[Path] = []
sales_paths: list[Path] = []
master_recon_file: Path | None = None

if page == "📋 Reconciliation Run":
  with st.sidebar:
    folder_path = st.text_input(
        "Scenario Folder Path",
        value=r"c:\Users\TanJunJie\OneDrive - SRKK Group\Project\watson_entriesmatching\OneDrive_2026-03-09\Shopee Sample Reports (Testing)\scenario2",
        help="Path to a scenario folder containing Income, Balance, and Sales Excel files.",
    )
    st.markdown("---")

    root = Path(folder_path).expanduser()
    folder_exists = root.exists() and root.is_dir()

    income_paths: list[Path] = []
    balance_paths: list[Path] = []
    sales_paths: list[Path] = []
    master_recon_file: Path | None = None
    if folder_exists:
        income_paths, balance_paths, sales_paths = _collect_folder_files(root)
        master_recon_file = _find_master_recon_file(root)

    all_found = bool(income_paths and balance_paths and sales_paths)

    if folder_exists and all_found:
        st.markdown(f"""
        <div style="background:rgba(191,178,249,0.1); border-radius:8px; padding:0.75rem; margin-bottom:0.75rem;">
            <div style="font-size:0.75rem; opacity:0.7;">FILES DETECTED</div>
            <div style="font-size:0.85rem; margin-top:0.3rem;">
                Income: <b>{len(income_paths)}</b> &nbsp;|&nbsp;
                Balance: <b>{len(balance_paths)}</b> &nbsp;|&nbsp;
                Sales: <b>{len(sales_paths)}</b>
            </div>
        </div>
        """, unsafe_allow_html=True)
        if master_recon_file:
            st.caption(f"Master Recon: {master_recon_file.name}")
        else:
            st.warning("Master Recon file not found.")

    force_rerun = st.checkbox("Force rerun (skip cache)", value=False)
    run_btn = st.button(
        "Run Reconciliation",
        type="primary",
        use_container_width=True,
        disabled=not (folder_exists and all_found),
    )

    if not folder_exists:
        st.error("Folder path does not exist.")
    elif not all_found:
        missing = [
            name for name, group in [
                ("Income", income_paths),
                ("Balance", balance_paths),
                ("Sales", sales_paths),
            ] if not group
        ]
        st.warning(f"Missing: {', '.join(missing)}")


# ──────────────────────────────────────────────────────────────────────────────
# Session-state
# ──────────────────────────────────────────────────────────────────────────────

if "result" not in st.session_state:
    st.session_state["result"] = None
if "run_history" not in st.session_state:
    st.session_state["run_history"] = []
if "run_results" not in st.session_state:
    st.session_state["run_results"] = {}  # Run ID -> result dict


# ──────────────────────────────────────────────────────────────────────────────
# Helper: styled metric card
# ──────────────────────────────────────────────────────────────────────────────

def _metric_card(label: str, value: str, accent: str = TEAL):
    return f"""
    <div class="metric-card">
        <div style="display:flex; align-items:center;">
            <span class="metric-accent" style="background:{accent};"></span>
            <div>
                <div class="metric-label">{label}</div>
                <div class="metric-value">{value}</div>
            </div>
        </div>
    </div>
    """


# ──────────────────────────────────────────────────────────────────────────────
# Helper: Plotly chart defaults
# ──────────────────────────────────────────────────────────────────────────────

_PLOTLY_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(family="Inter, system-ui, sans-serif", color=TEXT_PRIMARY),
)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE: Reconciliation Summary
# ══════════════════════════════════════════════════════════════════════════════

if page == "📊 Reconciliation Summary":
    mock_df = _generate_mock_runs()
    # Merge any live runs from session state
    if st.session_state["run_history"]:
        live_df = pd.DataFrame(st.session_state["run_history"])
        mock_df = pd.concat([mock_df, live_df], ignore_index=True)

    # ── Header ────────────────────────────────────────────────────
    st.markdown("""
    <div class="dashboard-header">
        <h1>Reconciliation Summary</h1>
        <p>Aggregated view of all reconciliation runs — filter, review, and drill into any run.</p>
    </div>
    """, unsafe_allow_html=True)

    # ── Sidebar date filter ───────────────────────────────────────
    with st.sidebar:
        st.markdown("### Filters")
        all_dates = mock_df["Run Date"].sort_values()
        date_min = all_dates.min()
        date_max = all_dates.max()
        date_range = st.date_input(
            "Run Date Range",
            value=(date_min, date_max),
            min_value=date_min,
            max_value=date_max,
            key="overview_date_range",
        )
        status_filter = st.selectbox(
            "Review Status",
            ["All", "⚠️ Needs Review", "✅ OK"],
            key="overview_status_filter",
        )

    # Apply filters
    filtered = mock_df.copy()
    if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
        filtered = filtered[
            (filtered["Run Date"] >= date_range[0])
            & (filtered["Run Date"] <= date_range[1])
        ]
    if status_filter != "All":
        filtered = filtered[filtered["Status"] == status_filter]

    # ── KPI Summary Cards ─────────────────────────────────────────
    total_runs = len(filtered)
    avg_match = filtered["Match Rate (%)"].mean() if total_runs else 0
    total_reviewed = filtered["Needs Review"].sum()
    total_ok = total_runs - total_reviewed
    sum_sales = filtered["Total Sales (RM)"].sum()
    sum_payment = filtered["Total Payment (RM)"].sum()
    sum_outstanding = filtered["Total Outstanding (RM)"].sum()
    sum_recon_rows = filtered["Recon Rows"].sum()
    avg_duration = filtered["Duration (s)"].mean() if total_runs else 0

    st.markdown(f'<div class="section-title"><span class="section-dot" style="background:{TEAL};"></span>Dashboard Summary</div>', unsafe_allow_html=True)

    k1, k2, k3, k4 = st.columns(4)
    k1.markdown(_metric_card("Total Runs", f"{total_runs}", TEAL), unsafe_allow_html=True)
    k2.markdown(_metric_card("Avg Match Rate", f"{avg_match:.1f}%", GREEN if avg_match >= 95 else AMBER), unsafe_allow_html=True)
    k3.markdown(_metric_card("Needs Review", f"{int(total_reviewed)}", RED if total_reviewed else GREEN), unsafe_allow_html=True)
    k4.markdown(_metric_card("All Clear", f"{int(total_ok)}", GREEN), unsafe_allow_html=True)

    st.markdown("<div style='height:0.75rem'></div>", unsafe_allow_html=True)

    k5, k6, k7, k8 = st.columns(4)
    k5.markdown(_metric_card("Total Sales", f"RM {sum_sales:,.2f}", PURPLE), unsafe_allow_html=True)
    k6.markdown(_metric_card("Total Payment", f"RM {sum_payment:,.2f}", TEAL), unsafe_allow_html=True)
    k7.markdown(_metric_card("Total Outstanding", f"RM {sum_outstanding:,.2f}", RED), unsafe_allow_html=True)
    k8.markdown(_metric_card("Avg Duration", f"{avg_duration:.1f}s", PINK), unsafe_allow_html=True)

    # ── Trend Charts ──────────────────────────────────────────────
    st.markdown(f'<div class="section-title"><span class="section-dot" style="background:{PURPLE};"></span>Trends</div>', unsafe_allow_html=True)

    trend_left, trend_right = st.columns(2)

    with trend_left:
        trend_data = filtered.sort_values("Run Date")
        fig_trend = go.Figure()
        fig_trend.add_trace(go.Scatter(
            x=trend_data["Run Date"], y=trend_data["Match Rate (%)"],
            mode="lines+markers",
            name="Match Rate",
            line=dict(color=TEAL, width=2.5),
            marker=dict(size=7),
            hovertemplate="<b>%{x}</b><br>Match Rate: %{y:.1f}%<extra></extra>",
        ))
        fig_trend.add_hline(y=95, line_dash="dash", line_color=GREEN,
                            annotation_text="Target 95%", annotation_position="top left")
        fig_trend.add_hline(y=92, line_dash="dot", line_color=RED,
                            annotation_text="Review Threshold 92%", annotation_position="bottom left")
        fig_trend.update_layout(
            **_PLOTLY_LAYOUT, height=380,
            title=dict(text="Match Rate Over Time", font=dict(size=14)),
            yaxis=dict(range=[80, 102], showgrid=True, gridcolor="#F0F0F0", title="Match Rate (%)"),
            xaxis=dict(showgrid=False, title="Run Date"),
            margin=dict(l=50, r=20, t=50, b=50),
        )
        st.plotly_chart(fig_trend, use_container_width=True, config={"displayModeBar": False})

    with trend_right:
        fig_vol = go.Figure()
        fig_vol.add_trace(go.Bar(
            x=trend_data["Run Date"], y=trend_data["Total Sales (RM)"],
            name="Sales", marker=dict(color=PURPLE, cornerradius=4),
            hovertemplate="<b>%{x}</b><br>Sales: RM %{y:,.0f}<extra></extra>",
        ))
        fig_vol.add_trace(go.Bar(
            x=trend_data["Run Date"], y=trend_data["Total Payment (RM)"],
            name="Payment", marker=dict(color=TEAL, cornerradius=4),
            hovertemplate="<b>%{x}</b><br>Payment: RM %{y:,.0f}<extra></extra>",
        ))
        fig_vol.update_layout(
            **_PLOTLY_LAYOUT, height=380, barmode="group",
            title=dict(text="Sales vs Payment Per Run", font=dict(size=14)),
            yaxis=dict(showgrid=True, gridcolor="#F0F0F0", title="Amount (RM)"),
            xaxis=dict(showgrid=False, title="Run Date"),
            legend=dict(orientation="h", yanchor="top", y=-0.18, xanchor="center", x=0.5),
            margin=dict(l=60, r=20, t=50, b=70),
        )
        st.plotly_chart(fig_vol, use_container_width=True, config={"displayModeBar": False})

    # ── Run History Table ─────────────────────────────────────────
    st.markdown(f'<div class="section-title"><span class="section-dot" style="background:{PINK};"></span>Reconciliation Run History</div>', unsafe_allow_html=True)

    display_cols = [
        "Run ID", "Run Date", "Period From", "Period To", "Status",
        "Match Rate (%)", "Recon Rows", "Outstanding Orders",
        "Total Sales (RM)", "Total Outstanding (RM)", "Duration (s)",
    ]
    display_df = filtered[display_cols].sort_values("Run Date", ascending=False).reset_index(drop=True)

    def _highlight_row(row):
        if row["Status"] == "⚠️ Needs Review":
            return ["background-color: #FEF3C7; color: #92400E;"] * len(row)
        return [""] * len(row)

    styled = display_df.style.apply(_highlight_row, axis=1).format({
        "Match Rate (%)": "{:.2f}",
        "Total Sales (RM)": "RM {:,.2f}",
        "Total Outstanding (RM)": "RM {:,.2f}",
        "Duration (s)": "{:.2f}s",
    })

    st.markdown(f"""
    <div style="display:flex; align-items:center; gap:0.5rem; margin:0.5rem 0;">
        <span style="background:{TEAL}; color:white; padding:0.15rem 0.6rem; border-radius:12px; font-size:0.78rem; font-weight:600;">
            {len(display_df):,}
        </span>
        <span style="color:{TEXT_MUTED}; font-size:0.82rem;">runs shown</span>
        <span style="background:{RED}; color:white; padding:0.15rem 0.6rem; border-radius:12px; font-size:0.78rem; font-weight:600; margin-left:0.5rem;">
            {int(filtered['Needs Review'].sum())}
        </span>
        <span style="color:{TEXT_MUTED}; font-size:0.82rem;">need review</span>
    </div>
    """, unsafe_allow_html=True)

    st.dataframe(styled, use_container_width=True, height=420)

    # ── Drill-Down Detail ─────────────────────────────────────────
    st.markdown(f'<div class="section-title"><span class="section-dot" style="background:{TEAL};"></span>Run Detail View</div>', unsafe_allow_html=True)

    run_ids = filtered.sort_values("Run Date", ascending=False)["Run ID"].tolist()
    selected_run_id = st.selectbox("Select a run to inspect", run_ids, key="detail_run_id")

    if selected_run_id:
        run_row = filtered[filtered["Run ID"] == selected_run_id].iloc[0]
        is_review = run_row["Needs Review"]

        # ── Find previous run for period-over-period comparison ──
        all_sorted = filtered.sort_values("Run Date", ascending=True).reset_index(drop=True)
        current_idx = all_sorted[all_sorted["Run ID"] == selected_run_id].index[0]
        prev_row = all_sorted.iloc[current_idx - 1] if current_idx > 0 else None

        def _delta_html(current_val, prev_val, fmt=",.2f", prefix="", suffix="", invert=False):
            """Return a small ▲/▼ delta indicator HTML string."""
            if prev_val is None:
                return ""
            diff = current_val - prev_val
            if diff == 0:
                return f'<span style="font-size:0.72rem; color:{TEXT_MUTED}; margin-left:0.3rem;">— vs prev</span>'
            # For metrics where higher is worse (outstanding, fees%), invert colours
            is_up = diff > 0
            if invert:
                color = RED if is_up else GREEN
            else:
                color = GREEN if is_up else RED
            arrow = "▲" if is_up else "▼"
            return f'<span style="font-size:0.72rem; color:{color}; margin-left:0.3rem;">{arrow} {prefix}{abs(diff):{fmt}}{suffix} vs prev</span>'

        # Detail header
        status_color = RED if is_review else GREEN
        status_label = run_row["Status"]
        st.markdown(f"""
        <div style="background:{CARD_BG}; border:2px solid {status_color}; border-radius:12px; padding:1.25rem 1.5rem; margin-bottom:1rem;">
            <div style="display:flex; justify-content:space-between; align-items:center;">
                <div>
                    <span style="font-size:1.3rem; font-weight:700; color:{TEXT_PRIMARY};">{run_row['Run ID']}</span>
                    <span style="margin-left:1rem; font-size:0.88rem; color:{TEXT_MUTED};">Run Date: {run_row['Run Date']}</span>
                </div>
                <div style="background:{status_color}; color:white; padding:0.3rem 1rem; border-radius:20px; font-weight:600; font-size:0.85rem;">
                    {status_label}
                </div>
            </div>
            <div style="margin-top:0.5rem; font-size:0.88rem; color:{TEXT_MUTED};">
                Period: {run_row['Period From']} &rarr; {run_row['Period To']} &nbsp;&nbsp;|&nbsp;&nbsp; Duration: {run_row['Duration (s)']:.2f}s
            </div>
        </div>
        """, unsafe_allow_html=True)

        if is_review:
            st.warning(f"**Review Reasons:** {run_row['Review Reasons']}")

        # ── SECTION 1: Financial Health (the money story) ──────────
        st.markdown(f'<div class="section-title"><span class="section-dot" style="background:{PURPLE};"></span>Financial Health</div>', unsafe_allow_html=True)

        match_rate_val = run_row["Match Rate (%)"]
        gauge_color = TEAL if match_rate_val >= 95 else (AMBER if match_rate_val >= 92 else RED)
        fees_pct_val = run_row.get("Fees % of Sales", 0)
        if fees_pct_val == 0 and run_row["Total Sales (RM)"] > 0:
            fees_pct_val = round(run_row["Total Fees (RM)"] / run_row["Total Sales (RM)"] * 100, 2)
        fees_color = GREEN if fees_pct_val <= 8 else (AMBER if fees_pct_val <= 12 else RED)

        # Waterfall: Sales - Payment - Fees = Outstanding (full width)
        f1, op1, f2, op2, f3, op3, f4 = st.columns([3, 0.5, 3, 0.5, 3, 0.5, 3])
        prev_sales = prev_row["Total Sales (RM)"] if prev_row is not None else None
        prev_payment = prev_row["Total Payment (RM)"] if prev_row is not None else None
        prev_fees = prev_row["Total Fees (RM)"] if prev_row is not None else None
        prev_outstanding = prev_row["Total Outstanding (RM)"] if prev_row is not None else None

        f1.markdown(f"""
        <div class="metric-card">
            <div style="display:flex; align-items:center;"><span class="metric-accent" style="background:{TEAL};"></span>
            <div><div class="metric-label">TOTAL SALES</div>
            <div class="metric-value">RM {run_row['Total Sales (RM)']:,.2f}</div>
            {_delta_html(run_row['Total Sales (RM)'], prev_sales, prefix='RM ')}
            </div></div></div>""", unsafe_allow_html=True)
        op1.markdown(f'<div style="display:flex;align-items:center;justify-content:center;font-size:1.5rem;font-weight:700;color:{TEXT_MUTED};padding-top:1rem;">\u2212</div>', unsafe_allow_html=True)
        f2.markdown(f"""
        <div class="metric-card">
            <div style="display:flex; align-items:center;"><span class="metric-accent" style="background:{PURPLE};"></span>
            <div><div class="metric-label">TOTAL PAYMENT</div>
            <div class="metric-value">RM {run_row['Total Payment (RM)']:,.2f}</div>
            {_delta_html(run_row['Total Payment (RM)'], prev_payment, prefix='RM ')}
            </div></div></div>""", unsafe_allow_html=True)
        op2.markdown(f'<div style="display:flex;align-items:center;justify-content:center;font-size:1.5rem;font-weight:700;color:{TEXT_MUTED};padding-top:1rem;">\u2212</div>', unsafe_allow_html=True)
        f3.markdown(f"""
        <div class="metric-card">
            <div style="display:flex; align-items:center;"><span class="metric-accent" style="background:{AMBER};"></span>
            <div><div class="metric-label">TOTAL FEES</div>
            <div class="metric-value">RM {run_row['Total Fees (RM)']:,.2f}</div>
            {_delta_html(run_row['Total Fees (RM)'], prev_fees, prefix='RM ', invert=True)}
            </div></div></div>""", unsafe_allow_html=True)
        op3.markdown(f'<div style="display:flex;align-items:center;justify-content:center;font-size:1.5rem;font-weight:700;color:{RED};padding-top:1rem;">\uff1d</div>', unsafe_allow_html=True)
        outstanding_val = run_row['Total Outstanding (RM)']
        outstanding_color = GREEN if outstanding_val == 0 else RED
        f4.markdown(f"""
        <div class="metric-card">
            <div style="display:flex; align-items:center;"><span class="metric-accent" style="background:{outstanding_color};"></span>
            <div><div class="metric-label">OUTSTANDING</div>
            <div class="metric-value" style="color:{outstanding_color};">RM {outstanding_val:,.2f}</div>
            {_delta_html(outstanding_val, prev_outstanding, prefix='RM ', invert=True)}
            </div></div></div>""", unsafe_allow_html=True)

        st.markdown("<div style='height:0.5rem'></div>", unsafe_allow_html=True)

        # Match Rate Gauge (below waterfall, centered in a narrower column)
        gauge_spacer_l, gauge_center, gauge_spacer_r = st.columns([1, 2, 1])
        with gauge_center:
            fig_gauge = go.Figure(go.Indicator(
                mode="gauge+number",
                value=match_rate_val,
                number=dict(suffix="%", font=dict(size=36)),
                title=dict(text="Match Rate", font=dict(size=14)),
                gauge=dict(
                    axis=dict(range=[0, 100]),
                    bar=dict(color=gauge_color),
                    bgcolor="#F0F0F0",
                    steps=[
                        dict(range=[0, 92], color="#FDE8E8"),
                        dict(range=[92, 95], color="#FEF3C7"),
                        dict(range=[95, 100], color="#D1FAE5"),
                    ],
                ),
            ))
            fig_gauge.update_layout(
                **_PLOTLY_LAYOUT, height=260,
                margin=dict(l=30, r=30, t=60, b=10),
            )
            st.plotly_chart(fig_gauge, use_container_width=True, config={"displayModeBar": False})

        # \u2500\u2500 SECTION 2: Fees & Trend Analysis \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
        st.markdown(f'<div class="section-title"><span class="section-dot" style="background:{AMBER};"></span>Fee Analysis & Outstanding Trend</div>', unsafe_allow_html=True)

        fee_col, trend_col = st.columns(2)

        with fee_col:
            # Fees % of Sales gauge
            prev_fees_pct = None
            if prev_row is not None and prev_row["Total Sales (RM)"] > 0:
                prev_fees_pct = round(prev_row["Total Fees (RM)"] / prev_row["Total Sales (RM)"] * 100, 2)

            fig_fee = go.Figure(go.Indicator(
                mode="gauge+number+delta",
                value=fees_pct_val,
                number=dict(suffix="%", font=dict(size=34)),
                title=dict(text="Fees % of Sales", font=dict(size=14)),
                delta=dict(
                    reference=prev_fees_pct if prev_fees_pct is not None else fees_pct_val,
                    increasing=dict(color=RED),
                    decreasing=dict(color=GREEN),
                    suffix="%",
                    font=dict(size=14),
                ),
                gauge=dict(
                    axis=dict(range=[0, 20]),
                    bar=dict(color=fees_color),
                    bgcolor="#F0F0F0",
                    steps=[
                        dict(range=[0, 5], color="#D1FAE5"),
                        dict(range=[5, 8], color="#E8FFE8"),
                        dict(range=[8, 12], color="#FEF3C7"),
                        dict(range=[12, 20], color="#FDE8E8"),
                    ],
                    threshold=dict(
                        line=dict(color=RED, width=3),
                        thickness=0.8,
                        value=12,
                    ),
                ),
            ))
            fig_fee.update_layout(
                **_PLOTLY_LAYOUT, height=300,
                margin=dict(l=30, r=30, t=60, b=20),
            )
            st.plotly_chart(fig_fee, use_container_width=True, config={"displayModeBar": False})

            # Fee context card
            fee_status = "Within normal range" if fees_pct_val <= 8 else ("Above average — verify contract" if fees_pct_val <= 12 else "Exceeds threshold — investigate")
            fee_icon = "✅" if fees_pct_val <= 8 else ("⚠️" if fees_pct_val <= 12 else "🚨")
            st.markdown(f"""
            <div style="background:{CARD_BG}; border:1px solid {BORDER}; border-radius:8px; padding:0.75rem 1rem; font-size:0.85rem;">
                {fee_icon} <b>Fee Rate:</b> {fees_pct_val:.2f}% &nbsp;&nbsp;|&nbsp;&nbsp;
                <b>Expected Shopee range:</b> 3-8% &nbsp;&nbsp;|&nbsp;&nbsp;
                <b>Assessment:</b> {fee_status}
            </div>
            """, unsafe_allow_html=True)

        with trend_col:
            # Outstanding Amount Trend — last N runs
            trend_runs = all_sorted[["Run Date", "Total Outstanding (RM)", "Outstanding Orders", "Run ID"]].copy()
            trend_runs = trend_runs.tail(10)  # show up to last 10 runs

            bar_colors = [GREEN if v == 0 else RED for v in trend_runs["Total Outstanding (RM)"]]
            # Highlight current run
            highlight = ["rgba(0,0,0,0.15)" if rid == selected_run_id else "rgba(0,0,0,0)" for rid in trend_runs["Run ID"]]

            fig_trend_out = go.Figure()
            fig_trend_out.add_trace(go.Bar(
                x=trend_runs["Run Date"].astype(str),
                y=trend_runs["Total Outstanding (RM)"],
                marker=dict(color=bar_colors, cornerradius=4,
                            line=dict(color=highlight, width=3)),
                text=[f"RM {v:,.0f}" for v in trend_runs["Total Outstanding (RM)"]],
                textposition="outside",
                textfont=dict(size=10),
                hovertemplate="<b>%{x}</b><br>Outstanding: RM %{y:,.2f}<extra></extra>",
            ))
            fig_trend_out.add_hline(y=0, line_color=GREEN, line_width=2)
            fig_trend_out.update_layout(
                **_PLOTLY_LAYOUT, height=300,
                title=dict(text="Outstanding Amount (Last Runs)", font=dict(size=14)),
                yaxis=dict(showgrid=True, gridcolor="#F0F0F0", title="Outstanding (RM)"),
                xaxis=dict(showgrid=False, title="Run Date", tickangle=-45),
                margin=dict(l=60, r=20, t=50, b=70),
            )
            st.plotly_chart(fig_trend_out, use_container_width=True, config={"displayModeBar": False})

            # Trend assessment
            if len(trend_runs) >= 2:
                last_two = trend_runs["Total Outstanding (RM)"].tail(2).tolist()
                if last_two[-1] == 0:
                    st.success("✅ Outstanding is zero this run — fully reconciled.")
                elif last_two[-1] < last_two[-2]:
                    st.info(f"ℹ️ Outstanding decreased from RM {last_two[-2]:,.2f} to RM {last_two[-1]:,.2f} — improving.")
                elif last_two[-1] > last_two[-2]:
                    st.warning(f"⚠️ Outstanding increased from RM {last_two[-2]:,.2f} to RM {last_two[-1]:,.2f} — investigate.")
                else:
                    st.info(f"— Outstanding unchanged at RM {last_two[-1]:,.2f}.")

        # ── SECTION 3: Exceptions & Data Quality ───────────────────
        st.markdown(f'<div class="section-title"><span class="section-dot" style="background:{RED};"></span>Exceptions & Data Quality</div>', unsafe_allow_html=True)

        exc1, exc2, exc3, exc4 = st.columns(4)

        prev_oo = prev_row["Outstanding Orders"] if prev_row is not None else None
        prev_ref = prev_row["Refund Orders"] if prev_row is not None else None
        prev_inb = prev_row["Income Not In Balance"] if prev_row is not None else None
        prev_bni = prev_row["Balance Not In Income"] if prev_row is not None else None

        exc1.markdown(f"""
        <div class="metric-card">
            <div style="display:flex; align-items:center;"><span class="metric-accent" style="background:{RED if run_row['Outstanding Orders'] > 0 else GREEN};"></span>
            <div><div class="metric-label">OUTSTANDING ORDERS</div>
            <div class="metric-value">{run_row['Outstanding Orders']:,}</div>
            {_delta_html(run_row['Outstanding Orders'], prev_oo, fmt=',', invert=True)}
            </div></div></div>""", unsafe_allow_html=True)

        exc2.markdown(f"""
        <div class="metric-card">
            <div style="display:flex; align-items:center;"><span class="metric-accent" style="background:{PINK};"></span>
            <div><div class="metric-label">REFUND ORDERS</div>
            <div class="metric-value">{run_row['Refund Orders']:,}</div>
            {_delta_html(run_row['Refund Orders'], prev_ref, fmt=',', invert=True)}
            </div></div></div>""", unsafe_allow_html=True)

        exc3.markdown(f"""
        <div class="metric-card">
            <div style="display:flex; align-items:center;"><span class="metric-accent" style="background:{RED_DARK};"></span>
            <div><div class="metric-label">INCOME NOT IN BALANCE</div>
            <div class="metric-value">{run_row['Income Not In Balance']:,}</div>
            {_delta_html(run_row['Income Not In Balance'], prev_inb, fmt=',', invert=True)}
            </div></div></div>""", unsafe_allow_html=True)

        exc4.markdown(f"""
        <div class="metric-card">
            <div style="display:flex; align-items:center;"><span class="metric-accent" style="background:{PURPLE_DARK};"></span>
            <div><div class="metric-label">BALANCE NOT IN INCOME</div>
            <div class="metric-value">{run_row['Balance Not In Income']:,}</div>
            {_delta_html(run_row['Balance Not In Income'], prev_bni, fmt=',', invert=True)}
            </div></div></div>""", unsafe_allow_html=True)

        st.markdown("<div style='height:0.5rem'></div>", unsafe_allow_html=True)

        # Collapsible: Raw data counts (less important, available on demand)
        with st.expander("📋 Raw Data Counts (click to expand)"):
            rc1, rc2, rc3, rc4 = st.columns(4)
            rc1.markdown(_metric_card("Income Rows", f"{run_row['Income Rows']:,}", PURPLE), unsafe_allow_html=True)
            rc2.markdown(_metric_card("Balance Rows", f"{run_row['Balance Rows']:,}", TEAL), unsafe_allow_html=True)
            rc3.markdown(_metric_card("Sales Rows", f"{run_row['Sales Rows']:,}", PINK), unsafe_allow_html=True)
            rc4.markdown(_metric_card("Recon Rows", f"{run_row['Recon Rows']:,}", PURPLE_DARK), unsafe_allow_html=True)

        # ── Data tables for live runs ───────────────────────────────
        live_result = st.session_state.get("run_results", {}).get(selected_run_id)
        if live_result is not None:
            st.markdown(f'<div class="section-title"><span class="section-dot" style="background:{PINK};"></span>Reconciliation Data</div>', unsafe_allow_html=True)

            live_stats = live_result["stats"]
            tab_labels = [
                f"Reconciliation  ({live_stats['recon_rows']:,})",
                f"Outstanding  ({live_stats['outstanding_rows']:,})",
                f"Refund  ({live_stats['refund_rows']:,})",
                f"Income Not In Balance  ({live_stats['income_not_balance']:,})",
                f"Balance Not In Income  ({live_stats['balance_not_income']:,})",
            ]
            tab_keys = ["ov_recon", "ov_out", "ov_refund", "ov_ib", "ov_bi"]
            data_keys = ["recon_report", "outstanding", "refund", "income_not_balance", "balance_not_income"]

            tabs = st.tabs(tab_labels)
            for tab, dk, tk in zip(tabs, data_keys, tab_keys):
                with tab:
                    df_pl = live_result.get(dk)
                    if df_pl is not None and not df_pl.is_empty():
                        pdf = df_pl.to_pandas()
                        st.markdown(f"""
                        <div style="display:flex; align-items:center; gap:0.5rem; margin:0.5rem 0;">
                            <span style="background:{TEAL}; color:white; padding:0.15rem 0.6rem; border-radius:12px; font-size:0.78rem; font-weight:600;">
                                {len(pdf):,}
                            </span>
                            <span style="color:{TEXT_MUTED}; font-size:0.82rem;">rows</span>
                        </div>
                        """, unsafe_allow_html=True)
                        st.dataframe(pdf, use_container_width=True, height=420)
                    else:
                        st.info("No records to display.")

    st.stop()


# ──────────────────────────────────────────────────────────────────────────────
# Run pipeline when button clicked
# ──────────────────────────────────────────────────────────────────────────────

if run_btn and folder_exists and all_found:
    if force_rerun:
        _cached_run.clear()
    result = _run_with_progress(
        income_paths=tuple(str(p) for p in income_paths),
        balance_paths=tuple(str(p) for p in balance_paths),
        sales_paths=tuple(str(p) for p in sales_paths),
        master_recon_path=str(master_recon_file) if master_recon_file else None,
    )
    st.session_state["result"] = result

    # ── Save run summary to history for the Summary page ──────
    s = result["stats"]
    recon_pdf = result["recon_report"].to_pandas()
    total_sales = recon_pdf["SalesCenterAmount"].sum() if "SalesCenterAmount" in recon_pdf.columns else 0
    total_payment = recon_pdf["Payment Amount"].sum() if "Payment Amount" in recon_pdf.columns else 0
    total_fees_val = sum(
        recon_pdf[c].sum() if c in recon_pdf.columns else 0
        for c in ["Commission Fee", "Transaction Fee", "Service Fee",
                  "Actual Shipping Fee", "AMS Commission Fee", "Return QC Fee"]
    )
    total_outstanding_val = recon_pdf["Outstanding"].sum() if "Outstanding" in recon_pdf.columns else 0
    recon_rows = s["recon_rows"]
    outstanding_rows = s["outstanding_rows"]
    match_rate = round((recon_rows - outstanding_rows) / recon_rows * 100, 2) if recon_rows else 0
    run_id = hashlib.sha256(
        f"live-{datetime.datetime.now().isoformat()}".encode()
    ).hexdigest()[:8].upper()

    # Flag for human review when outstanding is not zero
    needs_review = outstanding_rows != 0 or total_outstanding_val != 0
    review_reasons = []
    if outstanding_rows != 0:
        review_reasons.append(f"Outstanding orders ({outstanding_rows})")
    if total_outstanding_val != 0:
        review_reasons.append(f"Outstanding amount (RM {total_outstanding_val:,.2f})")

    date_from = s.get("date_from", "N/A")
    date_to = s.get("date_to", "N/A")
    run_record = {
        "Run ID": f"RUN-{run_id}",
        "Run Date": datetime.date.today(),
        "Period From": date_from,
        "Period To": date_to,
        "Income Rows": s["income_rows"],
        "Balance Rows": s["balance_rows"],
        "Sales Rows": s["sales_rows"],
        "Recon Rows": recon_rows,
        "Outstanding Orders": outstanding_rows,
        "Refund Orders": s["refund_rows"],
        "Match Rate (%)": match_rate,
        "Total Sales (RM)": round(total_sales, 2),
        "Total Payment (RM)": round(total_payment, 2),
        "Total Fees (RM)": round(total_fees_val, 2),
        "Total Outstanding (RM)": round(total_outstanding_val, 2),
        "Income Not In Balance": s["income_not_balance"],
        "Balance Not In Income": s["balance_not_income"],
        "Duration (s)": round(result.get("total_elapsed", 0), 2),
        "Fees % of Sales": round(total_fees_val / total_sales * 100, 2) if total_sales else 0,
        "Needs Review": needs_review,
        "Review Reasons": "; ".join(review_reasons) if review_reasons else "\u2014",
        "Status": "\u26a0\ufe0f Needs Review" if needs_review else "\u2705 OK",
        "Source": "Live",
    }
    st.session_state["run_history"].append(run_record)
    st.session_state["run_results"][f"RUN-{run_id}"] = result


# ──────────────────────────────────────────────────────────────────────────────
# Header
# ──────────────────────────────────────────────────────────────────────────────

st.markdown("""
<div class="dashboard-header">
    <h1>Shopee Reconciliation Dashboard</h1>
    <p>Select a scenario folder in the sidebar, then run the reconciliation engine.</p>
</div>
""", unsafe_allow_html=True)

result = st.session_state["result"]

if result is None:
    st.markdown(f"""
    <div style="text-align:center; padding:4rem 2rem;">
        <div style="font-size:3rem; margin-bottom:1rem;">📊</div>
        <div style="font-size:1.1rem; color:{TEXT_MUTED}; max-width:420px; margin:0 auto;">
            No results yet. Enter a valid scenario folder path in the sidebar
            and click <b>Run Reconciliation</b> to begin.
        </div>
    </div>
    """, unsafe_allow_html=True)
    st.stop()


# ──────────────────────────────────────────────────────────────────────────────
# Date range & Download
# ──────────────────────────────────────────────────────────────────────────────

s = result["stats"]
date_from = s.get("date_from", "N/A")
date_to = s.get("date_to", "N/A")

top_left, top_right = st.columns([3, 1])
with top_left:
    st.markdown(f"""
    <div class="date-badge">
        <span style="font-weight:600;">Dataset Period</span>
        <span>{date_from}</span>
        <span class="date-arrow">→</span>
        <span>{date_to}</span>
    </div>
    """, unsafe_allow_html=True)
with top_right:
    excel_data = result.get("excel_output")
    if excel_data is not None:
        st.download_button(
            label="Download Excel Report",
            data=excel_data.getvalue(),
            file_name="reconciliation_output.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            type="primary",
            use_container_width=True,
        )

# ──────────────────────────────────────────────────────────────────────────────
# Timing breakdown
# ──────────────────────────────────────────────────────────────────────────────

timings = result.get("timings", [])
total_elapsed = result.get("total_elapsed")

if timings:
    st.markdown(f'<div class="section-title"><span class="section-dot" style="background:{PURPLE};"></span>Timing Breakdown</div>', unsafe_allow_html=True)

    n = len(timings)
    step_names = [t[0] for t in timings]
    durations = [t[1] for t in timings]
    # Assign Watsons Teal palette to the largest slice, other colors for the rest
    _other_colors = [PINK, PURPLE, PINK_DARK, PURPLE_DARK, RED]
    max_idx = durations.index(max(durations))
    donut_colors = []
    other_i = 0
    for i in range(n):
        if i == max_idx:
            donut_colors.append("#00A3B2")  # Watsons Teal
        else:
            donut_colors.append(_other_colors[other_i % len(_other_colors)])
            other_i += 1

    chart_col, total_col = st.columns([3, 1])

    with chart_col:
        fig_timing = go.Figure(go.Pie(
            labels=step_names,
            values=durations,
            hole=0.5,
            marker=dict(colors=donut_colors),
            textinfo="label+value",
            texttemplate="<b>%{label}</b><br>%{value:.2f}s (%{percent})",
            textposition="outside",
            textfont=dict(size=13, family="Inter, sans-serif"),
            hovertemplate="<b>%{label}</b><br>%{value:.2f}s<br>%{percent}<extra></extra>",
            sort=False,
            automargin=True,
        ))
        total_text = f"{total_elapsed:.2f}s" if total_elapsed is not None else ""
        fig_timing.update_layout(
            **_PLOTLY_LAYOUT,
            height=520,
            showlegend=True,
            legend=dict(orientation="h", yanchor="top", y=-0.1, xanchor="center", x=0.5, font=dict(size=12)),
            margin=dict(l=120, r=120, t=40, b=80),
            uniformtext=dict(minsize=10, mode="show"),
            annotations=[dict(
                text=f"<b>{total_text}</b><br><span style='font-size:12px;color:{TEXT_MUTED}'>Total</span>",
                x=0.5, y=0.5, font_size=22, showarrow=False,
            )],
        )
        st.plotly_chart(fig_timing, use_container_width=True, config={"displayModeBar": False})

    with total_col:
        if total_elapsed is not None:
            st.markdown(f"""
            <div style="display:flex; align-items:center; height:520px;">
                <div class="timing-total" style="width:100%;">
                    <div class="timing-label">Total</div>
                    <div class="timing-value">{total_elapsed:.2f}s</div>
                </div>
            </div>
            """, unsafe_allow_html=True)

# ──────────────────────────────────────────────────────────────────────────────
# Summary metrics
# ──────────────────────────────────────────────────────────────────────────────

st.markdown(f'<div class="section-title"><span class="section-dot" style="background:{TEAL};"></span>Summary Overview</div>', unsafe_allow_html=True)

# Row 1: data source counts
m1, m2, m3, m4 = st.columns(4)
m1.markdown(_metric_card("Income Rows", f"{s['income_rows']:,}", PURPLE), unsafe_allow_html=True)
m2.markdown(_metric_card("Balance Rows", f"{s['balance_rows']:,}", TEAL), unsafe_allow_html=True)
m3.markdown(_metric_card("Sales Rows", f"{s['sales_rows']:,}", PINK), unsafe_allow_html=True)
m4.markdown(_metric_card("Recon Rows", f"{s['recon_rows']:,}", PURPLE_DARK), unsafe_allow_html=True)

st.markdown("<div style='height:0.75rem'></div>", unsafe_allow_html=True)

# Row 2: result counts
m5, m6, m7, m8 = st.columns(4)
m5.markdown(_metric_card("Outstanding Orders", f"{s['outstanding_rows']:,}", RED), unsafe_allow_html=True)
m6.markdown(_metric_card("Refund Orders", f"{s['refund_rows']:,}", PINK), unsafe_allow_html=True)
m7.markdown(_metric_card("Compare Rows", f"{s.get('compare_rows', 0):,}", TEAL), unsafe_allow_html=True)
m8.markdown(_metric_card("Income Not In Balance", f"{s['income_not_balance']:,}", RED_DARK), unsafe_allow_html=True)

st.markdown("<div style='height:0.75rem'></div>", unsafe_allow_html=True)

m9, _, _, _ = st.columns(4)
m9.markdown(_metric_card("Balance Not In Income", f"{s['balance_not_income']:,}", PURPLE_DARK), unsafe_allow_html=True)

# ──────────────────────────────────────────────────────────────────────────────
# Charts row
# ──────────────────────────────────────────────────────────────────────────────

st.markdown(f'<div class="section-title"><span class="section-dot" style="background:{PINK};"></span>Visual Analytics</div>', unsafe_allow_html=True)

chart_left, chart_right = st.columns(2)

with chart_left:
    # Donut: data source distribution
    labels = ["Income", "Balance", "Sales"]
    values = [s["income_rows"], s["balance_rows"], s["sales_rows"]]
    fig_donut = go.Figure(go.Pie(
        labels=labels,
        values=values,
        hole=0.55,
        marker=dict(colors=[PURPLE, TEAL, PINK]),
        textinfo="label+percent",
        textfont=dict(size=13, family="Inter, sans-serif"),
        hovertemplate="<b>%{label}</b><br>%{value:,.0f} rows<br>%{percent}<extra></extra>",
    ))
    fig_donut.update_layout(
        **_PLOTLY_LAYOUT,
        height=320,
        showlegend=False,
        margin=dict(l=0, r=0, t=30, b=0),
        title=dict(text="Data Source Distribution", font=dict(size=14)),
        annotations=[dict(
            text=f"<b>{sum(values):,}</b><br><span style='font-size:11px;color:{TEXT_MUTED}'>Total rows</span>",
            x=0.5, y=0.5, font_size=18, showarrow=False,
        )],
    )
    st.plotly_chart(fig_donut, use_container_width=True, config={"displayModeBar": False})

with chart_right:
    # Bar: reconciliation outcomes
    matched = s["recon_rows"] - s["outstanding_rows"]
    cat_labels = ["Matched", "Outstanding", "Refund", "Income Not\nIn Balance", "Balance Not\nIn Income"]
    cat_values = [matched, s["outstanding_rows"], s["refund_rows"], s["income_not_balance"], s["balance_not_income"]]
    cat_colors = [TEAL, RED, PINK, PURPLE, PURPLE_DARK]

    fig_bar = go.Figure(go.Bar(
        x=cat_labels,
        y=cat_values,
        marker=dict(color=cat_colors, cornerradius=6),
        text=[f"{v:,}" for v in cat_values],
        textposition="outside",
        textfont=dict(size=12, family="Inter, sans-serif"),
    ))
    fig_bar.update_layout(
        **_PLOTLY_LAYOUT,
        height=320,
        title=dict(text="Reconciliation Outcomes", font=dict(size=14)),
        yaxis=dict(showgrid=True, gridcolor="#F0F0F0"),
        xaxis=dict(showgrid=False),
        margin=dict(l=40, r=20, t=40, b=60),
    )
    st.plotly_chart(fig_bar, use_container_width=True, config={"displayModeBar": False})

# ──────────────────────────────────────────────────────────────────────────────
# Financial Summary
# ──────────────────────────────────────────────────────────────────────────────

st.markdown(f'<div class="section-title"><span class="section-dot" style="background:{TEAL};"></span>Financial Summary</div>', unsafe_allow_html=True)

recon_df = result["recon_report"]
recon_pdf = recon_df.to_pandas()

total_sales = recon_pdf["SalesCenterAmount"].sum() if "SalesCenterAmount" in recon_pdf.columns else 0
total_payment = recon_pdf["Payment Amount"].sum() if "Payment Amount" in recon_pdf.columns else 0
total_commission = recon_pdf["Commission Fee"].sum() if "Commission Fee" in recon_pdf.columns else 0
total_transaction = recon_pdf["Transaction Fee"].sum() if "Transaction Fee" in recon_pdf.columns else 0
total_service = recon_pdf["Service Fee"].sum() if "Service Fee" in recon_pdf.columns else 0
total_shipping = recon_pdf["Actual Shipping Fee"].sum() if "Actual Shipping Fee" in recon_pdf.columns else 0
total_ams = recon_pdf["AMS Commission Fee"].sum() if "AMS Commission Fee" in recon_pdf.columns else 0
total_returnqc = recon_pdf["Return QC Fee"].sum() if "Return QC Fee" in recon_pdf.columns else 0
total_outstanding = recon_pdf["Outstanding"].sum() if "Outstanding" in recon_pdf.columns else 0
total_fees = total_commission + total_transaction + total_service + total_shipping + total_ams + total_returnqc
match_rate = ((s["recon_rows"] - s["outstanding_rows"]) / s["recon_rows"] * 100) if s["recon_rows"] > 0 else 0

# ── Row 1: Financial cards with formula symbols ──────────────────────────────
# Layout: Sales  −  Payment  +  Fees & Charges  =  Outstanding
_OP_STYLE = (
    "display:flex; align-items:center; justify-content:center; "
    "font-size:1.8rem; font-weight:700; color:{color}; "
    "padding-top:1.2rem;"
)

f1, op1, f2, op2, f3, op3, f4 = st.columns([3, 0.5, 3, 0.5, 3, 0.5, 3])
f1.markdown(_metric_card("Total Sales Amount", f"RM {total_sales:,.2f}", "#00A3B2"), unsafe_allow_html=True)
op1.markdown(f'<div style="{_OP_STYLE.format(color=TEXT_MUTED)}">−</div>', unsafe_allow_html=True)
f2.markdown(_metric_card("Total Payment Received", f"RM {total_payment:,.2f}", "#006770"), unsafe_allow_html=True)
op2.markdown(f'<div style="{_OP_STYLE.format(color=TEXT_MUTED)}">+</div>', unsafe_allow_html=True)
f3.markdown(_metric_card("Total Fees & Charges", f"RM {total_fees:,.2f}", PURPLE_DARK), unsafe_allow_html=True)
op3.markdown(f'<div style="{_OP_STYLE.format(color=RED)}">＝</div>', unsafe_allow_html=True)
f4.markdown(_metric_card("Total Outstanding", f"RM {total_outstanding:,.2f}", RED), unsafe_allow_html=True)

st.markdown("<div style='height:0.75rem'></div>", unsafe_allow_html=True)

# ── Chart view selector ──────────────────────────────────────────────────────
chart_view = st.radio(
    "Select chart view",
    ["📊 Match Rate & Payment Flow", "💰 Fees & Daily Payments", "📈 Trends & Distribution"],
    horizontal=True,
    label_visibility="collapsed",
)

st.markdown("<div style='height:0.5rem'></div>", unsafe_allow_html=True)

# ── View 1: Match Rate Gauge + Waterfall Chart ──────────────────────────────
if chart_view == "📊 Match Rate & Payment Flow":
    gauge_col, waterfall_col = st.columns(2)

    with gauge_col:
        gauge_color = "#00A3B2" if match_rate >= 95 else ("#70C9D2" if match_rate >= 80 else RED)
        fig_gauge = go.Figure(go.Indicator(
            mode="gauge+number+delta",
            value=match_rate,
            number=dict(suffix="%", font=dict(size=42, family="Inter, sans-serif")),
            delta=dict(reference=100, valueformat=".1f", suffix="%",
                       increasing=dict(color="#00A3B2"), decreasing=dict(color=RED)),
            title=dict(text="Reconciliation Match Rate", font=dict(size=16)),
            gauge=dict(
                axis=dict(range=[0, 100], tickwidth=1, tickcolor=BORDER),
                bar=dict(color=gauge_color),
                bgcolor="#F0F0F0",
                borderwidth=0,
                steps=[
                    dict(range=[0, 80], color="#FDE8E8"),
                    dict(range=[80, 95], color="#FEF3C7"),
                    dict(range=[95, 100], color="#D1FAE5"),
                ],
                threshold=dict(line=dict(color=RED, width=3), thickness=0.8, value=match_rate),
            ),
        ))
        fig_gauge.update_layout(
            **_PLOTLY_LAYOUT,
            height=420,
            margin=dict(l=40, r=40, t=80, b=30),
        )
        st.plotly_chart(fig_gauge, use_container_width=True, config={"displayModeBar": False})

    with waterfall_col:
        wf_labels = ["Sales Amount", "Commission", "Transaction", "Service", "Shipping", "Outstanding", "Payment Received"]
        wf_values = [total_sales, total_commission, total_transaction, total_service, total_shipping, -total_outstanding, 0]
        wf_measures = ["absolute", "relative", "relative", "relative", "relative", "relative", "total"]
        payment_received = total_sales + total_commission + total_transaction + total_service + total_shipping - total_outstanding
        wf_text = [
            f"RM {total_sales:,.0f}",
            f"RM {total_commission:,.0f}",
            f"RM {total_transaction:,.0f}",
            f"RM {total_service:,.0f}",
            f"RM {total_shipping:,.0f}",
            f"RM {-total_outstanding:,.0f}",
            f"RM {payment_received:,.0f}",
        ]

        fig_wf = go.Figure(go.Waterfall(
            x=wf_labels,
            y=wf_values,
            measure=wf_measures,
            connector=dict(line=dict(color="#E0E0E0", width=1)),
            increasing=dict(marker=dict(color="#00A3B2")),
            decreasing=dict(marker=dict(color=RED)),
            totals=dict(marker=dict(color="#006770")),
            text=wf_text,
            textposition="outside",
            textfont=dict(size=11, family="Inter, sans-serif"),
            hovertemplate="<b>%{x}</b><br>%{text}<extra></extra>",
        ))
        fig_wf.update_layout(
            **_PLOTLY_LAYOUT,
            height=420,
            title=dict(text="Sales → Deductions → Payment Flow", font=dict(size=16)),
            yaxis=dict(showgrid=True, gridcolor="#F0F0F0", title="Amount (RM)"),
            xaxis=dict(showgrid=False, tickangle=-30),
            margin=dict(l=60, r=20, t=50, b=80),
        )
        st.plotly_chart(fig_wf, use_container_width=True, config={"displayModeBar": False})

# ── View 2: Fees breakdown + Daily payment ───────────────────────────────────
elif chart_view == "💰 Fees & Daily Payments":
    fee_chart_col, daily_chart_col = st.columns(2)

    with fee_chart_col:
        fee_labels = ["Commission", "Transaction", "Service", "Shipping", "AMS Commission", "Return QC"]
        fee_values = [abs(total_commission), abs(total_transaction), abs(total_service), abs(total_shipping), abs(total_ams), abs(total_returnqc)]
        fee_pcts = [v / sum(fee_values) * 100 if sum(fee_values) > 0 else 0 for v in fee_values]
        fee_colors = ["#00A3B2", "#70C9D2", "#006770", PURPLE, PINK, PURPLE_DARK]

        fig_fees = go.Figure(go.Bar(
            y=fee_labels,
            x=fee_values,
            orientation="h",
            marker=dict(color=fee_colors, cornerradius=6),
            text=[f"RM {v:,.2f} ({p:.1f}%)" for v, p in zip(fee_values, fee_pcts)],
            textposition="auto",
            textfont=dict(size=12, family="Inter, sans-serif", color="white"),
            hovertemplate="<b>%{y}</b><br>RM %{x:,.2f}<extra></extra>",
        ))
        fig_fees.update_layout(
            **_PLOTLY_LAYOUT,
            height=420,
            title=dict(text="Fees & Charges Breakdown", font=dict(size=16)),
            xaxis=dict(showgrid=True, gridcolor="#F0F0F0", title="Amount (RM)"),
            yaxis=dict(showgrid=False, autorange="reversed"),
            margin=dict(l=100, r=30, t=50, b=50),
        )
        st.plotly_chart(fig_fees, use_container_width=True, config={"displayModeBar": False})

    with daily_chart_col:
        matched_df = recon_pdf[recon_pdf["Outstanding"].fillna(0) == 0].copy()
        if "Payment Date" in matched_df.columns and not matched_df.empty:
            daily = (
                matched_df
                .groupby("Payment Date", as_index=False)["Payment Amount"]
                .sum()
                .sort_values("Payment Date")
            )
            daily["Cumulative"] = daily["Payment Amount"].cumsum()

            fig_daily = go.Figure()
            fig_daily.add_trace(go.Bar(
                x=daily["Payment Date"],
                y=daily["Payment Amount"],
                name="Daily Amount",
                marker=dict(color="#00A3B2", cornerradius=4),
                hovertemplate="<b>%{x}</b><br>Daily: RM %{y:,.2f}<extra></extra>",
            ))
            fig_daily.add_trace(go.Scatter(
                x=daily["Payment Date"],
                y=daily["Cumulative"],
                name="Cumulative",
                mode="lines+markers",
                line=dict(color="#006770", width=2.5),
                marker=dict(size=5),
                yaxis="y2",
                hovertemplate="<b>%{x}</b><br>Cumulative: RM %{y:,.2f}<extra></extra>",
            ))
            fig_daily.update_layout(
                **_PLOTLY_LAYOUT,
                height=420,
                title=dict(text="Daily Payment Amount (Matched)", font=dict(size=16)),
                yaxis=dict(showgrid=True, gridcolor="#F0F0F0", title="Daily (RM)"),
                yaxis2=dict(title="Cumulative (RM)", overlaying="y", side="right", showgrid=False),
                xaxis=dict(showgrid=False, title="Payment Date", tickangle=-45),
                legend=dict(orientation="h", yanchor="top", y=-0.25, xanchor="center", x=0.5, font=dict(size=11)),
                margin=dict(l=60, r=60, t=50, b=90),
            )
            st.plotly_chart(fig_daily, use_container_width=True, config={"displayModeBar": False})
        else:
            st.markdown(f"""
            <div style="text-align:center; padding:3rem 1rem; color:{TEXT_MUTED};">
                No matched payment data available for daily chart.
            </div>
            """, unsafe_allow_html=True)

# ── View 3: Monthly trend + Outstanding distribution ─────────────────────────
else:
    monthly_col, dist_col = st.columns(2)

    with monthly_col:
        if "Payment Month" in recon_pdf.columns and not recon_pdf.empty:
            monthly = (
                recon_pdf
                .groupby("Payment Month", as_index=False)
                .agg({"Payment Amount": "sum", "SalesCenterAmount": "sum", "Outstanding": "sum"})
                .sort_values("Payment Month")
            )
            fig_monthly = go.Figure()
            fig_monthly.add_trace(go.Bar(
                x=monthly["Payment Month"],
                y=monthly["SalesCenterAmount"],
                name="Sales Amount",
                marker=dict(color="#70C9D2", cornerradius=6),
                hovertemplate="<b>%{x}</b><br>Sales: RM %{y:,.2f}<extra></extra>",
            ))
            fig_monthly.add_trace(go.Bar(
                x=monthly["Payment Month"],
                y=monthly["Payment Amount"],
                name="Payment Received",
                marker=dict(color="#00A3B2", cornerradius=6),
                hovertemplate="<b>%{x}</b><br>Payment: RM %{y:,.2f}<extra></extra>",
            ))
            fig_monthly.add_trace(go.Scatter(
                x=monthly["Payment Month"],
                y=monthly["Outstanding"],
                name="Outstanding",
                mode="lines+markers",
                line=dict(color=RED, width=2.5),
                marker=dict(size=8),
                yaxis="y2",
                hovertemplate="<b>%{x}</b><br>Outstanding: RM %{y:,.2f}<extra></extra>",
            ))
            fig_monthly.update_layout(
                **_PLOTLY_LAYOUT,
                height=450,
                title=dict(text="Monthly Sales vs Payment vs Outstanding", font=dict(size=16)),
                barmode="group",
                yaxis=dict(showgrid=True, gridcolor="#F0F0F0", title="Sales / Payment (RM)"),
                yaxis2=dict(title="Outstanding (RM)", overlaying="y", side="right", showgrid=False,
                            title_font=dict(color=RED), tickfont=dict(color=RED)),
                xaxis=dict(showgrid=False, title="Month"),
                legend=dict(orientation="h", yanchor="top", y=-0.15, xanchor="center", x=0.5, font=dict(size=11)),
                margin=dict(l=60, r=60, t=50, b=80),
            )
            st.plotly_chart(fig_monthly, use_container_width=True, config={"displayModeBar": False})

    with dist_col:
        outstanding_df = result["outstanding"]
        if outstanding_df is not None and not outstanding_df.is_empty():
            out_pdf = outstanding_df.to_pandas()
            if "Outstanding" in out_pdf.columns:
                fig_dist = go.Figure()
                fig_dist.add_trace(go.Histogram(
                    x=out_pdf["Outstanding"],
                    nbinsx=30,
                    name="Orders",
                    marker=dict(color="#00A3B2", line=dict(color="#006770", width=1)),
                    hovertemplate="Range: RM %{x}<br>Count: %{y}<extra></extra>",
                ))
                median_val = out_pdf["Outstanding"].median()
                mean_val = out_pdf["Outstanding"].mean()
                # Separate annotations vertically so they don't overlap
                fig_dist.add_vline(x=mean_val, line_dash="dot", line_color=RED,
                                   annotation_text=f"Mean: RM {mean_val:,.2f}",
                                   annotation_position="top left",
                                   annotation_font=dict(size=11, color=RED),
                                   annotation_yshift=0)
                fig_dist.add_vline(x=median_val, line_dash="dash", line_color=PINK,
                                   annotation_text=f"Median: RM {median_val:,.2f}",
                                   annotation_position="bottom right",
                                   annotation_font=dict(size=11, color=PINK),
                                   annotation_yshift=0)
                fig_dist.update_layout(
                    **_PLOTLY_LAYOUT,
                    height=450,
                    title=dict(text="Outstanding Amount Distribution", font=dict(size=16)),
                    xaxis=dict(showgrid=False, title="Outstanding Amount (RM)"),
                    yaxis=dict(showgrid=True, gridcolor="#F0F0F0", title="Number of Orders"),
                    showlegend=False,
                    margin=dict(l=60, r=30, t=50, b=50),
                )
                st.plotly_chart(fig_dist, use_container_width=True, config={"displayModeBar": False})

st.markdown("---")


# ──────────────────────────────────────────────────────────────────────────────
# Interactive table helper
# ──────────────────────────────────────────────────────────────────────────────

def _render_tab(df_pl, tab_key: str) -> None:
    """Render filter widgets + an interactive dataframe inside the current tab."""
    if df_pl is None or df_pl.is_empty():
        st.markdown(f"""
        <div style="text-align:center; padding:3rem 1rem; color:{TEXT_MUTED};">
            <div style="font-size:2rem; margin-bottom:0.5rem;">—</div>
            <div>No records to display.</div>
        </div>
        """, unsafe_allow_html=True)
        return

    pdf: pd.DataFrame = df_pl.to_pandas()

    # ── Filter row ────────────────────────────────────────────────
    filter_cols = st.columns(4)
    col_idx = 0

    def _next_col():
        nonlocal col_idx
        c = filter_cols[col_idx % 4]
        col_idx += 1
        return c

    month_col = next(
        (c for c in ["Sales Month", "Payment Month", "Payment Mth"] if c in pdf.columns),
        None,
    )
    if month_col:
        unique_months = sorted(pdf[month_col].dropna().unique().tolist())
        sel_months = _next_col().multiselect(
            month_col, unique_months, key=f"{tab_key}_month"
        )
        if sel_months:
            pdf = pdf[pdf[month_col].isin(sel_months)]

    if "Outstanding" in pdf.columns:
        status_opts = ["All", "Outstanding (non-zero)", "Matched (zero)"]
        sel_status = _next_col().selectbox(
            "Outstanding Status", status_opts, key=f"{tab_key}_status"
        )
        if sel_status == "Outstanding (non-zero)":
            pdf = pdf[pdf["Outstanding"].fillna(0) != 0]
        elif sel_status == "Matched (zero)":
            pdf = pdf[pdf["Outstanding"].fillna(0) == 0]

    if "Payment Date" in pdf.columns:
        unique_dates = sorted(pdf["Payment Date"].dropna().unique().tolist())
        sel_dates = _next_col().multiselect(
            "Payment Date", unique_dates, key=f"{tab_key}_date"
        )
        if sel_dates:
            pdf = pdf[pdf["Payment Date"].isin(sel_dates)]

    if "Order ID" in pdf.columns:
        order_q = _next_col().text_input(
            "Search Order ID", key=f"{tab_key}_order"
        )
        if order_q:
            pdf = pdf[
                pdf["Order ID"].astype(str).str.contains(
                    order_q, case=False, na=False, regex=False
                )
            ]

    # ── Row count ─────────────────────────────────────────────────
    st.markdown(f"""
    <div style="display:flex; align-items:center; gap:0.5rem; margin:0.5rem 0;">
        <span style="background:{TEAL}; color:white; padding:0.15rem 0.6rem; border-radius:12px; font-size:0.78rem; font-weight:600;">
            {len(pdf):,}
        </span>
        <span style="color:{TEXT_MUTED}; font-size:0.82rem;">
            of {df_pl.height:,} rows
        </span>
    </div>
    """, unsafe_allow_html=True)

    # ── Interactive table ─────────────────────────────────────────
    st.dataframe(pdf, use_container_width=True, height=520)

    # ── Download ──────────────────────────────────────────────────
    csv_bytes = pdf.to_csv(index=False).encode("utf-8-sig")
    st.download_button(
        label="Download as CSV",
        data=csv_bytes,
        file_name=f"{tab_key}.csv",
        mime="text/csv",
        key=f"{tab_key}_dl",
    )


# ──────────────────────────────────────────────────────────────────────────────
# Tabs
# ──────────────────────────────────────────────────────────────────────────────

tab_recon, tab_out, tab_refund, tab_compare, tab_ib, tab_bi = st.tabs([
    f"Reconciliation  ({s['recon_rows']:,})",
    f"Outstanding  ({s['outstanding_rows']:,})",
    f"Refund  ({s['refund_rows']:,})",
    f"Compare  ({s.get('compare_rows', 0):,})",
    f"Income Not In Balance  ({s['income_not_balance']:,})",
    f"Balance Not In Income  ({s['balance_not_income']:,})",
])

with tab_recon:
    _render_tab(result["recon_report"],       "recon")

with tab_out:
    _render_tab(result["outstanding"],        "out")

with tab_refund:
    _render_tab(result["refund"],             "refund")

with tab_compare:
    _render_tab(result.get("compare"),        "compare")

with tab_ib:
    _render_tab(result["income_not_balance"], "ib")

with tab_bi:
    _render_tab(result["balance_not_income"], "bi")
