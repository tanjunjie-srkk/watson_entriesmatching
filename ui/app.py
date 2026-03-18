"""
ui/app.py
---------
Streamlit dashboard for the Shopee Reconciliation pipeline.

Run from the project root:
    streamlit run ui/app.py
"""
from __future__ import annotations

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
    section[data-testid="stSidebar"] .stCheckbox label {{
        color: #C4B5FD !important;
    }}
    section[data-testid="stSidebar"] .stTextInput input {{
        background-color: rgba(191,178,249,0.15) !important;
        border: 1px solid rgba(191,178,249,0.3) !important;
        color: #004D55 !important;
        border-radius: 8px;
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
    div.stRadio > div > label:hover:not(:has(input:checked)) {{
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
