"""
ui/app.py
---------
Streamlit demo dashboard for the Shopee Reconciliation pipeline.

Run from the project root:
    streamlit run ui/app.py
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

import pandas as pd
import streamlit as st

# Ensure the project root is on sys.path so `reconciliation` package is importable
# regardless of the working directory used to launch the app.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from reconciliation import run_reconciliation_from_paths  # noqa: E402
from reconciliation.excel_export import export_to_excel   # noqa: E402

# ──────────────────────────────────────────────────────────────────────────────
# Page config
# ──────────────────────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Shopee Reconciliation",
    page_icon="📊",
    layout="wide",
)


# ──────────────────────────────────────────────────────────────────────────────
# Cached pipeline runner
# st.cache_data hashes plain bytes + str tuples efficiently, so the pipeline
# only reruns when the file contents genuinely change.
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
    TOTAL_STEPS = 5  # scan, income, balance, sales, reports
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

        # Excel export step (tracked separately)
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
            label=f"✅ Completed in {total_elapsed:.2f}s",
            state="complete",
        )

    progress_bar.empty()  # remove the bar once done
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
# Sidebar — folder input
# ──────────────────────────────────────────────────────────────────────────────

with st.sidebar:
    st.title("📂 Reconciliation Matching")
    st.caption(
        "Paste the local scenario folder path. The app will scan and load all matching Excel files automatically."
    )
    st.markdown("---")

    folder_path = st.text_input(
        "Scenario Folder Path",
        value=r"c:\Users\TanJunJie\OneDrive - SRKK Group\Project\watson_entriesmatching\OneDrive_2026-03-09\Shopee Sample Reports (Testing)\scenario2",
        help="Example: c:\\...\\Shopee Sample Reports (Testing)\\scenario2",
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
    force_rerun = st.checkbox("Force rerun (skip cache)", value=False)
    run_btn = st.button(
        "▶ Run Reconciliation",
        type="primary",
        use_container_width=True,
        disabled=not (folder_exists and all_found),
    )

    if not folder_exists:
        st.error("Folder path does not exist or is not a directory.")
    else:
        st.write(f"Income files: {len(income_paths)}")
        st.write(f"Balance files: {len(balance_paths)}")
        st.write(f"Sales files: {len(sales_paths)}")
        if master_recon_file is None:
            st.warning("Master Recon file not found. Compare table will be empty.")
        else:
            st.caption(f"Master Recon: {master_recon_file.name}")

    if folder_exists and not all_found:
        missing = [
            name
            for name, group in [
                ("Income", income_paths),
                ("Balance", balance_paths),
                ("Sales", sales_paths),
            ]
            if not group
        ]
        st.warning(f"Missing: {', '.join(missing)}")


# ──────────────────────────────────────────────────────────────────────────────
# Session-state initialisation
# ──────────────────────────────────────────────────────────────────────────────

if "result" not in st.session_state:
    st.session_state["result"] = None


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
    st.success("✅ Reconciliation complete.")


# ──────────────────────────────────────────────────────────────────────────────
# Main page header
# ──────────────────────────────────────────────────────────────────────────────

st.title("📊 Shopee Reconciliation Dashboard")
st.caption(
    "Point the app to a scenario folder in the sidebar, then click **Run Reconciliation**."
)

result = st.session_state["result"]

if result is None:
    st.info(
        "No results yet. Enter a valid scenario folder path in the sidebar "
        "and click **▶ Run Reconciliation** to begin."
    )
    st.stop()


# ──────────────────────────────────────────────────────────────────────────────
# Date range
# ──────────────────────────────────────────────────────────────────────────────

s = result["stats"]
date_from = s.get("date_from", "N/A")
date_to = s.get("date_to", "N/A")

st.subheader("📅 Dataset Date Range")
d1, d2 = st.columns(2)
d1.metric("From", date_from)
d2.metric("To", date_to)

# ──────────────────────────────────────────────────────────────────────────────
# Timing breakdown
# ──────────────────────────────────────────────────────────────────────────────

timings = result.get("timings", [])
total_elapsed = result.get("total_elapsed")

if timings:
    st.subheader("⏱ Timing Breakdown")
    timing_cols = st.columns(len(timings))
    for i, (step_name, duration) in enumerate(timings):
        timing_cols[i].metric(step_name, f"{duration:.2f}s")
    if total_elapsed is not None:
        st.metric("Total", f"{total_elapsed:.2f}s")

# ──────────────────────────────────────────────────────────────────────────────
# Summary metrics
# ──────────────────────────────────────────────────────────────────────────────

st.subheader("📈 Summary")
c1, c2, c3, c4 = st.columns(4)
c1.metric("Income Rows",        f"{s['income_rows']:,}")
c2.metric("Balance Rows",       f"{s['balance_rows']:,}")
c3.metric("Sales Rows",         f"{s['sales_rows']:,}")
c4.metric("Recon Rows",         f"{s['recon_rows']:,}")

c5, c6, c7, c8 = st.columns(4)
c5.metric("Outstanding Orders", f"{s['outstanding_rows']:,}",
          delta=f"−{s['outstanding_rows']}" if s["outstanding_rows"] else None,
          delta_color="inverse")
c6.metric("Refund Orders",      f"{s['refund_rows']:,}")
c7.metric("Compare Rows",       f"{s.get('compare_rows', 0):,}")
c8.metric("Income Not In Balance",   f"{s['income_not_balance']:,}")

c9, _, _, _ = st.columns(4)
c9.metric("Balance Not In Income",   f"{s['balance_not_income']:,}")

# ──────────────────────────────────────────────────────────────────────────────
# Excel download
# ──────────────────────────────────────────────────────────────────────────────

st.markdown("---")
excel_data = result.get("excel_output")
if excel_data is not None:
    st.download_button(
        label="📥 Download Excel Report",
        data=excel_data.getvalue(),
        file_name="reconciliation_output.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        type="primary",
        use_container_width=True,
    )

st.markdown("---")


# ──────────────────────────────────────────────────────────────────────────────
# Interactive table helper
# ──────────────────────────────────────────────────────────────────────────────

def _render_tab(df_pl, tab_key: str) -> None:
    """
    Render filter widgets + an interactive dataframe inside the current tab.

    Filters shown depend on which columns are present in the DataFrame, so the
    same function works for all five result tabs.
    """
    if df_pl is None or df_pl.is_empty():
        st.info("No records to display.")
        return

    # Convert to pandas once; all downstream filtering done in pandas.
    pdf: pd.DataFrame = df_pl.to_pandas()

    # ── Filter row ────────────────────────────────────────────────────────────
    filter_cols = st.columns(4)
    col_idx = 0  # slot counter

    def _next_col():
        nonlocal col_idx
        c = filter_cols[col_idx % 4]
        col_idx += 1
        return c

    # Month filter (Sales Month / Payment Month / Payment Mth)
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

    # Outstanding status filter
    if "Outstanding" in pdf.columns:
        status_opts = ["All", "Outstanding (≠ 0)", "Matched (= 0)"]
        sel_status = _next_col().selectbox(
            "Outstanding Status", status_opts, key=f"{tab_key}_status"
        )
        if sel_status == "Outstanding (≠ 0)":
            pdf = pdf[pdf["Outstanding"].fillna(0) != 0]
        elif sel_status == "Matched (= 0)":
            pdf = pdf[pdf["Outstanding"].fillna(0) == 0]

    # Payment Date filter
    if "Payment Date" in pdf.columns:
        unique_dates = sorted(pdf["Payment Date"].dropna().unique().tolist())
        sel_dates = _next_col().multiselect(
            "Payment Date", unique_dates, key=f"{tab_key}_date"
        )
        if sel_dates:
            pdf = pdf[pdf["Payment Date"].isin(sel_dates)]

    # Order ID free-text search
    if "Order ID" in pdf.columns:
        order_q = _next_col().text_input(
            "🔍 Search Order ID", key=f"{tab_key}_order"
        )
        if order_q:
            pdf = pdf[
                pdf["Order ID"].astype(str).str.contains(
                    order_q, case=False, na=False, regex=False
                )
            ]

    # ── Row count ─────────────────────────────────────────────────────────────
    st.caption(f"Showing **{len(pdf):,}** of **{df_pl.height:,}** rows")

    # ── Interactive table ─────────────────────────────────────────────────────
    # st.dataframe uses virtual scrolling → handles 50k+ rows without issues.
    st.dataframe(
        pdf,
        use_container_width=True,
        height=520,
    )

    # ── Download ──────────────────────────────────────────────────────────────
    csv_bytes = pdf.to_csv(index=False).encode("utf-8-sig")
    st.download_button(
        label="⬇ Download as CSV",
        data=csv_bytes,
        file_name=f"{tab_key}.csv",
        mime="text/csv",
        key=f"{tab_key}_dl",
    )


# ──────────────────────────────────────────────────────────────────────────────
# Tabs
# ──────────────────────────────────────────────────────────────────────────────

tab_recon, tab_out, tab_refund, tab_compare, tab_ib, tab_bi = st.tabs([
    f"📋 Reconciliation ({s['recon_rows']:,})",
    f"⚠️ Outstanding ({s['outstanding_rows']:,})",
    f"↩️ Refund ({s['refund_rows']:,})",
    f"🧾 Compare ({s.get('compare_rows', 0):,})",
    f"🔴 Income Not In Balance ({s['income_not_balance']:,})",
    f"🔵 Balance Not In Income ({s['balance_not_income']:,})",
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
