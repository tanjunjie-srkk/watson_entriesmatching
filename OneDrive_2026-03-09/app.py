import streamlit as st
import time
from pathlib import Path
from reconciliation_engine import run_reconciliation

st.set_page_config(page_title="Shopee Reconciliation", page_icon="\U0001F4CA", layout="wide")

st.title("\U0001F4CA Shopee Reconciliation Tool")
st.caption("Run the full reconciliation pipeline and download the formatted Excel report.")
st.markdown("---")

root_path = st.text_input(
    "Scenario Folder Path",
    value=r"c:\Users\TanJunJie\OneDrive - SRKK Group\Project\watson_entriesmatching\OneDrive_2026-03-09\Shopee Sample Reports (Testing)\scenario2",
)

if "result" not in st.session_state:
    st.session_state.result = None
if "total_elapsed" not in st.session_state:
    st.session_state.total_elapsed = None

if st.button("\u25B6 Run Reconciliation", type="primary", use_container_width=True):
    root = Path(root_path)
    if not root.exists() or not root.is_dir():
        st.error("Invalid folder path. Please check and try again.")
    else:
        overall_start = time.time()

        with st.status("Running reconciliation...", expanded=True) as status:
            def on_progress(msg: str):
                st.write(msg)

            result = run_reconciliation(root, progress_callback=on_progress)
            total_elapsed = time.time() - overall_start
            status.update(
                label=f"Completed in {total_elapsed:.2f}s",
                state="complete",
            )

        st.session_state.result = result
        st.session_state.total_elapsed = total_elapsed

result = st.session_state.result
total_elapsed = st.session_state.total_elapsed

if result is not None:
    # ── Date range ────────────────────────────────────────────
    s = result["stats"]
    date_from = s.get("date_from", "N/A")
    date_to = s.get("date_to", "N/A")
    st.subheader("\U0001F4C5 Dataset Date Range")
    d1, d2 = st.columns(2)
    d1.metric("From", date_from)
    d2.metric("To", date_to)

    # ── Timing breakdown ─────────────────────────────────────
    st.subheader("\u23F1 Timing Breakdown")
    timing_cols = st.columns(len(result["timings"]))
    for i, (step_name, duration) in enumerate(result["timings"]):
        timing_cols[i].metric(step_name, f"{duration:.2f}s")
    st.metric("Total", f"{total_elapsed:.2f}s")

    # ── Summary statistics ───────────────────────────────────
    st.subheader("\U0001F4C8 Summary")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Income Rows", s.get("income_rows", 0))
    c2.metric("Balance Rows", s.get("balance_rows", 0))
    c3.metric("Sales Rows", s.get("sales_rows", 0))
    c4.metric("Report Rows", s.get("report_rows", 0))

    c5, c6, c7, c8 = st.columns(4)
    c5.metric("Reconciliation", s.get("recon_rows", 0))
    c6.metric("Outstanding", s.get("outstanding_rows", 0))
    c7.metric("Refund", s.get("refund_rows", 0))
    c8.metric("Income not in Balance", s.get("income_not_balance", 0))

    # ── Download ─────────────────────────────────────────────
    st.markdown("---")
    st.download_button(
        label="\U0001F4E5 Download Excel Report",
        data=result["output"],
        file_name="reconciliation_output.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        type="primary",
        use_container_width=True,
    )
