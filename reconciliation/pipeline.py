"""
reconciliation/pipeline.py
--------------------------
Core Shopee reconciliation logic extracted from the pipeline notebook.

Public API
----------
    result = run_reconciliation(
        income_files  = [(bytes, filename), ...],
        balance_files = [(bytes, filename), ...],
        sales_files   = [(bytes, filename), ...],
    )

    result["recon_report"]       – pl.DataFrame  (main reconciliation table)
    result["outstanding"]        – pl.DataFrame  (Outstanding != 0)
    result["refund"]             – pl.DataFrame  (Refund != 0)
    result["income_not_balance"] – pl.DataFrame  (anti-join)
    result["balance_not_income"] – pl.DataFrame  (anti-join)
    result["stats"]              – dict          (summary counts)
"""
from __future__ import annotations

import re
import time
from pathlib import Path
from typing import Any, Callable

import polars as pl

from reconciliation.io_utils import concat_excel_files, load_files_from_paths, read_excel_bytes


# ──────────────────────────────────────────────────────────────────────────────
# Income normalisation
# ──────────────────────────────────────────────────────────────────────────────

_INCOME_COLS = [
    "Sequence No.", "Order ID", "Order Creation Date", "Payout Completed Date",
    "Total Released Amount (RM)", "Product Price", "Refund Amount",
    "Shipping Fee Paid by Buyer (excl. SST)",
    "Shipping Fee Charged by Logistic Provider",
    "Seller Paid Shipping Fee SST", "Shipping Rebate From Shopee",
    "Reverse Shipping Fee", "Reverse Shipping Fee SST",
    "Saver Programme Shipping Fee Savings", "Return to Seller Fee",
    "Rebate Provided by Shopee", "Voucher Sponsored by Seller",
    "Coin Cashback Sponsored by Seller",
    "Commission Fee (incl. SST)", "Service Fee (Incl. SST)",
    "Transaction Fee (Incl. SST)", "AMS Commission Fee",
    "Saver Programme Fee (Incl. SST)",
    "Username (Buyer)", "Transaction Fee Rate (%)",
    "Buyer Payment Method",
    "Buyer Payment Method Details_1(if applicable)",
    "Payment Details / Installment Plan",
    "Voucher Code From Seller", "Lost Compensation",
]

_INCOME_FLOAT_COLS = [
    "Product Price", "Refund Amount",
    "Shipping Fee Paid by Buyer (excl. SST)",
    "Shipping Fee Charged by Logistic Provider",
    "Seller Paid Shipping Fee SST", "Shipping Rebate From Shopee",
    "Reverse Shipping Fee", "Reverse Shipping Fee SST",
    "Saver Programme Shipping Fee Savings", "Return to Seller Fee",
    "Rebate Provided by Shopee", "Voucher Sponsored by Seller",
    "Coin Cashback Sponsored by Seller",
    "Commission Fee (incl. SST)", "Service Fee (Incl. SST)",
    "Transaction Fee (Incl. SST)", "AMS Commission Fee",
    "Saver Programme Fee (Incl. SST)",
    "Transaction Fee Rate (%)", "Lost Compensation",
    "Total Released Amount (RM)",
]


def _normalize_income(raw: pl.DataFrame) -> pl.DataFrame:
    """De-duplicate on Order ID, filter to 'Order' view, cast columns."""
    raw = raw.unique(subset=["Order ID"], keep="first", maintain_order=True)

    df = (
        raw
        .filter(pl.col("View By") == "Order")
        .select(_INCOME_COLS)
        .with_columns([
            pl.col("Order ID").cast(pl.Utf8).str.strip_chars(),
            pl.col("Order Creation Date")
                .cast(pl.Utf8)
                .str.strptime(pl.Date, "%Y-%m-%d", strict=False),
            pl.col("Payout Completed Date")
                .cast(pl.Utf8)
                .str.strptime(pl.Date, "%Y-%m-%d", strict=False),
            *[pl.col(c).cast(pl.Float64, strict=False) for c in _INCOME_FLOAT_COLS],
        ])
        .drop_nulls(["Order ID"])
    )

    # Derived summary columns
    df = df.with_columns(
        pl.sum_horizontal([
            pl.col("Rebate Provided by Shopee"),
            pl.col("Voucher Sponsored by Seller"),
            pl.col("Coin Cashback Sponsored by Seller"),
        ]).fill_null(0).alias("Net Voucher")
    ).with_columns(
        pl.sum_horizontal([
            pl.col("Shipping Fee Paid by Buyer (excl. SST)"),
            pl.col("Shipping Fee Charged by Logistic Provider"),
            pl.col("Seller Paid Shipping Fee SST"),
            pl.col("Shipping Rebate From Shopee"),
            pl.col("Reverse Shipping Fee"),
            pl.col("Reverse Shipping Fee SST"),
        ]).fill_null(0).alias("Net Shipping Fees")
    )

    return df


# ──────────────────────────────────────────────────────────────────────────────
# Balance normalisation
# ──────────────────────────────────────────────────────────────────────────────

def _normalize_balance(raw: pl.DataFrame) -> pl.DataFrame:
    """De-duplicate on Order ID and parse date/amount columns."""
    raw = raw.unique(subset=["Order ID"], keep="first", maintain_order=True)

    return (
        raw
        .select([
            "Date", "Transaction Type", "Description", "Order ID",
            "Money Direction", "Amount", "Status",
            "Balance After Transactions", "Transaction Report",
        ])
        .with_columns([
            pl.col("Order ID").cast(pl.Utf8).str.strip_chars(),
            pl.col("Date").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S"),
            pl.col("Amount").cast(pl.Float64, strict=False).alias("Payment Amount"),
            pl.col("Balance After Transactions").cast(pl.Float64, strict=False),
        ])
        .with_columns([
            pl.col("Date").dt.strftime("%d-%b-%Y").str.to_uppercase().alias("Payment Date"),
            pl.col("Date").dt.strftime("%b-%Y").str.to_uppercase().alias("Payment Month"),
        ])
    )


# ──────────────────────────────────────────────────────────────────────────────
# Sales normalisation
# ──────────────────────────────────────────────────────────────────────────────

def _normalize_sales(raw: pl.DataFrame) -> pl.DataFrame:
    """Parse work date, create Order ID alias and numeric amount."""
    return (
        raw
        .with_columns([
            # SalesWorkDate may arrive as a Date object or a string like "31-12-2025"
            pl.coalesce([
                pl.col("SalesWorkDate").cast(pl.Date, strict=False),
                pl.col("SalesWorkDate")
                    .cast(pl.Utf8, strict=False)
                    .str.strip_chars()
                    .str.strptime(pl.Date, "%d-%m-%Y", strict=False),
            ]).alias("WorkDate"),
            pl.col("MarketPlaceOrderNum")
                .cast(pl.Utf8)
                .str.strip_chars()
                .alias("Order ID"),
            pl.col("TotalAmount").cast(pl.Float64, strict=False).alias("SalesCenterAmount"),
        ])
        .with_columns([
            pl.col("WorkDate")
                .dt.strftime("%b-%Y")
                .str.to_uppercase()
                .alias("Sales Month"),
        ])
    )


# ──────────────────────────────────────────────────────────────────────────────
# Reconciliation assembly
# ──────────────────────────────────────────────────────────────────────────────

def _build_recon_report(
    income: pl.DataFrame,
    balance: pl.DataFrame,
    sales: pl.DataFrame,
) -> pl.DataFrame:
    """Join sales + balance + income and compute the Outstanding column."""

    first_part = sales.select(
        ["Order ID", "OrderNum", "MarketPlaceOrderNum",
         "SalesWorkDate", "SalesCenterAmount", "Sales Month"]
    )

    payment_part = balance.select(
        ["Order ID", "Payment Date", "Payment Month", "Payment Amount"]
    )

    third_part = income.select([
        "Order ID",
        pl.col("Commission Fee (incl. SST)").alias("Commission Fee"),
        pl.col("Transaction Fee (Incl. SST)").alias("Transaction Fee"),
        pl.col("Service Fee (Incl. SST)").alias("Service Fee"),
        pl.col("AMS Commission Fee"),
        pl.col("Return to Seller Fee").alias("Return QC Fee"),
        pl.col("Voucher Sponsored by Seller").alias("Voucher/(Disc rebate)"),
        pl.col("Refund Amount").alias("Refund"),
        (
            pl.col("Shipping Fee Paid by Buyer (excl. SST)")
            + pl.col("Shipping Fee Charged by Logistic Provider")
            + pl.col("Seller Paid Shipping Fee SST")
            + pl.col("Shipping Rebate From Shopee")
            + pl.col("Reverse Shipping Fee")
            + pl.col("Reverse Shipping Fee SST")
        ).alias("Actual Shipping Fee"),
    ])

    recon = (
        first_part
        .join(payment_part, on="Order ID", how="inner")
        .join(third_part,   on="Order ID", how="inner")
    )

    # Outstanding = Sales amount minus payment received plus all fees/charges.
    # fill_null(0) ensures null columns (missing join matches) don't null the result.
    # round(2) + negative-zero normalisation removes IEEE 754 -0.0 artefacts.
    _outstanding = (
        pl.col("SalesCenterAmount").fill_null(0)
        - pl.col("Payment Amount").fill_null(0)
        + pl.col("Commission Fee").fill_null(0)
        + pl.col("Transaction Fee").fill_null(0)
        + pl.col("Service Fee").fill_null(0)
        + pl.col("AMS Commission Fee").fill_null(0)
        + pl.col("Return QC Fee").fill_null(0)
        # + pl.col("Voucher/(Disc rebate)").fill_null(0)
        + pl.col("Actual Shipping Fee").fill_null(0)
    ).round(2)

    return recon.with_columns(
        pl.when(_outstanding == 0)
          .then(pl.lit(0.0))
          .otherwise(_outstanding)
          .alias("Outstanding")
    )


def _normalize_original_recon(ori_recon: pl.DataFrame) -> pl.DataFrame:
    """Normalize key columns from the original Shopee master Recon sheet."""
    ori_recon = ori_recon.select(pl.col("MarketPlaceOrderNum", "OrderNum", "WorkDate", "SalesCenterAmount", "Pymt Amt", "Pymt Date", "Commission Fee", "Transaction Fee", "Service Fee", "AMS Commission Fee", "Return QC Fee", "Voucher/(Disc rebate)", "Refund", "Actual Shipping Fee", "Outstanding"))

    ori_renamed = ori_recon.with_columns(
        pl.col("MarketPlaceOrderNum").str.strip_chars().alias("Order ID Ori"),
        pl.col("OrderNum").str.strip_chars().alias("OrderNum Ori"),
        pl.col("MarketPlaceOrderNum").str.strip_chars().alias("MarketPlaceOrderNum Ori"),
        pl.col("WorkDate").str.strptime(pl.Date, '%Y-%m-%d', strict=False).alias("SalesWorkDate Ori"),
        pl.col("SalesCenterAmount").cast(pl.Float64, strict=False).alias("SalesCenterAmount Ori"),
        pl.col("Pymt Amt").cast(pl.Float64, strict=False).alias("Payment Amount Ori"),
        pl.col("Pymt Date").dt.strftime("%b-%Y").str.to_uppercase().alias("Payment Month Ori"),
        pl.col("Commission Fee").cast(pl.Float64, strict=False).alias("Commission Fee Ori"),
        pl.col("Transaction Fee").cast(pl.Float64, strict=False).alias("Transaction Fee Ori"),
        pl.col("Service Fee").cast(pl.Float64, strict=False).alias("Service Fee Ori"),
        pl.col("AMS Commission Fee").cast(pl.Float64, strict=False).alias("AMS Commission Fee Ori"),
        pl.col("Return QC Fee").cast(pl.Float64, strict=False).alias("Return QC Fee Ori"),
        pl.col("Voucher/(Disc rebate)").cast(pl.Float64, strict=False).alias("Voucher/(Disc rebate) Ori"), 
        pl.col("Refund").cast(pl.Float64, strict=False).alias("Refund Ori"),
        pl.col("Actual Shipping Fee").cast(pl.Float64, strict=False).alias("Actual Shipping Fee Ori"),
        pl.col("Outstanding").cast(pl.Float64, strict=False).alias("Outstanding Ori"), 
        )
    
    ori_renamed = ori_renamed.drop([
            "MarketPlaceOrderNum",
            "OrderNum",
            "WorkDate",
            "SalesCenterAmount",
            "Pymt Amt",
            "Pymt Date",
            "Commission Fee",
            "Transaction Fee",
            "Service Fee",
            "AMS Commission Fee",
            "Return QC Fee",
            "Voucher/(Disc rebate)",
            "Refund",
            "Actual Shipping Fee",
            "Outstanding",
        ])
        
    return ori_renamed

def _build_compare_table(recon_report: pl.DataFrame, ori_recon: pl.DataFrame) -> pl.DataFrame:
    """Build side-by-side compare table between generated and original Recon columns."""
    cols = [
    "Order ID",
    "OrderNum",
    "MarketPlaceOrderNum",
    "SalesWorkDate",
    "SalesCenterAmount",
    "Payment Amount",
    "Payment Month",
    "Commission Fee",
    "Transaction Fee",
    "Service Fee",
    "AMS Commission Fee",
    "Return QC Fee",
    "Voucher/(Disc rebate)",
    "Refund",
    "Actual Shipping Fee",
    "Outstanding"
    ]

    # rename columns
    recon_renamed = recon_report.rename({c: f"{c} Recon" for c in cols})
    #ori_renamed = ori_recon.rename({c: f"{c} Ori" for c in cols})

    # join using Order ID
    compare = recon_renamed.join(
        ori_recon,
        left_on="Order ID Recon",
        right_on="Order ID Ori",
        how="inner"
    )

    cols = [
    "OrderNum",
    "MarketPlaceOrderNum",
    "SalesWorkDate",
    "SalesCenterAmount",
    "Payment Amount",
    "Payment Month",
    "Commission Fee",
    "Transaction Fee",
    "Service Fee",
    "AMS Commission Fee",
    "Return QC Fee",
    "Voucher/(Disc rebate)",
    "Refund",
    "Actual Shipping Fee",
    "Outstanding"
    ]
    ordered_cols = []
    for c in cols:
        ordered_cols.append(f"{c} Recon")
        ordered_cols.append(f"{c} Ori")

    compare = compare.select(ordered_cols)
        
    
    return compare


# ──────────────────────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────────────────────

def run_reconciliation(
    income_files:  list[tuple[bytes, str]],
    balance_files: list[tuple[bytes, str]],
    sales_files:   list[tuple[bytes, str]],
) -> dict[str, Any]:
    """
    Run the full Shopee reconciliation pipeline.

    Parameters
    ----------
    income_files, balance_files, sales_files
        Each is a list of ``(raw_bytes, filename)`` tuples for that category.
        Obtain ``raw_bytes`` via ``Path.read_bytes()`` or ``UploadedFile.read()``.

    Returns
    -------
    dict with keys:

    ==================  =================================================
    ``recon_report``    Main reconciliation DataFrame
    ``outstanding``     Rows where Outstanding ≠ 0
    ``refund``          Rows where Refund ≠ 0
    ``income_not_balance`` Income orders absent from balance
    ``balance_not_income`` Balance orders absent from income
    ``stats``           Summary counts dict
    ==================  =================================================
    """
    # ── Load raw files ────────────────────────────────────────────────────────
    raw_income = concat_excel_files(
        income_files,
        label="income_all",
        sheet_pattern="Income",
        has_header=False,
    )
    raw_balance = concat_excel_files(
        balance_files,
        label="balance_all",
        sheet_pattern="Transaction",
        has_header=False,
    )

    # Sales files use a standard header on sheet "SalesReport"
    sales_dfs: list[pl.DataFrame] = []
    for data, filename in sales_files:
        df = read_excel_bytes(
            data,
            filename,
            sheet_pattern="SalesReport",
            has_header=True,
        )
        sales_dfs.append(df)
    raw_sales = (
        pl.concat(sales_dfs, how="diagonal_relaxed") if sales_dfs else pl.DataFrame()
    )

    # ── Normalise ─────────────────────────────────────────────────────────────
    income  = _normalize_income(raw_income)
    balance = _normalize_balance(raw_balance)
    sales   = _normalize_sales(raw_sales)

    # ── Discrepancy sets ──────────────────────────────────────────────────────
    income_not_balance = income.join(balance, on="Order ID", how="anti")
    balance_not_income = balance.join(income, on="Order ID", how="anti")

    # ── Reconciliation report ─────────────────────────────────────────────────
    recon_report = _build_recon_report(income, balance, sales)

    outstanding_df = recon_report.filter((pl.col("Outstanding") != 0))

    #Find Refund and Outstanding with Refund
    Refund = income.filter(pl.col("Refund Amount").fill_null(0) != 0)
    
    refund_df     = outstanding_df.join(Refund.select("Order ID"), on="Order ID", how="inner")
    
    stats: dict[str, int] = {
        "income_rows":        income.height,
        "balance_rows":       balance.height,
        "sales_rows":         sales.height,
        "recon_rows":         recon_report.height,
        "outstanding_rows":   outstanding_df.height,
        "refund_rows":        refund_df.height,
        "income_not_balance": income_not_balance.height,
        "balance_not_income": balance_not_income.height,
    }

    return {
        "recon_report":        recon_report,
        "outstanding":         outstanding_df,
        "refund":              refund_df,
        "income_not_balance":  income_not_balance,
        "balance_not_income":  balance_not_income,
        "stats":               stats,
    }


def run_reconciliation_from_paths(
    income_paths: list[str | Path],
    balance_paths: list[str | Path],
    sales_paths: list[str | Path],
    master_recon_path: str | Path | None = None,
    progress_callback: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    """Path-based wrapper used by the Streamlit folder workflow.

    Accepts an optional *progress_callback* that receives status messages
    (e.g. "Loading income files…") and returns a dict that now also contains
    ``timings`` (list of (step_name, seconds) tuples) for the UI to display.
    """
    timings: list[tuple[str, float]] = []

    def notify(msg: str):
        if progress_callback:
            progress_callback(msg)

    income_path_objs = [Path(p) for p in income_paths]
    balance_path_objs = [Path(p) for p in balance_paths]
    sales_path_objs = [Path(p) for p in sales_paths]

    # ── 1. Scan & extract date range ─────────────────────────────
    t0 = time.time()
    notify("📂 Scanning folder structure…")
    stats: dict[str, Any] = {}
    # Gather all parent directories to extract date ranges from subfolder names
    parent_dirs: set[Path] = set()
    for p in income_path_objs + balance_path_objs + sales_path_objs:
        parent_dirs.add(p.parent)
    all_dates: list[str] = []
    for d in parent_dirs:
        m = re.search(r"(\d{8})_(\d{8})$", d.name)
        if m:
            all_dates.extend([m.group(1), m.group(2)])
    if all_dates:
        import datetime as _dt
        parsed = sorted(_dt.datetime.strptime(d, "%Y%m%d").date() for d in all_dates)
        stats["date_from"] = parsed[0].strftime("%d %b %Y")
        stats["date_to"] = parsed[-1].strftime("%d %b %Y")
    timings.append(("Scan files", time.time() - t0))

    # ── 2. Load income ───────────────────────────────────────────
    t0 = time.time()
    notify("📄 Loading income files…")
    raw_income = load_files_from_paths(
        income_path_objs,
        label="income_all",
        sheet_pattern="Income",
        has_header=False,
    )
    income = _normalize_income(raw_income)
    timings.append(("Load income", time.time() - t0))

    # ── 3. Load balance ──────────────────────────────────────────
    t0 = time.time()
    notify("📄 Loading balance files…")
    raw_balance = load_files_from_paths(
        balance_path_objs,
        label="balance_all",
        sheet_pattern="Transaction",
        has_header=False,
    )
    balance = _normalize_balance(raw_balance)
    timings.append(("Load balance", time.time() - t0))

    # ── 4. Load sales ────────────────────────────────────────────
    t0 = time.time()
    notify("📄 Loading sales files…")
    sales_dfs: list[pl.DataFrame] = []
    for path in sales_path_objs:
        df = pl.read_excel(str(path), sheet_name="SalesReport", has_header=True)
        df = df.with_columns(pl.lit(path.name).alias("_source_file"))
        sales_dfs.append(df)
    raw_sales = (
        pl.concat(sales_dfs, how="diagonal_relaxed") if sales_dfs else pl.DataFrame()
    )
    sales = _normalize_sales(raw_sales)
    timings.append(("Load sales", time.time() - t0))

    # ── 5. Generate reports ──────────────────────────────────────
    t0 = time.time()
    notify("🔍 Generating reconciliation report…")

    income_not_balance = income.join(balance, on="Order ID", how="anti")
    balance_not_income = balance.join(income, on="Order ID", how="anti")

    # "Report" sheet: income INNER JOIN balance (with Payment Date / Month)
    balance_report = balance.select(["Date", "Order ID"]).with_columns(
        pl.col("Date").dt.date().alias("Payment Date"),
    ).with_columns(
        pl.col("Payment Date").dt.strftime("%d-%b-%Y").str.to_uppercase().alias("Payment Date"),
        pl.col("Order ID").str.strip_chars(),
    ).with_columns(
        pl.col("Date").dt.strftime("%b-%Y").str.to_uppercase().alias("Payment Mth"),
    )
    report = income.join(balance_report, on="Order ID", how="inner")

    recon_report = _build_recon_report(income, balance, sales)
    outstanding_df = recon_report.filter(pl.col("Outstanding") != 0)
    refund_df = recon_report.filter(pl.col("Refund").fill_null(0) != 0)
    outstanding_with_refund = outstanding_df.join(refund_df.select("Order ID"), on="Order ID", how="inner")

    compare = pl.DataFrame()
    if master_recon_path is not None:
        master_path_obj = Path(master_recon_path)
        if master_path_obj.exists():
            ori_recon = pl.read_excel(str(master_path_obj), sheet_name="Recon", has_header=True)
            ori_recon = _normalize_original_recon(ori_recon)
            compare = _build_compare_table(recon_report, ori_recon)

    timings.append(("Generate reports", time.time() - t0))

    stats.update({
        "income_rows": income.height,
        "balance_rows": balance.height,
        "sales_rows": sales.height,
        "report_rows": report.height,
        "recon_rows": recon_report.height,
        "outstanding_rows": outstanding_df.height,
        "refund_rows": refund_df.height,
        "compare_rows": compare.height,
        "income_not_balance": income_not_balance.height,
        "balance_not_income": balance_not_income.height,
        "outstanding_refund_rows": outstanding_with_refund.height,
    })

    return {
        "report": report,
        "recon_report": recon_report,
        "outstanding": outstanding_df,
        "refund": refund_df,
        "compare": compare,
        "outstanding_with_refund": outstanding_with_refund,
        "income_not_balance": income_not_balance,
        "balance_not_income": balance_not_income,
        "stats": stats,
        "timings": timings,
    }
