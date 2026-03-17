import polars as pl
from pathlib import Path
import fastexcel
import xlsxwriter
import datetime
import io
import time
from typing import Callable


def _read_excel(path: Path, sheet_pattern: str | None = None, had_header: bool = True) -> pl.DataFrame:
    reader = fastexcel.read_excel(path)
    sheets = [s for s in reader.sheet_names
              if sheet_pattern is None or s.startswith(sheet_pattern)]
    dfs = []
    for name in sheets:
        df = pl.read_excel(path, sheet_name=name, has_header=had_header)
        df = df.with_columns(pl.lit(name).alias("_source_sheet"))
        dfs.append(df)
    if not dfs:
        return pl.DataFrame()
    combined = pl.concat(dfs, how="diagonal_relaxed") if len(dfs) > 1 else dfs[0]
    return combined.with_columns(pl.lit(path.name).alias("_source_file"))


def _concat_files(files: list[Path], label: str, sheet_pattern: str | None = None, has_header: bool = True) -> pl.DataFrame:
    if not files:
        return pl.DataFrame()
    dfs = []
    for f in files:
        df = _read_excel(f, sheet_pattern=sheet_pattern, had_header=has_header)
        if label == "income_all" and not has_header:
            header = df.row(2)
            df = df.slice(3)
            df.columns = header
            dfs.append(df)
        elif label == "balance_all" and not has_header:
            header = df.row(13)
            df = df.slice(14)
            df.columns = header
            dfs.append(df)
    return pl.concat(dfs, how="diagonal_relaxed")


def _export_to_excel(dfs: dict) -> io.BytesIO:
    output = io.BytesIO()
    wb = xlsxwriter.Workbook(output, {"in_memory": True})

    # ── Color palette ────────────────────────────────────────────
    PAL = {
        "navy":    {"dark": "#1F3864", "med": "#2E75B6", "light": "#B4C6E7"},
        "teal":    {"dark": "#1D6D37", "med": "#2E8B57", "light": "#A3CFBB"},
        "amber":   {"dark": "#7D5A00", "med": "#BF8F00", "light": "#FFE699"},
        "crimson": {"dark": "#833C0B", "med": "#C0504D", "light": "#F8CBAD"},
        "purple":  {"dark": "#4A1A6B", "med": "#7B4F9E", "light": "#D9C4EC"},
        "slate":   {"dark": "#2D3436", "med": "#636E72", "light": "#DFE6E9"},
    }
    BORDER_CLR = "#C0C0C0"

    # ── Format factories ─────────────────────────────────────────
    def _hdr(bg, fc="white", sz=10, bold=True):
        return wb.add_format({
            "bold": bold, "font_color": fc, "bg_color": bg,
            "border": 1, "border_color": BORDER_CLR,
            "align": "center", "valign": "vcenter",
            "font_size": sz, "text_wrap": True, "font_name": "Calibri",
        })

    def _data(bg="#FFFFFF", num_fmt=None, align="left", fc="#1A1A1A"):
        p = {
            "border": 1, "border_color": BORDER_CLR,
            "font_size": 10, "valign": "vcenter", "align": align,
            "font_color": fc, "bg_color": bg, "font_name": "Calibri",
        }
        if num_fmt:
            p["num_format"] = num_fmt
        return wb.add_format(p)

    # ── Report L1 banners ────────────────────────────────────────
    L1_FMT = {
        "Order Info":              _hdr(PAL["navy"]["dark"], sz=11),
        "Released Amount Details": _hdr(PAL["teal"]["dark"], sz=11),
        "Vouchers and Rebates":    _hdr(PAL["amber"]["dark"], sz=11),
        "Fees and Charges":        _hdr(PAL["crimson"]["dark"], sz=11),
        "Buyer Info":              _hdr(PAL["purple"]["dark"], sz=11),
        "Reference Info":          _hdr(PAL["slate"]["dark"], sz=11),
    }
    L1_DEFAULT = _hdr(PAL["slate"]["dark"], sz=11)
    L1_BLANK   = wb.add_format({"bg_color": "#FFFFFF", "bottom": 1, "bottom_color": BORDER_CLR})

    # ── Report L2 sub-group banners ──────────────────────────────
    L2_GROUP_FMT = {
        "Released Amount Details": _hdr(PAL["teal"]["med"]),
        "Reference Info":          _hdr(PAL["slate"]["med"]),
    }
    L2_BLANK = wb.add_format({"bg_color": "#FFFFFF", "bottom": 1, "bottom_color": BORDER_CLR})

    # ── Report L3 column headers ─────────────────────────────────
    L3_FMT = {
        "Order Info":              _hdr(PAL["navy"]["light"],    fc="#1A1A1A"),
        "Released Amount Details": _hdr(PAL["teal"]["light"],    fc="#1A1A1A"),
        "Vouchers and Rebates":    _hdr(PAL["amber"]["light"],   fc="#1A1A1A"),
        "Fees and Charges":        _hdr(PAL["crimson"]["light"], fc="#1A1A1A"),
        "Buyer Info":              _hdr(PAL["purple"]["light"],  fc="#1A1A1A"),
        "Reference Info":          _hdr(PAL["slate"]["light"],   fc="#1A1A1A"),
    }
    L3_DEFAULT = _hdr("#E8E8E8", fc="#1A1A1A")

    # ── Reconciliation group banners ─────────────────────────────
    RECON_L1 = {
        "Order Info":        _hdr(PAL["navy"]["dark"],    sz=11),
        "Sales Center":      _hdr(PAL["teal"]["dark"],    sz=11),
        "Payment Info":      _hdr(PAL["amber"]["dark"],   sz=11),
        "Fees & Deductions": _hdr(PAL["crimson"]["dark"], sz=11),
        "Result":            _hdr(PAL["purple"]["dark"],  sz=11),
    }
    RECON_COL = {
        "Order Info":        _hdr(PAL["navy"]["light"],    fc="#1A1A1A"),
        "Sales Center":      _hdr(PAL["teal"]["light"],    fc="#1A1A1A"),
        "Payment Info":      _hdr(PAL["amber"]["light"],   fc="#1A1A1A"),
        "Fees & Deductions": _hdr(PAL["crimson"]["light"], fc="#1A1A1A"),
        "Result":            _hdr(PAL["purple"]["light"],  fc="#1A1A1A"),
    }

    fmt_generic_hdr = _hdr(PAL["navy"]["dark"])

    # ── Zebra striping ───────────────────────────────────────────
    def _make_row_fmts(bg):
        return {
            "text":   _data(bg),
            "num":    _data(bg, num_fmt='#,##0.00;[Red](#,##0.00);"-"', align="right"),
            "date":   _data(bg, num_fmt="DD-MMM-YY", align="center"),
            "month":  _data(bg, align="center"),
            "center": _data(bg, align="center"),
        }

    ROW_W = _make_row_fmts("#FFFFFF")
    ROW_G = _make_row_fmts("#F5F7FA")

    MONEY_COLS = {
        "SalesCenterAmount", "Payment Amount", "Total Released Amount (RM)",
        "Commission Fee", "Transaction Fee", "Service Fee",
        "Return QC Fee", "Voucher/(Disc rebate)", "Refund",
        "Actual Shipping Fee", "Outstanding",
        "Product Price", "Refund Amount", "Net Voucher", "Net Shipping Fees",
        "Shipping Fee Paid by Buyer (excl. SST)", "Shipping Fee Charged by Logistic Provider",
        "Seller Paid Shipping Fee SST", "Shipping Rebate From Shopee",
        "Reverse Shipping Fee", "Reverse Shipping Fee SST",
        "Saver Programme Shipping Fee Savings", "Return to Seller Fee",
        "Rebate Provided by Shopee", "Voucher Sponsored by Seller",
        "Coin Cashback Sponsored by Seller", "Commission Fee (incl. SST)",
        "Service Fee (Incl. SST)", "Transaction Fee (Incl. SST)",
        "AMS Commission Fee", "Saver Programme Fee (Incl. SST)",
        "Transaction Fee Rate (%)", "Lost Compensation", "Amount",
        "Balance After Transactions",
    }
    DATE_COLS  = {"SalesWorkDate", "WorkDate", "Order Creation Date", "Payout Completed Date", "Date"}
    MONTH_COLS = {"Sales Month", "Payment Month", "Payment Mth"}

    def _fmt_key(c):
        if c in MONEY_COLS:  return "num"
        if c in DATE_COLS:   return "date"
        if c in MONTH_COLS:  return "month"
        if c == "Payment Date": return "center"
        return "text"

    def data_fmt(col, ri):
        return (ROW_W if ri % 2 == 0 else ROW_G)[_fmt_key(col)]

    def write_val(ws, r, c, v, fmt):
        if v is None:
            ws.write_blank(r, c, None, fmt)
        elif isinstance(v, (datetime.date, datetime.datetime)):
            ws.write_datetime(r, c, v, fmt)
        elif isinstance(v, (int, float)):
            ws.write_number(r, c, v, fmt)
        else:
            ws.write_string(r, c, str(v), fmt)

    # ═════════════════════════════════════════════════════════════
    # REPORT SHEET — 3-level grouped headers
    # ═════════════════════════════════════════════════════════════
    def write_report_sheet(name, df):
        ws = wb.add_worksheet(name)
        ws.hide_gridlines(2)

        LAYOUT = [
            ("Order Info", "", [
                "Sequence No.", "Order ID", "Order Creation Date", "Payout Completed Date"]),
            ("Released Amount Details", "Order Income", [
                "Total Released Amount (RM)"]),
            ("Released Amount Details", "Merchandise", [
                "Product Price", "Refund Amount"]),
            ("Released Amount Details", "Shipping", [
                "Shipping Fee Paid by Buyer (excl. SST)",
                "Shipping Fee Charged by Logistic Provider",
                "Seller Paid Shipping Fee SST",
                "Shipping Rebate From Shopee",
                "Reverse Shipping Fee",
                "Reverse Shipping Fee SST",
                "Saver Programme Shipping Fee Savings",
                "Return to Seller Fee"]),
            ("Vouchers and Rebates", "", [
                "Rebate Provided by Shopee",
                "Voucher Sponsored by Seller",
                "Coin Cashback Sponsored by Seller"]),
            ("Fees and Charges", "", [
                "Commission Fee (incl. SST)",
                "Service Fee (Incl. SST)",
                "Transaction Fee (Incl. SST)",
                "AMS Commission Fee",
                "Saver Programme Fee (Incl. SST)"]),
            ("Buyer Info", "", [
                "Username (Buyer)",
                "Transaction Fee Rate (%)",
                "Buyer Payment Method",
                "Buyer Payment Method Details_1(if applicable)",
                "Payment Details / Installment Plan"]),
            ("Reference Info", "Promotion", [
                "Voucher Code From Seller"]),
            ("Reference Info", "Compensation", [
                "Lost Compensation"]),
            ("", "", [
                "Date", "Payment Date", "Payment Mth",
                "Net Voucher", "Net Shipping Fees"]),
        ]

        flat = []
        for l1, l2, cols in LAYOUT:
            for c in cols:
                if c in df.columns:
                    flat.append((l1, l2, c))
        used = {f[2] for f in flat}
        for c in df.columns:
            if c not in used and not c.startswith("_source"):
                flat.append(("", "", c))

        col_order = [f[2] for f in flat]
        N = len(col_order)

        # Row 0: L1 banner
        i = 0
        while i < N:
            l1 = flat[i][0]
            j = i + 1
            while j < N and flat[j][0] == l1:
                j += 1
            if l1:
                fmt = L1_FMT.get(l1, L1_DEFAULT)
                if j - i == 1:
                    ws.write(0, i, l1, fmt)
                else:
                    ws.merge_range(0, i, 0, j - 1, l1, fmt)
            else:
                for k in range(i, j):
                    ws.write_blank(0, k, None, L1_BLANK)
            i = j

        # Row 1: L2 sub-group
        i = 0
        while i < N:
            l1, l2 = flat[i][0], flat[i][1]
            j = i + 1
            while j < N and flat[j][0] == l1 and flat[j][1] == l2:
                j += 1
            if l2:
                l2f = L2_GROUP_FMT.get(l1, _hdr(PAL["slate"]["med"]))
                if j - i == 1:
                    ws.write(1, i, l2, l2f)
                else:
                    ws.merge_range(1, i, 1, j - 1, l2, l2f)
            else:
                for k in range(i, j):
                    ws.write_blank(1, k, None, L2_BLANK)
            i = j

        # Row 2: L3 column names
        for i, (l1, _, cn) in enumerate(flat):
            ws.write(2, i, cn, L3_FMT.get(l1, L3_DEFAULT))

        # Row 3+: Data
        for ri, row in enumerate(df.select(col_order).iter_rows()):
            for ci, v in enumerate(row):
                write_val(ws, ri + 3, ci, v, data_fmt(col_order[ci], ri))

        ws.autofilter(2, 0, df.height + 2, N - 1)
        for i, cn in enumerate(col_order):
            ws.set_column(i, i, max(len(cn) + 2, 15))
        ws.freeze_panes(3, 0)
        ws.set_row(0, 26)
        ws.set_row(1, 22)
        ws.set_row(2, 38)

    # ═════════════════════════════════════════════════════════════
    # RECONCILIATION SHEET — 2-level, distinct group colors
    # ═════════════════════════════════════════════════════════════
    def write_recon_sheet(name, df):
        ws = wb.add_worksheet(name)
        ws.hide_gridlines(2)

        groups = [
            ("Order Info",        ["Order ID", "OrderNum", "MarketPlaceOrderNum"]),
            ("Sales Center",      ["SalesWorkDate", "SalesCenterAmount", "Sales Month"]),
            ("Payment Info",      ["Payment Date", "Payment Month", "Payment Amount"]),
            ("Fees & Deductions", ["Commission Fee", "Transaction Fee", "Service Fee",
                                   "AMS Commission Fee", "Return QC Fee",
                                   "Voucher/(Disc rebate)", "Refund",
                                   "Actual Shipping Fee"]),
            ("Result",            ["Outstanding"]),
        ]

        col_info = []
        for gn, cols in groups:
            for c in cols:
                if c in df.columns:
                    col_info.append((gn, c))
        col_order = [ci[1] for ci in col_info]
        N = len(col_order)

        # Row 0: Group banners
        idx = 0
        for gn, cols in groups:
            present = [c for c in cols if c in df.columns]
            if not present:
                continue
            gf = RECON_L1.get(gn, _hdr(PAL["slate"]["dark"], sz=11))
            if len(present) == 1:
                ws.write(0, idx, gn, gf)
            else:
                ws.merge_range(0, idx, 0, idx + len(present) - 1, gn, gf)
            idx += len(present)

        # Row 1: Column headers
        for i, (gn, cn) in enumerate(col_info):
            ws.write(1, i, cn, RECON_COL.get(gn, _hdr("#E8E8E8", fc="#1A1A1A")))

        # Row 2+: Data
        for ri, row in enumerate(df.select(col_order).iter_rows()):
            for ci, v in enumerate(row):
                write_val(ws, ri + 2, ci, v, data_fmt(col_order[ci], ri))

        ws.autofilter(1, 0, df.height + 1, N - 1)
        for i, cn in enumerate(col_order):
            ws.set_column(i, i, max(len(cn) + 2, 15))
        ws.freeze_panes(2, 0)
        ws.set_row(0, 28)
        ws.set_row(1, 36)

    # ═════════════════════════════════════════════════════════════
    # GENERIC STYLED SHEET
    # ═════════════════════════════════════════════════════════════
    def write_styled_sheet(name, df):
        ws = wb.add_worksheet(name)
        ws.hide_gridlines(2)
        cols = [c for c in df.columns if not c.startswith("_source")]
        for i, cn in enumerate(cols):
            ws.write(0, i, cn, fmt_generic_hdr)
        for ri, row in enumerate(df.select(cols).iter_rows()):
            for ci, v in enumerate(row):
                write_val(ws, ri + 1, ci, v, data_fmt(cols[ci], ri))
        ws.autofilter(0, 0, df.height, len(cols) - 1)
        for i, cn in enumerate(cols):
            ws.set_column(i, i, max(len(cn) + 2, 15))
        ws.freeze_panes(1, 0)
        ws.set_row(0, 36)

    # ═════════════════════════════════════════════════════════════
    # WRITE ALL SHEETS
    # ═════════════════════════════════════════════════════════════
    write_report_sheet("Report",               dfs["report"])
    write_recon_sheet("Reconciliation",        dfs["recon_report"])
    write_styled_sheet("Outstanding",           dfs["Outstanding"])
    write_styled_sheet("Refund",                dfs["Refund"])
    write_styled_sheet("Outstanding & Refund",  dfs["outstanding_with_refund"])
    write_styled_sheet("Income not in Balance", dfs["income_not_balance"])
    write_styled_sheet("Balance not in Income", dfs["balance_not_income"])

    wb.close()
    output.seek(0)
    return output


def run_reconciliation(root: Path, progress_callback: Callable[[str], None] | None = None) -> dict:
    """Run the full Shopee reconciliation pipeline.

    Returns dict with keys: output (BytesIO), stats (dict), timings (list).
    """
    timings: list[tuple[str, float]] = []
    stats: dict[str, int] = {}

    def notify(msg: str):
        if progress_callback:
            progress_callback(msg)

    # ── 1. Scan files ────────────────────────────────────────────
    t0 = time.time()
    notify("Scanning files...")
    xlsx_files = sorted(root.rglob("*.xlsx"))

    # Extract date range from subfolder names (e.g. Income.released.my.20251231_20260107)
    import re
    all_dates = []
    for d in root.iterdir():
        if d.is_dir():
            m = re.search(r'(\d{8})_(\d{8})$', d.name)
            if m:
                all_dates.extend([m.group(1), m.group(2)])
    if all_dates:
        parsed = sorted(datetime.datetime.strptime(d, "%Y%m%d").date() for d in all_dates)
        stats["date_from"] = parsed[0].strftime("%d %b %Y")
        stats["date_to"]   = parsed[-1].strftime("%d %b %Y")
    timings.append(("Scan files", time.time() - t0))

    # ── 2. Load & process income ─────────────────────────────────
    t0 = time.time()
    notify("Loading income files...")
    income_files = [f for f in xlsx_files if f.name.startswith("Income.released")]
    income_all = _concat_files(income_files, "income_all", sheet_pattern="Income", has_header=False)
    income_all = income_all.unique(subset=["Order ID"], keep="first", maintain_order=True)

    income_all_normalized = (
        income_all
        .filter(pl.col("View By") == "Order")
        .select([
            "Sequence No.", "Order ID", "Order Creation Date", "Payout Completed Date",
            "Total Released Amount (RM)", "Product Price", "Refund Amount",
            "Shipping Fee Paid by Buyer (excl. SST)", "Shipping Fee Charged by Logistic Provider",
            "Seller Paid Shipping Fee SST", "Shipping Rebate From Shopee",
            "Reverse Shipping Fee", "Reverse Shipping Fee SST",
            "Saver Programme Shipping Fee Savings", "Return to Seller Fee",
            "Rebate Provided by Shopee", "Voucher Sponsored by Seller",
            "Coin Cashback Sponsored by Seller", "Commission Fee (incl. SST)",
            "Service Fee (Incl. SST)", "Transaction Fee (Incl. SST)",
            "AMS Commission Fee", "Saver Programme Fee (Incl. SST)",
            "Username (Buyer)", "Transaction Fee Rate (%)",
            "Buyer Payment Method", "Buyer Payment Method Details_1(if applicable)",
            "Payment Details / Installment Plan",
            "Voucher Code From Seller", "Lost Compensation",
        ])
        .with_columns([
            pl.col("Order ID").cast(pl.Utf8).str.strip_chars(),
            pl.col("Total Released Amount (RM)").cast(pl.Float64, strict=False),
            pl.col("Order Creation Date").cast(pl.Utf8).str.strptime(pl.Date, "%Y-%m-%d", strict=False),
            pl.col("Payout Completed Date").cast(pl.Utf8).str.strptime(pl.Date, "%Y-%m-%d", strict=False),
            pl.col("Product Price").cast(pl.Float64, strict=False),
            pl.col("Refund Amount").cast(pl.Float64, strict=False),
            pl.col("Shipping Fee Paid by Buyer (excl. SST)").cast(pl.Float64, strict=False),
            pl.col("Shipping Fee Charged by Logistic Provider").cast(pl.Float64, strict=False),
            pl.col("Seller Paid Shipping Fee SST").cast(pl.Float64, strict=False),
            pl.col("Shipping Rebate From Shopee").cast(pl.Float64, strict=False),
            pl.col("Reverse Shipping Fee").cast(pl.Float64, strict=False),
            pl.col("Reverse Shipping Fee SST").cast(pl.Float64, strict=False),
            pl.col("Saver Programme Shipping Fee Savings").cast(pl.Float64, strict=False),
            pl.col("Return to Seller Fee").cast(pl.Float64, strict=False),
            pl.col("Rebate Provided by Shopee").cast(pl.Float64, strict=False),
            pl.col("Voucher Sponsored by Seller").cast(pl.Float64, strict=False),
            pl.col("Coin Cashback Sponsored by Seller").cast(pl.Float64, strict=False),
            pl.col("Commission Fee (incl. SST)").cast(pl.Float64, strict=False),
            pl.col("Service Fee (Incl. SST)").cast(pl.Float64, strict=False),
            pl.col("Transaction Fee (Incl. SST)").cast(pl.Float64, strict=False),
            pl.col("AMS Commission Fee").cast(pl.Float64, strict=False),
            pl.col("Saver Programme Fee (Incl. SST)").cast(pl.Float64, strict=False),
            pl.col("Transaction Fee Rate (%)").cast(pl.Float64, strict=False),
            pl.col("Lost Compensation").cast(pl.Float64, strict=False),
        ])
        .drop_nulls(["Order ID"])
    )

    income_all_normalized = income_all_normalized.with_columns(
        pl.sum_horizontal([
            pl.col("Rebate Provided by Shopee"),
            pl.col("Voucher Sponsored by Seller"),
            pl.col("Coin Cashback Sponsored by Seller"),
        ]).fill_null(0).alias("Net Voucher")
    )
    income_all_normalized = income_all_normalized.with_columns(
        pl.sum_horizontal([
            pl.col("Shipping Fee Paid by Buyer (excl. SST)"),
            pl.col("Shipping Fee Charged by Logistic Provider"),
            pl.col("Seller Paid Shipping Fee SST"),
            pl.col("Shipping Rebate From Shopee"),
            pl.col("Reverse Shipping Fee"),
            pl.col("Reverse Shipping Fee SST"),
        ]).fill_null(0).alias("Net Shipping Fees")
    )

    stats["income_rows"] = income_all_normalized.height
    timings.append(("Load & process income", time.time() - t0))

    # ── 3. Load & process balance ────────────────────────────────
    t0 = time.time()
    notify("Loading balance files...")
    balance_files = [f for f in xlsx_files if f.name.startswith("my_balance_transaction")]
    balance_all = _concat_files(balance_files, "balance_all", sheet_pattern="Transaction", has_header=False)
    balance_all = balance_all.unique(subset=["Order ID"], keep="first", maintain_order=True)

    balance_all_normalized = (
        balance_all
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

    stats["balance_rows"] = balance_all_normalized.height
    timings.append(("Load & process balance", time.time() - t0))

    # ── 4. Load & process sales ──────────────────────────────────
    t0 = time.time()
    notify("Loading sales files...")
    sales_files = sorted(root.parent.glob("SalesReport*.xlsx"))
    sales_dfs = []
    for f in sales_files:
        df = pl.read_excel(f, sheet_name="SalesReport", has_header=True)
        df = df.with_columns(pl.lit(f.name).alias("_source_file"))
        sales_dfs.append(df)
    sales_report = pl.concat(sales_dfs, how="diagonal_relaxed") if sales_dfs else pl.DataFrame()

    sales_report_normalized = (
        sales_report
        .with_columns([
            pl.coalesce([
                pl.col("SalesWorkDate").cast(pl.Date, strict=False),
                pl.col("SalesWorkDate").cast(pl.Utf8, strict=False).str.strip_chars()
                  .str.strptime(pl.Date, "%d-%m-%Y", strict=False),
            ]).alias("WorkDate"),
            pl.col("MarketPlaceOrderNum").cast(pl.Utf8).str.strip_chars().alias("Order ID"),
            pl.col("TotalAmount").cast(pl.Float64, strict=False).alias("SalesCenterAmount"),
        ])
        .with_columns([
            pl.col("WorkDate").dt.strftime("%b-%Y").str.to_uppercase().alias("Sales Month"),
        ])
    )

    stats["sales_rows"] = sales_report_normalized.height
    timings.append(("Load & process sales", time.time() - t0))

    # ── 5. Generate reports ──────────────────────────────────────
    t0 = time.time()
    notify("Generating reports...")

    # Balance report subset for joining
    balance_all_report = balance_all_normalized.select(["Date", "Order ID"])
    balance_all_report = balance_all_report.with_columns(
        pl.col("Date").dt.date().alias("Payment Date")
    )
    balance_all_report = balance_all_report.with_columns(
        pl.col("Payment Date").dt.strftime("%d-%b-%Y").str.to_uppercase().alias("Payment Date")
    )
    balance_all_report = balance_all_report.with_columns(
        pl.col("Order ID").str.strip_chars(),
        pl.col("Payment Date"),
    )

    # Report = income INNER JOIN balance
    report = income_all_normalized.join(balance_all_report, on="Order ID", how="inner")
    report = report.with_columns(
        pl.col("Date").dt.strftime("%b-%Y").str.to_uppercase().alias("Payment Mth")
    )
    stats["report_rows"] = report.height

    # Anti joins
    income_not_balance = income_all_normalized.join(balance_all_normalized, on="Order ID", how="anti")
    balance_not_income = balance_all_normalized.join(income_all_normalized, on="Order ID", how="anti")
    stats["income_not_balance"] = income_not_balance.height
    stats["balance_not_income"] = balance_not_income.height

    # Reconciliation parts
    first_part = sales_report_normalized.select([
        "Order ID", "OrderNum", "MarketPlaceOrderNum",
        "SalesWorkDate", "SalesCenterAmount", "Sales Month",
    ])
    payment_date = balance_all_normalized.select([
        "Order ID", "Payment Date", "Payment Month", "Payment Amount",
    ])
    thrid_part = income_all_normalized.select(
        "Order ID",
        pl.col("Commission Fee (incl. SST)").alias("Commission Fee"),
        pl.col("Transaction Fee (Incl. SST)").alias("Transaction Fee"),
        pl.col("Service Fee (Incl. SST)").alias("Service Fee"),
        pl.col("AMS Commission Fee"),
        pl.col("Return to Seller Fee").alias("Return QC Fee"),
        pl.col("Rebate Provided by Shopee").alias("Voucher/(Disc rebate)"),
        pl.col("Refund Amount").alias("Refund"),
        (
            pl.col("Shipping Fee Paid by Buyer (excl. SST)")
            + pl.col("Shipping Fee Charged by Logistic Provider")
            + pl.col("Seller Paid Shipping Fee SST")
            + pl.col("Shipping Rebate From Shopee")
            + pl.col("Reverse Shipping Fee")
            + pl.col("Reverse Shipping Fee SST")
        ).alias("Actual Shipping Fee"),
    )

    recon_report = (
        first_part
        .join(payment_date, on="Order ID", how="left")
        .join(thrid_part, on="Order ID", how="left")
    )
    _outstanding = (
            pl.col("SalesCenterAmount") - pl.col("Payment Amount")
            + pl.col("Commission Fee") + pl.col("Transaction Fee")
            + pl.col("Service Fee") + pl.col("Return QC Fee")
            + pl.col("Voucher/(Disc rebate)") + pl.col("Actual Shipping Fee")
    ).round(2)
    recon_report = recon_report.with_columns(
        pl.when(_outstanding == 0).then(pl.lit(0.0)).otherwise(_outstanding).alias("Outstanding")
    )
    stats["recon_rows"] = recon_report.height

    Outstanding = recon_report.filter(pl.col("Outstanding") != 0).select([
        "Order ID", "SalesCenterAmount", "Payment Amount",
        "Commission Fee", "Transaction Fee", "Service Fee",
        "Return QC Fee", "Voucher/(Disc rebate)",
        "Actual Shipping Fee", "Outstanding",
    ])
    stats["outstanding_rows"] = Outstanding.height

    Refund = income_all_normalized.filter(pl.col("Refund Amount") != 0)
    stats["refund_rows"] = Refund.height

    outstanding_with_refund = Outstanding.join(Refund, on="Order ID", how="inner")
    stats["outstanding_refund_rows"] = outstanding_with_refund.height

    timings.append(("Generate reports", time.time() - t0))

    # ── 6. Export to Excel ───────────────────────────────────────
    t0 = time.time()
    notify("Exporting to Excel...")

    excel_output = _export_to_excel({
        "report": report,
        "recon_report": recon_report,
        "Outstanding": Outstanding,
        "Refund": Refund,
        "outstanding_with_refund": outstanding_with_refund,
        "income_not_balance": income_not_balance,
        "balance_not_income": balance_not_income,
    })

    timings.append(("Export to Excel", time.time() - t0))

    return {
        "output": excel_output,
        "stats": stats,
        "timings": timings,
    }
