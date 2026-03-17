"""
reconciliation/excel_export.py
------------------------------
Styled Excel export using xlsxwriter.  Produces a multi-sheet workbook with
corporate colour-coded headers, zebra-striped data rows, and grouped banners.
"""
from __future__ import annotations

import datetime
import io
from typing import Any

import polars as pl
import xlsxwriter


def export_to_excel(dfs: dict[str, pl.DataFrame]) -> io.BytesIO:
    """Create a formatted Excel workbook from reconciliation DataFrames.

    Parameters
    ----------
    dfs : dict
        Expected keys (all pl.DataFrame):
          report, recon_report, Outstanding, Refund,
          outstanding_with_refund, income_not_balance, balance_not_income

    Returns
    -------
    io.BytesIO
        In-memory Excel file ready for download.
    """
    output = io.BytesIO()
    wb = xlsxwriter.Workbook(output, {"in_memory": True})

    # ── Colour palette ───────────────────────────────────────────
    PAL = {
        "navy":    {"dark": "#1F3864", "med": "#2E75B6", "light": "#B4C6E7"},
        "teal":    {"dark": "#1D6D37", "med": "#2E8B57", "light": "#A3CFBB"},
        "amber":   {"dark": "#7D5A00", "med": "#BF8F00", "light": "#FFE699"},
        "crimson": {"dark": "#833C0B", "med": "#C0504D", "light": "#F8CBAD"},
        "purple":  {"dark": "#4A1A6B", "med": "#7B4F9E", "light": "#D9C4EC"},
        "slate":   {"dark": "#2D3436", "med": "#636E72", "light": "#DFE6E9"},
    }
    BORDER_CLR = "#C0C0C0"

    # ── Format helpers ───────────────────────────────────────────
    def _hdr(bg: str, fc: str = "white", sz: int = 10, bold: bool = True):
        return wb.add_format({
            "bold": bold, "font_color": fc, "bg_color": bg,
            "border": 1, "border_color": BORDER_CLR,
            "align": "center", "valign": "vcenter",
            "font_size": sz, "text_wrap": True, "font_name": "Calibri",
        })

    def _data(bg: str = "#FFFFFF", num_fmt: str | None = None,
              align: str = "left", fc: str = "#1A1A1A"):
        p: dict[str, Any] = {
            "border": 1, "border_color": BORDER_CLR,
            "font_size": 10, "valign": "vcenter", "align": align,
            "font_color": fc, "bg_color": bg, "font_name": "Calibri",
        }
        if num_fmt:
            p["num_format"] = num_fmt
        return wb.add_format(p)

    # ── L1 / L2 / L3 formats for the Report sheet ───────────────
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

    L2_GROUP_FMT = {
        "Released Amount Details": _hdr(PAL["teal"]["med"]),
        "Reference Info":          _hdr(PAL["slate"]["med"]),
    }
    L2_BLANK = wb.add_format({"bg_color": "#FFFFFF", "bottom": 1, "bottom_color": BORDER_CLR})

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

    # ── Zebra-striped row formats ────────────────────────────────
    def _make_row_fmts(bg: str) -> dict[str, Any]:
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

    def _fmt_key(c: str) -> str:
        if c in MONEY_COLS:  return "num"
        if c in DATE_COLS:   return "date"
        if c in MONTH_COLS:  return "month"
        if c == "Payment Date": return "center"
        return "text"

    def data_fmt(col: str, ri: int):
        return (ROW_W if ri % 2 == 0 else ROW_G)[_fmt_key(col)]

    def write_val(ws, r: int, c: int, v: Any, fmt):
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
    def write_report_sheet(name: str, df: pl.DataFrame):
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

        flat: list[tuple[str, str, str]] = []
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
    # RECONCILIATION SHEET — 2-level, distinct group colours
    # ═════════════════════════════════════════════════════════════
    def write_recon_sheet(name: str, df: pl.DataFrame):
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

        col_info: list[tuple[str, str]] = []
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
    def write_styled_sheet(name: str, df: pl.DataFrame):
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
