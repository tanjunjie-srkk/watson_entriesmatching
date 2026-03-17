# Shopee Reconciliation Pipeline

> Automated financial reconciliation system for Shopee marketplace transactions — matching Income Released reports, Balance Transaction reports, and Sales Center records to identify outstanding orders, refunds, and discrepancies.

---

## Table of Contents

1. [Overview](#1-overview)
2. [Architecture](#2-architecture)
3. [Project Structure](#3-project-structure)
4. [Module Reference](#4-module-reference)
5. [Data Flow](#5-data-flow)
6. [Input File Specifications](#6-input-file-specifications)
7. [Output Artifacts](#7-output-artifacts)
8. [Getting Started](#8-getting-started)
9. [Usage](#9-usage)
10. [Testing](#10-testing)
11. [Configuration](#11-configuration)
12. [Glossary](#12-glossary)

---

## 1. Overview

### 1.1 Purpose

This application automates the reconciliation of Shopee Malaysia seller financial reports. It cross-references three distinct data sources — **Income Released**, **Balance Transaction**, and **Sales Center** reports — to produce a unified reconciliation view that highlights:

- **Outstanding orders** — where the calculated payout does not match the expected amount.
- **Refund orders** — outstanding orders that involve a non-zero refund.
- **Income-not-in-Balance discrepancies** — orders present in Income but absent from Balance.
- **Balance-not-in-Income discrepancies** — orders present in Balance but absent from Income.
- **Compare table** — side-by-side comparison against a master reconciliation file (optional).

### 1.2 Key Features

| Feature | Description |
|---|---|
| Multi-file ingestion | Handles multiple Excel files per report type across date ranges |
| Smart header detection | Automatically locates the real header row in Shopee's non-standard Excel layouts |
| High-performance processing | Built on [Polars](https://pola.rs/) for columnar, zero-copy DataFrame operations |
| Interactive dashboard | Streamlit-based UI with filters, search, and live progress tracking |
| Formatted Excel export | Corporate-styled multi-sheet workbook with colour-coded group headers and zebra striping |
| Master comparison | Optional comparison against an existing manual reconciliation master file |
| Caching | `st.cache_data` avoids redundant pipeline reruns on unchanged inputs |

### 1.3 Technology Stack

| Layer | Technology | Version |
|---|---|---|
| Language | Python | 3.10+ |
| Data processing | Polars | >= 1.0.0 |
| Excel reading | fastexcel / openpyxl | >= 0.11.0 / >= 3.1.0 |
| Excel writing | xlsxwriter | >= 3.1.0 |
| Web UI | Streamlit | >= 1.35.0 |
| Tabular display | Pandas (Streamlit interop) | >= 2.0.0 |

---

## 2. Architecture

### 2.1 High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        PRESENTATION LAYER                           │
│                                                                     │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │                    ui/app.py (Streamlit)                      │  │
│  │  • Sidebar: folder path input, file discovery, run button    │  │
│  │  • Main: metrics, timing, tabbed DataFrames, Excel download  │  │
│  └──────────────────────────┬────────────────────────────────────┘  │
│                             │ calls                                 │
├─────────────────────────────┼───────────────────────────────────────┤
│                        BUSINESS LOGIC LAYER                         │
│                             │                                       │
│  ┌──────────────────────────▼────────────────────────────────────┐  │
│  │               reconciliation/pipeline.py                      │  │
│  │  • run_reconciliation_from_paths() — path-based entry point  │  │
│  │  • run_reconciliation()            — bytes-based entry point │  │
│  │  • _normalize_income()   — income column casting & derivation│  │
│  │  • _normalize_balance()  — balance parsing & date formatting │  │
│  │  • _normalize_sales()    — sales date/amount normalisation   │  │
│  │  • _build_recon_report() — 3-way join + Outstanding calc     │  │
│  │  • _build_compare_table()— side-by-side vs. master file      │  │
│  └────────┬──────────────────────────────────────┬───────────────┘  │
│           │ uses                                  │ uses            │
│  ┌────────▼──────────────┐            ┌──────────▼──────────────┐  │
│  │ reconciliation/       │            │ reconciliation/          │  │
│  │   io_utils.py         │            │   excel_export.py        │  │
│  │ • read_excel_bytes()  │            │ • export_to_excel()      │  │
│  │ • read_excel_path()   │            │ • write_report_sheet()   │  │
│  │ • concat_excel_files()│            │ • write_recon_sheet()    │  │
│  │ • load_files_from_    │            │ • write_styled_sheet()   │  │
│  │     paths()           │            │                          │  │
│  └───────────────────────┘            └──────────────────────────┘  │
│                                                                     │
├─────────────────────────────────────────────────────────────────────┤
│                          DATA LAYER                                 │
│                                                                     │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │              Local File System (Excel .xlsx)                  │  │
│  │  scenario_folder/                                             │  │
│  │  ├── Income.released.my.YYYYMMDD_YYYYMMDD/                   │  │
│  │  │   └── Income.released.*.xlsx                              │  │
│  │  ├── my_balance_transaction_report.shopee.YYYYMMDD_YYYYMMDD/ │  │
│  │  │   └── my_balance_transaction*.xlsx                        │  │
│  │  └── SalesReport/                                            │  │
│  │      └── SalesReport*.xlsx                                   │  │
│  └───────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
```

### 2.2 Component Interaction

```
User ──► Streamlit UI ──► run_reconciliation_from_paths()
                                    │
              ┌─────────────────────┼─────────────────────┐
              ▼                     ▼                     ▼
      load_files_from_paths() load_files_from_paths() pl.read_excel()
        (income, Income)       (balance, Transaction)   (sales, SalesReport)
              │                     │                     │
              ▼                     ▼                     ▼
      _normalize_income()   _normalize_balance()   _normalize_sales()
              │                     │                     │
              └─────────────────────┼─────────────────────┘
                                    ▼
                         _build_recon_report()
                          (3-way inner join)
                                    │
              ┌────────────┬────────┼────────┬────────────┐
              ▼            ▼        ▼        ▼            ▼
         Outstanding    Refund   Compare  Income≠Bal  Bal≠Income
              │            │        │        │            │
              └────────────┴────────┼────────┴────────────┘
                                    ▼
                          export_to_excel()
                                    │
                                    ▼
                         .xlsx download (7 sheets)
```

### 2.3 Design Decisions

| Decision | Rationale |
|---|---|
| **Polars over Pandas** | Polars provides significantly faster read/join/filter performance on the 10k–100k row datasets typical in Shopee reports, with lazy evaluation and zero-copy operations. |
| **Bytes-based API + Path wrapper** | `run_reconciliation()` accepts raw bytes for portability (Streamlit file uploads, tests, APIs). `run_reconciliation_from_paths()` wraps it for the folder-based workflow. |
| **fastexcel for reading** | `fastexcel` uses the Calamine Rust engine under the hood, providing ~5–10x faster Excel reads than openpyxl. |
| **xlsxwriter for writing** | Only `xlsxwriter` supports the level of cell-level formatting (merged banners, conditional number formats, zebra striping) required for the corporate report style. |
| **Streamlit folder path input** | Avoids the multi-file upload limit and preserves the subfolder date-range metadata that Shopee encodes in directory names. |

---

## 3. Project Structure

```
watson_entriesmatching/
│
├── reconciliation/                  # Core library package
│   ├── __init__.py                  # Public API re-exports
│   ├── pipeline.py                  # Reconciliation business logic
│   ├── io_utils.py                  # Excel I/O (read, concat, header detection)
│   └── excel_export.py              # Styled multi-sheet Excel writer
│
├── ui/
│   └── app.py                       # Streamlit dashboard (production UI)
│
├── OneDrive_2026-03-09/             # Legacy / prototype code (reference only)
│   ├── app.py                       # Original monolithic Streamlit app
│   ├── reconciliation_engine.py     # Original monolithic pipeline + Excel export
│   ├── concatenation.py             # Early data-loading experiment
│   ├── *.ipynb                      # Exploratory Jupyter notebooks
│   └── Shopee Sample Reports (Testing)/
│       ├── scenario1/               # Test dataset — single date range
│       └── scenario2/               # Test dataset — multiple date ranges
│
├── requirements.txt                 # pip dependencies (authoritative)
├── requirement.txt                  # Legacy dependency notes
├── test_smoke.py                    # End-to-end smoke test
└── README.md                        # This file
```

### 3.1 Package Boundary

Only the `reconciliation/` package and `ui/app.py` are production code. Everything under `OneDrive_2026-03-09/` is retained as historical reference from the prototyping phase and should not be imported in production.

---

## 4. Module Reference

### 4.1 `reconciliation/__init__.py`

Re-exports the public API surface:

```python
from reconciliation.pipeline import run_reconciliation, run_reconciliation_from_paths
from reconciliation.excel_export import export_to_excel
```

### 4.2 `reconciliation/pipeline.py`

The core business logic module. Contains all data normalisation, joining, and reconciliation computation.

#### Public Functions

| Function | Signature | Description |
|---|---|---|
| `run_reconciliation` | `(income_files, balance_files, sales_files) → dict` | Bytes-based entry point. Accepts `list[tuple[bytes, str]]` for each report type. |
| `run_reconciliation_from_paths` | `(income_paths, balance_paths, sales_paths, master_recon_path?, progress_callback?) → dict` | Path-based entry point used by the Streamlit UI. Supports progress callbacks and master file comparison. |

#### Return Dictionary Keys

| Key | Type | Description |
|---|---|---|
| `report` | `pl.DataFrame` | Income inner-joined with Balance (full detail report) |
| `recon_report` | `pl.DataFrame` | 3-way joined reconciliation table with Outstanding column |
| `outstanding` | `pl.DataFrame` | Subset where `Outstanding ≠ 0` |
| `refund` | `pl.DataFrame` | Subset where `Refund ≠ 0` |
| `compare` | `pl.DataFrame` | Side-by-side comparison with master file (empty if no master) |
| `outstanding_with_refund` | `pl.DataFrame` | Intersection of outstanding and refund orders |
| `income_not_balance` | `pl.DataFrame` | Anti-join: income orders missing from balance |
| `balance_not_income` | `pl.DataFrame` | Anti-join: balance orders missing from income |
| `stats` | `dict` | Row counts for all result sets |
| `timings` | `list[tuple[str, float]]` | Per-step execution durations in seconds |

#### Internal Functions

| Function | Purpose |
|---|---|
| `_normalize_income(raw)` | De-duplicate, filter to `View By == "Order"`, select/cast 30+ columns, derive `Net Voucher` and `Net Shipping Fees` |
| `_normalize_balance(raw)` | De-duplicate, parse datetime, compute `Payment Date` and `Payment Month` |
| `_normalize_sales(raw)` | Parse `SalesWorkDate`, create `Order ID` alias, compute `Sales Month` |
| `_build_recon_report(income, balance, sales)` | Inner-join all three sources on `Order ID`, compute `Outstanding = Sales − Payment + Fees` |
| `_normalize_original_recon(ori_recon)` | Prepare master reconciliation file columns with `" Ori"` suffix |
| `_build_compare_table(recon_report, ori_recon)` | Join generated recon with master, interleave `Recon` / `Ori` columns |

### 4.3 `reconciliation/io_utils.py`

Handles all Excel file reading with smart header detection for Shopee's non-standard layouts.

| Function | Description |
|---|---|
| `read_excel_path(path, filename, sheet_pattern?, has_header?)` | Read matching sheets from an Excel path into a Polars DataFrame |
| `read_excel_bytes(data, filename, sheet_pattern?, has_header?)` | Same as above but from raw bytes (writes to temp file internally) |
| `concat_excel_files(files, label, sheet_pattern?, has_header?)` | Concatenate multiple `(bytes, filename)` files. Applies header-row detection: row 2 for income, row 13 for balance |
| `load_files_from_paths(paths, label, sheet_pattern?, has_header?)` | Convenience wrapper that reads from disk paths and delegates to `concat_excel_files` |

#### Header Detection Logic

Shopee Excel files contain metadata rows above the actual column headers:

| Report Type | Label | Header Row Index | Data Starts At |
|---|---|---|---|
| Income Released | `"income_all"` | Row 2 (0-indexed) | Row 3 |
| Balance Transaction | `"balance_all"` | Row 13 (0-indexed) | Row 14 |
| Sales Report | (standard) | Row 0 | Row 1 |

### 4.4 `reconciliation/excel_export.py`

Generates a professionally formatted multi-sheet Excel workbook using `xlsxwriter`.

| Function | Description |
|---|---|
| `export_to_excel(dfs)` | Main entry point. Accepts a dict of DataFrames and returns `io.BytesIO` |

#### Output Sheets

| Sheet Name | Source | Style |
|---|---|---|
| **Report** | Income ⋈ Balance | 3-level grouped headers (L1 banner → L2 sub-group → L3 column) |
| **Reconciliation** | 3-way join | 2-level grouped headers with colour-coded groups |
| **Outstanding** | `Outstanding ≠ 0` | Generic styled sheet |
| **Refund** | `Refund ≠ 0` | Generic styled sheet |
| **Outstanding & Refund** | Intersection | Generic styled sheet |
| **Income not in Balance** | Anti-join | Generic styled sheet |
| **Balance not in Income** | Anti-join | Generic styled sheet |

#### Colour Palette

| Group | Dark | Medium | Light |
|---|---|---|---|
| Navy (Order Info) | `#1F3864` | `#2E75B6` | `#B4C6E7` |
| Teal (Released Amount) | `#1D6D37` | `#2E8B57` | `#A3CFBB` |
| Amber (Payment Info) | `#7D5A00` | `#BF8F00` | `#FFE699` |
| Crimson (Fees) | `#833C0B` | `#C0504D` | `#F8CBAD` |
| Purple (Buyer / Result) | `#4A1A6B` | `#7B4F9E` | `#D9C4EC` |
| Slate (Reference) | `#2D3436` | `#636E72` | `#DFE6E9` |

### 4.5 `ui/app.py`

Streamlit dashboard providing the interactive front-end.

#### Key Features

- **Sidebar**: folder path text input, automatic file discovery by prefix pattern, master recon file detection, force-rerun toggle.
- **Progress tracking**: real-time `st.status` panel and `st.progress` bar with per-step updates.
- **Caching**: `@st.cache_data` memoises pipeline results keyed on file path tuples.
- **Metrics panel**: date range, timing breakdown, summary row counts.
- **Tabbed results**: six tabs (Reconciliation, Outstanding, Refund, Compare, Income≠Balance, Balance≠Income) each with:
  - Month filter (multiselect)
  - Outstanding status filter (All / Outstanding / Matched)
  - Payment Date filter
  - Order ID free-text search
  - Row count display
  - Interactive scrollable `st.dataframe`
  - CSV download button
- **Excel download**: single-click download of the complete formatted workbook.

---

## 5. Data Flow

### 5.1 Reconciliation Formula

The **Outstanding** column is computed as:

$$
\text{Outstanding} = \text{SalesCenterAmount} - \text{PaymentAmount} + \text{CommissionFee} + \text{TransactionFee} + \text{ServiceFee} + \text{AMSCommissionFee} + \text{ReturnQCFee} + \text{ActualShippingFee}
$$

Where:

$$
\text{ActualShippingFee} = \sum(\text{ShippingFeeBuyer}, \text{ShippingFeeLogistic}, \text{SellerShippingSST}, \text{ShippingRebate}, \text{ReverseShipping}, \text{ReverseShippingSST})
$$

A result of `0.00` indicates the order is **fully matched**.

### 5.2 Join Strategy

```
Sales ──┐
        ├── INNER JOIN on Order ID ──► sales + payment columns
Balance─┘
        ├── INNER JOIN on Order ID ──► + income fee columns
Income──┘
                                       = recon_report
```

### 5.3 Discrepancy Detection

| Set | Join Type | Meaning |
|---|---|---|
| `income_not_balance` | `income ANTI JOIN balance` | Orders with income released but no balance transaction recorded |
| `balance_not_income` | `balance ANTI JOIN income` | Orders with balance transactions but no income release |

---

## 6. Input File Specifications

### 6.1 Income Released Report

- **Filename pattern**: `Income.released.*.xlsx`
- **Sheet name prefix**: `Income`
- **Header row**: Row index 2 (rows 0–1 contain Shopee metadata)
- **Key columns**: `Order ID`, `View By`, `Total Released Amount (RM)`, `Product Price`, `Refund Amount`, commission/fee columns (30+ total)
- **Dedup key**: `Order ID` (first occurrence kept)
- **Filter**: Only rows where `View By == "Order"` are retained

### 6.2 Balance Transaction Report

- **Filename pattern**: `my_balance_transaction*.xlsx`
- **Sheet name prefix**: `Transaction`
- **Header row**: Row index 13 (rows 0–12 contain Shopee metadata)
- **Key columns**: `Order ID`, `Date`, `Amount`, `Transaction Type`, `Status`
- **Dedup key**: `Order ID` (first occurrence kept)

### 6.3 Sales Center Report

- **Filename pattern**: `SalesReport*.xlsx`
- **Sheet name**: `SalesReport`
- **Header row**: Row 0 (standard format)
- **Key columns**: `MarketPlaceOrderNum` (→ `Order ID`), `TotalAmount` (→ `SalesCenterAmount`), `SalesWorkDate`, `OrderNum`

### 6.4 Master Reconciliation File (Optional)

- **Filename pattern**: `*Shopee Payment Master List*.xlsx`
- **Sheet name**: `Recon`
- **Purpose**: Side-by-side comparison with generated reconciliation output
- **Location**: Searched in the scenario folder and its parent folder

### 6.5 Folder Structure Convention

```
scenario_folder/
├── Income.released.my.YYYYMMDD_YYYYMMDD/
│   └── *.xlsx
├── my_balance_transaction_report.shopee.YYYYMMDD_YYYYMMDD/
│   └── *.xlsx
├── SalesReport/
│   └── *.xlsx
└── Shopee Payment Master List*.xlsx  (optional)
```

The `YYYYMMDD_YYYYMMDD` subfolder naming convention encodes the report date range and is parsed automatically.

---

## 7. Output Artifacts

### 7.1 Excel Workbook

A single `.xlsx` file with 7 sheets:

| # | Sheet | Rows | Description |
|---|---|---|---|
| 1 | Report | Income ⋈ Balance | Full detail view with 3-level colour-coded headers |
| 2 | Reconciliation | Sales ⋈ Balance ⋈ Income | Grouped by Order Info / Sales / Payment / Fees / Result |
| 3 | Outstanding | `Outstanding ≠ 0` | Orders with unresolved amounts |
| 4 | Refund | `Refund ≠ 0` | Orders involving refunds |
| 5 | Outstanding & Refund | Intersection | Outstanding orders that also have refunds |
| 6 | Income not in Balance | Anti-join | Missing balance records |
| 7 | Balance not in Income | Anti-join | Missing income records |

### 7.2 Dashboard Outputs

- **Inline metrics**: row counts, date ranges, per-step timing
- **Filterable tables**: per-tab interactive DataFrames with virtual scrolling
- **CSV exports**: per-tab CSV download with UTF-8 BOM encoding

---

## 8. Getting Started

### 8.1 Prerequisites

- **Python 3.10+** (required for `X | Y` union type syntax)
- **pip** or **uv** package manager

### 8.2 Installation

```bash
# 1. Clone or download the project
cd watson_entriesmatching

# 2. Create a virtual environment
python -m venv .venv

# 3. Activate the virtual environment
# Windows (PowerShell):
.venv\Scripts\Activate.ps1
# macOS / Linux:
source .venv/bin/activate

# 4. Install dependencies
pip install -r requirements.txt
```

### 8.3 Verify Installation

```bash
python -c "import reconciliation; print('OK')"
```

---

## 9. Usage

### 9.1 Running the Streamlit Dashboard (Recommended)

```bash
# From the project root directory
streamlit run ui/app.py
```

This opens the browser at `http://localhost:8501`. Then:

1. **Paste the scenario folder path** into the sidebar text input.
2. The app auto-discovers Income, Balance, and Sales files, and displays the file counts.
3. Click **"Run Reconciliation"** to execute the pipeline.
4. View results across six tabbed panels with filtering and search.
5. Click **"Download Excel Report"** to export the formatted workbook.

### 9.2 Programmatic Usage (Python Script)

```python
from pathlib import Path
from reconciliation import run_reconciliation_from_paths, export_to_excel

# Define file paths
root = Path("path/to/scenario_folder")
xlsx = sorted(root.rglob("*.xlsx"))

income  = [str(p) for p in xlsx if p.name.startswith("Income.released")]
balance = [str(p) for p in xlsx if p.name.startswith("my_balance_transaction")]
sales   = [str(p) for p in xlsx if p.name.startswith("SalesReport")]

# Run pipeline
result = run_reconciliation_from_paths(income, balance, sales)

# Access results
print(f"Recon rows: {result['stats']['recon_rows']}")
print(f"Outstanding: {result['stats']['outstanding_rows']}")

# Export to Excel
excel = export_to_excel({
    "report":                  result["report"],
    "recon_report":            result["recon_report"],
    "Outstanding":             result["outstanding"],
    "Refund":                  result["refund"],
    "outstanding_with_refund": result["outstanding_with_refund"],
    "income_not_balance":      result["income_not_balance"],
    "balance_not_income":      result["balance_not_income"],
})

Path("output.xlsx").write_bytes(excel.getvalue())
```

### 9.3 Using the Bytes-Based API

For integration with file upload systems or REST APIs:

```python
from reconciliation import run_reconciliation

result = run_reconciliation(
    income_files=[(file1_bytes, "Income.released.xlsx")],
    balance_files=[(file2_bytes, "my_balance_transaction.xlsx")],
    sales_files=[(file3_bytes, "SalesReport.xlsx")],
)
```

---

## 10. Testing

### 10.1 Smoke Test

The project includes an end-to-end smoke test that validates the full pipeline and Excel export:

```bash
python test_smoke.py
```

**What it verifies:**

- All three file types load and parse correctly
- Pipeline produces non-empty results for all output keys
- Timing data is captured for every step
- Excel export produces a valid non-zero-byte `.xlsx` binary
- Prints `ALL TESTS PASSED` on success

> **Note**: The smoke test requires the sample data in `OneDrive_2026-03-09/Shopee Sample Reports (Testing)/scenario2/`.

---

## 11. Configuration

### 11.1 Environment Variables

No environment variables are required. All configuration is handled through input folder paths.

### 11.2 Streamlit Configuration

The app uses default Streamlit configuration with:

- `layout="wide"` — full-width dashboard
- `page_title="Shopee Reconciliation"` — browser tab title
- `page_icon="📊"` — browser tab icon

Custom Streamlit configuration can be placed in `.streamlit/config.toml` if needed.

### 11.3 Caching

Pipeline results are cached by Streamlit using `@st.cache_data`, keyed on the file path tuples. Use the **"Force rerun (skip cache)"** checkbox in the sidebar to clear the cache and rerun the pipeline.

---

## 12. Glossary

| Term | Definition |
|---|---|
| **Income Released** | Shopee seller payout report showing released amounts per order |
| **Balance Transaction** | Shopee seller wallet transaction history |
| **Sales Center** | Internal ERP/POS sales record for the same marketplace orders |
| **Outstanding** | Net difference between expected (Sales) and actual (Payment) amounts after all fees |
| **Anti-join** | A join returning only rows from the left table that have no match in the right table |
| **SalesCenterAmount** | The total amount recorded in the Sales Center system |
| **Payment Amount** | The amount received in the Shopee seller balance |
| **Recon Report** | The 3-way joined reconciliation table |
| **Master Recon** | An existing manually-prepared reconciliation file used for comparison |

---

## License

Internal use only — SRKK Group.
