import polars as pl
from pathlib import Path

root = Path(r"c:\Users\TanJunJie\OneDrive - SRKK Group\Project\watson_entriesmatching\OneDrive_2026-03-09\Shopee Sample Reports (Testing)\scenario1")

# Find all .xlsx files recursively
xlsx_files = sorted(root.rglob("*.xlsx"))

# --- Helper: read one Excel file into a Polars DataFrame ---
def read_excel(path: Path) -> pl.DataFrame:
    df = pl.read_excel(path)
    # Add source filename column for traceability
    return df.with_columns(pl.lit(path.name).alias("_source_file"))

# --- Load & concatenate by report type ---
income_files = [f for f in xlsx_files if f.name.startswith("Income.released")]
balance_files = [f for f in xlsx_files if f.name.startswith("my_balance_transaction")]
#sales_files = [f for f in xlsx_files if f.name.startswith("SalesReport")]
#payment_files = [f for f in xlsx_files if f.name.startswith("Shopee Payment")]

def concat_files(files: list[Path], label: str) -> pl.DataFrame:
    if not files:
        return pl.DataFrame()
    dfs = []
    for f in files:
        df = read_excel(f)
        dfs.append(df)
        print(f"  Loaded {f.name}: {df.shape}")
    combined = pl.concat(dfs, how="diagonal_relaxed")
    before = combined.height
    # Drop duplicates (exclude _source_file so rows from overlapping files are caught)
    data_cols = [c for c in combined.columns if c != "_source_file"]
    combined = combined.unique(subset=data_cols, keep="first")
    after = combined.height
    dupes = before - after
    print(f"=> {label}: {combined.shape} (removed {dupes} duplicate rows)\n")
    return combined

print("=== Income Released ===")
income_all = concat_files(income_files, "income_all")

print("=== Balance Transaction ===")
balance_all = concat_files(balance_files, "balance_all")

#print("=== Sales Reports ===")
#sales_all = concat_files(sales_files, "sales_all")

#print("=== Payment Master ===")
#payment_all = concat_files(payment_files, "payment_all")

# Quick preview
for name, df in [("income_all", income_all), ("balance_all", balance_all),
                   #("sales_all", sales_all), ("payment_all", payment_all)
                   ]:
    if df.height > 0:
        print(f"\n--- {name} columns ---")
        print(df.columns)