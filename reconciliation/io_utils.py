"""
reconciliation/io_utils.py
--------------------------
Excel I/O utilities for the Shopee reconciliation pipeline.

All functions accept raw bytes (from Streamlit file-uploader or disk read)
so the same code works in both the Streamlit UI and plain scripts.
"""
from __future__ import annotations

import tempfile
from pathlib import Path

import fastexcel
import polars as pl


def read_excel_path(
    path: str | Path,
    filename: str,
    sheet_pattern: str | None = None,
    has_header: bool = True,
) -> pl.DataFrame:
    """Read matching sheets from an Excel file path into a Polars DataFrame."""
    reader = fastexcel.read_excel(str(path))

    sheets = [
        s for s in reader.sheet_names
        if sheet_pattern is None or s.startswith(sheet_pattern)
    ]

    dfs: list[pl.DataFrame] = []
    for name in sheets:
        df = pl.read_excel(str(path), sheet_name=name, has_header=has_header)
        df = df.with_columns(pl.lit(name).alias("_source_sheet"))
        dfs.append(df)

    if not dfs:
        return pl.DataFrame()

    combined = pl.concat(dfs, how="diagonal_relaxed") if len(dfs) > 1 else dfs[0]
    return combined.with_columns(pl.lit(filename).alias("_source_file"))


# ──────────────────────────────────────────────────────────────────────────────
# Low-level reader
# ──────────────────────────────────────────────────────────────────────────────

def read_excel_bytes(
    data: bytes,
    filename: str,
    sheet_pattern: str | None = None,
    has_header: bool = True,
) -> pl.DataFrame:
    """
    Read matching sheets from an Excel file supplied as raw bytes.

    Parameters
    ----------
    data:          Raw `.xlsx` bytes.
    filename:      Original filename (stored in ``_source_file`` column).
    sheet_pattern: Sheet-name prefix to match (e.g. ``"Income"`` matches
                   ``"Income - 1"``, ``"Income - 2"``).  ``None`` = all sheets.
    has_header:    Whether the first row of each sheet is a header row.
    """
    with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
        tmp.write(data)
        tmp_path = Path(tmp.name)

    try:
        return read_excel_path(
            tmp_path,
            filename,
            sheet_pattern=sheet_pattern,
            has_header=has_header,
        )
    finally:
        tmp_path.unlink(missing_ok=True)


# ──────────────────────────────────────────────────────────────────────────────
# Multi-file concatenator
# ──────────────────────────────────────────────────────────────────────────────

def concat_excel_files(
    files: list[tuple[bytes, str]],
    label: str,
    sheet_pattern: str | None = None,
    has_header: bool = True,
) -> pl.DataFrame:
    """
    Load and vertically concatenate a list of ``(bytes, filename)`` Excel pairs.

    The ``label`` parameter controls how the real header row is identified for
    Shopee-formatted files that store metadata above the column names:

    * ``"income_all"``  – header at row index 2, data from row 3 onward.
    * ``"balance_all"`` – header at row index 13, data from row 14 onward.
    * anything else    – standard ``has_header`` behaviour.
    """
    if not files:
        return pl.DataFrame()

    dfs: list[pl.DataFrame] = []
    for data, filename in files:
        df = read_excel_bytes(
            data, filename, sheet_pattern=sheet_pattern, has_header=has_header
        )

        if label == "income_all" and not has_header:
            header = [
                str(h) if h is not None else f"col_{i}"
                for i, h in enumerate(df.row(2))
            ]
            df = df.slice(3)
            df.columns = header

        elif label == "balance_all" and not has_header:
            header = [
                str(h) if h is not None else f"col_{i}"
                for i, h in enumerate(df.row(13))
            ]
            df = df.slice(14)
            df.columns = header

        dfs.append(df)

    return pl.concat(dfs, how="diagonal_relaxed")


# ──────────────────────────────────────────────────────────────────────────────
# Convenience: load from disk paths (useful in notebooks / tests)
# ──────────────────────────────────────────────────────────────────────────────

def load_files_from_paths(
    paths: list[Path],
    label: str,
    sheet_pattern: str | None = None,
    has_header: bool = True,
) -> pl.DataFrame:
    """
    Convenience wrapper that reads files from disk and delegates to
    :func:`concat_excel_files`.
    """
    files = [(p.read_bytes(), p.name) for p in paths]
    return concat_excel_files(files, label, sheet_pattern, has_header)
