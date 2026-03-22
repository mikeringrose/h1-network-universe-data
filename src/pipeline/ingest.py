"""Read CSV/Excel into polars LazyFrame or DataFrame."""

from pathlib import Path

import polars as pl


def read_csv(path: str | Path) -> pl.LazyFrame:
    """Ingest CSV as LazyFrame (lazy evaluation)."""
    return pl.scan_csv(path)


def read_excel(path: str | Path, sheet_id: int = 0) -> pl.DataFrame:
    """Ingest Excel as DataFrame (eager; no scan_excel in polars). Use .lazy() to chain lazily."""
    return pl.read_excel(path, sheet_id=sheet_id, engine="openpyxl")


def read_file(path: str | Path, sheet_id: int = 0) -> pl.LazyFrame:
    """Dispatch by suffix: CSV -> LazyFrame, Excel -> read then .lazy()."""
    p = Path(path)
    suffix = p.suffix.lower()
    if suffix == ".csv":
        return read_csv(p)
    if suffix in (".xlsx", ".xls"):
        return read_excel(p, sheet_id=sheet_id).lazy()
    raise ValueError(f"Unsupported file type: {suffix}")
