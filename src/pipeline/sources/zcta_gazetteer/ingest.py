"""Ingest Census ZCTA Gazetteer file (tab- or pipe-delimited)."""

from pathlib import Path

import polars as pl

from pipeline.config import get_settings


def read(path: str | Path) -> pl.DataFrame:
    """Read ZCTA gazetteer file. Path resolved relative to DATA_DIR if not absolute.
    Delimiter auto-detected from first line: tab or pipe (Census 2025 uses pipe)."""
    settings = get_settings()
    resolved = Path(path) if Path(path).is_absolute() else Path(settings.data_dir) / path
    if not resolved.exists():
        raise FileNotFoundError(resolved)
    first_line = resolved.read_text(encoding="utf-8", errors="replace").splitlines()[0]
    sep = "|" if "|" in first_line and first_line.count("|") > 1 else "\t"
    return pl.read_csv(resolved, separator=sep, infer_schema_length=0)
