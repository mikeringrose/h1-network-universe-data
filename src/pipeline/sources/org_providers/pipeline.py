"""Orchestrate ingest -> transform -> load for org_providers."""

from __future__ import annotations

import logging
from pathlib import Path

import psycopg2

from pipeline.sources.hsd_tables.ingest import read
from pipeline.sources.org_providers.transform import transform
from pipeline.sources.org_providers.load import load

logger = logging.getLogger(__name__)


def run(
    file_path: str | Path,
    organization_id: str,
    source_file_id: str,
    api_database_url: str,
) -> None:
    """Read a PROVIDER upload file and write rows to org_providers."""
    df = read(file_path, "org_provider")
    out = transform(df, organization_id=organization_id, source_file_id=source_file_id)
    conn = psycopg2.connect(api_database_url)
    conn.autocommit = False
    try:
        n = load(conn, out, source_file_id)
        logger.info("org_providers: wrote %d rows for source_file_id=%s", n, source_file_id)
    finally:
        conn.close()
