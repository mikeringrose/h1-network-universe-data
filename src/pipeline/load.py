"""Write polars DataFrame to PostgreSQL (ADBC driver, no pandas)."""

import polars as pl


def load(df: pl.DataFrame, table_name: str, connection: str, if_table_exists: str = "replace") -> None:
    """Write DataFrame to Postgres via ADBC connection string."""
    df.write_database(
        table_name=table_name,
        connection=connection,
        engine="adbc",
        if_table_exists=if_table_exists,
    )
