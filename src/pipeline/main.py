"""Orchestrate ingest -> transform -> load."""

import argparse
from pathlib import Path

from pipeline.config import get_settings
from pipeline.ingest import read_file
from pipeline.load import load
from pipeline.transform import transform


def run(
    input_path: str | Path,
    table_name: str = "pipeline_output",
    *,
    sheet_id: int = 0,
    if_table_exists: str = "replace",
) -> None:
    settings = get_settings()
    path = Path(settings.data_dir) / input_path if not Path(input_path).is_absolute() else Path(input_path)
    lf = read_file(path, sheet_id=sheet_id)
    lf = transform(lf)
    df = lf.collect()
    load(df, table_name=table_name, connection=settings.database_url, if_table_exists=if_table_exists)


def _main() -> None:
    parser = argparse.ArgumentParser(description="Run pipeline: generic ETL or source-specific (e.g. HSD Reference).")
    parser.add_argument("--source", default="generic", help="Source: generic | hsd_reference | zcta_gazetteer | county_boundaries | hsd_tables")
    parser.add_argument("--file", help="Input file path (relative to DATA_DIR for source-specific runs)")
    parser.add_argument("--year", type=int, help="Source year (required for hsd_reference, hsd_tables)")
    parser.add_argument("--type", dest="table_type", choices=("provider", "facility"), help="Table type for hsd_tables: provider | facility")
    parser.add_argument("--replace", action="store_true", default=True, help="Replace table if exists (default: True)")
    parser.add_argument("--append", action="store_true", help="Append to table instead of replace")
    args = parser.parse_args()

    if_table_exists = "append" if args.append else "replace"

    if args.source == "hsd_reference":
        if not args.file or args.year is None:
            parser.error("--source hsd_reference requires --file and --year")
        from pipeline.sources.hsd_reference.pipeline import run as run_hsd
        run_hsd(args.file, args.year, if_table_exists=if_table_exists)
    elif args.source == "zcta_gazetteer":
        if not args.file or args.year is None:
            parser.error("--source zcta_gazetteer requires --file and --year")
        from pipeline.sources.zcta_gazetteer.pipeline import run as run_zcta
        run_zcta(args.file, args.year, if_table_exists=if_table_exists)
    elif args.source == "county_boundaries":
        if not args.file or args.year is None:
            parser.error("--source county_boundaries requires --file and --year")
        from pipeline.sources.county_boundaries.pipeline import run as run_county
        run_county(args.file, args.year, if_table_exists=if_table_exists)
    elif args.source == "hsd_tables":
        if not args.file or args.year is None or not args.table_type:
            parser.error("--source hsd_tables requires --file, --year, and --type (provider|facility)")
        from pipeline.sources.hsd_tables.pipeline import run as run_hsd_tables
        run_hsd_tables(args.file, args.year, args.table_type, if_table_exists=if_table_exists)
    else:
        run(args.file or "example.csv", table_name="pipeline_output", if_table_exists=if_table_exists)


if __name__ == "__main__":
    _main()
