"""Inspect a single native FINO CSV/ASCII export in dry-run mode."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from om_pipeline.metocean.fino_export_inspector import (  # noqa: E402
    DEFAULT_OUTPUT_REPORT,
    inspect_fino_export,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Inspect a manually exported FINO CSV/ASCII file. This writes only a "
            "Markdown inspection report and never creates a processed FINO archive."
        )
    )
    parser.add_argument("--input", type=Path, required=True, help="Native FINO CSV/ASCII export to inspect.")
    parser.add_argument("--station", required=True, help="FINO station ID, for example FINO1.")
    parser.add_argument(
        "--output-report",
        type=Path,
        default=PROJECT_ROOT / DEFAULT_OUTPUT_REPORT,
        help="Markdown inspection report path.",
    )
    parser.add_argument(
        "--timestamp-column",
        default=None,
        help="Optional explicit timestamp column when auto-detection is ambiguous.",
    )
    parser.add_argument(
        "--delimiter",
        choices=["comma", "semicolon", "tab", "whitespace"],
        default=None,
        help="Optional delimiter override. Auto-detected when omitted.",
    )
    parser.add_argument(
        "--encoding",
        default="utf-8-sig",
        help="Input file encoding. Defaults to utf-8-sig to tolerate BOM markers.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        required=True,
        help="Required guardrail: inspect/report only, with no FINO import.",
    )
    args = parser.parse_args()

    result = inspect_fino_export(
        input_path=args.input,
        station_id=args.station,
        output_report=args.output_report,
        timestamp_column=args.timestamp_column,
        delimiter=args.delimiter,
        encoding=args.encoding,
        dry_run=args.dry_run,
    )

    print("FINO native export inspection complete.")
    print(f"Station: {result.station_id}")
    print(f"Rows: {result.row_count}")
    print(f"Columns: {len(result.columns)}")
    print(f"Inferred delimiter: {result.delimiter.label}")
    print(f"Timestamp range UTC: {result.timestamp.timestamp_start_utc} to {result.timestamp.timestamp_end_utc}")
    print(f"Inferred cadence: {result.timestamp.inferred_cadence}")
    print(f"10-minute cadence: {result.timestamp.ten_minute_cadence}")
    print(f"Duplicate timestamps: {result.timestamp.duplicate_timestamp_count}")
    print(f"Canonical mapping: {result.canonical_mapping}")
    print(f"QC/status columns: {result.qc_columns}")
    print(f"Safe for small import pilot: {result.safe_for_small_import_pilot}")
    print(f"Warnings: {len(result.warnings)}")
    print(f"Wrote report: {result.output_report}")
    print("Dry-run guardrail: no FINO import, source fusion, current download, or dwell-table rebuild was performed.")


if __name__ == "__main__":
    main()
