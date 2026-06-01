"""Materialize or dry-run Baltic Copernicus wave time series."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from om_pipeline.metocean.baltic_wave_materializer import (  # noqa: E402
    DEFAULT_OUTPUT_ROOT,
    DEFAULT_QA_REPORT,
    DEFAULT_RAW_ROOT,
    DEFAULT_TURBINE_COORDINATES,
    materialize_baltic_wave_timeseries,
    normalize_farm_name,
)


def _parse_farms(values: list[str] | None) -> set[str] | None:
    if not values:
        return None
    parsed: set[str] = set()
    for value in values:
        parsed.update(item.strip() for item in value.split(",") if item.strip())
    return {normalize_farm_name(value) for value in parsed} or None


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Build a source-labelled Baltic wave continuous archive from reviewed "
            "raw NetCDF subsets. Use --dry-run before any materialization."
        )
    )
    parser.add_argument(
        "--raw-root",
        type=Path,
        default=PROJECT_ROOT / DEFAULT_RAW_ROOT,
        help="Root containing per-farm Baltic raw NetCDF subset directories.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=PROJECT_ROOT / DEFAULT_OUTPUT_ROOT,
        help="Partitioned Baltic wave output directory.",
    )
    parser.add_argument(
        "--qa-report",
        type=Path,
        default=PROJECT_ROOT / DEFAULT_QA_REPORT,
        help="Markdown QA report path.",
    )
    parser.add_argument(
        "--turbine-coordinates",
        type=Path,
        default=PROJECT_ROOT / DEFAULT_TURBINE_COORDINATES,
        help="European turbine coordinate table used for sample points.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Inspect raw files and expected outputs without writing parquet partitions.",
    )
    parser.add_argument(
        "--limit-farms",
        type=int,
        default=None,
        help="Limit processing to the N smallest expected-row farm plans.",
    )
    parser.add_argument(
        "--farms",
        nargs="+",
        default=None,
        help="Optional farm-name filters, comma-separated or space-separated.",
    )
    parser.add_argument(
        "--max-spatial-distance-km",
        type=float,
        default=25.0,
        help="Maximum nearest valid grid distance for materialization.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite selected existing partitions. Default preserves existing output.",
    )
    args = parser.parse_args()

    result = materialize_baltic_wave_timeseries(
        raw_root=args.raw_root,
        output_root=args.output_dir,
        turbine_coordinates_path=args.turbine_coordinates,
        qa_report=args.qa_report,
        dry_run=args.dry_run,
        limit_farms=args.limit_farms,
        farms=_parse_farms(args.farms),
        overwrite=args.overwrite,
        max_spatial_distance_km=args.max_spatial_distance_km,
    )

    display_columns = [
        "wind_farm",
        "raw_farm_dir",
        "time_start_utc",
        "time_end_utc",
        "time_count",
        "sample_point_count",
        "expected_partitions",
        "expected_rows",
        "status",
    ]
    available = [column for column in display_columns if column in result.plan.columns]
    print(result.plan[available].to_string(index=False))
    if result.qa_report is not None:
        print(f"Wrote QA report: {result.qa_report}")
    if args.dry_run:
        print("Dry-run complete; no Baltic parquet partitions were written.")
    else:
        print(result.qa.to_string(index=False))
        print("Baltic wave materialization complete; no current or fused variables were produced.")


if __name__ == "__main__":
    main()
