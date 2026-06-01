"""Assign EMODnet bathymetry to common metocean sample points."""

from __future__ import annotations

import argparse
import shlex
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from om_pipeline.metocean.bathymetry_assignment import (  # noqa: E402
    DEFAULT_OUTPUT_ROOT,
    DEFAULT_QA_REPORT,
    DEFAULT_REQUIREMENTS_PATH,
    DEFAULT_SOURCE_ROOT,
    DEFAULT_TURBINE_COORDINATES,
    assign_bathymetry_to_metocean_points,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Acquire EMODnet bathymetry point samples and write the static "
            "bathymetry point table for common metocean sample points."
        )
    )
    parser.add_argument(
        "--requirements",
        type=Path,
        default=PROJECT_ROOT / DEFAULT_REQUIREMENTS_PATH,
        help="Common metocean farm requirements CSV or parquet.",
    )
    parser.add_argument(
        "--turbine-coordinates",
        type=Path,
        default=PROJECT_ROOT / DEFAULT_TURBINE_COORDINATES,
        help="European turbine coordinates table used to expand sample points.",
    )
    parser.add_argument(
        "--source-root",
        type=Path,
        default=PROJECT_ROOT / DEFAULT_SOURCE_ROOT,
        help="Raw/cache bathymetry source root.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=PROJECT_ROOT / DEFAULT_OUTPUT_ROOT,
        help="Processed bathymetry output directory.",
    )
    parser.add_argument(
        "--qa-report",
        type=Path,
        default=PROJECT_ROOT / DEFAULT_QA_REPORT,
        help="Markdown QA report path.",
    )
    parser.add_argument(
        "--limit-scope",
        default="common-requirements",
        choices=["common-requirements"],
        help="Assignment scope. Only common-requirements is supported.",
    )
    parser.add_argument(
        "--primary-source",
        default="emodnet",
        choices=["emodnet"],
        help="Primary bathymetry source.",
    )
    parser.add_argument(
        "--fallback-source",
        default="gebco_2026",
        choices=["gebco_2026"],
        help="Planned fallback/cross-check source.",
    )
    parser.add_argument(
        "--no-overwrite",
        action="store_true",
        help="Abort if processed bathymetry output already exists.",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=8,
        help="Concurrent EMODnet point-sample requests.",
    )
    args = parser.parse_args()

    command = " ".join(shlex.quote(part) for part in sys.argv)

    def progress(message: str) -> None:
        print(message, flush=True)

    result = assign_bathymetry_to_metocean_points(
        requirements_path=args.requirements,
        turbine_coordinates_path=args.turbine_coordinates,
        source_root=args.source_root,
        output_root=args.output_dir,
        qa_report=args.qa_report,
        primary_source=args.primary_source,
        fallback_source=args.fallback_source,
        no_overwrite=args.no_overwrite,
        limit_scope=args.limit_scope,
        max_workers=args.max_workers,
        progress_callback=progress,
        command=command,
    )

    qa = result.qa
    print("Bathymetry assignment complete.")
    print(f"Rows: {qa['row_count']}")
    print(f"Farms: {qa['farm_count']}")
    print(f"Missing depths: {qa['missing_depth_count']} ({qa['missing_depth_rate']:.6f})")
    print(f"Duplicate farm/sample rows: {qa['duplicate_wind_farm_sample_point_count']}")
    print(f"Fallback rows: {qa['fallback_row_count']}")
    print(f"Processed output: {result.output_path}")
    print(f"Metadata: {result.processed_metadata_path}")
    print(f"Source cache: {result.source_cache_path}")
    if result.qa_report is not None:
        print(f"QA report: {result.qa_report}")


if __name__ == "__main__":
    main()
