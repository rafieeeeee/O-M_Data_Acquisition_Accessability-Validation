"""Plan static bathymetry assignment in dry-run mode only."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from om_pipeline.metocean.bathymetry_planner import (  # noqa: E402
    DEFAULT_OUTPUT_ROOT,
    DEFAULT_QA_REPORT,
    DEFAULT_REQUIREMENTS_PATH,
    plan_bathymetry_assignment,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Dry-run the static bathymetry assignment plan. This command writes "
            "only a QA report and never downloads rasters or writes final "
            "bathymetry point archives."
        )
    )
    parser.add_argument(
        "--requirements",
        type=Path,
        default=PROJECT_ROOT / DEFAULT_REQUIREMENTS_PATH,
        help="Common metocean farm requirements CSV or parquet.",
    )
    parser.add_argument(
        "--primary-source",
        default="emodnet",
        help="Planned primary bathymetry source. Supported: emodnet.",
    )
    parser.add_argument(
        "--fallback-source",
        default="gebco_2026",
        help="Planned fallback bathymetry source. Supported: gebco_2026.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=PROJECT_ROOT / DEFAULT_OUTPUT_ROOT,
        help="Future bathymetry output directory to describe in the report.",
    )
    parser.add_argument(
        "--qa-report",
        type=Path,
        default=PROJECT_ROOT / DEFAULT_QA_REPORT,
        help="Dry-run markdown report path.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        required=True,
        help="Required guardrail: plan only, with no final output writes.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Record overwrite intent in the plan. Dry-run still writes no final output.",
    )
    args = parser.parse_args()

    result = plan_bathymetry_assignment(
        requirements_path=args.requirements,
        primary_source=args.primary_source,
        fallback_source=args.fallback_source,
        output_root=args.output_dir,
        qa_report=args.qa_report,
        dry_run=args.dry_run,
        overwrite=args.overwrite,
    )

    summary = result.summary
    print("Bathymetry assignment planning dry-run complete.")
    print(f"Requirements rows: {summary['input_requirements_row_count']}")
    print(f"Farms: {summary['farm_count']}")
    print(f"Sample points: {summary['sample_point_count']}")
    print(f"Coordinate bounds: lon {summary['min_lon']} to {summary['max_lon']}; lat {summary['min_lat']} to {summary['max_lat']}")
    print(f"Primary source: {summary['primary_source']}")
    print(f"Fallback source: {summary['fallback_source']}")
    print(f"Expected output path: {summary['expected_output_path']}")
    print(f"Estimated final point table MB: {summary['estimated_final_point_table_mb']}")
    print(f"Estimated degree tiles: {summary['estimated_degree_tile_count']}")
    if result.qa_report is not None:
        print(f"Wrote QA report: {result.qa_report}")
    print("Dry-run guardrail: no raster, tile, parquet, output directory, current, FINO, or fused metocean writes were performed.")


if __name__ == "__main__":
    main()
