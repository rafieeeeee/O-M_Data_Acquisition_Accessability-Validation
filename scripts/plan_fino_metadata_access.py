"""Plan FINO metadata access in dry-run mode only."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from om_pipeline.metocean.fino_metadata_planner import (  # noqa: E402
    DEFAULT_BATHYMETRY_POINTS,
    DEFAULT_OUTPUT_REPORT,
    DEFAULT_PROCESSED_FINO_ROOT,
    DEFAULT_RAW_METOCEAN_ROOT,
    DEFAULT_REQUIREMENTS_PATH,
    plan_fino_metadata_access,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Dry-run FINO station metadata and access planning. This command writes "
            "only a Markdown plan and never downloads or imports FINO time series."
        )
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=PROJECT_ROOT / DEFAULT_OUTPUT_REPORT,
        help="Dry-run Markdown report path.",
    )
    parser.add_argument(
        "--requirements",
        type=Path,
        default=PROJECT_ROOT / DEFAULT_REQUIREMENTS_PATH,
        help="Common metocean requirements CSV.",
    )
    parser.add_argument(
        "--bathymetry-points",
        type=Path,
        default=PROJECT_ROOT / DEFAULT_BATHYMETRY_POINTS,
        help="Accepted bathymetry point table used for station-to-farm matching.",
    )
    parser.add_argument(
        "--raw-metocean-root",
        type=Path,
        default=PROJECT_ROOT / DEFAULT_RAW_METOCEAN_ROOT,
        help="Raw metocean root to inspect for local FINO placeholders.",
    )
    parser.add_argument(
        "--processed-fino-root",
        type=Path,
        default=PROJECT_ROOT / DEFAULT_PROCESSED_FINO_ROOT,
        help="Future processed FINO archive root to describe.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        required=True,
        help="Required guardrail: plan only, with no FINO data import.",
    )
    args = parser.parse_args()

    result = plan_fino_metadata_access(
        output_report=args.output,
        requirements_path=args.requirements,
        bathymetry_points_path=args.bathymetry_points,
        raw_metocean_root=args.raw_metocean_root,
        processed_fino_root=args.processed_fino_root,
        dry_run=args.dry_run,
    )

    summary = result.summary
    print("FINO metadata/access planning dry-run complete.")
    print(f"Stations: {summary['station_count']} ({summary['station_ids']})")
    print(f"Farm count: {summary['farm_count']}")
    print(f"Sample points used for matching: {summary['sample_point_count']}")
    print(f"Sample point source: {summary['sample_point_source']}")
    print(f"Processed FINO archive exists: {summary['processed_fino_archive_exists']}")
    print(f"Nearby candidates within 25 km: {summary['nearest_candidate_counts_within_25km']}")
    print(f"Wrote report: {result.output_report}")
    print("Dry-run guardrail: no FINO download, import, current download, source fusion, or dwell-table rebuild was performed.")


if __name__ == "__main__":
    main()
