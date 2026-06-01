"""Run Current Pilot v1 true u/v current candidate extraction."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from om_pipeline.metocean.current_pilot_v1 import (  # noqa: E402
    DEFAULT_DWELL_WEATHER_INPUT,
    DEFAULT_OUTPUT_DIR,
    DEFAULT_REPORT_DIR,
    build_current_pilot_v1,
)


DEFAULT_FARM_BY_PILOT = {
    "baltic": "Wikinger",
    "nws": "Borkum_Riffgrund_2",
}


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Build one-farm/year Current Pilot v1 candidate rows from true "
            "Eulerian u/v current sources. This command never uses legacy "
            "CMEMS fallback CSVs or simulated currents."
        )
    )
    parser.add_argument("--pilot", choices=["baltic", "nws"], required=True)
    parser.add_argument("--farm", default=None, help="Wind farm name or partition slug.")
    parser.add_argument("--year", type=int, default=2020)
    parser.add_argument(
        "--dwell-weather",
        type=Path,
        default=PROJECT_ROOT / DEFAULT_DWELL_WEATHER_INPUT,
        help="Dwell-weather parquet used for event-scale suitability checks.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=PROJECT_ROOT / DEFAULT_OUTPUT_DIR,
        help="Directory for pilot candidate parquet outputs.",
    )
    parser.add_argument(
        "--report-dir",
        type=Path,
        default=PROJECT_ROOT / DEFAULT_REPORT_DIR,
        help="Directory for product assessment and validation reports.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Write blocked audit candidate rows and reports without opening current products.",
    )
    parser.add_argument(
        "--no-overwrite",
        action="store_true",
        help="Preserve an existing pilot candidate table. This is the default behavior.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Replace an existing pilot candidate table.",
    )
    args = parser.parse_args()

    farm = args.farm or DEFAULT_FARM_BY_PILOT[args.pilot]
    overwrite = bool(args.overwrite and not args.no_overwrite)
    result = build_current_pilot_v1(
        pilot=args.pilot,
        farm=farm,
        year=args.year,
        dwell_weather=args.dwell_weather,
        output_dir=args.output_dir,
        report_dir=args.report_dir,
        dry_run=args.dry_run,
        overwrite=overwrite,
    )

    print("Current Pilot v1 complete.")
    print(f"Pilot: {result.pilot}")
    print(f"Candidate table: {result.candidate_path}")
    print(f"Product assessment: {result.product_assessment_path}")
    print(f"Validation report: {result.validation_report_path}")
    print(f"Rows: {result.validation['row_count']}")
    print(f"Valid u/v rows: {result.validation['valid_uv_row_count']}")
    print(f"Status: {'ran' if result.validation['ran'] else 'blocked'}")
    print(f"Confidence: {result.validation['current_confidence_class']}")
    print(
        "Guardrail: no broad current download, final archive build, dwell-metocean "
        "rebuild, FINO import, legacy fallback CSV promotion, or simulated current "
        "generation was performed."
    )


if __name__ == "__main__":
    main()

