#!/usr/bin/env python
"""Build NWS current scale eligibility and Baltic hourly-current decision reports."""

from __future__ import annotations

import argparse
from pathlib import Path

from om_pipeline.metocean.current_scaling_preflight import (
    DEFAULT_BATHYMETRY,
    DEFAULT_DWELL_WEATHER,
    DEFAULT_FUSION_V1,
    DEFAULT_OUTPUT_DIR,
    DEFAULT_REPORT_DIR,
    DEFAULT_REQUIREMENTS,
    run_current_scaling_preflight,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--requirements", type=Path, default=DEFAULT_REQUIREMENTS)
    parser.add_argument("--dwell-weather", type=Path, default=DEFAULT_DWELL_WEATHER)
    parser.add_argument("--fusion-v1", type=Path, default=DEFAULT_FUSION_V1)
    parser.add_argument("--bathymetry", type=Path, default=DEFAULT_BATHYMETRY)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--report-dir", type=Path, default=DEFAULT_REPORT_DIR)
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite the eligibility table and reports if they already exist.",
    )
    parser.add_argument(
        "--no-overwrite",
        action="store_true",
        help="Fail if outputs already exist. This is the default and is kept for explicit audit commands.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    overwrite = bool(args.overwrite and not args.no_overwrite)
    result = run_current_scaling_preflight(
        requirements_path=args.requirements,
        dwell_weather_path=args.dwell_weather,
        fusion_v1_path=args.fusion_v1,
        bathymetry_path=args.bathymetry,
        output_dir=args.output_dir,
        report_dir=args.report_dir,
        overwrite=overwrite,
    )

    eligibility = result.eligibility
    recommended = eligibility[eligibility["recommended_for_scale"].eq("yes")]
    stress = eligibility[eligibility["recommended_for_scale"].eq("stress_test_only")]
    print(f"Eligibility path: {result.eligibility_path}")
    print(f"Baltic assessment path: {result.baltic_assessment_path}")
    print(f"NWS preflight report path: {result.preflight_report_path}")
    print(f"Rows evaluated: {len(eligibility)}")
    print(f"Recommended farm-years: {len(recommended)}")
    print(f"Stress-test farm-years: {len(stress)}")
    print(f"Recommended estimated rows: {int(recommended['estimated_current_rows'].sum()):,}")
    print(
        "Recommended estimated processed size MB: "
        f"{recommended['estimated_processed_size_mb'].sum():.1f}"
    )


if __name__ == "__main__":
    main()

