"""Build RQ9 farm-level maintenance intervention intensity outputs."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from om_pipeline.analysis.rq9_intervention_intensity import (  # noqa: E402
    ANALYSIS_LABEL,
    DEFAULT_LONG_DWELL_THRESHOLD_MIN,
    build_rq9_farm_outputs,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build farm-level RQ9 maintenance intervention intensity outputs."
    )
    parser.add_argument(
        "--dwell-input",
        type=Path,
        default=PROJECT_ROOT
        / "Data"
        / "Processed"
        / "ais_dwell_backfill"
        / "cross_farm_dwell_weather_features.parquet",
        help="Existing weather-joined AIS dwell feature parquet.",
    )
    parser.add_argument(
        "--manifest-input",
        type=Path,
        default=PROJECT_ROOT
        / "Data"
        / "Processed"
        / "ais_dwell_backfill"
        / "logs"
        / "backfill_manifest.csv",
        help="Existing AIS dwell backfill manifest.",
    )
    parser.add_argument(
        "--turbine-input",
        type=Path,
        default=PROJECT_ROOT / "Data" / "Interim" / "European_Turbine_Coordinates.csv",
        help="Existing turbine coordinate table used for farm-level turbine counts.",
    )
    parser.add_argument(
        "--processed-output-dir",
        type=Path,
        default=PROJECT_ROOT
        / "Data"
        / "Processed"
        / "analysis"
        / "rq9_intervention_intensity",
        help="Derived farm-level intensity output directory.",
    )
    parser.add_argument(
        "--report-output-dir",
        type=Path,
        default=PROJECT_ROOT / "reports" / "rq9_intervention_intensity",
        help="RQ9 report output directory.",
    )
    parser.add_argument(
        "--long-dwell-threshold-min",
        type=float,
        default=DEFAULT_LONG_DWELL_THRESHOLD_MIN,
        help="Duration threshold for long Tier A/B candidate interventions.",
    )
    args = parser.parse_args()

    outputs = build_rq9_farm_outputs(
        dwell_path=args.dwell_input,
        manifest_path=args.manifest_input,
        turbine_path=args.turbine_input,
        processed_output_dir=args.processed_output_dir,
        report_output_dir=args.report_output_dir,
        long_dwell_threshold_min=args.long_dwell_threshold_min,
    )

    validation = outputs.validation
    print(ANALYSIS_LABEL)
    print("This is intervention intensity, not failure rate.")
    print(f"Farm rows: {validation['farm_output_rows']}")
    print(f"Observed farm-years: {validation['observed_years_total']:.3f}")
    print(f"Candidate intervention count: {validation['candidate_intervention_count_total']}")
    print(f"Tier A count: {validation['tier_a_visit_count_total']}")
    print(f"Tier B count: {validation['tier_b_visit_count_total']}")
    print(f"Long dwell count: {validation['long_dwell_count_total']}")
    print(f"Farm output: {outputs.files['farm_intervention_intensity_csv']}")
    print(f"Validation summary: {outputs.files['validation_summary_csv']}")
    print(f"Methodology report: {outputs.files['methodology_report_md']}")


if __name__ == "__main__":
    main()
