"""Build RQ9 turbine-level maintenance intervention feasibility v0 outputs."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from om_pipeline.analysis.rq9_intervention_intensity import (  # noqa: E402
    DEFAULT_RAMP_UP_MONTHS,
)
from om_pipeline.analysis.rq9_turbine_feasibility import (  # noqa: E402
    ANALYSIS_LABEL,
    build_rq9_turbine_feasibility_outputs,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build turbine-level RQ9 maintenance intervention feasibility outputs."
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
        "--turbine-input",
        type=Path,
        default=PROJECT_ROOT / "Data" / "Interim" / "European_Turbine_Coordinates.csv",
        help="Existing turbine coordinate table.",
    )
    parser.add_argument(
        "--farm-intensity-input",
        type=Path,
        default=PROJECT_ROOT
        / "Data"
        / "Processed"
        / "analysis"
        / "rq9_intervention_intensity"
        / "farm_intervention_intensity.csv",
        help="Existing farm-level RQ9 intervention intensity output.",
    )
    parser.add_argument(
        "--processed-output-dir",
        type=Path,
        default=PROJECT_ROOT
        / "Data"
        / "Processed"
        / "analysis"
        / "rq9_intervention_intensity",
        help="Derived turbine feasibility output directory.",
    )
    parser.add_argument(
        "--report-output-dir",
        type=Path,
        default=PROJECT_ROOT / "reports" / "rq9_intervention_intensity",
        help="RQ9 report output directory.",
    )
    parser.add_argument(
        "--ramp-up-months",
        type=int,
        default=DEFAULT_RAMP_UP_MONTHS,
        help="Ramp-up months used to derive lifecycle phases for Tier A events.",
    )
    args = parser.parse_args()

    outputs = build_rq9_turbine_feasibility_outputs(
        dwell_path=args.dwell_input,
        turbine_path=args.turbine_input,
        farm_intensity_path=args.farm_intensity_input,
        processed_output_dir=args.processed_output_dir,
        report_output_dir=args.report_output_dir,
        ramp_up_months=args.ramp_up_months,
    )
    validation = outputs.validation

    print(ANALYSIS_LABEL)
    print("This is intervention feasibility, not failure rate.")
    print(f"Tier A event rows inspected: {validation['tier_a_event_rows']}")
    print(f"Assigned within 500 m: {validation['assigned_event_rows']}")
    print(f"High-confidence assignments: {validation['high_confidence_event_rows']}")
    print(f"Medium-confidence assignments: {validation['medium_confidence_event_rows']}")
    print(f"Unassigned events: {validation['unassigned_event_rows']}")
    print(
        "Duplicate-adjusted Tier A event weight total: "
        f"{validation['duplicate_adjusted_event_weight_total']:.3f}"
    )
    print(
        "Metadata completeness: "
        f"{validation['metadata_complete_field_count']}/"
        f"{validation['metadata_field_count']} fields complete"
    )
    print(f"Turbine event output: {outputs.files['turbine_intervention_events_v0_csv']}")
    print(f"Metadata completeness: {outputs.files['turbine_metadata_completeness_csv']}")
    print(f"Feasibility report: {outputs.files['turbine_level_feasibility_report_md']}")


if __name__ == "__main__":
    main()
