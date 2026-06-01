"""Build RQ9 turbine characteristics intervention intensity comparison outputs."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from om_pipeline.analysis.rq9_turbine_characteristics import (  # noqa: E402
    ANALYSIS_LABEL,
    build_rq9_turbine_characteristics_outputs,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build RQ9 turbine characteristics intervention intensity comparison outputs."
    )
    parser.add_argument(
        "--turbine-intensity-input",
        type=Path,
        default=PROJECT_ROOT
        / "Data"
        / "Processed"
        / "analysis"
        / "rq9_intervention_intensity"
        / "turbine_intervention_intensity_v1.csv",
        help="Existing RQ9 turbine intervention intensity v1 output.",
    )
    parser.add_argument(
        "--turbine-denominator-input",
        type=Path,
        default=PROJECT_ROOT
        / "Data"
        / "Processed"
        / "analysis"
        / "rq9_intervention_intensity"
        / "turbine_exposure_denominator.csv",
        help="Existing RQ9 turbine exposure denominator output.",
    )
    parser.add_argument(
        "--turbine-events-input",
        type=Path,
        default=PROJECT_ROOT
        / "Data"
        / "Processed"
        / "analysis"
        / "rq9_intervention_intensity"
        / "turbine_intervention_events_v0.csv",
        help="Existing RQ9 turbine-assigned Tier A event output.",
    )
    parser.add_argument(
        "--turbine-coordinates-input",
        type=Path,
        default=PROJECT_ROOT / "Data" / "Interim" / "European_Turbine_Coordinates.csv",
        help="Existing turbine coordinate metadata input.",
    )
    parser.add_argument(
        "--processed-output-dir",
        type=Path,
        default=PROJECT_ROOT
        / "Data"
        / "Processed"
        / "analysis"
        / "rq9_intervention_intensity",
        help="Derived RQ9 turbine output directory.",
    )
    parser.add_argument(
        "--report-output-dir",
        type=Path,
        default=PROJECT_ROOT / "reports" / "rq9_intervention_intensity",
        help="RQ9 report output directory.",
    )
    args = parser.parse_args()

    outputs = build_rq9_turbine_characteristics_outputs(
        turbine_intensity_path=args.turbine_intensity_input,
        turbine_denominator_path=args.turbine_denominator_input,
        turbine_events_path=args.turbine_events_input,
        turbine_coordinates_path=args.turbine_coordinates_input,
        processed_output_dir=args.processed_output_dir,
        report_output_dir=args.report_output_dir,
    )
    validation = outputs.validation

    print(ANALYSIS_LABEL)
    print("This is maintenance intervention intensity, not confirmed fault evidence.")
    print(f"Turbines: {validation['turbine_rows']}")
    print(f"Observed steady turbine-years: {validation['observed_steady_years_total']:.3f}")
    print(f"High-confidence steady events: {validation['high_confidence_event_count']}")
    print(f"High+medium steady events: {validation['high_medium_event_count']}")
    print(f"Rates rows: {validation['rates_rows']}")
    print(f"Comparison rows: {validation['comparison_rows']}")
    print(f"Rates output: {outputs.files['turbine_characteristics_rates_csv']}")
    print(f"Comparison output: {outputs.files['turbine_characteristics_comparison_csv']}")
    print(f"Report: {outputs.files['turbine_characteristics_report_md']}")


if __name__ == "__main__":
    main()
