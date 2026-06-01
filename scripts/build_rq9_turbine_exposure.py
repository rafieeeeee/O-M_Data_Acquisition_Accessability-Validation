"""Build RQ9 turbine exposure denominator and intervention intensity v1 outputs."""

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
from om_pipeline.analysis.rq9_turbine_exposure import (  # noqa: E402
    ANALYSIS_LABEL,
    build_rq9_turbine_exposure_outputs,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build RQ9 turbine-level maintenance intervention intensity v1 outputs."
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
        "--turbine-input",
        type=Path,
        default=PROJECT_ROOT / "Data" / "Interim" / "European_Turbine_Coordinates.csv",
        help="Existing turbine coordinate table.",
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
        help="Existing AIS backfill manifest.",
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
        "--bathymetry-input",
        type=Path,
        default=PROJECT_ROOT
        / "Data"
        / "Processed"
        / "metocean"
        / "bathymetry"
        / "site_bathymetry_points.parquet",
        help="Optional existing processed bathymetry points. No bathymetry extraction is run.",
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
    parser.add_argument(
        "--ramp-up-months",
        type=int,
        default=DEFAULT_RAMP_UP_MONTHS,
        help="Ramp-up months excluded from steady-operational turbine denominators.",
    )
    args = parser.parse_args()

    outputs = build_rq9_turbine_exposure_outputs(
        turbine_events_path=args.turbine_events_input,
        turbine_path=args.turbine_input,
        manifest_path=args.manifest_input,
        farm_intensity_path=args.farm_intensity_input,
        processed_output_dir=args.processed_output_dir,
        report_output_dir=args.report_output_dir,
        bathymetry_path=args.bathymetry_input,
        ramp_up_months=args.ramp_up_months,
    )
    validation = outputs.validation

    print(ANALYSIS_LABEL)
    print("This is maintenance intervention intensity, not confirmed fault evidence.")
    print(f"Turbines: {validation['turbine_rows']}")
    print(f"Farms: {validation['turbine_farm_count']}")
    print(f"Observed steady turbine-years: {validation['observed_steady_years_total']:.3f}")
    print(f"High-confidence steady events: {validation['high_confidence_steady_event_count']}")
    print(f"High+medium steady events: {validation['high_medium_steady_event_count']}")
    print(
        "Bathymetry matches: "
        f"{validation['bathymetry_matched_turbines']}/{validation['turbine_rows']}"
    )
    print(f"Denominator output: {outputs.files['turbine_exposure_denominator_csv']}")
    print(f"Intensity output: {outputs.files['turbine_intervention_intensity_v1_csv']}")
    print(f"Exposure comparison: {outputs.files['turbine_exposure_comparison_csv']}")
    print(f"Report: {outputs.files['turbine_exposure_intervention_report_md']}")


if __name__ == "__main__":
    main()
