#!/usr/bin/env python3
"""Build the AIS receiver/source observability-bias audit."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from om_pipeline.analysis.ais_observability_bias import (  # noqa: E402
    build_ais_observability_bias_outputs,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build AIS receiver/source observability-bias audit outputs."
    )
    parser.add_argument(
        "--processed-output-dir",
        type=Path,
        default=PROJECT_ROOT / "Data" / "Processed" / "analysis" / "ais_observability_bias",
        help="Output directory for ignored processed observability-bias matrices.",
    )
    parser.add_argument(
        "--report-output-dir",
        type=Path,
        default=PROJECT_ROOT / "reports" / "evidence_readiness",
        help="Output directory for tracked observability-bias reports.",
    )
    parser.add_argument(
        "--methodology-path",
        type=Path,
        default=PROJECT_ROOT
        / "analysis"
        / "00_data_foundation"
        / "AIS_RECEIVER_DISTANCE_OBSERVABILITY_AUDIT.md",
        help="Methodology document path.",
    )
    parser.add_argument(
        "--external-receiver-reference-path",
        type=Path,
        default=None,
        help="Optional CSV with receiver_id, receiver_latitude, receiver_longitude.",
    )
    parser.add_argument(
        "--external-reference-provenance-path",
        type=Path,
        default=None,
        help="Required provenance CSV when external receiver/coastline references are used.",
    )
    args = parser.parse_args()

    outputs = build_ais_observability_bias_outputs(
        project_root=PROJECT_ROOT,
        processed_output_dir=args.processed_output_dir,
        report_output_dir=args.report_output_dir,
        methodology_path=args.methodology_path,
        manifest_path=PROJECT_ROOT
        / "Data"
        / "Processed"
        / "ais_dwell_backfill"
        / "logs"
        / "backfill_manifest.csv",
        dwell_path=PROJECT_ROOT
        / "Data"
        / "Processed"
        / "ais_dwell_backfill"
        / "cross_farm_dwell_weather_features.parquet",
        turbine_path=PROJECT_ROOT / "Data" / "Interim" / "European_Turbine_Coordinates.csv",
        farm_intensity_path=PROJECT_ROOT
        / "Data"
        / "Processed"
        / "analysis"
        / "rq9_intervention_intensity"
        / "farm_intervention_intensity.csv",
        turbine_intensity_path=PROJECT_ROOT
        / "Data"
        / "Processed"
        / "analysis"
        / "rq9_intervention_intensity"
        / "turbine_intervention_intensity_v1.csv",
        turbine_exposure_path=PROJECT_ROOT
        / "Data"
        / "Processed"
        / "analysis"
        / "rq9_intervention_intensity"
        / "turbine_exposure_denominator.csv",
        turbine_events_path=PROJECT_ROOT
        / "Data"
        / "Processed"
        / "analysis"
        / "rq9_intervention_intensity"
        / "turbine_intervention_events_v0.csv",
        raw_ais_root=PROJECT_ROOT / "Data" / "Raw" / "AIS",
        external_receiver_reference_path=args.external_receiver_reference_path,
        external_reference_provenance_path=args.external_reference_provenance_path,
    )

    print("AIS observability-bias audit complete.")
    print(f"Farm-month rows: {outputs.validation['farm_month_rows']}")
    print(f"Observed source months: {outputs.validation['observed_source_months']}")
    print(f"Missing source months: {outputs.validation['missing_source_months']}")
    print(f"Observed-zero months: {outputs.validation['observed_zero_months']}")
    print(
        "Per-message receiver-like schema fields: "
        f"{outputs.validation['direct_receiver_field_count']}"
    )
    print(f"Observed AIS base-station MMSIs: {outputs.validation['base_station_mmsi_count']}")
    print(f"Observed AIS base-station messages: {outputs.validation['base_station_record_count']}")
    print(
        "Base-station distance eligible strata: "
        f"{outputs.validation['base_station_distance_eligible_strata']}"
    )
    print(
        "Base-station distance bias-consistent strata: "
        f"{outputs.validation['base_station_distance_bias_consistent_strata']}"
    )
    print(f"Raw AIS files inspected: {outputs.validation['raw_ais_file_count']}")
    print(f"Report: {outputs.files['ais_receiver_distance_observability_report_md']}")
    print(f"Farm matrix: {outputs.files['farm_month_observability_bias_features_csv']}")
    print(f"Farm summary: {outputs.files['farm_observability_bias_summary_csv']}")
    print(f"Receiver inventory: {outputs.files['receiver_metadata_inventory_csv']}")
    print(f"Base-station catalogue: {outputs.files['ais_base_station_geometry_catalogue_csv']}")
    print(
        "Base-station distance strata: "
        f"{outputs.files['base_station_distance_stratum_diagnostic_csv']}"
    )
    print(
        "Base-station distance gradients: "
        f"{outputs.files['base_station_distance_gradient_summary_csv']}"
    )


if __name__ == "__main__":
    main()
