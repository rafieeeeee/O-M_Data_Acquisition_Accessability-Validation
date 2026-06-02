"""Build evidence-readiness matrices and reports from existing local data."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from om_pipeline.analysis.evidence_readiness import build_evidence_readiness_outputs  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build the data-integration evidence-readiness audit."
    )
    parser.add_argument(
        "--processed-output-dir",
        type=Path,
        default=PROJECT_ROOT / "Data" / "Processed" / "analysis" / "evidence_readiness",
        help="Output directory for processed readiness matrices.",
    )
    parser.add_argument(
        "--report-output-dir",
        type=Path,
        default=PROJECT_ROOT / "reports" / "evidence_readiness",
        help="Output directory for readiness reports.",
    )
    parser.add_argument(
        "--methodology-path",
        type=Path,
        default=PROJECT_ROOT
        / "analysis"
        / "00_data_foundation"
        / "EVIDENCE_READINESS_METHODOLOGY.md",
        help="Evidence-readiness methodology document path.",
    )
    args = parser.parse_args()

    outputs = build_evidence_readiness_outputs(
        project_root=PROJECT_ROOT,
        processed_output_dir=args.processed_output_dir,
        report_output_dir=args.report_output_dir,
        methodology_path=args.methodology_path,
        dwell_path=PROJECT_ROOT
        / "Data"
        / "Processed"
        / "ais_dwell_backfill"
        / "cross_farm_dwell_weather_features.parquet",
        manifest_path=PROJECT_ROOT
        / "Data"
        / "Processed"
        / "ais_dwell_backfill"
        / "logs"
        / "backfill_manifest.csv",
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
        fusion_v2_path=PROJECT_ROOT
        / "Data"
        / "Processed"
        / "metocean"
        / "fusion_v2"
        / "dwell_metocean_fusion_v2.parquet",
        bathymetry_path=PROJECT_ROOT
        / "Data"
        / "Processed"
        / "metocean"
        / "bathymetry"
        / "site_bathymetry_points.parquet",
        current_manifest_path=PROJECT_ROOT
        / "Data"
        / "Processed"
        / "metocean"
        / "nws_current_timeseries"
        / "manifest.csv",
        nws_wave_root=PROJECT_ROOT
        / "Data"
        / "Processed"
        / "metocean"
        / "nws_wave_timeseries",
        baltic_wave_root=PROJECT_ROOT
        / "Data"
        / "Processed"
        / "metocean"
        / "baltic_wave_timeseries",
        care_root=PROJECT_ROOT / "Data" / "CARE_To_Compare",
    )

    print("Evidence readiness audit complete.")
    print(f"Farm-month rows: {outputs.validation['farm_month_rows']}")
    print(f"Turbine-month rows: {outputs.validation['turbine_month_rows']}")
    print(f"Observed source months: {outputs.validation['observed_source_months']}")
    print(f"Missing source months: {outputs.validation['skipped_missing_source_months']}")
    print(f"Report: {outputs.files['data_limitations_report_md']}")
    print(f"Farm matrix: {outputs.files['farm_month_evidence_matrix_csv']}")
    print(f"Turbine matrix: {outputs.files['turbine_month_evidence_matrix_csv']}")
    print(f"RQ matrix: {outputs.files['rq_readiness_matrix_csv']}")


if __name__ == "__main__":
    main()
