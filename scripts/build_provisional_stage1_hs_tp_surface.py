"""Build the provisional Stage 1 Hs/Tp observed operational envelope."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from om_pipeline.analysis.provisional_stage1_hs_tp import (  # noqa: E402
    PROVISIONAL_STAGE1_LABEL,
    build_provisional_stage1_outputs,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Build the dedicated provisional NORA3-derived Tier A Hs/Tp "
            "observed-envelope outputs."
        )
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=PROJECT_ROOT
        / "Data"
        / "Processed"
        / "ais_dwell_backfill"
        / "cross_farm_dwell_weather_features.parquet",
        help="Weather-joined dwell feature parquet.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=PROJECT_ROOT / "reports" / "stage1_hs_tp_provisional",
        help="Report/figure output directory.",
    )
    parser.add_argument(
        "--processed-output-dir",
        type=Path,
        default=PROJECT_ROOT
        / "Data"
        / "Processed"
        / "analysis"
        / "stage1_hs_tp_provisional",
        help="Clean modelling-input output directory.",
    )
    args = parser.parse_args()

    outputs = build_provisional_stage1_outputs(
        input_path=args.input,
        report_output_dir=args.output_dir,
        processed_output_dir=args.processed_output_dir,
    )
    primary = outputs.validation["primary_summary"]
    sensitivity = outputs.validation["sensitivity_summary"]
    print(PROVISIONAL_STAGE1_LABEL)
    print(f"Input rows: {outputs.validation['input_rows']}")
    print(f"Primary Tier A Hs/Tp rows: {primary['rows']}")
    print(f"Sensitivity all-tier Hs/Tp rows: {sensitivity['rows']}")
    print(f"Primary farms: {primary['farm_count']}")
    print(f"Primary MMSIs: {primary['mmsi_count']}")
    print(f"Report directory: {outputs.report_output_dir}")
    print(f"Processed output directory: {outputs.processed_output_dir}")


if __name__ == "__main__":
    main()
