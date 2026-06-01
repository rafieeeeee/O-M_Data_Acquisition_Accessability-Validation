"""Build Metocean Fusion v0: source-resolved waves plus bathymetry."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from om_pipeline.metocean.metocean_fusion_v0 import (  # noqa: E402
    DEFAULT_BALTIC_ROOT,
    DEFAULT_BATHYMETRY_POINTS,
    DEFAULT_DWELL_WEATHER_INPUT,
    DEFAULT_NWS_ROOT,
    DEFAULT_OUTPUT_PATH,
    DEFAULT_REPORT_PATH,
    build_metocean_fusion_v0,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Build the Fusion v0 research artifact from accepted local wave and "
            "bathymetry sources. This does not download currents, import FINO, "
            "mutate source archives, rerun NORA3, or rebuild the final production table."
        )
    )
    parser.add_argument(
        "--dwell-weather-input",
        type=Path,
        default=PROJECT_ROOT / DEFAULT_DWELL_WEATHER_INPUT,
        help="Current NORA3-derived dwell-weather parquet table.",
    )
    parser.add_argument(
        "--nws-root",
        type=Path,
        default=PROJECT_ROOT / DEFAULT_NWS_ROOT,
        help="Processed NWS wave archive root.",
    )
    parser.add_argument(
        "--baltic-root",
        type=Path,
        default=PROJECT_ROOT / DEFAULT_BALTIC_ROOT,
        help="Processed Baltic wave archive root.",
    )
    parser.add_argument(
        "--bathymetry-points",
        type=Path,
        default=PROJECT_ROOT / DEFAULT_BATHYMETRY_POINTS,
        help="Accepted bathymetry point archive.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=PROJECT_ROOT / DEFAULT_OUTPUT_PATH,
        help="Fusion v0 output parquet path.",
    )
    parser.add_argument(
        "--report",
        type=Path,
        default=PROJECT_ROOT / DEFAULT_REPORT_PATH,
        help="Fusion v0 validation report path.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Allow replacing an existing Fusion v0 output/report. Defaults to preserve existing artifacts.",
    )
    args = parser.parse_args()

    result = build_metocean_fusion_v0(
        dwell_weather_input=args.dwell_weather_input,
        nws_root=args.nws_root,
        baltic_root=args.baltic_root,
        bathymetry_points=args.bathymetry_points,
        output_path=args.output,
        report_path=args.report,
        overwrite=args.overwrite,
    )

    coverage = result.validation["coverage_comparison"]
    print("Metocean Fusion v0 build complete.")
    print(f"Output table: {result.output_path}")
    print(f"Validation report: {result.report_path}")
    print(f"Input rows: {result.validation['input_row_count']}")
    print(f"Output rows: {result.validation['output_row_count']}")
    print(f"Row count preserved: {result.validation['row_count_preserved']}")
    print(f"NORA3 Hs/Tp rows: {coverage['nora3_hs_tp_rows']}")
    print(f"Fusion Hs/Tp rows: {coverage['fusion_hs_tp_rows']}")
    print(f"Absolute coverage gain rows: {coverage['absolute_gain_rows']}")
    print(f"Percentage gain vs NORA3: {coverage['percentage_gain_vs_nora3']:.2f}%")
    print("Guardrail: no current download, FINO import, source archive mutation, NORA3 rerun, or final production rebuild was performed.")


if __name__ == "__main__":
    main()
