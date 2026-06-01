"""Build Metocean Fusion v1 wave-source agreement and confidence artifacts."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from om_pipeline.metocean.metocean_fusion_v1_source_agreement import (  # noqa: E402
    DEFAULT_BALTIC_ROOT,
    DEFAULT_BATHYMETRY_POINTS,
    DEFAULT_DWELL_WEATHER_INPUT,
    DEFAULT_FUSION_V0_INPUT,
    DEFAULT_NWS_ROOT,
    DEFAULT_OUTPUT_DIR,
    DEFAULT_REPORT_DIR,
    build_metocean_fusion_v1_source_agreement,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Build Fusion v1 wave-source agreement and confidence artifacts from "
            "existing NORA3, NWS, Baltic, and bathymetry sources. This does not "
            "download currents, import FINO, mutate archives, rerun NORA3, or "
            "rebuild the final production dwell-metocean table."
        )
    )
    parser.add_argument(
        "--dwell-weather",
        type=Path,
        default=PROJECT_ROOT / DEFAULT_DWELL_WEATHER_INPUT,
        help="Current dwell-weather parquet table with NORA3 active event fields.",
    )
    parser.add_argument(
        "--fusion-v0",
        type=Path,
        default=PROJECT_ROOT / DEFAULT_FUSION_V0_INPUT,
        help="Fusion v0 output parquet for comparison only.",
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
        "--bathymetry",
        type=Path,
        default=PROJECT_ROOT / DEFAULT_BATHYMETRY_POINTS,
        help="Accepted bathymetry point archive.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=PROJECT_ROOT / DEFAULT_OUTPUT_DIR,
        help="Directory for Fusion v1 parquet outputs.",
    )
    parser.add_argument(
        "--report-dir",
        type=Path,
        default=PROJECT_ROOT / DEFAULT_REPORT_DIR,
        help="Directory for Fusion v1 validation report and tables.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Allow replacing existing Fusion v1 outputs. Defaults to preserve existing artifacts.",
    )
    args = parser.parse_args()

    result = build_metocean_fusion_v1_source_agreement(
        dwell_weather=args.dwell_weather,
        fusion_v0=args.fusion_v0,
        nws_root=args.nws_root,
        baltic_root=args.baltic_root,
        bathymetry=args.bathymetry,
        output_dir=args.output_dir,
        report_dir=args.report_dir,
        overwrite=args.overwrite,
    )

    print("Metocean Fusion v1 source agreement build complete.")
    print(f"Candidate table: {result.candidate_path}")
    print(f"Pairwise agreement table: {result.pairwise_path}")
    print(f"Event confidence table: {result.confidence_path}")
    print(f"Validation report: {result.report_path}")
    print(f"Candidate rows: {result.validation['output_counts']['candidate_rows']}")
    print(f"Pairwise rows: {result.validation['output_counts']['pairwise_rows']}")
    print(f"Confidence rows: {result.validation['output_counts']['confidence_rows']}")
    print(f"Fusion v1 accepted: {result.validation['accepted']}")
    print(
        "Guardrail: no current download, FINO import, source archive mutation, "
        "NORA3 rerun, CTV/SOV inference, or final production rebuild was performed."
    )


if __name__ == "__main__":
    main()
