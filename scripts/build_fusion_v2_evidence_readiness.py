"""Build the Fusion v2 evidence-readiness audit outputs."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from om_pipeline.analysis.fusion_v2_evidence_readiness import (  # noqa: E402
    DEFAULT_BATHYMETRY,
    DEFAULT_CURRENT_CONFIDENCE,
    DEFAULT_DWELL_WEATHER,
    DEFAULT_FUSION_V2,
    DEFAULT_FUSION_V2_REPORT,
    DEFAULT_OUTPUT_DIR,
    DEFAULT_REPORT_DIR,
    DEFAULT_WAVE_CONFIDENCE,
    DEFAULT_WIND_CONFIDENCE,
    build_fusion_v2_evidence_readiness,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build report-only Fusion v2 evidence-readiness audit outputs."
    )
    parser.add_argument("--fusion-v2", type=Path, default=DEFAULT_FUSION_V2)
    parser.add_argument("--dwell-weather", type=Path, default=DEFAULT_DWELL_WEATHER)
    parser.add_argument("--wave-confidence", type=Path, default=DEFAULT_WAVE_CONFIDENCE)
    parser.add_argument("--wind-confidence", type=Path, default=DEFAULT_WIND_CONFIDENCE)
    parser.add_argument("--current-confidence", type=Path, default=DEFAULT_CURRENT_CONFIDENCE)
    parser.add_argument("--bathymetry", type=Path, default=DEFAULT_BATHYMETRY)
    parser.add_argument("--fusion-v2-report", type=Path, default=DEFAULT_FUSION_V2_REPORT)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--report-dir", type=Path, default=DEFAULT_REPORT_DIR)
    parser.add_argument("--overwrite", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = build_fusion_v2_evidence_readiness(
        fusion_v2_path=args.fusion_v2,
        dwell_weather_path=args.dwell_weather,
        wave_confidence_path=args.wave_confidence,
        wind_confidence_path=args.wind_confidence,
        current_confidence_path=args.current_confidence,
        bathymetry_path=args.bathymetry,
        fusion_v2_report_path=args.fusion_v2_report,
        output_dir=args.output_dir,
        report_dir=args.report_dir,
        overwrite=args.overwrite,
    )
    summary = result.summary
    subset_counts = summary["readiness_subset_counts"]
    tier_counts = summary["tier_a_subset_counts"]

    print("Fusion v2 evidence-readiness audit complete")
    print(f"  recommendation:                  {summary['final_recommendation']}")
    print(f"  readiness_report:                {result.files['readiness_report_md']}")
    print(f"  readiness_summary_json:          {result.files['readiness_summary_json']}")
    print(f"  wave_wind_current_rows:          {subset_counts['model_ready_wave_wind_current']['event_count']}")
    print(f"  high_confidence_rows:            {subset_counts['model_ready_high_confidence']['event_count']}")
    print(f"  tier_a_wave_wind_current_rows:   {tier_counts['model_ready_wave_wind_current']['tier_a_event_count']}")
    print(f"  tier_a_high_confidence_rows:     {tier_counts['model_ready_high_confidence']['tier_a_event_count']}")
    if summary["key_caveats"]:
        print("  caveats:")
        for caveat in summary["key_caveats"]:
            print(f"    - {caveat}")


if __name__ == "__main__":
    main()
