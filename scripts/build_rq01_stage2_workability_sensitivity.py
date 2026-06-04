"""Build RQ01 restricted Stage 2 workability sensitivity outputs."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from om_pipeline.analysis.rq01_stage2_workability_sensitivity import (  # noqa: E402
    DEFAULT_FUSION_V2,
    DEFAULT_OUTPUT_DIR,
    DEFAULT_READINESS_SUMMARY,
    DEFAULT_REPORT_DIR,
    build_rq01_stage2_workability_sensitivity,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build restricted RQ01 observed-envelope sensitivity outputs."
    )
    parser.add_argument("--fusion-v2", type=Path, default=DEFAULT_FUSION_V2)
    parser.add_argument("--readiness-summary", type=Path, default=DEFAULT_READINESS_SUMMARY)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--report-dir", type=Path, default=DEFAULT_REPORT_DIR)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = build_rq01_stage2_workability_sensitivity(
        fusion_v2_path=args.fusion_v2,
        readiness_summary_path=args.readiness_summary,
        output_dir=args.output_dir,
        report_dir=args.report_dir,
    )
    summary = result.summary
    screening = summary["screening_summary"]
    wave_wind = screening["wave_wind_speed"]
    current = screening["current_sensitive_lanes"]

    print("RQ01 Stage 2 workability sensitivity complete")
    print(f"  analysis_recommendation: {summary['analysis_recommendation']}")
    print(f"  sensitivity_report:      {result.files['sensitivity_report_md']}")
    print(f"  sensitivity_summary:     {result.files['sensitivity_summary_json']}")
    print(
        "  wave_wind_speed_screen: "
        f"{wave_wind.get('materiality_screen_result', 'missing')}"
    )
    for lane_id, lane_summary in current.items():
        print(
            f"  {lane_id}_screen: "
            f"{lane_summary.get('materiality_screen_result', 'missing')}"
        )


if __name__ == "__main__":
    main()
