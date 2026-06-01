"""Build Wind Confidence v1 outputs from existing local NORA3 evidence."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from om_pipeline.metocean.wind_confidence_v1 import (  # noqa: E402
    DEFAULT_CURRENT_CONFIDENCE,
    DEFAULT_DWELL_WEATHER,
    DEFAULT_NORA3_JOINED_CACHE,
    DEFAULT_NORA3_RAW_CACHE,
    DEFAULT_OUTPUT_DIR,
    DEFAULT_REPORT_DIR,
    DEFAULT_WAVE_CONFIDENCE,
    build_wind_confidence_v1,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build event-level Wind Confidence v1 from existing dwell-weather "
            "NORA3 wind fields and local cache inventories."
        )
    )
    parser.add_argument("--dwell-weather", type=Path, default=DEFAULT_DWELL_WEATHER)
    parser.add_argument("--nora3-joined-cache", type=Path, default=DEFAULT_NORA3_JOINED_CACHE)
    parser.add_argument("--nora3-raw-cache", type=Path, default=DEFAULT_NORA3_RAW_CACHE)
    parser.add_argument("--wave-confidence", type=Path, default=DEFAULT_WAVE_CONFIDENCE)
    parser.add_argument("--current-confidence", type=Path, default=DEFAULT_CURRENT_CONFIDENCE)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--report-dir", type=Path, default=DEFAULT_REPORT_DIR)
    parser.add_argument("--overwrite", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = build_wind_confidence_v1(
        dwell_weather=args.dwell_weather,
        nora3_joined_cache=args.nora3_joined_cache,
        nora3_raw_cache=args.nora3_raw_cache,
        wave_confidence_path=args.wave_confidence,
        current_confidence_path=args.current_confidence,
        output_dir=args.output_dir,
        report_dir=args.report_dir,
        overwrite=args.overwrite,
    )
    validation = result.validation
    print("Wind Confidence v1 build complete")
    print(f"  candidate_table:          {result.candidate_path}")
    print(f"  confidence_table:         {result.confidence_path}")
    print(f"  validation_report:        {result.report_path}")
    print(f"  input_dwell_rows:         {validation['input_dwell_rows']}")
    print(f"  candidate_rows:           {validation['candidate_rows']}")
    print(f"  confidence_rows:          {validation['confidence_rows']}")
    print(f"  wind_speed_events:        {validation['wind_speed_events']}")
    print(f"  wind_direction_events:    {validation['wind_direction_events']}")
    print(f"  tier_a_wind_speed_events: {validation['tier_a_wind_speed_events']}")
    print(f"  confidence_counts:        {validation['confidence_counts']}")


if __name__ == "__main__":
    main()
