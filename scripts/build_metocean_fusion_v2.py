"""Build Fusion v2 multi-parameter metocean event features."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from om_pipeline.metocean.metocean_fusion_v2 import (  # noqa: E402
    DEFAULT_BATHYMETRY,
    DEFAULT_CURRENT_CONFIDENCE,
    DEFAULT_DWELL_WEATHER,
    DEFAULT_OUTPUT_DIR,
    DEFAULT_REPORT_DIR,
    DEFAULT_WAVE_CONFIDENCE,
    DEFAULT_WIND_CONFIDENCE,
    build_metocean_fusion_v2,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build Fusion v2 by joining accepted wave, wind, current, and "
            "bathymetry confidence/evidence layers."
        )
    )
    parser.add_argument("--dwell-weather", type=Path, default=DEFAULT_DWELL_WEATHER)
    parser.add_argument("--wave-confidence", type=Path, default=DEFAULT_WAVE_CONFIDENCE)
    parser.add_argument("--wind-confidence", type=Path, default=DEFAULT_WIND_CONFIDENCE)
    parser.add_argument("--current-confidence", type=Path, default=DEFAULT_CURRENT_CONFIDENCE)
    parser.add_argument("--bathymetry", type=Path, default=DEFAULT_BATHYMETRY)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--report-dir", type=Path, default=DEFAULT_REPORT_DIR)
    parser.add_argument("--overwrite", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = build_metocean_fusion_v2(
        dwell_weather=args.dwell_weather,
        wave_confidence_path=args.wave_confidence,
        wind_confidence_path=args.wind_confidence,
        current_confidence_path=args.current_confidence,
        bathymetry_path=args.bathymetry,
        output_dir=args.output_dir,
        report_dir=args.report_dir,
        overwrite=args.overwrite,
    )
    validation = result.validation
    coverage = validation["coverage"]
    tier = validation["tier_a_coverage"]
    print("Fusion v2 build complete")
    print(f"  fusion_v2_table:               {result.output_path}")
    print(f"  validation_report:             {result.report_path}")
    print(f"  input_dwell_rows:              {validation['dwell_rows']}")
    print(f"  output_rows:                   {validation['output_rows']}")
    print(f"  row_identity_preserved:        {validation['row_identity_preserved']}")
    print(f"  wave_rows:                     {coverage['wave_rows']}")
    print(f"  wind_speed_rows:               {coverage['wind_speed_rows']}")
    print(f"  current_rows:                  {coverage['current_rows']}")
    print(f"  wave_wind_current_bathymetry:  {coverage['wave_wind_current_bathymetry_rows']}")
    print(f"  high_confidence_rows:          {coverage['high_confidence_rows']}")
    print(f"  tier_a_wave_wind_current:      {tier['tier_a_wave_wind_current']}")
    print(f"  tier_a_high_confidence:        {tier['tier_a_high_confidence']}")


if __name__ == "__main__":
    main()
