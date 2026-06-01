#!/usr/bin/env python
"""Build Current Confidence v1 from accepted local NWS current archives."""

from __future__ import annotations

import argparse
from pathlib import Path

from om_pipeline.metocean.current_confidence_v1 import (
    DEFAULT_BATHYMETRY,
    DEFAULT_DWELL_WEATHER,
    DEFAULT_NWS_CURRENT_MANIFEST,
    DEFAULT_NWS_CURRENT_ROOT,
    DEFAULT_OUTPUT_DIR,
    DEFAULT_REPORT_DIR,
    DEFAULT_WAVE_CONFIDENCE,
    build_current_confidence_v1,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dwell-weather", type=Path, default=DEFAULT_DWELL_WEATHER)
    parser.add_argument("--nws-current-root", type=Path, default=DEFAULT_NWS_CURRENT_ROOT)
    parser.add_argument("--nws-current-manifest", type=Path, default=DEFAULT_NWS_CURRENT_MANIFEST)
    parser.add_argument("--wave-confidence", type=Path, default=DEFAULT_WAVE_CONFIDENCE)
    parser.add_argument("--bathymetry", type=Path, default=DEFAULT_BATHYMETRY)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--report-dir", type=Path, default=DEFAULT_REPORT_DIR)
    parser.add_argument("--overwrite", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = build_current_confidence_v1(
        dwell_weather=args.dwell_weather,
        nws_current_root=args.nws_current_root,
        nws_current_manifest=args.nws_current_manifest,
        wave_confidence_path=args.wave_confidence,
        bathymetry_path=args.bathymetry,
        output_dir=args.output_dir,
        report_dir=args.report_dir,
        overwrite=args.overwrite,
    )
    print(f"Candidate table: {result.candidate_path}")
    print(f"Confidence table: {result.confidence_path}")
    print(f"Validation report: {result.report_path}")
    print(f"Input dwell rows: {result.validation['input_dwell_rows']:,}")
    print(f"Candidate rows: {result.validation['candidate_rows']:,}")
    print(f"Confidence rows: {result.validation['confidence_rows']:,}")
    print(f"NWS eligible events: {result.validation['nws_eligible_events']:,}")
    print(f"Event-scale current events: {result.validation['event_scale_current_events']:,}")
    print(f"Tier A valid current events: {result.validation['tier_a_valid_current_events']:,}")
    print(f"Confidence distribution: {result.validation['confidence_counts']}")


if __name__ == "__main__":
    main()
