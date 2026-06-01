from __future__ import annotations

import argparse
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from om_pipeline.analysis.ais_backfill_audit import build_ais_audit_summary  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Report effective AIS slice state and exact captured O&M event counts."
    )
    parser.add_argument(
        "--manifest-path",
        default=str(PROJECT_ROOT / "Data/Interim/ais_backfill_manifest.csv"),
    )
    parser.add_argument(
        "--interim-dir",
        default=str(PROJECT_ROOT / "Data/Interim"),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    summary = build_ais_audit_summary(
        manifest_path=args.manifest_path,
        interim_dir=args.interim_dir,
    )

    print("AIS backfill effective audit")
    print(f"  manifest_rows:              {summary.manifest_rows}")
    print(f"  effective_slice_rows:       {summary.effective_slice_rows}")
    print(f"  effective_status_counts:    {summary.effective_status_counts}")
    print(f"  effective_stage_counts:     {summary.effective_stage_counts}")
    print(f"  total_event_files:          {summary.total_event_files}")
    print(f"  total_event_rows:           {summary.total_event_rows}")
    print(f"  total_registry_files:       {summary.total_registry_files}")
    print(f"  total_registry_rows:        {summary.total_registry_rows}")
    print(f"  event_class_counts:         {summary.event_class_counts}")
    print(f"  unique_event_vessels:       {summary.unique_event_vessels}")
    print(f"  unresolved_slice_count:     {len(summary.unresolved_slices)}")
    for row in summary.unresolved_slices:
        print(
            "  unresolved_slice:"
            f" year={row['year']}"
            f" month={row['month']:02d}"
            f" stage={row['stage']}"
            f" status={row['status']}"
            f" message={row['message']}"
        )


if __name__ == "__main__":
    main()
