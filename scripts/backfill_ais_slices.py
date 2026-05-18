import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, os.fspath(Path(__file__).resolve().parents[1] / "src"))

from om_pipeline.common.paths import CACHE_DIR, INTERIM_DIR, TMP_DIR
from om_pipeline.analysis.ais_backfill import run_backfill


def parse_args():
    parser = argparse.ArgumentParser(
        description="Resumable Europe-wide AIS farm-candidate backfill runner."
    )
    parser.add_argument("--start-year", type=int, default=2010)
    parser.add_argument("--end-year", type=int, default=2020)
    parser.add_argument(
        "--phase",
        choices=["quarterly", "backfill", "all"],
        default="all",
        help="quarterly runs Jan/Apr/Jul/Oct first; backfill runs the other months; all runs both in that order.",
    )
    parser.add_argument("--region", default="european_master")
    parser.add_argument(
        "--mode",
        choices=["farm_candidate", "regional"],
        default="farm_candidate",
        help="farm_candidate filters against buffered turbine bounds; regional filters against the configured region bbox.",
    )
    parser.add_argument("--max-sog", type=float, default=2.0)
    parser.add_argument("--buffer-nm", type=float, default=2.0)
    parser.add_argument(
        "--turbine-file",
        default=os.path.join(INTERIM_DIR, "European_Turbine_Coordinates.csv"),
    )
    parser.add_argument("--chunk-size", type=int, default=500000)
    parser.add_argument("--progress-interval", type=int, default=5)
    parser.add_argument("--retries", type=int, default=3)
    parser.add_argument("--retry-sleep-seconds", type=int, default=60)
    parser.add_argument(
        "--manifest-path",
        default=os.path.join(INTERIM_DIR, "ais_backfill_manifest.csv"),
    )
    parser.add_argument("--force-raw", action="store_true")
    parser.add_argument("--force-identification", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    os.environ["TMPDIR"] = TMP_DIR
    os.environ["TEMP"] = TMP_DIR
    os.environ["TMP"] = TMP_DIR
    os.environ.setdefault("XDG_CACHE_HOME", CACHE_DIR)

    args = parse_args()
    run_backfill(
        start_year=args.start_year,
        end_year=args.end_year,
        phase=args.phase,
        region_name=args.region,
        mode=args.mode,
        max_sog=args.max_sog,
        buffer_nm=args.buffer_nm,
        turbine_file=args.turbine_file,
        chunk_size=args.chunk_size,
        progress_interval=args.progress_interval,
        retries=args.retries,
        retry_sleep_seconds=args.retry_sleep_seconds,
        manifest_path=args.manifest_path,
        force_raw=args.force_raw,
        force_identification=args.force_identification,
        limit=args.limit,
        dry_run=args.dry_run,
    )
