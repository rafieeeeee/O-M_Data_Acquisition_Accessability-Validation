import os
import argparse
from om_pipeline.identification.dwell_events import identify_vessels
from om_pipeline.common.paths import INTERIM_DIR

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Identify O&M vessel dwell events from a filtered AIS CSV."
    )
    parser.add_argument("ais_csv", help="Filtered AIS CSV to process.")
    parser.add_argument(
        "--turbine-file",
        default=os.path.join(INTERIM_DIR, "European_Turbine_Coordinates.csv"),
        help="Prepared turbine coordinate CSV.",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=500000,
        help="Number of AIS rows to process per pandas chunk.",
    )
    parser.add_argument(
        "--progress-interval",
        type=int,
        default=1,
        help="Print progress every N chunks.",
    )
    parser.add_argument(
        "--total-rows",
        type=int,
        default=None,
        help="Known AIS input row count, used to show percentage and ETA.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print resolved inputs/outputs and exit without processing.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Allow overwriting existing output files.",
    )
    args = parser.parse_args()

    base_name = os.path.basename(args.ais_csv).replace(".csv", "")
    events_path = os.path.join(INTERIM_DIR, f"OM_Events_{base_name}.csv")
    registry_path = os.path.join(INTERIM_DIR, f"Fleet_Registry_{base_name}.csv")
    existing_outputs = [path for path in (events_path, registry_path) if os.path.exists(path)]

    print("Vessel identification preflight")
    print(f"  AIS input:      {args.ais_csv}")
    print(f"  Turbine input:  {args.turbine_file}")
    print(f"  Events output:  {events_path}")
    print(f"  Registry output:{registry_path}")
    print(f"  Chunk size:     {args.chunk_size:,}")
    if args.total_rows:
        print(f"  Total rows:     {args.total_rows:,}")

    if existing_outputs and not args.force:
        print("\nOutput files already exist:")
        for path in existing_outputs:
            print(f"  {path}")
        print("Re-run with --force to overwrite, or move/archive those files first.")
        raise SystemExit(2)

    if args.dry_run:
        print("\nDry run complete. No processing was started.")
        raise SystemExit(0)

    identify_vessels(
        args.ais_csv,
        args.turbine_file,
        chunk_size=args.chunk_size,
        progress_interval=args.progress_interval,
        total_rows=args.total_rows,
    )
