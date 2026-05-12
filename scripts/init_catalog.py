import argparse
import os

from om_pipeline.common.database import (
    CATALOG_PATH,
    get_connection,
    list_views,
    register_data_source,
)
from om_pipeline.common.paths import AIS_RAW_DIR, INTERIM_DIR


DEFAULT_TURBINE_CSV = os.path.join(INTERIM_DIR, "European_Turbine_Coordinates.csv")
DEFAULT_EVENTS_GLOB = os.path.join(INTERIM_DIR, "OM_Events_*.csv")
DEFAULT_REGISTRY_GLOB = os.path.join(INTERIM_DIR, "Fleet_Registry_*.csv")
DEFAULT_RAW_AIS_GLOB = os.path.join(AIS_RAW_DIR, "*.csv")


def _register_sources(con, required_sources, optional_sources):
    missing_required = []
    registered_count = 0

    for table_name, file_path in required_sources:
        try:
            register_data_source(con, table_name, file_path)
            registered_count += 1
        except FileNotFoundError as exc:
            missing_required.append(str(exc))

    if missing_required:
        raise FileNotFoundError(
            "Catalog initialization is missing required source files:\n"
            + "\n".join(f"- {message}" for message in missing_required)
        )

    for table_name, file_path in optional_sources:
        try:
            register_data_source(con, table_name, file_path)
            registered_count += 1
        except FileNotFoundError as exc:
            print(f"Skipping optional source: {exc}")

    return registered_count


def init_catalog(
    catalog_path=CATALOG_PATH,
    turbine_csv=DEFAULT_TURBINE_CSV,
    events_glob=DEFAULT_EVENTS_GLOB,
    registry_glob=DEFAULT_REGISTRY_GLOB,
    raw_ais_glob=DEFAULT_RAW_AIS_GLOB,
    include_raw_ais=False,
):
    print(f"Initializing DuckDB catalog at {catalog_path}...")
    con = get_connection(catalog_path=catalog_path)

    required_sources = [
        ("turbines", turbine_csv),
        ("dwell_events", events_glob),
        ("fleet_registry", registry_glob),
    ]
    optional_sources = [("ais_raw", raw_ais_glob)] if include_raw_ais else []

    try:
        registered_count = _register_sources(con, required_sources, optional_sources)
        views = list_views(con)
    finally:
        con.close()

    print(f"\nInitialization complete. Registered {registered_count} views.")
    if views:
        print(f"Available views: {[v[0] for v in views]}")
    return registered_count


def _parse_args():
    parser = argparse.ArgumentParser(description="Initialize the local DuckDB catalog.")
    parser.add_argument("--catalog-path", default=CATALOG_PATH)
    parser.add_argument("--turbine-csv", default=DEFAULT_TURBINE_CSV)
    parser.add_argument("--events-glob", default=DEFAULT_EVENTS_GLOB)
    parser.add_argument("--registry-glob", default=DEFAULT_REGISTRY_GLOB)
    parser.add_argument("--raw-ais-glob", default=DEFAULT_RAW_AIS_GLOB)
    parser.add_argument(
        "--include-raw-ais",
        action="store_true",
        help="Also register raw AIS CSVs as the ais_raw view.",
    )
    return parser.parse_args()

if __name__ == "__main__":
    args = _parse_args()
    init_catalog(
        catalog_path=args.catalog_path,
        turbine_csv=args.turbine_csv,
        events_glob=args.events_glob,
        registry_glob=args.registry_glob,
        raw_ais_glob=args.raw_ais_glob,
        include_raw_ais=args.include_raw_ais,
    )
