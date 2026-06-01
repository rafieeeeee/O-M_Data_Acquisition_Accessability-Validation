"""
build_data_index.py

Constructs a read-only metadata index parquet file containing the status of every
farm x year-month partition across the pipeline lifecycle.

Output: <root_dir>/Data/Processed/dashboard/data_index.parquet
"""

import os
import re
import sys
import argparse
from pathlib import Path
import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

# Ensure src is on path when run as script
PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "src"))

from om_pipeline.dashboard.farm_universe import (
    FARM_UNIVERSE,
    BALTIC_CLUSTER_FARMS,
    RawSourceStatus,
    FilteredAisStatus,
    VisitsStatus,
    DwellsStatus,
    DuplicateStatus,
    WeatherJoinStatus,
    QaMapStatus,
    ValidationStatus
)

def get_safe_id(farm_name: str) -> str:
    """Resolve raw name to the safe folder id used in partitioning."""
    if not isinstance(farm_name, str) or pd.isna(farm_name):
        return ""
    for safe_id, auth_name in FARM_UNIVERSE.items():
        if auth_name.strip().lower() == farm_name.strip().lower() or safe_id.lower() == farm_name.strip().lower():
            return safe_id
    # Fallback synonym cleanup
    return str(farm_name).replace(" ", "_").replace("/", "_").strip()

def fast_line_count(filepath: Path, deep: bool = False) -> int:
    """Line count helper. Skips huge CSV line scans unless --deep is active."""
    try:
        if not filepath.exists() or filepath.stat().st_size == 0:
            return 0
        # If not deep scan, return -1 to signal skipped scan
        if not deep:
            return -1
        with open(filepath, 'rb') as f:
            return sum(1 for _ in f) - 1 # exclude header
    except Exception:
        return 0

def build_index(root_path: Path, deep: bool = False):
    print(f"Initializing Data Observatory Indexer at ROOT: {root_path} (Deep Scan={deep})")

    # Resolve exact pathways relative to root_path
    TURBINE_FILE = root_path / "Data/Interim/European_Turbine_Coordinates.csv"
    AIS_DIR = root_path / "Data/Raw/AIS"
    RAW_MANIFEST_FILE = root_path / "Data/Interim/ais_backfill_manifest.csv"
    PROCESSED_MANIFEST_FILE = root_path / "Data/Processed/ais_dwell_backfill/logs/backfill_manifest.csv"
    DWELL_BACKFILL_DIR = root_path / "Data/Processed/ais_dwell_backfill"
    WEATHER_PARQUET_FILE = DWELL_BACKFILL_DIR / "cross_farm_dwell_weather_features.parquet"
    REPORTS_DIR = root_path / "reports/ais_dwell"

    # Make sure AIS_DIR exists to prevent crash
    AIS_DIR.mkdir(parents=True, exist_ok=True)

    # 1. Compile temporal range from raw files and manifests (2010 - 2025 default)
    years = list(range(2010, 2026))
    months = list(range(1, 13))

    # Grid cartesian product of deterministic universe x temporal slots
    records = []
    for year in years:
        for month in months:
            for safe_id, auth_name in FARM_UNIVERSE.items():
                records.append({
                    "farm_id": safe_id,
                    "farm_name": auth_name,
                    "year": year,
                    "month": month,
                    "is_target_cluster": safe_id in BALTIC_CLUSTER_FARMS
                })
    df_grid = pd.DataFrame(records)

    # 2. Pre-load high-level manifests for quick lookups
    print("Loading pipeline manifests...")
    df_raw_manifest = pd.DataFrame()
    if RAW_MANIFEST_FILE.exists():
        try:
            df_raw_manifest = pd.read_csv(RAW_MANIFEST_FILE)
            # Normalize casing/columns
            df_raw_manifest.columns = [c.strip().lower() for c in df_raw_manifest.columns]
        except Exception as e:
            print(f"Warning: Failed to load raw manifest: {e}")

    df_proc_manifest = pd.DataFrame()
    if PROCESSED_MANIFEST_FILE.exists():
        try:
            df_proc_manifest = pd.read_csv(PROCESSED_MANIFEST_FILE)
        except Exception as e:
            print(f"Warning: Failed to load processed manifest: {e}")

    # 3. Pre-load optional weather feature parquet
    print("Checking optional metocean weather feature outputs...")
    df_weather = pd.DataFrame()
    if WEATHER_PARQUET_FILE.exists():
        try:
            df_weather = pd.read_parquet(WEATHER_PARQUET_FILE)
            if not df_weather.empty:
                # Standardize datetime parsing
                df_weather['start_dt'] = pd.to_datetime(df_weather['start_utc'], errors='coerce')
                df_weather['year_parsed'] = df_weather['start_dt'].dt.year
                df_weather['month_parsed'] = df_weather['start_dt'].dt.month
                df_weather['farm_safe_id'] = df_weather['farm_id'].apply(get_safe_id)
        except Exception as e:
            print(f"Warning: Failed to load weather parquet: {e}")
            df_weather = pd.DataFrame() # Reset on failure

    # 4. Pre-load QA samples
    print("Mapping QA map samples...")
    qa_samples = []
    for qa_csv in [REPORTS_DIR / "visual_qa_sample.csv", REPORTS_DIR / "wikinger_visual_qa_sample.csv"]:
        if qa_csv.exists():
            try:
                qa_df = pd.read_csv(qa_csv)
                qa_samples.append(qa_df)
            except Exception as e:
                print(f"Warning: Failed to load QA sample {qa_csv.name}: {e}")

    df_qa = pd.DataFrame()
    if qa_samples:
        df_qa = pd.concat(qa_samples, ignore_index=True)
        if not df_qa.empty:
            df_qa['start_dt'] = pd.to_datetime(df_qa['start_utc'], errors='coerce')
            df_qa['year_parsed'] = df_qa['start_dt'].dt.year
            df_qa['month_parsed'] = df_qa['start_dt'].dt.month
            df_qa['farm_safe_id'] = df_qa['farm_id'].apply(get_safe_id)

    # 5. Populate separate statuses dynamically
    print(f"Building statuses for {len(df_grid)} universe slots...")

    # Store dynamic fields
    results = []

    for idx, row in df_grid.iterrows():
        safe_id = row["farm_id"]
        auth_name = row["farm_name"]
        year = row["year"]
        month = row["month"]

        # Check source raw file availability
        raw_pattern = f"Farm-Candidates_European-Master_{year}_{month:02d}_SogMax2.0_Buffer2.0nm.csv"
        raw_file = AIS_DIR / raw_pattern

        # In case it has a different buffer suffix
        if not raw_file.exists():
            candidates = list(AIS_DIR.glob(f"Farm-Candidates_European-Master_{year}_{month:02d}_*.csv"))
            # filter out .tmp files
            candidates = [c for c in candidates if not c.name.endswith('.tmp')]
            if candidates:
                raw_file = candidates[0]

        raw_source_status = RawSourceStatus.MISSING.value
        raw_source_file_path = ""
        raw_source_file_size_mb = 0.0
        raw_source_rows = 0

        if raw_file.exists():
            raw_source_status = RawSourceStatus.EXISTS.value
            raw_source_file_path = str(raw_file.relative_to(root_path))
            size_bytes = raw_file.stat().st_size
            raw_source_file_size_mb = round(size_bytes / (1024 * 1024), 2)
            if size_bytes == 0:
                raw_source_status = RawSourceStatus.EMPTY.value
            else:
                # Retrieve raw rows count from manifestations if possible
                rows_found = False
                if not df_proc_manifest.empty:
                    match = df_proc_manifest[
                        (df_proc_manifest['farm_id'] == auth_name) &
                        (df_proc_manifest['year'] == year) &
                        (df_proc_manifest['month'] == month)
                    ]
                    if not match.empty:
                        raw_source_rows = int(match.iloc[0].get('input_rows', 0))
                        rows_found = True

                if not rows_found and not df_raw_manifest.empty:
                    match = df_raw_manifest[
                        (df_raw_manifest['year'] == year) &
                        (df_raw_manifest['month'] == month)
                    ]
                    if not match.empty:
                        raw_source_rows = fast_line_count(raw_file, deep=deep)
                        rows_found = True

                if not rows_found:
                    raw_source_rows = fast_line_count(raw_file, deep=deep)

        # Filtered AIS (interim registry / events outputs)
        filtered_ais_status = FilteredAisStatus.MISSING.value
        filtered_ais_rows = 0

        # Check if there are temporary/interrupted files
        tmp_pattern = f"Farm-Candidates_European-Master_{year}_{month:02d}_*.tmp"
        tmp_files = list(AIS_DIR.glob(tmp_pattern)) + list(root_path.glob(f"Data/Interim/*{year}_{month:02d}*.tmp"))

        om_event_pattern = f"OM_Events_Farm-Candidates_European-Master_{year}_{month:02d}_*.csv"
        om_files = list(root_path.glob(f"Data/Interim/{om_event_pattern}"))

        if om_files:
            om_file = om_files[0]
            size_bytes = om_file.stat().st_size
            if size_bytes == 0:
                filtered_ais_status = FilteredAisStatus.INTERRUPTED.value
            else:
                filtered_ais_status = FilteredAisStatus.EXISTS.value
                try:
                    df_om = pd.read_csv(om_file)
                    filtered_ais_rows = len(df_om)
                except Exception:
                    filtered_ais_status = FilteredAisStatus.INTERRUPTED.value
        elif tmp_files:
            filtered_ais_status = FilteredAisStatus.INTERRUPTED.value

        # If we have clean_rows from processed manifest, update
        if not df_proc_manifest.empty:
            match = df_proc_manifest[
                (df_proc_manifest['farm_id'] == auth_name) &
                (df_proc_manifest['year'] == year) &
                (df_proc_manifest['month'] == month)
            ]
            if not match.empty and match.iloc[0].get('status') == 'success':
                filtered_ais_rows = int(match.iloc[0].get('clean_rows', filtered_ais_rows))
                if filtered_ais_status == FilteredAisStatus.MISSING.value:
                    filtered_ais_status = FilteredAisStatus.EXISTS.value

        # Visits Parquet Checks
        visits_status = VisitsStatus.MISSING.value
        visits_count = 0
        visit_pq = DWELL_BACKFILL_DIR / f"visits/farm_id={safe_id}/year={year}/month={month}/data.parquet"
        if visit_pq.exists():
            visits_status = VisitsStatus.EXISTS.value
            if visit_pq.stat().st_size == 0:
                visits_status = VisitsStatus.EMPTY.value
            else:
                try:
                    visits_count = pq.read_metadata(visit_pq).num_rows
                    if visits_count == 0:
                        visits_status = VisitsStatus.EMPTY.value
                except Exception:
                    visits_status = VisitsStatus.EMPTY.value

        # Dwells Parquet Checks
        dwells_status = DwellsStatus.MISSING.value
        dwells_count = 0
        tier_a_count = 0
        tier_b_count = 0
        tier_c_count = 0
        tier_d_count = 0
        duplicate_status = DuplicateStatus.MISSING.value
        duplicate_flag_count = 0

        dwell_pq = DWELL_BACKFILL_DIR / f"dwells/farm_id={safe_id}/year={year}/month={month}/data.parquet"
        if dwell_pq.exists():
            dwells_status = DwellsStatus.EXISTS.value
            if dwell_pq.stat().st_size == 0:
                dwells_status = DwellsStatus.EMPTY.value
            else:
                try:
                    dwells_count = pq.read_metadata(dwell_pq).num_rows
                    if dwells_count > 0:
                        df_d = pd.read_parquet(dwell_pq)
                        if 'dwell_tier' in df_d.columns:
                            tier_a_count = int((df_d['dwell_tier'] == 'Tier A').sum())
                            tier_b_count = int((df_d['dwell_tier'] == 'Tier B').sum())
                            tier_c_count = int((df_d['dwell_tier'] == 'Tier C').sum())
                            tier_d_count = int((df_d['dwell_tier'] == 'Tier D').sum())
                        if 'possible_cross_farm_duplicate' in df_d.columns:
                            duplicate_flag_count = int(df_d['possible_cross_farm_duplicate'].sum())
                            duplicate_status = DuplicateStatus.CALCULATED.value
                    else:
                        dwells_status = DwellsStatus.EMPTY.value
                except Exception:
                    dwells_status = DwellsStatus.EMPTY.value

        # Metocean Weather Joins (Optional)
        weather_join_status = WeatherJoinStatus.MISSING.value
        weather_join_count = 0
        weather_missing_fraction = np.nan

        if not df_weather.empty:
            df_sub_w = df_weather[
                (df_weather['farm_safe_id'] == safe_id) &
                (df_weather['year_parsed'] == year) &
                (df_weather['month_parsed'] == month)
            ]
            if not df_sub_w.empty:
                weather_join_status = WeatherJoinStatus.JOINED.value
                weather_join_count = len(df_sub_w)
                if 'active_weather_missing_fraction' in df_sub_w.columns:
                    weather_missing_fraction = float(df_sub_w['active_weather_missing_fraction'].mean())
                elif 'approach_weather_missing_fraction' in df_sub_w.columns:
                    weather_missing_fraction = float(df_sub_w['approach_weather_missing_fraction'].mean())

        # Validation (Events registry)
        validation_status = ValidationStatus.MISSING.value
        validation_event_count = 0

        if om_files:
            validation_status = ValidationStatus.EXISTS.value
            validation_event_count = filtered_ais_rows

        # Fleet Vessel Registry Counts
        fleet_registry_file_exists = False
        fleet_registry_vessel_count = 0
        fleet_pattern = f"Fleet_Registry_Farm-Candidates_European-Master_{year}_{month:02d}_*.csv"
        fleet_files = list(root_path.glob(f"Data/Interim/{fleet_pattern}"))
        if fleet_files:
            fleet_registry_file_exists = True
            try:
                df_fleet = pd.read_csv(fleet_files[0])
                fleet_registry_vessel_count = len(df_fleet)
            except Exception:
                pass

        # QA maps status
        qa_map_status = QaMapStatus.MISSING.value
        qa_map_count = 0
        if not df_qa.empty:
            df_sub_qa = df_qa[
                (df_qa['farm_safe_id'] == safe_id) &
                (df_qa['year_parsed'] == year) &
                (df_qa['month_parsed'] == month)
            ]
            if not df_sub_qa.empty:
                qa_map_status = QaMapStatus.EXISTS.value
                qa_map_count = len(df_sub_qa)

        # Source reason flags for the deterministic universe
        present_in_raw = (raw_source_status == RawSourceStatus.EXISTS.value)
        present_in_processed_manifest = False
        if not df_proc_manifest.empty:
            present_in_processed_manifest = not df_proc_manifest[
                (df_proc_manifest['farm_id'] == auth_name) &
                (df_proc_manifest['year'] == year) &
                (df_proc_manifest['month'] == month)
            ].empty

        present_in_dwell_outputs = (dwells_status == DwellsStatus.EXISTS.value)
        present_in_weather_join = (weather_join_status == WeatherJoinStatus.JOINED.value)
        present_in_qa_sample = (qa_map_status == QaMapStatus.EXISTS.value)

        # Collect metrics
        results.append({
            "farm_id": safe_id,
            "farm_name": auth_name,
            "year": year,
            "month": month,
            "is_target_cluster": row["is_target_cluster"],
            # Source reasons
            "present_in_raw": present_in_raw,
            "present_in_processed_manifest": present_in_processed_manifest,
            "present_in_dwell_outputs": present_in_dwell_outputs,
            "present_in_weather_join": present_in_weather_join,
            "present_in_qa_sample": present_in_qa_sample,
            # Raw Source
            "raw_source_status": raw_source_status,
            "raw_source_rows": raw_source_rows,
            "raw_source_file_path": raw_source_file_path,
            "raw_source_file_size_mb": raw_source_file_size_mb,
            # Filtered AIS
            "filtered_ais_status": filtered_ais_status,
            "filtered_ais_rows": filtered_ais_rows,
            # Visits
            "visits_status": visits_status,
            "visits_count": visits_count,
            # Dwells
            "dwells_status": dwells_status,
            "dwells_count": dwells_count,
            "tier_a_count": tier_a_count,
            "tier_b_count": tier_b_count,
            "tier_c_count": tier_c_count,
            "tier_d_count": tier_d_count,
            # Duplicates
            "duplicate_status": duplicate_status,
            "duplicate_flag_count": duplicate_flag_count,
            # Weather
            "weather_join_status": weather_join_status,
            "weather_join_count": weather_join_count,
            "weather_missing_fraction": weather_missing_fraction,
            # Validation
            "validation_status": validation_status,
            "validation_event_count": validation_event_count,
            "fleet_registry_file_exists": fleet_registry_file_exists,
            "fleet_registry_vessel_count": fleet_registry_vessel_count,
            # QA Maps
            "qa_map_status": qa_map_status,
            "qa_map_count": qa_map_count
        })

    df_index = pd.DataFrame(results)

    # 6. Save data index strictly as a single consolidated parquet file
    out_dir = root_path / "Data/Processed/dashboard"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "data_index.parquet"

    df_index.to_parquet(out_path, index=False)
    print(f"Observability index successfully written to: {out_path} ({len(df_index)} partitions cataloged)")
    return df_index

def parse_args():
    parser = argparse.ArgumentParser(description="Build Data Observability Index Parquet file.")
    parser.add_argument("--root", type=str, default=None,
                        help="Alternative workspace root directory path (for testing)")
    parser.add_argument("--deep", action="store_true",
                        help="Enable deep row scanning on raw AIS candidate files (heavy I/O)")
    return parser.parse_args()

def main():
    args = parse_args()
    root_dir = Path(args.root).resolve() if args.root else PROJECT_ROOT
    build_index(root_dir, deep=args.deep)

if __name__ == "__main__":
    main()
