"""
analysis/00_data_foundation/build_event_table.py
----------------------------------------
Builds the canonical, deduplicated, and fully enriched event-level dataset:
Data/Processed/analysis/om_event_table.parquet

This serves as the shared data plumbing backbone for all downstream Research Questions (RQs).
It inventory-scans and combines available dwells, aggregates vessel registry characteristics 
via high-speed DuckDB queries, standardises timestamps, and preserves continuous vessel length 
and raw AIS ship type without applying heuristic operational classes.
"""

import os
import sys
import pandas as pd
import numpy as np
import duckdb
from pathlib import Path

# Setup paths relative to project root
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

# Outputs
OUTPUT_DIR = PROJECT_ROOT / "Data" / "Processed" / "analysis"
OUTPUT_PARQUET = OUTPUT_DIR / "om_event_table.parquet"

def build_vessel_registry_duckdb() -> pd.DataFrame:
    """
    Leverages DuckDB to scan and aggregate all 180+ monthly Fleet Registry CSVs in seconds.
    Extracts continuous length, vessel name, draft, and raw ship type for each MMSI.
    """
    print("\nScanning monthly fleet registries via DuckDB...")
    con = duckdb.connect()
    
    # We use a union-by-name scan ignoring errors to bypass CSV parsing issues on single dirty lines
    # and map columns with different cased headers dynamically.
    query = """
        SELECT 
            MMSI,
            max(length) as length,
            max(draught) as draft,
            any_value(Name) as name,
            any_value("Ship type") as ship_type
        FROM read_csv_auto('Data/Interim/Fleet_Registry_*.csv', ignore_errors=true, union_by_name=true)
        WHERE MMSI IS NOT NULL AND MMSI > 0
        GROUP BY MMSI
    """
    try:
        registry_df = con.execute(query).fetchdf()
        print(f"  [SUCCESS] Aggregated metadata for {len(registry_df):,} unique MMSIs.")
        return registry_df
    except Exception as e:
        print(f"  [WARN] DuckDB registry scan failed: {e}. Falling back to empty registry stub.")
        return pd.DataFrame(columns=['MMSI', 'length', 'draft', 'name', 'ship_type'])

def build_canonical_event_table():
    """Compiles, enriches, and exports the canonical O&M event table."""
    print("=" * 60)
    print("BUILDING CANONICAL DATA FOUNDATION EVENT TABLE")
    print("=" * 60)
    
    # 1. Load available dwells
    backfill_path = PROJECT_ROOT / "Data" / "Processed" / "ais_dwell_backfill" / "cross_farm_dwell_weather_features.parquet"
    pilot_path = PROJECT_ROOT / "Data" / "Processed" / "cross_farm_dwell_weather_features.parquet"
    
    if not backfill_path.exists():
        print(f"[ERROR] Backfill features parquet not found at {backfill_path}!")
        sys.exit(1)
        
    print(f"Loading backfill dwells: {backfill_path}")
    df_backfill = pd.read_parquet(backfill_path)
    print(f"  Loaded {len(df_backfill):,} rows.")
    
    df_pilot = pd.DataFrame()
    if pilot_path.exists():
        print(f"Loading pilot dwells: {pilot_path}")
        df_pilot = pd.read_parquet(pilot_path)
        print(f"  Loaded {len(df_pilot):,} rows.")
        
    # Combine and harmonize wind_farm vs farm_id
    if not df_pilot.empty:
        for df in [df_backfill, df_pilot]:
            if 'wind_farm' not in df.columns and 'farm_id' in df.columns:
                df['wind_farm'] = df['farm_id']
            if 'wind_farm' in df.columns and 'farm_id' not in df.columns:
                df['farm_id'] = df['wind_farm']
        
        # Concatenate and drop duplicate dwell IDs (pilot holds Borkum validation overlaps)
        df_combined = pd.concat([df_backfill, df_pilot], ignore_index=True)
        df_combined = df_combined.drop_duplicates(subset=['dwell_id']).reset_index(drop=True)
    else:
        df_combined = df_backfill.copy()
        
    print(f"Total deduplicated combined dwells: {len(df_combined):,}")
    
    # 2. Enrich from Vessel Registry
    vessel_reg = build_vessel_registry_duckdb()
    
    df_combined['mmsi'] = pd.to_numeric(df_combined['mmsi'], errors='coerce').fillna(0).astype(int)
    vessel_reg['MMSI'] = vessel_reg['MMSI'].astype(int)
    
    # Merge vessel specifications
    df_enriched = df_combined.merge(
        vessel_reg,
        left_on='mmsi',
        right_on='MMSI',
        how='left',
        suffixes=('', '_registry')
    )
    
    # Populate missing vessel dimensions and names with registry values
    df_enriched['vessel_length_m'] = df_enriched['length'].combine_first(df_enriched['vessel_length_m'])
    df_enriched['vessel_draft_m'] = df_enriched['draft'].combine_first(df_enriched['vessel_draft_m'])
    df_enriched['vessel_name'] = df_enriched['name'].combine_first(df_enriched['centroid_x'].apply(lambda x: None)) # stub helper
    df_enriched['vessel_raw_ship_type'] = df_enriched['ship_type']
    
    # Drop intermediate merge columns
    df_enriched = df_enriched.drop(columns=['MMSI', 'length', 'draft', 'name', 'ship_type'], errors='ignore')
    
    # 3. Standardise columns & identifiers
    df_enriched = df_enriched.rename(columns={'dwell_id': 'event_id'})
    
    # Verify standard timezone-aware UTC timestamps
    df_enriched['start_utc'] = pd.to_datetime(df_enriched['start_utc'], utc=True)
    df_enriched['end_utc'] = pd.to_datetime(df_enriched['end_utc'], utc=True)
    
    # Drop columns that are entirely NaN to keep the table clean
    nan_cols = [c for c in df_enriched.columns if df_enriched[c].isna().mean() == 1.0]
    if nan_cols:
        print(f"\nDropping {len(nan_cols)} completely empty columns to maintain hygiene:")
        print(f"  {nan_cols}")
        df_enriched = df_enriched.drop(columns=nan_cols)
        
    # Sort chronologically
    df_enriched = df_enriched.sort_values('start_utc').reset_index(drop=True)
    
    # Create output directory and export
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    df_enriched.to_parquet(OUTPUT_PARQUET, index=False)
    print(f"\n[SUCCESS] Canonical event table exported → {OUTPUT_PARQUET}")
    
    # 4. Generate Diagnostic Summary
    print("\n" + "=" * 60)
    print("CANONICAL EVENT TABLE INVENTORY DIAGNOSTICS")
    print("=" * 60)
    print(f"  Total records (Rows)  : {len(df_enriched):,}")
    print(f"  Total variables (Cols): {df_enriched.shape[1]}")
    print(f"  Date range (UTC)      : {df_enriched['start_utc'].min()} → {df_enriched['start_utc'].max()}")
    print(f"  Unique Wind Farms     : {df_enriched['wind_farm'].nunique()} observed lease areas")
    print(f"  Unique MMSIs          : {df_enriched['mmsi'].nunique()} active O&M vessels")
    print(f"  Raw Ship Types        : {df_enriched['vessel_raw_ship_type'].dropna().unique().tolist()}")
    
    # Check null rates of primary parameters
    print("\n  Primary Feature Coverage:")
    core_features = [
        'event_id', 'mmsi', 'dwell_tier', 'duration_min',
        'vessel_length_m', 'vessel_raw_ship_type',
        'active_hs_mean', 'active_tp_mean', 'active_wind_speed_mean',
        'current_speed_mean', 'approach_n_weather_records', 'current_source_available'
    ]
    for feat in core_features:
        if feat in df_enriched.columns:
            null_pct = df_enriched[feat].isna().mean() * 100
            print(f"    {feat:<30} : {100 - null_pct:>6.1f}% cover ({null_pct:>5.1f}% null)")
            
    print("=" * 60)

if __name__ == "__main__":
    build_canonical_event_table()
