"""
test_dashboard_index.py

Refactored unit tests for build_data_index.py utilizing mock workspaces
and verifying enums, CLI options, and isolated pathways.
"""

import sys
from pathlib import Path
import pandas as pd
import numpy as np
import pytest

# Ensure src is in PYTHONPATH
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "src"))

import om_pipeline.dashboard.build_data_index as bdi
from om_pipeline.dashboard.farm_universe import (
    FARM_UNIVERSE,
    RawSourceStatus,
    FilteredAisStatus,
    VisitsStatus,
    DwellsStatus,
    DuplicateStatus,
    WeatherJoinStatus
)

@pytest.fixture
def mock_isolated_tree(tmp_path):
    """Sets up a synthetic isolated directory tree with custom file sizes and logs."""
    # Create directories
    (tmp_path / "Data/Interim").mkdir(parents=True, exist_ok=True)
    (tmp_path / "Data/Raw/AIS").mkdir(parents=True, exist_ok=True)
    (tmp_path / "Data/Processed/ais_dwell_backfill/logs").mkdir(parents=True, exist_ok=True)
    (tmp_path / "reports/ais_dwell").mkdir(parents=True, exist_ok=True)
    
    # 1. Create a dummy European Turbine Coordinates file
    df_turbines = pd.DataFrame({
        "wind_farm": ["Wikinger", "Arkona-Becken Südost", "Baltic Eagle"],
        "latitude": [54.0, 54.1, 54.2],
        "longitude": [14.0, 14.1, 14.2],
        "oem_manufacturer": ["Vestas", "Siemens", "Vestas"]
    })
    df_turbines.to_csv(tmp_path / "Data/Interim/European_Turbine_Coordinates.csv", index=False)
    
    # 2. Create one raw AIS CSV file with exactly 3 lines of data
    df_ais = pd.DataFrame({
        "MMSI": [211000000, 211000000, 211000000],
        "Timestamp": ["2020-01-01 00:00:00", "2020-01-01 00:10:00", "2020-01-01 00:20:00"],
        "Latitude": [54.0, 54.0, 54.0],
        "Longitude": [14.0, 14.0, 14.0],
        "SOG": [0.1, 0.0, 0.0]
    })
    raw_file = tmp_path / "Data/Raw/AIS/Farm-Candidates_European-Master_2020_01_SogMax2.0_Buffer2.0nm.csv"
    df_ais.to_csv(raw_file, index=False)
    
    # Create a zero-byte interrupted Raw file
    empty_file = tmp_path / "Data/Raw/AIS/Farm-Candidates_European-Master_2020_02_SogMax2.0_Buffer2.0nm.csv"
    empty_file.touch()
    
    # 3. Create raw manifest
    df_raw_manifest = pd.DataFrame({
        "timestamp": ["2026-05-12 17:38:36"],
        "year": [2020],
        "month": [1],
        "stage": ["stream"],
        "status": ["success"],
        "message": [""]
    })
    df_raw_manifest.to_csv(tmp_path / "Data/Interim/ais_backfill_manifest.csv", index=False)
    
    # 4. Create processed manifest
    df_proc_manifest = pd.DataFrame({
        "partition_id": ["Wikinger_2020_01"],
        "farm_id": ["Wikinger"],
        "year": [2020],
        "month": [1],
        "input_rows": [3],
        "clean_rows": [3],
        "visit_count": [1],
        "dwell_count": [1],
        "tier_a_count": [1],
        "tier_b_count": [0],
        "tier_c_count": [0],
        "tier_d_count": [0],
        "processing_seconds": [0.1],
        "status": ["success"]
    })
    df_proc_manifest.to_csv(tmp_path / "Data/Processed/ais_dwell_backfill/logs/backfill_manifest.csv", index=False)
    
    # 5. Create dwells parquets
    dwell_dir = tmp_path / "Data/Processed/ais_dwell_backfill/dwells/farm_id=Wikinger/year=2020/month=1"
    dwell_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({
        "dwell_id": ["d1"], 
        "dwell_tier": ["Tier A"],
        "possible_cross_farm_duplicate": [False]
    }).to_parquet(dwell_dir / "data.parquet", index=False)

    return tmp_path

def test_enums_stable():
    """Verifies pipeline status enums are stable and map to correct values."""
    assert RawSourceStatus.MISSING.value == "missing"
    assert RawSourceStatus.EXISTS.value == "exists"
    assert RawSourceStatus.EMPTY.value == "empty"
    
    assert FilteredAisStatus.INTERRUPTED.value == "interrupted"
    assert VisitsStatus.MISSING.value == "missing"
    assert DwellsStatus.EXISTS.value == "exists"

def test_build_index_default_fast(mock_isolated_tree):
    """Verifies that by default build_index does not parse CSV for raw rows, returning -1 or manifest lookup."""
    # Run the indexer without --deep (deep=False) and using isolated root
    df_index = bdi.build_index(mock_isolated_tree, deep=False)
    
    # Check outputs
    out_file = mock_isolated_tree / "Data/Processed/dashboard/data_index.parquet"
    assert out_file.exists()
    
    # Retrieve Wikinger 2020-01 partition which exists in the manifest logs
    row_manifest = df_index[
        (df_index["farm_id"] == "Wikinger") & 
        (df_index["year"] == 2020) & 
        (df_index["month"] == 1)
    ].iloc[0]
    # Retreived 3 rows from processed manifest
    assert row_manifest["raw_source_rows"] == 3
    
    # Retrieve Arkona-Becken_Südost 2020-01 partition which is NOT in processed manifests but has a raw file
    row_no_manifest = df_index[
        (df_index["farm_id"] == "Arkona-Becken_Südost") & 
        (df_index["year"] == 2020) & 
        (df_index["month"] == 1)
    ].iloc[0]
    # Row scan is skipped by default -> returns -1
    assert row_no_manifest["raw_source_rows"] == -1
    assert row_no_manifest["raw_source_status"] == RawSourceStatus.EXISTS.value

def test_build_index_deep_scan(mock_isolated_tree):
    """Verifies that when deep=True is passed, build_index performs exact CSV line scans for raw files."""
    df_index = bdi.build_index(mock_isolated_tree, deep=True)
    
    # Retrieve Arkona-Becken_Südost 2020-01 partition which is NOT in processed manifests but raw file has 3 rows
    row = df_index[
        (df_index["farm_id"] == "Arkona-Becken_Südost") & 
        (df_index["year"] == 2020) & 
        (df_index["month"] == 1)
    ].iloc[0]
    # Scanned exact count -> 3 rows!
    assert row["raw_source_rows"] == 3
    assert row["raw_source_status"] == RawSourceStatus.EXISTS.value

def test_build_index_zero_byte_interrupted(mock_isolated_tree):
    """Verifies zero-byte raw file is marked as empty and missing months as missing."""
    df_index = bdi.build_index(mock_isolated_tree, deep=False)
    
    # Zero-byte 2020-02 raw file
    row_empty = df_index[
        (df_index["farm_id"] == "Wikinger") & 
        (df_index["year"] == 2020) & 
        (df_index["month"] == 2)
    ].iloc[0]
    assert row_empty["raw_source_status"] == RawSourceStatus.EMPTY.value
    
    # Missing 2020-05 month
    row_missing = df_index[
        (df_index["farm_id"] == "Wikinger") & 
        (df_index["year"] == 2020) & 
        (df_index["month"] == 5)
    ].iloc[0]
    assert row_missing["raw_source_status"] == RawSourceStatus.MISSING.value
    assert row_missing["dwells_status"] == DwellsStatus.MISSING.value
