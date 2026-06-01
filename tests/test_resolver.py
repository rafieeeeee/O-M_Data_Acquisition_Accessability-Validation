"""
test_resolver.py

Unit tests for resolver.py verifying path resolution, turbine columns, empty dwell checks, and schemas.
"""

from pathlib import Path
import pandas as pd
import pytest

from om_pipeline.dashboard.resolver import (
    resolve_dwell_parquet_path,
    validate_turbine_columns,
    is_dwell_month_empty,
    check_dwell_schema
)

def test_resolve_dwell_parquet_path():
    """Asserts that the structured partitions path is constructed correctly."""
    root = Path("/mock/root")
    path = resolve_dwell_parquet_path(root, "Wikinger", 2020, 5)
    expected = Path("/mock/root/Data/Processed/ais_dwell_backfill/dwells/farm_id=Wikinger/year=2020/month=5/data.parquet")
    assert path == expected
    
    # Handle empty/missing inputs
    assert resolve_dwell_parquet_path(None, "Wikinger", 2020, 5) == Path()
    assert resolve_dwell_parquet_path(root, None, 2020, 5) == Path()

def test_validate_turbine_columns():
    """Asserts that turbine coordinates columns are correctly validated."""
    # Healthy case
    df_healthy = pd.DataFrame(columns=["latitude", "longitude", "wind_farm", "oem_manufacturer"])
    assert validate_turbine_columns(df_healthy) is True
    
    # Missing columns
    df_missing = pd.DataFrame(columns=["latitude", "wind_farm"])
    assert validate_turbine_columns(df_missing) is False
    
    # Empty DataFrame
    assert validate_turbine_columns(pd.DataFrame()) is False

def test_is_dwell_month_empty():
    """Asserts that empty dwell months are identified correctly."""
    # Healthy non-empty case
    df_data = pd.DataFrame([{"dwell_id": "d1"}])
    assert is_dwell_month_empty(df_data) is False
    
    # Empty case
    assert is_dwell_month_empty(pd.DataFrame()) is True
    assert is_dwell_month_empty(None) is True

def test_check_dwell_schema():
    """Asserts that dwell schema validations require the core coordinate and metadata columns."""
    # Expected schema columns
    cols = ["dwell_id", "visit_id", "mmsi", "dwell_tier", "centroid_lat", "centroid_lon", "extra"]
    df_healthy = pd.DataFrame([{"dwell_id": "d1"}], columns=cols)
    assert check_dwell_schema(df_healthy) is True
    
    # Missing required schema columns (e.g. centroid_lat)
    df_bad = pd.DataFrame([{"dwell_id": "d1"}], columns=["dwell_id", "visit_id", "mmsi", "dwell_tier", "centroid_lon"])
    assert check_dwell_schema(df_bad) is False
    
    # None/Empty
    assert check_dwell_schema(None) is False
    assert check_dwell_schema(pd.DataFrame()) is False

def test_resolve_missing_dwell_parquet(tmp_path):
    """Asserts that a missing dwell parquet path is resolved correctly and identifies as not existing."""
    # A path that does not exist
    path = resolve_dwell_parquet_path(tmp_path, "Wikinger", 2020, 12)
    assert path.exists() is False

def test_resolve_active_dwell_month_with_schema(tmp_path):
    """Asserts that an active dwell month parquet can be successfully created, resolved, and verified."""
    # Create the structured directories
    dwell_dir = tmp_path / "Data/Processed/ais_dwell_backfill/dwells/farm_id=Wikinger/year=2020/month=1"
    dwell_dir.mkdir(parents=True, exist_ok=True)
    
    # Define a healthy dwells schema
    cols = ["dwell_id", "visit_id", "mmsi", "dwell_tier", "centroid_lat", "centroid_lon"]
    df_data = pd.DataFrame([
        {
            "dwell_id": "d1",
            "visit_id": "v1",
            "mmsi": 211000000,
            "dwell_tier": "Tier A",
            "centroid_lat": 54.0,
            "centroid_lon": 14.0
        }
    ], columns=cols)
    
    # Save parquet file
    pq_file = dwell_dir / "data.parquet"
    df_data.to_parquet(pq_file, index=False)
    
    # Resolve using helper
    resolved_path = resolve_dwell_parquet_path(tmp_path, "Wikinger", 2020, 1)
    assert resolved_path == pq_file
    assert resolved_path.exists() is True
    
    # Load and validate schema
    df_loaded = pd.read_parquet(resolved_path)
    assert is_dwell_month_empty(df_loaded) is False
    assert check_dwell_schema(df_loaded) is True

