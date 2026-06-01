"""
resolver.py

Pure helper functions to resolve dwell parquet file paths and validate
coordinate and schema integrity for turbines and dwells.
"""

from pathlib import Path
import pandas as pd

def resolve_dwell_parquet_path(root_path: Path, farm_id: str, year: int, month: int) -> Path:
    """
    Resolves the structured partition path for a processed dwell parquet file.
    """
    if not root_path or not farm_id:
        return Path()
    return root_path / f"Data/Processed/ais_dwell_backfill/dwells/farm_id={farm_id}/year={year}/month={month}/data.parquet"

def validate_turbine_columns(df_turbines: pd.DataFrame) -> bool:
    """
    Validates that the turbine DataFrame contains the required spatial coordinate fields.
    """
    if df_turbines is None:
        return False
    required = {"latitude", "longitude", "wind_farm"}
    return required.issubset(df_turbines.columns)

def is_dwell_month_empty(df_dwells: pd.DataFrame) -> bool:
    """
    Determines if a loaded dwells DataFrame is empty of events.
    """
    return df_dwells is None or df_dwells.empty or len(df_dwells) == 0

def check_dwell_schema(df_dwells: pd.DataFrame) -> bool:
    """
    Checks that the dwells DataFrame contains the core O&M dwell metadata columns.
    """
    if df_dwells is None or df_dwells.empty:
        return False
    required = {"dwell_id", "visit_id", "mmsi", "dwell_tier", "centroid_lat", "centroid_lon"}
    return required.issubset(df_dwells.columns)
