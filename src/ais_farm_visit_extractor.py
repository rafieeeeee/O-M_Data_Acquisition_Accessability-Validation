"""
ais_farm_visit_extractor.py

Segmenting telemetry tracks into visit episodes starting when a vessel
enters the outer context buffer and ending upon exit or time gap.
"""

import os
import pandas as pd
from om_pipeline.identification.ais_visit_extractor import extract_farm_visits as _extract_farm_visits

def extract_farm_visits(df_proj: pd.DataFrame, farm_id: str, context_boundary_utm) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Wrapper around the package logic to segment tracks into visit episodes.
    
    Args:
        df_proj: DataFrame of points projected to UTM (must have x, y, Timestamp, MMSI).
        farm_id: Identifier for the farm.
        context_boundary_utm: Shapely Polygon of the 5km context buffer (UTM).
    
    Returns:
        tuple containing:
          - DataFrame of points with a 'visit_id' and 'visit_quality_flag'.
          - DataFrame of visit metadata [visit_id, mmsi, start, end, duration, etc.].
    """
    return _extract_farm_visits(df_proj, farm_id, context_boundary_utm)

def save_visits_catalog(df_visits_metadata: pd.DataFrame, output_path: str):
    """
    Saves the visit metadata catalog as a Parquet file.
    """
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df_visits_metadata.to_parquet(output_path, index=False)
