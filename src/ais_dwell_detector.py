"""
ais_dwell_detector.py

Extracts tiered dwell candidates from AIS visit tracks based on speed, duration,
and proximity parameters.
"""

import pandas as pd
from om_pipeline.identification.ais_dwell_detector import detect_dwell_events as _detect_dwell_events

def detect_dwell_events(df_visits: pd.DataFrame, geoms_utm: dict, farm_id: str | None = None, event_year: int | None = None) -> pd.DataFrame:
    """
    Extracts tiered dwell candidates from segmented visit tracks.
    
    Tiers:
      - Tier A: Asset-Proximal (<= 200 m to turbine, SOG <= 1.0 kn, >= 20 min)
      - Tier B: Farm-Internal Slow (Inside farm poly, SOG <= 1.0 kn, >= 30 min)
      - Tier C: Slow Operational (Inside farm/asset buffer, 1.0 < SOG <= 3.0 kn, >= 20 min)
      - Tier D: Context Holding (Inside context, outside farm, SOG <= 1.0 kn, >= 30 min)
    """
    return _detect_dwell_events(df_visits, geoms_utm, farm_id=farm_id, event_year=event_year)
