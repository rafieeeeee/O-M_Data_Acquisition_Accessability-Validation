"""
dwell_weather_join.py

Wrapper for Phase-Based Metocean Exposure Join (NORA3 wave and wind).
Extracts wave/wind parameters across approach, active, departure, and matched comparator phases.
"""

import pandas as pd
from om_pipeline.features.dwell_weather_join import WeatherJoiner as _WeatherJoiner

class WeatherJoiner(_WeatherJoiner):
    """
    Wrapper for WeatherJoiner to extract metocean features for dwell candidates.
    """
    pass
