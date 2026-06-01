"""
ais_cleaning.py

AIS Cleaning entrypoint. Imports clean_ais_telemetry, generate_cleaning_report,
and calculate_track_speed from the main om_pipeline package.
"""

from om_pipeline.identification.ais_cleaning import (
    clean_ais_telemetry,
    calculate_track_speed,
    generate_cleaning_report
)
