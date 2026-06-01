# Copernicus NWS Variable Schema and Aliases

WAVE_ALIASES = {
    "hs": ["VHM0", "swh", "hs", "Hm0", "significant_wave_height"],
    "tp": ["VTPK", "tp", "peak_wave_period", "wave_period"],
    "tm": ["VTM10", "tm", "mean_wave_period"],
    "dir": ["VMDR", "mwd", "wave_direction"],
}

CURRENT_ALIASES = {
    "uo": ["uo", "u", "eastward_sea_water_velocity"],
    "vo": ["vo", "v", "northward_sea_water_velocity"],
    "speed": ["current_speed", "sea_water_speed"],
    "dir": ["current_direction", "sea_water_velocity_direction"],
}

# The required final columns in the extracted NWS features Parquet file
REQUIRED_OUTPUT_COLUMNS = [
    "event_id",
    # Wave Features
    "nws_wave_hs_mean",
    "nws_wave_hs_max",
    "nws_wave_tp_mean",
    "nws_wave_tp_max",
    "nws_wave_tm_mean",
    "nws_wave_dir_mean",
    # Current Features
    "nws_current_u_mean",
    "nws_current_v_mean",
    "nws_current_speed_mean",
    "nws_current_speed_max",
    "nws_current_dir_mean",
    # Provenance / QA
    "nws_event_midpoint_utc",
    "nws_matched_time_utc",
    "nws_temporal_offset_minutes",
    "nws_temporal_offset_exceeds_threshold",
    "nws_grid_lat",
    "nws_grid_lon",
    "nws_spatial_distance_km",
    "nws_spatial_match_status", # "ok", "out_of_bounds_or_too_far"
    "nws_assignment_method",    # "event_position", "asset_position", "farm_centroid"
    "nws_assignment_status",    # "ok", "missing_asset_coordinates"
    "nws_time_method",          # "nearest"
    "nws_wave_source_file",
    "nws_current_source_file"
]
