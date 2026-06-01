import os
import numpy as np
import pandas as pd
import xarray as xr
from ..common.paths import INTERIM_DIR
from .metocean_schema import WAVE_ALIASES, CURRENT_ALIASES, REQUIRED_OUTPUT_COLUMNS

def haversine_distance(lat1, lon1, lat2, lon2):
    """
    Computes geodesic distance in km between points using the Haversine formula.
    """
    R = 6371.0  # Earth radius in km
    phi1 = np.radians(lat1)
    phi2 = np.radians(lat2)
    dphi = np.radians(lat2 - lat1)
    dlambda = np.radians(lon2 - lon1)

    a = np.sin(dphi/2.0)**2 + np.cos(phi1) * np.cos(phi2) * np.sin(dlambda/2.0)**2
    c = 2.0 * np.arctan2(np.sqrt(a), np.sqrt(1.0 - a))
    return R * c

class NWSExtractor:
    def __init__(self, turbine_coordinates_path=None):
        if turbine_coordinates_path is None:
            turbine_coordinates_path = os.path.join(INTERIM_DIR, "European_Turbine_Coordinates.csv")

        self.turbine_coordinates_path = turbine_coordinates_path
        self._df_turbines = None

    def _load_turbines(self):
        if self._df_turbines is None:
            if os.path.exists(self.turbine_coordinates_path):
                self._df_turbines = pd.read_csv(self.turbine_coordinates_path)
            else:
                self._df_turbines = pd.DataFrame(columns=['wind_farm', 'latitude', 'longitude'])
        return self._df_turbines

    def resolve_target_coordinates(self, event_row, assignment_mode):
        """
        Resolves the target latitude and longitude for an event based on assignment_mode.
        Returns (lat, lon, assignment_method, assignment_status).
        """
        centroid_lat = event_row['centroid_lat']
        centroid_lon = event_row['centroid_lon']
        wind_farm = event_row.get('wind_farm', None)

        if assignment_mode == "event_position":
            return centroid_lat, centroid_lon, "event_position", "ok"

        elif assignment_mode == "asset_position":
            if not wind_farm or pd.isna(wind_farm):
                return np.nan, np.nan, "asset_position", "missing_asset_coordinates"

            df_turb = self._load_turbines()
            df_farm_turb = df_turb[df_turb['wind_farm'] == wind_farm]

            if df_farm_turb.empty:
                return np.nan, np.nan, "asset_position", "missing_asset_coordinates"

            # Find nearest turbine
            dists = haversine_distance(
                centroid_lat, centroid_lon,
                df_farm_turb['latitude'].values, df_farm_turb['longitude'].values
            )
            nearest_idx = dists.argmin()
            nearest_row = df_farm_turb.iloc[nearest_idx]

            return nearest_row['latitude'], nearest_row['longitude'], "asset_position", "ok"

        elif assignment_mode == "farm_centroid":
            if not wind_farm or pd.isna(wind_farm):
                return np.nan, np.nan, "farm_centroid", "missing_asset_coordinates"

            df_turb = self._load_turbines()
            df_farm_turb = df_turb[df_turb['wind_farm'] == wind_farm]

            if df_farm_turb.empty:
                return np.nan, np.nan, "farm_centroid", "missing_asset_coordinates"

            # Calculate mean coordinates of turbines
            mean_lat = df_farm_turb['latitude'].mean()
            mean_lon = df_farm_turb['longitude'].mean()
            return mean_lat, mean_lon, "farm_centroid", "ok"

        else:
            raise ValueError(f"Unknown assignment mode: {assignment_mode}")

    def find_variable_name(self, ds, aliases):
        """
        Finds the actual variable name in the dataset matching the list of aliases.
        """
        for alias in aliases:
            if alias in ds.variables:
                return alias
        return None

    def extract_features(self, df_events, wave_dir, current_dir,
                         assignment_mode="event_position",
                         time_method="nearest",
                         max_time_offset_minutes=90,
                         max_spatial_distance_km=25.0,
                         limit_events=None):
        """
        Performs NWS feature extraction for events.
        """
        if time_method == "interpolate":
            raise NotImplementedError("Interpolation is not implemented yet.")
        elif time_method != "nearest":
            raise ValueError(f"Unknown time method: {time_method}")

        df = df_events.copy()
        if limit_events:
            df = df.head(limit_events)

        # Parse timestamps
        df['start_utc'] = pd.to_datetime(df['start_utc'])
        df['end_utc'] = pd.to_datetime(df['end_utc'])

        # Calculate midpoint in datetime64[ns]
        df['midpoint_utc'] = df['start_utc'] + (df['end_utc'] - df['start_utc']) / 2

        # Group events by year to process NetCDF files year-by-year
        df['event_year'] = df['midpoint_utc'].dt.year

        results = []

        for year, df_year in df.groupby('event_year'):
            print(f"Processing year {year} ({len(df_year)} events)...")

            # 1. Open year NetCDF files
            wave_path = os.path.join(wave_dir, f"NWS_Wave_Reanalysis_{year}.nc")
            current_path = os.path.join(current_dir, f"NWS_Currents_Reanalysis_{year}.nc")

            ds_wav = None
            ds_cur = None

            if os.path.exists(wave_path):
                try:
                    ds_wav = xr.open_dataset(wave_path)
                except Exception as e:
                    print(f"Error opening wave file {wave_path}: {e}")
            else:
                print(f"Wave reanalysis file not found for year {year}: {wave_path}")

            if os.path.exists(current_path):
                try:
                    ds_cur = xr.open_dataset(current_path)
                except Exception as e:
                    print(f"Current reanalysis file not found for year {year}: {current_path}")
            else:
                print(f"Current reanalysis file not found for year {year}: {current_path}")

            # 2. Extract features for each event in this year
            for idx, row in df_year.iterrows():
                event_id = row['event_id']
                mid_time = row['midpoint_utc']
                if hasattr(mid_time, 'tz') and mid_time.tz is not None:
                    mid_time = mid_time.tz_localize(None)

                # Resolve spatial target coordinates
                target_lat, target_lon, assign_method, assign_status = self.resolve_target_coordinates(row, assignment_mode)

                event_res = {
                    "event_id": event_id,
                    "nws_event_midpoint_utc": mid_time,
                    "nws_assignment_method": assign_method,
                    "nws_assignment_status": assign_status,
                    "nws_time_method": "nearest",
                    "nws_spatial_match_status": "out_of_bounds_or_too_far",
                    "nws_grid_lat": np.nan,
                    "nws_grid_lon": np.nan,
                    "nws_spatial_distance_km": np.nan,
                    "nws_matched_time_utc": pd.NaT,
                    "nws_temporal_offset_minutes": np.nan,
                    "nws_temporal_offset_exceeds_threshold": True,
                    "nws_wave_source_file": os.path.basename(wave_path) if ds_wav else "Missing",
                    "nws_current_source_file": os.path.basename(current_path) if ds_cur else "Missing"
                }

                # Initialize variables to NaN
                for col in REQUIRED_OUTPUT_COLUMNS:
                    if col not in event_res:
                        event_res[col] = np.nan

                if assign_status != "ok" or pd.isna(target_lat) or pd.isna(target_lon):
                    results.append(event_res)
                    continue

                # 3. Wave extraction
                wav_matched = False
                wav_hs = wav_tp = wav_tm = wav_dir = np.nan
                wav_time = pd.NaT
                wav_offset = np.nan
                wav_offset_exceeds = True
                wav_grid_lat = wav_grid_lon = np.nan
                wav_dist = np.nan

                if ds_wav is not None:
                    # Find nearest grid point
                    wav_lat_arr = ds_wav.latitude.values
                    wav_lon_arr = ds_wav.longitude.values

                    # Bounding box check
                    in_wav_bbox = (wav_lat_arr.min() <= target_lat <= wav_lat_arr.max()) and \
                                  (wav_lon_arr.min() <= target_lon <= wav_lon_arr.max())

                    wav_lat_idx = np.abs(wav_lat_arr - target_lat).argmin()
                    wav_lon_idx = np.abs(wav_lon_arr - target_lon).argmin()

                    wav_grid_lat = wav_lat_arr[wav_lat_idx]
                    wav_grid_lon = wav_lon_arr[wav_lon_idx]
                    wav_dist = haversine_distance(target_lat, target_lon, wav_grid_lat, wav_grid_lon)

                    if in_wav_bbox and wav_dist <= max_spatial_distance_km:
                        # Extract 1D time-series at that grid point
                        event_ds_wav = ds_wav.isel(latitude=wav_lat_idx, longitude=wav_lon_idx)

                        # Find nearest time index
                        wav_times = event_ds_wav.time.values
                        wav_time_idx = np.abs(wav_times - mid_time.to_datetime64()).argmin()
                        wav_time = pd.to_datetime(wav_times[wav_time_idx])

                        wav_offset = abs(wav_time - mid_time.to_pydatetime()).total_seconds() / 60.0
                        wav_offset_exceeds = wav_offset > max_time_offset_minutes

                        if not wav_offset_exceeds:
                            # Load values
                            time_ds_wav = event_ds_wav.isel(time=wav_time_idx)

                            hs_var = self.find_variable_name(ds_wav, WAVE_ALIASES["hs"])
                            tp_var = self.find_variable_name(ds_wav, WAVE_ALIASES["tp"])
                            tm_var = self.find_variable_name(ds_wav, WAVE_ALIASES["tm"])
                            dir_var = self.find_variable_name(ds_wav, WAVE_ALIASES["dir"])

                            wav_hs = float(time_ds_wav[hs_var].values) if hs_var else np.nan
                            wav_tp = float(time_ds_wav[tp_var].values) if tp_var else np.nan
                            wav_tm = float(time_ds_wav[tm_var].values) if tm_var else np.nan
                            wav_dir = float(time_ds_wav[dir_var].values) if dir_var else np.nan

                            # Fallback if peak period is missing but mean period is present
                            if pd.isna(wav_tp) and not pd.isna(wav_tm):
                                wav_tp = wav_tm

                            wav_matched = True

                # 4. Currents extraction
                cur_matched = False
                cur_uo = cur_vo = cur_speed = cur_dir = np.nan
                cur_time = pd.NaT
                cur_offset = np.nan
                cur_offset_exceeds = True
                cur_grid_lat = cur_grid_lon = np.nan
                cur_dist = np.nan

                if ds_cur is not None:
                    # Find nearest grid point
                    cur_lat_arr = ds_cur.latitude.values
                    cur_lon_arr = ds_cur.longitude.values

                    in_cur_bbox = (cur_lat_arr.min() <= target_lat <= cur_lat_arr.max()) and \
                                  (cur_lon_arr.min() <= target_lon <= cur_lon_arr.max())

                    cur_lat_idx = np.abs(cur_lat_arr - target_lat).argmin()
                    cur_lon_idx = np.abs(cur_lon_arr - target_lon).argmin()

                    cur_grid_lat = cur_lat_arr[cur_lat_idx]
                    cur_grid_lon = cur_lon_arr[cur_lon_idx]
                    cur_dist = haversine_distance(target_lat, target_lon, cur_grid_lat, cur_grid_lon)

                    if in_cur_bbox and cur_dist <= max_spatial_distance_km:
                        event_ds_cur = ds_cur.isel(latitude=cur_lat_idx, longitude=cur_lon_idx)

                        # Find nearest time index
                        cur_times = event_ds_cur.time.values
                        cur_time_idx = np.abs(cur_times - mid_time.to_datetime64()).argmin()
                        cur_time = pd.to_datetime(cur_times[cur_time_idx])

                        cur_offset = abs(cur_time - mid_time.to_pydatetime()).total_seconds() / 60.0
                        cur_offset_exceeds = cur_offset > max_time_offset_minutes

                        if not cur_offset_exceeds:
                            time_ds_cur = event_ds_cur.isel(time=cur_time_idx)

                            uo_var = self.find_variable_name(ds_cur, CURRENT_ALIASES["uo"])
                            vo_var = self.find_variable_name(ds_cur, CURRENT_ALIASES["vo"])

                            cur_uo = float(time_ds_cur[uo_var].values) if uo_var else np.nan
                            cur_vo = float(time_ds_cur[vo_var].values) if vo_var else np.nan

                            # Derive current speed and direction
                            if not pd.isna(cur_uo) and not pd.isna(cur_vo):
                                cur_speed = np.sqrt(cur_uo**2 + cur_vo**2)
                                # Oceanographic convention: direction current flows "to" in degrees clockwise from North
                                # Mathematically, atan2(uo, vo) gives the angle relative to vo (North), clockwise.
                                cur_dir = (np.degrees(np.arctan2(cur_uo, cur_vo)) + 360.0) % 360.0

                            cur_matched = True

                # 5. Populate and write out event records
                # Priority: if we matched waves, we use wave spatial metadata, otherwise current.
                # Since NWS waves are spatial priority (as currents are 0% coverage anyway),
                # this keeps diagnostic fields clean.
                if wav_matched:
                    event_res["nws_spatial_match_status"] = "ok"
                    event_res["nws_grid_lat"] = wav_grid_lat
                    event_res["nws_grid_lon"] = wav_grid_lon
                    event_res["nws_spatial_distance_km"] = wav_dist
                    event_res["nws_matched_time_utc"] = wav_time
                    event_res["nws_temporal_offset_minutes"] = wav_offset
                    event_res["nws_temporal_offset_exceeds_threshold"] = wav_offset_exceeds
                elif cur_matched:
                    event_res["nws_spatial_match_status"] = "ok"
                    event_res["nws_grid_lat"] = cur_grid_lat
                    event_res["nws_grid_lon"] = cur_grid_lon
                    event_res["nws_spatial_distance_km"] = cur_dist
                    event_res["nws_matched_time_utc"] = cur_time
                    event_res["nws_temporal_offset_minutes"] = cur_offset
                    event_res["nws_temporal_offset_exceeds_threshold"] = cur_offset_exceeds
                else:
                    # Not matched, record nearest distance of what was attempted
                    if not pd.isna(wav_dist):
                        event_res["nws_grid_lat"] = wav_grid_lat
                        event_res["nws_grid_lon"] = wav_grid_lon
                        event_res["nws_spatial_distance_km"] = wav_dist
                    elif not pd.isna(cur_dist):
                        event_res["nws_grid_lat"] = cur_grid_lat
                        event_res["nws_grid_lon"] = cur_grid_lon
                        event_res["nws_spatial_distance_km"] = cur_dist

                # Fill Wave metrics
                event_res["nws_wave_hs_mean"] = wav_hs
                event_res["nws_wave_hs_max"] = wav_hs  # same for nearest lookup
                event_res["nws_wave_tp_mean"] = wav_tp
                event_res["nws_wave_tp_max"] = wav_tp
                event_res["nws_wave_tm_mean"] = wav_tm
                event_res["nws_wave_dir_mean"] = wav_dir

                # Fill Current metrics
                event_res["nws_current_u_mean"] = cur_uo
                event_res["nws_current_v_mean"] = cur_vo
                event_res["nws_current_speed_mean"] = cur_speed
                event_res["nws_current_speed_max"] = cur_speed
                event_res["nws_current_dir_mean"] = cur_dir

                results.append(event_res)

            # Close files
            if ds_wav: ds_wav.close()
            if ds_cur: ds_cur.close()

        # Re-merge to preserve original event row indices/structure and build final DataFrame
        out_df = pd.DataFrame(results)

        # Ensure all required output columns are present in exactly the correct order
        for col in REQUIRED_OUTPUT_COLUMNS:
            if col not in out_df.columns:
                out_df[col] = np.nan

        return out_df[REQUIRED_OUTPUT_COLUMNS]
