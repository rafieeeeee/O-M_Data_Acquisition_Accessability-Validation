import os
import pandas as pd
import xarray as xr
from ..common.paths import DATA_DIR

NORA3_CACHE_DIR = os.path.join(DATA_DIR, "Raw", "Metocean", "NORA3")

def fetch_nora3_point(lat, lon, time_start, time_end, thredds_url=None):
    """
    Fetches NORA3 wave data (hs, tp, direction) for a specific coordinate and time window.
    Downloads are cached locally.
    
    Args:
        lat (float): Latitude
        lon (float): Longitude
        time_start (datetime): Start time of the window
        time_end (datetime): End time of the window
        thredds_url (str): Optional OPenDAP endpoint. Defaults to MET Norway NORA3 aggregation.
    """
    os.makedirs(NORA3_CACHE_DIR, exist_ok=True)
    
    # Round to 2 decimal places to share cache across nearby turbines within the 3km NORA3 grid
    lat_rounded = round(lat, 2)
    lon_rounded = round(lon, 2)
    
    start = pd.to_datetime(time_start)
    end = pd.to_datetime(time_end)
    # Pad time window by 2 hours on each side to ensure interpolation boundaries are clean
    padded_start = start - pd.Timedelta(hours=2)
    padded_end = end + pd.Timedelta(hours=2)
    
    # We cache by coordinate and month to batch requests for multiple events in the same month
    month_str = start.strftime("%Y_%m")
    cache_filename = f"nora3_raw_{lat_rounded:.2f}_{lon_rounded:.2f}_{month_str}.csv"
    cache_path = os.path.join(NORA3_CACHE_DIR, cache_filename)
    
    if os.path.exists(cache_path):
        # Read cache
        df = pd.read_csv(cache_path, parse_dates=['time'])
        # Return padded subset for interpolation bracketing
        return df[(df['time'] >= padded_start) & (df['time'] <= padded_end)].copy()
        
    print(f"Network Fetch: NORA3 THREDDS for Lat: {lat:.3f}, Lon: {lon:.3f} ({month_str})")
    
    if thredds_url is None:
        # Construct monthly active NORA3 subset wave OPeNDAP URL
        # Format: https://thredds.met.no/thredds/dodsC/nora3_subset_wave/wave_tser/{YYYY}{MM}_NORA3wave_sub_time_unlimited.nc
        year_month_str = start.strftime("%Y%m")
        thredds_url = f"https://thredds.met.no/thredds/dodsC/nora3_subset_wave/wave_tser/{year_month_str}_NORA3wave_sub_time_unlimited.nc"
        print(f"Using active NORA3 monthly URL: {thredds_url}")
        
    try:
        # Open DAP connection
        ds = xr.open_dataset(thredds_url)
        
        # Standardize variable names (hs, tp, mwd/thq)
        target_vars = ['hs', 'tp', 'thq', 'latitude', 'longitude', 'lat', 'lon'] 
        available_vars = [v for v in target_vars if v in ds.variables or v in ds.coords]
        if not any(v in ['hs', 'tp', 'thq'] for v in available_vars):
            # Fallbacks
            target_vars = ['Hs', 'Tp', 'mwd', 'dir', 'wave_direction', 'latitude', 'longitude', 'lat', 'lon']
            available_vars = [v for v in target_vars if v in ds.variables or v in ds.coords]
            
        ds = ds[available_vars]
        
        # Fetch full calendar month(s) covering both start and end, plus a 2-hour tail for padding overlap
        fetch_start = start.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        fetch_end = end.replace(day=1, hour=0, minute=0, second=0, microsecond=0) + pd.DateOffset(months=1) + pd.Timedelta(hours=2)
        
        # Perform server-side subsetting
        # Note: 2024 monthly subsets use rotated coordinates (rlat, rlon) 
        # with 2D latitude/longitude auxiliary arrays.
        if 'latitude' in ds.variables and 'longitude' in ds.variables and ds['latitude'].ndim == 2:
            import numpy as np
            
            # Load lat/lon arrays to find nearest index
            lat_coord = ds['latitude']
            lon_coord = ds['longitude']
            lats = lat_coord.values
            lons = lon_coord.values
            
            # Compute Euclidean distance to find nearest grid cell
            dist = (lats - lat_rounded)**2 + (lons - lon_rounded)**2
            idx = np.unravel_index(dist.argmin(), dist.shape)
            
            indexers = {dim: index for dim, index in zip(lat_coord.dims, idx)}
            subset = ds.isel(indexers)
            lat_var = 'latitude'
            lon_var = 'longitude'
        else:
            # Standard 1D coordinates (lat/lon)
            lat_var = 'lat' if 'lat' in ds.coords else 'latitude'
            lon_var = 'lon' if 'lon' in ds.coords else 'longitude'
            subset = ds.sel({lat_var: lat_rounded, lon_var: lon_rounded}, method="nearest")
            
        # Temporal slice
        time_var = 'time'
        subset = subset.sel({time_var: slice(fetch_start, fetch_end)})
        
        # Load into memory
        df = subset.to_dataframe().reset_index()
        
        # Standardize column output
        df = df.rename(columns={time_var: 'time', lat_var: 'lat', lon_var: 'lon'})
        
        # Normalize variables to our target schema
        rename_map = {}
        for col in df.columns:
            if col.lower() == 'hs' and col != 'hs': rename_map[col] = 'hs'
            elif col.lower() == 'tp' and col != 'tp': rename_map[col] = 'tp'
            elif col.lower() in ['thq', 'mwd', 'dir', 'wave_direction'] and col != 'wave_direction': 
                rename_map[col] = 'wave_direction'
        
        df = df.rename(columns=rename_map)
            
        # Ensure standard types
        df['time'] = pd.to_datetime(df['time'])
        
        # Save raw fetched data to cache
        df.to_csv(cache_path, index=False)
        print(f"Cached {len(df)} hourly NORA3 records to {cache_path}")
        # Return padded subset for interpolation bracketing
        return df[(df['time'] >= padded_start) & (df['time'] <= padded_end)].copy()
        
    except Exception as e:
        print(f"Failed to fetch from THREDDS ({thredds_url}): {e}")
        # Return an empty df matching the expected schema to allow pipeline to proceed
        return pd.DataFrame(columns=['time', 'lat', 'lon', 'hs', 'tp', 'wave_direction'])

def fetch_nora3_wind(lat, lon, time_start, time_end, thredds_url=None):
    """
    Fetches NORA3 atmospheric wind data (wind_speed_10m, wind_direction_10m,
    wind_speed_100m, wind_direction_100m) for a specific coordinate and time window.
    Downloads are cached locally.
    
    Args:
        lat (float): Latitude
        lon (float): Longitude
        time_start (datetime): Start time of the window
        time_end (datetime): End time of the window
        thredds_url (str): Optional OPenDAP endpoint.
    """
    os.makedirs(NORA3_CACHE_DIR, exist_ok=True)
    
    # Round to 2 decimal places to share cache across nearby turbines within the 3km NORA3 grid
    lat_rounded = round(lat, 2)
    lon_rounded = round(lon, 2)
    
    start = pd.to_datetime(time_start)
    end = pd.to_datetime(time_end)
    # Pad time window by 2 hours on each side to ensure interpolation boundaries are clean
    padded_start = start - pd.Timedelta(hours=2)
    padded_end = end + pd.Timedelta(hours=2)
    
    # We cache by coordinate and month to batch requests for multiple events in the same month
    month_str = start.strftime("%Y_%m")
    cache_filename = f"nora3_wind_raw_{lat_rounded:.2f}_{lon_rounded:.2f}_{month_str}.csv"
    cache_path = os.path.join(NORA3_CACHE_DIR, cache_filename)
    
    if os.path.exists(cache_path):
        # Read cache
        df = pd.read_csv(cache_path, parse_dates=['time'])
        # Return padded subset for interpolation bracketing
        return df[(df['time'] >= padded_start) & (df['time'] <= padded_end)].copy()
        
    print(f"Network Fetch: NORA3 Wind THREDDS for Lat: {lat:.3f}, Lon: {lon:.3f} ({month_str})")
    
    if thredds_url is None:
        year_month_str = start.strftime("%Y%m")
        thredds_url = f"https://thredds.met.no/thredds/dodsC/nora3_subset_atmos/wind_hourly_v2/arome3kmwind_1hr_{year_month_str}.nc"
        print(f"Using active NORA3 monthly wind URL: {thredds_url}")
        
    try:
        # Open DAP connection
        ds = xr.open_dataset(thredds_url)
        
        # Fetch full calendar month(s) covering both start and end, plus a 2-hour tail for padding overlap
        fetch_start = start.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        fetch_end = end.replace(day=1, hour=0, minute=0, second=0, microsecond=0) + pd.DateOffset(months=1) + pd.Timedelta(hours=2)
        
        # Nearest spatial point search using rotated grid coordinates
        if 'latitude' in ds.variables and 'longitude' in ds.variables and ds['latitude'].ndim == 2:
            import numpy as np
            
            # Load lat/lon arrays to find nearest index
            lat_coord = ds['latitude']
            lon_coord = ds['longitude']
            lats = lat_coord.values
            lons = lon_coord.values
            
            # Compute Euclidean distance to find nearest grid cell
            dist = (lats - lat_rounded)**2 + (lons - lon_rounded)**2
            idx = np.unravel_index(dist.argmin(), dist.shape)
            
            indexers = {dim: index for dim, index in zip(lat_coord.dims, idx)}
            subset = ds.isel(indexers)
        else:
            # Standard 1D coordinates (lat/lon)
            lat_var = 'lat' if 'lat' in ds.coords else 'latitude'
            lon_var = 'lon' if 'lon' in ds.coords else 'longitude'
            subset = ds.sel({lat_var: lat_rounded, lon_var: lon_rounded}, method="nearest")
            
        # Select 10m and 100m heights and slice temporally
        time_var = 'time'
        
        # height=10
        subset_10 = subset.sel(height=10).sel({time_var: slice(fetch_start, fetch_end)})
        df_10 = subset_10[['wind_speed', 'wind_direction']].to_dataframe().reset_index()
        df_10 = df_10.rename(columns={'wind_speed': 'wind_speed_10m', 'wind_direction': 'wind_direction_10m'})
        
        # height=100
        subset_100 = subset.sel(height=100).sel({time_var: slice(fetch_start, fetch_end)})
        df_100 = subset_100[['wind_speed', 'wind_direction']].to_dataframe().reset_index()
        df_100 = df_100.rename(columns={'wind_speed': 'wind_speed_100m', 'wind_direction': 'wind_direction_100m'})
        
        # Merge on time and subset columns
        df_wind = pd.merge(
            df_10[[time_var, 'wind_speed_10m', 'wind_direction_10m']],
            df_100[[time_var, 'wind_speed_100m', 'wind_direction_100m']],
            on=time_var
        )
        
        df_wind = df_wind.rename(columns={time_var: 'time'})
        df_wind['lat'] = lat_rounded
        df_wind['lon'] = lon_rounded
        
        # Ensure standard types
        df_wind['time'] = pd.to_datetime(df_wind['time'])
        
        # Save to cache
        df_wind.to_csv(cache_path, index=False)
        print(f"Cached {len(df_wind)} hourly NORA3 wind records to {cache_path}")
        return df_wind[(df_wind['time'] >= padded_start) & (df_wind['time'] <= padded_end)].copy()
        
    except Exception as e:
        print(f"Failed to fetch wind from THREDDS ({thredds_url}): {e}")
        return pd.DataFrame(columns=['time', 'lat', 'lon', 'wind_speed_10m', 'wind_direction_10m', 'wind_speed_100m', 'wind_direction_100m'])

