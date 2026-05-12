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
    
    start = pd.to_datetime(time_start)
    end = pd.to_datetime(time_end)
    # Pad time window by 2 hours on each side to ensure interpolation boundaries are clean
    padded_start = start - pd.Timedelta(hours=2)
    padded_end = end + pd.Timedelta(hours=2)
    
    # We cache by coordinate and month to batch requests for multiple events in the same month
    month_str = start.strftime("%Y_%m")
    cache_filename = f"nora3_raw_{lat:.4f}_{lon:.4f}_{month_str}.csv"
    cache_path = os.path.join(NORA3_CACHE_DIR, cache_filename)
    
    if os.path.exists(cache_path):
        # Read cache
        df = pd.read_csv(cache_path, parse_dates=['time'])
        # Return padded subset for interpolation bracketing
        return df[(df['time'] >= padded_start) & (df['time'] <= padded_end)].copy()
        
    print(f"Network Fetch: NORA3 THREDDS for Lat: {lat:.3f}, Lon: {lon:.3f} ({month_str})")
    
    if thredds_url is None:
        # For 2024+ data, MET Norway often provides monthly pre-aggregated subsets 
        # that are more reliable than the global aggregation.
        subset_month = start.strftime("%Y%m")
        thredds_url = f"https://thredds.met.no/thredds/dodsC/nora3_subset_wave/wave_tser/{subset_month}_NORA3wave_sub_time_unlimited.nc"
        print(f"Using monthly subset endpoint: {thredds_url}")
    
    try:
        # Check if the monthly URL is reachable, otherwise fallback to the user's previously verified global agg
        import requests
        resp = requests.head(thredds_url + ".dds", timeout=5)
        if resp.status_code != 200:
            print(f"Monthly subset not found ({resp.status_code}). Falling back to global aggregation...")
            thredds_url = "https://thredds.met.no/thredds/dodsC/windsurfer/mywavewam3km_files/aggregate/nora3_wave_agg.nc"
    except Exception as e:
        print(f"Connectivity check failed: {e}. Attempting monthly URL anyway...")
        
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
        if 'rlat' in ds.dims and 'rlon' in ds.dims:
            # Find nearest point in 2D coordinates
            # We compute distance on the server (DAP) or locally after a small spatial crop
            # Since these are monthly 1D timeseries per point, we find the index first.
            import numpy as np
            
            # Load lat/lon arrays to find nearest index
            lats = ds['latitude'].values
            lons = ds['longitude'].values
            
            # Compute Euclidean distance to find nearest grid cell
            dist = (lats - lat)**2 + (lons - lon)**2
            idx = np.unravel_index(dist.argmin(), dist.shape)
            
            # Select the point by index
            subset = ds.isel(rlon=idx[0], rlat=idx[1])
            lat_var = 'latitude'
            lon_var = 'longitude'
        else:
            # Standard 1D coordinates (lat/lon)
            lat_var = 'lat' if 'lat' in ds.coords else 'latitude'
            lon_var = 'lon' if 'lon' in ds.coords else 'longitude'
            subset = ds.sel({lat_var: lat, lon_var: lon}, method="nearest")
            
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
