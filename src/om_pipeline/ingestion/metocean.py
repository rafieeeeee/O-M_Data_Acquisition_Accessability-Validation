import os
import numpy as np
import pandas as pd
from ..common.paths import PROCESSED_DIR

def circular_interpolate(theta_start, theta_end, weight):
    """
    Interpolate between two angles (in degrees) using circular interpolation.
    
    Args:
        theta_start: Starting angle in degrees.
        theta_end: Ending angle in degrees.
        weight: Interpolation weight (0.0 to 1.0).
        
    Returns:
        Interpolated angle in degrees [0, 360).
    """
    # Convert to radians
    r1 = np.radians(theta_start)
    r2 = np.radians(theta_end)
    
    # Decompose to unit vector components
    x1, y1 = np.cos(r1), np.sin(r1)
    x2, y2 = np.cos(r2), np.sin(r2)
    
    # Linear interpolate components
    xt = (1 - weight) * x1 + weight * x2
    yt = (1 - weight) * y1 + weight * y2
    
    # Reconstruct angle
    rt = np.arctan2(yt, xt)
    return np.degrees(rt) % 360

class MetoceanIngestor:
    """
    Base class for metocean data ingestion (FINO1, NORA3).
    """
    def __init__(self):
        self.output_dir = os.path.join(PROCESSED_DIR, "metocean")
        os.makedirs(self.output_dir, exist_ok=True)

    def fetch_fino1_wave_data(self, start_date, end_date):
        """
        Placeholder for FINO1 10-minute ground-truth wave spectra.
        Source: FINO1 Database (Hs, Tp, Direction).
        """
        print(f"FETCH: FINO1 wave data from {start_date} to {end_date}")
        # TODO: Implement API/DB client
        pass

    def fetch_nora3_hindcast(self, year, month, bbox):
        """
        Placeholder for NORA3 3km-resolution hindcast data.
        Source: NetCDF files (Hourly).
        """
        print(f"FETCH: NORA3 hindcast for {year}-{month:02d}")
        # TODO: Implement NetCDF streaming/subsetting
        pass

    def upscale_to_10min(self, df_hourly):
        """
        Upscale 1-hour metocean data to 10-minute resolution.
        Uses Cubic Spline for Hs/Tp and Circular Interpolation for Direction.
        
        Args:
            df_hourly (pd.DataFrame): Must contain 'time', 'hs', 'tp', 'wave_direction'
        Returns:
            pd.DataFrame: 10-minute upscaled dataframe.
        """
        if df_hourly.empty:
            return pd.DataFrame(columns=['time', 'hs', 'tp', 'wave_direction'])
            
        df = df_hourly.copy()
        df = df.set_index('time')
        
        # Pre-calculate u, v components for wave_direction
        if 'wave_direction' in df.columns:
            r = np.radians(df['wave_direction'])
            df['u'] = np.cos(r)
            df['v'] = np.sin(r)
            
        # Resample to 10-minute frequency
        df_10m = df.resample('10min').asfreq()
        
        # Interpolate scalars with cubic spline
        for col in ['hs', 'tp']:
            if col in df_10m.columns:
                df_10m[col] = df_10m[col].interpolate(method='cubicspline')
                
        # Interpolate vectors linearly
        if 'u' in df_10m.columns and 'v' in df_10m.columns:
            df_10m['u'] = df_10m['u'].interpolate(method='linear')
            df_10m['v'] = df_10m['v'].interpolate(method='linear')
            
            # Reconstruct angle
            df_10m['wave_direction'] = np.degrees(np.arctan2(df_10m['v'], df_10m['u'])) % 360
            df_10m = df_10m.drop(columns=['u', 'v'])
            
        df_10m = df_10m.reset_index()
        return df_10m
