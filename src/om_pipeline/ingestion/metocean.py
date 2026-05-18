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
        Uses Cubic Spline for scalar fields and Circular Vector Interpolation for directions.
        
        Args:
            df_hourly (pd.DataFrame): Hourly dataframe containing columns like 'time', 'hs', 'tp', 'wave_direction', etc.
        Returns:
            pd.DataFrame: 10-minute upscaled dataframe.
        """
        if df_hourly.empty:
            return pd.DataFrame()
            
        df = df_hourly.copy()
        df = df.set_index('time')
        
        # Identify scalar and direction columns dynamically
        direction_cols = [c for c in df.columns if 'direction' in c.lower() or c.lower() == 'dir' or c.lower() == 'thq']
        scalar_cols = [c for c in df.columns if c not in direction_cols and c not in ['lat', 'lon', 'source', 'interpolation_method', 'found_id']]
        
        # Pre-calculate u, v components for all direction variables
        for dcol in direction_cols:
            r = np.radians(df[dcol])
            df[f'{dcol}_u'] = np.cos(r)
            df[f'{dcol}_v'] = np.sin(r)
            
        # Resample to 10-minute frequency
        df_10m = df.resample('10min').asfreq()
        
        # Interpolate scalars with cubic spline
        for col in scalar_cols:
            if col in df_10m.columns:
                df_10m[col] = df_10m[col].interpolate(method='cubicspline')
                
        # Forward/Backward fill metadata/coordinates if present
        for meta_col in ['lat', 'lon', 'source', 'interpolation_method', 'found_id']:
            if meta_col in df_10m.columns:
                df_10m[meta_col] = df_10m[meta_col].ffill().bfill()
                
        # Interpolate orthogonal direction vectors linearly and reconstruct angles
        for dcol in direction_cols:
            ucol = f'{dcol}_u'
            vcol = f'{dcol}_v'
            if ucol in df_10m.columns and vcol in df_10m.columns:
                df_10m[ucol] = df_10m[ucol].interpolate(method='linear')
                df_10m[vcol] = df_10m[vcol].interpolate(method='linear')
                
                # Reconstruct angle in [0, 360)
                df_10m[dcol] = np.degrees(np.arctan2(df_10m[vcol], df_10m[ucol])) % 360
                df_10m = df_10m.drop(columns=[ucol, vcol])
                
        df_10m = df_10m.reset_index()
        return df_10m

