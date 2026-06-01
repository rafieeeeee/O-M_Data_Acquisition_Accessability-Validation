"""
spatial_bounds.py

Spatial Foundation Layer for offshore wind O&M vessel dwell detection.
Computes all calculations in projected meters (UTM) using pyproj and shapely to avoid distortion.
Exposes loaded boundaries, asset points, and tiered buffers.
"""

import os
import pandas as pd
import numpy as np
from pyproj import Transformer, CRS
from shapely.geometry import Point, Polygon, MultiPoint
from shapely.ops import unary_union

def get_utm_zone(lat: float, lon: float) -> int:
    """
    Determine the UTM zone for a given lat/lon coordinate.
    Standard calculation for the North Sea and Baltic Sea region.
    """
    zone = int((lon + 180) / 6) + 1
    return zone

def get_crs_transformer(lat: float, lon: float, to_utm: bool = True) -> tuple[Transformer, int]:
    """
    Create a coordinate transformer between WGS84 (EPSG:4326) and local UTM.
    """
    zone = get_utm_zone(lat, lon)
    utm_crs = CRS.from_epsg(32600 + zone)  # EPSG 326xx for WGS 84 / UTM zone xxN
    wgs84_crs = CRS.from_epsg(4326)
    
    if to_utm:
        return Transformer.from_crs(wgs84_crs, utm_crs, always_xy=True), zone
    else:
        return Transformer.from_crs(utm_crs, wgs84_crs, always_xy=True), zone

def project_points(df: pd.DataFrame, lat_col: str = 'latitude', lon_col: str = 'longitude') -> pd.DataFrame:
    """
    Projects a DataFrame of lat/lon points into their local UTM coordinates (meters).
    Dynamically determines the zone based on the centroid of the coordinates.
    """
    if df.empty:
        return df
    
    # Handle capitalized columns dynamically
    if lat_col not in df.columns and 'Latitude' in df.columns:
        lat_col = 'Latitude'
    if lon_col not in df.columns and 'Longitude' in df.columns:
        lon_col = 'Longitude'
    
    centroid_lat = df[lat_col].mean()
    centroid_lon = df[lon_col].mean()
    
    transformer, zone = get_crs_transformer(centroid_lat, centroid_lon, to_utm=True)
    x, y = transformer.transform(df[lon_col].values, df[lat_col].values)
    
    df_proj = df.copy()
    df_proj['x'] = x
    df_proj['y'] = y
    df_proj['utm_zone'] = zone
    return df_proj

def load_farm_boundaries(turbine_coords_path: str) -> dict[str, Polygon]:
    """
    Loads turbines from the CSV file, groups them by wind farm,
    projects them to local UTM, and builds the 500m-buffered farm boundary.
    """
    if not os.path.exists(turbine_coords_path):
        raise FileNotFoundError(f"Turbine coords path not found: {turbine_coords_path}")
        
    df = pd.read_csv(turbine_coords_path)
    boundaries = {}
    
    for farm_id, group in df.groupby('wind_farm'):
        proj_df = project_points(group)
        points = [Point(row.x, row.y) for _, row in proj_df.iterrows()]
        
        if len(points) < 3:
            # Handling degenerate farms with few turbines
            multipoint = MultiPoint(points)
            boundaries[farm_id] = multipoint.buffer(500)
        else:
            multipoint = MultiPoint(points)
            boundaries[farm_id] = multipoint.convex_hull.buffer(500)
            
    return boundaries

def load_asset_points(turbine_coords_path: str) -> dict[str, list[Point]]:
    """
    Loads turbines from the CSV file, projects them to local UTM,
    and returns a dictionary mapping each wind farm to its list of turbine Points in UTM.
    """
    if not os.path.exists(turbine_coords_path):
        raise FileNotFoundError(f"Turbine coords path not found: {turbine_coords_path}")
        
    df = pd.read_csv(turbine_coords_path)
    asset_points = {}
    
    for farm_id, group in df.groupby('wind_farm'):
        proj_df = project_points(group)
        asset_points[farm_id] = [Point(row.x, row.y) for _, row in proj_df.iterrows()]
        
    return asset_points

def build_asset_buffers(farm_id: str, turbine_coords_path: str) -> dict:
    """
    Builds the complete spatial foundation layers for a specific wind farm in UTM meters.
    
    Returns a dictionary with:
      - 'farm_boundary': Authoritative farm boundary (convex hull + 500m buffer)
      - 'asset_buffers': List of 200m buffers around each turbine point (Tier A zone)
      - 'context_boundary': Outer context holding buffer (5km buffer from farm boundary)
      - 'asset_points': List of shapely Points representing coordinates in UTM
      - 'utm_zone': UTM zone used for projection
    """
    if not os.path.exists(turbine_coords_path):
        raise FileNotFoundError(f"Turbine coords path not found: {turbine_coords_path}")
        
    df = pd.read_csv(turbine_coords_path)
    group = df[df['wind_farm'] == farm_id]
    
    if group.empty:
        raise ValueError(f"Wind farm '{farm_id}' not found in coordinates registry.")
        
    proj_df = project_points(group)
    utm_zone = proj_df['utm_zone'].iloc[0]
    
    points = [Point(row.x, row.y) for _, row in proj_df.iterrows()]
    
    # 1. Authoritative Farm Boundary (Convex Hull + 500m buffer)
    if len(points) < 3:
        farm_boundary = MultiPoint(points).buffer(500)
    else:
        farm_boundary = MultiPoint(points).convex_hull.buffer(500)
        
    # 2. Asset Buffers (200m buffer around each turbine point)
    asset_buffers = [p.buffer(200) for p in points]
    
    # 3. Outer Context Buffer (5km buffer from farm boundary)
    context_boundary = farm_boundary.buffer(4500) # farm_boundary is already buffered by 500m, adding 4500m gives 5000m total context boundary
    
    return {
        'farm_boundary': farm_boundary,
        'asset_buffers': asset_buffers,
        'context_boundary': context_boundary,
        'asset_points': points,
        'utm_zone': utm_zone
    }
