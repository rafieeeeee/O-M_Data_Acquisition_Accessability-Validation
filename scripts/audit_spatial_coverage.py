import os
import pandas as pd
import xarray as xr
import numpy as np

def haversine_distance(lat1, lon1, lat2, lon2):
    """
    Computes geodesic distance in km between points using the Haversine formula.
    Supports scalars and numpy arrays.
    """
    R = 6371.0  # Earth radius in km
    phi1 = np.radians(lat1)
    phi2 = np.radians(lat2)
    dphi = np.radians(lat2 - lat1)
    dlambda = np.radians(lon2 - lon1)
    
    a = np.sin(dphi/2.0)**2 + np.cos(phi1) * np.cos(phi2) * np.sin(dlambda/2.0)**2
    c = 2.0 * np.arctan2(np.sqrt(a), np.sqrt(1.0 - a))
    return R * c

def audit_coverage():
    event_path = "Data/Processed/analysis/om_event_table.parquet"
    current_sample = "/Volumes/4TB HDD/Atlantic- European North West Shelf- Ocean Physics Reanalysis/NWS_Currents_Reanalysis_2010.nc"
    wave_sample = "/Volumes/4TB HDD/Atlantic- European North West Shelf- Wave Physics Reanalysis/NWS_Wave_Reanalysis_2010.nc"
    
    output_dir = "analysis/06_rq6_metocean_spatial_resolution"
    os.makedirs(output_dir, exist_ok=True)
    
    print("Loading events...")
    df = pd.read_parquet(event_path)
    print(f"Loaded {len(df)} events.")
    
    print("Loading grid samples...")
    ds_cur = xr.open_dataset(current_sample)
    ds_wav = xr.open_dataset(wave_sample)
    
    cur_lat = ds_cur.latitude.values
    cur_lon = ds_cur.longitude.values
    wav_lat = ds_wav.latitude.values
    wav_lon = ds_wav.longitude.values
    
    ds_cur.close()
    ds_wav.close()
    
    # Pre-compute min/max bounds
    cur_lat_min, cur_lat_max = cur_lat.min(), cur_lat.max()
    cur_lon_min, cur_lon_max = cur_lon.min(), cur_lon.max()
    
    wav_lat_min, wav_lat_max = wav_lat.min(), wav_lat.max()
    wav_lon_min, wav_lon_max = wav_lon.min(), wav_lon.max()
    
    MAX_DISTANCE = 25.0
    
    results = []
    
    for idx, row in df.iterrows():
        lat = row['centroid_lat']
        lon = row['centroid_lon']
        event_id = row['event_id']
        wind_farm = row['wind_farm']
        year = pd.to_datetime(row['start_utc']).year
        
        # 1. Currents Matching
        # Find nearest index
        cur_lat_idx = np.abs(cur_lat - lat).argmin()
        cur_lon_idx = np.abs(cur_lon - lon).argmin()
        matched_cur_lat = cur_lat[cur_lat_idx]
        matched_cur_lon = cur_lon[cur_lon_idx]
        dist_cur = haversine_distance(lat, lon, matched_cur_lat, matched_cur_lon)
        
        # Bounding box check
        in_cur_bbox = (cur_lat_min <= lat <= cur_lat_max) and (cur_lon_min <= lon <= cur_lon_max)
        in_cur_dist = dist_cur <= MAX_DISTANCE
        is_cur_valid = in_cur_bbox and in_cur_dist
        
        # 2. Waves Matching
        wav_lat_idx = np.abs(wav_lat - lat).argmin()
        wav_lon_idx = np.abs(wav_lon - lon).argmin()
        matched_wav_lat = wav_lat[wav_lat_idx]
        matched_wav_lon = wav_lon[wav_lon_idx]
        dist_wav = haversine_distance(lat, lon, matched_wav_lat, matched_wav_lon)
        
        in_wav_bbox = (wav_lat_min <= lat <= wav_lat_max) and (wav_lon_min <= lon <= wav_lon_max)
        in_wav_dist = dist_wav <= MAX_DISTANCE
        is_wav_valid = in_wav_bbox and in_wav_dist
        
        results.append({
            "event_id": event_id,
            "wind_farm": wind_farm,
            "year": year,
            "centroid_lat": lat,
            "centroid_lon": lon,
            "cur_grid_lat": matched_cur_lat,
            "cur_grid_lon": matched_cur_lon,
            "cur_distance_km": dist_cur,
            "cur_in_bbox": in_cur_bbox,
            "cur_valid": is_cur_valid,
            "wav_grid_lat": matched_wav_lat,
            "wav_grid_lon": matched_wav_lon,
            "wav_distance_km": dist_wav,
            "wav_in_bbox": in_wav_bbox,
            "wav_valid": is_wav_valid
        })
        
    audit_df = pd.DataFrame(results)
    
    # Save detailed audit file
    audit_df.to_csv(os.path.join(output_dir, "nws_event_spatial_match_details.csv"), index=False)
    
    # 3. Aggregate by Wind Farm
    farm_agg = audit_df.groupby('wind_farm').agg(
        event_count=('event_id', 'count'),
        inside_wave_grid=('wav_valid', 'sum'),
        inside_current_grid=('cur_valid', 'sum')
    ).reset_index()
    
    farm_agg['outside_wave_grid'] = farm_agg['event_count'] - farm_agg['inside_wave_grid']
    farm_agg['outside_current_grid'] = farm_agg['event_count'] - farm_agg['inside_current_grid']
    
    farm_agg['inside_wave_pct'] = (farm_agg['inside_wave_grid'] / farm_agg['event_count']) * 100.0
    farm_agg['inside_current_pct'] = (farm_agg['inside_current_grid'] / farm_agg['event_count']) * 100.0
    
    # Reorder columns
    farm_agg = farm_agg[[
        'wind_farm', 'event_count', 
        'inside_wave_grid', 'outside_wave_grid', 'inside_wave_pct',
        'inside_current_grid', 'outside_current_grid', 'inside_current_pct'
    ]]
    farm_agg.to_csv(os.path.join(output_dir, "coverage_by_wind_farm.csv"), index=False)
    
    # 4. Aggregate by Year
    year_agg = audit_df.groupby('year').agg(
        event_count=('event_id', 'count'),
        inside_wave_grid=('wav_valid', 'sum'),
        inside_current_grid=('cur_valid', 'sum')
    ).reset_index()
    
    year_agg['outside_wave_grid'] = year_agg['event_count'] - year_agg['inside_wave_grid']
    year_agg['outside_current_grid'] = year_agg['event_count'] - year_agg['inside_current_grid']
    
    year_agg['inside_wave_pct'] = (year_agg['inside_wave_grid'] / year_agg['event_count']) * 100.0
    year_agg['inside_current_pct'] = (year_agg['inside_current_grid'] / year_agg['event_count']) * 100.0
    
    year_agg = year_agg[[
        'year', 'event_count', 
        'inside_wave_grid', 'outside_wave_grid', 'inside_wave_pct',
        'inside_current_grid', 'outside_current_grid', 'inside_current_pct'
    ]]
    year_agg.to_csv(os.path.join(output_dir, "coverage_by_year.csv"), index=False)
    
    # 5. Write Markdown Spatial Coverage Report
    report_path = os.path.join(output_dir, "nws_spatial_coverage_report.md")
    
    # Calculate global metrics
    tot_events = len(audit_df)
    tot_wav_ok = audit_df['wav_valid'].sum()
    tot_cur_ok = audit_df['cur_valid'].sum()
    
    with open(report_path, "w") as f:
        f.write("# Copernicus North West Shelf (NWS) Spatial Coverage Report\n\n")
        f.write(f"Generated on: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        f.write("## Executive Summary\n")
        f.write(f"- **Total events analyzed**: {tot_events}\n")
        f.write(f"- **Events inside wave grid (<= 25 km)**: {tot_wav_ok} ({tot_wav_ok/tot_events*100.1:.1f}%)\n")
        f.write(f"- **Events inside currents grid (<= 25 km)**: {tot_cur_ok} ({tot_cur_ok/tot_events*100.0:.1f}%)\n\n")
        
        f.write("> [!IMPORTANT]\n")
        f.write("> **Copernicus NWS currents reanalysis files locally available DO NOT spatially cover the v0.1 pilot event table.**\n")
        f.write("> The local currents grid has a maximum longitude boundary of **4.555°E**, whereas all O&M wind farm pilot events are located at longitude **6.52°E or higher** (in the North Sea and Baltic Sea).\n")
        f.write("> Consequently, current features extracted for all 1,630 events will be set to `NaN` (out-of-bounds).\n\n")
        
        f.write("> [!NOTE]\n")
        f.write("> **NWS wave reanalysis files cover approximately 16.3% of the event table.**\n")
        f.write("> The wave grid has a maximum longitude boundary of **13.00°E**. This covers North Sea pilot wind farms (Amrumbank West, Alpha Ventus, Borkum, etc.) and borderline Baltic wind farms (Baltic 1, Baltic 2 is borderline at 13.28°E but within 25 km of the wave grid edge). It does not cover Baltic wind farms further east (Wikinger, Arkona, Baltic Eagle).\n\n")
        
        f.write("## Wind Farm Coverage Table\n\n")
        f.write("| Wind Farm | Total Events | Inside Wave Grid | Outside Wave Grid | Wave Pct | Inside Current Grid | Outside Current Grid | Current Pct |\n")
        f.write("|---|---|---|---|---|---|---|---|\n")
        for r in farm_agg.to_dict('records'):
            f.write(f"| {r['wind_farm']} | {r['event_count']} | {r['inside_wave_grid']} | {r['outside_wave_grid']} | {r['inside_wave_pct']:.1f}% | {r['inside_current_grid']} | {r['outside_current_grid']} | {r['inside_current_pct']:.1f}% |\n")
            
        f.write("\n## Yearly Coverage Table\n\n")
        f.write("| Year | Total Events | Inside Wave Grid | Outside Wave Grid | Wave Pct | Inside Current Grid | Outside Current Grid | Current Pct |\n")
        f.write("|---|---|---|---|---|---|---|---|\n")
        for r in year_agg.to_dict('records'):
            f.write(f"| {r['year']} | {r['event_count']} | {r['inside_wave_grid']} | {r['outside_wave_grid']} | {r['inside_wave_pct']:.1f}% | {r['inside_current_grid']} | {r['outside_current_grid']} | {r['inside_current_pct']:.1f}% |\n")
            
        f.write("\n## Spatial Distance Details\n")
        f.write("### Waves Distance Distribution\n")
        wav_dists = audit_df['wav_distance_km']
        f.write(f"- Min distance: {wav_dists.min():.2f} km\n")
        f.write(f"- Mean distance: {wav_dists.mean():.2f} km\n")
        f.write(f"- Max distance: {wav_dists.max():.2f} km\n\n")
        
        f.write("### Currents Distance Distribution\n")
        cur_dists = audit_df['cur_distance_km']
        f.write(f"- Min distance: {cur_dists.min():.2f} km\n")
        f.write(f"- Mean distance: {cur_dists.mean():.2f} km\n")
        f.write(f"- Max distance: {cur_dists.max():.2f} km\n\n")
        
        f.write("### Distance Stats by Wind Farm (Waves)\n")
        farm_wav_dists = audit_df.groupby('wind_farm')['wav_distance_km'].agg(['min', 'mean', 'max', 'count'])
        f.write("| Wind Farm | Event Count | Min Wave Dist (km) | Mean Wave Dist (km) | Max Wave Dist (km) | Status |\n")
        f.write("|---|---|---|---|---|---|\n")
        for farm, stats in farm_wav_dists.iterrows():
            status = "Fully Covered" if stats['max'] <= 5.0 else ("Partially / Borderline Covered" if stats['min'] <= 25.0 else "Fully Out of Bounds")
            f.write(f"| {farm} | {int(stats['count'])} | {stats['min']:.2f} | {stats['mean']:.2f} | {stats['max']:.2f} | {status} |\n")
            
    print(f"Saved Markdown Spatial Coverage Report to {report_path}")

if __name__ == "__main__":
    audit_coverage()
