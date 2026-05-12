import os
import argparse
import pandas as pd
from om_pipeline.common.database import get_connection
from om_pipeline.ingestion.nora3 import fetch_nora3_point
from om_pipeline.ingestion.metocean import MetoceanIngestor
from om_pipeline.common.paths import INTERIM_DIR

def extract_metocean_for_events(nora3_url=None, dry_run=False):
    """
    Queries DuckDB for the spatial/temporal bounding boxes of all events,
    fetches raw NORA3 data for those bounds, upscales to 10 minutes,
    and saves the metocean event table.
    """
    print("Connecting to DuckDB to extract event bounding boxes...")
    conn = get_connection(read_only=True)
    
    # We need the min and max timestamp per foundation, and its coordinates.
    # We join dwell_events to turbines to get lat/lon.
    query = """
    SELECT 
        e.found_id,
        t.latitude as lat,
        t.longitude as lon,
        EXTRACT(year FROM e.start) as event_year,
        EXTRACT(month FROM e.start) as event_month,
        MIN(e.start) as time_start,
        MAX(e.end) as time_end
    FROM dwell_events e
    JOIN turbines t ON e.found_id = t."Unnamed: 0"
    GROUP BY e.found_id, t.latitude, t.longitude, EXTRACT(year FROM e.start), EXTRACT(month FROM e.start)
    """
    
    try:
        bounds_df = conn.execute(query).df()
    except Exception as e:
        print(f"Failed to query events from DuckDB. Ensure events are registered: {e}")
        return
    finally:
        conn.close()

    if bounds_df.empty:
        print("No events found in catalog. Run identification first.")
        return
        
    print(f"Found {len(bounds_df)} unique foundations with events.")
    
    ingestor = MetoceanIngestor()
    all_metocean = []
    
    for _, row in bounds_df.iterrows():
        lat = row['lat']
        lon = row['lon']
        t_start = row['time_start']
        t_end = row['time_end']
        found_id = row['found_id']
        
        print(f"Processing foundation {found_id} (Lat: {lat:.3f}, Lon: {lon:.3f}) from {t_start} to {t_end}")
        
        if dry_run:
            continue
            
        # Fetch raw hourly NORA3
        raw_df = fetch_nora3_point(
            lat=lat, 
            lon=lon, 
            time_start=t_start, 
            time_end=t_end, 
            thredds_url=nora3_url
        )
        
        if raw_df.empty:
            continue
            
        # Upscale to 10-minute backbone (this creates the interpolated bracket)
        upscaled_df = ingestor.upscale_to_10min(raw_df)
        
        # Clip back to the strict event boundaries now that interpolation is complete
        upscaled_df = upscaled_df[(upscaled_df['time'] >= t_start) & (upscaled_df['time'] <= t_end)].copy()
        
        if not upscaled_df.empty:
            upscaled_df['found_id'] = found_id
            upscaled_df['source'] = 'NORA3'
            upscaled_df['interpolation_method'] = 'cubic_scalar+circular_vector'
            # We don't map MMSI or event_uid here per the spec. 
            # The join boundary happens later by merging on found_id and timestamp_10min.
            all_metocean.append(upscaled_df)
            
    if dry_run:
        print("Dry run complete.")
        return
        
    if all_metocean:
        final_df = pd.concat(all_metocean, ignore_index=True)
        # Rename 'time' to 'timestamp_10min' as requested in the schema
        final_df = final_df.rename(columns={'time': 'timestamp_10min'})
        
        output_path = os.path.join(INTERIM_DIR, "Metocean_NORA3_Backbone.csv")
        final_df.to_csv(output_path, index=False)
        print(f"Successfully extracted and upscaled metocean data to {output_path}")
        print(f"Total 10-minute records: {len(final_df)}")
    else:
        print("No metocean data was successfully extracted.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract and interpolate NORA3 metocean data for identified O&M events.")
    parser.add_argument("--thredds-url", default=None, help="Optional NORA3 OPenDAP URL override.")
    parser.add_argument("--dry-run", action="store_true", help="Print bounding boxes and exit without fetching.")
    
    args = parser.parse_args()
    extract_metocean_for_events(nora3_url=args.thredds_url, dry_run=args.dry_run)
