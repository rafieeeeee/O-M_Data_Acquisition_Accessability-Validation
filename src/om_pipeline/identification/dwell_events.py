import pandas as pd
import numpy as np
from math import radians, cos, sin, asin, sqrt
import os
import time
from ..common.paths import INTERIM_DIR

def haversine(lon1, lat1, lon2, lat2):
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    r = 6371000 # Meters
    return c * r

def format_progress(processed_rows, total_rows, matched_rows, started_at):
    elapsed = max(time.time() - started_at, 0.001)
    rows_per_sec = processed_rows / elapsed
    message = (
        f"Processed {processed_rows / 1e6:.1f}M rows"
        f" | matches collected: {matched_rows:,}"
        f" | {rows_per_sec:,.0f} rows/s"
    )
    if total_rows:
        percent = min(processed_rows / total_rows, 1.0)
        filled = int(percent * 30)
        bar = "#" * filled + "-" * (30 - filled)
        remaining_rows = max(total_rows - processed_rows, 0)
        eta_min = remaining_rows / rows_per_sec / 60
        message = (
            f"[{bar}] {percent * 100:5.1f}% | "
            f"{message} | ETA {eta_min:.1f} min"
        )
    return message


def identify_vessels(
    ais_file,
    turbine_file,
    chunk_size=500000,
    progress_interval=5,
    total_rows=None,
):
    print(f"Loading Master European Turbine Database...")
    turbines = pd.read_csv(turbine_file)
    # Ensure foundation ID exists (column 0 or index)
    if turbines.columns[0] == 'Unnamed: 0' or turbines.columns[0] == '':
        turbines = turbines.rename(columns={turbines.columns[0]: 'found_id'})
    else:
        turbines.index.name = 'found_id'
        turbines = turbines.reset_index()

    # Calculate per-farm bounding boxes
    print("Calculating precise bounding boxes for all European wind farms...")
    farm_metadata = turbines.groupby('wind_farm').agg({
        'latitude': ['min', 'max'],
        'longitude': ['min', 'max'],
        'country': 'first'
    })
    farm_metadata.columns = ['lat_min', 'lat_max', 'lon_min', 'lon_max', 'country']
    farm_metadata['lat_min'] -= 0.01
    farm_metadata['lat_max'] += 0.01
    farm_metadata['lon_min'] -= 0.01
    farm_metadata['lon_max'] += 0.01
    
    print(f"Loading AIS from {ais_file}...")
    reader = pd.read_csv(ais_file, chunksize=chunk_size)
    
    potential_matches = []
    chunk_count = 0
    processed_rows = 0
    started_at = time.time()

    for chunk in reader:
        processed_rows += len(chunk)
        chunk.columns = [c.strip().replace('# ', '').replace('#', '') for c in chunk.columns]
        
        # Fast numeric conversion
        chunk['Latitude'] = pd.to_numeric(chunk['Latitude'].astype(str).str.replace(',', '.'), errors='coerce')
        chunk['Longitude'] = pd.to_numeric(chunk['Longitude'].astype(str).str.replace(',', '.'), errors='coerce')
        chunk['SOG'] = pd.to_numeric(chunk['SOG'].astype(str).str.replace(',', '.'), errors='coerce')
        
        # Stationary filter
        stationary = chunk[chunk['SOG'] < 0.5].dropna(subset=['Latitude', 'Longitude'])
        if stationary.empty:
            chunk_count += 1
            if progress_interval and chunk_count % progress_interval == 0:
                matched_rows = sum(len(match) for match in potential_matches)
                print(format_progress(processed_rows, total_rows, matched_rows, started_at), flush=True)
            continue

        # Spatial join (Vectorized optimization)
        for farm_name, meta in farm_metadata.iterrows():
            farm_mask = (stationary['Latitude'].between(meta['lat_min'], meta['lat_max'])) & \
                        (stationary['Longitude'].between(meta['lon_min'], meta['lon_max']))
            
            pings_in_farm = stationary[farm_mask].copy()
            if pings_in_farm.empty: continue
            
            farm_t = turbines[turbines['wind_farm'] == farm_name]
            
            for _, t in farm_t.iterrows():
                # Fine filter (100m)
                found_mask = (pings_in_farm['Latitude'].between(t['latitude']-0.002, t['latitude']+0.002)) & \
                             (pings_in_farm['Longitude'].between(t['longitude']-0.002, t['longitude']+0.002))
                
                candidates = pings_in_farm[found_mask].copy()
                if not candidates.empty:
                    candidates['dist'] = candidates.apply(
                        lambda row: haversine(row['Longitude'], row['Latitude'], t['longitude'], t['latitude']), axis=1
                    )
                    success = candidates[candidates['dist'] < 100].copy()
                    if not success.empty:
                        success['wind_farm'] = farm_name
                        success['country'] = meta['country']
                        success['found_id'] = t['found_id']
                        potential_matches.append(success)
        
        chunk_count += 1
        if progress_interval and chunk_count % progress_interval == 0:
            matched_rows = sum(len(match) for match in potential_matches)
            print(format_progress(processed_rows, total_rows, matched_rows, started_at), flush=True)

    if not potential_matches:
        print("No O&M activity detected.")
        return None, None

    print("Building event sequences and consolidating MMSI metadata...")
    all_pings = pd.concat(potential_matches)
    
    # Strict Assignment: Drop duplicate pings (same MMSI/Timestamp) keeping the nearest foundation
    all_pings = all_pings.sort_values(['MMSI', 'Timestamp', 'dist'])
    all_pings = all_pings.drop_duplicates(subset=['MMSI', 'Timestamp'], keep='first')
    
    all_pings['Timestamp'] = pd.to_datetime(all_pings['Timestamp'], dayfirst=True)
    all_pings = all_pings.sort_values(['MMSI', 'Timestamp'])

    # Resolve "Best" metadata per MMSI to handle missing names/types in some pings
    def get_best_meta(series):
        valid = series.dropna().unique()
        valid = [v for v in valid if str(v).lower() not in ('', 'unknown', 'undefined')]
        if not valid: return 'Unknown'
        return max(set(valid), key=list(series).count) # Return most frequent

    mmsi_meta = all_pings.groupby('MMSI').agg({
        'Name': get_best_meta,
        'Ship type': get_best_meta,
        'Length': 'max',
        'Draught': 'max'
    })
    
    # Map back to all_pings
    all_pings['Name'] = all_pings['MMSI'].map(mmsi_meta['Name'])
    all_pings['Ship type'] = all_pings['MMSI'].map(mmsi_meta['Ship type'])
    all_pings['Length'] = all_pings['MMSI'].map(mmsi_meta['Length'])
    all_pings['Draught'] = all_pings['MMSI'].map(mmsi_meta['Draught'])

    # Identify events: Consecutive pings at same foundation within 30 minutes
    # Strict Sequencing: Sort by MMSI/Timestamp and detect changes in found_id OR time gaps
    all_pings['time_diff'] = all_pings.groupby('MMSI')['Timestamp'].diff().dt.total_seconds() / 60
    all_pings['found_changed'] = all_pings.groupby('MMSI')['found_id'].shift() != all_pings['found_id']
    
    # New event starts if time_diff > 30 mins OR foundation changed OR it's a new MMSI
    all_pings['new_event'] = (all_pings['time_diff'] > 30) | (all_pings['found_changed']) | (all_pings['time_diff'].isna())
    all_pings['event_id'] = all_pings.groupby('MMSI')['new_event'].cumsum()

    # Aggregate events
    events = all_pings.groupby(['MMSI', 'Name', 'Ship type', 'wind_farm', 'found_id', 'event_id'], dropna=False).agg({
        'Timestamp': ['min', 'max', 'count'],
        'SOG': 'mean',
        'dist': 'min',
        'Length': 'first',
        'Draught': 'first'
    })
    events.columns = ['start', 'end', 'ping_count', 'mean_sog', 'min_dist', 'length', 'draught']
    events = events.reset_index()
    
    events['duration_min'] = (events['end'] - events['start']).dt.total_seconds() / 60
    
    # Filter by minimum duration (15 minutes)
    valid_events = events[events['duration_min'] >= 15].copy()
    
    # Event Classification
    def classify_event(dur):
        if dur < 720: return "Transfer"
        if dur <= 1440: return "Extended"
        return "Stationary/Construction"
    
    valid_events['event_class'] = valid_events['duration_min'].apply(classify_event)
    
    # Save Events
    base_name = os.path.basename(ais_file).replace('.csv', '')
    events_path = os.path.join(INTERIM_DIR, f"OM_Events_{base_name}.csv")
    valid_events.to_csv(events_path, index=False)
    print(f"Saved {len(valid_events)} events to {events_path}")

    # Derive Registry
    registry = valid_events.groupby(['MMSI', 'Name', 'Ship type', 'wind_farm'], dropna=False).agg({
        'duration_min': ['sum', 'median'],
        'event_id': 'count',
        'length': 'first',
        'draught': 'max'
    })
    registry.columns = ['Total_Dwell_Time', 'Median_Duration', 'Event_Count', 'length', 'draught']
    registry = registry.reset_index()
    
    # Empirical Vessel Type
    def classify_vessel(row):
        if 10 <= row['length'] <= 40 and row['Median_Duration'] < 120:
            return "Probable CTV"
        return row['Ship type']
    
    registry['empirical_type'] = registry.apply(classify_vessel, axis=1)
    registry = registry.sort_values('Total_Dwell_Time', ascending=False)
    
    registry_path = os.path.join(INTERIM_DIR, f"Fleet_Registry_{base_name}.csv")
    registry.to_csv(registry_path)
    print(f"Registry compiled: {len(registry)} vessels identified. Saved to {registry_path}")
    
    return events_path, registry_path
