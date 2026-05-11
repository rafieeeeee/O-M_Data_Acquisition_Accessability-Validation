import pandas as pd
import numpy as np
from math import radians, cos, sin, asin, sqrt
import os
from ..common.paths import INTERIM_DIR

def haversine(lon1, lat1, lon2, lat2):
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    r = 6371000 # Meters
    return c * r

def identify_vessels(ais_file, turbine_file):
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
    chunk_size = 500000
    reader = pd.read_csv(ais_file, chunksize=chunk_size)
    
    potential_matches = []
    chunk_count = 0

    for chunk in reader:
        chunk.columns = [c.strip().replace('# ', '').replace('#', '') for c in chunk.columns]
        
        # Fast numeric conversion
        chunk['Latitude'] = pd.to_numeric(chunk['Latitude'].astype(str).str.replace(',', '.'), errors='coerce')
        chunk['Longitude'] = pd.to_numeric(chunk['Longitude'].astype(str).str.replace(',', '.'), errors='coerce')
        chunk['SOG'] = pd.to_numeric(chunk['SOG'].astype(str).str.replace(',', '.'), errors='coerce')
        
        # Stationary filter
        stationary = chunk[chunk['SOG'] < 0.5].dropna(subset=['Latitude', 'Longitude'])
        if stationary.empty: continue

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
        if chunk_count % 5 == 0:
            print(f"Processed {chunk_count * chunk_size / 1e6:.1f}M rows...")

    if not potential_matches:
        print("No O&M activity detected.")
        return None, None

    print("Building event sequences...")
    all_pings = pd.concat(potential_matches)
    all_pings['Timestamp'] = pd.to_datetime(all_pings['Timestamp'], dayfirst=True)
    all_pings = all_pings.sort_values(['MMSI', 'Timestamp'])

    # Identify events: Consecutive pings at same foundation within 30 minutes
    all_pings['time_diff'] = all_pings.groupby(['MMSI', 'found_id'])['Timestamp'].diff().dt.total_seconds() / 60
    # New event starts if time_diff > 30 mins OR it's a new (MMSI, found_id) pair
    all_pings['new_event'] = (all_pings['time_diff'] > 30) | (all_pings['time_diff'].isna())
    all_pings['event_id'] = all_pings.groupby(['MMSI', 'found_id'])['new_event'].cumsum()

    # Aggregate events
    events = all_pings.groupby(['MMSI', 'Name', 'Ship type', 'wind_farm', 'found_id', 'event_id'], dropna=False).agg({
        'Timestamp': ['min', 'max', 'count'],
        'SOG': 'mean',
        'dist': 'min',
        'Length': 'first',
        'Draught': 'max'
    })
    events.columns = ['start', 'end', 'ping_count', 'mean_sog', 'min_dist', 'length', 'draught']
    events = events.reset_index()
    
    events['duration_min'] = (events['end'] - events['start']).dt.total_seconds() / 60
    
    # Filter by minimum duration (15 minutes)
    valid_events = events[events['duration_min'] >= 15].copy()
    
    # Save Events
    base_name = os.path.basename(ais_file).replace('.csv', '')
    events_path = os.path.join(INTERIM_DIR, f"OM_Events_{base_name}.csv")
    valid_events.to_csv(events_path, index=False)
    print(f"Saved {len(valid_events)} events to {events_path}")

    # Derive Registry
    registry = valid_events.groupby(['MMSI', 'Name', 'Ship type', 'wind_farm'], dropna=False).agg({
        'duration_min': 'sum',
        'event_id': 'count',
        'length': 'first',
        'draught': 'max'
    }).rename(columns={'duration_min': 'Total_Dwell_Time', 'event_id': 'Event_Count'}).sort_values('Total_Dwell_Time', ascending=False)
    
    registry_path = os.path.join(INTERIM_DIR, f"Fleet_Registry_{base_name}.csv")
    registry.to_csv(registry_path)
    print(f"Registry compiled: {len(registry)} vessels identified. Saved to {registry_path}")
    
    return events_path, registry_path
