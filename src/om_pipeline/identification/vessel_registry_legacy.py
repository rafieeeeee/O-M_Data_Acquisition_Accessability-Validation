import pandas as pd
import numpy as np
from math import radians, cos, sin, asin, sqrt

# Load Turbines
turbines = pd.read_csv('Data/Interim/Alpha_Ventus_Coordinates.csv')

# Load AIS (Filtered for German North Sea)
# Header indices: 0: Timestamp, 2: MMSI, 3: Lat, 4: Lon, 7: SOG
print("Loading AIS data...")
df = pd.read_csv('Data/Raw/AIS/German_North_Sea_2008_07.csv')

# Convert coordinates (handle comma/dot)
df['Latitude'] = df['Latitude'].astype(str).str.replace(',', '.').astype(float)
df['Longitude'] = df['Longitude'].astype(str).str.replace(',', '.').astype(float)
df['SOG'] = df['SOG'].astype(str).str.replace(',', '.').astype(float)

def haversine(lon1, lat1, lon2, lat2):
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    r = 6371000 # Meters
    return c * r

print("Running Proximity Search (this may take a minute)...")
om_candidates = []

# To speed this up, we only look at low-speed pings
low_speed_pings = df[df['SOG'] < 1.0].copy()

for idx, t in turbines.iterrows():
    # Coarse spatial filter first (0.01 degree ~ 1km)
    mask = (low_speed_pings['Latitude'].between(t['latitude']-0.01, t['latitude']+0.01)) & \
           (low_speed_pings['Longitude'].between(t['longitude']-0.01, t['longitude']+0.01))
    
    candidates = low_speed_pings[mask].copy()
    if not candidates.empty:
        # Fine haversine filter
        candidates['dist'] = candidates.apply(lambda row: haversine(row['Longitude'], row['Latitude'], t['longitude'], t['latitude']), axis=1)
        close_pings = candidates[candidates['dist'] < 100] # 100 meters
        if not close_pings.empty:
            om_candidates.append(close_pings)

if om_candidates:
    results = pd.concat(om_candidates)
    summary = results.groupby('MMSI').agg({
        'Timestamp': 'count',
        'Name': 'first',
        'Ship type': 'first',
        'Length': 'first'
    }).rename(columns={'Timestamp': 'Dwell_Pings'}).sort_values('Dwell_Pings', ascending=False)
    
    print("\nCandidate O&M / Construction Vessels Found at Alpha Ventus (July 2008):")
    print(summary)
    
    summary.to_csv('Data/Interim/Candidate_OM_Vessels_2008_07.csv')
else:
    print("No vessels found dwelling at Alpha Ventus foundations in this period.")
