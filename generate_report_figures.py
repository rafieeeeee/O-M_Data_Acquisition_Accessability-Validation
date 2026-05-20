import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import glob
import os

# 1. Load Turbines
print("Loading turbines...")
turbines = pd.read_csv('Data/Interim/European_Turbine_Coordinates.csv')
# dropna on lat/lon
turbines = turbines.dropna(subset=['latitude', 'longitude'])

# 2. Load OM_Events to see which windfarms have AIS data
print("Loading OM Events to find AIS coverage...")
event_files = glob.glob('Data/Interim/OM_Events_Farm-Candidates_European-Master_*.csv')
ais_farms = set()
for f in event_files:
    df_e = pd.read_csv(f, usecols=['wind_farm'])
    ais_farms.update(df_e['wind_farm'].dropna().unique())

turbines['has_ais'] = turbines['wind_farm'].isin(ais_farms)

# Plot Turbines and AIS coverage
plt.figure(figsize=(10, 8))
plt.scatter(turbines[~turbines['has_ais']]['longitude'], turbines[~turbines['has_ais']]['latitude'], 
            c='lightgray', s=10, label='Turbines (No AIS Events)', alpha=0.5)
plt.scatter(turbines[turbines['has_ais']]['longitude'], turbines[turbines['has_ais']]['latitude'], 
            c='blue', s=15, label='Turbines (With AIS Events)', alpha=0.7)
plt.xlabel('Longitude')
plt.ylabel('Latitude')
plt.title('Spatial Range of Turbines and AIS Data Coverage')
plt.legend()
plt.grid(True, linestyle='--', alpha=0.5)
plt.tight_layout()
plt.savefig('reports/figures/spatial_range_ais_turbines.png', dpi=300)
plt.close()

# 3. Metocean Data
print("Loading Metocean data...")
metocean = pd.read_csv('Data/Interim/Metocean_NORA3_Backbone.csv', usecols=['timestamp_10min', 'lat', 'lon', 'hs'])
metocean['timestamp_10min'] = pd.to_datetime(metocean['timestamp_10min'])

# Plot Metocean Spatial Coverage
plt.figure(figsize=(10, 8))
# group by lat/lon to get unique locations
meto_locs = metocean[['lat', 'lon']].drop_duplicates()
plt.scatter(turbines['longitude'], turbines['latitude'], c='lightgray', s=10, label='All Turbines', alpha=0.5)
plt.scatter(meto_locs['lon'], meto_locs['lat'], c='red', s=20, marker='x', label='Metocean Data Points')
plt.xlabel('Longitude')
plt.ylabel('Latitude')
plt.title('Metocean NORA3 Data Spatial Coverage')
plt.legend()
plt.grid(True, linestyle='--', alpha=0.5)
plt.tight_layout()
plt.savefig('reports/figures/metocean_spatial_coverage.png', dpi=300)
plt.close()

# Plot Metocean Time Coverage
plt.figure(figsize=(12, 4))
metocean['date'] = metocean['timestamp_10min'].dt.date
date_counts = metocean.groupby('date').size()
plt.plot(date_counts.index, date_counts.values, marker='.', linestyle='none')
plt.xlabel('Date')
plt.ylabel('Data Points Count')
plt.title('Metocean NORA3 Data Temporal Coverage')
plt.grid(True, linestyle='--', alpha=0.5)
plt.tight_layout()
plt.savefig('reports/figures/metocean_temporal_coverage.png', dpi=300)
plt.close()

print("Figures generated in reports/figures/")
