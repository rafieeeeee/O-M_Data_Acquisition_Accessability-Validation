import pandas as pd

# Load the extracted coordinates
df = pd.read_csv('Data/Interim/Alpha_Ventus_Coordinates.csv')

# Calculate the bounding box based on turbine locations
min_lat, max_lat = df['latitude'].min(), df['latitude'].max()
min_lon, max_lon = df['longitude'].min(), df['longitude'].max()

# Add a 2 nautical mile (approx 0.033 degrees) buffer for approach/waiting zones
buffer = 0.033
bbox = {
    "min_lat": min_lat - buffer,
    "max_lat": max_lat + buffer,
    "min_lon": min_lon - buffer,
    "max_lon": max_lon + buffer
}

print(f"Alpha Ventus Bounding Box (with 2nm buffer):")
print(f"Lat: {bbox['min_lat']:.4f} to {bbox['max_lat']:.4f}")
print(f"Lon: {bbox['min_lon']:.4f} to {bbox['max_lon']:.4f}")
