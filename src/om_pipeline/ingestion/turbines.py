import pandas as pd
import os
from ..common.paths import RAW_DIR, INTERIM_DIR

# Default Path for the raw turbine database
DEFAULT_RAW_DB = os.path.join(RAW_DIR, "Open European offshore wind turbine database", "20251218_eww_opendatabase.csv")

def prepare_turbine_data(raw_db_path=None):
    if raw_db_path is None:
        raw_db_path = DEFAULT_RAW_DB

    if not os.path.exists(raw_db_path):
        print(f"Error: Raw database not found at {raw_db_path}")
        return

    print(f"Loading raw database from {raw_db_path}...")
    df = pd.read_csv(raw_db_path)
    
    # Ensure interim directory exists
    os.makedirs(INTERIM_DIR, exist_ok=True)

    # 1. European Coordinates (Full Copy)
    euro_path = os.path.join(INTERIM_DIR, "European_Turbine_Coordinates.csv")
    df.to_csv(euro_path, index=False)
    print(f"Saved European Master Coordinates to {euro_path}")

    # 2. German Coordinates
    german_df = df[df['country'] == 'Germany']
    german_path = os.path.join(INTERIM_DIR, "German_Turbine_Coordinates.csv")
    german_df.to_csv(german_path, index=False)
    print(f"Saved German Turbine Coordinates to {german_path}")

    # 3. Alpha Ventus Coordinates
    av_df = df[df['wind_farm'] == 'Alpha Ventus']
    av_path = os.path.join(INTERIM_DIR, "Alpha_Ventus_Coordinates.csv")
    av_df.to_csv(av_path, index=False)
    print(f"Saved Alpha Ventus Coordinates to {av_path}")

    print("\nTurbine data preparation complete.")
    return euro_path, german_path, av_path
