import os
import sys
from om_pipeline.common.database import get_connection, register_data_source, list_views
from om_pipeline.common.paths import AIS_RAW_DIR, INTERIM_DIR

def init_catalog():
    print("Initializing DuckDB Catalog...")
    con = get_connection()
    
    # Define files to register
    files_to_register = [
        ("ais_pilot", os.path.join(AIS_RAW_DIR, "German-Bight_2024_07_SogMax2.0.csv")),
        ("events_pilot", os.path.join(INTERIM_DIR, "OM_Events_German-Bight_2024_07_SogMax2.0.csv")),
        ("registry_pilot", os.path.join(INTERIM_DIR, "Fleet_Registry_German-Bight_2024_07_SogMax2.0.csv"))
    ]
    
    registered_count = 0
    for table_name, file_path in files_to_register:
        if os.path.exists(file_path):
            register_data_source(con, table_name, file_path)
            registered_count += 1
        else:
            print(f"Skipping {table_name}: File not found at {file_path}")
            
    print(f"\nInitialization complete. Registered {registered_count} views.")
    views = list_views(con)
    if views:
        print(f"Available views: {[v[0] for v in views]}")
    
    con.close()

if __name__ == "__main__":
    init_catalog()
