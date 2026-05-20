import os
import pandas as pd
from ..common.database import get_connection
from ..common.paths import INTERIM_DIR, DATA_DIR
from .scada_handshake import handshake_scada_and_dwell

def join_events_and_metocean(backbone_path=None, output_path=None, wind_farm=None):
    """
    Relational join of the dwell_events and the metocean backbone.
    Matches found_id and joins timestamps strictly within [start, end].
    """
    if backbone_path is None:
        backbone_path = os.path.join(INTERIM_DIR, "Metocean_NORA3_Backbone.csv")
    if output_path is None:
        output_path = os.path.join(DATA_DIR, "Processed", "Metocean_Vessel_Dwell_Join.csv")
        
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    if not os.path.exists(backbone_path):
        raise FileNotFoundError(f"Metocean backbone not found at {backbone_path}")
        
    print("Connecting to DuckDB for relational join...")
    conn = get_connection(read_only=True)
    
    # Register the CSV as a virtual table in DuckDB
    print(f"Registering metocean backbone: {backbone_path}")
    conn.execute(f"CREATE OR REPLACE TEMPORARY TABLE metocean_backbone AS SELECT * FROM read_csv_auto('{backbone_path}')")
    
    # Query to join
    query = """
    SELECT 
        b.timestamp_10min,
        b.hs,
        b.tp,
        b.wave_direction,
        b.wind_speed_10m,
        b.wind_direction_10m,
        b.wind_speed_100m,
        b.wind_direction_100m,
        b.current_speed,
        b.current_direction,
        b.lat,
        b.lon,
        b.found_id,
        b.source,
        b.interpolation_method,
        e.MMSI,
        e.Name as vessel_name,
        e."Ship type" as ship_type,
        e.wind_farm,
        e.event_id,
        e.start as event_start,
        e.end as event_end,
        e.ping_count,
        e.mean_sog,
        e.min_dist,
        e.length as vessel_length,
        e.draught as vessel_draught,
        e.duration_min,
        e.event_class
    FROM metocean_backbone b
    JOIN dwell_events e 
      ON b.found_id = e.found_id
     AND CAST(b.timestamp_10min AS TIMESTAMP) >= e.start
     AND CAST(b.timestamp_10min AS TIMESTAMP) <= e.end
    """
    
    if wind_farm:
        query += f" WHERE e.wind_farm = '{wind_farm}'"
        
    query += " ORDER BY b.found_id, b.timestamp_10min"
    
    print("Executing relational join...")
    joined_df = conn.execute(query).df()
    conn.close()
    
    print(f"Join completed. Resulting rows: {len(joined_df)}")
    
    # Apply SCADA Handshake labeling
    joined_df = handshake_scada_and_dwell(joined_df)
    
    joined_df.to_csv(output_path, index=False)
    print(f"Saved joined feature matrix to {output_path}")
    return joined_df

