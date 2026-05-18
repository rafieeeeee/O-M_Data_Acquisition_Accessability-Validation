import os
import sys
import pandas as pd
from om_pipeline.common.paths import INTERIM_DIR
from om_pipeline.common.database import get_connection

def qa_metocean():
    csv_path = os.path.join(INTERIM_DIR, "Metocean_NORA3_Backbone.csv")
    if not os.path.exists(csv_path):
        print(f"CRITICAL: Metocean backbone not found at {csv_path}. Run extract_metocean.py first.")
        sys.exit(1)

    print("Loading Metocean Backbone...")
    df = pd.read_csv(csv_path)
    
    print("\n--- QA Results ---")
    print(f"Total Rows: {len(df)}")
    
    # 1. Missing Values check
    print("\n1. Checking Missing Values:")
    check_cols = [
        'hs', 'tp', 'wave_direction', 
        'wind_speed_10m', 'wind_direction_10m', 
        'wind_speed_100m', 'wind_direction_100m', 
        'current_speed', 'current_direction'
    ]
    missing = df[check_cols].isnull().sum()
    print(missing.to_string())
    if missing.sum() > 0:
        print("CRITICAL QA FAILURE: Found missing metocean values!")
        sys.exit(1)
    print("SUCCESS: Zero missing values.")
    
    # 2. Direction Bounds check
    print("\n2. Checking Direction Bounds [0, 360):")
    direction_cols = ['wave_direction', 'wind_direction_10m', 'wind_direction_100m', 'current_direction']
    for col in direction_cols:
        min_dir = df[col].min()
        max_dir = df[col].max()
        print(f"  {col}: min = {min_dir:.2f}, max = {max_dir:.2f}")
        if min_dir < 0 or max_dir >= 360:
            print(f"CRITICAL QA FAILURE: {col} falls outside standard [0, 360) bounds.")
            sys.exit(1)
    print("SUCCESS: Direction bounds are valid.")

        
    # 3. Timestamp Alignment check
    print("\n3. Checking Timestamp Alignment:")
    df['timestamp_10min'] = pd.to_datetime(df['timestamp_10min'])
    minutes = df['timestamp_10min'].dt.minute
    misaligned = len(df[~minutes.isin([0, 10, 20, 30, 40, 50])])
    print(f"Rows NOT aligned to 10-minute grid: {misaligned}")
    if misaligned > 0:
        print("CRITICAL QA FAILURE: Interpolation upscaler produced off-grid timestamps.")
        sys.exit(1)
    print("SUCCESS: All timestamps aligned to 10-minute boundaries.")
    
    # 4. Row Count Match check
    print("\n4. Checking Row Count Match with theoretical grid:")
    try:
        found_ids = df['found_id'].unique().tolist()
        found_ids_str = ", ".join([str(x) for x in found_ids])
        conn = get_connection(read_only=True)
        query = f"""
        SELECT 
            MIN(e.start) as time_start,
            MAX(e.end) as time_end
        FROM dwell_events e
        JOIN turbines t ON e.found_id = t."Unnamed: 0"
        WHERE e.found_id IN ({found_ids_str})
        GROUP BY e.found_id, EXTRACT(year FROM e.start), EXTRACT(month FROM e.start)
        """
        bounds_df = conn.execute(query).df()
        conn.close()
        
        expected_rows = 0
        for _, row in bounds_df.iterrows():
            t_start = pd.to_datetime(row['time_start'])
            t_end = pd.to_datetime(row['time_end'])
            # Generate the 10-minute grid covering the event, then filter strictly within boundaries
            grid = pd.date_range(start=t_start.floor('10min'), end=t_end.ceil('10min'), freq='10min')
            valid_grid = grid[(grid >= t_start) & (grid <= t_end)]
            expected_rows += len(valid_grid)
            
        print(f"Expected Rows (from precise event bounds): {expected_rows}")
        print(f"Actual Rows in Backbone: {len(df)}")
        if len(df) != expected_rows:
            print(f"CRITICAL QA FAILURE: Row count mismatch! Expected: {expected_rows}, Actual: {len(df)}")
            sys.exit(1)
        print("SUCCESS: Row count matches theoretical event grid.")
    except Exception as e:
        print(f"CRITICAL QA FAILURE: Failed to calculate expected rows from DuckDB: {e}")
        sys.exit(1)
    
    # 5. Continuous Span Sanity Check
    print("\n5. Checking Continuous Span Sanity:")
    df = df.sort_values(by=['found_id', 'timestamp_10min'])
    df['gap'] = df.groupby('found_id')['timestamp_10min'].diff()
    # Correctly identify new spans per foundation
    df['is_new_span'] = (df['gap'] > pd.Timedelta(minutes=10)) | df['gap'].isnull()
    df['span_id'] = df.groupby('found_id')['is_new_span'].cumsum()
    span_durations = df.groupby(['found_id', 'span_id'])['timestamp_10min'].agg(lambda x: x.max() - x.min())
    max_span = span_durations.max()
    print(f"Max continuous metocean span for a single foundation: {max_span}")
    if max_span > pd.Timedelta(days=32):
        print("CRITICAL QA FAILURE: Exceptionally long continuous spans found. Ensure grouping logic is bounded by events.")
        sys.exit(1)
    print("SUCCESS: Continuous spans are sane.")
        
    print("\nQA PASSED 100%!")

if __name__ == "__main__":
    qa_metocean()
