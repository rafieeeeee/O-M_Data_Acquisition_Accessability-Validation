import os
import pandas as pd
from om_pipeline.common.paths import INTERIM_DIR

def qa_metocean():
    csv_path = os.path.join(INTERIM_DIR, "Metocean_NORA3_Backbone.csv")
    if not os.path.exists(csv_path):
        print(f"Metocean backbone not found at {csv_path}. Run extract_metocean.py first.")
        return

    print("Loading Metocean Backbone...")
    df = pd.read_csv(csv_path)
    
    print("\n--- QA Results ---")
    print(f"Total Rows: {len(df)}")
    
    print("\n1. Missing Values:")
    missing = df[['hs', 'tp', 'wave_direction']].isnull().sum()
    print(missing.to_string())
    if missing.sum() > 0:
        print("WARNING: Found missing metocean values! Check interpolation bounds.")
    
    print("\n2. Direction Bounds [0, 360):")
    min_dir = df['wave_direction'].min()
    max_dir = df['wave_direction'].max()
    print(f"Min direction: {min_dir:.2f}")
    print(f"Max direction: {max_dir:.2f}")
    if min_dir < 0 or max_dir >= 360:
        print("WARNING: Direction values fall outside standard [0, 360) bounds.")
        
    print("\n3. Timestamp Alignment:")
    df['timestamp_10min'] = pd.to_datetime(df['timestamp_10min'])
    minutes = df['timestamp_10min'].dt.minute
    misaligned = len(df[~minutes.isin([0, 10, 20, 30, 40, 50])])
    print(f"Rows NOT aligned to 10-minute grid: {misaligned}")
    if misaligned > 0:
        print("WARNING: Interpolation upscaler is producing off-grid timestamps.")
    
    print("\n4. Summary by Month:")
    df['month'] = df['timestamp_10min'].dt.month
    print(df.groupby('month').size().to_string())
    
    print("\n5. Continuous Span Sanity Check:")
    # Check max continuous duration per foundation
    df = df.sort_values(by=['found_id', 'timestamp_10min'])
    df['gap'] = df.groupby('found_id')['timestamp_10min'].diff()
    df['new_span'] = (df['gap'] > pd.Timedelta(minutes=10)).cumsum()
    span_durations = df.groupby(['found_id', 'new_span'])['timestamp_10min'].agg(lambda x: x.max() - x.min())
    max_span = span_durations.max()
    print(f"Max continuous metocean span for a single foundation: {max_span}")
    if max_span > pd.Timedelta(days=14):
        print("WARNING: Exceptionally long continuous spans found. Ensure grouping logic is bounded by events.")
        
    print("\nQA Complete.")

if __name__ == "__main__":
    qa_metocean()
