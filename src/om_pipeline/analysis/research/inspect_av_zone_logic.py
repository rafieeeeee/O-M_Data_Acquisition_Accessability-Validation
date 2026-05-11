import pandas as pd
import sys

def inspect(file_path):
    print(f"Loading AIS data from {file_path}...")
    df = pd.read_csv(file_path)

    # Clean column names
    df.columns = [c.strip().replace('# ', '').replace('#', '') for c in df.columns]

    # Convert coordinates
    df['Latitude'] = df['Latitude'].astype(str).str.replace(',', '.').astype(float)
    df['Longitude'] = df['Longitude'].astype(str).str.replace(',', '.').astype(float)
    df['SOG'] = df['SOG'].astype(str).str.replace(',', '.').astype(float)

    # Filter for Alpha Ventus region only
    mask = (df['Latitude'].between(53.96, 54.06)) & (df['Longitude'].between(6.55, 6.66))
    av_df = df[mask].copy()

    print(f"Total pings in Alpha Ventus zone: {len(av_df)}")

    # Find stationary pings
    stationary = av_df[av_df['SOG'] < 0.5].copy()
    print(f"Stationary pings: {len(stationary)}")

    if not stationary.empty:
        count_col = df.columns[0]
        # Use available columns for grouping, handle missing Name/Type
        group_cols = ['MMSI']
        if 'Name' in df.columns: group_cols.append('Name')
        if 'Ship type' in df.columns: group_cols.append('Ship type')
        
        summary = stationary.groupby(group_cols).agg({
            count_col: 'count',
            'Latitude': 'mean',
            'Longitude': 'mean'
        }).rename(columns={count_col: 'Pings'}).sort_values('Pings', ascending=False)
        
        print(f"\nVessels dwelling in Alpha Ventus Zone:")
        print(summary)
    else:
        print("No stationary vessels found in the Alpha Ventus zone.")

if __name__ == "__main__":
    if len(sys.argv) == 2:
        inspect(sys.argv[1])
    else:
        print("Usage: python3 Scripts/inspect_av_zone.py <csv_file>")
