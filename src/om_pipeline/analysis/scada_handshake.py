import os
import pandas as pd
import numpy as np

class SCADAHandshake:
    """
    Synchronizes SCADA operational status codes with vessel dwell events in the 10-minute backbone.
    Implements the labeling taxonomy defined in docs/stage2-label-taxonomy.md.
    """
    
    def __init__(self, care_base_dir=None):
        if care_base_dir is None:
            # Default path relative to project root
            care_base_dir = os.path.join("Data", "CARE_To_Compare")
        self.care_base_dir = care_base_dir
        self._scada_cache = {}  # Cache SCADA CSVs in memory by event_id to optimize speed
        
    def _find_scada_file(self, event_id):
        """Locates the SCADA CSV file for a given event_id in Wind Farm B or C."""
        if not event_id or pd.isna(event_id):
            return None
            
        # Convert to int/str to handle numeric formats
        event_str = str(int(event_id))
        
        # Check Wind Farm B and C subdirectories
        for farm in ["Wind Farm B", "Wind Farm C"]:
            candidate_path = os.path.join(self.care_base_dir, farm, "datasets", f"{event_str}.csv")
            if os.path.exists(candidate_path):
                return candidate_path
        return None

    def load_scada(self, event_id):
        """Loads and caches the SCADA dataset for a given event_id."""
        if not event_id or pd.isna(event_id):
            return None
            
        event_str = str(int(event_id))
        if event_str in self._scada_cache:
            return self._scada_cache[event_str]
            
        file_path = self._find_scada_file(event_id)
        if file_path is None:
            return None
            
        try:
            # Load SCADA CSV (semicolon delimited)
            df = pd.read_csv(file_path, sep=";")
            df['time_stamp'] = pd.to_datetime(df['time_stamp'])
            # Set index for fast lookup
            df = df.set_index('time_stamp')
            self._scada_cache[event_str] = df
            return df
        except Exception as e:
            print(f"Warning: Failed to load SCADA file {file_path}: {e}")
            return None

    def get_status_at_time(self, event_id, timestamp):
        """Looks up the status_type_id for a given event and timestamp."""
        df = self.load_scada(event_id)
        if df is None:
            return np.nan
            
        ts = pd.to_datetime(timestamp)
        # Try exact match
        if ts in df.index:
            return df.loc[ts, 'status_type_id']
            
        # Fallback: nearest match within 10 minutes
        try:
            idx = df.index.get_indexer([ts], method='nearest', tolerance=pd.Timedelta('10m'))
            if idx[0] != -1:
                return df.iloc[idx[0]]['status_type_id']
        except Exception:
            pass
            
        return np.nan

    def assign_label(self, status_type_id, duration_min, min_dist):
        """
        Applies the O&M label taxonomy based on turbine status, vessel proximity, and duration.
        - status_type_id:
            0: Normal Operation
            1: Derated Operation
            2: Idling
            3: Service (Active maintenance team on site)
            4: Downtime (Fault / Standstill)
            5: Other
        """
        # Default fallback when SCADA is missing
        if pd.isna(status_type_id):
            return "unknown"
            
        status = int(status_type_id)
        duration = float(duration_min) if not pd.isna(duration_min) else 0.0
        dist = float(min_dist) if not pd.isna(min_dist) else 999.0
        
        if status == 3: # Service Mode
            if duration >= 30.0 and dist < 100.0:
                return "maintenance_success"
            else:
                return "attempted_transfer"
                
        elif status == 4: # Downtime
            if duration >= 60.0:
                return "standby_weather"
            else:
                return "attempted_transfer"
                
        elif status in [0, 1, 2]: # Operation / Idling (No service team)
            if dist < 100.0 and duration < 30.0:
                return "attempted_transfer"
            return "unknown"
            
        else: # State 5: Other
            return "unknown"

    def apply_handshake(self, joined_df):
        """
        Performs the temporal handshake. Maps status codes to the 10-minute backbone
        and assigns O&M activity labels.
        """
        df = joined_df.copy()
        
        # Initialize target columns
        df['status_type_id'] = np.nan
        df['label'] = "unknown"
        
        # Group by event_id to optimize file access
        if 'event_id' not in df.columns:
            print("Warning: 'event_id' column not found in joined dataframe. SCADA handshake skipped.")
            return df
            
        unique_events = df['event_id'].dropna().unique()
        print(f"Applying SCADA handshake for {len(unique_events)} unique events...")
        
        for event_id in unique_events:
            scada_df = self.load_scada(event_id)
            if scada_df is None:
                continue
                
            # Filter rows for this event
            mask = df['event_id'] == event_id
            event_rows = df[mask]
            
            # Map timestamps
            for idx, row in event_rows.iterrows():
                lookup_ts = pd.to_datetime(row['timestamp_10min'])
                
                # Lookup status
                status = np.nan
                if lookup_ts in scada_df.index:
                    status = scada_df.loc[lookup_ts, 'status_type_id']
                else:
                    # Fallback to nearest 10-minute
                    try:
                        pos = scada_df.index.get_indexer([lookup_ts], method='nearest', tolerance=pd.Timedelta('10m'))
                        if pos[0] != -1:
                            status = scada_df.iloc[pos[0]]['status_type_id']
                    except Exception:
                        pass
                
                if not pd.isna(status):
                    df.at[idx, 'status_type_id'] = status
                    
                    # Determine label
                    duration = row.get('duration_min', np.nan)
                    dist = row.get('min_dist', np.nan)
                    label = self.assign_label(status, duration, dist)
                    df.at[idx, 'label'] = label
                    
        return df

def handshake_scada_and_dwell(joined_df, care_base_dir=None):
    """Convenience functional interface to execute the handshake."""
    handshaker = SCADAHandshake(care_base_dir=care_base_dir)
    return handshaker.apply_handshake(joined_df)
