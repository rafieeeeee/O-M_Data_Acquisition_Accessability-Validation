import pytest
import pandas as pd
import numpy as np
import io
import os
from om_pipeline.identification.dwell_events import identify_vessels
from om_pipeline.common.paths import INTERIM_DIR

@pytest.fixture
def sample_turbine_file(tmp_path):
    f = tmp_path / "turbines.csv"
    df = pd.DataFrame({
        'wind_farm': ['FarmA', 'FarmA'],
        'latitude': [55.0, 55.001],
        'longitude': [7.0, 7.001],
        'country': ['Denmark', 'Denmark']
    })
    df.to_csv(f, index=False)
    return str(f)

def test_identify_vessels_strict_sequencing(tmp_path, sample_turbine_file):
    # Create synthetic AIS file with overlapping foundation pings and gaps
    # MMSI 1: 
    #   00:00 - Near F0 (dist 10)
    #   00:00 - Near F1 (dist 50) -> Should be dropped (overlapping assignment)
    #   00:10 - Near F0 (dist 10) -> Continues Event 1
    #   00:41 - Near F0 (dist 10) -> New Event 2 (Gap > 30)
    #   00:45 - Near F1 (dist 10) -> New Event 3 (Found changed)
    
    ais_data = [
        ["# Timestamp", "MMSI", "Latitude", "Longitude", "SOG", "Name", "Ship type", "Length", "Draught"],
        ["01/01/2024 00:00:00", "1", "55.0", "7.0", "0.0", "V1", "CTV", "20", "2"],
        ["01/01/2024 00:00:00", "1", "55.001", "7.001", "0.0", "V1", "CTV", "20", "2"],
        ["01/01/2024 00:10:00", "1", "55.0", "7.0", "0.0", "V1", "CTV", "20", "2"],
        ["01/01/2024 00:41:00", "1", "55.0", "7.0", "0.0", "V1", "CTV", "20", "2"],
        ["01/01/2024 00:45:00", "1", "55.001", "7.001", "0.0", "V1", "CTV", "20", "2"],
        ["01/01/2024 01:10:00", "1", "55.001", "7.001", "0.0", "V1", "CTV", "20", "2"],
    ]
    
    ais_file = tmp_path / "ais.csv"
    with open(ais_file, "w") as f:
        import csv
        writer = csv.writer(f)
        writer.writerows(ais_data)
        
    events_path, registry_path = identify_vessels(str(ais_file), sample_turbine_file)
    
    events = pd.read_csv(events_path)
    # We should have 3 events for MMSI 1
    # Event 1: 00:00 - 00:10 (F0)
    # Event 2: 00:41 - 00:41 (F0) -> Will be dropped because < 15 mins
    # Event 3: 00:45 - 01:10 (F1)
    
    # Wait, the 15 min filter might drop event 1 too (10 mins).
    # Let's check Event 3 (25 mins)
    assert len(events) == 1
    assert events.iloc[0]['event_class'] == "Transfer"
    assert events.iloc[0]['found_id'] == 1 # Second row in turbines csv
    assert events.iloc[0]['ping_count'] == 2 # 00:45 and 01:10

def test_vessel_empirical_classification(tmp_path, sample_turbine_file):
    # MMSI 1: 20m length, 30 min event -> Probable CTV
    # MMSI 2: 20m length, 300 min event (pings every 25 mins) -> Cargo (stays original)
    
    ais_data = [
        ["# Timestamp", "MMSI", "Latitude", "Longitude", "SOG", "Name", "Ship type", "Length", "Draught"],
        ["01/01/2024 00:00:00", "1", "55.0", "7.0", "0.0", "V1", "Cargo", "20", "2"],
        ["01/01/2024 00:30:00", "1", "55.0", "7.0", "0.0", "V1", "Cargo", "20", "2"],
    ]
    # Add pings for MMSI 2 every 25 mins for 300 mins
    for i in range(0, 325, 25):
        m = i % 60
        h = i // 60
        ais_data.append([f"01/01/2024 {h:02d}:{m:02d}:00", "2", "55.001", "7.001", "0.0", "V2", "Cargo", "20", "2"])
    
    ais_file = tmp_path / "ais_vessels.csv"
    with open(ais_file, "w") as f:
        import csv
        writer = csv.writer(f)
        writer.writerows(ais_data)
        
    events_path, registry_path = identify_vessels(str(ais_file), sample_turbine_file)
    
    registry = pd.read_csv(registry_path)
    v1 = registry[registry['MMSI'] == 1].iloc[0]
    v2 = registry[registry['MMSI'] == 2].iloc[0]
    
    assert v1['empirical_type'] == "Probable CTV"
    assert v2['empirical_type'] == "Cargo"
    
    events = pd.read_csv(events_path)
    e1 = events[events['MMSI'] == 1].iloc[0]
    e2 = events[events['MMSI'] == 2].iloc[0]
    
    assert e1['event_class'] == "Transfer"
    assert e2['event_class'] == "Transfer"
