import os
import pandas as pd
import numpy as np
import pytest
from om_pipeline.analysis.scada_handshake import SCADAHandshake, handshake_scada_and_dwell

def test_assign_label():
    handshaker = SCADAHandshake()
    
    # 1. Test status 3 (Service Mode)
    # Success: Duration >= 30, Proximity < 100
    assert handshaker.assign_label(3, 45.0, 50.0) == "maintenance_success"
    # Aborted: Duration < 30 or Proximity >= 100
    assert handshaker.assign_label(3, 15.0, 50.0) == "attempted_transfer"
    assert handshaker.assign_label(3, 45.0, 150.0) == "attempted_transfer"
    
    # 2. Test status 4 (Downtime Mode)
    # WoW/Standby: Duration >= 60
    assert handshaker.assign_label(4, 75.0, 150.0) == "standby_weather"
    # Aborted: Duration < 60
    assert handshaker.assign_label(4, 20.0, 150.0) == "attempted_transfer"
    
    # 3. Test status 0, 1, 2 (Normal / Derated / Idling)
    # Attempted Transfer: Proximity < 100, Duration < 30
    assert handshaker.assign_label(0, 15.0, 50.0) == "attempted_transfer"
    assert handshaker.assign_label(1, 15.0, 50.0) == "attempted_transfer"
    # Unknown: default
    assert handshaker.assign_label(0, 45.0, 50.0) == "unknown"
    assert handshaker.assign_label(2, 45.0, 150.0) == "unknown"
    
    # 4. Test missing or other status
    assert handshaker.assign_label(np.nan, 45.0, 50.0) == "unknown"
    assert handshaker.assign_label(5, 45.0, 50.0) == "unknown"


def test_scada_handshake_integration(tmp_path):
    # Create mock SCADA data for Event 101 (Wind Farm B) and Event 202 (Wind Farm C)
    farm_b_dir = tmp_path / "Wind Farm B" / "datasets"
    farm_c_dir = tmp_path / "Wind Farm C" / "datasets"
    os.makedirs(farm_b_dir, exist_ok=True)
    os.makedirs(farm_c_dir, exist_ok=True)
    
    # Event 101 (Wind Farm B): No temporal shift, starts in Feb 2022
    scada_101_df = pd.DataFrame([
        {"time_stamp": "2022-02-02 12:00:00", "status_type_id": 3},
        {"time_stamp": "2022-02-02 12:10:00", "status_type_id": 3},
        {"time_stamp": "2022-02-02 12:20:00", "status_type_id": 0}
    ])
    scada_101_df.to_csv(farm_b_dir / "101.csv", sep=";", index=False)
    
    # Event 202 (Wind Farm C): 0-year shift. The SCADA file timestamps already
    # match the true operating calendar.
    scada_202_df = pd.DataFrame([
        {"time_stamp": "2022-12-01 12:00:00", "status_type_id": 4},
        {"time_stamp": "2022-12-01 12:10:00", "status_type_id": 4}
    ])
    scada_202_df.to_csv(farm_c_dir / "202.csv", sep=";", index=False)
    
    # Create mock joined backbone/dwell dataframe
    joined_df = pd.DataFrame([
        # Row 0: Event 101, Farm B (No shift)
        {
            "timestamp_10min": "2022-02-02 12:00:00",
            "wind_farm": "Wind Farm B",
            "event_id": 101.0,
            "duration_min": 45.0,
            "min_dist": 20.0
        },
        # Row 1: Event 202, Farm C (0-year shift: true time maps directly to SCADA time)
        {
            "timestamp_10min": "2022-12-01 12:00:00",
            "wind_farm": "Wind Farm C",
            "event_id": 202.0,
            "duration_min": 90.0,
            "min_dist": 150.0
        },
        # Row 2: Event 999 (Missing SCADA dataset)
        {
            "timestamp_10min": "2022-02-02 12:00:00",
            "wind_farm": "Wind Farm B",
            "event_id": 999.0,
            "duration_min": 45.0,
            "min_dist": 20.0
        }
    ])
    
    # Run handshake
    handshaker = SCADAHandshake(care_base_dir=str(tmp_path))
    result_df = handshaker.apply_handshake(joined_df)
    
    # Verify results
    assert len(result_df) == 3
    
    # Event 101 checks (Row 0): Status should be 3, label should be maintenance_success
    assert result_df.loc[0, 'status_type_id'] == 3
    assert result_df.loc[0, 'label'] == "maintenance_success"
    
    # Event 202 checks (Row 1): True timestamp directly matches SCADA. Status should be 4, label standby_weather
    assert result_df.loc[1, 'status_type_id'] == 4
    assert result_df.loc[1, 'label'] == "standby_weather"
    
    # Event 999 checks (Row 2): Missing SCADA, status nan, label unknown
    assert pd.isna(result_df.loc[2, 'status_type_id'])
    assert result_df.loc[2, 'label'] == "unknown"
