import os
import pandas as pd
import duckdb
import pytest
from om_pipeline.analysis.join_events import join_events_and_metocean
from om_pipeline.common import database

def test_join_events_and_metocean(tmp_path, monkeypatch):
    # Create mock metocean backbone CSV
    backbone_df = pd.DataFrame([
        {
            "timestamp_10min": "2024-07-01 12:00:00",
            "hs": 1.5,
            "tp": 8.0,
            "wave_direction": 180.0,
            "wind_speed_10m": 12.0,
            "wind_direction_10m": 90.0,
            "wind_speed_100m": 14.0,
            "wind_direction_100m": 95.0,
            "current_speed": 0.3,
            "current_direction": 200.0,
            "lat": 54.8,
            "lon": 14.0,
            "found_id": 1,
            "source": "NORA3",
            "interpolation_method": "cubic"
        },
        {
            "timestamp_10min": "2024-07-01 12:10:00",
            "hs": 1.6,
            "tp": 8.1,
            "wave_direction": 185.0,
            "wind_speed_10m": 12.5,
            "wind_direction_10m": 92.0,
            "wind_speed_100m": 14.5,
            "wind_direction_100m": 97.0,
            "current_speed": 0.35,
            "current_direction": 205.0,
            "lat": 54.8,
            "lon": 14.0,
            "found_id": 1,
            "source": "NORA3",
            "interpolation_method": "cubic"
        }
    ])
    backbone_csv = tmp_path / "mock_backbone.csv"
    backbone_df.to_csv(backbone_csv, index=False)
    
    # Setup in-memory DuckDB with mock tables
    conn = duckdb.connect(":memory:")
    conn.execute("""
    CREATE TABLE dwell_events (
        MMSI BIGINT,
        Name VARCHAR,
        "Ship type" VARCHAR,
        wind_farm VARCHAR,
        found_id BIGINT,
        event_id BIGINT,
        start TIMESTAMP,
        "end" TIMESTAMP,
        ping_count BIGINT,
        mean_sog DOUBLE,
        min_dist DOUBLE,
        length DOUBLE,
        draught VARCHAR,
        duration_min DOUBLE,
        event_class VARCHAR
    )
    """)
    conn.execute("""
    CREATE TABLE turbines (
        "Unnamed: 0" BIGINT,
        wind_farm VARCHAR,
        latitude DOUBLE,
        longitude DOUBLE
    )
    """)
    
    # Insert mock event and turbine
    conn.execute("""
    INSERT INTO dwell_events VALUES (
        123456789, 'Mock SOV', 'O&M SOV', 'Wikinger', 1, 101,
        '2024-07-01 11:55:00', '2024-07-01 12:15:00', 10, 0.2, 20.0, 80.0, '6.0', 20.0, 'Transfer'
    )
    """)
    conn.execute("INSERT INTO turbines VALUES (1, 'Wikinger', 54.8, 14.0)")
    
    def mock_get_connection(read_only=True):
        return conn
        
    import om_pipeline.analysis.join_events
    monkeypatch.setattr(om_pipeline.analysis.join_events, "get_connection", mock_get_connection)
    
    output_csv = tmp_path / "mock_joined.csv"
    
    # Run join
    result_df = join_events_and_metocean(
        backbone_path=str(backbone_csv),
        output_path=str(output_csv),
        wind_farm="Wikinger"
    )
    
    # Assertions
    assert len(result_df) == 2
    assert list(result_df['found_id']) == [1, 1]
    assert list(result_df['MMSI']) == [123456789, 123456789]
    assert list(result_df['vessel_name']) == ['Mock SOV', 'Mock SOV']
    assert 'wind_speed_10m' in result_df.columns
    assert 'wind_direction_10m' in result_df.columns
    assert 'current_speed' in result_df.columns
    assert 'current_direction' in result_df.columns
    assert list(result_df['wind_speed_10m']) == [12.0, 12.5]
    assert list(result_df['current_speed']) == [0.3, 0.35]
    assert os.path.exists(output_csv)

