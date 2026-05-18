import pytest
import pandas as pd
import numpy as np
from om_pipeline.ingestion.metocean import circular_interpolate, MetoceanIngestor
from om_pipeline.ingestion import nora3

def test_circular_interpolate_basic():
    """Test basic interpolation between angles."""
    # Halfway between 0 and 90 is 45
    assert np.isclose(circular_interpolate(0, 90, 0.5), 45.0)

def test_circular_interpolate_cross_zero():
    """Test interpolation crossing the 360/0 boundary using linear components."""
    # Halfway between 350 and 10 should be exactly 0/360 because the chord bisector matches the arc bisector.
    res1 = circular_interpolate(350, 10, 0.5)
    assert np.isclose(res1, 0.0) or np.isclose(res1, 360.0)
    
    # 25% on the chord from 350 to 30.
    res2 = circular_interpolate(350, 30, 0.25)
    # The arc would be 0, but the chord puts it slightly before 0.
    assert 359.0 < res2 < 360.0

def test_upscale_to_10min():
    """Test that hourly data is correctly upscaled to 10-minute intervals."""
    # Create mock hourly data
    times = pd.date_range("2024-07-01 12:00:00", "2024-07-01 14:00:00", freq="1H")
    df_hourly = pd.DataFrame({
        'time': times,
        'hs': [1.0, 2.0, 1.0],
        'tp': [5.0, 6.0, 5.0],
        'wave_direction': [350.0, 10.0, 30.0]
    })
    
    ingestor = MetoceanIngestor()
    df_10min = ingestor.upscale_to_10min(df_hourly)
    
    # Check length: 120 minutes / 10 = 12 intervals + 1 for inclusive end = 13 rows
    assert len(df_10min) == 13
    
    # Check frequencies
    time_diffs = df_10min['time'].diff().dropna().dt.total_seconds()
    assert all(time_diffs == 600)  # 600 seconds = 10 mins
    
    # Check cubic interpolation peak (should smoothly curve between 1 and 2, exceeding 2 is possible with cubic depending on boundary, 
    # but at exact hours it should match)
    hour1 = df_10min[df_10min['time'] == "2024-07-01 13:00:00"].iloc[0]
    assert np.isclose(hour1['hs'], 2.0)
    assert np.isclose(hour1['tp'], 6.0)
    assert np.isclose(hour1['wave_direction'], 10.0)
    
    # Check circular interpolation exactly halfway between 12:00 (350 deg) and 13:00 (10 deg)
    # i.e., at 12:30:00
    halfway = df_10min[df_10min['time'] == "2024-07-01 12:30:00"].iloc[0]
    assert np.isclose(halfway['wave_direction'], 0.0) or np.isclose(halfway['wave_direction'], 360.0)


def test_fetch_nora3_point_defaults_to_monthly_subset(monkeypatch, tmp_path):
    captured = {}

    def fake_open_dataset(url):
        captured["url"] = url
        raise RuntimeError("stop before network")

    monkeypatch.setattr(nora3, "NORA3_CACHE_DIR", tmp_path)
    monkeypatch.setattr(nora3.xr, "open_dataset", fake_open_dataset)

    result = nora3.fetch_nora3_point(
        lat=54.8,
        lon=14.0,
        time_start="2014-01-01",
        time_end="2014-01-02",
    )

    assert captured["url"] == "https://thredds.met.no/thredds/dodsC/nora3_subset_wave/wave_tser/201401_NORA3wave_sub_time_unlimited.nc"
    assert result.empty

def test_fetch_nora3_wind_defaults_to_monthly_subset(monkeypatch, tmp_path):
    captured = {}

    def fake_open_dataset(url):
        captured["url"] = url
        raise RuntimeError("stop before network")

    monkeypatch.setattr(nora3, "NORA3_CACHE_DIR", tmp_path)
    monkeypatch.setattr(nora3.xr, "open_dataset", fake_open_dataset)

    result = nora3.fetch_nora3_wind(
        lat=54.8,
        lon=14.0,
        time_start="2014-01-01",
        time_end="2014-01-02",
    )

    assert captured["url"] == "https://thredds.met.no/thredds/dodsC/nora3_subset_atmos/wind_hourly_v2/arome3kmwind_1hr_201401.nc"
    assert result.empty

def test_fetch_cmems_current_fallback(monkeypatch, tmp_path):
    import os
    from om_pipeline.ingestion import cmems
    monkeypatch.setattr(cmems, "CMEMS_CACHE_DIR", tmp_path)
    
    result = cmems.fetch_cmems_current(
        lat=54.8,
        lon=14.0,
        time_start="2014-01-01 12:00:00",
        time_end="2014-01-01 14:00:00",
    )
    
    assert not result.empty
    assert 'current_speed' in result.columns
    assert 'current_direction' in result.columns
    assert len(result) > 0
    
    # Verify cached file exists
    cache_files = os.listdir(tmp_path)
    assert any("cmems_raw_54.80_14.00_2014_01.csv" in f for f in cache_files)

def test_generalized_upscale_to_10min():
    """Test that upscaling operates dynamically on waves, wind, and current fields."""
    times = pd.date_range("2024-07-01 12:00:00", "2024-07-01 14:00:00", freq="1H")
    df_hourly = pd.DataFrame({
        'time': times,
        'hs': [1.0, 2.0, 1.0],
        'tp': [5.0, 6.0, 5.0],
        'wave_direction': [350.0, 10.0, 30.0],
        'wind_speed_10m': [10.0, 15.0, 10.0],
        'wind_direction_10m': [90.0, 100.0, 110.0],
        'current_speed': [0.2, 0.4, 0.2],
        'current_direction': [180.0, 190.0, 200.0],
    })
    
    ingestor = MetoceanIngestor()
    df_10min = ingestor.upscale_to_10min(df_hourly)
    
    assert len(df_10min) == 13
    assert 'wind_speed_10m' in df_10min.columns
    assert 'wind_direction_10m' in df_10min.columns
    assert 'current_speed' in df_10min.columns
    assert 'current_direction' in df_10min.columns
    
    # Check interpolation at exact hours
    hour1 = df_10min[df_10min['time'] == "2024-07-01 13:00:00"].iloc[0]
    assert np.isclose(hour1['wind_speed_10m'], 15.0)
    assert np.isclose(hour1['wind_direction_10m'], 100.0)
    assert np.isclose(hour1['current_speed'], 0.4)
    assert np.isclose(hour1['current_direction'], 190.0)

