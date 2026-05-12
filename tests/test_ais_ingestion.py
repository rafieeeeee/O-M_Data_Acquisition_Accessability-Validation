import os
import zipfile
import csv
import pytest
import io
import math
from om_pipeline.ingestion.ais import filter_zip_to_writer, find_column, load_farm_bounds

def test_find_column():
    header = ["# Timestamp", "Type", "MMSI", "Latitude", "Longitude", "SOG"]
    assert find_column(header, ["latitude", "lat"]) == 3
    assert find_column(header, ["LATITUDE"]) == 3 # Case-insensitive
    assert find_column(header, ["sog", "speed"]) == 5
    
    with pytest.raises(ValueError, match="Required column"):
        find_column(header, ["nonexistent"])

def test_filter_zip_to_writer(tmp_path):
    # 1. Create a synthetic CSV with comma decimals
    csv_content = [
        ["Timestamp", "Type", "MMSI", "Latitude", "Longitude", "SOG"],
        ["01/01/2024 00:00:00", "1", "123456789", "55,0", "7,0", "0,0"], # Inside (comma decimal)
        ["01/01/2024 00:01:00", "1", "123456789", "40.0", "7.0", "0.0"], # Outside Lat
        ["01/01/2024 00:02:00", "1", "123456789", "55.0", "20.0", "0.0"] # Outside Lon
    ]
    
    csv_file = tmp_path / "test.csv"
    with open(csv_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(csv_content)
        
    # 2. Pack into a ZIP
    zip_path = tmp_path / "test.zip"
    with zipfile.ZipFile(zip_path, "w") as z:
        z.write(csv_file, arcname="test.csv")
        
    # 3. Filter
    output = io.StringIO()
    writer = csv.writer(output)
    bounds = (46.5, 60.0, -4.5, 15.0)
    
    stats, write_header = filter_zip_to_writer(zip_path, writer, True, bounds)
    
    # 4. Assert
    assert stats["seen"] == 3
    assert stats["kept"] == 1
    
    output_lines = output.getvalue().strip().split("\n")
    assert len(output_lines) == 2 # Header + 1 match
    assert "55,0" in output_lines[1]
    assert "7,0" in output_lines[1]

def test_filter_zip_with_sog(tmp_path):
    # 1. Create synthetic CSV with varying speeds and mixed headers
    csv_content = [
        ["time", "mmsi", "lat", "lon", "speed over ground"],
        ["01/01/2024 00:00:00", "111", "55.0", "7.0", "0.5"],  # Stationary
        ["01/01/2024 00:01:00", "222", "55.0", "7.0", "15.0"], # High speed
        ["01/01/2024 00:02:00", "333", "55.0", "7.0", "1.9"],  # Near threshold
    ]
    
    csv_file = tmp_path / "speed_test.csv"
    with open(csv_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(csv_content)
        
    zip_path = tmp_path / "speed_test.zip"
    with zipfile.ZipFile(zip_path, "w") as z:
        z.write(csv_file, arcname="speed_test.csv")
        
    # 2. Filter with max_sog=2.0
    output = io.StringIO()
    writer = csv.writer(output)
    bounds = (46.5, 60.0, -4.5, 15.0)
    
    stats, write_header = filter_zip_to_writer(zip_path, writer, True, bounds, max_sog=2.0)
    
    # 3. Assert (Should keep 111 and 333, drop 222)
    assert stats["seen"] == 3
    assert stats["kept"] == 2
    assert stats["above_max_sog"] == 1
    
    output_lines = output.getvalue().strip().split("\n")
    assert len(output_lines) == 3 # Header + 2 matches
    assert "111" in output_lines[1]
    assert "333" in output_lines[2]
    assert "222" not in output.getvalue()

def test_load_farm_bounds(tmp_path):
    # Create synthetic turbine file
    turbine_csv = tmp_path / "turbines.csv"
    content = [
        ["wind_farm", "latitude", "longitude"],
        ["FarmA", "50.0", "10.0"],
        ["FarmA", "50.1", "10.1"]
    ]
    with open(turbine_csv, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(content)
        
    buffer_nm = 2.0
    bounds = load_farm_bounds(str(turbine_csv), buffer_nm=buffer_nm)
    
    assert len(bounds) == 1
    lat_min, lat_max, lon_min, lon_max = bounds[0]
    
    expected_lat_buffer = buffer_nm / 60.0
    assert lat_min == pytest.approx(50.0 - expected_lat_buffer)
    assert lat_max == pytest.approx(50.1 + expected_lat_buffer)
    
    lat_mid = (50.0 + 50.1) / 2.0
    expected_lon_buffer = buffer_nm / (60.0 * math.cos(math.radians(lat_mid)))
    assert lon_min == pytest.approx(10.0 - expected_lon_buffer)
    assert lon_max == pytest.approx(10.1 + expected_lon_buffer)

def test_filter_farm_candidate(tmp_path):
    # 1. Setup farm bounds (around 50.0, 10.0)
    farm_bounds = [(49.9, 50.1, 9.9, 10.1)]
    
    # 2. Synthetic CSV
    csv_content = [
        ["lat", "lon", "sog"],
        ["50.0", "10.0", "1.0"],  # Inside
        ["51.0", "10.0", "1.0"],  # Outside
        ["50.0", "10.0", "5.0"],  # Inside but high SOG
    ]
    csv_file = tmp_path / "farm_test.csv"
    with open(csv_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(csv_content)
        
    zip_path = tmp_path / "farm_test.zip"
    with zipfile.ZipFile(zip_path, "w") as z:
        z.write(csv_file, arcname="farm_test.csv")
        
    # 3. Filter
    output = io.StringIO()
    writer = csv.writer(output)
    
    stats, _ = filter_zip_to_writer(
        zip_path, writer, True, (0, 90, 0, 180), # Large regional bounds
        max_sog=2.0, mode="farm_candidate", farm_bounds=farm_bounds
    )
    
    assert stats["seen"] == 3
    assert stats["kept"] == 1
    assert "50.0" in output.getvalue()
    assert "51.0" not in output.getvalue()
