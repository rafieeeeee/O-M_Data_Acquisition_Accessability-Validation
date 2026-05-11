import os
import zipfile
import csv
import pytest
import io
from om_pipeline.ingestion.ais import filter_zip_to_writer

def test_filter_zip_to_writer(tmp_path):
    # 1. Create a synthetic CSV
    csv_content = [
        ["Timestamp", "Type", "MMSI", "Latitude", "Longitude", "SOG"],
        ["01/01/2024 00:00:00", "1", "123456789", "55.0", "7.0", "0.0"], # Inside
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
    
    count, matched, write_header = filter_zip_to_writer(zip_path, writer, True, bounds)
    
    # 4. Assert
    assert count == 3
    assert matched == 1
    
    output_lines = output.getvalue().strip().split("\r\n")
    assert len(output_lines) == 2 # Header + 1 match
    assert "55.0" in output_lines[1]
    assert "7.0" in output_lines[1]

def test_filter_zip_with_sog(tmp_path):
    # 1. Create synthetic CSV with varying speeds
    csv_content = [
        ["Timestamp", "Type", "MMSI", "Latitude", "Longitude", "SOG"],
        ["01/01/2024 00:00:00", "1", "111", "55.0", "7.0", "0.5"],  # Stationary
        ["01/01/2024 00:01:00", "1", "222", "55.0", "7.0", "15.0"], # High speed
        ["01/01/2024 00:02:00", "1", "333", "55.0", "7.0", "1.9"],  # Near threshold
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
    
    count, matched, write_header = filter_zip_to_writer(zip_path, writer, True, bounds, max_sog=2.0)
    
    # 3. Assert (Should keep 111 and 333, drop 222)
    assert count == 3
    assert matched == 2
    
    output_lines = output.getvalue().strip().split("\r\n")
    assert len(output_lines) == 3 # Header + 2 matches
    assert "111" in output_lines[1]
    assert "333" in output_lines[2]
    assert "222" not in output.getvalue()
