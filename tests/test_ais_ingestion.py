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
