import requests
import zipfile
import tempfile
import csv
import io
import os
import shutil
import xml.etree.ElementTree as ET
import yaml
import math
import pandas as pd
from ..common.paths import AIS_RAW_DIR, CONFIG_DIR, INTERIM_DIR

BUCKET_URL = "http://aisdata.ais.dk.s3.eu-central-1.amazonaws.com"

def find_column(header, synonyms):
    """Find column index by case-insensitive synonyms. Fails loudly if missing."""
    for i, col in enumerate(header):
        col_clean = col.strip().lower()
        for syn in synonyms:
            if syn.lower() == col_clean: # Exact match preferred
                return i
        for syn in synonyms:
            if syn.lower() in col_clean: # Substring match as fallback
                return i
    
    raise ValueError(f"Required column (one of {synonyms}) not found in header: {header}")

def load_region_bounds(region_name="european_master"):
    """Load regional bounds from configuration file."""
    config_path = os.path.join(CONFIG_DIR, "regions.yaml")
    if not os.path.exists(config_path):
        return (46.5, 60.0, -4.5, 15.0) # Fallback
        
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    
    region = config.get("regions", {}).get(region_name, {})
    return (
        region.get("lat_min", 46.5),
        region.get("lat_max", 60.0),
        region.get("lon_min", -4.5),
        region.get("lon_max", 15.0)
    )

def load_farm_bounds(turbine_file, buffer_nm=2.0):
    """Load wind farm bounding boxes with a nautical mile buffer."""
    if not os.path.exists(turbine_file):
        raise FileNotFoundError(f"Turbine database not found at {turbine_file}")
        
    df = pd.read_csv(turbine_file)
    # Ensure column names match expected or normalize them
    # Based on dwell_events.py, it uses 'wind_farm', 'latitude', 'longitude'
    
    farms = df.groupby('wind_farm').agg({
        'latitude': ['min', 'max'],
        'longitude': ['min', 'max']
    })
    farms.columns = ['lat_min', 'lat_max', 'lon_min', 'lon_max']
    
    lat_buffer = buffer_nm / 60.0
    
    buffered_bounds = []
    for _, row in farms.iterrows():
        lat_mid = (row['lat_min'] + row['lat_max']) / 2.0
        # Guard cos calculation
        cos_lat = math.cos(math.radians(lat_mid))
        lon_buffer = buffer_nm / (60.0 * max(cos_lat, 0.01))
        
        buffered_bounds.append((
            row['lat_min'] - lat_buffer,
            row['lat_max'] + lat_buffer,
            row['lon_min'] - lon_buffer,
            row['lon_max'] + lon_buffer
        ))
    return buffered_bounds

def list_month_keys(year, month):
    """Return all DMA archive keys for a requested year/month."""
    prefix = f"{year}/aisdk-{year}-{month:02d}"
    url = f"{BUCKET_URL}/?prefix={prefix}"
    print(f"Enumerating S3 keys with prefix {prefix}...")

    keys = []
    continuation_token = None

    while True:
        params = {"prefix": prefix}
        if continuation_token:
            params["continuation-token"] = continuation_token

        response = requests.get(BUCKET_URL + "/", params=params, timeout=30)
        response.raise_for_status()

        root = ET.fromstring(response.content)
        namespace = ""
        if root.tag.startswith("{"):
            namespace = root.tag.split("}")[0] + "}"

        keys.extend(
            key.text
            for key in root.findall(f".//{namespace}Key")
            if key.text and key.text.endswith(".zip")
        )

        truncated = root.findtext(f"{namespace}IsTruncated")
        if truncated != "true":
            break

        continuation_token = root.findtext(f"{namespace}NextContinuationToken")
        if not continuation_token:
            break

    return sorted(keys)


def download_zip(key):
    url = f"{BUCKET_URL}/{key}"
    with requests.get(url, stream=True, timeout=60) as response:
        if response.status_code == 404:
            raise FileNotFoundError(f"404 Not Found for {url}")
        response.raise_for_status()

        with tempfile.NamedTemporaryFile(delete=False, suffix=".zip") as tmp_zip:
            shutil.copyfileobj(response.raw, tmp_zip)
            return tmp_zip.name


def open_text_member(zip_file, member_name):
    binary_member = zip_file.open(member_name)
    return io.TextIOWrapper(binary_member, encoding="utf-8-sig", newline="")


def filter_zip_to_writer(zip_path, writer, write_header, bounds, max_sog=None, mode="regional", farm_bounds=None):
    min_lat, max_lat, min_lon, max_lon = bounds
    with zipfile.ZipFile(zip_path, "r") as z:
        csv_names = [name for name in z.namelist() if name.lower().endswith(".csv")]
        if not csv_names:
            raise ValueError(f"No CSV member found in {zip_path}")

        csv_filename = csv_names[0]
        print(f"Extracting and filtering {csv_filename} (Mode: {mode})...")

        with open_text_member(z, csv_filename) as text_stream:
            first_line = text_stream.readline()
            if not first_line:
                return {"seen": 0, "kept": 0}, write_header

            delim = ";" if ";" in first_line else ","
            header = next(csv.reader(io.StringIO(first_line), delimiter=delim))
            if write_header:
                writer.writerow(header)
                write_header = False

            reader = csv.reader(text_stream, delimiter=delim, quotechar='"')
            
            # Use explicit header resolver
            lat_idx = find_column(header, ["latitude", "lat"])
            lon_idx = find_column(header, ["longitude", "lon", "long"])
            sog_idx = find_column(header, ["sog", "speed over ground", "speed"])

            stats = {
                "seen": 0,
                "kept": 0,
                "bad_lat_lon": 0,
                "bad_sog": 0,
                "outside_region": 0,
                "above_max_sog": 0
            }


            for row in reader:
                if len(row) <= max(lat_idx, lon_idx, sog_idx):
                    continue

                stats["seen"] += 1
                try:
                    lat_str = row[lat_idx].replace(",", ".")
                    lon_str = row[lon_idx].replace(",", ".")
                    lat = float(lat_str)
                    lon = float(lon_str)
                except (ValueError, IndexError):
                    stats["bad_lat_lon"] += 1
                    continue

                # Spatial Filter
                if mode == "regional":
                    if not (min_lat <= lat <= max_lat and min_lon <= lon <= max_lon):
                        stats["outside_region"] += 1
                        continue
                elif mode == "farm_candidate":
                    if not farm_bounds:
                        raise ValueError("farm_bounds must be provided for farm_candidate mode")
                    
                    in_any_farm = False
                    for b_lat_min, b_lat_max, b_lon_min, b_lon_max in farm_bounds:
                        if (b_lat_min <= lat <= b_lat_max and b_lon_min <= lon <= b_lon_max):
                            in_any_farm = True
                            break
                    if not in_any_farm:
                        stats["outside_region"] += 1
                        continue

                # Optional Speed Filter
                if max_sog is not None:
                    try:
                        sog = float(row[sog_idx].replace(",", "."))
                        if sog > max_sog:
                            stats["above_max_sog"] += 1
                            continue
                    except (ValueError, IndexError):
                        stats["bad_sog"] += 1
                        continue

                writer.writerow(row)
                stats["kept"] += 1

            return stats, write_header


def stream_and_filter(year, month, region_name="european_master", max_sog=None, mode="regional", buffer_nm=2.0, turbine_file=None):
    bounds = load_region_bounds(region_name)
    farm_bounds = None
    if mode == "farm_candidate":
        if not turbine_file:
            turbine_file = os.path.join(INTERIM_DIR, "European_Turbine_Coordinates.csv")
        farm_bounds = load_farm_bounds(turbine_file, buffer_nm=buffer_nm)
    
    # Construct filename based on mode and filters
    region_suffix = region_name.replace("_", "-").title()
    sog_suffix = f"_SogMax{max_sog}" if max_sog is not None else ""
    
    if mode == "regional":
        output_file = os.path.join(AIS_RAW_DIR, f"{region_suffix}_{year}_{month:02d}{sog_suffix}.csv")
    else:
        output_file = os.path.join(AIS_RAW_DIR, f"Farm-Candidates_{region_suffix}_{year}_{month:02d}{sog_suffix}_Buffer{buffer_nm}nm.csv")
    
    temp_output = output_file + ".tmp"
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    keys = list_month_keys(year, month)
    if not keys:
        print(f"No DMA ZIP files found for {year}-{month:02d}.")
        return

    print(f"Found {len(keys)} ZIP file(s) for {year}-{month:02d}.")
    print(f"Filter: Mode={mode}, Region={region_name}, MaxSOG={max_sog if max_sog else 'None'}")
    if mode == "farm_candidate":
        print(f"Farm Buffer: {buffer_nm} nm")

    total_rows = 0
    total_matches = 0
    total_bad_lat_lon = 0
    total_bad_sog = 0
    processed_files = 0
    write_header = True

    try:
        with open(temp_output, "w", newline="") as f_out:
            writer = csv.writer(f_out)

            for index, key in enumerate(keys, start=1):
                tmp_zip_path = None
                print(f"[{index}/{len(keys)}] Processing {key}...")
                try:
                    tmp_zip_path = download_zip(key)
                    zip_stats, write_header = filter_zip_to_writer(
                        tmp_zip_path, writer, write_header, bounds, 
                        max_sog=max_sog, mode=mode, farm_bounds=farm_bounds
                    )
                    
                    # Update totals
                    total_rows += zip_stats["seen"]
                    total_matches += zip_stats["kept"]
                    total_bad_lat_lon += zip_stats.get("bad_lat_lon", 0)
                    total_bad_sog += zip_stats.get("bad_sog", 0)
                    processed_files += 1
                    
                    # Per-ZIP summary
                    print(f"    Summary: Kept {zip_stats['kept']} of {zip_stats['seen']} rows.")
                finally:
                    if tmp_zip_path and os.path.exists(tmp_zip_path):
                        os.remove(tmp_zip_path)

        if processed_files == 0 or not os.path.exists(temp_output):
            raise RuntimeError(f"No files were successfully processed for {year}-{month:02d}.")

        os.replace(temp_output, output_file)
        
        retention_pct = (total_matches / total_rows * 100) if total_rows > 0 else 0
        
        print("\n--- Processing Summary ---")
        print(f"Mode:              {mode}")
        if mode == "farm_candidate":
            print(f"Buffer:            {buffer_nm} nm")
        print(f"Region:            {region_name}")
        print(f"Output Path:       {output_file}")
        print(f"Rows Scanned:      {total_rows:,}")
        print(f"Rows Kept:         {total_matches:,}")
        print(f"Retention %:       {retention_pct:.2f}%")
        print(f"Malformed SOG:     {total_bad_sog:,}")
        print(f"Malformed Lat/Lon: {total_bad_lat_lon:,}")
        print("--------------------------\n")
        
        return output_file

    except Exception:
        if os.path.exists(temp_output):
            os.remove(temp_output)
        raise
