import requests
import zipfile
import tempfile
import csv
import io
import os
import shutil
import xml.etree.ElementTree as ET
import yaml
from ..common.paths import AIS_RAW_DIR, CONFIG_DIR

BUCKET_URL = "http://aisdata.ais.dk.s3.eu-central-1.amazonaws.com"

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


def filter_zip_to_writer(zip_path, writer, write_header, bounds, max_sog=None):
    min_lat, max_lat, min_lon, max_lon = bounds
    with zipfile.ZipFile(zip_path, "r") as z:
        csv_names = [name for name in z.namelist() if name.lower().endswith(".csv")]
        if not csv_names:
            raise ValueError(f"No CSV member found in {zip_path}")

        csv_filename = csv_names[0]
        print(f"Extracting and filtering {csv_filename}...")

        with open_text_member(z, csv_filename) as text_stream:
            first_line = text_stream.readline()
            if not first_line:
                return 0, 0, write_header

            delim = ";" if ";" in first_line else ","
            header = next(csv.reader(io.StringIO(first_line), delimiter=delim))
            if write_header:
                writer.writerow(header)
                write_header = False

            reader = csv.reader(text_stream, delimiter=delim, quotechar='"')
            count = 0
            matched = 0

            # Find indices for Lat, Lon, SOG (Indices can vary slightly by DMA version)
            try:
                lat_idx = header.index("Latitude")
                lon_idx = header.index("Longitude")
                sog_idx = header.index("SOG")
            except ValueError:
                # Fallback to standard indices if header names differ
                lat_idx, lon_idx, sog_idx = 3, 4, 5

            for row in reader:
                if len(row) <= max(lat_idx, lon_idx, sog_idx):
                    continue

                count += 1
                try:
                    lat = float(row[lat_idx].replace(",", "."))
                    lon = float(row[lon_idx].replace(",", "."))
                except (ValueError, IndexError):
                    continue

                # Regional Filter
                if not (min_lat <= lat <= max_lat and min_lon <= lon <= max_lon):
                    continue

                # Optional Speed Filter
                if max_sog is not None:
                    try:
                        sog = float(row[sog_idx].replace(",", "."))
                        if sog > max_sog:
                            continue
                    except (ValueError, IndexError):
                        continue

                writer.writerow(row)
                matched += 1

                if count % 1000000 == 0:
                    print(f"Processed {count / 1e6:.1f}M rows from current ZIP, found {matched} matches...")

            return count, matched, write_header


def stream_and_filter(year, month, region_name="european_master", max_sog=None):
    bounds = load_region_bounds(region_name)
    
    # Construct filename based on filters
    region_suffix = region_name.replace("_", "-").title()
    sog_suffix = f"_SogMax{max_sog}" if max_sog is not None else ""
    output_file = os.path.join(AIS_RAW_DIR, f"{region_suffix}_{year}_{month:02d}{sog_suffix}.csv")
    
    temp_output = output_file + ".tmp"
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    keys = list_month_keys(year, month)
    if not keys:
        print(f"No DMA ZIP files found for {year}-{month:02d}.")
        return

    print(f"Found {len(keys)} ZIP file(s) for {year}-{month:02d}.")
    print(f"Filter: Region={region_name}, MaxSOG={max_sog if max_sog else 'None'}")

    total_rows = 0
    total_matches = 0
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
                    rows, matches, write_header = filter_zip_to_writer(
                        tmp_zip_path, writer, write_header, bounds, max_sog=max_sog
                    )
                finally:
                    if tmp_zip_path and os.path.exists(tmp_zip_path):
                        os.remove(tmp_zip_path)

                total_rows += rows
                total_matches += matches
                processed_files += 1
                print(f"[{index}/{len(keys)}] Kept {matches} of {rows} rows.")

        if processed_files == 0 or not os.path.exists(temp_output):
            raise RuntimeError(f"No files were successfully processed for {year}-{month:02d}.")

        os.replace(temp_output, output_file)
        print(
            f"Successfully processed {processed_files} ZIP file(s) for {year}-{month:02d}. "
            f"Saved {total_matches} of {total_rows} rows to {output_file}"
        )
        return output_file

    except Exception:
        if os.path.exists(temp_output):
            os.remove(temp_output)
        raise
