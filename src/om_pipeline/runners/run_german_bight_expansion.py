import os
import sys
from pathlib import Path
import csv
import glob

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from om_pipeline.common.paths import AIS_RAW_DIR, INTERIM_DIR
from om_pipeline.ingestion.ais import filter_local_csv_to_writer, load_farm_bounds, load_region_bounds

def expand_german_bight():
    turbine_file = os.path.join(INTERIM_DIR, "German_Bight_Mega_Arrays.csv")
    
    if not os.path.exists(turbine_file):
        raise FileNotFoundError(f"Missing {turbine_file}. Please generate coordinates first.")
        
    farm_bounds = load_farm_bounds(turbine_file, buffer_nm=2.0)
    region_bounds = load_region_bounds("european_master")
    
    # Identify local bulk DMA files
    # Farm-Candidates are usually something like Farm-Candidates_European-Master_*
    # Wait, the user said "local bulk DMA files". We might have European_Waters_2024_07.csv or Farm-Candidates...
    # Let's filter whatever large Regional CSVs we have in Data/Raw/AIS that match European-Master
    pattern = os.path.join(AIS_RAW_DIR, "*European-Master*.csv")
    bulk_files = [f for f in glob.glob(pattern) if "Farm-Candidates_German-Bight" not in f]
    
    if not bulk_files:
        print("No local bulk files found matching *European-Master*.csv")
        return
        
    for bulk_file in bulk_files:
        basename = os.path.basename(bulk_file)
        
        # We only want to process the raw full region ones if we can, but if they are already Farm-Candidates for Baltic, 
        # wait! The Baltic ones are already filtered for Baltic. If we want German Bight, we need the original bulk DMA.
        # But if the user says "local bulk DMA files", they likely mean the 100+ GB total of files or the ones we have downloaded.
        
        # Create output file path
        # If input is European-Master_2024_01_SogMax2.0.csv -> Farm-Candidates_German-Bight_2024_01_SogMax2.0_Buffer2.0nm.csv
        out_name = basename.replace("European-Master", "German-Bight")
        if not out_name.startswith("Farm-Candidates_"):
            out_name = "Farm-Candidates_" + out_name
            
        out_path = os.path.join(AIS_RAW_DIR, out_name)
        temp_out = out_path + ".tmp"
        
        if os.path.exists(out_path):
            print(f"Skipping {basename}, already expanded to {out_path}")
            continue
            
        print(f"Expanding German Bight from {basename} -> {out_name}")
        
        write_header = True
        try:
            with open(temp_out, "w", newline="") as f_out:
                writer = csv.writer(f_out)
                stats, _ = filter_local_csv_to_writer(
                    bulk_file, writer, write_header, region_bounds, 
                    max_sog=None, mode="farm_candidate", farm_bounds=farm_bounds
                )
            
            os.replace(temp_out, out_path)
            print(f"  Finished: Kept {stats['kept']} out of {stats['seen']} rows.")
        except Exception as e:
            if os.path.exists(temp_out):
                os.remove(temp_out)
            print(f"  Error processing {basename}: {e}")

if __name__ == "__main__":
    expand_german_bight()
