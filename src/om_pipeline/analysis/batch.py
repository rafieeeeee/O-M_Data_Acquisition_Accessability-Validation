import os
import yaml
from ..ingestion.ais import stream_and_filter
from ..identification.dwell_events import identify_vessels
from ..common.paths import AIS_RAW_DIR, INTERIM_DIR, CONFIG_DIR

def load_batch_config():
    """Load batch processing configuration."""
    config_path = os.path.join(CONFIG_DIR, "ais_slices.yaml")
    if not os.path.exists(config_path):
        return range(2009, 2025), [1, 7], []
        
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    
    slices = config.get("ais_slices", {})
    return (
        slices.get("years", range(2009, 2025)), 
        slices.get("months", [1, 7]),
        slices.get("exclusions", [])
    )

def batch_process(years=None, months=None, region_name="german_bight", max_sog=None):
    c_years, c_months, exclusions = load_batch_config()
    if years is None: years = c_years
    if months is None: months = c_months
    
    turbine_file = os.path.join(INTERIM_DIR, 'European_Turbine_Coordinates.csv')
    if not os.path.exists(turbine_file):
        raise FileNotFoundError(f"Turbine coordinate file not found: {turbine_file}. Run prepare_turbine_data first.")

    for year in years:
        for month in months:
            # Check exclusions
            if any(exc.get('year') == year and exc.get('month') == month for exc in exclusions):
                print(f"Skipping excluded slice: {year}-{month:02d}")
                continue
                
            print(f"\n{'='*60}")
            print(f"PROCESSING {region_name.upper()} SLICE: {year}-{month:02d}")
            if max_sog: print(f"Speed Filter: SOG <= {max_sog} kn")
            print(f"{'='*60}")
            
            # Construct standard filenames (must match ais.py logic)
            region_suffix = region_name.replace("_", "-").title()
            sog_suffix = f"_SogMax{max_sog}" if max_sog is not None else ""
            ais_filename = os.path.join(AIS_RAW_DIR, f"{region_suffix}_{year}_{month:02d}{sog_suffix}.csv")
            
            registry_filename = os.path.join(INTERIM_DIR, f"Fleet_Registry_{region_suffix}_{year}_{month:02d}{sog_suffix}.csv")
            
            # Step 1: Stream and Filter
            if not os.path.exists(ais_filename):
                print(f"Streaming and filtering AIS for {year}-{month:02d}...")
                ais_filename = stream_and_filter(year, month, region_name=region_name, max_sog=max_sog)
            else:
                print(f"AIS file already exists: {ais_filename}")
            
            # Step 2: Identify Vessels
            if not os.path.exists(registry_filename):
                if ais_filename and os.path.exists(ais_filename):
                    print(f"Identifying vessels for {year}-{month:02d}...")
                    identify_vessels(ais_filename, turbine_file)
                else:
                    print(f"Skipping identification: AIS file missing for {year}-{month:02d}")
            else:
                print(f"Registry already exists: {registry_filename}")
