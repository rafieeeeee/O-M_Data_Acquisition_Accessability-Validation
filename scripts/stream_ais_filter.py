import argparse
from om_pipeline.ingestion.ais import stream_and_filter

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Stream and filter AIS data from DMA.")
    parser.add_argument("year", type=int, help="Year to process")
    parser.add_argument("month", type=int, help="Month to process")
    parser.add_argument("--region", default="german_bight", help="Region name from regions.yaml")
    parser.add_argument("--max-sog", type=float, default=None, help="Optional maximum SOG filter")
    parser.add_argument("--mode", choices=["regional", "farm_candidate"], default="regional", help="Filtering mode")
    parser.add_argument("--buffer-nm", type=float, default=2.0, help="Nautical mile buffer for farm_candidate mode")
    parser.add_argument("--turbine-file", help="Path to turbine coordinates CSV")

    args = parser.parse_args()
    stream_and_filter(
        args.year, 
        args.month, 
        region_name=args.region, 
        max_sog=args.max_sog,
        mode=args.mode,
        buffer_nm=args.buffer_nm,
        turbine_file=args.turbine_file
    )
