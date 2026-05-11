import argparse
from om_pipeline.analysis.batch import batch_process

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Batch process AIS slices.")
    parser.add_argument("--region", default="german_bight", help="Region name from regions.yaml")
    parser.add_argument("--max-sog", type=float, default=None, help="Optional maximum SOG filter")

    args = parser.parse_args()
    batch_process(region_name=args.region, max_sog=args.max_sog)
