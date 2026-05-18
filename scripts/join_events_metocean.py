import argparse
import sys
from om_pipeline.analysis.join_events import join_events_and_metocean

def main():
    parser = argparse.ArgumentParser(description="Join identified vessel events with 10-minute metocean backbone.")
    parser.add_argument("--backbone", help="Path to input metocean CSV backbone file.")
    parser.add_argument("--output", help="Path to save output joined CSV file.")
    parser.add_argument("--wind-farm", help="Optional wind farm name to filter (e.g. Wikinger).")
    
    args = parser.parse_args()
    
    try:
        join_events_and_metocean(
            backbone_path=args.backbone,
            output_path=args.output,
            wind_farm=args.wind_farm
        )
    except Exception as e:
        print(f"Error during join: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
