import sys
from om_pipeline.ingestion.ais import stream_and_filter

if __name__ == "__main__":
    if len(sys.argv) == 3:
        year = int(sys.argv[1])
        month = int(sys.argv[2])
        stream_and_filter(year, month)
    else:
        print("Usage: python3 scripts/stream_ais_filter.py <year> <month>")
