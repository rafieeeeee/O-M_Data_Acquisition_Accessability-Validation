import sys
import os
from om_pipeline.identification.dwell_events import identify_vessels
from om_pipeline.common.paths import INTERIM_DIR

if __name__ == "__main__":
    if len(sys.argv) == 2:
        ais_file = sys.argv[1]
        turbine_file = os.path.join(INTERIM_DIR, 'European_Turbine_Coordinates.csv')
        identify_vessels(ais_file, turbine_file)
    else:
        print("Usage: python3 scripts/identify_vessels_at_scale.py <ais_csv>")
