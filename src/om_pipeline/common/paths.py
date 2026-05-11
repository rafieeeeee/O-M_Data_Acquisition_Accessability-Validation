import os

# Root of the repository
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))

# Data Tiered Storage
DATA_DIR = os.path.join(ROOT_DIR, "Data")
RAW_DIR = os.path.join(DATA_DIR, "Raw")
INTERIM_DIR = os.path.join(DATA_DIR, "Interim")
PROCESSED_DIR = os.path.join(DATA_DIR, "Processed")

# Specific Data Paths
AIS_RAW_DIR = os.path.join(RAW_DIR, "AIS")

# Configs
CONFIG_DIR = os.path.join(ROOT_DIR, "configs")

def ensure_dirs():
    """Ensure all required data directories exist."""
    for d in [DATA_DIR, RAW_DIR, INTERIM_DIR, PROCESSED_DIR, AIS_RAW_DIR, CONFIG_DIR]:
        os.makedirs(d, exist_ok=True)
