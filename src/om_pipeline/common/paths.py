import os

# Root of the repository
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))

# Data Tiered Storage
DATA_DIR = os.path.join(ROOT_DIR, "Data")
RAW_DIR = os.path.join(DATA_DIR, "Raw")
INTERIM_DIR = os.path.join(DATA_DIR, "Interim")
PROCESSED_DIR = os.path.join(DATA_DIR, "Processed")
TMP_DIR = os.environ.get("OM_PIPELINE_TMP_DIR", os.path.join(INTERIM_DIR, "tmp"))
CACHE_DIR = os.environ.get("OM_PIPELINE_CACHE_DIR", os.path.join(INTERIM_DIR, "cache"))

# Specific Data Paths
AIS_RAW_DIR = os.path.join(RAW_DIR, "AIS")

# Configs
CONFIG_DIR = os.path.join(ROOT_DIR, "configs")

def configure_external_runtime_dirs():
    """Keep Python, xarray, and DuckDB scratch files inside the project data tree."""
    for directory in [TMP_DIR, CACHE_DIR]:
        os.makedirs(directory, exist_ok=True)

    for env_name in ["TMPDIR", "TEMP", "TMP"]:
        current = os.environ.get(env_name)
        if not current or current.startswith(("/tmp", "/var", "/private/var")):
            os.environ[env_name] = TMP_DIR

    os.environ.setdefault("XDG_CACHE_HOME", CACHE_DIR)
    os.environ.setdefault("MPLCONFIGDIR", os.path.join(CACHE_DIR, "matplotlib"))
    os.environ.setdefault("HDF5_USE_FILE_LOCKING", "FALSE")

def ensure_dirs():
    """Ensure all required data directories exist."""
    for d in [DATA_DIR, RAW_DIR, INTERIM_DIR, PROCESSED_DIR, AIS_RAW_DIR, CONFIG_DIR, TMP_DIR, CACHE_DIR]:
        os.makedirs(d, exist_ok=True)


configure_external_runtime_dirs()
