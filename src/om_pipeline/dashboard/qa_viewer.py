"""
qa_viewer.py

Modular helper library to load visual QA manifest catalogs and safely resolve
local absolute file paths for PNG track map displays. Strictly read-only.
"""

from pathlib import Path
import pandas as pd

# The standard schema columns for Visual QA samples
QA_SCHEMA_COLUMNS = [
    "qa_id", "farm_id", "visit_id", "dwell_id", "vessel_id",
    "dwell_tier", "start_utc", "end_utc", "duration_min",
    "map_path", "automated_reason", "manual_qa_label"
]

def load_qa_samples(root_path: Path) -> pd.DataFrame:
    """
    Finds and consolidates visual_qa_sample.csv and wikinger_visual_qa_sample.csv.
    Handles missing files gracefully by returning an empty DataFrame.

    Args:
        root_path: Path object representing the project root directory

    Returns:
        DataFrame containing consolidated QA samples
    """
    qa_dir = root_path / "reports/ais_dwell"
    sample_csvs = [
        qa_dir / "visual_qa_sample.csv",
        qa_dir / "wikinger_visual_qa_sample.csv"
    ]

    loaded_dfs = []

    for csv_path in sample_csvs:
        if csv_path.exists():
            try:
                df = pd.read_csv(csv_path)
                # Filter/ensure columns match our schema
                cols_to_keep = [c for c in QA_SCHEMA_COLUMNS if c in df.columns]
                df_filtered = df[cols_to_keep].copy()

                # Add any missing schema columns as NaN
                for col in QA_SCHEMA_COLUMNS:
                    if col not in df_filtered.columns:
                        df_filtered[col] = pd.NA

                loaded_dfs.append(df_filtered[QA_SCHEMA_COLUMNS])
            except Exception:
                # Suppress errors to ensure dashboard never crashes due to malformed CSVs
                pass

    if not loaded_dfs:
        return pd.DataFrame(columns=QA_SCHEMA_COLUMNS)

    df_consolidated = pd.concat(loaded_dfs, ignore_index=True)
    return df_consolidated.drop_duplicates(subset=["qa_id"]).reset_index(drop=True)

def resolve_qa_image_path(root_path: Path, relative_map_path: str) -> Path:
    """
    Resolves the local absolute path of a pre-generated visual QA PNG map.
    Checks file existence to prevent downstream loading errors.

    Args:
        root_path: Path object representing the project root directory
        relative_map_path: Relative map path string stored in visual QA csv

    Returns:
        Absolute Path object if the file exists, otherwise None
    """
    if not relative_map_path or not isinstance(relative_map_path, str):
        return None

    # Standardize path string
    clean_rel_path = relative_map_path.strip()

    # Resolve absolute path relative to root_path
    abs_path = (root_path / clean_rel_path).resolve()

    # Guard check for path traversal and file existence
    try:
        if abs_path.is_file() and abs_path.exists():
            return abs_path
    except Exception:
        pass

    return None
