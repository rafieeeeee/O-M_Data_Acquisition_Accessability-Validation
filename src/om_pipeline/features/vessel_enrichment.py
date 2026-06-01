import pandas as pd
import numpy as np

def enrich_vessel_class(df: pd.DataFrame, length_col: str = 'length') -> pd.DataFrame:
    """
    Preserve raw AIS ship type taxonomy.

    vessel_class is retained only as a backwards-compatible alias.
    It must not be inferred from vessel length, name, MMSI, or operational heuristics.
    """
    ship_type_candidates = [
        "ship_type_label",
        "ship_type",
        "Ship type",
        "shiptype",
        "vessel_type",
        "empirical_type"
    ]

    source_col = next((c for c in ship_type_candidates if c in df.columns), None)

    if source_col is None:
        df["vessel_class"] = "Unknown"
        df["ship_type_raw"] = "Unknown"
        df["ship_type_label"] = "Unknown"
    else:
        # Compatibility alias only.
        # Do not infer vessel_class from length, name, MMSI, or operational heuristics.
        # This column mirrors the raw AIS / ITU-R M.585 ship type label/value.
        cleaned_series = (
            df[source_col]
            .astype("string")
            .fillna("Unknown")
            .replace({"": "Unknown", "nan": "Unknown", "None": "Unknown"})
        )
        df["vessel_class"] = cleaned_series
        df["ship_type_raw"] = df[source_col].fillna("Unknown")
        df["ship_type_label"] = cleaned_series

    # Canonical columns alignment
    if "ship_type_raw" not in df.columns:
        df["ship_type_raw"] = "Unknown"
    if "ship_type_label" not in df.columns:
        df["ship_type_label"] = "Unknown"

    return df
