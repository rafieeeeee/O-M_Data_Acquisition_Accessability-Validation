import pytest
import pandas as pd
import numpy as np
from om_pipeline.features.vessel_enrichment import enrich_vessel_class

def test_vessel_class_does_not_use_length_thresholds():
    """Verify that length thresholds do not influence classification."""
    df = pd.DataFrame({
        "mmsi": [1, 2, 3],
        "ship_type": ["Cargo", "Passenger", "Towing"],
        "length": [25.0, 65.0, 120.0],
        "vessel_name": ["CTV Example", "SOV Example", "BIG CRANE"]
    })

    out = enrich_vessel_class(df)

    assert list(out["vessel_class"]) == ["Cargo", "Passenger", "Towing"]
    assert "CTV" not in set(out["vessel_class"])
    assert "SOV" not in set(out["vessel_class"])
    assert "HLV" not in set(out["vessel_class"])
    assert list(out["ship_type_raw"]) == ["Cargo", "Passenger", "Towing"]
    assert list(out["ship_type_label"]) == ["Cargo", "Passenger", "Towing"]

def test_vessel_class_mirrors_raw_ship_type_synonyms():
    """Verify synonym columns for raw ship type are resolved correctly."""
    df1 = pd.DataFrame({"ship_type_label": ["Cargo"]})
    df2 = pd.DataFrame({"ship_type": ["Passenger"]})
    df3 = pd.DataFrame({"Ship type": ["Towing"]})
    df4 = pd.DataFrame({"empirical_type": ["Dredger"]})

    out1 = enrich_vessel_class(df1)
    out2 = enrich_vessel_class(df2)
    out3 = enrich_vessel_class(df3)
    out4 = enrich_vessel_class(df4)

    assert out1.iloc[0]["vessel_class"] == "Cargo"
    assert out2.iloc[0]["vessel_class"] == "Passenger"
    assert out3.iloc[0]["vessel_class"] == "Towing"
    assert out4.iloc[0]["vessel_class"] == "Dredger"

def test_missing_ship_type_defaults_safely_to_unknown():
    """Verify that missing/null ship types map safely to 'Unknown'."""
    df = pd.DataFrame({
        "mmsi": [1, 2, 3],
        "ship_type": [None, np.nan, ""]
    })

    out = enrich_vessel_class(df)

    assert list(out["vessel_class"]) == ["Unknown", "Unknown", "Unknown"]
    assert list(out["ship_type_label"]) == ["Unknown", "Unknown", "Unknown"]
