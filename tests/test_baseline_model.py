import importlib.util
from pathlib import Path
import numpy as np
import pandas as pd
import pytest

SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "train_wind_farm_c_baseline.py"
SPEC = importlib.util.spec_from_file_location("train_wind_farm_c_baseline", SCRIPT_PATH)
train_baseline = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(train_baseline)


def test_preprocess_circular_features():
    df = pd.DataFrame({
        "wave_direction_circular_mean": [0.0, 90.0, 180.0, np.nan],
        "other_column": [1, 2, 3, 4]
    })
    
    processed = train_baseline.preprocess_circular_features(df)
    
    # Original column should be dropped
    assert "wave_direction_circular_mean" not in processed.columns
    
    # Sine and cosine columns should be present
    assert "wave_direction_sin" in processed.columns
    assert "wave_direction_cos" in processed.columns
    
    # Verify values (deg to rad: 0 -> sin=0, cos=1; 90 -> sin=1, cos=0; 180 -> sin=0, cos=-1)
    assert np.isclose(processed.loc[0, "wave_direction_sin"], 0.0)
    assert np.isclose(processed.loc[0, "wave_direction_cos"], 1.0)
    
    assert np.isclose(processed.loc[1, "wave_direction_sin"], 1.0)
    assert np.isclose(processed.loc[1, "wave_direction_cos"], 0.0)
    
    assert np.isclose(processed.loc[2, "wave_direction_sin"], 0.0)
    assert np.isclose(processed.loc[2, "wave_direction_cos"], -1.0)
    
    # NaN check
    assert pd.isna(processed.loc[3, "wave_direction_sin"])
    assert pd.isna(processed.loc[3, "wave_direction_cos"])


def test_select_features_for_task():
    df = pd.DataFrame({
        "event_id": [1, 2],
        "asset_id": [10, 20],
        "event_label_care": ["anomaly", "normal"],
        "event_label_model": ["maintenance_success", "unknown"],
        "hs_mean": [1.5, 2.0],
        "hs_null_share": [0.0, 0.0],
        "tp_mean": [6.0, 8.0],
        "share_status_3": [0.4, 0.0],
        "share_label_maintenance_success": [0.4, 0.0]
    })
    
    # Task A: CARE Anomaly Classifier (should contain metocean + SCADA shares)
    X_A, y_A = train_baseline.select_features_for_task(df, "A")
    assert list(y_A.to_numpy()) == [1, 0]
    assert "hs_mean" in X_A.columns
    assert "tp_mean" in X_A.columns
    assert "share_status_3" in X_A.columns
    assert "share_label_maintenance_success" in X_A.columns
    assert "event_id" not in X_A.columns
    assert "event_label_care" not in X_A.columns
    
    # Task B: O&M Workability Boundary (should contain metocean only, no SCADA shares)
    X_B, y_B = train_baseline.select_features_for_task(df, "B")
    assert list(y_B.to_numpy()) == [1, 0]
    assert "hs_mean" in X_B.columns
    assert "tp_mean" in X_B.columns
    assert "share_status_3" not in X_B.columns
    assert "share_label_maintenance_success" not in X_B.columns
    assert "hs_null_share" not in X_B.columns
    assert "event_id" not in X_B.columns
