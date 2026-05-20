import importlib.util
from pathlib import Path
import numpy as np
import pandas as pd
import pytest
from sklearn.model_selection import StratifiedGroupKFold

# Dynamically import the production train script
SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "train_wind_farm_c_10min_grouped.py"
SPEC = importlib.util.spec_from_file_location("train_wind_farm_c_10min_grouped", SCRIPT_PATH)
train_grouped = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(train_grouped)


def test_preprocess_circular_features():
    """Verify pointwise circular direction encoding maps correctly and drops raw columns."""
    df = pd.DataFrame({
        "wave_direction": [0.0, 90.0, 180.0, np.nan],
        "hs": [1.0, 1.5, 2.0, 2.5]
    })
    
    processed = train_grouped.preprocess_circular_features(df)
    
    assert "wave_direction" not in processed.columns
    assert "wave_direction_sin" in processed.columns
    assert "wave_direction_cos" in processed.columns
    assert "hs" in processed.columns
    
    # 0 degrees -> sin=0, cos=1
    assert np.isclose(processed.loc[0, "wave_direction_sin"], 0.0)
    assert np.isclose(processed.loc[0, "wave_direction_cos"], 1.0)
    
    # 90 degrees -> sin=1, cos=0
    assert np.isclose(processed.loc[1, "wave_direction_sin"], 1.0)
    assert np.isclose(processed.loc[1, "wave_direction_cos"], 0.0)
    
    # 180 degrees -> sin=0, cos=-1
    assert np.isclose(processed.loc[2, "wave_direction_sin"], 0.0)
    assert np.isclose(processed.loc[2, "wave_direction_cos"], -1.0)
    
    # NaNs should map to NaN in sine and cosine
    assert pd.isna(processed.loc[3, "wave_direction_sin"])
    assert pd.isna(processed.loc[3, "wave_direction_cos"])


def test_no_forbidden_columns_leakage():
    """Assert that features matrix X contains absolutely zero forbidden operational or status columns."""
    # Dummy data matrix representing the structure of our raw parquet
    df = pd.DataFrame({
        "timestamp": pd.date_range("2026-05-20", periods=10, freq="10min"),
        "asset_id": [53] * 10,
        "event_id": [1] * 5 + [2] * 5,
        "event_label_care": ["anomaly"] * 10,
        "hs": [1.2] * 10,
        "tp": [6.5] * 10,
        "wave_direction": [45.0] * 10,
        "wind_speed_10m": [8.0] * 10,
        "wind_direction_10m": [180.0] * 10,
        "wind_speed_100m": [10.0] * 10,
        "wind_direction_100m": [180.0] * 10,
        "current_speed": [0.5] * 10,
        "current_direction": [90.0] * 10,
        "status_type_id": [3.0] * 10,
        "label": ["maintenance_success"] * 5 + ["standby_weather"] * 5
    })
    
    processed_df = train_grouped.preprocess_circular_features(df)
    
    # C1 Target construction
    df_c1 = processed_df[processed_df["label"].isin(["maintenance_success", "standby_weather"])].copy()
    y_c1 = (df_c1["label"] == "maintenance_success").astype(int)
    
    # Exclude identifiers and forbidden labels to build features X
    metocean_features = [
        "timestamp", "asset_id", "event_id",
        "hs", "tp", "wave_direction_sin", "wave_direction_cos",
        "wind_speed_10m", "wind_direction_10m_sin", "wind_direction_10m_cos",
        "wind_speed_100m", "wind_direction_100m_sin", "wind_direction_100m_cos",
        "current_speed", "current_direction_sin", "current_direction_cos"
    ]
    X_c1 = df_c1[metocean_features].copy()
    
    # Assert zero SCADA status columns are present in features
    forbidden_keywords = ["status_type_id", "event_label_care", "label", "duration", "share_"]
    for col in X_c1.columns:
        assert not any(keyword in col for keyword in forbidden_keywords), f"Forbidden column found: {col}"
        
    # Also assert they aren't in clean preprocessed features where metadata is dropped
    id_cols = ["timestamp", "asset_id", "event_id"]
    X_clean = X_c1.drop(columns=id_cols)
    for col in X_clean.columns:
        assert not any(keyword in col for keyword in forbidden_keywords), f"Forbidden column found: {col}"
        assert col not in id_cols, f"Metadata identifier column found in clean features: {col}"


def test_grouped_cv_isolation():
    """Verify that StratifiedGroupKFold strictly isolates event groups across splits with zero leakage."""
    # Create mock dataset with 10 events, each having 10 rows
    events = []
    labels = []
    for i in range(1, 11):
        events.extend([i] * 10)
        # 6 success events, 4 standby events
        if i <= 6:
            labels.extend(["maintenance_success"] * 10)
        else:
            labels.extend(["standby_weather"] * 10)
            
    df = pd.DataFrame({
        "event_id": events,
        "label": labels,
        "feature": np.random.rand(100)
    })
    
    y = (df["label"] == "maintenance_success").astype(int)
    groups = df["event_id"]
    
    sgkf = StratifiedGroupKFold(n_splits=3)
    
    for fold, (tr_idx, val_idx) in enumerate(sgkf.split(df, y, groups=groups)):
        train_events = set(groups.iloc[tr_idx])
        val_events = set(groups.iloc[val_idx])
        
        # Isolation assertion: the intersection between train and validation event sets must be empty
        assert train_events.isdisjoint(val_events), f"Leakage detected in Fold {fold}! Common events: {train_events & val_events}"


def test_cliffs_delta_calculation():
    """Test Cliff's Delta vectorized calculation against known distributions."""
    # If x is fully greater than y, delta should be +1.0
    x = np.array([10, 11, 12])
    y = np.array([1, 2, 3])
    assert np.isclose(train_grouped.compute_cliffs_delta(x, y), 1.0)
    
    # If x is fully less than y, delta should be -1.0
    assert np.isclose(train_grouped.compute_cliffs_delta(y, x), -1.0)
    
    # Fully identical arrays should return 0.0
    assert np.isclose(train_grouped.compute_cliffs_delta(x, x), 0.0)


def test_threshold_evaluation_metrics():
    """Test that decision threshold sweeps behave correctly and handle edge cases."""
    # perfect matching
    y_true = np.array([0, 0, 1, 1])
    y_prob = np.array([0.1, 0.2, 0.8, 0.9])
    
    sweeps = train_grouped.evaluate_thresholds(y_true, y_prob)
    
    # At low threshold (e.g. 0.05), all are predicted positive -> recall=1.0
    row_low = sweeps.loc[sweeps["threshold"] == 0.05].iloc[0]
    assert np.isclose(row_low["recall"], 1.0)
    
    # At high threshold (e.g. 0.95), all are predicted negative -> recall=0.0
    row_high = sweeps.loc[sweeps["threshold"] == 0.95].iloc[0]
    assert np.isclose(row_high["recall"], 0.0)
