import importlib.util
from pathlib import Path

import numpy as np
import pandas as pd


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "build_wind_farm_c_event_aggregates.py"
SPEC = importlib.util.spec_from_file_location("build_wind_farm_c_event_aggregates", SCRIPT_PATH)
event_aggregates = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(event_aggregates)


def test_circular_stats_handle_zero_boundary():
    angles = pd.Series([350.0, 10.0])

    mean = event_aggregates.circular_mean_degrees(angles)
    variance = event_aggregates.circular_variance(angles)

    assert np.isclose(mean, 0.0) or np.isclose(mean, 360.0)
    assert 0.0 <= variance < 0.02


def test_event_aggregation_scalar_stats_and_shares():
    feature_df = pd.DataFrame(
        {
            "timestamp": pd.date_range("2023-01-01", periods=4, freq="10min"),
            "asset_id": [12, 12, 12, 12],
            "event_id": [101, 101, 101, 101],
            "event_label_care": ["anomaly", "anomaly", "anomaly", "anomaly"],
            "hs": [1.0, 2.0, 3.0, 4.0],
            "tp": [5.0, 6.0, 7.0, 8.0],
            "wave_direction": [350.0, 10.0, 0.0, np.nan],
            "wind_speed_10m": [8.0, 10.0, 12.0, 14.0],
            "wind_direction_10m": [90.0, 90.0, 90.0, 90.0],
            "wind_speed_100m": [12.0, 14.0, 16.0, 18.0],
            "wind_direction_100m": [180.0, 180.0, 180.0, 180.0],
            "current_speed": [0.1, 0.2, 0.3, 0.4],
            "current_direction": [270.0, 270.0, 270.0, 270.0],
            "status_type_id": [3.0, 3.0, 4.0, np.nan],
            "label": ["maintenance_success", "maintenance_success", "standby_weather", "unknown"],
        }
    )

    result = event_aggregates.aggregate_feature_matrix(feature_df)

    assert len(result) == 1
    row = result.iloc[0]
    assert row["event_id"] == 101
    assert row["asset_id"] == 12
    assert row["event_label_care"] == "anomaly"
    assert row["event_label_model"] == "maintenance_success"
    assert row["row_count_10min"] == 4

    assert np.isclose(row["hs_mean"], 2.5)
    assert np.isclose(row["hs_max"], 4.0)
    assert np.isclose(row["hs_min"], 1.0)
    assert np.isclose(row["hs_std"], np.std([1.0, 2.0, 3.0, 4.0], ddof=0))

    assert np.isclose(row["share_status_3"], 0.5)
    assert np.isclose(row["share_status_4"], 0.25)
    assert np.isclose(row["share_status_nan"], 0.25)
    status_share_cols = [c for c in result.columns if c.startswith("share_status_")]
    assert np.isclose(row[status_share_cols].sum(), 1.0)

    assert np.isclose(row["share_label_maintenance_success"], 0.5)
    assert np.isclose(row["share_label_standby_weather"], 0.25)
    assert np.isclose(row["share_label_unknown"], 0.25)
    label_share_cols = [c for c in result.columns if c.startswith("share_label_")]
    assert np.isclose(row[label_share_cols].sum(), 1.0)


def test_event_aggregation_preserves_one_row_per_event_asset_pair():
    feature_df = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                [
                    "2023-01-01 00:00",
                    "2023-01-01 00:10",
                    "2023-01-02 00:00",
                    "2023-01-02 00:10",
                ]
            ),
            "asset_id": [1, 1, 2, 2],
            "event_id": [10, 10, 20, 20],
            "event_label_care": ["normal", "normal", "anomaly", "anomaly"],
            "status_type_id": [0, 3, 4, 4],
            "label": ["unknown", "maintenance_success", "standby_weather", "standby_weather"],
        }
    )

    result = event_aggregates.aggregate_feature_matrix(feature_df)

    assert len(result) == 2
    assert result["event_id"].tolist() == [10, 20]
    assert result["asset_id"].tolist() == [1, 2]
