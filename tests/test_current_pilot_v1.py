from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from om_pipeline.metocean.current_pilot_v1 import (
    BALTIC_PRODUCT,
    CURRENT_CANDIDATE_COLUMNS,
    classify_current_confidence,
    derive_current_speed_direction,
    ensure_candidate_schema,
    extract_candidates_from_xarray,
    blocked_candidate_rows,
    validate_real_current_candidates,
    write_candidate_table,
)


def _sample_points() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "wind_farm": ["Example Farm", "Example Farm"],
            "sample_point_id": ["farm_centroid", "turbine_001"],
            "sample_point_type": ["farm_centroid", "turbine"],
            "lat": [54.0, 54.1],
            "lon": [14.0, 14.1],
        }
    )


def _current_dataset() -> xr.Dataset:
    times = pd.date_range("2020-01-01", periods=3, freq="1D", tz="UTC")
    lat = np.array([54.0, 54.1])
    lon = np.array([14.0, 14.1])
    uo = np.ones((3, 2, 2)) * 0.3
    vo = np.ones((3, 2, 2)) * 0.4
    return xr.Dataset(
        data_vars={
            "uo": (("time", "latitude", "longitude"), uo),
            "vo": (("time", "latitude", "longitude"), vo),
        },
        coords={"time": times, "latitude": lat, "longitude": lon},
    )


def test_speed_from_uv_and_direction_convention() -> None:
    speed, direction = derive_current_speed_direction(
        pd.Series([1.0, 0.0, -1.0]),
        pd.Series([0.0, 1.0, 0.0]),
    )

    assert np.allclose(speed, [1.0, 1.0, 1.0])
    assert np.allclose(direction, [90.0, 0.0, 270.0])


def test_extract_candidates_preserves_schema_and_rejects_no_synthetic_values() -> None:
    candidates = extract_candidates_from_xarray(
        ds=_current_dataset(),
        sample_points=_sample_points(),
        config=BALTIC_PRODUCT,
        farm="Example Farm",
        year=2020,
        source_file="real_source.nc",
    )

    assert list(candidates.columns) == CURRENT_CANDIDATE_COLUMNS
    assert len(candidates) == 6
    assert candidates["sample_point_id"].nunique() == 2
    assert candidates["provenance_status"].eq("real_uo_vo").all()
    assert np.allclose(candidates["current_speed"], 0.5)
    assert np.allclose(candidates["current_direction"], np.degrees(np.arctan2(0.3, 0.4)) % 360)


def test_no_synthetic_or_fallback_current_acceptance() -> None:
    candidates = ensure_candidate_schema(
        pd.DataFrame(
            {
                "provenance_status": ["simulated tidal fallback"],
                "current_u": [0.1],
                "current_v": [0.2],
            }
        )
    )

    with pytest.raises(ValueError, match="fallback/simulated"):
        validate_real_current_candidates(candidates)


def test_blocked_pilot_when_source_unavailable() -> None:
    candidates = blocked_candidate_rows(
        sample_points=_sample_points(),
        config=BALTIC_PRODUCT,
        farm="Example Farm",
        year=2020,
        reason="credentials unavailable",
    )

    assert list(candidates.columns) == CURRENT_CANDIDATE_COLUMNS
    assert len(candidates) == 2
    assert candidates["current_u"].isna().all()
    assert candidates["provenance_status"].str.startswith("blocked:").all()
    confidence, reason = classify_current_confidence(candidates)
    assert confidence == "D_unsuitable"
    assert "No true u/v" in reason


def test_confidence_class_logic() -> None:
    base = ensure_candidate_schema(
        pd.DataFrame(
            {
                "timestamp_utc": pd.date_range("2020-01-01", periods=3, freq="1h", tz="UTC"),
                "current_u": [0.1, 0.2, 0.3],
                "current_v": [0.2, 0.2, 0.2],
                "current_speed": [0.2236068, 0.2828427, 0.3605551],
                "provenance_status": ["real_uo_vo"] * 3,
                "current_native_temporal_resolution_minutes": [60] * 3,
                "current_spatial_distance_km": [2.0, 2.0, 2.0],
            }
        )
    )

    high, _ = classify_current_confidence(
        base,
        {"dwell_event_count": 10, "event_scale_suitable_pct": 0.9, "events_with_bracketing_current_samples": 9},
    )
    contextual, _ = classify_current_confidence(
        base.assign(current_native_temporal_resolution_minutes=1440),
        {"dwell_event_count": 10, "event_scale_suitable_pct": 0.0, "events_with_bracketing_current_samples": 0},
    )
    low, _ = classify_current_confidence(
        base.assign(current_spatial_distance_km=40.0),
        {"dwell_event_count": 10, "event_scale_suitable_pct": 0.0, "events_with_bracketing_current_samples": 0},
    )

    assert high == "A_event_scale"
    assert contextual == "B_contextual"
    assert low == "C_low_confidence"


def test_no_overwrite_default(tmp_path: Path) -> None:
    output = tmp_path / "current.parquet"
    candidates = blocked_candidate_rows(_sample_points(), BALTIC_PRODUCT, "Example Farm", 2020, "blocked")
    write_candidate_table(candidates, output, overwrite=False)

    with pytest.raises(FileExistsError):
        write_candidate_table(candidates, output, overwrite=False)

    write_candidate_table(candidates, output, overwrite=True)
    assert output.exists()
