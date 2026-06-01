from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from om_pipeline.metocean.wind_confidence_v1 import (
    WIND_DIRECTION_CONVENTION,
    WIND_EVENT_CANDIDATE_COLUMNS,
    WIND_EVENT_CONFIDENCE_COLUMNS,
    build_wind_confidence_v1,
    classify_wind_confidence,
)


def _write_inputs(tmp_path: Path) -> dict[str, Path]:
    dwell = tmp_path / "dwell.parquet"
    direction_deg = 90.0
    pd.DataFrame(
        [
            {
                "dwell_id": "d1",
                "visit_id": "v1",
                "wind_farm": "Good Farm",
                "farm_id": "Good_Farm",
                "dwell_tier": "Tier A",
                "start_utc": pd.Timestamp("2024-01-01T00:15:00Z"),
                "end_utc": pd.Timestamp("2024-01-01T01:15:00Z"),
                "active_wind_speed_mean": 8.0,
                "active_wind_speed_max": 9.5,
                "active_wind_direction_sin_mean": np.sin(np.deg2rad(direction_deg)),
                "active_wind_direction_cos_mean": np.cos(np.deg2rad(direction_deg)),
                "active_n_weather_records": 2,
                "active_weather_missing_fraction": 0.0,
                "active_source_available": True,
            },
            {
                "dwell_id": "d2",
                "visit_id": "v2",
                "wind_farm": "Speed Only Farm",
                "farm_id": "Speed_Only_Farm",
                "dwell_tier": "Tier B",
                "start_utc": pd.Timestamp("2024-01-02T00:15:00Z"),
                "end_utc": pd.Timestamp("2024-01-02T01:15:00Z"),
                "active_wind_speed_mean": 6.0,
                "active_wind_speed_max": 7.0,
                "active_wind_direction_sin_mean": np.nan,
                "active_wind_direction_cos_mean": np.nan,
                "active_n_weather_records": 2,
                "active_weather_missing_fraction": 0.0,
                "active_source_available": True,
            },
            {
                "dwell_id": "d3",
                "visit_id": "v3",
                "wind_farm": "Missing Farm",
                "farm_id": "Missing_Farm",
                "dwell_tier": "Tier C",
                "start_utc": pd.Timestamp("2024-01-03T00:15:00Z"),
                "end_utc": pd.Timestamp("2024-01-03T01:15:00Z"),
                "active_wind_speed_mean": np.nan,
                "active_wind_speed_max": np.nan,
                "active_wind_direction_sin_mean": np.nan,
                "active_wind_direction_cos_mean": np.nan,
                "active_n_weather_records": 0,
                "active_weather_missing_fraction": 1.0,
                "active_source_available": False,
            },
        ]
    ).to_parquet(dwell, index=False)

    wave = tmp_path / "wave.parquet"
    pd.DataFrame(
        [
            {"dwell_id": "d1", "visit_id": "v1", "wave_confidence_class": "A_high"},
            {"dwell_id": "d2", "visit_id": "v2", "wave_confidence_class": "B_medium"},
            {"dwell_id": "d3", "visit_id": "v3", "wave_confidence_class": "C_low"},
        ]
    ).to_parquet(wave, index=False)

    current = tmp_path / "current.parquet"
    pd.DataFrame(
        [
            {"dwell_id": "d1", "visit_id": "v1", "current_confidence_class": "A_event_scale"},
            {"dwell_id": "d2", "visit_id": "v2", "current_confidence_class": "D_unsuitable"},
            {"dwell_id": "d3", "visit_id": "v3", "current_confidence_class": "D_unsuitable"},
        ]
    ).to_parquet(current, index=False)

    joined = tmp_path / "nora3_joined_cache"
    (joined / "batch_id=000001").mkdir(parents=True)
    pd.DataFrame(
        {
            "time": pd.date_range("2024-01-01", periods=2, freq="1h"),
            "wind_speed_10m": [7.5, 8.0],
            "wind_direction_10m": [90.0, 95.0],
        }
    ).to_parquet(joined / "batch_id=000001" / "data.parquet", index=False)
    pd.DataFrame(
        [{"pair_id": "p1", "batch_id": 1, "status": "success", "year": 2024, "month": 1}]
    ).to_csv(joined / "manifest.csv", index=False)

    raw = tmp_path / "raw_nora3"
    raw.mkdir()
    pd.DataFrame({"time": ["2024-01-01T00:00:00"], "wind_speed_10m": [8.0]}).to_csv(
        raw / "nora3_wind_raw_54.00_7.00_2024_01.csv",
        index=False,
    )
    pd.DataFrame(
        {
            "time": ["2024-01-01T00:00:00"],
            "wind_speed_10m": [8.0],
            "wind_direction_10m": [90.0],
        }
    ).to_csv(raw / "nora3_wind_raw_54.00_7.00_2024_02.csv", index=False)

    return {
        "dwell": dwell,
        "wave": wave,
        "current": current,
        "joined": joined,
        "raw": raw,
        "output": tmp_path / "out",
        "report": tmp_path / "reports",
    }


def test_build_preserves_row_identity_and_classifies_wind_layers(tmp_path: Path) -> None:
    paths = _write_inputs(tmp_path)

    result = build_wind_confidence_v1(
        dwell_weather=paths["dwell"],
        nora3_joined_cache=paths["joined"],
        nora3_raw_cache=paths["raw"],
        wave_confidence_path=paths["wave"],
        current_confidence_path=paths["current"],
        output_dir=paths["output"],
        report_dir=paths["report"],
        overwrite=True,
    )

    assert result.candidates["dwell_id"].tolist() == ["d1", "d2", "d3"]
    assert result.confidence["dwell_id"].tolist() == ["d1", "d2", "d3"]
    assert result.confidence["wind_confidence_class"].tolist() == [
        "A_speed_direction",
        "B_speed_only",
        "D_unsuitable",
    ]


def test_missing_direction_is_not_converted_to_zero(tmp_path: Path) -> None:
    paths = _write_inputs(tmp_path)

    result = build_wind_confidence_v1(
        dwell_weather=paths["dwell"],
        nora3_joined_cache=paths["joined"],
        nora3_raw_cache=paths["raw"],
        wave_confidence_path=paths["wave"],
        current_confidence_path=paths["current"],
        output_dir=paths["output"],
        report_dir=paths["report"],
        overwrite=True,
    )
    speed_only = result.candidates[result.candidates["dwell_id"].eq("d2")].iloc[0]

    assert pd.isna(speed_only["wind_direction_deg_mean"])
    assert speed_only["wind_missing_reason"] == "wind_direction_missing_in_existing_active_fields"


def test_direction_convention_and_sin_cos_validity(tmp_path: Path) -> None:
    paths = _write_inputs(tmp_path)

    result = build_wind_confidence_v1(
        dwell_weather=paths["dwell"],
        nora3_joined_cache=paths["joined"],
        nora3_raw_cache=paths["raw"],
        wave_confidence_path=paths["wave"],
        current_confidence_path=paths["current"],
        output_dir=paths["output"],
        report_dir=paths["report"],
        overwrite=True,
    )
    candidate = result.candidates.iloc[0]
    norm = np.sqrt(candidate["wind_direction_sin_mean"] ** 2 + candidate["wind_direction_cos_mean"] ** 2)

    assert candidate["wind_direction_convention"] == WIND_DIRECTION_CONVENTION
    assert np.isclose(candidate["wind_direction_deg_mean"], 90.0)
    assert np.isclose(norm, 1.0)


def test_fallback_or_legacy_wind_provenance_cannot_be_a_or_b() -> None:
    cls, score, has_speed, has_direction, _ = classify_wind_confidence(
        pd.Series(
            {
                "wind_source": "NORA3",
                "wind_speed_mean": 8.0,
                "wind_direction_sin_mean": 1.0,
                "wind_direction_cos_mean": 0.0,
                "provenance_status": "legacy fallback csv",
            }
        )
    )

    assert cls == "D_unsuitable"
    assert score == 0.0
    assert not has_speed
    assert not has_direction


def test_output_schema_is_stable(tmp_path: Path) -> None:
    paths = _write_inputs(tmp_path)

    result = build_wind_confidence_v1(
        dwell_weather=paths["dwell"],
        nora3_joined_cache=paths["joined"],
        nora3_raw_cache=paths["raw"],
        wave_confidence_path=paths["wave"],
        current_confidence_path=paths["current"],
        output_dir=paths["output"],
        report_dir=paths["report"],
        overwrite=True,
    )

    assert result.candidates.columns.tolist() == WIND_EVENT_CANDIDATE_COLUMNS
    assert result.confidence.columns.tolist() == WIND_EVENT_CONFIDENCE_COLUMNS


def test_no_overwrite_default(tmp_path: Path) -> None:
    paths = _write_inputs(tmp_path)
    kwargs = dict(
        dwell_weather=paths["dwell"],
        nora3_joined_cache=paths["joined"],
        nora3_raw_cache=paths["raw"],
        wave_confidence_path=paths["wave"],
        current_confidence_path=paths["current"],
        output_dir=paths["output"],
        report_dir=paths["report"],
    )
    build_wind_confidence_v1(**kwargs, overwrite=True)

    with pytest.raises(FileExistsError):
        build_wind_confidence_v1(**kwargs, overwrite=False)
