from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from om_pipeline.metocean.metocean_fusion_v2 import (
    FUSION_V2_COLUMNS,
    build_metocean_fusion_v2,
)


def _write_inputs(tmp_path: Path, duplicate_wave: bool = False) -> dict[str, Path]:
    dwell = tmp_path / "dwell.parquet"
    pd.DataFrame(
        [
            {
                "dwell_id": "d1",
                "visit_id": "v1",
                "wind_farm": "Farm A",
                "farm_id": "Farm_A",
                "dwell_tier": "Tier A",
                "start_utc": pd.Timestamp("2024-01-01T00:00:00Z"),
                "end_utc": pd.Timestamp("2024-01-01T01:00:00Z"),
                "duration_min": 60.0,
                "centroid_lat": 54.0,
                "centroid_lon": 7.0,
            },
            {
                "dwell_id": "d2",
                "visit_id": "v2",
                "wind_farm": "Farm A",
                "farm_id": "Farm_A",
                "dwell_tier": "Tier B",
                "start_utc": pd.Timestamp("2024-01-02T00:00:00Z"),
                "end_utc": pd.Timestamp("2024-01-02T01:00:00Z"),
                "duration_min": 60.0,
                "centroid_lat": 54.01,
                "centroid_lon": 7.01,
            },
            {
                "dwell_id": "d3",
                "visit_id": "v3",
                "wind_farm": "Farm A",
                "farm_id": "Farm_A",
                "dwell_tier": "Tier C",
                "start_utc": pd.Timestamp("2024-01-03T00:00:00Z"),
                "end_utc": pd.Timestamp("2024-01-03T01:00:00Z"),
                "duration_min": 60.0,
                "centroid_lat": 54.02,
                "centroid_lon": 7.02,
            },
        ]
    ).to_parquet(dwell, index=False)

    wave_rows = [
        {
            "dwell_id": "d1",
            "visit_id": "v1",
            "selected_wave_source": "nws",
            "selected_hs_mean": 1.0,
            "selected_tp_mean": 5.0,
            "selected_wave_direction_sin_mean": 1.0,
            "selected_wave_direction_cos_mean": 0.0,
            "wave_confidence_score": 1.0,
            "wave_confidence_class": "A_high",
            "source_disagreement_hs_range": 0.1,
            "source_disagreement_tp_range": 0.4,
            "selection_reason": "agreeing sources",
        },
        {
            "dwell_id": "d2",
            "visit_id": "v2",
            "selected_wave_source": "nws",
            "selected_hs_mean": 0.8,
            "selected_tp_mean": 4.5,
            "selected_wave_direction_sin_mean": 0.0,
            "selected_wave_direction_cos_mean": 1.0,
            "wave_confidence_score": 0.75,
            "wave_confidence_class": "B_medium",
            "source_disagreement_hs_range": 0.2,
            "source_disagreement_tp_range": 0.8,
            "selection_reason": "single good source",
        },
        {
            "dwell_id": "d3",
            "visit_id": "v3",
            "selected_wave_source": "missing",
            "selected_hs_mean": np.nan,
            "selected_tp_mean": np.nan,
            "selected_wave_direction_sin_mean": np.nan,
            "selected_wave_direction_cos_mean": np.nan,
            "wave_confidence_score": 0.0,
            "wave_confidence_class": "D_unsuitable",
            "source_disagreement_hs_range": np.nan,
            "source_disagreement_tp_range": np.nan,
            "selection_reason": "missing",
        },
    ]
    if duplicate_wave:
        wave_rows.append(dict(wave_rows[0]))
    wave = tmp_path / "wave.parquet"
    pd.DataFrame(wave_rows).to_parquet(wave, index=False)

    wind_dir = 90.0
    wind = tmp_path / "wind_event_confidence.parquet"
    pd.DataFrame(
        [
            {
                "dwell_id": "d1",
                "visit_id": "v1",
                "has_wind_speed": True,
                "has_wind_direction": True,
                "wind_confidence_class": "A_speed_direction",
                "wind_confidence_score": 1.0,
                "wind_source": "NORA3",
                "wind_speed_mean": 8.0,
                "wind_speed_p95": 9.0,
                "wind_direction_sin_mean": np.sin(np.deg2rad(wind_dir)),
                "wind_direction_cos_mean": np.cos(np.deg2rad(wind_dir)),
                "wind_direction_deg_mean": wind_dir,
                "wind_height_m": 10.0,
                "wind_missing_reason": None,
            },
            {
                "dwell_id": "d2",
                "visit_id": "v2",
                "has_wind_speed": True,
                "has_wind_direction": False,
                "wind_confidence_class": "B_speed_only",
                "wind_confidence_score": 0.75,
                "wind_source": "NORA3",
                "wind_speed_mean": 6.0,
                "wind_speed_p95": 7.0,
                "wind_direction_sin_mean": np.nan,
                "wind_direction_cos_mean": np.nan,
                "wind_direction_deg_mean": np.nan,
                "wind_height_m": 10.0,
                "wind_missing_reason": "wind_direction_missing_in_existing_active_fields",
            },
            {
                "dwell_id": "d3",
                "visit_id": "v3",
                "has_wind_speed": False,
                "has_wind_direction": False,
                "wind_confidence_class": "D_unsuitable",
                "wind_confidence_score": 0.0,
                "wind_source": "NORA3",
                "wind_speed_mean": np.nan,
                "wind_speed_p95": np.nan,
                "wind_direction_sin_mean": np.nan,
                "wind_direction_cos_mean": np.nan,
                "wind_direction_deg_mean": np.nan,
                "wind_height_m": 10.0,
                "wind_missing_reason": "no_active_nora3_wind_records",
            },
        ]
    ).to_parquet(wind, index=False)
    pd.DataFrame(
        [
            {
                "dwell_id": "d1",
                "visit_id": "v1",
                "wind_direction_convention": "meteorological_from_degrees_clockwise_from_true_north",
            },
            {
                "dwell_id": "d2",
                "visit_id": "v2",
                "wind_direction_convention": "meteorological_from_degrees_clockwise_from_true_north",
            },
            {
                "dwell_id": "d3",
                "visit_id": "v3",
                "wind_direction_convention": "meteorological_from_degrees_clockwise_from_true_north",
            },
        ]
    ).to_parquet(wind.with_name("wind_event_candidates.parquet"), index=False)

    current = tmp_path / "current_event_confidence.parquet"
    pd.DataFrame(
        [
            {
                "dwell_id": "d1",
                "visit_id": "v1",
                "has_event_scale_current": True,
                "current_confidence_class": "A_event_scale",
                "current_confidence_score": 1.0,
                "current_source": "NWS",
                "current_u_mean": 0.3,
                "current_v_mean": 0.4,
                "current_speed_mean": 0.5,
                "current_direction_to_sin_mean": 0.6,
                "current_direction_to_cos_mean": 0.8,
                "current_depth_m": 0.0,
                "event_window_sample_count": 2,
                "nearest_time_gap_minutes": 10.0,
                "current_missing_reason": None,
            },
            {
                "dwell_id": "d2",
                "visit_id": "v2",
                "has_event_scale_current": False,
                "current_confidence_class": "D_unsuitable",
                "current_confidence_score": 0.0,
                "current_source": "NWS",
                "current_u_mean": np.nan,
                "current_v_mean": np.nan,
                "current_speed_mean": np.nan,
                "current_direction_to_sin_mean": np.nan,
                "current_direction_to_cos_mean": np.nan,
                "current_depth_m": np.nan,
                "event_window_sample_count": 0,
                "nearest_time_gap_minutes": np.nan,
                "current_missing_reason": "missing_nws_current_partition",
            },
            {
                "dwell_id": "d3",
                "visit_id": "v3",
                "has_event_scale_current": False,
                "current_confidence_class": "D_unsuitable",
                "current_confidence_score": 0.0,
                "current_source": "NWS",
                "current_u_mean": 0.1,
                "current_v_mean": 0.2,
                "current_speed_mean": 0.2236,
                "current_direction_to_sin_mean": 0.4,
                "current_direction_to_cos_mean": 0.9,
                "current_depth_m": 0.0,
                "event_window_sample_count": 2,
                "nearest_time_gap_minutes": 10.0,
                "current_missing_reason": "fallback_or_invalid",
            },
        ]
    ).to_parquet(current, index=False)
    pd.DataFrame(
        [
            {
                "dwell_id": "d1",
                "visit_id": "v1",
                "current_speed_p95": 0.7,
                "current_direction_to_deg_mean": np.degrees(np.arctan2(0.3, 0.4)) % 360,
                "current_direction_convention": "flow_to_degrees_clockwise_from_true_north",
            },
            {
                "dwell_id": "d2",
                "visit_id": "v2",
                "current_speed_p95": np.nan,
                "current_direction_to_deg_mean": np.nan,
                "current_direction_convention": "flow_to_degrees_clockwise_from_true_north",
            },
            {
                "dwell_id": "d3",
                "visit_id": "v3",
                "current_speed_p95": 0.4,
                "current_direction_to_deg_mean": 20.0,
                "current_direction_convention": "flow_to_degrees_clockwise_from_true_north",
            },
        ]
    ).to_parquet(current.with_name("current_event_candidates.parquet"), index=False)

    bathy = tmp_path / "bathymetry.parquet"
    pd.DataFrame(
        [
            {
                "wind_farm": "Farm A",
                "sample_point_id": "farm_centroid",
                "sample_point_type": "farm_centroid",
                "lat": 54.0,
                "lon": 7.0,
                "water_depth_m": 25.0,
                "bathymetry_source": "emodnet",
                "bathymetry_distance_m": 5.0,
                "bathymetry_spatial_match_status": "ok",
            }
        ]
    ).to_parquet(bathy, index=False)

    return {
        "dwell": dwell,
        "wave": wave,
        "wind": wind,
        "current": current,
        "bathymetry": bathy,
        "output": tmp_path / "out",
        "report": tmp_path / "reports",
    }


def _build(paths: dict[str, Path], overwrite: bool = True):
    return build_metocean_fusion_v2(
        dwell_weather=paths["dwell"],
        wave_confidence_path=paths["wave"],
        wind_confidence_path=paths["wind"],
        current_confidence_path=paths["current"],
        bathymetry_path=paths["bathymetry"],
        output_dir=paths["output"],
        report_dir=paths["report"],
        overwrite=overwrite,
    )


def test_row_preservation_confidences_and_model_ready_flags(tmp_path: Path) -> None:
    paths = _write_inputs(tmp_path)

    result = _build(paths)
    output = result.output

    assert output["dwell_id"].tolist() == ["d1", "d2", "d3"]
    assert output.columns.tolist() == FUSION_V2_COLUMNS
    assert output.loc[0, "wave_confidence_class"] == "A_high"
    assert output.loc[0, "wind_confidence_class"] == "A_speed_direction"
    assert output.loc[0, "current_confidence_class"] == "A_event_scale"
    assert bool(output.loc[0, "model_ready_wave_wind_current"])
    assert bool(output.loc[0, "model_ready_high_confidence"])
    assert bool(output.loc[1, "model_ready_wave_wind"])
    assert not bool(output.loc[1, "model_ready_wave_current"])


def test_missing_current_is_not_converted_to_zero(tmp_path: Path) -> None:
    paths = _write_inputs(tmp_path)

    output = _build(paths).output
    missing_current = output[output["dwell_id"].eq("d2")].iloc[0]
    invalid_current = output[output["dwell_id"].eq("d3")].iloc[0]

    assert pd.isna(missing_current["current_speed_mean"])
    assert pd.isna(missing_current["current_u_mean"])
    assert pd.isna(invalid_current["current_speed_mean"])
    assert not bool(invalid_current["has_current"])


def test_missing_wind_direction_is_not_converted_to_zero_and_speed_is_ready(tmp_path: Path) -> None:
    paths = _write_inputs(tmp_path)

    output = _build(paths).output
    speed_only = output[output["dwell_id"].eq("d2")].iloc[0]

    assert bool(speed_only["has_wind_speed"])
    assert not bool(speed_only["has_wind_direction"])
    assert pd.isna(speed_only["wind_direction_deg_mean"])
    assert pd.isna(speed_only["wind_direction_sin_mean"])
    assert speed_only["wind_speed_mean"] == 6.0


def test_duplicate_layer_rows_fail_one_to_one_join(tmp_path: Path) -> None:
    paths = _write_inputs(tmp_path, duplicate_wave=True)

    with pytest.raises(ValueError, match="duplicate dwell_id"):
        _build(paths)


def test_no_overwrite_default(tmp_path: Path) -> None:
    paths = _write_inputs(tmp_path)
    _build(paths, overwrite=True)

    with pytest.raises(FileExistsError):
        _build(paths, overwrite=False)
