from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from om_pipeline.metocean.current_confidence_v1 import (
    CURRENT_EVENT_CANDIDATE_COLUMNS,
    CURRENT_EVENT_CONFIDENCE_COLUMNS,
    build_current_confidence_v1,
    classify_event_current_confidence,
)


def _write_partition(path: Path, provenance_status: str = "real_uo_vo") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    times = pd.date_range("2024-01-01T00:00:00Z", periods=4, freq="1h")
    rows = []
    for timestamp in times:
        rows.append(
            {
                "timestamp_utc": timestamp,
                "wind_farm": "Good Farm",
                "farm_id": "Good_Farm",
                "year": 2024,
                "sample_point_id": "farm_centroid",
                "sample_point_type": "farm_centroid",
                "lat": 54.0,
                "lon": 7.0,
                "current_grid_lat": 54.0,
                "current_grid_lon": 7.0,
                "current_spatial_distance_km": 0.1,
                "current_u": 0.3,
                "current_v": 0.4,
                "current_speed": 0.5,
                "current_direction_to_deg": np.degrees(np.arctan2(0.3, 0.4)) % 360,
                "current_direction_convention": "flow_to_degrees_clockwise_from_true_north",
                "current_depth_m": 0.0,
                "current_depth_selection_rule": "surface_2d_no_depth_dimension",
                "current_source": "NWS",
                "current_product_id": "prod",
                "current_dataset_id": "dataset",
                "current_native_temporal_resolution_minutes": 60,
                "current_native_spatial_resolution_km": 7.0,
                "current_assignment_method": "nearest_1d_grid",
                "current_spatial_match_status": "ok",
                "source_file": "raw.nc",
                "provenance_status": provenance_status,
                "emodnet_water_depth_m": 20.0,
                "depth_warning_le_1m": False,
                "depth_warning_le_5m": False,
                "depth_warning_le_10m": False,
                "current_model_bathymetry_warning": "none",
            }
        )
    pd.DataFrame(rows).to_parquet(path, index=False)


def _write_inputs(tmp_path: Path) -> dict[str, Path]:
    current_root = tmp_path / "nws_current_timeseries"
    partition = current_root / "wind_farm=Good_Farm" / "year=2024" / "part.parquet"
    _write_partition(partition)

    manifest = current_root / "manifest.csv"
    pd.DataFrame(
        [
            {
                "wind_farm": "Good Farm",
                "farm_id": "Good_Farm",
                "year": 2024,
                "status": "validated",
                "row_count": 4,
                "sample_point_count": 1,
                "timestamp_start": "2024-01-01 00:00:00+00:00",
                "timestamp_end": "2024-01-01 03:00:00+00:00",
                "source_file": "raw.nc",
                "processed_path": str(partition),
                "qa_status": "passed",
                "message": "ok",
            }
        ]
    ).to_csv(manifest, index=False)

    dwell = tmp_path / "dwell.parquet"
    pd.DataFrame(
        [
            {
                "dwell_id": "d1",
                "visit_id": "v1",
                "wind_farm": "Good Farm",
                "farm_id": "Good_Farm",
                "dwell_tier": "Tier A",
                "start_utc": pd.Timestamp("2024-01-01T00:15:00Z"),
                "end_utc": pd.Timestamp("2024-01-01T00:45:00Z"),
                "centroid_lat": 54.0,
                "centroid_lon": 7.0,
            },
            {
                "dwell_id": "d2",
                "visit_id": "v2",
                "wind_farm": "Missing Farm",
                "farm_id": "Missing_Farm",
                "dwell_tier": "Tier B",
                "start_utc": pd.Timestamp("2024-01-01T00:30:00Z"),
                "end_utc": pd.Timestamp("2024-01-01T01:30:00Z"),
                "centroid_lat": 55.0,
                "centroid_lon": 8.0,
            },
        ]
    ).to_parquet(dwell, index=False)

    wave = tmp_path / "wave.parquet"
    pd.DataFrame(
        [
            {"dwell_id": "d1", "visit_id": "v1", "wave_confidence_class": "A_high"},
            {"dwell_id": "d2", "visit_id": "v2", "wave_confidence_class": "C_low"},
        ]
    ).to_parquet(wave, index=False)

    bathy = tmp_path / "bathymetry.parquet"
    pd.DataFrame(
        [
            {
                "wind_farm": "Good Farm",
                "sample_point_id": "farm_centroid",
                "sample_point_type": "farm_centroid",
                "lat": 54.0,
                "lon": 7.0,
                "water_depth_m": 20.0,
            },
            {
                "wind_farm": "Missing Farm",
                "sample_point_id": "farm_centroid",
                "sample_point_type": "farm_centroid",
                "lat": 55.0,
                "lon": 8.0,
                "water_depth_m": 6.0,
            },
        ]
    ).to_parquet(bathy, index=False)
    return {
        "current_root": current_root,
        "manifest": manifest,
        "dwell": dwell,
        "wave": wave,
        "bathymetry": bathy,
        "output": tmp_path / "out",
        "report": tmp_path / "reports",
    }


def test_build_preserves_dwell_identity_and_missing_partition_rows(tmp_path: Path) -> None:
    paths = _write_inputs(tmp_path)

    result = build_current_confidence_v1(
        dwell_weather=paths["dwell"],
        nws_current_root=paths["current_root"],
        nws_current_manifest=paths["manifest"],
        wave_confidence_path=paths["wave"],
        bathymetry_path=paths["bathymetry"],
        output_dir=paths["output"],
        report_dir=paths["report"],
        overwrite=True,
    )

    assert result.candidates["dwell_id"].tolist() == ["d1", "d2"]
    assert result.confidence["dwell_id"].tolist() == ["d1", "d2"]
    assert result.confidence.loc[0, "current_confidence_class"] == "A_event_scale"
    assert result.confidence.loc[1, "current_confidence_class"] == "D_unsuitable"
    assert result.candidates.loc[1, "current_missing_reason"] == "missing_nws_current_partition"


def test_event_window_bracketing_aggregates_speed_and_direction(tmp_path: Path) -> None:
    paths = _write_inputs(tmp_path)

    result = build_current_confidence_v1(
        dwell_weather=paths["dwell"],
        nws_current_root=paths["current_root"],
        nws_current_manifest=paths["manifest"],
        wave_confidence_path=paths["wave"],
        bathymetry_path=paths["bathymetry"],
        output_dir=paths["output"],
        report_dir=paths["report"],
        overwrite=True,
    )
    candidate = result.candidates.iloc[0]

    assert candidate["temporal_assignment_method"] == "bracketing_pair_mean"
    assert candidate["event_window_sample_count"] == 0
    assert np.isclose(candidate["current_speed_mean"], 0.5)
    assert np.isclose(candidate["current_direction_to_deg_mean"], np.degrees(np.arctan2(0.3, 0.4)) % 360)


def test_fallback_or_legacy_provenance_cannot_be_a_or_b() -> None:
    row = pd.Series(
        {
            "source": "NWS",
            "current_u_mean": 0.2,
            "current_v_mean": 0.1,
            "current_speed_mean": np.sqrt(0.05),
            "provenance_status": "legacy cmems csv",
            "source_sample_distance_km": 0.1,
            "event_bracketed_by_source_times": True,
            "event_has_in_window_samples": True,
            "nearest_time_gap_minutes": 0.0,
            "current_missing_fraction": 0.0,
        }
    )

    cls, _, has_event_scale, _ = classify_event_current_confidence(row)

    assert cls == "D_unsuitable"
    assert not has_event_scale


def test_output_schema_is_stable(tmp_path: Path) -> None:
    paths = _write_inputs(tmp_path)

    result = build_current_confidence_v1(
        dwell_weather=paths["dwell"],
        nws_current_root=paths["current_root"],
        nws_current_manifest=paths["manifest"],
        wave_confidence_path=paths["wave"],
        bathymetry_path=paths["bathymetry"],
        output_dir=paths["output"],
        report_dir=paths["report"],
        overwrite=True,
    )

    assert result.candidates.columns.tolist() == CURRENT_EVENT_CANDIDATE_COLUMNS
    assert result.confidence.columns.tolist() == CURRENT_EVENT_CONFIDENCE_COLUMNS


def test_no_overwrite_default(tmp_path: Path) -> None:
    paths = _write_inputs(tmp_path)
    kwargs = dict(
        dwell_weather=paths["dwell"],
        nws_current_root=paths["current_root"],
        nws_current_manifest=paths["manifest"],
        wave_confidence_path=paths["wave"],
        bathymetry_path=paths["bathymetry"],
        output_dir=paths["output"],
        report_dir=paths["report"],
    )
    build_current_confidence_v1(**kwargs, overwrite=True)

    with pytest.raises(FileExistsError):
        build_current_confidence_v1(**kwargs, overwrite=False)
