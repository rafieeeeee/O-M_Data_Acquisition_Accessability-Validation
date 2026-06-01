from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from om_pipeline.metocean.nws_current_batch import (
    CURRENT_DIRECTION_CONVENTION,
    NWS_CURRENT_BATCH_COLUMNS,
    ensure_batch_schema,
    extract_archive_rows_from_raw,
    partition_path,
    load_recommended_farm_years,
    raw_cache_path,
    run_nws_current_batch,
    validate_archive_rows,
)


def _write_raw_dataset(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    times = pd.date_range("2024-01-01", periods=4, freq="1h")
    lat = np.array([54.0, 54.1])
    lon = np.array([7.0, 7.1])
    uo = np.ones((4, 2, 2)) * 0.3
    vo = np.ones((4, 2, 2)) * 0.4
    ds = xr.Dataset(
        data_vars={
            "uo": (("time", "latitude", "longitude"), uo),
            "vo": (("time", "latitude", "longitude"), vo),
        },
        coords={"time": times, "latitude": lat, "longitude": lon},
    )
    ds.to_netcdf(path)


def _sample_points() -> pd.DataFrame:
    return pd.DataFrame(
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
                "wind_farm": "Good Farm",
                "sample_point_id": "turbine_0001",
                "sample_point_type": "turbine",
                "lat": 54.1,
                "lon": 7.1,
                "water_depth_m": 8.0,
            },
        ]
    )


def _write_batch_inputs(tmp_path: Path) -> tuple[Path, Path, Path, Path]:
    eligibility = tmp_path / "eligibility.parquet"
    pd.DataFrame(
        [
            {
                "wind_farm": "Good Farm",
                "farm_id": "Good_Farm",
                "year": 2024,
                "recommended_for_scale": "yes",
                "dwell_count": 2,
                "tier_a_dwell_count": 1,
                "sample_point_count": 2,
                "estimated_current_rows": 8,
                "estimated_processed_size_mb": 0.001,
            },
            {
                "wind_farm": "Stress Farm",
                "farm_id": "Stress_Farm",
                "year": 2024,
                "recommended_for_scale": "stress_test_only",
                "dwell_count": 10,
                "tier_a_dwell_count": 5,
                "sample_point_count": 2,
                "estimated_current_rows": 8,
                "estimated_processed_size_mb": 0.001,
            },
            {
                "wind_farm": "Second Good Farm",
                "farm_id": "Second_Good_Farm",
                "year": 2024,
                "recommended_for_scale": "yes",
                "dwell_count": 12,
                "tier_a_dwell_count": 4,
                "sample_point_count": 2,
                "estimated_current_rows": 8,
                "estimated_processed_size_mb": 0.001,
            },
        ]
    ).to_parquet(eligibility, index=False)

    bathy = tmp_path / "bathymetry.parquet"
    _sample_points().to_parquet(bathy, index=False)

    dwell = tmp_path / "dwell.parquet"
    pd.DataFrame(
        [
            {
                "dwell_id": "d1",
                "visit_id": "v1",
                "wind_farm": "Good Farm",
                "farm_id": "Good_Farm",
                "dwell_tier": "Tier A",
                "start_utc": pd.Timestamp("2024-01-01T00:30:00Z"),
                "end_utc": pd.Timestamp("2024-01-01T02:30:00Z"),
                "centroid_lat": 54.0,
                "centroid_lon": 7.0,
            },
            {
                "dwell_id": "d2",
                "visit_id": "v2",
                "wind_farm": "Good Farm",
                "farm_id": "Good_Farm",
                "dwell_tier": "Tier D",
                "start_utc": pd.Timestamp("2024-01-01T01:15:00Z"),
                "end_utc": pd.Timestamp("2024-01-01T02:45:00Z"),
                "centroid_lat": 54.1,
                "centroid_lon": 7.1,
            },
        ]
    ).to_parquet(dwell, index=False)
    return eligibility, bathy, dwell, tmp_path / "reports"


def test_extract_archive_rows_schema_direction_and_depth_warning(tmp_path: Path) -> None:
    raw = tmp_path / "raw.nc"
    _write_raw_dataset(raw)

    rows = extract_archive_rows_from_raw(
        raw_path=raw,
        sample_points=_sample_points(),
        wind_farm="Good Farm",
        farm_id="Good_Farm",
        year=2024,
        product_id="prod",
        dataset_id="dataset",
    )

    assert list(rows.columns) == NWS_CURRENT_BATCH_COLUMNS
    assert len(rows) == 8
    assert rows["current_direction_convention"].eq(CURRENT_DIRECTION_CONVENTION).all()
    assert np.allclose(rows["current_speed"], 0.5)
    assert np.allclose(rows["current_direction_to_deg"], np.degrees(np.arctan2(0.3, 0.4)) % 360)
    assert rows.loc[rows["sample_point_id"].eq("turbine_0001"), "depth_warning_le_10m"].all()


def test_validate_archive_rejects_synthetic_provenance() -> None:
    rows = ensure_batch_schema(
        pd.DataFrame(
            {
                "timestamp_utc": [pd.Timestamp("2024-01-01T00:00:00Z")],
                "wind_farm": ["Good Farm"],
                "year": [2024],
                "sample_point_id": ["p1"],
                "current_u": [0.1],
                "current_v": [0.2],
                "current_speed": [0.2236068],
                "current_direction_to_deg": [26.565],
                "current_native_temporal_resolution_minutes": [60],
                "provenance_status": ["simulated fallback"],
            }
        )
    )

    with pytest.raises(ValueError, match="fallback/simulated"):
        validate_archive_rows(rows)


def test_dry_run_writes_manifest_but_no_partition(tmp_path: Path) -> None:
    eligibility, bathy, dwell, report_dir = _write_batch_inputs(tmp_path)
    output_root = tmp_path / "processed"
    raw_root = tmp_path / "Data" / "Raw" / "Metocean" / "CMEMS" / "NorthWestShelf" / "Currents" / "Pilots"

    result = run_nws_current_batch(
        eligibility_path=eligibility,
        output_root=output_root,
        raw_cache_root=raw_root,
        report_dir=report_dir,
        bathymetry_path=bathy,
        dwell_weather_path=dwell,
        top_n=1,
        dry_run=True,
        overwrite=False,
    )

    assert result.manifest["status"].tolist() == ["planned"]
    assert result.manifest_path.exists()
    assert result.dry_run_report_path.exists()
    assert not partition_path(output_root, "Good Farm", 2024).exists()


def test_actual_run_uses_local_raw_cache_and_skips_stress_rows(tmp_path: Path) -> None:
    eligibility, bathy, dwell, report_dir = _write_batch_inputs(tmp_path)
    output_root = tmp_path / "processed"
    raw_root = tmp_path / "Data" / "Raw" / "Metocean" / "CMEMS" / "NorthWestShelf" / "Currents" / "Pilots"
    raw = raw_cache_path(raw_root, "Good Farm", 2024, "dataset")
    _write_raw_dataset(raw)

    result = run_nws_current_batch(
        eligibility_path=eligibility,
        output_root=output_root,
        raw_cache_root=raw_root,
        report_dir=report_dir,
        bathymetry_path=bathy,
        dwell_weather_path=dwell,
        top_n=1,
        dry_run=False,
        overwrite=False,
        product_id="prod",
        dataset_id="dataset",
    )

    processed = partition_path(output_root, "Good Farm", 2024)
    assert processed.exists()
    assert result.validation_report_path and result.validation_report_path.exists()
    assert result.manifest["status"].tolist() == ["validated"]
    assert result.manifest["row_count"].tolist() == [8]
    assert "Stress Farm" not in result.selected["wind_farm"].tolist()


def test_existing_partition_is_skipped_without_overwrite(tmp_path: Path) -> None:
    eligibility, bathy, dwell, report_dir = _write_batch_inputs(tmp_path)
    output_root = tmp_path / "processed"
    raw_root = tmp_path / "Data" / "Raw" / "Metocean" / "CMEMS" / "NorthWestShelf" / "Currents" / "Pilots"
    raw = raw_cache_path(raw_root, "Good Farm", 2024, "dataset")
    _write_raw_dataset(raw)

    kwargs = dict(
        eligibility_path=eligibility,
        output_root=output_root,
        raw_cache_root=raw_root,
        report_dir=report_dir,
        bathymetry_path=bathy,
        dwell_weather_path=dwell,
        top_n=1,
        dry_run=False,
        product_id="prod",
        dataset_id="dataset",
    )
    run_nws_current_batch(**kwargs, overwrite=False)
    second = run_nws_current_batch(**kwargs, overwrite=False)

    assert second.manifest["status"].tolist() == ["skipped_existing"]
    assert second.manifest["qa_status"].tolist() == ["passed"]
    assert "Good Farm" in second.validation_report_path.read_text(encoding="utf-8")


def test_remaining_recommended_selects_all_normal_rows_and_excludes_stress(tmp_path: Path) -> None:
    eligibility, _, _, _ = _write_batch_inputs(tmp_path)

    selected = load_recommended_farm_years(
        eligibility_path=eligibility,
        all_recommended=True,
    )

    assert selected["wind_farm"].tolist() == ["Good Farm", "Second Good Farm"]
    assert selected["recommended_for_scale"].eq("yes").all()
