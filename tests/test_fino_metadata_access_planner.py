import pandas as pd
import pytest

from om_pipeline.metocean.fino_metadata_planner import (
    STATION_METADATA_SCHEMA,
    build_fino_metadata_access_plan,
    plan_fino_metadata_access,
)


def _write_requirements(path):
    pd.DataFrame(
        [
            {"wind_farm": "Alpha Ventus", "farm_id": "Alpha_Ventus"},
            {"wind_farm": "Far Farm", "farm_id": "Far_Farm"},
        ]
    ).to_csv(path, index=False)


def _write_bathymetry_points(path):
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "wind_farm": "Alpha Ventus",
                "sample_point_id": "farm_centroid",
                "sample_point_type": "farm_centroid",
                "lat": 54.01095,
                "lon": 6.606525,
            },
            {
                "wind_farm": "Alpha Ventus",
                "sample_point_id": "turbine_0000",
                "sample_point_type": "turbine",
                "lat": 54.0147,
                "lon": 6.5882,
            },
            {
                "wind_farm": "Far Farm",
                "sample_point_id": "farm_centroid",
                "sample_point_type": "farm_centroid",
                "lat": 58.0,
                "lon": -3.0,
            },
        ]
    ).to_parquet(path, index=False)


def test_fino_planner_builds_station_catalog_and_matches(tmp_path):
    requirements = tmp_path / "requirements.csv"
    bathymetry = tmp_path / "bathymetry.parquet"
    _write_requirements(requirements)
    _write_bathymetry_points(bathymetry)

    result = build_fino_metadata_access_plan(
        requirements_path=requirements,
        bathymetry_points_path=bathymetry,
        raw_metocean_root=tmp_path / "raw",
        processed_fino_root=tmp_path / "processed" / "fino",
    )

    assert result.summary["station_count"] == 3
    assert set(result.station_plan["station_id"]) == {"FINO1", "FINO2", "FINO3"}
    assert result.station_metadata_schema == STATION_METADATA_SCHEMA
    assert "timestamp_utc" in result.timeseries_schema
    fino1_alpha = result.station_farm_matches[
        (result.station_farm_matches["station_id"] == "FINO1")
        & (result.station_farm_matches["wind_farm"] == "Alpha Ventus")
    ].iloc[0]
    assert fino1_alpha["match_role"] == "direct_validation_candidate"
    assert fino1_alpha["nearest_sample_distance_km"] < 1.0


def test_fino_planner_dry_run_writes_only_report(tmp_path):
    requirements = tmp_path / "requirements.csv"
    bathymetry = tmp_path / "bathymetry.parquet"
    raw_root = tmp_path / "raw"
    output = tmp_path / "fino_plan.md"
    processed_root = tmp_path / "processed" / "fino"
    _write_requirements(requirements)
    _write_bathymetry_points(bathymetry)
    (raw_root / "FINO1").mkdir(parents=True)

    result = plan_fino_metadata_access(
        output_report=output,
        requirements_path=requirements,
        bathymetry_points_path=bathymetry,
        raw_metocean_root=raw_root,
        processed_fino_root=processed_root,
        dry_run=True,
    )

    assert output.exists()
    assert not processed_root.exists()
    assert result.output_report == output
    text = output.read_text(encoding="utf-8")
    assert "dry-run planning only" in text
    assert "FINO1" in text
    assert "BSH-Login" in text
    assert "No FINO bulk download" in text


def test_fino_planner_blocks_non_dry_run(tmp_path):
    requirements = tmp_path / "requirements.csv"
    bathymetry = tmp_path / "bathymetry.parquet"
    _write_requirements(requirements)
    _write_bathymetry_points(bathymetry)

    with pytest.raises(ValueError, match="dry-run mode only"):
        plan_fino_metadata_access(
            output_report=tmp_path / "report.md",
            requirements_path=requirements,
            bathymetry_points_path=bathymetry,
            dry_run=False,
        )


def test_fino_planner_missing_requirements_fails_cleanly(tmp_path):
    with pytest.raises(FileNotFoundError, match="Common metocean requirements table"):
        build_fino_metadata_access_plan(
            requirements_path=tmp_path / "missing.csv",
            bathymetry_points_path=tmp_path / "missing.parquet",
        )


def test_fino_planner_falls_back_to_requirement_centroids(tmp_path):
    requirements = tmp_path / "requirements.csv"
    pd.DataFrame(
        [
            {
                "wind_farm": "Alpha Ventus",
                "farm_id": "Alpha_Ventus",
                "min_lon": 6.3,
                "max_lon": 6.9,
                "min_lat": 53.8,
                "max_lat": 54.2,
            }
        ]
    ).to_csv(requirements, index=False)

    result = build_fino_metadata_access_plan(
        requirements_path=requirements,
        bathymetry_points_path=tmp_path / "missing.parquet",
        raw_metocean_root=tmp_path / "raw",
        processed_fino_root=tmp_path / "processed",
    )

    assert result.summary["sample_point_source"] == "requirements_bbox_centroids"
    assert result.summary["farm_count"] == 1
