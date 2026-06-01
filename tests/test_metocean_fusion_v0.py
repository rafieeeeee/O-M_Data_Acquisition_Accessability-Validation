from pathlib import Path

import pandas as pd
import pytest

from om_pipeline.metocean.metocean_fusion_v0 import (
    REQUIRED_OUTPUT_COLUMNS,
    build_metocean_fusion_v0,
)


def _write_dwell(path: Path) -> None:
    rows = [
        {
            "dwell_id": "dw_baltic",
            "visit_id": "v1",
            "wind_farm": "Baltic_Farm",
            "farm_id": "Baltic Farm",
            "dwell_tier": "Tier A",
            "start_utc": "2024-01-01T00:05:00Z",
            "end_utc": "2024-01-01T00:35:00Z",
            "centroid_lat": 55.0,
            "centroid_lon": 13.0,
            "active_hs_mean": None,
            "active_tp_mean": None,
            "active_wave_direction_sin_mean": None,
            "active_wave_direction_cos_mean": None,
        },
        {
            "dwell_id": "dw_nws",
            "visit_id": "v2",
            "wind_farm": "NWS_Farm",
            "farm_id": "NWS Farm",
            "dwell_tier": "Tier B",
            "start_utc": "2024-01-01T00:20:00Z",
            "end_utc": "2024-01-01T00:50:00Z",
            "centroid_lat": 54.0,
            "centroid_lon": 2.0,
            "active_hs_mean": None,
            "active_tp_mean": None,
            "active_wave_direction_sin_mean": None,
            "active_wave_direction_cos_mean": None,
        },
        {
            "dwell_id": "dw_nora3",
            "visit_id": "v3",
            "wind_farm": "No_Source_Farm",
            "farm_id": "No Source Farm",
            "dwell_tier": "Tier A",
            "start_utc": "2024-01-01T00:00:00Z",
            "end_utc": "2024-01-01T00:30:00Z",
            "centroid_lat": 53.0,
            "centroid_lon": 1.0,
            "active_hs_mean": 1.8,
            "active_tp_mean": 7.0,
            "active_wave_direction_sin_mean": 0.0,
            "active_wave_direction_cos_mean": -1.0,
        },
    ]
    pd.DataFrame(rows).to_parquet(path, index=False)


def _write_bathymetry(path: Path) -> None:
    rows = []
    for farm, lat, lon, depth in [
        ("Baltic Farm", 55.0, 13.0, 40.0),
        ("NWS Farm", 54.0, 2.0, 30.0),
        ("No Source Farm", 53.0, 1.0, 20.0),
    ]:
        rows.append(
            {
                "wind_farm": farm,
                "sample_point_id": "farm_centroid",
                "sample_point_type": "farm_centroid",
                "lat": lat,
                "lon": lon,
                "water_depth_m": depth,
                "bathymetry_source": "emodnet",
                "bathymetry_version": "test",
                "bathymetry_grid_lat": lat,
                "bathymetry_grid_lon": lon,
                "bathymetry_distance_m": 10.0,
                "bathymetry_assignment_method": "test",
                "depth_sign_convention": "positive_down_meters_in_processed_table",
                "bathymetry_vertical_datum": "LAT",
                "bathymetry_spatial_match_status": "ok",
            }
        )
    pd.DataFrame(rows).to_parquet(path, index=False)


def _write_source_partition(root: Path, farm_slug: str, year: int, prefix: str, rows: list[dict]) -> None:
    path = root / f"wind_farm={farm_slug}" / f"year={year}" / "part.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_parquet(path, index=False)


def test_fusion_v0_preserves_rows_and_priority(tmp_path):
    dwell = tmp_path / "dwell.parquet"
    bathy = tmp_path / "bathymetry.parquet"
    nws_root = tmp_path / "nws"
    baltic_root = tmp_path / "baltic"
    output = tmp_path / "fusion.parquet"
    report = tmp_path / "report.md"
    _write_dwell(dwell)
    _write_bathymetry(bathy)
    _write_source_partition(
        baltic_root,
        "Baltic_Farm",
        2024,
        "baltic",
        [
            {
                "timestamp_utc": "2024-01-01T00:00:00Z",
                "sample_point_id": "farm_centroid",
                "baltic_wave_hs": 1.0,
                "baltic_wave_tp": 5.0,
                "baltic_wave_dir": 90.0,
            },
            {
                "timestamp_utc": "2024-01-01T01:00:00Z",
                "sample_point_id": "farm_centroid",
                "baltic_wave_hs": 2.0,
                "baltic_wave_tp": 6.0,
                "baltic_wave_dir": 90.0,
            },
        ],
    )
    _write_source_partition(
        nws_root,
        "NWS_Farm",
        2024,
        "nws",
        [
            {
                "timestamp_utc": "2024-01-01T00:00:00Z",
                "sample_point_id": "farm_centroid",
                "nws_wave_hs": 1.4,
                "nws_wave_tp": 6.4,
                "nws_wave_dir": 180.0,
            }
        ],
    )

    result = build_metocean_fusion_v0(
        dwell_weather_input=dwell,
        nws_root=nws_root,
        baltic_root=baltic_root,
        bathymetry_points=bathy,
        output_path=output,
        report_path=report,
    )

    out = pd.read_parquet(output)
    assert len(out) == 3
    assert set(REQUIRED_OUTPUT_COLUMNS).issubset(out.columns)
    assert out.loc[out["dwell_id"].eq("dw_baltic"), "fusion_wave_source"].iloc[0] == "baltic"
    assert out.loc[out["dwell_id"].eq("dw_nws"), "fusion_wave_source"].iloc[0] == "nws"
    assert out.loc[out["dwell_id"].eq("dw_nora3"), "fusion_wave_source"].iloc[0] == "nora3"
    assert out["water_depth_m"].notna().all()
    assert result.validation["row_count_preserved"] is True
    assert report.exists()
    assert "Research Question" in report.read_text(encoding="utf-8")


def test_fusion_v0_blocks_overwrite(tmp_path):
    dwell = tmp_path / "dwell.parquet"
    bathy = tmp_path / "bathymetry.parquet"
    nws_root = tmp_path / "nws"
    baltic_root = tmp_path / "baltic"
    output = tmp_path / "fusion.parquet"
    report = tmp_path / "report.md"
    _write_dwell(dwell)
    _write_bathymetry(bathy)
    nws_root.mkdir()
    baltic_root.mkdir()
    output.write_text("exists", encoding="utf-8")

    with pytest.raises(FileExistsError):
        build_metocean_fusion_v0(
            dwell_weather_input=dwell,
            nws_root=nws_root,
            baltic_root=baltic_root,
            bathymetry_points=bathy,
            output_path=output,
            report_path=report,
        )


def test_fusion_v0_marks_missing_when_no_source_has_wave_pair(tmp_path):
    dwell = tmp_path / "dwell.parquet"
    bathy = tmp_path / "bathymetry.parquet"
    nws_root = tmp_path / "nws"
    baltic_root = tmp_path / "baltic"
    output = tmp_path / "fusion.parquet"
    report = tmp_path / "report.md"
    _write_dwell(dwell)
    _write_bathymetry(bathy)
    nws_root.mkdir()
    baltic_root.mkdir()

    result = build_metocean_fusion_v0(
        dwell_weather_input=dwell,
        nws_root=nws_root,
        baltic_root=baltic_root,
        bathymetry_points=bathy,
        output_path=output,
        report_path=report,
    )

    out = result.output
    missing_sources = out.loc[out["dwell_id"].isin(["dw_baltic", "dw_nws"]), "fusion_wave_source"].tolist()
    assert missing_sources == ["missing", "missing"]
    assert out.loc[out["dwell_id"].eq("dw_nora3"), "fusion_wave_source"].iloc[0] == "nora3"
