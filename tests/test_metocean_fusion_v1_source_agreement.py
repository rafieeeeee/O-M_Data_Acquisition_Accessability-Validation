from pathlib import Path

import pandas as pd
import pytest

from om_pipeline.metocean.metocean_fusion_v1_source_agreement import (
    REQUIRED_CANDIDATE_COLUMNS,
    build_metocean_fusion_v1_source_agreement,
    build_wave_source_candidates,
    compute_pairwise_agreement,
    score_event_confidence,
)


def _write_dwell(path: Path) -> None:
    rows = [
        {
            "dwell_id": "dw_agree",
            "visit_id": "v1",
            "wind_farm": "Farm_A",
            "farm_id": "Farm A",
            "dwell_tier": "Tier A",
            "start_utc": "2024-01-01T00:10:00Z",
            "end_utc": "2024-01-01T00:40:00Z",
            "centroid_lat": 55.0,
            "centroid_lon": 13.0,
            "active_hs_mean": 1.00,
            "active_tp_mean": 5.00,
            "active_wave_direction_sin_mean": 1.0,
            "active_wave_direction_cos_mean": 0.0,
            "active_n_weather_records": 3,
            "active_source_available": True,
        },
        {
            "dwell_id": "dw_single",
            "visit_id": "v2",
            "wind_farm": "Farm_A",
            "farm_id": "Farm A",
            "dwell_tier": "Tier B",
            "start_utc": "2024-01-01T03:10:00Z",
            "end_utc": "2024-01-01T03:40:00Z",
            "centroid_lat": 55.0,
            "centroid_lon": 13.0,
            "active_hs_mean": None,
            "active_tp_mean": None,
            "active_wave_direction_sin_mean": None,
            "active_wave_direction_cos_mean": None,
            "active_n_weather_records": 0,
            "active_source_available": False,
        },
    ]
    pd.DataFrame(rows).to_parquet(path, index=False)


def _write_bathymetry(path: Path) -> None:
    pd.DataFrame(
        [
            {
                "wind_farm": "Farm A",
                "sample_point_id": "farm_centroid",
                "sample_point_type": "farm_centroid",
                "lat": 55.0,
                "lon": 13.0,
                "water_depth_m": 25.0,
                "bathymetry_source": "emodnet",
                "bathymetry_version": "test",
                "bathymetry_grid_lat": 55.0,
                "bathymetry_grid_lon": 13.0,
                "bathymetry_distance_m": 5.0,
                "bathymetry_assignment_method": "test",
                "depth_sign_convention": "positive_down_meters_in_processed_table",
                "bathymetry_vertical_datum": "LAT",
                "bathymetry_spatial_match_status": "ok",
            }
        ]
    ).to_parquet(path, index=False)


def _write_fusion_v0(path: Path) -> None:
    pd.DataFrame(
        [
            {
                "dwell_id": "dw_agree",
                "dwell_tier": "Tier A",
                "fusion_wave_source": "baltic",
                "fusion_hs_mean": 1.04,
                "fusion_tp_mean": 5.2,
            },
            {
                "dwell_id": "dw_single",
                "dwell_tier": "Tier B",
                "fusion_wave_source": "nws",
                "fusion_hs_mean": 0.8,
                "fusion_tp_mean": 4.5,
            },
        ]
    ).to_parquet(path, index=False)


def _write_source_partition(root: Path, farm_slug: str, year: int, prefix: str, rows: list[dict]) -> None:
    path = root / f"wind_farm={farm_slug}" / f"year={year}" / "part.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_parquet(path, index=False)


def _source_rows(prefix: str, hs: float, tp: float, direction: float, cadence_hours: int) -> list[dict]:
    rows = []
    for hour in [0, cadence_hours]:
        rows.append(
            {
                "timestamp_utc": f"2024-01-01T{hour:02d}:00:00Z",
                "sample_point_id": "farm_centroid",
                "sample_point_type": "farm_centroid",
                "lat": 55.0,
                "lon": 13.0,
                f"{prefix}_grid_lat": 55.0,
                f"{prefix}_grid_lon": 13.0,
                f"{prefix}_spatial_distance_km": 0.25,
                f"{prefix}_source_file": f"{prefix}.nc",
                f"{prefix}_extraction_method": "nearest_valid_grid",
                f"{prefix}_spatial_match_status": "ok",
                f"{prefix}_wave_hs": hs,
                f"{prefix}_wave_tp": tp,
                f"{prefix}_wave_dir": direction,
            }
        )
    return rows


def _minimal_candidate(
    dwell_id: str,
    source: str,
    hs: float | None,
    tp: float | None,
    depth: float = 25.0,
    distance: float | None = 0.5,
    gap: float | None = 10.0,
) -> dict:
    direction_sin = 1.0 if hs is not None and tp is not None else None
    direction_cos = 0.0 if hs is not None and tp is not None else None
    return {
        "dwell_id": dwell_id,
        "visit_id": f"visit_{dwell_id}",
        "wind_farm": "Farm_A",
        "farm_id": "Farm A",
        "dwell_tier": "Tier A",
        "start_utc": pd.Timestamp("2024-01-01T00:00:00Z"),
        "end_utc": pd.Timestamp("2024-01-01T00:30:00Z"),
        "centroid_lat": 55.0,
        "centroid_lon": 13.0,
        "source": source,
        "source_product": source,
        "source_native_temporal_resolution_minutes": 60,
        "source_native_spatial_resolution_km": 2.0,
        "source_domain": source,
        "source_domain_match": hs is not None and tp is not None,
        "sample_point_id": "farm_centroid",
        "sample_point_type": "farm_centroid",
        "source_sample_lat": 55.0,
        "source_sample_lon": 13.0,
        "source_sample_distance_km": distance,
        "event_window_sample_count": 1 if hs is not None and tp is not None else 0,
        "nearest_time_gap_minutes": gap,
        "event_bracketed_by_source_times": True,
        "temporal_assignment_method": "test",
        "spatial_assignment_method": "test",
        "hs_mean": hs,
        "tp_mean": tp,
        "wave_direction_sin_mean": direction_sin,
        "wave_direction_cos_mean": direction_cos,
        "variable_completeness_score": 1.0 if hs is not None and tp is not None else 0.0,
        "source_missing_reason": None if hs is not None and tp is not None else "missing",
        "water_depth_m": depth,
        "shallow_water_flag": depth <= 10.0,
        "coastal_complexity_flag": depth <= 10.0,
        "source_quality_notes": "test",
    }


def _candidate_frame(rows: list[dict]) -> pd.DataFrame:
    frame = pd.DataFrame(rows)
    for column in REQUIRED_CANDIDATE_COLUMNS:
        if column not in frame:
            frame[column] = None
    return frame[REQUIRED_CANDIDATE_COLUMNS]


def test_candidate_table_preserves_row_identity_and_multiple_sources(tmp_path):
    dwell_path = tmp_path / "dwell.parquet"
    bathy_path = tmp_path / "bathymetry.parquet"
    nws_root = tmp_path / "nws"
    baltic_root = tmp_path / "baltic"
    _write_dwell(dwell_path)
    _write_bathymetry(bathy_path)
    _write_source_partition(nws_root, "Farm_A", 2024, "nws", _source_rows("nws", 1.05, 5.1, 90.0, 3))
    _write_source_partition(
        baltic_root,
        "Farm_A",
        2024,
        "baltic",
        _source_rows("baltic", 1.04, 5.2, 90.0, 1),
    )

    dwell = pd.read_parquet(dwell_path)
    dwell["start_utc"] = pd.to_datetime(dwell["start_utc"], utc=True)
    dwell["end_utc"] = pd.to_datetime(dwell["end_utc"], utc=True)
    dwell["_event_midpoint_utc"] = dwell["start_utc"] + (dwell["end_utc"] - dwell["start_utc"]) / 2
    dwell["_event_year"] = dwell["start_utc"].dt.year
    dwell["_source_farm_name"] = dwell["farm_id"]
    bathy = pd.read_parquet(bathy_path)

    candidates = build_wave_source_candidates(dwell, bathy, nws_root=nws_root, baltic_root=baltic_root)

    assert set(candidates["dwell_id"]) == {"dw_agree", "dw_single"}
    assert len(candidates) == 6
    assert candidates.groupby("dwell_id")["source"].nunique().eq(3).all()
    assert set(REQUIRED_CANDIDATE_COLUMNS).issubset(candidates.columns)


def test_pairwise_agreement_computes_hs_tp_differences():
    candidates = _candidate_frame(
        [
            _minimal_candidate("dw", "nora3", 1.0, 5.0),
            _minimal_candidate("dw", "nws", 1.2, 6.0),
        ]
    )

    pairwise = compute_pairwise_agreement(candidates)

    row = pairwise.iloc[0]
    assert row["hs_diff"] == pytest.approx(-0.2)
    assert row["hs_abs_diff"] == pytest.approx(0.2)
    assert row["tp_diff"] == pytest.approx(-1.0)
    assert row["tp_abs_diff"] == pytest.approx(1.0)
    assert row["agreement_class"] == "moderate_agreement"


def test_confidence_class_logic_agreeing_single_disagreeing_and_missing():
    rows = [
        _minimal_candidate("dw_agree", "nora3", 1.0, 5.0),
        _minimal_candidate("dw_agree", "nws", 1.08, 5.2),
        _minimal_candidate("dw_single", "nws", 0.8, 4.5),
        _minimal_candidate("dw_disagree", "nora3", 1.0, 5.0),
        _minimal_candidate("dw_disagree", "nws", 2.0, 8.0),
        _minimal_candidate("dw_missing", "nora3", None, None),
        _minimal_candidate("dw_missing", "nws", None, None),
    ]
    candidates = _candidate_frame(rows)
    pairwise = compute_pairwise_agreement(candidates)

    confidence = score_event_confidence(candidates, pairwise).set_index("dwell_id")

    assert confidence.loc["dw_agree", "wave_confidence_class"] == "A_high"
    assert confidence.loc["dw_single", "wave_confidence_class"] == "B_medium"
    assert confidence.loc["dw_disagree", "wave_confidence_class"] == "C_low"
    assert confidence.loc["dw_missing", "wave_confidence_class"] == "D_unsuitable"


def test_fusion_v0_priority_is_not_final_selection_rule():
    candidates = _candidate_frame(
        [
            _minimal_candidate("dw", "nora3", 1.00, 5.00),
            _minimal_candidate("dw", "nws", 1.05, 5.10),
            _minimal_candidate("dw", "baltic", 2.50, 9.00),
        ]
    )
    pairwise = compute_pairwise_agreement(candidates)

    confidence = score_event_confidence(candidates, pairwise)

    assert confidence["selected_wave_source"].iloc[0] != "baltic"
    assert "not by Fusion v0 source priority" in confidence["selection_reason"].iloc[0]


def test_build_preserves_confidence_rows_and_blocks_overwrite(tmp_path):
    dwell_path = tmp_path / "dwell.parquet"
    fusion_v0_path = tmp_path / "fusion_v0.parquet"
    bathy_path = tmp_path / "bathymetry.parquet"
    nws_root = tmp_path / "nws"
    baltic_root = tmp_path / "baltic"
    output_dir = tmp_path / "out"
    report_dir = tmp_path / "report"
    _write_dwell(dwell_path)
    _write_fusion_v0(fusion_v0_path)
    _write_bathymetry(bathy_path)
    _write_source_partition(nws_root, "Farm_A", 2024, "nws", _source_rows("nws", 1.05, 5.1, 90.0, 3))
    _write_source_partition(
        baltic_root,
        "Farm_A",
        2024,
        "baltic",
        _source_rows("baltic", 1.04, 5.2, 90.0, 1),
    )

    result = build_metocean_fusion_v1_source_agreement(
        dwell_weather=dwell_path,
        fusion_v0=fusion_v0_path,
        nws_root=nws_root,
        baltic_root=baltic_root,
        bathymetry=bathy_path,
        output_dir=output_dir,
        report_dir=report_dir,
        overwrite=True,
    )

    assert len(result.confidence) == 2
    assert result.validation["output_counts"]["confidence_row_count_preserved"] is True
    assert result.candidate_path.exists()
    assert result.pairwise_path.exists()
    assert result.confidence_path.exists()
    assert result.report_path.exists()

    with pytest.raises(FileExistsError):
        build_metocean_fusion_v1_source_agreement(
            dwell_weather=dwell_path,
            fusion_v0=fusion_v0_path,
            nws_root=nws_root,
            baltic_root=baltic_root,
            bathymetry=bathy_path,
            output_dir=output_dir,
            report_dir=report_dir,
            overwrite=False,
        )
