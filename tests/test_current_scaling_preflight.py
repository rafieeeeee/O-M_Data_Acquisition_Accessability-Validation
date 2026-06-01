from pathlib import Path

import pandas as pd
import pytest

from om_pipeline.metocean.current_scaling_preflight import (
    ELIGIBILITY_COLUMNS,
    build_nws_current_scale_eligibility,
    classify_baltic_hourly_source,
    classify_runtime,
    classify_shallow_warning,
    estimate_current_rows,
    estimate_processed_size_mb,
    estimate_raw_size_mb,
    recommend_farm_year,
    run_current_scaling_preflight,
    write_eligibility_table,
)


def _write_inputs(tmp_path: Path) -> tuple[Path, Path, Path, Path]:
    requirements = tmp_path / "requirements.csv"
    pd.DataFrame(
        [
            {
                "wind_farm": "Good Farm",
                "farm_id": "Good_Farm",
                "country": "Germany",
                "temporal_start": "2020-01-01",
                "temporal_end": "2020-12-31",
                "min_lon": 5.0,
                "max_lon": 5.5,
                "min_lat": 54.0,
                "max_lat": 54.5,
                "sample_point_count": 2,
            },
            {
                "wind_farm": "Shallow Farm",
                "farm_id": "Shallow_Farm",
                "country": "Germany",
                "temporal_start": "2020-01-01",
                "temporal_end": "2020-12-31",
                "min_lon": 6.0,
                "max_lon": 6.5,
                "min_lat": 54.0,
                "max_lat": 54.5,
                "sample_point_count": 3,
            },
            {
                "wind_farm": "Outside Farm",
                "farm_id": "Outside_Farm",
                "country": "Denmark",
                "temporal_start": "2020-01-01",
                "temporal_end": "2020-12-31",
                "min_lon": 14.0,
                "max_lon": 14.5,
                "min_lat": 54.0,
                "max_lat": 54.5,
                "sample_point_count": 2,
            },
        ]
    ).to_csv(requirements, index=False)

    dwell = tmp_path / "dwell.parquet"
    dwell_rows = []
    for farm, total, tier_a in [
        ("Good Farm", 12, 4),
        ("Shallow Farm", 20, 5),
        ("Outside Farm", 20, 5),
    ]:
        for idx in range(total):
            dwell_rows.append(
                {
                    "wind_farm": farm,
                    "farm_id": farm.replace(" ", "_"),
                    "dwell_tier": "Tier A" if idx < tier_a else "Tier D",
                    "start_utc": pd.Timestamp("2020-06-01", tz="UTC") + pd.Timedelta(days=idx),
                }
            )
    pd.DataFrame(dwell_rows).to_parquet(dwell, index=False)

    fusion = tmp_path / "fusion.parquet"
    wave_rows = []
    for farm in ["Good Farm", "Shallow Farm", "Outside Farm"]:
        for idx in range(10):
            wave_rows.append(
                {
                    "wind_farm": farm,
                    "farm_id": farm.replace(" ", "_"),
                    "start_utc": pd.Timestamp("2020-06-01", tz="UTC") + pd.Timedelta(days=idx),
                    "selected_hs_mean": 1.0,
                    "selected_tp_mean": 5.0,
                    "wave_confidence_class": "A_high" if idx < 6 else "C_low",
                }
            )
    pd.DataFrame(wave_rows).to_parquet(fusion, index=False)

    bathy = tmp_path / "bathymetry.parquet"
    pd.DataFrame(
        [
            {"wind_farm": "Good Farm", "sample_point_id": "p1", "water_depth_m": 25.0},
            {"wind_farm": "Good Farm", "sample_point_id": "p2", "water_depth_m": 30.0},
            {"wind_farm": "Shallow Farm", "sample_point_id": "p1", "water_depth_m": 5.0},
            {"wind_farm": "Shallow Farm", "sample_point_id": "p2", "water_depth_m": 8.0},
            {"wind_farm": "Shallow Farm", "sample_point_id": "p3", "water_depth_m": 9.0},
            {"wind_farm": "Outside Farm", "sample_point_id": "p1", "water_depth_m": 25.0},
            {"wind_farm": "Outside Farm", "sample_point_id": "p2", "water_depth_m": 30.0},
        ]
    ).to_parquet(bathy, index=False)
    return requirements, dwell, fusion, bathy


def test_eligibility_schema_and_recommendation_logic(tmp_path: Path) -> None:
    requirements, dwell, fusion, bathy = _write_inputs(tmp_path)

    eligibility = build_nws_current_scale_eligibility(
        requirements_path=requirements,
        dwell_weather_path=dwell,
        fusion_v1_path=fusion,
        bathymetry_path=bathy,
    )

    assert list(eligibility.columns) == ELIGIBILITY_COLUMNS
    assert len(eligibility) == 3
    by_farm = eligibility.set_index("wind_farm")
    assert by_farm.loc["Good Farm", "recommended_for_scale"] == "yes"
    assert by_farm.loc["Shallow Farm", "recommended_for_scale"] == "stress_test_only"
    assert by_farm.loc["Outside Farm", "recommended_for_scale"] == "no"
    assert by_farm.loc["Good Farm", "fusion_v1_valid_wave_count"] == 10
    assert by_farm.loc["Good Farm", "wave_confidence_a_b_count"] == 6


def test_shallow_water_warning_logic() -> None:
    assert classify_shallow_warning(0.0) == "none"
    assert classify_shallow_warning(0.1) == "some_depth_le_10m"
    assert classify_shallow_warning(0.4) == "moderate_depth_le_10m"
    assert classify_shallow_warning(0.6) == "dominated_by_depth_le_10m"
    assert classify_shallow_warning(0.9) == "severe_depth_le_10m_dominated"


def test_storage_estimate_calculation() -> None:
    rows = estimate_current_rows(sample_point_count=57, year=2020)
    assert rows == 500_688
    assert estimate_raw_size_mb(rows) == pytest.approx(32.044, abs=0.001)
    assert estimate_processed_size_mb(rows) == pytest.approx(4.499, abs=0.001)
    assert classify_runtime(rows) == "small"


def test_baltic_hourly_source_classification() -> None:
    decision = classify_baltic_hourly_source()
    assert decision["historical_hourly_source_exists"] is False
    assert decision["classification"] == "keep_baltic_contextual"
    assert "2010-2020" in decision["decision"]


def test_recommendation_can_mark_contextual_sensitivity_as_stress_test() -> None:
    row = pd.Series(
        {
            "nws_product_domain_match": "inside_nws_current_domain",
            "dwell_count": 80,
            "tier_a_dwell_count": 0,
            "sample_point_count": 10,
            "estimated_processed_size_mb": 1.0,
            "pct_sample_points_depth_le_10m": 0.0,
            "shallow_model_warning": "none",
        }
    )

    recommendation, reason = recommend_farm_year(row)

    assert recommendation == "stress_test_only"
    assert "non-Tier-A sensitivity" in reason


def test_no_download_or_current_candidate_write_behavior(tmp_path: Path) -> None:
    requirements, dwell, fusion, bathy = _write_inputs(tmp_path)
    output_dir = tmp_path / "out"
    report_dir = tmp_path / "reports"

    result = run_current_scaling_preflight(
        requirements_path=requirements,
        dwell_weather_path=dwell,
        fusion_v1_path=fusion,
        bathymetry_path=bathy,
        output_dir=output_dir,
        report_dir=report_dir,
        overwrite=False,
    )

    assert result.eligibility_path.exists()
    assert result.baltic_assessment_path.exists()
    assert result.preflight_report_path.exists()
    assert not (output_dir / "nws_current_candidates.parquet").exists()
    assert not (output_dir / "baltic_current_candidates.parquet").exists()

    with pytest.raises(FileExistsError):
        write_eligibility_table(result.eligibility, output_dir, overwrite=False)

