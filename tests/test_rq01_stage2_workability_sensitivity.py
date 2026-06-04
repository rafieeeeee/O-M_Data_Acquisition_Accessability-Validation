from pathlib import Path

import pandas as pd
import pytest

from om_pipeline.analysis.rq01_stage2_workability_sensitivity import (
    CURRENT_VALUE_COLUMNS,
    WIND_DIRECTION_COLUMNS,
    build_binned_occupancy_table,
    build_claim_boundary_checks,
    build_lane_comparisons,
    build_lane_counts,
    build_lane_feature_summary,
    build_rq01_stage2_workability_sensitivity,
    validate_required_columns,
)


def _fusion_frame() -> pd.DataFrame:
    rows = [
        {
            "dwell_id": "d1",
            "wind_farm": "Farm A",
            "dwell_tier": "Tier A",
            "start_utc": "2020-01-01T00:00:00Z",
            "selected_wave_source": "NORA3",
            "selected_hs_mean": 0.8,
            "selected_tp_mean": 4.0,
            "has_wind_speed": True,
            "wind_speed_mean": 6.0,
            "wind_source": "NORA3",
            "has_current": True,
            "current_speed_mean": 0.3,
            "current_source": "NWS",
            "depth_warning_le_10m": False,
            "current_u_mean": 0.1,
            "current_v_mean": 0.2,
            "current_speed_p95": 0.4,
            "current_direction_to_sin_mean": 0.0,
            "current_direction_to_cos_mean": 1.0,
            "current_direction_to_deg_mean": 0.0,
            "current_depth_m": 5.0,
            "wind_direction_sin_mean": 0.0,
            "wind_direction_cos_mean": 1.0,
            "wind_direction_deg_mean": 0.0,
            "model_ready_wave_only": True,
            "model_ready_wave_wind": True,
            "model_ready_wave_current": True,
            "model_ready_wave_wind_current": True,
            "model_ready_high_confidence": True,
        },
        {
            "dwell_id": "d2",
            "wind_farm": "Farm B",
            "dwell_tier": "Tier A",
            "start_utc": "2021-01-01T00:00:00Z",
            "selected_wave_source": "NORA3",
            "selected_hs_mean": 1.2,
            "selected_tp_mean": 5.0,
            "has_wind_speed": True,
            "wind_speed_mean": 8.0,
            "wind_source": "NORA3",
            "has_current": False,
            "current_speed_mean": pd.NA,
            "current_source": pd.NA,
            "depth_warning_le_10m": True,
            "current_u_mean": pd.NA,
            "current_v_mean": pd.NA,
            "current_speed_p95": pd.NA,
            "current_direction_to_sin_mean": pd.NA,
            "current_direction_to_cos_mean": pd.NA,
            "current_direction_to_deg_mean": pd.NA,
            "current_depth_m": pd.NA,
            "wind_direction_sin_mean": pd.NA,
            "wind_direction_cos_mean": pd.NA,
            "wind_direction_deg_mean": pd.NA,
            "model_ready_wave_only": True,
            "model_ready_wave_wind": True,
            "model_ready_wave_current": False,
            "model_ready_wave_wind_current": False,
            "model_ready_high_confidence": False,
        },
        {
            "dwell_id": "d3",
            "wind_farm": "Farm C",
            "dwell_tier": "Tier A",
            "start_utc": "2021-02-01T00:00:00Z",
            "selected_wave_source": "NORA3",
            "selected_hs_mean": 2.0,
            "selected_tp_mean": 8.0,
            "has_wind_speed": False,
            "wind_speed_mean": pd.NA,
            "wind_source": pd.NA,
            "has_current": True,
            "current_speed_mean": 0.6,
            "current_source": "NWS",
            "depth_warning_le_10m": False,
            "current_u_mean": 0.3,
            "current_v_mean": 0.4,
            "current_speed_p95": 0.7,
            "current_direction_to_sin_mean": 0.0,
            "current_direction_to_cos_mean": 1.0,
            "current_direction_to_deg_mean": 0.0,
            "current_depth_m": 5.0,
            "wind_direction_sin_mean": pd.NA,
            "wind_direction_cos_mean": pd.NA,
            "wind_direction_deg_mean": pd.NA,
            "model_ready_wave_only": True,
            "model_ready_wave_wind": False,
            "model_ready_wave_current": True,
            "model_ready_wave_wind_current": False,
            "model_ready_high_confidence": False,
        },
        {
            "dwell_id": "d4",
            "wind_farm": "Farm A",
            "dwell_tier": "Tier B",
            "start_utc": "2022-01-01T00:00:00Z",
            "selected_wave_source": "NORA3",
            "selected_hs_mean": 3.0,
            "selected_tp_mean": 10.0,
            "has_wind_speed": True,
            "wind_speed_mean": 11.0,
            "wind_source": "NORA3",
            "has_current": True,
            "current_speed_mean": 0.8,
            "current_source": "NWS",
            "depth_warning_le_10m": False,
            "current_u_mean": 0.5,
            "current_v_mean": 0.6,
            "current_speed_p95": 0.9,
            "current_direction_to_sin_mean": 0.0,
            "current_direction_to_cos_mean": 1.0,
            "current_direction_to_deg_mean": 0.0,
            "current_depth_m": 5.0,
            "wind_direction_sin_mean": 0.0,
            "wind_direction_cos_mean": 1.0,
            "wind_direction_deg_mean": 0.0,
            "model_ready_wave_only": True,
            "model_ready_wave_wind": True,
            "model_ready_wave_current": True,
            "model_ready_wave_wind_current": True,
            "model_ready_high_confidence": True,
        },
    ]
    return pd.DataFrame(rows)


def _readiness_summary() -> dict:
    return {
        "final_recommendation": "proceed_with_restrictions",
        "key_caveats": [
            "partial_event_scale_current_coverage",
            "wind_direction_sensitivity_only",
            "depth_warning_sensitivity_required",
        ],
    }


def test_required_column_validation_reports_missing_columns() -> None:
    frame = _fusion_frame().drop(columns=["model_ready_wave_only"])

    with pytest.raises(ValueError, match="model_ready_wave_only"):
        validate_required_columns(frame)


def test_lane_counts_use_tier_a_readiness_flags_and_depth_scopes() -> None:
    frame = _fusion_frame()
    counts = build_lane_counts(frame).set_index(["lane_id", "depth_scope"])

    assert counts.loc[("wave_only", "all_tier_a"), "tier_a_event_count"] == 3
    assert counts.loc[("wave_wind_speed", "all_tier_a"), "tier_a_event_count"] == 2
    assert counts.loc[("wave_current", "all_tier_a"), "tier_a_event_count"] == 2
    assert counts.loc[("wave_wind_current", "depth_warning_excluded"), "tier_a_event_count"] == 1
    assert counts.loc[("wave_only", "depth_warning_only"), "tier_a_event_count"] == 1


def test_lane_comparison_flags_material_subset_retention_change() -> None:
    frame = _fusion_frame()
    counts = build_lane_counts(frame)
    summary = build_lane_feature_summary(frame)
    comparisons = build_lane_comparisons(counts, summary)

    wave_wind = comparisons[
        comparisons["comparison_lane_id"].eq("wave_wind_speed")
        & comparisons["depth_scope"].eq("all_tier_a")
    ].iloc[0]

    assert wave_wind["retained_share_vs_wave_only"] == pytest.approx(2 / 3)
    assert wave_wind["materiality_screen_result"] == "material_difference_screened"
    assert "retained_share_below_threshold" in wave_wind["materiality_triggers"]


def test_binned_occupancy_excludes_null_current_without_zero_fill() -> None:
    frame = _fusion_frame()
    occupancy = build_binned_occupancy_table(frame)
    current = occupancy[
        occupancy["lane_id"].eq("wave_current") & occupancy["depth_scope"].eq("all_tier_a")
    ]

    assert current["observed_count"].sum() == 2
    assert current["current_speed_bin_left"].min() >= 0.0


def test_claim_boundary_fails_when_missing_current_has_value() -> None:
    frame = _fusion_frame()
    frame.loc[frame["dwell_id"].eq("d2"), "current_speed_mean"] = 0.0
    checks = build_claim_boundary_checks(frame, _readiness_summary()).set_index("check")

    assert checks.loc["missing_current_null_not_zero", "status"] == "fail"


def test_wind_direction_columns_are_not_lane_predictors() -> None:
    frame = _fusion_frame()
    checks = build_claim_boundary_checks(frame, _readiness_summary()).set_index("check")

    assert checks.loc["wind_direction_excluded_from_primary_predictors", "status"] == "pass"
    for column in WIND_DIRECTION_COLUMNS:
        assert column not in "|".join(build_lane_counts(frame)["feature_columns"].unique())


def test_build_outputs_writes_summary_report_and_tables(tmp_path: Path) -> None:
    frame = _fusion_frame()
    fusion_path = tmp_path / "fusion.parquet"
    readiness_path = tmp_path / "readiness_summary.json"
    output_dir = tmp_path / "processed"
    report_dir = tmp_path / "reports"
    frame.to_parquet(fusion_path, index=False)
    readiness_path.write_text(
        "{"
        '"final_recommendation":"proceed_with_restrictions",'
        '"key_caveats":["partial_event_scale_current_coverage",'
        '"wind_direction_sensitivity_only","depth_warning_sensitivity_required"]'
        "}\n",
        encoding="utf-8",
    )

    result = build_rq01_stage2_workability_sensitivity(
        fusion_v2_path=fusion_path,
        readiness_summary_path=readiness_path,
        output_dir=output_dir,
        report_dir=report_dir,
        run_timestamp_utc="2026-06-04T00:00:00Z",
    )

    assert result.summary["timestamp_utc"] == "2026-06-04T00:00:00Z"
    assert result.summary["analysis_recommendation"] == "restricted_descriptive_sensitivity_ready_for_review"
    assert result.files["sensitivity_summary_json"].exists()
    assert result.files["sensitivity_report_md"].exists()
    assert result.files["lane_counts_csv"].exists()
    report = result.files["sensitivity_report_md"].read_text(encoding="utf-8")
    assert "not train models" in report
    assert "P(operation | weather)" in report
