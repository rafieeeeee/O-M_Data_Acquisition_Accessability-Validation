from pathlib import Path

import pandas as pd
import pytest

from om_pipeline.analysis.fusion_v2_evidence_readiness import (
    CURRENT_VALUE_COLUMNS,
    WIND_DIRECTION_COLUMNS,
    build_fusion_v2_evidence_readiness,
    build_guardrail_table,
    build_input_cross_check_table,
    build_readiness_semantics_table,
    build_readiness_subset_counts,
    build_tier_a_readiness_counts,
    choose_recommendation,
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
            "wave_confidence_class": "A_high",
            "has_wind_speed": True,
            "has_wind_direction": True,
            "wind_confidence_class": "A_speed_direction",
            "wind_source": "NORA3",
            "wind_speed_mean": 6.0,
            "wind_direction_sin_mean": 0.0,
            "wind_direction_cos_mean": 1.0,
            "wind_direction_deg_mean": 0.0,
            "has_event_scale_current": True,
            "has_current": True,
            "current_confidence_class": "A_event_scale",
            "current_source": "NWS",
            "current_u_mean": 0.1,
            "current_v_mean": 0.2,
            "current_speed_mean": 0.3,
            "current_speed_p95": 0.4,
            "current_direction_to_sin_mean": 0.0,
            "current_direction_to_cos_mean": 1.0,
            "current_direction_to_deg_mean": 0.0,
            "current_depth_m": 5.0,
            "water_depth_m": 30.0,
            "depth_warning_le_10m": False,
            "has_wave": True,
            "has_bathymetry": True,
            "model_ready_wave_only": True,
            "model_ready_wave_wind": True,
            "model_ready_wave_current": True,
            "model_ready_wave_wind_current": True,
            "model_ready_high_confidence": True,
            "metocean_feature_class": "wave_wind_current_bathymetry_high_confidence",
            "metocean_missing_reason": "none",
        },
        {
            "dwell_id": "d2",
            "wind_farm": "Farm B",
            "dwell_tier": "Tier A",
            "start_utc": "2021-01-01T00:00:00Z",
            "selected_wave_source": "NORA3",
            "selected_hs_mean": 0.7,
            "selected_tp_mean": 3.5,
            "wave_confidence_class": "B_medium",
            "has_wind_speed": True,
            "has_wind_direction": False,
            "wind_confidence_class": "B_speed_only",
            "wind_source": "NORA3",
            "wind_speed_mean": 5.0,
            "wind_direction_sin_mean": pd.NA,
            "wind_direction_cos_mean": pd.NA,
            "wind_direction_deg_mean": pd.NA,
            "has_event_scale_current": False,
            "has_current": False,
            "current_confidence_class": "D_unsuitable",
            "current_source": pd.NA,
            "current_u_mean": pd.NA,
            "current_v_mean": pd.NA,
            "current_speed_mean": pd.NA,
            "current_speed_p95": pd.NA,
            "current_direction_to_sin_mean": pd.NA,
            "current_direction_to_cos_mean": pd.NA,
            "current_direction_to_deg_mean": pd.NA,
            "current_depth_m": pd.NA,
            "water_depth_m": 4.0,
            "depth_warning_le_10m": True,
            "has_wave": True,
            "has_bathymetry": True,
            "model_ready_wave_only": True,
            "model_ready_wave_wind": True,
            "model_ready_wave_current": False,
            "model_ready_wave_wind_current": False,
            "model_ready_high_confidence": False,
            "metocean_feature_class": "wave_wind_bathymetry_no_current",
            "metocean_missing_reason": "missing_nws_current_partition; depth_warning_le_10m",
        },
        {
            "dwell_id": "d3",
            "wind_farm": "Farm A",
            "dwell_tier": "Tier D",
            "start_utc": "2021-02-01T00:00:00Z",
            "selected_wave_source": "NORA3",
            "selected_hs_mean": 1.1,
            "selected_tp_mean": 5.0,
            "wave_confidence_class": "C_low",
            "has_wind_speed": False,
            "has_wind_direction": False,
            "wind_confidence_class": "D_unsuitable",
            "wind_source": pd.NA,
            "wind_speed_mean": pd.NA,
            "wind_direction_sin_mean": pd.NA,
            "wind_direction_cos_mean": pd.NA,
            "wind_direction_deg_mean": pd.NA,
            "has_event_scale_current": True,
            "has_current": True,
            "current_confidence_class": "A_event_scale",
            "current_source": "NWS",
            "current_u_mean": 0.1,
            "current_v_mean": 0.2,
            "current_speed_mean": 0.3,
            "current_speed_p95": 0.5,
            "current_direction_to_sin_mean": 0.0,
            "current_direction_to_cos_mean": 1.0,
            "current_direction_to_deg_mean": 0.0,
            "current_depth_m": 5.0,
            "water_depth_m": 15.0,
            "depth_warning_le_10m": False,
            "has_wave": True,
            "has_bathymetry": True,
            "model_ready_wave_only": True,
            "model_ready_wave_wind": False,
            "model_ready_wave_current": True,
            "model_ready_wave_wind_current": False,
            "model_ready_high_confidence": False,
            "metocean_feature_class": "wave_current_bathymetry_no_wind",
            "metocean_missing_reason": "no_active_nora3_wind_records",
        },
    ]
    return pd.DataFrame(rows)


def _cross_checks(frame: pd.DataFrame) -> pd.DataFrame:
    inputs = {
        "dwell_weather": frame[["dwell_id"]].copy(),
        "wave_confidence": frame[["dwell_id"]].copy(),
        "wind_confidence": frame[["dwell_id"]].copy(),
        "current_confidence": frame[["dwell_id"]].copy(),
        "bathymetry": pd.DataFrame({"wind_farm": ["Farm A", "Farm B"]}),
    }
    paths = {name: Path(f"{name}.parquet") for name in inputs}
    return build_input_cross_check_table(frame, inputs, paths)


def test_required_column_validation_reports_missing_columns() -> None:
    frame = _fusion_frame().drop(columns=["model_ready_wave_only"])

    with pytest.raises(ValueError, match="model_ready_wave_only"):
        validate_required_columns(frame)


def test_readiness_subset_and_tier_a_counts_use_canonical_flags() -> None:
    frame = _fusion_frame()

    subset = build_readiness_subset_counts(frame).set_index("subset")
    tier = build_tier_a_readiness_counts(frame).set_index("subset")

    assert subset.loc["model_ready_wave_wind_current", "event_count"] == 1
    assert subset.loc["model_ready_high_confidence", "event_count"] == 1
    assert tier.loc["model_ready_wave_wind", "tier_a_event_count"] == 2
    assert tier.loc["model_ready_high_confidence", "tier_a_event_count"] == 1


def test_duplicate_row_identity_check_is_an_integrity_failure() -> None:
    frame = _fusion_frame()
    duplicated = pd.concat([frame, frame.iloc[[0]]], ignore_index=True)
    semantics = build_readiness_semantics_table(duplicated)
    guardrails = build_guardrail_table(duplicated, _cross_checks(duplicated), semantics)

    duplicate = guardrails.set_index("guardrail").loc["duplicate_dwell_identity"]

    assert duplicate["status"] == "fail"
    assert duplicate["issue_count"] == 2


def test_missing_current_must_remain_null_not_zero() -> None:
    frame = _fusion_frame()
    frame.loc[frame["dwell_id"].eq("d2"), "current_speed_mean"] = 0.0
    semantics = build_readiness_semantics_table(frame)
    guardrails = build_guardrail_table(frame, _cross_checks(frame), semantics)

    missing_current = guardrails.set_index("guardrail").loc["missing_current_null_not_zero"]

    assert missing_current["status"] == "fail"
    assert missing_current["issue_count"] == 1


def test_wind_direction_is_quarantined_to_direction_ready_rows() -> None:
    frame = _fusion_frame()
    frame.loc[frame["dwell_id"].eq("d2"), WIND_DIRECTION_COLUMNS] = [0.0, 1.0, 0.0]
    semantics = build_readiness_semantics_table(frame)
    guardrails = build_guardrail_table(frame, _cross_checks(frame), semantics)

    wind_direction = guardrails.set_index("guardrail").loc["wind_direction_quarantined"]

    assert wind_direction["status"] == "fail"


def test_high_confidence_semantics_exclude_depth_warning_rows() -> None:
    frame = _fusion_frame()
    frame.loc[frame["dwell_id"].eq("d2"), "has_current"] = True
    frame.loc[frame["dwell_id"].eq("d2"), "current_confidence_class"] = "A_event_scale"
    frame.loc[frame["dwell_id"].eq("d2"), CURRENT_VALUE_COLUMNS] = [0.1, 0.2, 0.3, 0.4, 0.0, 1.0, 0.0, 5.0]
    frame.loc[frame["dwell_id"].eq("d2"), "model_ready_wave_current"] = True
    frame.loc[frame["dwell_id"].eq("d2"), "model_ready_wave_wind_current"] = True
    frame.loc[frame["dwell_id"].eq("d2"), "model_ready_high_confidence"] = True

    semantics = build_readiness_semantics_table(frame).set_index("readiness_flag")

    assert semantics.loc["model_ready_high_confidence", "mismatch_count"] == 1


def test_recommendation_logic_prioritizes_integrity_then_restrictions() -> None:
    assert choose_recommendation(["duplicate_dwell_identity"], []) == "repair_evidence_first"
    assert choose_recommendation([], ["wind_direction_sensitivity_only"]) == "proceed_with_restrictions"
    assert choose_recommendation([], []) == "proceed_to_stage2"


def test_build_outputs_writes_deterministic_summary(tmp_path: Path) -> None:
    frame = _fusion_frame()
    fusion_path = tmp_path / "fusion.parquet"
    dwell_path = tmp_path / "dwell.parquet"
    wave_path = tmp_path / "wave.parquet"
    wind_path = tmp_path / "wind.parquet"
    current_path = tmp_path / "current.parquet"
    bathy_path = tmp_path / "bathy.parquet"
    report_path = tmp_path / "fusion_report.md"

    frame.to_parquet(fusion_path, index=False)
    frame[["dwell_id"]].to_parquet(dwell_path, index=False)
    frame[["dwell_id"]].to_parquet(wave_path, index=False)
    frame[["dwell_id"]].to_parquet(wind_path, index=False)
    frame[["dwell_id"]].to_parquet(current_path, index=False)
    pd.DataFrame({"wind_farm": ["Farm A", "Farm B"]}).to_parquet(bathy_path, index=False)
    report_path.write_text("- Output rows: 3\n- High-confidence multivariate rows: 1\n", encoding="utf-8")

    result = build_fusion_v2_evidence_readiness(
        fusion_v2_path=fusion_path,
        dwell_weather_path=dwell_path,
        wave_confidence_path=wave_path,
        wind_confidence_path=wind_path,
        current_confidence_path=current_path,
        bathymetry_path=bathy_path,
        fusion_v2_report_path=report_path,
        output_dir=tmp_path / "processed",
        report_dir=tmp_path / "reports",
        run_timestamp_utc="2026-06-04T00:00:00Z",
    )

    assert result.summary["timestamp_utc"] == "2026-06-04T00:00:00Z"
    assert result.summary["final_recommendation"] == "proceed_with_restrictions"
    assert result.files["readiness_summary_json"].exists()
    assert result.files["readiness_report_md"].exists()
    summary = result.files["readiness_summary_json"].read_text(encoding="utf-8")
    assert "2026-06-04T00:00:00Z" in summary
