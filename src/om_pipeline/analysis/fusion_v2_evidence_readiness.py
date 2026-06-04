"""Fusion v2 evidence-readiness audit.

This module audits the accepted Fusion v2 event feature table before Stage 2
modelling starts. It summarizes readiness, coverage bias, and claim boundaries;
it does not rebuild Fusion v2 joins, train models, compare observed envelopes,
download data, import FINO, or mutate accepted evidence products.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from om_pipeline.metocean.metocean_fusion_v2 import (
    CURRENT_READY_CLASS,
    DEFAULT_BATHYMETRY,
    DEFAULT_CURRENT_CONFIDENCE,
    DEFAULT_DWELL_WEATHER,
    DEFAULT_WAVE_CONFIDENCE,
    DEFAULT_WIND_CONFIDENCE,
    WIND_DIRECTION_READY_CLASS,
    WIND_SPEED_READY_CLASSES,
)


ANALYSIS_LABEL = "Fusion v2 evidence-readiness audit"
DEFAULT_FUSION_V2 = Path("Data/Processed/metocean/fusion_v2/dwell_metocean_fusion_v2.parquet")
DEFAULT_FUSION_V2_REPORT = Path("reports/metocean_fusion_v2/fusion_v2_validation_report.md")
DEFAULT_OUTPUT_DIR = Path("Data/Processed/metocean/fusion_v2_evidence_readiness")
DEFAULT_REPORT_DIR = Path("reports/fusion_v2_evidence_readiness")

READINESS_FLAGS = [
    "model_ready_wave_only",
    "model_ready_wave_wind",
    "model_ready_wave_current",
    "model_ready_wave_wind_current",
    "model_ready_high_confidence",
]

CURRENT_VALUE_COLUMNS = [
    "current_u_mean",
    "current_v_mean",
    "current_speed_mean",
    "current_speed_p95",
    "current_direction_to_sin_mean",
    "current_direction_to_cos_mean",
    "current_direction_to_deg_mean",
    "current_depth_m",
]

WIND_DIRECTION_COLUMNS = [
    "wind_direction_sin_mean",
    "wind_direction_cos_mean",
    "wind_direction_deg_mean",
]

MISSINGNESS_FIELDS = [
    "selected_hs_mean",
    "selected_tp_mean",
    "wind_speed_mean",
    "wind_direction_deg_mean",
    "current_u_mean",
    "current_v_mean",
    "current_speed_mean",
    "water_depth_m",
]

REQUIRED_COLUMNS = [
    "dwell_id",
    "wind_farm",
    "dwell_tier",
    "start_utc",
    "selected_wave_source",
    "wave_confidence_class",
    "has_wind_speed",
    "has_wind_direction",
    "wind_confidence_class",
    "wind_source",
    *WIND_DIRECTION_COLUMNS,
    "has_event_scale_current",
    "current_confidence_class",
    "current_source",
    *CURRENT_VALUE_COLUMNS,
    "depth_warning_le_10m",
    "has_wave",
    "has_current",
    "has_bathymetry",
    *READINESS_FLAGS,
    "metocean_feature_class",
    "metocean_missing_reason",
]

REPORT_COUNT_MAP = {
    "Output rows": "all_events",
    "Wave rows": "has_wave",
    "Wind speed rows": "has_wind_speed",
    "Wind direction rows": "has_wind_direction",
    "Current rows": "has_current",
    "Wave + wind speed + current + bathymetry rows": "model_ready_wave_wind_current",
    "High-confidence multivariate rows": "model_ready_high_confidence",
    "Tier A total": "tier_a_total",
    "Tier A with wave + wind + current": "tier_a_model_ready_wave_wind_current",
    "High-confidence Tier A subset": "tier_a_model_ready_high_confidence",
}


@dataclass(frozen=True)
class FusionV2EvidenceReadinessOutputs:
    """Paths, summary, and tables written by the readiness audit."""

    processed_output_dir: Path
    report_output_dir: Path
    files: dict[str, Path]
    summary: dict[str, Any]
    tables: dict[str, pd.DataFrame]


def _jsonable(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        return float(value)
    if isinstance(value, np.bool_):
        return bool(value)
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return value


def _as_bool(series: pd.Series) -> pd.Series:
    return series.fillna(False).astype(bool)


def _share(count: int, total: int) -> float:
    return float(count / total) if total else 0.0


def _now_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _read_parquet(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Readiness audit input not found: {path}")
    return pd.read_parquet(path)


def validate_required_columns(fusion: pd.DataFrame) -> None:
    """Raise if Fusion v2 is missing required audit columns."""
    missing = sorted(set(REQUIRED_COLUMNS) - set(fusion.columns))
    if missing:
        raise ValueError(f"Fusion v2 input is missing required columns: {missing}")


def _is_tier_a(frame: pd.DataFrame) -> pd.Series:
    return frame["dwell_tier"].astype(str).str.casefold().eq("tier a")


def _with_audit_columns(fusion: pd.DataFrame) -> pd.DataFrame:
    result = fusion.copy()
    result["audit_year"] = pd.to_datetime(result["start_utc"], utc=True, errors="coerce").dt.year
    result["source_domain"] = _source_domain(result)
    return result


def _source_domain(frame: pd.DataFrame) -> pd.Series:
    wave = frame["selected_wave_source"].fillna("missing_wave").astype(str)
    wind_ready = _as_bool(frame["has_wind_speed"])
    wind_source = frame["wind_source"].fillna("unknown_wind_source").astype(str)
    wind = pd.Series(
        np.where(wind_ready, wind_source, "missing_wind_speed"),
        index=frame.index,
    )
    current_ready = _as_bool(frame["has_current"])
    current_source = frame["current_source"].fillna("unknown_current_source").astype(str)
    current = pd.Series(
        np.where(current_ready, current_source, "missing_current"),
        index=frame.index,
    )
    return "wave=" + wave + "; wind=" + wind + "; current=" + current


def readiness_subset_masks(fusion: pd.DataFrame) -> dict[str, pd.Series]:
    """Return canonical readiness and guardrail indicator masks."""
    return {
        "all_events": pd.Series(True, index=fusion.index),
        "has_wave": _as_bool(fusion["has_wave"]),
        "has_wind_speed": _as_bool(fusion["has_wind_speed"]),
        "has_wind_direction": _as_bool(fusion["has_wind_direction"]),
        "has_current": _as_bool(fusion["has_current"]),
        "has_bathymetry": _as_bool(fusion["has_bathymetry"]),
        **{flag: _as_bool(fusion[flag]) for flag in READINESS_FLAGS},
    }


def build_readiness_subset_counts(fusion: pd.DataFrame) -> pd.DataFrame:
    masks = readiness_subset_masks(fusion)
    total = len(fusion)
    rows = []
    for subset, mask in masks.items():
        count = int(mask.sum())
        rows.append(
            {
                "subset": subset,
                "source_column": subset if subset in fusion.columns else "",
                "subset_type": "canonical_readiness_flag" if subset in READINESS_FLAGS else "guardrail_indicator",
                "event_count": count,
                "share_of_all": _share(count, total),
            }
        )
    return pd.DataFrame(rows)


def build_tier_a_readiness_counts(fusion: pd.DataFrame) -> pd.DataFrame:
    masks = readiness_subset_masks(fusion)
    tier_a = _is_tier_a(fusion)
    tier_total = int(tier_a.sum())
    rows = []
    for subset, mask in masks.items():
        count = int((tier_a & mask).sum())
        rows.append(
            {
                "subset": subset,
                "tier_a_event_count": count,
                "share_of_tier_a": _share(count, tier_total),
            }
        )
    return pd.DataFrame(rows)


def _top_summary(frame: pd.DataFrame, column: str) -> dict[str, Any]:
    if frame.empty or column not in frame.columns:
        return {
            f"unique_{column}": 0,
            f"top_{column}": None,
            f"top_{column}_count": 0,
            f"top_{column}_share": 0.0,
            f"top_5_{column}_count": 0,
            f"top_5_{column}_share": 0.0,
        }
    counts = frame[column].fillna("missing").astype(str).value_counts(dropna=False)
    total = int(len(frame))
    top_value = counts.index[0] if not counts.empty else None
    top_count = int(counts.iloc[0]) if not counts.empty else 0
    top_5_count = int(counts.head(5).sum())
    return {
        f"unique_{column}": int(counts.shape[0]),
        f"top_{column}": top_value,
        f"top_{column}_count": top_count,
        f"top_{column}_share": _share(top_count, total),
        f"top_5_{column}_count": top_5_count,
        f"top_5_{column}_share": _share(top_5_count, total),
    }


def build_concentration_table(fusion: pd.DataFrame) -> pd.DataFrame:
    frame = _with_audit_columns(fusion)
    masks = readiness_subset_masks(frame)
    subsets = ["all_events", *READINESS_FLAGS]
    rows = []
    for subset in subsets:
        subset_frame = frame.loc[masks[subset]].copy()
        row: dict[str, Any] = {"subset": subset, "event_count": int(len(subset_frame))}
        row.update(_top_summary(subset_frame, "wind_farm"))
        row.update(_top_summary(subset_frame, "audit_year"))
        row.update(_top_summary(subset_frame, "source_domain"))
        rows.append(row)
    return pd.DataFrame(rows)


def build_current_bias_table(fusion: pd.DataFrame) -> pd.DataFrame:
    frame = _with_audit_columns(fusion)
    current_ready = _as_bool(frame["has_current"])
    groups = {
        "current_ready": current_ready,
        "current_missing": ~current_ready,
    }
    total = len(frame)
    tier_a = _is_tier_a(frame)
    rows = []
    for group_name, mask in groups.items():
        subset_frame = frame.loc[mask].copy()
        row: dict[str, Any] = {
            "current_group": group_name,
            "event_count": int(mask.sum()),
            "share_of_all": _share(int(mask.sum()), total),
            "tier_a_event_count": int((mask & tier_a).sum()),
        }
        row.update(_top_summary(subset_frame, "wind_farm"))
        row.update(_top_summary(subset_frame, "audit_year"))
        row.update(_top_summary(subset_frame, "source_domain"))
        rows.append(row)
    return pd.DataFrame(rows)


def build_confidence_distribution(fusion: pd.DataFrame) -> pd.DataFrame:
    rows = []
    tier_a = _is_tier_a(fusion)
    for variable, column in (
        ("wave", "wave_confidence_class"),
        ("wind", "wind_confidence_class"),
        ("current", "current_confidence_class"),
    ):
        counts = fusion[column].fillna("missing").astype(str).value_counts(dropna=False)
        for confidence_class, count in counts.items():
            class_mask = fusion[column].fillna("missing").astype(str).eq(str(confidence_class))
            rows.append(
                {
                    "variable": variable,
                    "confidence_class": confidence_class,
                    "event_count": int(count),
                    "share_of_all": _share(int(count), len(fusion)),
                    "tier_a_event_count": int((class_mask & tier_a).sum()),
                }
            )
    return pd.DataFrame(rows)


def build_missingness_summary(fusion: pd.DataFrame) -> pd.DataFrame:
    rows = []
    total = len(fusion)
    for field in MISSINGNESS_FIELDS:
        missing = int(fusion[field].isna().sum()) if field in fusion.columns else total
        zero = 0
        if field in fusion.columns and pd.api.types.is_numeric_dtype(fusion[field]):
            zero = int(fusion[field].eq(0).sum())
        rows.append(
            {
                "field": field,
                "missing_count": missing,
                "missing_share": _share(missing, total),
                "zero_count": zero,
            }
        )
    return pd.DataFrame(rows)


def build_missing_reason_counts(fusion: pd.DataFrame) -> pd.DataFrame:
    counts = fusion["metocean_missing_reason"].fillna("missing").astype(str).value_counts(dropna=False)
    return counts.rename_axis("metocean_missing_reason").reset_index(name="event_count")


def build_depth_sensitivity_table(fusion: pd.DataFrame) -> pd.DataFrame:
    masks = readiness_subset_masks(fusion)
    depth_warning = _as_bool(fusion["depth_warning_le_10m"])
    tier_a = _is_tier_a(fusion)
    rows = []
    for subset in ["all_events", *READINESS_FLAGS]:
        mask = masks[subset]
        count = int(mask.sum())
        warning_count = int((mask & depth_warning).sum())
        after_excluding = int((mask & ~depth_warning).sum())
        rows.append(
            {
                "subset": subset,
                "event_count": count,
                "depth_warning_le_10m_count": warning_count,
                "depth_warning_le_10m_share": _share(warning_count, count),
                "after_excluding_depth_warning_count": after_excluding,
                "tier_a_event_count": int((mask & tier_a).sum()),
                "tier_a_after_excluding_depth_warning_count": int((mask & tier_a & ~depth_warning).sum()),
            }
        )
    return pd.DataFrame(rows)


def build_input_cross_check_table(
    fusion: pd.DataFrame,
    input_frames: dict[str, pd.DataFrame],
    input_paths: dict[str, Path],
) -> pd.DataFrame:
    rows = []
    fusion_ids = fusion["dwell_id"].astype(str)
    fusion_id_set = set(fusion_ids)
    for name, frame in input_frames.items():
        row: dict[str, Any] = {
            "input_name": name,
            "input_path": str(input_paths[name]),
            "rows": int(len(frame)),
            "row_count_matches_fusion": bool(len(frame) == len(fusion)),
            "has_dwell_id": bool("dwell_id" in frame.columns),
            "duplicate_dwell_id_rows": None,
            "dwell_id_order_matches_fusion": None,
            "dwell_id_set_matches_fusion": None,
        }
        if "dwell_id" in frame.columns:
            ids = frame["dwell_id"].astype(str)
            row["duplicate_dwell_id_rows"] = int(ids.duplicated(keep=False).sum())
            row["dwell_id_order_matches_fusion"] = bool(
                len(ids) == len(fusion_ids) and ids.tolist() == fusion_ids.tolist()
            )
            row["dwell_id_set_matches_fusion"] = bool(set(ids) == fusion_id_set)
        rows.append(row)
    return pd.DataFrame(rows)


def _read_cross_check_inputs(paths: dict[str, Path]) -> dict[str, pd.DataFrame]:
    return {name: _read_parquet(path) for name, path in paths.items()}


def build_readiness_semantics_table(fusion: pd.DataFrame) -> pd.DataFrame:
    expected = {
        "model_ready_wave_only": _as_bool(fusion["has_wave"]) & _as_bool(fusion["has_bathymetry"]),
        "model_ready_wave_wind": (
            _as_bool(fusion["has_wave"]) & _as_bool(fusion["has_wind_speed"]) & _as_bool(fusion["has_bathymetry"])
        ),
        "model_ready_wave_current": (
            _as_bool(fusion["has_wave"]) & _as_bool(fusion["has_current"]) & _as_bool(fusion["has_bathymetry"])
        ),
        "model_ready_wave_wind_current": (
            _as_bool(fusion["has_wave"])
            & _as_bool(fusion["has_wind_speed"])
            & _as_bool(fusion["has_current"])
            & _as_bool(fusion["has_bathymetry"])
        ),
    }
    expected["model_ready_high_confidence"] = (
        expected["model_ready_wave_wind_current"]
        & fusion["wave_confidence_class"].eq("A_high")
        & fusion["wind_confidence_class"].isin(WIND_SPEED_READY_CLASSES)
        & fusion["current_confidence_class"].eq(CURRENT_READY_CLASS)
        & ~_as_bool(fusion["depth_warning_le_10m"])
    )
    rows = []
    for flag in READINESS_FLAGS:
        actual = _as_bool(fusion[flag])
        mismatch = actual.ne(expected[flag])
        rows.append(
            {
                "readiness_flag": flag,
                "actual_true_count": int(actual.sum()),
                "expected_true_count": int(expected[flag].sum()),
                "mismatch_count": int(mismatch.sum()),
            }
        )
    return pd.DataFrame(rows)


def build_guardrail_table(
    fusion: pd.DataFrame,
    input_cross_checks: pd.DataFrame,
    semantics: pd.DataFrame,
) -> pd.DataFrame:
    current_missing = ~_as_bool(fusion["has_current"])
    missing_current_non_null = fusion.loc[current_missing, CURRENT_VALUE_COLUMNS].notna().any(axis=1)
    missing_current_zero = fusion.loc[current_missing, CURRENT_VALUE_COLUMNS].eq(0).any(axis=1)

    direction_ready = fusion["wind_confidence_class"].eq(WIND_DIRECTION_READY_CLASS)
    wind_direction_flag_mismatch = _as_bool(fusion["has_wind_direction"]).ne(direction_ready)
    wind_direction_outside_ready = fusion.loc[~direction_ready, WIND_DIRECTION_COLUMNS].notna().any(axis=1)

    duplicate_dwell_rows = int(fusion["dwell_id"].duplicated(keep=False).sum())
    dwell_row = input_cross_checks[input_cross_checks["input_name"].eq("dwell_weather")]
    row_identity_preserved = bool(dwell_row["dwell_id_order_matches_fusion"].iloc[0]) if not dwell_row.empty else False

    subset_counts = build_readiness_subset_counts(fusion).set_index("subset")["event_count"].to_dict()
    tier_counts = build_tier_a_readiness_counts(fusion).set_index("subset")["tier_a_event_count"].to_dict()

    rows = [
        {
            "guardrail": "required_columns_present",
            "status": "pass",
            "issue_count": 0,
            "evidence": "Fusion v2 has every column required by the readiness audit.",
            "severity": "integrity",
        },
        {
            "guardrail": "dwell_row_identity_preserved",
            "status": "pass" if row_identity_preserved else "fail",
            "issue_count": 0 if row_identity_preserved else 1,
            "evidence": "Fusion v2 dwell_id order matches the accepted dwell-weather input.",
            "severity": "integrity",
        },
        {
            "guardrail": "duplicate_dwell_identity",
            "status": "pass" if duplicate_dwell_rows == 0 else "fail",
            "issue_count": duplicate_dwell_rows,
            "evidence": "Fusion v2 should contain no duplicate dwell_id rows.",
            "severity": "integrity",
        },
        {
            "guardrail": "readiness_flag_semantics",
            "status": "pass" if int(semantics["mismatch_count"].sum()) == 0 else "fail",
            "issue_count": int(semantics["mismatch_count"].sum()),
            "evidence": "Canonical readiness flags match column masks from ADR 0029.",
            "severity": "integrity",
        },
        {
            "guardrail": "missing_current_null_not_zero",
            "status": "pass" if int(missing_current_non_null.sum()) == 0 else "fail",
            "issue_count": int(missing_current_non_null.sum()),
            "evidence": f"Missing-current zero-like rows: {int(missing_current_zero.sum())}.",
            "severity": "integrity",
        },
        {
            "guardrail": "wind_direction_quarantined",
            "status": "pass"
            if int(wind_direction_flag_mismatch.sum()) == 0 and int(wind_direction_outside_ready.sum()) == 0
            else "fail",
            "issue_count": int(wind_direction_flag_mismatch.sum() + wind_direction_outside_ready.sum()),
            "evidence": "Wind direction is usable only for A_speed_direction rows.",
            "severity": "integrity",
        },
        {
            "guardrail": "high_confidence_multivariate_nonzero",
            "status": "pass" if int(subset_counts["model_ready_high_confidence"]) > 0 else "fail",
            "issue_count": 0 if int(subset_counts["model_ready_high_confidence"]) > 0 else 1,
            "evidence": "High-confidence multivariate subset must be non-empty.",
            "severity": "integrity",
        },
        {
            "guardrail": "tier_a_high_confidence_nonzero",
            "status": "pass" if int(tier_counts["model_ready_high_confidence"]) > 0 else "fail",
            "issue_count": 0 if int(tier_counts["model_ready_high_confidence"]) > 0 else 1,
            "evidence": "Tier A high-confidence multivariate subset must be non-empty.",
            "severity": "integrity",
        },
        {
            "guardrail": "partial_event_scale_current_coverage",
            "status": "caveat" if int((~_as_bool(fusion["has_current"])).sum()) > 0 else "pass",
            "issue_count": int((~_as_bool(fusion["has_current"])).sum()),
            "evidence": "Missing current remains missing; current-aware claims require restrictions.",
            "severity": "restriction",
        },
        {
            "guardrail": "wind_direction_sensitivity_only",
            "status": "caveat"
            if int(_as_bool(fusion["has_wind_direction"]).sum()) < int(_as_bool(fusion["has_wind_speed"]).sum())
            else "pass",
            "issue_count": int(_as_bool(fusion["has_wind_speed"]).sum() - _as_bool(fusion["has_wind_direction"]).sum()),
            "evidence": "Wind direction is much narrower than wind-speed-ready evidence.",
            "severity": "restriction",
        },
        {
            "guardrail": "depth_warning_sensitivity_required",
            "status": "caveat" if int(_as_bool(fusion["depth_warning_le_10m"]).sum()) > 0 else "pass",
            "issue_count": int(_as_bool(fusion["depth_warning_le_10m"]).sum()),
            "evidence": "Rows with <=10 m depth warnings require exclusion or sensitivity treatment.",
            "severity": "restriction",
        },
        {
            "guardrail": "fusion_v2_claim_boundary",
            "status": "pass",
            "issue_count": 0,
            "evidence": "Audit is report-only readiness; it makes no calibrated access-probability claim.",
            "severity": "claim_boundary",
        },
        {
            "guardrail": "fino_not_imported",
            "status": "pass",
            "issue_count": 0,
            "evidence": "Audit does not read FINO observations.",
            "severity": "claim_boundary",
        },
        {
            "guardrail": "baltic_daily_current_not_promoted",
            "status": "pass",
            "issue_count": 0,
            "evidence": "Audit uses Fusion v2 event-scale current flags only.",
            "severity": "claim_boundary",
        },
    ]
    return pd.DataFrame(rows)


def choose_recommendation(integrity_failures: list[str], restriction_caveats: list[str]) -> str:
    if integrity_failures:
        return "repair_evidence_first"
    if restriction_caveats:
        return "proceed_with_restrictions"
    return "proceed_to_stage2"


def _guardrail_lists(guardrails: pd.DataFrame) -> tuple[list[str], list[str]]:
    integrity = guardrails[
        guardrails["severity"].eq("integrity") & guardrails["status"].eq("fail")
    ]["guardrail"].astype(str).tolist()
    restrictions = guardrails[
        guardrails["severity"].eq("restriction") & guardrails["status"].eq("caveat")
    ]["guardrail"].astype(str).tolist()
    return integrity, restrictions


def parse_validation_report_counts(report_path: Path) -> dict[str, int]:
    if not report_path.exists():
        return {}
    counts: dict[str, int] = {}
    pattern = re.compile(r"^- (?P<label>[^:]+): (?P<count>[0-9][0-9,]*)\s*$")
    for line in report_path.read_text(encoding="utf-8").splitlines():
        match = pattern.match(line.strip())
        if match:
            counts[match.group("label")] = int(match.group("count").replace(",", ""))
    return counts


def build_report_count_cross_check(
    report_path: Path,
    subset_counts: pd.DataFrame,
    tier_counts: pd.DataFrame,
) -> pd.DataFrame:
    report_counts = parse_validation_report_counts(report_path)
    subset_lookup = subset_counts.set_index("subset")["event_count"].to_dict()
    tier_lookup = tier_counts.set_index("subset")["tier_a_event_count"].to_dict()
    audit_counts = {
        "all_events": int(subset_lookup.get("all_events", 0)),
        "has_wave": int(subset_lookup.get("has_wave", 0)),
        "has_wind_speed": int(subset_lookup.get("has_wind_speed", 0)),
        "has_wind_direction": int(subset_lookup.get("has_wind_direction", 0)),
        "has_current": int(subset_lookup.get("has_current", 0)),
        "model_ready_wave_wind_current": int(subset_lookup.get("model_ready_wave_wind_current", 0)),
        "model_ready_high_confidence": int(subset_lookup.get("model_ready_high_confidence", 0)),
        "tier_a_total": int(tier_lookup.get("all_events", 0)),
        "tier_a_model_ready_wave_wind_current": int(
            tier_lookup.get("model_ready_wave_wind_current", 0)
        ),
        "tier_a_model_ready_high_confidence": int(tier_lookup.get("model_ready_high_confidence", 0)),
    }
    rows = []
    for label, audit_key in REPORT_COUNT_MAP.items():
        if label not in report_counts:
            continue
        report_count = report_counts[label]
        audit_count = audit_counts[audit_key]
        rows.append(
            {
                "report_label": label,
                "audit_key": audit_key,
                "report_count": report_count,
                "audit_count": audit_count,
                "matches": bool(report_count == audit_count),
            }
        )
    return pd.DataFrame(rows)


def build_summary(
    *,
    run_timestamp_utc: str,
    input_paths: dict[str, Path],
    output_paths: dict[str, Path],
    subset_counts: pd.DataFrame,
    tier_counts: pd.DataFrame,
    concentration: pd.DataFrame,
    current_bias: pd.DataFrame,
    depth_sensitivity: pd.DataFrame,
    guardrails: pd.DataFrame,
    report_count_cross_check: pd.DataFrame,
) -> dict[str, Any]:
    integrity_failures, restriction_caveats = _guardrail_lists(guardrails)
    recommendation = choose_recommendation(integrity_failures, restriction_caveats)

    concentration_lookup = concentration.set_index("subset").to_dict(orient="index")
    current_lookup = current_bias.set_index("current_group").to_dict(orient="index")
    depth_lookup = depth_sensitivity.set_index("subset").to_dict(orient="index")

    return _jsonable(
        {
            "analysis_label": ANALYSIS_LABEL,
            "final_recommendation": recommendation,
            "timestamp_utc": run_timestamp_utc,
            "input_paths": input_paths,
            "output_paths": output_paths,
            "key_caveats": [*integrity_failures, *restriction_caveats],
            "integrity_failures": integrity_failures,
            "restriction_caveats": restriction_caveats,
            "readiness_subset_counts": subset_counts.set_index("subset").to_dict(orient="index"),
            "tier_a_subset_counts": tier_counts.set_index("subset").to_dict(orient="index"),
            "top_farm_concentration": {
                subset: {
                    "top_farm": values.get("top_wind_farm"),
                    "top_farm_count": values.get("top_wind_farm_count"),
                    "top_farm_share": values.get("top_wind_farm_share"),
                    "top_5_farm_count": values.get("top_5_wind_farm_count"),
                    "top_5_farm_share": values.get("top_5_wind_farm_share"),
                }
                for subset, values in concentration_lookup.items()
            },
            "top_5_farm_concentration": {
                subset: values.get("top_5_wind_farm_share") for subset, values in concentration_lookup.items()
            },
            "year_concentration": {
                subset: {
                    "top_year": values.get("top_audit_year"),
                    "top_year_count": values.get("top_audit_year_count"),
                    "top_year_share": values.get("top_audit_year_share"),
                }
                for subset, values in concentration_lookup.items()
            },
            "current_ready_versus_current_missing_summary": current_lookup,
            "wind_direction_quarantine_result": guardrails[
                guardrails["guardrail"].eq("wind_direction_quarantined")
            ].to_dict(orient="records"),
            "depth_warning_sensitivity_result": depth_lookup,
            "report_count_cross_check": report_count_cross_check.to_dict(orient="records"),
        }
    )


def _markdown_table(frame: pd.DataFrame, columns: list[str] | None = None, limit: int | None = None) -> list[str]:
    if frame.empty:
        return ["No rows."]
    table = frame if columns is None else frame[[column for column in columns if column in frame.columns]]
    if limit is not None:
        table = table.head(limit)
    return table.to_markdown(index=False).splitlines()


def render_readiness_report(summary: dict[str, Any], tables: dict[str, pd.DataFrame]) -> str:
    subset_counts = tables["readiness_subset_counts"]
    tier_counts = tables["tier_a_readiness_counts"]
    concentration = tables["concentration"]
    current_bias = tables["current_bias"]
    confidence = tables["confidence_distribution"]
    missingness = tables["missingness_summary"]
    depth = tables["depth_sensitivity"]
    guardrails = tables["guardrails"]
    report_counts = tables["report_count_cross_check"]

    lines = [
        "# Fusion v2 Evidence-Readiness Report",
        "",
        "## Executive Summary",
        "",
        f"- Final recommendation: `{summary['final_recommendation']}`",
        f"- Run timestamp UTC: `{summary['timestamp_utc']}`",
        "- This is a report-only readiness audit. It does not train models, compare envelopes, rebuild Fusion v2, download data, import FINO, repair NORA3, repair wind direction, or expand current stress-test farm-years.",
        "- Fusion v2 remains a provisional source-resolved event feature layer, not a calibrated `P(operation | weather)` model.",
        "",
        "## Key Caveats",
        "",
    ]
    caveats = summary.get("key_caveats", [])
    if caveats:
        lines.extend([f"- `{caveat}`" for caveat in caveats])
    else:
        lines.append("- No readiness caveats were triggered.")

    lines.extend(["", "## Readiness Subset Counts", ""])
    lines.extend(_markdown_table(subset_counts, ["subset", "subset_type", "event_count", "share_of_all"]))
    lines.extend(["", "## Tier A Readiness Counts", ""])
    lines.extend(_markdown_table(tier_counts, ["subset", "tier_a_event_count", "share_of_tier_a"]))
    lines.extend(["", "## Concentration Diagnostics", ""])
    lines.extend(
        _markdown_table(
            concentration,
            [
                "subset",
                "event_count",
                "top_wind_farm",
                "top_wind_farm_share",
                "top_5_wind_farm_share",
                "top_audit_year",
                "top_audit_year_share",
                "top_source_domain",
                "top_source_domain_share",
            ],
        )
    )
    lines.extend(["", "## Current-Ready Versus Current-Missing Bias", ""])
    lines.extend(
        _markdown_table(
            current_bias,
            [
                "current_group",
                "event_count",
                "share_of_all",
                "tier_a_event_count",
                "top_wind_farm",
                "top_wind_farm_share",
                "top_audit_year",
                "top_audit_year_share",
                "top_source_domain",
            ],
        )
    )
    lines.extend(["", "## Confidence-Class Distribution", ""])
    lines.extend(_markdown_table(confidence, ["variable", "confidence_class", "event_count", "share_of_all"]))
    lines.extend(["", "## Missingness", ""])
    lines.extend(_markdown_table(missingness, ["field", "missing_count", "missing_share", "zero_count"]))
    lines.extend(["", "## Depth-Warning Sensitivity", ""])
    lines.extend(
        _markdown_table(
            depth,
            [
                "subset",
                "event_count",
                "depth_warning_le_10m_count",
                "depth_warning_le_10m_share",
                "after_excluding_depth_warning_count",
                "tier_a_after_excluding_depth_warning_count",
            ],
        )
    )
    lines.extend(["", "## Guardrail Checks", ""])
    lines.extend(_markdown_table(guardrails, ["guardrail", "status", "issue_count", "severity", "evidence"]))
    lines.extend(["", "## Fusion v2 Report Count Cross-Checks", ""])
    if report_counts.empty:
        lines.append("No Fusion v2 validation-report counts were parsed for cross-checking.")
    else:
        lines.extend(_markdown_table(report_counts, ["report_label", "report_count", "audit_count", "matches"]))
    lines.extend(
        [
            "",
            "## Claim Boundary",
            "",
            "- Missing current remains missing/null and must not be interpreted as zero current.",
            "- Wind direction remains sensitivity-only and excluded from primary Stage 2 predictors unless a later repair increment is accepted.",
            "- Baltic daily/contextual current is not promoted to event-scale current evidence.",
            "- FINO remains validation/planning only and is not imported here.",
            "- Any later Stage 2 work should review this recommendation before merge.",
        ]
    )
    return "\n".join(lines) + "\n"


def _table_files(output_dir: Path) -> dict[str, Path]:
    stems = [
        "readiness_subset_counts",
        "tier_a_readiness_counts",
        "concentration",
        "current_bias",
        "confidence_distribution",
        "missingness_summary",
        "missing_reason_counts",
        "depth_sensitivity",
        "input_cross_checks",
        "readiness_semantics",
        "guardrails",
        "report_count_cross_check",
    ]
    files: dict[str, Path] = {}
    for stem in stems:
        files[f"{stem}_csv"] = output_dir / f"{stem}.csv"
        files[f"{stem}_parquet"] = output_dir / f"{stem}.parquet"
    return files


def write_readiness_outputs(
    *,
    output_dir: Path,
    report_dir: Path,
    summary: dict[str, Any],
    tables: dict[str, pd.DataFrame],
    overwrite: bool = False,
) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    report_dir.mkdir(parents=True, exist_ok=True)
    files = _table_files(output_dir)
    files["readiness_summary_json"] = output_dir / "readiness_summary.json"
    files["readiness_report_md"] = report_dir / "readiness_report.md"

    for table_name, frame in tables.items():
        csv_key = f"{table_name}_csv"
        parquet_key = f"{table_name}_parquet"
        if csv_key in files:
            frame.to_csv(files[csv_key], index=False)
        if parquet_key in files:
            frame.to_parquet(files[parquet_key], index=False)

    files["readiness_summary_json"].write_text(
        json.dumps(_jsonable(summary), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    files["readiness_report_md"].write_text(
        render_readiness_report(summary, tables),
        encoding="utf-8",
    )
    return files


def build_fusion_v2_evidence_readiness(
    *,
    fusion_v2_path: Path = DEFAULT_FUSION_V2,
    dwell_weather_path: Path = DEFAULT_DWELL_WEATHER,
    wave_confidence_path: Path = DEFAULT_WAVE_CONFIDENCE,
    wind_confidence_path: Path = DEFAULT_WIND_CONFIDENCE,
    current_confidence_path: Path = DEFAULT_CURRENT_CONFIDENCE,
    bathymetry_path: Path = DEFAULT_BATHYMETRY,
    fusion_v2_report_path: Path = DEFAULT_FUSION_V2_REPORT,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    report_dir: Path = DEFAULT_REPORT_DIR,
    overwrite: bool = False,
    run_timestamp_utc: str | None = None,
) -> FusionV2EvidenceReadinessOutputs:
    """Build Fusion v2 evidence-readiness report-only outputs."""
    run_timestamp_utc = run_timestamp_utc or _now_utc()
    fusion = _read_parquet(fusion_v2_path)
    validate_required_columns(fusion)

    input_paths = {
        "fusion_v2": fusion_v2_path,
        "dwell_weather": dwell_weather_path,
        "wave_confidence": wave_confidence_path,
        "wind_confidence": wind_confidence_path,
        "current_confidence": current_confidence_path,
        "bathymetry": bathymetry_path,
        "fusion_v2_validation_report": fusion_v2_report_path,
    }
    cross_check_paths = {
        "dwell_weather": dwell_weather_path,
        "wave_confidence": wave_confidence_path,
        "wind_confidence": wind_confidence_path,
        "current_confidence": current_confidence_path,
        "bathymetry": bathymetry_path,
    }
    input_frames = _read_cross_check_inputs(cross_check_paths)

    subset_counts = build_readiness_subset_counts(fusion)
    tier_counts = build_tier_a_readiness_counts(fusion)
    concentration = build_concentration_table(fusion)
    current_bias = build_current_bias_table(fusion)
    confidence = build_confidence_distribution(fusion)
    missingness = build_missingness_summary(fusion)
    missing_reasons = build_missing_reason_counts(fusion)
    depth = build_depth_sensitivity_table(fusion)
    input_cross_checks = build_input_cross_check_table(fusion, input_frames, cross_check_paths)
    semantics = build_readiness_semantics_table(fusion)
    guardrails = build_guardrail_table(fusion, input_cross_checks, semantics)
    report_count_cross_check = build_report_count_cross_check(
        fusion_v2_report_path,
        subset_counts,
        tier_counts,
    )

    tables = {
        "readiness_subset_counts": subset_counts,
        "tier_a_readiness_counts": tier_counts,
        "concentration": concentration,
        "current_bias": current_bias,
        "confidence_distribution": confidence,
        "missingness_summary": missingness,
        "missing_reason_counts": missing_reasons,
        "depth_sensitivity": depth,
        "input_cross_checks": input_cross_checks,
        "readiness_semantics": semantics,
        "guardrails": guardrails,
        "report_count_cross_check": report_count_cross_check,
    }

    output_paths = {
        "processed_output_dir": output_dir,
        "report_output_dir": report_dir,
        "readiness_summary_json": output_dir / "readiness_summary.json",
        "readiness_report_md": report_dir / "readiness_report.md",
    }
    summary = build_summary(
        run_timestamp_utc=run_timestamp_utc,
        input_paths=input_paths,
        output_paths=output_paths,
        subset_counts=subset_counts,
        tier_counts=tier_counts,
        concentration=concentration,
        current_bias=current_bias,
        depth_sensitivity=depth,
        guardrails=guardrails,
        report_count_cross_check=report_count_cross_check,
    )
    files = write_readiness_outputs(
        output_dir=output_dir,
        report_dir=report_dir,
        summary=summary,
        tables=tables,
        overwrite=overwrite,
    )
    return FusionV2EvidenceReadinessOutputs(
        processed_output_dir=output_dir,
        report_output_dir=report_dir,
        files=files,
        summary=summary,
        tables=tables,
    )
