"""RQ01 restricted Stage 2 workability sensitivity analysis.

This module compares observed Tier A Fusion v2 workability-envelope lanes. It
does not train models, estimate calibrated operation probabilities, rebuild
Fusion v2, download data, import FINO, repair wind/current evidence, or treat
missing current as zero.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from om_pipeline.analysis.fusion_v2_evidence_readiness import CURRENT_VALUE_COLUMNS


ANALYSIS_LABEL = "RQ01 Stage 2 Fusion v2 workability sensitivity"
DEFAULT_FUSION_V2 = Path("Data/Processed/metocean/fusion_v2/dwell_metocean_fusion_v2.parquet")
DEFAULT_READINESS_SUMMARY = Path(
    "Data/Processed/metocean/fusion_v2_evidence_readiness/readiness_summary.json"
)
DEFAULT_OUTPUT_DIR = Path("Data/Processed/analysis/rq01_stage2_workability_sensitivity")
DEFAULT_REPORT_DIR = Path("reports/rq01_stage2_workability_sensitivity")

PRIMARY_WAVE_LANE = "wave_only"
PRIMARY_WIND_LANE = "wave_wind_speed"
CURRENT_LANES = ("wave_current", "wave_wind_current")
PRIMARY_COMPARISON_DEPTH_SCOPES = ("all_tier_a", "depth_warning_excluded")

READINESS_FLAGS = (
    "model_ready_wave_only",
    "model_ready_wave_wind",
    "model_ready_wave_current",
    "model_ready_wave_wind_current",
    "model_ready_high_confidence",
)

WIND_DIRECTION_COLUMNS = (
    "wind_direction_sin_mean",
    "wind_direction_cos_mean",
    "wind_direction_deg_mean",
)

REQUIRED_COLUMNS = (
    "dwell_id",
    "wind_farm",
    "dwell_tier",
    "start_utc",
    "selected_wave_source",
    "selected_hs_mean",
    "selected_tp_mean",
    "has_wind_speed",
    "wind_speed_mean",
    "wind_source",
    "has_current",
    "current_speed_mean",
    "current_source",
    "depth_warning_le_10m",
    *CURRENT_VALUE_COLUMNS,
    *WIND_DIRECTION_COLUMNS,
    *READINESS_FLAGS,
)

MATERIALITY_THRESHOLDS = {
    "retained_share_below": 0.80,
    "hs_p95_abs_delta_m": 0.25,
    "tp_p95_abs_delta_s": 0.50,
    "top_farm_share_abs_delta": 0.10,
}


@dataclass(frozen=True)
class FeatureSpec:
    """Observed-envelope feature and binning metadata."""

    name: str
    column: str
    unit: str
    edges: tuple[float, ...]


@dataclass(frozen=True)
class LaneSpec:
    """One restricted Stage 2 observed-envelope sensitivity lane."""

    lane_id: str
    label: str
    readiness_flag: str
    role: str
    claim_scope: str
    features: tuple[FeatureSpec, ...]


@dataclass(frozen=True)
class RQ01SensitivityOutputs:
    """Paths, summary, and tables written by the RQ01 analysis."""

    processed_output_dir: Path
    report_output_dir: Path
    files: dict[str, Path]
    summary: dict[str, Any]
    tables: dict[str, pd.DataFrame]


HS_FEATURE = FeatureSpec(
    name="hs",
    column="selected_hs_mean",
    unit="m",
    edges=tuple(np.round(np.arange(0.0, 5.25, 0.25), 3)),
)
TP_FEATURE = FeatureSpec(
    name="tp",
    column="selected_tp_mean",
    unit="s",
    edges=tuple(np.round(np.arange(0.0, 21.5, 0.5), 3)),
)
WIND_SPEED_FEATURE = FeatureSpec(
    name="wind_speed",
    column="wind_speed_mean",
    unit="m/s",
    edges=tuple(np.round(np.arange(0.0, 25.5, 2.5), 3)),
)
CURRENT_SPEED_FEATURE = FeatureSpec(
    name="current_speed",
    column="current_speed_mean",
    unit="m/s",
    edges=tuple(np.round(np.arange(0.0, 1.31, 0.1), 3)),
)


LANE_SPECS = (
    LaneSpec(
        lane_id=PRIMARY_WAVE_LANE,
        label="Tier A wave-only observed envelope",
        readiness_flag="model_ready_wave_only",
        role="primary",
        claim_scope="Primary observed-envelope sensitivity lane.",
        features=(HS_FEATURE, TP_FEATURE),
    ),
    LaneSpec(
        lane_id=PRIMARY_WIND_LANE,
        label="Tier A wave plus wind-speed observed envelope",
        readiness_flag="model_ready_wave_wind",
        role="primary",
        claim_scope="Primary observed-envelope sensitivity lane; wind direction excluded.",
        features=(HS_FEATURE, TP_FEATURE, WIND_SPEED_FEATURE),
    ),
    LaneSpec(
        lane_id="wave_current",
        label="Tier A wave plus event-scale current observed envelope",
        readiness_flag="model_ready_wave_current",
        role="restricted_current_sensitivity",
        claim_scope="NWS-domain / coverage-limited current-aware sensitivity only.",
        features=(HS_FEATURE, TP_FEATURE, CURRENT_SPEED_FEATURE),
    ),
    LaneSpec(
        lane_id="wave_wind_current",
        label="Tier A wave plus wind speed plus event-scale current observed envelope",
        readiness_flag="model_ready_wave_wind_current",
        role="restricted_current_sensitivity",
        claim_scope="NWS-domain / coverage-limited current-aware sensitivity only; wind direction excluded.",
        features=(HS_FEATURE, TP_FEATURE, WIND_SPEED_FEATURE, CURRENT_SPEED_FEATURE),
    ),
    LaneSpec(
        lane_id="high_confidence_multivariate",
        label="Tier A high-confidence multivariate observed envelope",
        readiness_flag="model_ready_high_confidence",
        role="restricted_high_confidence_sensitivity",
        claim_scope="High-confidence multivariate sensitivity subset; still not calibrated probability.",
        features=(HS_FEATURE, TP_FEATURE, WIND_SPEED_FEATURE, CURRENT_SPEED_FEATURE),
    ),
)


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


def _now_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _as_bool(series: pd.Series) -> pd.Series:
    return series.fillna(False).astype(bool)


def _share(count: int, total: int) -> float:
    return float(count / total) if total else 0.0


def _read_parquet(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"RQ01 input not found: {path}")
    return pd.read_parquet(path)


def _read_readiness_summary(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def validate_required_columns(fusion: pd.DataFrame) -> None:
    """Raise if Fusion v2 is missing columns required by RQ01."""

    missing = sorted(set(REQUIRED_COLUMNS) - set(fusion.columns))
    if missing:
        raise ValueError(f"Fusion v2 input is missing required RQ01 columns: {missing}")


def _is_tier_a(frame: pd.DataFrame) -> pd.Series:
    return frame["dwell_tier"].astype(str).str.casefold().eq("tier a")


def _source_domain(frame: pd.DataFrame) -> pd.Series:
    wave = frame["selected_wave_source"].fillna("missing_wave").astype(str)
    wind_ready = _as_bool(frame["has_wind_speed"])
    wind = pd.Series(
        np.where(wind_ready, frame["wind_source"].fillna("unknown_wind_source"), "missing_wind_speed"),
        index=frame.index,
    )
    current_ready = _as_bool(frame["has_current"])
    current = pd.Series(
        np.where(current_ready, frame["current_source"].fillna("unknown_current_source"), "missing_current"),
        index=frame.index,
    )
    return "wave=" + wave + "; wind=" + wind.astype(str) + "; current=" + current.astype(str)


def _with_analysis_columns(fusion: pd.DataFrame) -> pd.DataFrame:
    frame = fusion.copy()
    frame["audit_year"] = pd.to_datetime(frame["start_utc"], utc=True, errors="coerce").dt.year
    frame["source_domain"] = _source_domain(frame)
    return frame


def _depth_scope_masks(frame: pd.DataFrame) -> dict[str, pd.Series]:
    depth_warning = _as_bool(frame["depth_warning_le_10m"])
    return {
        "all_tier_a": pd.Series(True, index=frame.index),
        "depth_warning_excluded": ~depth_warning,
        "depth_warning_only": depth_warning,
    }


def _lane_base_mask(frame: pd.DataFrame, lane: LaneSpec) -> pd.Series:
    return _is_tier_a(frame) & _as_bool(frame[lane.readiness_flag])


def _complete_feature_mask(frame: pd.DataFrame, lane: LaneSpec) -> pd.Series:
    columns = [feature.column for feature in lane.features]
    return frame[columns].notna().all(axis=1)


def _top_summary(frame: pd.DataFrame, column: str) -> dict[str, Any]:
    if frame.empty or column not in frame.columns:
        return {
            f"unique_{column}": 0,
            f"top_{column}": None,
            f"top_{column}_count": 0,
            f"top_{column}_share": 0.0,
        }
    counts = frame[column].fillna("missing").astype(str).value_counts(dropna=False)
    total = int(len(frame))
    top_value = counts.index[0] if not counts.empty else None
    top_count = int(counts.iloc[0]) if not counts.empty else 0
    return {
        f"unique_{column}": int(counts.shape[0]),
        f"top_{column}": top_value,
        f"top_{column}_count": top_count,
        f"top_{column}_share": _share(top_count, total),
    }


def _feature_stats(values: pd.Series) -> dict[str, Any]:
    numeric = pd.to_numeric(values, errors="coerce")
    complete = numeric.dropna()
    if complete.empty:
        return {
            "complete_count": 0,
            "missing_count": int(numeric.isna().sum()),
            "min": None,
            "p05": None,
            "p25": None,
            "median": None,
            "p75": None,
            "p95": None,
            "max": None,
        }
    return {
        "complete_count": int(complete.shape[0]),
        "missing_count": int(numeric.isna().sum()),
        "min": float(complete.min()),
        "p05": float(complete.quantile(0.05)),
        "p25": float(complete.quantile(0.25)),
        "median": float(complete.quantile(0.50)),
        "p75": float(complete.quantile(0.75)),
        "p95": float(complete.quantile(0.95)),
        "max": float(complete.max()),
    }


def build_lane_counts(fusion: pd.DataFrame, lanes: tuple[LaneSpec, ...] = LANE_SPECS) -> pd.DataFrame:
    """Count Tier A rows available for each sensitivity lane and depth scope."""

    frame = _with_analysis_columns(fusion)
    depth_masks = _depth_scope_masks(frame)
    rows = []
    for lane in lanes:
        base_mask = _lane_base_mask(frame, lane)
        complete_mask = _complete_feature_mask(frame, lane)
        for depth_scope, depth_mask in depth_masks.items():
            lane_mask = base_mask & depth_mask
            complete_lane_mask = lane_mask & complete_mask
            subset = frame.loc[lane_mask].copy()
            row: dict[str, Any] = {
                "lane_id": lane.lane_id,
                "lane_label": lane.label,
                "lane_role": lane.role,
                "claim_scope": lane.claim_scope,
                "readiness_flag": lane.readiness_flag,
                "depth_scope": depth_scope,
                "feature_columns": "|".join(feature.column for feature in lane.features),
                "tier_a_event_count": int(lane_mask.sum()),
                "complete_feature_count": int(complete_lane_mask.sum()),
                "excluded_null_feature_count": int((lane_mask & ~complete_mask).sum()),
                "depth_warning_le_10m_count": int((lane_mask & _as_bool(frame["depth_warning_le_10m"])).sum()),
                "start_year": int(subset["audit_year"].min()) if not subset.empty and subset["audit_year"].notna().any() else None,
                "end_year": int(subset["audit_year"].max()) if not subset.empty and subset["audit_year"].notna().any() else None,
            }
            row.update(_top_summary(subset, "wind_farm"))
            row.update(_top_summary(subset, "audit_year"))
            row.update(_top_summary(subset, "source_domain"))
            rows.append(row)

    table = pd.DataFrame(rows)
    baseline = table[
        table["lane_id"].eq(PRIMARY_WAVE_LANE) & table["depth_scope"].isin(table["depth_scope"].unique())
    ].set_index("depth_scope")["tier_a_event_count"].to_dict()
    table["retained_share_vs_wave_only"] = table.apply(
        lambda row: _share(int(row["tier_a_event_count"]), int(baseline.get(row["depth_scope"], 0))),
        axis=1,
    )
    return table


def build_lane_feature_summary(
    fusion: pd.DataFrame,
    lanes: tuple[LaneSpec, ...] = LANE_SPECS,
) -> pd.DataFrame:
    """Summarize observed feature ranges for each lane and depth scope."""

    frame = _with_analysis_columns(fusion)
    depth_masks = _depth_scope_masks(frame)
    rows = []
    for lane in lanes:
        base_mask = _lane_base_mask(frame, lane)
        for depth_scope, depth_mask in depth_masks.items():
            lane_mask = base_mask & depth_mask
            subset = frame.loc[lane_mask]
            for feature in lane.features:
                row: dict[str, Any] = {
                    "lane_id": lane.lane_id,
                    "lane_role": lane.role,
                    "depth_scope": depth_scope,
                    "feature": feature.name,
                    "feature_column": feature.column,
                    "unit": feature.unit,
                    "tier_a_event_count": int(lane_mask.sum()),
                }
                row.update(_feature_stats(subset[feature.column]))
                rows.append(row)
    return pd.DataFrame(rows)


def build_binned_occupancy_table(
    fusion: pd.DataFrame,
    lanes: tuple[LaneSpec, ...] = LANE_SPECS,
) -> pd.DataFrame:
    """Build occupied-bin observed-envelope tables for each lane.

    Only occupied bins are emitted. This keeps the output descriptive and avoids
    implying a denominator for unobserved conditions.
    """

    frame = _with_analysis_columns(fusion)
    depth_masks = _depth_scope_masks(frame)
    rows = []
    for lane in lanes:
        base_mask = _lane_base_mask(frame, lane)
        feature_columns = [feature.column for feature in lane.features]
        for depth_scope, depth_mask in depth_masks.items():
            lane_mask = base_mask & depth_mask
            subset = frame.loc[lane_mask, ["dwell_id", *feature_columns]].copy()
            complete = subset.dropna(subset=feature_columns).copy()
            if complete.empty:
                continue

            bin_columns = []
            for feature in lane.features:
                interval_column = f"{feature.name}_interval"
                complete[interval_column] = pd.cut(
                    pd.to_numeric(complete[feature.column], errors="coerce"),
                    bins=list(feature.edges),
                    right=False,
                    include_lowest=True,
                )
                bin_columns.append(interval_column)

            in_range = complete[bin_columns].notna().all(axis=1)
            binned = complete.loc[in_range].copy()
            if binned.empty:
                continue

            grouped = binned.groupby(bin_columns, observed=True, dropna=False).size()
            complete_count = int(len(complete))
            for key, observed_count in grouped.items():
                intervals = key if isinstance(key, tuple) else (key,)
                row: dict[str, Any] = {
                    "lane_id": lane.lane_id,
                    "lane_role": lane.role,
                    "depth_scope": depth_scope,
                    "feature_columns": "|".join(feature_columns),
                    "complete_feature_count": complete_count,
                    "observed_count": int(observed_count),
                    "share_of_lane_complete": _share(int(observed_count), complete_count),
                }
                for feature, interval in zip(lane.features, intervals):
                    row[f"{feature.name}_bin"] = f"{interval.left:g}-{interval.right:g}"
                    row[f"{feature.name}_bin_left"] = float(interval.left)
                    row[f"{feature.name}_bin_right"] = float(interval.right)
                    row[f"{feature.name}_unit"] = feature.unit
                rows.append(row)
    return pd.DataFrame(rows)


def _feature_value(
    feature_summary: pd.DataFrame,
    *,
    lane_id: str,
    depth_scope: str,
    feature: str,
    metric: str,
) -> float | None:
    match = feature_summary[
        feature_summary["lane_id"].eq(lane_id)
        & feature_summary["depth_scope"].eq(depth_scope)
        & feature_summary["feature"].eq(feature)
    ]
    if match.empty:
        return None
    value = match.iloc[0][metric]
    if pd.isna(value):
        return None
    return float(value)


def _count_value(lane_counts: pd.DataFrame, *, lane_id: str, depth_scope: str, column: str) -> Any:
    match = lane_counts[lane_counts["lane_id"].eq(lane_id) & lane_counts["depth_scope"].eq(depth_scope)]
    if match.empty:
        return None
    value = match.iloc[0][column]
    return _jsonable(value)


def _screen_materiality(row: dict[str, Any]) -> tuple[str, list[str]]:
    triggers: list[str] = []
    retained_share = row.get("retained_share_vs_wave_only")
    if retained_share is not None and retained_share < MATERIALITY_THRESHOLDS["retained_share_below"]:
        triggers.append("retained_share_below_threshold")
    for metric, threshold_key in (
        ("hs_p95_delta", "hs_p95_abs_delta_m"),
        ("tp_p95_delta", "tp_p95_abs_delta_s"),
        ("top_farm_share_delta", "top_farm_share_abs_delta"),
    ):
        value = row.get(metric)
        if value is not None and abs(float(value)) >= MATERIALITY_THRESHOLDS[threshold_key]:
            triggers.append(f"{metric}_above_threshold")
    result = "material_difference_screened" if triggers else "no_material_difference_screened"
    return result, triggers


def build_lane_comparisons(
    lane_counts: pd.DataFrame,
    feature_summary: pd.DataFrame,
    lanes: tuple[LaneSpec, ...] = LANE_SPECS,
) -> pd.DataFrame:
    """Compare each lane with the wave-only Tier A baseline."""

    rows = []
    comparison_lanes = [lane for lane in lanes if lane.lane_id != PRIMARY_WAVE_LANE]
    for lane in comparison_lanes:
        for depth_scope in PRIMARY_COMPARISON_DEPTH_SCOPES:
            baseline_count = _count_value(
                lane_counts,
                lane_id=PRIMARY_WAVE_LANE,
                depth_scope=depth_scope,
                column="tier_a_event_count",
            )
            comparison_count = _count_value(
                lane_counts,
                lane_id=lane.lane_id,
                depth_scope=depth_scope,
                column="tier_a_event_count",
            )
            row: dict[str, Any] = {
                "baseline_lane_id": PRIMARY_WAVE_LANE,
                "comparison_lane_id": lane.lane_id,
                "comparison_lane_role": lane.role,
                "depth_scope": depth_scope,
                "claim_scope": lane.claim_scope,
                "baseline_tier_a_event_count": int(baseline_count or 0),
                "comparison_tier_a_event_count": int(comparison_count or 0),
                "retained_share_vs_wave_only": _share(int(comparison_count or 0), int(baseline_count or 0)),
            }
            for feature in ("hs", "tp"):
                for metric in ("p95", "max"):
                    baseline_value = _feature_value(
                        feature_summary,
                        lane_id=PRIMARY_WAVE_LANE,
                        depth_scope=depth_scope,
                        feature=feature,
                        metric=metric,
                    )
                    comparison_value = _feature_value(
                        feature_summary,
                        lane_id=lane.lane_id,
                        depth_scope=depth_scope,
                        feature=feature,
                        metric=metric,
                    )
                    row[f"baseline_{feature}_{metric}"] = baseline_value
                    row[f"comparison_{feature}_{metric}"] = comparison_value
                    row[f"{feature}_{metric}_delta"] = (
                        comparison_value - baseline_value
                        if baseline_value is not None and comparison_value is not None
                        else None
                    )

            baseline_top_farm_share = _count_value(
                lane_counts,
                lane_id=PRIMARY_WAVE_LANE,
                depth_scope=depth_scope,
                column="top_wind_farm_share",
            )
            comparison_top_farm_share = _count_value(
                lane_counts,
                lane_id=lane.lane_id,
                depth_scope=depth_scope,
                column="top_wind_farm_share",
            )
            row["baseline_top_farm_share"] = baseline_top_farm_share
            row["comparison_top_farm_share"] = comparison_top_farm_share
            row["top_farm_share_delta"] = (
                float(comparison_top_farm_share) - float(baseline_top_farm_share)
                if baseline_top_farm_share is not None and comparison_top_farm_share is not None
                else None
            )
            result, triggers = _screen_materiality(row)
            row["materiality_screen_result"] = result
            row["materiality_triggers"] = "|".join(triggers)
            row["screening_note"] = (
                "Descriptive screen only; this is not a calibrated probability, causal effect, or final model."
            )
            rows.append(row)
    return pd.DataFrame(rows)


def build_claim_boundary_checks(
    fusion: pd.DataFrame,
    readiness_summary: dict[str, Any],
    lanes: tuple[LaneSpec, ...] = LANE_SPECS,
) -> pd.DataFrame:
    """Build integrity and claim-boundary checks for the RQ01 branch."""

    duplicate_dwell_rows = int(fusion["dwell_id"].duplicated(keep=False).sum())
    current_missing = ~_as_bool(fusion["has_current"])
    missing_current_non_null = int(fusion.loc[current_missing, CURRENT_VALUE_COLUMNS].notna().any(axis=1).sum())

    feature_columns = {feature.column for lane in lanes for feature in lane.features}
    wind_direction_feature_columns = sorted(feature_columns.intersection(WIND_DIRECTION_COLUMNS))
    lane_counts = build_lane_counts(fusion, lanes)
    lane_lookup = lane_counts[
        lane_counts["depth_scope"].eq("all_tier_a")
    ].set_index("lane_id")["tier_a_event_count"].to_dict()

    readiness_recommendation = readiness_summary.get("final_recommendation")
    readiness_caveats = set(readiness_summary.get("key_caveats", []))
    required_caveats = {
        "partial_event_scale_current_coverage",
        "wind_direction_sensitivity_only",
        "depth_warning_sensitivity_required",
    }

    rows = [
        {
            "check": "required_columns_present",
            "status": "pass",
            "severity": "integrity",
            "issue_count": 0,
            "evidence": "Fusion v2 contains every RQ01 required column.",
        },
        {
            "check": "duplicate_dwell_identity",
            "status": "pass" if duplicate_dwell_rows == 0 else "fail",
            "severity": "integrity",
            "issue_count": duplicate_dwell_rows,
            "evidence": "RQ01 requires unique dwell_id rows.",
        },
        {
            "check": "missing_current_null_not_zero",
            "status": "pass" if missing_current_non_null == 0 else "fail",
            "severity": "integrity",
            "issue_count": missing_current_non_null,
            "evidence": "Rows without event-scale current must retain null current values, not zero-filled values.",
        },
        {
            "check": "wave_only_tier_a_nonzero",
            "status": "pass" if int(lane_lookup.get(PRIMARY_WAVE_LANE, 0)) > 0 else "fail",
            "severity": "integrity",
            "issue_count": 0 if int(lane_lookup.get(PRIMARY_WAVE_LANE, 0)) > 0 else 1,
            "evidence": "Primary wave-only Tier A lane must be non-empty.",
        },
        {
            "check": "wave_wind_speed_tier_a_nonzero",
            "status": "pass" if int(lane_lookup.get(PRIMARY_WIND_LANE, 0)) > 0 else "fail",
            "severity": "integrity",
            "issue_count": 0 if int(lane_lookup.get(PRIMARY_WIND_LANE, 0)) > 0 else 1,
            "evidence": "Primary wave + wind-speed Tier A lane must be non-empty.",
        },
        {
            "check": "current_lanes_nonzero_but_restricted",
            "status": "pass"
            if all(int(lane_lookup.get(lane_id, 0)) > 0 for lane_id in CURRENT_LANES)
            else "fail",
            "severity": "integrity",
            "issue_count": sum(1 for lane_id in CURRENT_LANES if int(lane_lookup.get(lane_id, 0)) == 0),
            "evidence": "Current-aware lanes are available but remain NWS-domain / coverage-limited sensitivity only.",
        },
        {
            "check": "wind_direction_excluded_from_primary_predictors",
            "status": "pass" if not wind_direction_feature_columns else "fail",
            "severity": "claim_boundary",
            "issue_count": len(wind_direction_feature_columns),
            "evidence": "Lane feature columns do not include wind-direction fields.",
        },
        {
            "check": "current_lanes_labelled_restricted",
            "status": "pass"
            if all(
                lane.role == "restricted_current_sensitivity"
                for lane in lanes
                if lane.lane_id in CURRENT_LANES
            )
            else "fail",
            "severity": "claim_boundary",
            "issue_count": 0,
            "evidence": "Current-aware lanes are explicitly labelled restricted sensitivity.",
        },
        {
            "check": "readiness_restrictions_inherited",
            "status": "pass"
            if readiness_recommendation == "proceed_with_restrictions"
            and required_caveats.issubset(readiness_caveats)
            else "fail",
            "severity": "claim_boundary",
            "issue_count": 0 if required_caveats.issubset(readiness_caveats) else len(required_caveats - readiness_caveats),
            "evidence": "RQ01 inherits the Fusion v2 evidence-readiness restricted recommendation and caveats.",
        },
        {
            "check": "depth_warning_sensitivity_present",
            "status": "pass"
            if {"depth_warning_excluded", "depth_warning_only"}.issubset(set(lane_counts["depth_scope"]))
            else "fail",
            "severity": "claim_boundary",
            "issue_count": 0,
            "evidence": "RQ01 emits depth-warning exclusion and sensitivity subsets.",
        },
        {
            "check": "no_calibrated_probability_claim",
            "status": "pass",
            "severity": "claim_boundary",
            "issue_count": 0,
            "evidence": "Outputs are observed-envelope descriptive sensitivity only, not P(operation | weather).",
        },
    ]
    return pd.DataFrame(rows)


def _claim_check_lists(claim_checks: pd.DataFrame) -> tuple[list[str], list[str]]:
    integrity_failures = claim_checks[
        claim_checks["severity"].eq("integrity") & claim_checks["status"].eq("fail")
    ]["check"].astype(str).tolist()
    claim_boundary_failures = claim_checks[
        claim_checks["severity"].eq("claim_boundary") & claim_checks["status"].eq("fail")
    ]["check"].astype(str).tolist()
    return integrity_failures, claim_boundary_failures


def _screening_summary(lane_comparisons: pd.DataFrame) -> dict[str, Any]:
    depth_scope = "depth_warning_excluded"
    selected = lane_comparisons[lane_comparisons["depth_scope"].eq(depth_scope)]
    rows = selected.set_index("comparison_lane_id").to_dict(orient="index")
    return {
        "primary_depth_scope": depth_scope,
        "wave_wind_speed": rows.get(PRIMARY_WIND_LANE, {}),
        "current_sensitive_lanes": {lane_id: rows.get(lane_id, {}) for lane_id in CURRENT_LANES},
        "high_confidence_multivariate": rows.get("high_confidence_multivariate", {}),
    }


def build_summary(
    *,
    run_timestamp_utc: str,
    input_paths: dict[str, Path],
    output_paths: dict[str, Path],
    readiness_summary: dict[str, Any],
    lane_counts: pd.DataFrame,
    lane_comparisons: pd.DataFrame,
    claim_checks: pd.DataFrame,
) -> dict[str, Any]:
    integrity_failures, claim_boundary_failures = _claim_check_lists(claim_checks)
    if integrity_failures:
        analysis_recommendation = "repair_evidence_first"
    elif claim_boundary_failures:
        analysis_recommendation = "fix_claim_boundary_before_review"
    else:
        analysis_recommendation = "restricted_descriptive_sensitivity_ready_for_review"

    lane_count_lookup = lane_counts.set_index(["lane_id", "depth_scope"]).to_dict(orient="index")
    return _jsonable(
        {
            "analysis_label": ANALYSIS_LABEL,
            "timestamp_utc": run_timestamp_utc,
            "analysis_recommendation": analysis_recommendation,
            "research_question": (
                "Do wind speed and event-scale current materially change the observed Tier A "
                "workability envelope relative to wave-only Fusion v1/Fusion v2 evidence?"
            ),
            "claim_boundary": {
                "no_calibrated_probability": True,
                "no_model_training": True,
                "wind_direction_primary_predictor": False,
                "missing_current_interpreted_as_zero": False,
                "current_claim_scope": "NWS-domain / coverage-limited sensitivity only",
                "depth_warning_treatment": "depth-warning excluded and depth-warning-only sensitivity subsets emitted",
            },
            "materiality_thresholds": MATERIALITY_THRESHOLDS,
            "input_paths": input_paths,
            "output_paths": output_paths,
            "readiness_summary": {
                "final_recommendation": readiness_summary.get("final_recommendation"),
                "key_caveats": readiness_summary.get("key_caveats", []),
            },
            "integrity_failures": integrity_failures,
            "claim_boundary_failures": claim_boundary_failures,
            "lane_counts": {
                f"{lane_id}|{depth_scope}": values
                for (lane_id, depth_scope), values in lane_count_lookup.items()
            },
            "screening_summary": _screening_summary(lane_comparisons),
        }
    )


def _markdown_table(frame: pd.DataFrame, columns: list[str] | None = None, limit: int | None = None) -> list[str]:
    if frame.empty:
        return ["No rows."]
    table = frame if columns is None else frame[[column for column in columns if column in frame.columns]]
    if limit is not None:
        table = table.head(limit)
    table = table.copy()
    for column in table.columns:
        if table[column].dtype == object:
            table[column] = table[column].map(
                lambda value: str(value).replace("|", r"\|") if pd.notna(value) else value
            )
    return table.to_markdown(index=False).splitlines()


def render_report(summary: dict[str, Any], tables: dict[str, pd.DataFrame]) -> str:
    lane_counts = tables["lane_counts"]
    lane_comparisons = tables["lane_comparisons"]
    feature_summary = tables["lane_feature_summary"]
    claim_checks = tables["claim_boundary_checks"]

    lines = [
        "# RQ01 Stage 2 Fusion v2 Workability Sensitivity",
        "",
        "## Executive Summary",
        "",
        f"- Analysis recommendation: `{summary['analysis_recommendation']}`",
        f"- Run timestamp UTC: `{summary['timestamp_utc']}`",
        "- Scope: restricted descriptive sensitivity of observed Tier A envelopes.",
        "- This report does not train models, estimate calibrated `P(operation | weather)`, rebuild Fusion v2, download data, import FINO, repair wind/current evidence, or treat missing current as zero.",
        "- Wave-only and wave+wind-speed are primary lanes. Current-aware lanes are NWS-domain / coverage-limited sensitivity only.",
        "- Wind direction is excluded from primary predictors. Depth-warning exclusion and depth-warning-only sensitivity subsets are emitted.",
        "",
        "## Materiality Screen",
        "",
        "The screen is descriptive and threshold-based. It flags sensitivity lanes for review when subset retention, Hs/Tp envelope deltas, or farm concentration shift materially relative to wave-only Tier A evidence.",
        "",
        "- Retained share below wave-only threshold: "
        f"`{summary['materiality_thresholds']['retained_share_below']}`",
        "- Absolute Hs p95 delta threshold: "
        f"`{summary['materiality_thresholds']['hs_p95_abs_delta_m']} m`",
        "- Absolute Tp p95 delta threshold: "
        f"`{summary['materiality_thresholds']['tp_p95_abs_delta_s']} s`",
        "- Absolute top-farm share delta threshold: "
        f"`{summary['materiality_thresholds']['top_farm_share_abs_delta']}`",
        "",
        "## Lane Comparisons Versus Wave-Only",
        "",
    ]
    lines.extend(
        _markdown_table(
            lane_comparisons,
            [
                "comparison_lane_id",
                "comparison_lane_role",
                "depth_scope",
                "baseline_tier_a_event_count",
                "comparison_tier_a_event_count",
                "retained_share_vs_wave_only",
                "hs_p95_delta",
                "tp_p95_delta",
                "top_farm_share_delta",
                "materiality_screen_result",
                "materiality_triggers",
            ],
        )
    )
    lines.extend(["", "## Lane Counts", ""])
    lines.extend(
        _markdown_table(
            lane_counts,
            [
                "lane_id",
                "lane_role",
                "depth_scope",
                "tier_a_event_count",
                "complete_feature_count",
                "excluded_null_feature_count",
                "retained_share_vs_wave_only",
                "top_wind_farm",
                "top_wind_farm_share",
                "top_audit_year",
                "top_audit_year_share",
            ],
        )
    )
    lines.extend(["", "## Feature Envelope Summary", ""])
    lines.extend(
        _markdown_table(
            feature_summary,
            [
                "lane_id",
                "depth_scope",
                "feature",
                "unit",
                "complete_count",
                "min",
                "p25",
                "median",
                "p75",
                "p95",
                "max",
            ],
        )
    )
    lines.extend(["", "## Claim Boundary Checks", ""])
    lines.extend(_markdown_table(claim_checks, ["check", "status", "severity", "issue_count", "evidence"]))
    lines.extend(
        [
            "",
            "## Output Tables",
            "",
            "- `lane_counts`: counts, retention, and concentration by lane and depth scope.",
            "- `lane_feature_summary`: min/quantile/max summaries for observed lane features.",
            "- `binned_occupancy`: occupied bins only; unobserved bins are not denominators.",
            "- `lane_comparisons`: threshold-based descriptive comparison against wave-only Tier A evidence.",
            "- `claim_boundary_checks`: integrity and claim-boundary checks for this restricted analysis.",
            "",
            "## Claim Boundary",
            "",
            "- Observed envelopes are descriptive summaries of observed Tier A dwell conditions.",
            "- No calibrated operation-success probability or causal weather effect is claimed.",
            "- Current-aware comparisons are restricted to NWS-domain / coverage-limited sensitivity.",
            "- Wind direction remains excluded from primary predictors.",
            "- Missing current remains null/missing and is never coerced to zero.",
        ]
    )
    return "\n".join(lines) + "\n"


def _table_files(output_dir: Path) -> dict[str, Path]:
    stems = [
        "lane_counts",
        "lane_feature_summary",
        "binned_occupancy",
        "lane_comparisons",
        "claim_boundary_checks",
    ]
    files: dict[str, Path] = {}
    for stem in stems:
        files[f"{stem}_csv"] = output_dir / f"{stem}.csv"
        files[f"{stem}_parquet"] = output_dir / f"{stem}.parquet"
    return files


def write_outputs(
    *,
    output_dir: Path,
    report_dir: Path,
    summary: dict[str, Any],
    tables: dict[str, pd.DataFrame],
) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    report_dir.mkdir(parents=True, exist_ok=True)
    files = _table_files(output_dir)
    files["sensitivity_summary_json"] = output_dir / "sensitivity_summary.json"
    files["sensitivity_report_md"] = report_dir / "sensitivity_report.md"

    for table_name, frame in tables.items():
        csv_key = f"{table_name}_csv"
        parquet_key = f"{table_name}_parquet"
        if csv_key in files:
            frame.to_csv(files[csv_key], index=False)
        if parquet_key in files:
            frame.to_parquet(files[parquet_key], index=False)

    files["sensitivity_summary_json"].write_text(
        json.dumps(_jsonable(summary), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    files["sensitivity_report_md"].write_text(render_report(summary, tables), encoding="utf-8")
    return files


def build_rq01_stage2_workability_sensitivity(
    *,
    fusion_v2_path: Path = DEFAULT_FUSION_V2,
    readiness_summary_path: Path = DEFAULT_READINESS_SUMMARY,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    report_dir: Path = DEFAULT_REPORT_DIR,
    run_timestamp_utc: str | None = None,
) -> RQ01SensitivityOutputs:
    """Build restricted RQ01 observed-envelope sensitivity outputs."""

    run_timestamp_utc = run_timestamp_utc or _now_utc()
    fusion = _read_parquet(fusion_v2_path)
    validate_required_columns(fusion)
    readiness_summary = _read_readiness_summary(readiness_summary_path)

    lane_counts = build_lane_counts(fusion)
    lane_feature_summary = build_lane_feature_summary(fusion)
    binned_occupancy = build_binned_occupancy_table(fusion)
    lane_comparisons = build_lane_comparisons(lane_counts, lane_feature_summary)
    claim_checks = build_claim_boundary_checks(fusion, readiness_summary)

    tables = {
        "lane_counts": lane_counts,
        "lane_feature_summary": lane_feature_summary,
        "binned_occupancy": binned_occupancy,
        "lane_comparisons": lane_comparisons,
        "claim_boundary_checks": claim_checks,
    }
    input_paths = {
        "fusion_v2": fusion_v2_path,
        "readiness_summary": readiness_summary_path,
    }
    output_paths = {
        "processed_output_dir": output_dir,
        "report_output_dir": report_dir,
        "sensitivity_summary_json": output_dir / "sensitivity_summary.json",
        "sensitivity_report_md": report_dir / "sensitivity_report.md",
    }
    summary = build_summary(
        run_timestamp_utc=run_timestamp_utc,
        input_paths=input_paths,
        output_paths=output_paths,
        readiness_summary=readiness_summary,
        lane_counts=lane_counts,
        lane_comparisons=lane_comparisons,
        claim_checks=claim_checks,
    )
    files = write_outputs(output_dir=output_dir, report_dir=report_dir, summary=summary, tables=tables)
    return RQ01SensitivityOutputs(
        processed_output_dir=output_dir,
        report_output_dir=report_dir,
        files=files,
        summary=summary,
        tables=tables,
    )
