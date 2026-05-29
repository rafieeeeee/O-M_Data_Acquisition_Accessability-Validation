"""Metocean Fusion v2: source-resolved multi-parameter event features.

Fusion v2 combines the accepted wave, wind, current, and bathymetry evidence
layers into a single event feature table for modelling sensitivity. It is not a
new source-fusion algorithm and does not create calibrated access
probabilities. Confidence and provenance remain separate by variable group.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow.parquet as pq


DEFAULT_DWELL_WEATHER = Path(
    "Data/Processed/ais_dwell_backfill/cross_farm_dwell_weather_features.parquet"
)
DEFAULT_WAVE_CONFIDENCE = Path(
    "Data/Processed/metocean/fusion_v1_source_agreement/wave_event_confidence.parquet"
)
DEFAULT_WIND_CONFIDENCE = Path(
    "Data/Processed/metocean/wind_confidence_v1/wind_event_confidence.parquet"
)
DEFAULT_CURRENT_CONFIDENCE = Path(
    "Data/Processed/metocean/current_confidence_v1/current_event_confidence.parquet"
)
DEFAULT_BATHYMETRY = Path("Data/Processed/metocean/bathymetry/site_bathymetry_points.parquet")
DEFAULT_OUTPUT_DIR = Path("Data/Processed/metocean/fusion_v2")
DEFAULT_REPORT_DIR = Path("reports/metocean_fusion_v2")

FUSION_V2_FILENAME = "dwell_metocean_fusion_v2.parquet"
VALIDATION_REPORT_FILENAME = "fusion_v2_validation_report.md"

CURRENT_DIRECTION_CONVENTION = "flow_to_degrees_clockwise_from_true_north"
WIND_SPEED_READY_CLASSES = {"A_speed_direction", "B_speed_only"}
WIND_DIRECTION_READY_CLASS = "A_speed_direction"
CURRENT_READY_CLASS = "A_event_scale"
WAVE_READY_EXCLUDED_CLASSES = {"D_unsuitable"}


def haversine_distance(
    lat1: float,
    lon1: float,
    lat2: np.ndarray,
    lon2: np.ndarray,
) -> np.ndarray:
    """Return great-circle distance in kilometres for scalar-to-array points."""
    radius_km = 6371.0088
    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = np.radians(lat2)
    lon2_rad = np.radians(lon2)
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad
    a = np.sin(dlat / 2.0) ** 2 + math.cos(lat1_rad) * np.cos(lat2_rad) * np.sin(dlon / 2.0) ** 2
    return 2.0 * radius_km * np.arcsin(np.sqrt(a))

IDENTITY_COLUMNS = [
    "dwell_id",
    "visit_id",
    "wind_farm",
    "farm_id",
    "dwell_tier",
    "start_utc",
    "end_utc",
    "duration_minutes",
]

FUSION_V2_COLUMNS = [
    *IDENTITY_COLUMNS,
    "selected_wave_source",
    "selected_hs_mean",
    "selected_tp_mean",
    "selected_wave_direction_sin_mean",
    "selected_wave_direction_cos_mean",
    "wave_confidence_class",
    "wave_confidence_score",
    "source_disagreement_hs_range",
    "source_disagreement_tp_range",
    "wave_selection_reason",
    "has_wind_speed",
    "has_wind_direction",
    "wind_confidence_class",
    "wind_confidence_score",
    "wind_source",
    "wind_speed_mean",
    "wind_speed_p95",
    "wind_direction_sin_mean",
    "wind_direction_cos_mean",
    "wind_direction_deg_mean",
    "wind_direction_convention",
    "wind_height_m",
    "wind_missing_reason",
    "has_event_scale_current",
    "current_confidence_class",
    "current_confidence_score",
    "current_source",
    "current_u_mean",
    "current_v_mean",
    "current_speed_mean",
    "current_speed_p95",
    "current_direction_to_sin_mean",
    "current_direction_to_cos_mean",
    "current_direction_to_deg_mean",
    "current_direction_convention",
    "current_depth_m",
    "event_window_sample_count",
    "nearest_time_gap_minutes",
    "current_missing_reason",
    "water_depth_m",
    "bathymetry_source",
    "bathymetry_distance_m",
    "depth_warning_le_1m",
    "depth_warning_le_5m",
    "depth_warning_le_10m",
    "bathymetry_spatial_match_status",
    "has_wave",
    "has_current",
    "has_bathymetry",
    "model_ready_wave_only",
    "model_ready_wave_wind",
    "model_ready_wave_current",
    "model_ready_wave_wind_current",
    "model_ready_high_confidence",
    "metocean_feature_class",
    "metocean_missing_reason",
]

DWELL_COLUMNS = [
    "dwell_id",
    "visit_id",
    "wind_farm",
    "farm_id",
    "dwell_tier",
    "start_utc",
    "end_utc",
    "duration_min",
    "centroid_lat",
    "centroid_lon",
]

WAVE_COLUMNS = [
    "dwell_id",
    "visit_id",
    "selected_wave_source",
    "selected_hs_mean",
    "selected_tp_mean",
    "selected_wave_direction_sin_mean",
    "selected_wave_direction_cos_mean",
    "wave_confidence_score",
    "wave_confidence_class",
    "source_disagreement_hs_range",
    "source_disagreement_tp_range",
    "selection_reason",
]

WIND_COLUMNS = [
    "dwell_id",
    "visit_id",
    "has_wind_speed",
    "has_wind_direction",
    "wind_confidence_class",
    "wind_confidence_score",
    "wind_source",
    "wind_speed_mean",
    "wind_speed_p95",
    "wind_direction_sin_mean",
    "wind_direction_cos_mean",
    "wind_direction_deg_mean",
    "wind_height_m",
    "wind_missing_reason",
]

WIND_CANDIDATE_COLUMNS = [
    "dwell_id",
    "visit_id",
    "wind_direction_convention",
]

CURRENT_COLUMNS = [
    "dwell_id",
    "visit_id",
    "has_event_scale_current",
    "current_confidence_class",
    "current_confidence_score",
    "current_source",
    "current_u_mean",
    "current_v_mean",
    "current_speed_mean",
    "current_direction_to_sin_mean",
    "current_direction_to_cos_mean",
    "current_depth_m",
    "event_window_sample_count",
    "nearest_time_gap_minutes",
    "current_missing_reason",
]

CURRENT_CANDIDATE_COLUMNS = [
    "dwell_id",
    "visit_id",
    "current_speed_p95",
    "current_direction_to_deg_mean",
    "current_direction_convention",
]

BATHYMETRY_COLUMNS = [
    "wind_farm",
    "sample_point_id",
    "sample_point_type",
    "lat",
    "lon",
    "water_depth_m",
    "bathymetry_source",
    "bathymetry_distance_m",
    "bathymetry_spatial_match_status",
]


@dataclass(frozen=True)
class FusionV2Result:
    output_path: Path
    report_path: Path
    output: pd.DataFrame
    validation: dict[str, Any]


def _read_parquet_columns(path: Path, columns: list[str]) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Required parquet file not found: {path}")
    schema = pq.read_schema(path).names
    available = [column for column in columns if column in schema]
    frame = pd.read_parquet(path, columns=available)
    for column in columns:
        if column not in frame.columns:
            frame[column] = pd.NA
    return frame[columns].copy()


def _normalise_name(value: Any) -> str:
    if pd.isna(value):
        return ""
    return (
        str(value)
        .strip()
        .casefold()
        .replace("&", "and")
        .replace("/", "_")
        .replace("-", "_")
        .replace(" ", "_")
    )


def _direction_from_sin_cos(sin_value: Any, cos_value: Any) -> float:
    if pd.isna(sin_value) or pd.isna(cos_value):
        return math.nan
    try:
        sin_num = float(sin_value)
        cos_num = float(cos_value)
    except (TypeError, ValueError):
        return math.nan
    if not np.isfinite(sin_num) or not np.isfinite(cos_num):
        return math.nan
    if math.sqrt(sin_num**2 + cos_num**2) <= 0.0:
        return math.nan
    return float((math.degrees(math.atan2(sin_num, cos_num)) + 360.0) % 360.0)


def _duration_minutes(start: pd.Series, end: pd.Series, fallback: pd.Series) -> pd.Series:
    duration = pd.to_numeric(fallback, errors="coerce")
    computed = (pd.to_datetime(end, utc=True, errors="coerce") - pd.to_datetime(start, utc=True, errors="coerce"))
    computed_minutes = computed.dt.total_seconds() / 60.0
    return duration.where(duration.notna(), computed_minutes)


def _validate_unique_dwell_id(frame: pd.DataFrame, label: str) -> int:
    if "dwell_id" not in frame.columns:
        raise ValueError(f"{label} is missing dwell_id.")
    duplicates = int(frame["dwell_id"].duplicated(keep=False).sum())
    if duplicates:
        raise ValueError(f"{label} contains duplicate dwell_id rows: {duplicates}")
    return duplicates


def ensure_fusion_v2_schema(frame: pd.DataFrame) -> pd.DataFrame:
    for column in FUSION_V2_COLUMNS:
        if column not in frame.columns:
            frame[column] = pd.NA
    return frame[FUSION_V2_COLUMNS].copy()


def load_dwell_events(path: Path) -> pd.DataFrame:
    dwell = _read_parquet_columns(path, DWELL_COLUMNS)
    _validate_unique_dwell_id(dwell, "dwell-weather table")
    dwell["start_utc"] = pd.to_datetime(dwell["start_utc"], utc=True, errors="coerce")
    dwell["end_utc"] = pd.to_datetime(dwell["end_utc"], utc=True, errors="coerce")
    dwell["duration_minutes"] = _duration_minutes(dwell["start_utc"], dwell["end_utc"], dwell["duration_min"])
    dwell["_row_id"] = np.arange(len(dwell), dtype=np.int64)
    return dwell


def load_wave_confidence(path: Path) -> pd.DataFrame:
    wave = _read_parquet_columns(path, WAVE_COLUMNS)
    _validate_unique_dwell_id(wave, "wave confidence table")
    return wave.rename(columns={"selection_reason": "wave_selection_reason"})


def load_wind_confidence(path: Path) -> pd.DataFrame:
    wind = _read_parquet_columns(path, WIND_COLUMNS)
    _validate_unique_dwell_id(wind, "wind confidence table")
    candidate_path = path.with_name("wind_event_candidates.parquet")
    if candidate_path.exists():
        convention = _read_parquet_columns(candidate_path, WIND_CANDIDATE_COLUMNS)
        _validate_unique_dwell_id(convention, "wind candidate table")
        wind = wind.merge(convention, on=["dwell_id", "visit_id"], how="left", validate="one_to_one")
    else:
        wind["wind_direction_convention"] = pd.NA
    return wind


def load_current_confidence(path: Path) -> pd.DataFrame:
    current = _read_parquet_columns(path, CURRENT_COLUMNS)
    _validate_unique_dwell_id(current, "current confidence table")
    candidate_path = path.with_name("current_event_candidates.parquet")
    if candidate_path.exists():
        candidate = _read_parquet_columns(candidate_path, CURRENT_CANDIDATE_COLUMNS)
        _validate_unique_dwell_id(candidate, "current candidate table")
        current = current.merge(candidate, on=["dwell_id", "visit_id"], how="left", validate="one_to_one")
    else:
        current["current_speed_p95"] = pd.NA
        current["current_direction_to_deg_mean"] = pd.NA
        current["current_direction_convention"] = CURRENT_DIRECTION_CONVENTION
    current["current_direction_convention"] = current["current_direction_convention"].fillna(CURRENT_DIRECTION_CONVENTION)
    missing_deg = current["current_direction_to_deg_mean"].isna()
    if missing_deg.any():
        derived = current.loc[missing_deg].apply(
            lambda row: _direction_from_sin_cos(
                row.get("current_direction_to_sin_mean"),
                row.get("current_direction_to_cos_mean"),
            ),
            axis=1,
        )
        current.loc[missing_deg, "current_direction_to_deg_mean"] = derived
    return current


def load_bathymetry(path: Path) -> pd.DataFrame:
    bathymetry = _read_parquet_columns(path, BATHYMETRY_COLUMNS)
    bathymetry["water_depth_m"] = pd.to_numeric(bathymetry["water_depth_m"], errors="coerce")
    bathymetry["_norm_wind_farm"] = bathymetry["wind_farm"].map(_normalise_name)
    return bathymetry


def _nearest_bathymetry_point(event: pd.Series, points: pd.DataFrame) -> pd.Series | None:
    if points.empty:
        return None
    lat = event.get("centroid_lat")
    lon = event.get("centroid_lon")
    if pd.isna(lat) or pd.isna(lon):
        centroid = points[points["sample_point_id"].astype(str).eq("farm_centroid")]
        return centroid.iloc[0] if not centroid.empty else points.iloc[0]
    distances = haversine_distance(
        float(lat),
        float(lon),
        points["lat"].astype(float).to_numpy(),
        points["lon"].astype(float).to_numpy(),
    )
    return points.iloc[int(np.nanargmin(distances))]


def assign_bathymetry_to_events(dwell: pd.DataFrame, bathymetry: pd.DataFrame) -> pd.DataFrame:
    by_farm = {
        farm: group.reset_index(drop=True)
        for farm, group in bathymetry.groupby("_norm_wind_farm", dropna=True, sort=False)
    }
    rows: list[dict[str, Any]] = []
    for _, event in dwell.iterrows():
        points = pd.DataFrame()
        for key in (event.get("wind_farm"), event.get("farm_id")):
            norm = _normalise_name(key)
            if norm in by_farm:
                points = by_farm[norm]
                break
        point = _nearest_bathymetry_point(event, points)
        if point is None:
            water_depth = math.nan
            rows.append(
                {
                    "dwell_id": event["dwell_id"],
                    "visit_id": event["visit_id"],
                    "water_depth_m": pd.NA,
                    "bathymetry_source": pd.NA,
                    "bathymetry_distance_m": pd.NA,
                    "bathymetry_spatial_match_status": "missing_farm",
                    "depth_warning_le_1m": False,
                    "depth_warning_le_5m": False,
                    "depth_warning_le_10m": False,
                }
            )
            continue
        water_depth = float(point["water_depth_m"]) if pd.notna(point["water_depth_m"]) else math.nan
        rows.append(
            {
                "dwell_id": event["dwell_id"],
                "visit_id": event["visit_id"],
                "water_depth_m": water_depth if np.isfinite(water_depth) else pd.NA,
                "bathymetry_source": point.get("bathymetry_source"),
                "bathymetry_distance_m": point.get("bathymetry_distance_m"),
                "bathymetry_spatial_match_status": point.get("bathymetry_spatial_match_status"),
                "depth_warning_le_1m": bool(np.isfinite(water_depth) and water_depth <= 1.0),
                "depth_warning_le_5m": bool(np.isfinite(water_depth) and water_depth <= 5.0),
                "depth_warning_le_10m": bool(np.isfinite(water_depth) and water_depth <= 10.0),
            }
        )
    return pd.DataFrame(rows)


def _merge_layer(base: pd.DataFrame, layer: pd.DataFrame, label: str) -> pd.DataFrame:
    before = len(base)
    merged = base.merge(layer, on=["dwell_id", "visit_id"], how="left", validate="one_to_one")
    if len(merged) != before:
        raise ValueError(f"{label} join changed row count from {before} to {len(merged)}")
    return merged


def _as_bool(series: pd.Series) -> pd.Series:
    return series.fillna(False).astype(bool)


def _build_missing_reason(row: pd.Series) -> str:
    reasons: list[str] = []
    if not bool(row["has_wave"]):
        reasons.append("missing_wave")
    if not bool(row["has_wind_speed"]):
        reason = row.get("wind_missing_reason")
        reasons.append(str(reason) if pd.notna(reason) else "missing_wind_speed")
    if not bool(row["has_current"]):
        reason = row.get("current_missing_reason")
        reasons.append(str(reason) if pd.notna(reason) else "missing_event_scale_current")
    if not bool(row["has_bathymetry"]):
        reasons.append("missing_bathymetry")
    if bool(row.get("depth_warning_le_10m", False)):
        reasons.append("depth_warning_le_10m")
    return "none" if not reasons else "; ".join(dict.fromkeys(reasons))


def _feature_class(row: pd.Series) -> str:
    has_wave = bool(row["has_wave"])
    has_wind = bool(row["has_wind_speed"])
    has_current = bool(row["has_current"])
    has_bathy = bool(row["has_bathymetry"])
    if has_wave and has_wind and has_current and has_bathy:
        if bool(row["model_ready_high_confidence"]):
            return "wave_wind_current_bathymetry_high_confidence"
        return "wave_wind_current_bathymetry_mixed_confidence"
    if has_wave and has_wind and has_bathy and not has_current:
        return "wave_wind_bathymetry_no_current"
    if has_wave and has_current and has_bathy and not has_wind:
        return "wave_current_bathymetry_no_wind"
    if has_wave and has_bathy:
        return "wave_bathymetry_only"
    return "insufficient_metocean"


def _apply_modelling_masks(output: pd.DataFrame) -> pd.DataFrame:
    result = output.copy()
    result["has_wave"] = (
        result["selected_hs_mean"].notna()
        & result["selected_tp_mean"].notna()
        & ~result["wave_confidence_class"].isin(WAVE_READY_EXCLUDED_CLASSES)
    )
    result["has_wind_speed"] = (
        result["wind_confidence_class"].isin(WIND_SPEED_READY_CLASSES)
        & result["wind_speed_mean"].notna()
    )
    result["has_wind_direction"] = (
        result["wind_confidence_class"].eq(WIND_DIRECTION_READY_CLASS)
        & result["wind_direction_deg_mean"].notna()
    )
    result["has_current"] = (
        result["current_confidence_class"].eq(CURRENT_READY_CLASS)
        & result["current_u_mean"].notna()
        & result["current_v_mean"].notna()
        & result["current_speed_mean"].notna()
    )
    result["has_event_scale_current"] = result["has_current"]
    result["has_bathymetry"] = result["water_depth_m"].notna()

    wind_direction_cols = [
        "wind_direction_sin_mean",
        "wind_direction_cos_mean",
        "wind_direction_deg_mean",
    ]
    result.loc[~result["has_wind_direction"], wind_direction_cols] = pd.NA
    result.loc[~result["has_wind_speed"], ["wind_speed_mean", "wind_speed_p95"]] = pd.NA

    current_value_cols = [
        "current_u_mean",
        "current_v_mean",
        "current_speed_mean",
        "current_speed_p95",
        "current_direction_to_sin_mean",
        "current_direction_to_cos_mean",
        "current_direction_to_deg_mean",
        "current_depth_m",
    ]
    result.loc[~result["has_current"], current_value_cols] = pd.NA

    result["model_ready_wave_only"] = result["has_wave"] & result["has_bathymetry"]
    result["model_ready_wave_wind"] = result["has_wave"] & result["has_wind_speed"] & result["has_bathymetry"]
    result["model_ready_wave_current"] = result["has_wave"] & result["has_current"] & result["has_bathymetry"]
    result["model_ready_wave_wind_current"] = (
        result["has_wave"] & result["has_wind_speed"] & result["has_current"] & result["has_bathymetry"]
    )
    result["model_ready_high_confidence"] = (
        result["model_ready_wave_wind_current"]
        & result["wave_confidence_class"].eq("A_high")
        & result["wind_confidence_class"].isin(WIND_SPEED_READY_CLASSES)
        & result["current_confidence_class"].eq(CURRENT_READY_CLASS)
        & ~_as_bool(result["depth_warning_le_10m"])
    )
    result["metocean_feature_class"] = result.apply(_feature_class, axis=1)
    result["metocean_missing_reason"] = result.apply(_build_missing_reason, axis=1)
    return result


def build_fusion_v2_table(
    dwell: pd.DataFrame,
    wave: pd.DataFrame,
    wind: pd.DataFrame,
    current: pd.DataFrame,
    bathymetry: pd.DataFrame,
) -> pd.DataFrame:
    base = dwell[
        [
            "dwell_id",
            "visit_id",
            "wind_farm",
            "farm_id",
            "dwell_tier",
            "start_utc",
            "end_utc",
            "duration_minutes",
            "centroid_lat",
            "centroid_lon",
            "_row_id",
        ]
    ].copy()
    bathy_events = assign_bathymetry_to_events(dwell, bathymetry)
    output = _merge_layer(base, wave, "wave confidence")
    output = _merge_layer(output, wind, "wind confidence")
    output = _merge_layer(output, current, "current confidence")
    output = _merge_layer(output, bathy_events, "bathymetry assignment")
    output = output.rename(columns={"event_window_sample_count": "event_window_sample_count"})
    output = _apply_modelling_masks(output)
    output = output.sort_values("_row_id").reset_index(drop=True)
    return ensure_fusion_v2_schema(output)


def _is_tier_a(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str).str.casefold().str.contains("a")


def _quantiles(series: pd.Series) -> dict[str, float]:
    values = pd.to_numeric(series, errors="coerce").dropna()
    if values.empty:
        return {"min": math.nan, "p50": math.nan, "p95": math.nan, "max": math.nan}
    return {
        "min": float(values.min()),
        "p50": float(values.quantile(0.50)),
        "p95": float(values.quantile(0.95)),
        "max": float(values.max()),
    }


def _format_value(value: Any, digits: int = 3) -> str:
    if isinstance(value, (bool, np.bool_)):
        return str(bool(value))
    try:
        if pd.isna(value):
            return "NA"
        if isinstance(value, (int, np.integer)):
            return str(int(value))
        if isinstance(value, (float, np.floating)):
            return f"{float(value):.{digits}f}"
    except (TypeError, ValueError):
        pass
    return str(value).replace("|", "\\|")


def _markdown_table(frame: pd.DataFrame, columns: list[str], limit: int | None = None) -> list[str]:
    if frame.empty:
        return ["No rows."]
    subset = frame[columns].copy()
    if limit is not None:
        subset = subset.head(limit)
    lines = [
        "| " + " | ".join(columns) + " |",
        "| " + " | ".join(["---"] * len(columns)) + " |",
    ]
    for _, row in subset.iterrows():
        lines.append("| " + " | ".join(_format_value(row[column]) for column in columns) + " |")
    if limit is not None and len(frame) > limit:
        lines.append(f"| ... | {len(frame) - limit} more rows omitted |" + " |" * max(0, len(columns) - 2))
    return lines


def _count_table(frame: pd.DataFrame, group_cols: list[str], value_name: str = "event_count") -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=group_cols + [value_name])
    return (
        frame.groupby(group_cols, dropna=False)
        .size()
        .reset_index(name=value_name)
        .sort_values(value_name, ascending=False)
    )


def _interaction_counts(output: pd.DataFrame) -> tuple[dict[str, Any], pd.DataFrame]:
    hs_p95 = float(output.loc[output["has_wave"], "selected_hs_mean"].quantile(0.95))
    hs_p50 = float(output.loc[output["has_wave"], "selected_hs_mean"].quantile(0.50))
    wind_p95 = float(output.loc[output["has_wind_speed"], "wind_speed_mean"].quantile(0.95))
    current_p95 = float(output.loc[output["has_current"], "current_speed_mean"].quantile(0.95))
    cases = {
        "high_hs_high_wind": output["has_wave"]
        & output["has_wind_speed"]
        & (output["selected_hs_mean"] >= hs_p95)
        & (output["wind_speed_mean"] >= wind_p95),
        "high_hs_high_current": output["has_wave"]
        & output["has_current"]
        & (output["selected_hs_mean"] >= hs_p95)
        & (output["current_speed_mean"] >= current_p95),
        "low_hs_high_current": output["has_wave"]
        & output["has_current"]
        & (output["selected_hs_mean"] <= hs_p50)
        & (output["current_speed_mean"] >= current_p95),
        "high_wind_low_wave": output["has_wave"]
        & output["has_wind_speed"]
        & (output["selected_hs_mean"] <= hs_p50)
        & (output["wind_speed_mean"] >= wind_p95),
    }
    rows = []
    for label, mask in cases.items():
        subset = output[mask]
        rows.append(
            {
                "interaction_case": label,
                "event_count": int(len(subset)),
                "tier_a_count": int(_is_tier_a(subset["dwell_tier"]).sum()) if not subset.empty else 0,
            }
        )
    examples = []
    for label, mask in cases.items():
        subset = output[mask & _is_tier_a(output["dwell_tier"])].head(5)
        for _, row in subset.iterrows():
            examples.append(
                {
                    "interaction_case": label,
                    "dwell_id": row["dwell_id"],
                    "wind_farm": row["wind_farm"],
                    "dwell_tier": row["dwell_tier"],
                    "selected_hs_mean": row["selected_hs_mean"],
                    "selected_tp_mean": row["selected_tp_mean"],
                    "wind_speed_mean": row["wind_speed_mean"],
                    "current_speed_mean": row["current_speed_mean"],
                }
            )
    diagnostics = {
        "thresholds": {
            "hs_p95": hs_p95,
            "hs_p50": hs_p50,
            "wind_speed_p95": wind_p95,
            "current_speed_p95": current_p95,
        },
        "counts": pd.DataFrame(rows),
    }
    return diagnostics, pd.DataFrame(examples)


def validate_fusion_v2(
    dwell: pd.DataFrame,
    wave: pd.DataFrame,
    wind: pd.DataFrame,
    current: pd.DataFrame,
    bathymetry: pd.DataFrame,
    output: pd.DataFrame,
) -> dict[str, Any]:
    tier_a = _is_tier_a(output["dwell_tier"])
    wave_wind_crosstab = pd.crosstab(output["wave_confidence_class"], output["wind_confidence_class"])
    wave_current_crosstab = pd.crosstab(output["wave_confidence_class"], output["current_confidence_class"])
    wind_current_crosstab = pd.crosstab(output["wind_confidence_class"], output["current_confidence_class"])
    triple_summary = _count_table(
        output,
        ["wave_confidence_class", "wind_confidence_class", "current_confidence_class"],
    )
    interaction, interaction_examples = _interaction_counts(output)
    coverage = {
        "wave_rows": int(output["has_wave"].sum()),
        "wind_speed_rows": int(output["has_wind_speed"].sum()),
        "wind_direction_rows": int(output["has_wind_direction"].sum()),
        "current_rows": int(output["has_current"].sum()),
        "bathymetry_rows": int(output["has_bathymetry"].sum()),
        "wave_wind_rows": int((output["has_wave"] & output["has_wind_speed"]).sum()),
        "wave_current_rows": int((output["has_wave"] & output["has_current"]).sum()),
        "wave_wind_current_rows": int(
            (output["has_wave"] & output["has_wind_speed"] & output["has_current"]).sum()
        ),
        "wave_wind_current_bathymetry_rows": int(output["model_ready_wave_wind_current"].sum()),
        "high_confidence_rows": int(output["model_ready_high_confidence"].sum()),
    }
    tier_a_coverage = {
        "tier_a_total": int(tier_a.sum()),
        "tier_a_wave": int((tier_a & output["has_wave"]).sum()),
        "tier_a_wind_speed": int((tier_a & output["has_wind_speed"]).sum()),
        "tier_a_wind_direction": int((tier_a & output["has_wind_direction"]).sum()),
        "tier_a_current": int((tier_a & output["has_current"]).sum()),
        "tier_a_wave_wind": int((tier_a & output["model_ready_wave_wind"]).sum()),
        "tier_a_wave_current": int((tier_a & output["model_ready_wave_current"]).sum()),
        "tier_a_wave_wind_current": int((tier_a & output["model_ready_wave_wind_current"]).sum()),
        "tier_a_high_confidence": int((tier_a & output["model_ready_high_confidence"]).sum()),
    }
    validation = {
        "dwell_rows": int(len(dwell)),
        "wave_rows": int(len(wave)),
        "wind_rows": int(len(wind)),
        "current_rows": int(len(current)),
        "bathymetry_rows": int(len(bathymetry)),
        "output_rows": int(len(output)),
        "duplicate_dwell_ids": int(output["dwell_id"].duplicated(keep=False).sum()),
        "row_identity_preserved": bool(output["dwell_id"].tolist() == dwell["dwell_id"].tolist()),
        "coverage": coverage,
        "tier_a_coverage": tier_a_coverage,
        "feature_class_counts": output["metocean_feature_class"].value_counts(dropna=False).to_dict(),
        "missing_reason_counts": output["metocean_missing_reason"].value_counts(dropna=False).head(20).to_dict(),
        "wave_wind_crosstab": wave_wind_crosstab,
        "wave_current_crosstab": wave_current_crosstab,
        "wind_current_crosstab": wind_current_crosstab,
        "triple_summary": triple_summary,
        "distributions": {
            "selected_hs_mean": _quantiles(output["selected_hs_mean"]),
            "selected_tp_mean": _quantiles(output["selected_tp_mean"]),
            "wind_speed_mean": _quantiles(output["wind_speed_mean"]),
            "current_speed_mean": _quantiles(output["current_speed_mean"]),
            "water_depth_m": _quantiles(output["water_depth_m"]),
        },
        "bathymetry": {
            "complete_rows": int(output["has_bathymetry"].sum()),
            "depth_warning_le_1m": int(_as_bool(output["depth_warning_le_1m"]).sum()),
            "depth_warning_le_5m": int(_as_bool(output["depth_warning_le_5m"]).sum()),
            "depth_warning_le_10m": int(_as_bool(output["depth_warning_le_10m"]).sum()),
            "current_available_depth_warning_le_10m": int(
                (output["has_current"] & _as_bool(output["depth_warning_le_10m"])).sum()
            ),
        },
        "interaction": interaction,
        "interaction_examples": interaction_examples,
    }
    return validation


def build_validation_report(
    validation: dict[str, Any],
    output: pd.DataFrame,
    output_path: Path,
) -> str:
    coverage = validation["coverage"]
    tier = validation["tier_a_coverage"]
    distributions = validation["distributions"]
    bathy = validation["bathymetry"]
    feature_counts = pd.Series(validation["feature_class_counts"], name="event_count").reset_index()
    feature_counts = feature_counts.rename(columns={"index": "metocean_feature_class"})
    missing_counts = pd.Series(validation["missing_reason_counts"], name="event_count").reset_index()
    missing_counts = missing_counts.rename(columns={"index": "metocean_missing_reason"})
    by_farm = _count_table(output, ["wind_farm", "metocean_feature_class"])
    by_year = (
        output.assign(year=pd.to_datetime(output["start_utc"], utc=True, errors="coerce").dt.year)
        .groupby(["year", "metocean_feature_class"], dropna=False)
        .size()
        .reset_index(name="event_count")
        .sort_values(["year", "metocean_feature_class"])
    )
    by_tier = _count_table(output, ["dwell_tier", "metocean_feature_class"])
    wave_wind = validation["wave_wind_crosstab"].reset_index()
    wave_current = validation["wave_current_crosstab"].reset_index()
    wind_current = validation["wind_current_crosstab"].reset_index()
    triple = validation["triple_summary"]
    interaction_counts = validation["interaction"]["counts"]
    interaction_examples = validation["interaction_examples"]
    thresholds = validation["interaction"]["thresholds"]

    lines = [
        "# Fusion v2 Validation Report",
        "",
        "## A. Research Design",
        "",
        "Hypotheses: Fusion v2 creates the first modelling-ready multi-parameter event table; wind speed expands explanatory evidence beyond waves and currents; current-aware modelling is limited to accepted NWS-covered normal farm-years; wind direction is sensitivity-only because it is sparse; and farm/year/tier coverage bias must be explicit.",
        "",
        "Metrics: row preservation, wave coverage, wind speed/direction coverage, current coverage, combined feature coverage, Tier A coverage, high-confidence subset size, missingness reasons, farm/year/tier bias, depth-warning impact, and wave/wind/current confidence cross-tabs.",
        "",
        "## Executive Conclusion",
        "",
        "Fusion v2 combines accepted wave confidence, Wind Confidence v1, Current Confidence v1, and EMODnet bathymetry into one event feature table. It preserves all dwell rows and keeps source-specific confidence separate. It is ready for modelling sensitivity, not calibrated access probability.",
        "",
        f"- Fusion v2 table: `{output_path}`",
        f"- Input dwell rows: {validation['dwell_rows']:,}",
        f"- Output rows: {validation['output_rows']:,}",
        f"- Row identity preserved: {validation['row_identity_preserved']}",
        f"- Model-ready wave+wind+current+bathymetry rows: {coverage['wave_wind_current_bathymetry_rows']:,}",
        f"- High-confidence multivariate rows: {coverage['high_confidence_rows']:,}",
        "",
        "## Input Inventory",
        "",
        f"- Dwell rows: {validation['dwell_rows']:,}",
        f"- Wave confidence rows: {validation['wave_rows']:,}",
        f"- Wind confidence rows: {validation['wind_rows']:,}",
        f"- Current confidence rows: {validation['current_rows']:,}",
        f"- Bathymetry rows: {validation['bathymetry_rows']:,}",
        "",
        "## Row Identity",
        "",
        f"- Input rows: {validation['dwell_rows']:,}",
        f"- Output rows: {validation['output_rows']:,}",
        f"- Duplicate `dwell_id` rows in output: {validation['duplicate_dwell_ids']:,}",
        f"- One-to-one joins preserved input order: {validation['row_identity_preserved']}",
        "",
        "## Coverage",
        "",
        f"- Wave rows: {coverage['wave_rows']:,}",
        f"- Wind speed rows: {coverage['wind_speed_rows']:,}",
        f"- Wind direction rows: {coverage['wind_direction_rows']:,}",
        f"- Current rows: {coverage['current_rows']:,}",
        f"- Bathymetry rows: {coverage['bathymetry_rows']:,}",
        f"- Wave + wind speed rows: {coverage['wave_wind_rows']:,}",
        f"- Wave + current rows: {coverage['wave_current_rows']:,}",
        f"- Wave + wind speed + current rows: {coverage['wave_wind_current_rows']:,}",
        f"- Wave + wind speed + current + bathymetry rows: {coverage['wave_wind_current_bathymetry_rows']:,}",
        "",
        "Feature class distribution:",
        "",
    ]
    lines.extend(_markdown_table(feature_counts, ["metocean_feature_class", "event_count"]))
    lines.extend(["", "Feature coverage by dwell tier:", ""])
    lines.extend(_markdown_table(by_tier, ["dwell_tier", "metocean_feature_class", "event_count"], limit=20))
    lines.extend(["", "Feature coverage by year:", ""])
    lines.extend(_markdown_table(by_year, ["year", "metocean_feature_class", "event_count"], limit=30))
    lines.extend(["", "Feature coverage by farm (top rows):", ""])
    lines.extend(_markdown_table(by_farm, ["wind_farm", "metocean_feature_class", "event_count"], limit=25))
    lines.extend(
        [
            "",
            "## Tier A Coverage",
            "",
            f"- Tier A total: {tier['tier_a_total']:,}",
            f"- Tier A with wave: {tier['tier_a_wave']:,}",
            f"- Tier A with wind speed: {tier['tier_a_wind_speed']:,}",
            f"- Tier A with wind direction: {tier['tier_a_wind_direction']:,}",
            f"- Tier A with current: {tier['tier_a_current']:,}",
            f"- Tier A with wave + wind: {tier['tier_a_wave_wind']:,}",
            f"- Tier A with wave + current: {tier['tier_a_wave_current']:,}",
            f"- Tier A with wave + wind + current: {tier['tier_a_wave_wind_current']:,}",
            f"- High-confidence Tier A subset: {tier['tier_a_high_confidence']:,}",
            "",
            "## Confidence Cross-Tabs",
            "",
            "Wave x wind:",
            "",
        ]
    )
    lines.extend(_markdown_table(wave_wind, list(wave_wind.columns)))
    lines.extend(["", "Wave x current:", ""])
    lines.extend(_markdown_table(wave_current, list(wave_current.columns)))
    lines.extend(["", "Wind x current:", ""])
    lines.extend(_markdown_table(wind_current, list(wind_current.columns)))
    lines.extend(["", "Wave x wind x current summary (top rows):", ""])
    lines.extend(
        _markdown_table(
            triple,
            ["wave_confidence_class", "wind_confidence_class", "current_confidence_class", "event_count"],
            limit=20,
        )
    )
    lines.extend(["", "## Distributions", ""])
    for name, summary in distributions.items():
        lines.append(
            f"- `{name}` min/p50/p95/max: {summary['min']:.3f} / {summary['p50']:.3f} / {summary['p95']:.3f} / {summary['max']:.3f}"
        )
    lines.extend(
        [
            "",
            "## Interaction Diagnostics",
            "",
            f"Thresholds used for diagnostic interactions: Hs p95 `{thresholds['hs_p95']:.3f}`, Hs p50 `{thresholds['hs_p50']:.3f}`, wind speed p95 `{thresholds['wind_speed_p95']:.3f}`, current speed p95 `{thresholds['current_speed_p95']:.3f}`.",
            "",
        ]
    )
    lines.extend(_markdown_table(interaction_counts, ["interaction_case", "event_count", "tier_a_count"]))
    lines.extend(["", "Tier A examples:", ""])
    lines.extend(
        _markdown_table(
            interaction_examples,
            [
                "interaction_case",
                "dwell_id",
                "wind_farm",
                "dwell_tier",
                "selected_hs_mean",
                "selected_tp_mean",
                "wind_speed_mean",
                "current_speed_mean",
            ],
            limit=20,
        )
    )
    lines.extend(
        [
            "",
            "## Bathymetry",
            "",
            f"- Bathymetry complete rows: {bathy['complete_rows']:,}",
            f"- Depth warning <=1 m rows: {bathy['depth_warning_le_1m']:,}",
            f"- Depth warning <=5 m rows: {bathy['depth_warning_le_5m']:,}",
            f"- Depth warning <=10 m rows: {bathy['depth_warning_le_10m']:,}",
            f"- Current-available rows with <=10 m depth warning: {bathy['current_available_depth_warning_le_10m']:,}",
            "",
            "Zero or near-zero depth warnings are preserved as site-context caveats. They do not automatically invalidate an observed dwell, but high-confidence multivariate flags exclude <=10 m depth-warning rows.",
            "",
            "## Missingness Reasons",
            "",
        ]
    )
    lines.extend(_markdown_table(missing_counts, ["metocean_missing_reason", "event_count"], limit=20))
    lines.extend(
        [
            "",
            "## Bias And Caveats",
            "",
            "- NWS current coverage remains source/domain biased; non-covered events keep missing current values and are not treated as zero current.",
            "- NORA3 wind direction is sparse and remains nullable/sensitivity-only.",
            "- Baltic historical true currents remain daily/contextual and are not promoted to event-scale current evidence.",
            "- Stress-test current farm-years remain excluded from the accepted NWS current archive.",
            "- FINO validation has not been imported yet.",
            "- Fusion v2 is not a calibrated `P(operation | weather)` model and does not infer CTV/SOV roles from vessel length.",
            "",
            "## Research Interpretation",
            "",
            "Fusion v2 is ready for modelling sensitivity. The first modelling subset should compare wave-only against wave + wind speed, wave + event-scale current, and wave + wind speed + current. Wind direction should be held back except for narrow sensitivity checks on the 197 speed+direction rows. Current-aware modelling should be interpreted as NWS-domain evidence, not Europe-wide current coverage.",
            "",
            "## Recommendation",
            "",
            "Accept Fusion v2 if row identity is preserved, missing current and wind direction remain null, confidence fields are preserved separately, and the model-ready flags match the documented rules. Proceed to Stage 2 modelling sensitivity before targeted wind-direction repair, stress-test current increments, or FINO imports.",
        ]
    )
    return "\n".join(lines) + "\n"


def write_outputs(
    output: pd.DataFrame,
    report: str,
    output_dir: Path,
    report_dir: Path,
    overwrite: bool,
) -> tuple[Path, Path]:
    output_path = output_dir / FUSION_V2_FILENAME
    report_path = report_dir / VALIDATION_REPORT_FILENAME
    existing = [path for path in (output_path, report_path) if path.exists()]
    if existing and not overwrite:
        raise FileExistsError(
            "Fusion v2 outputs already exist; pass overwrite=True to replace: "
            + ", ".join(str(path) for path in existing)
        )
    output_dir.mkdir(parents=True, exist_ok=True)
    report_dir.mkdir(parents=True, exist_ok=True)
    ensure_fusion_v2_schema(output).to_parquet(output_path, index=False)
    report_path.write_text(report, encoding="utf-8")
    return output_path, report_path


def build_metocean_fusion_v2(
    dwell_weather: Path = DEFAULT_DWELL_WEATHER,
    wave_confidence_path: Path = DEFAULT_WAVE_CONFIDENCE,
    wind_confidence_path: Path = DEFAULT_WIND_CONFIDENCE,
    current_confidence_path: Path = DEFAULT_CURRENT_CONFIDENCE,
    bathymetry_path: Path = DEFAULT_BATHYMETRY,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    report_dir: Path = DEFAULT_REPORT_DIR,
    overwrite: bool = False,
) -> FusionV2Result:
    output_path = output_dir / FUSION_V2_FILENAME
    report_path = report_dir / VALIDATION_REPORT_FILENAME
    if not overwrite:
        existing = [path for path in (output_path, report_path) if path.exists()]
        if existing:
            raise FileExistsError(
                "Fusion v2 outputs already exist; pass overwrite=True to replace: "
                + ", ".join(str(path) for path in existing)
            )

    dwell = load_dwell_events(dwell_weather)
    wave = load_wave_confidence(wave_confidence_path)
    wind = load_wind_confidence(wind_confidence_path)
    current = load_current_confidence(current_confidence_path)
    bathymetry = load_bathymetry(bathymetry_path)

    output = build_fusion_v2_table(dwell, wave, wind, current, bathymetry)
    validation = validate_fusion_v2(dwell, wave, wind, current, bathymetry, output)
    report = build_validation_report(validation, output, output_path)
    output_path, report_path = write_outputs(output, report, output_dir, report_dir, overwrite)
    return FusionV2Result(output_path=output_path, report_path=report_path, output=output, validation=validation)
