"""Current Confidence v1: event-level NWS current evidence.

This increment attaches the accepted source-specific NWS hourly `uo`/`vo`
archive to AIS dwell events. It preserves one candidate row per dwell event,
including missingness reasons, and then derives an event-level confidence class.

It does not download currents, use legacy CMEMS CSV/fallback data, source-fuse
with waves, or rebuild the final dwell-metocean feature table.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from .current_pilot_v1 import farm_slug, normalize_farm_name
from .current_scaling_preflight import NWS_CURRENT_DATASET_ID, NWS_CURRENT_PRODUCT_ID
from .nws_current_batch import CURRENT_DIRECTION_CONVENTION


DEFAULT_DWELL_WEATHER = Path(
    "Data/Processed/ais_dwell_backfill/cross_farm_dwell_weather_features.parquet"
)
DEFAULT_NWS_CURRENT_ROOT = Path("Data/Processed/metocean/nws_current_timeseries")
DEFAULT_NWS_CURRENT_MANIFEST = DEFAULT_NWS_CURRENT_ROOT / "manifest.csv"
DEFAULT_WAVE_CONFIDENCE = Path(
    "Data/Processed/metocean/fusion_v1_source_agreement/wave_event_confidence.parquet"
)
DEFAULT_BATHYMETRY = Path("Data/Processed/metocean/bathymetry/site_bathymetry_points.parquet")
DEFAULT_OUTPUT_DIR = Path("Data/Processed/metocean/current_confidence_v1")
DEFAULT_REPORT_DIR = Path("reports/current_confidence_v1")

CURRENT_EVENT_CANDIDATES_FILENAME = "current_event_candidates.parquet"
CURRENT_EVENT_CONFIDENCE_FILENAME = "current_event_confidence.parquet"
VALIDATION_REPORT_FILENAME = "current_confidence_validation_report.md"

NWS_SOURCE = "NWS"
MAX_EVENT_SCALE_DISTANCE_KM = 10.0
MAX_EVENT_SCALE_TIME_GAP_MINUTES = 60.0

DWELL_COLUMNS = [
    "dwell_id",
    "visit_id",
    "wind_farm",
    "farm_id",
    "dwell_tier",
    "start_utc",
    "end_utc",
    "centroid_lat",
    "centroid_lon",
]

CURRENT_ARCHIVE_COLUMNS = [
    "timestamp_utc",
    "wind_farm",
    "farm_id",
    "year",
    "sample_point_id",
    "sample_point_type",
    "lat",
    "lon",
    "current_grid_lat",
    "current_grid_lon",
    "current_spatial_distance_km",
    "current_u",
    "current_v",
    "current_speed",
    "current_direction_to_deg",
    "current_direction_convention",
    "current_depth_m",
    "current_depth_selection_rule",
    "current_source",
    "current_product_id",
    "current_dataset_id",
    "current_native_temporal_resolution_minutes",
    "current_native_spatial_resolution_km",
    "current_assignment_method",
    "current_spatial_match_status",
    "source_file",
    "provenance_status",
    "emodnet_water_depth_m",
    "depth_warning_le_1m",
    "depth_warning_le_5m",
    "depth_warning_le_10m",
]

CURRENT_EVENT_CANDIDATE_COLUMNS = [
    "dwell_id",
    "visit_id",
    "wind_farm",
    "farm_id",
    "dwell_tier",
    "start_utc",
    "end_utc",
    "source",
    "current_product_id",
    "current_dataset_id",
    "sample_point_id",
    "sample_point_type",
    "source_sample_lat",
    "source_sample_lon",
    "source_sample_distance_km",
    "event_window_sample_count",
    "nearest_time_gap_minutes",
    "event_bracketed_by_source_times",
    "event_has_in_window_samples",
    "temporal_assignment_method",
    "spatial_assignment_method",
    "current_u_mean",
    "current_v_mean",
    "current_speed_mean",
    "current_speed_p95",
    "current_direction_to_sin_mean",
    "current_direction_to_cos_mean",
    "current_direction_to_deg_mean",
    "current_direction_convention",
    "current_depth_m",
    "current_depth_selection_rule",
    "current_missing_fraction",
    "current_missing_reason",
    "provenance_status",
    "emodnet_water_depth_m",
    "depth_warning_le_1m",
    "depth_warning_le_5m",
    "depth_warning_le_10m",
]

CURRENT_EVENT_CONFIDENCE_COLUMNS = [
    "dwell_id",
    "visit_id",
    "wind_farm",
    "farm_id",
    "dwell_tier",
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
    "event_bracketed_by_source_times",
    "event_has_in_window_samples",
    "current_missing_reason",
    "selection_reason",
    "wave_confidence_class",
    "water_depth_m",
    "depth_warning_le_1m",
    "depth_warning_le_5m",
    "depth_warning_le_10m",
]


@dataclass(frozen=True)
class CurrentConfidenceResult:
    candidate_path: Path
    confidence_path: Path
    report_path: Path
    candidates: pd.DataFrame
    confidence: pd.DataFrame
    validation: dict[str, Any]


def ensure_candidate_schema(frame: pd.DataFrame) -> pd.DataFrame:
    for col in CURRENT_EVENT_CANDIDATE_COLUMNS:
        if col not in frame.columns:
            frame[col] = pd.NA
    return frame[CURRENT_EVENT_CANDIDATE_COLUMNS].copy()


def ensure_confidence_schema(frame: pd.DataFrame) -> pd.DataFrame:
    for col in CURRENT_EVENT_CONFIDENCE_COLUMNS:
        if col not in frame.columns:
            frame[col] = pd.NA
    return frame[CURRENT_EVENT_CONFIDENCE_COLUMNS].copy()


def _read_parquet_columns(path: Path, columns: list[str]) -> pd.DataFrame:
    schema = pq.read_schema(path).names
    available = [col for col in columns if col in schema]
    frame = pd.read_parquet(path, columns=available)
    for col in columns:
        if col not in frame.columns:
            frame[col] = pd.NA
    return frame[columns].copy()


def load_dwell_events(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Dwell-weather table not found: {path}")
    dwell = _read_parquet_columns(path, DWELL_COLUMNS)
    dwell["start_utc"] = pd.to_datetime(dwell["start_utc"], utc=True, errors="coerce")
    dwell["end_utc"] = pd.to_datetime(dwell["end_utc"], utc=True, errors="coerce")
    dwell["event_year"] = dwell["start_utc"].dt.year.astype("Int64")
    dwell["__row_id"] = np.arange(len(dwell), dtype=np.int64)
    return dwell


def load_manifest(path: Path, current_root: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"NWS current manifest not found: {path}")
    manifest = pd.read_csv(path)
    accepted = manifest[
        manifest["status"].isin(["validated", "skipped_existing"])
        & manifest["qa_status"].eq("passed")
    ].copy()
    accepted["year"] = accepted["year"].astype(int)
    accepted["processed_path"] = accepted["processed_path"].map(
        lambda value: str(Path(value)) if pd.notna(value) else ""
    )
    accepted["processed_path"] = accepted["processed_path"].map(
        lambda value: str(Path(value)) if value else ""
    )
    return accepted.reset_index(drop=True)


def _manifest_lookup(manifest: pd.DataFrame) -> dict[tuple[str, int], pd.Series]:
    lookup: dict[tuple[str, int], pd.Series] = {}
    for _, row in manifest.iterrows():
        year = int(row["year"])
        for value in (row.get("wind_farm"), row.get("farm_id")):
            if pd.notna(value):
                lookup[(normalize_farm_name(value), year)] = row
    return lookup


def load_wave_confidence(path: Path) -> pd.DataFrame:
    columns = ["dwell_id", "visit_id", "wave_confidence_class"]
    if not path.exists():
        return pd.DataFrame(columns=columns)
    wave = _read_parquet_columns(path, columns)
    return wave.drop_duplicates(["dwell_id", "visit_id"]).reset_index(drop=True)


def load_bathymetry(path: Path) -> pd.DataFrame:
    columns = [
        "wind_farm",
        "sample_point_id",
        "sample_point_type",
        "lat",
        "lon",
        "water_depth_m",
    ]
    if not path.exists():
        return pd.DataFrame(columns=columns)
    bathy = _read_parquet_columns(path, columns)
    bathy["water_depth_m"] = pd.to_numeric(bathy["water_depth_m"], errors="coerce")
    bathy["_norm_wind_farm"] = bathy["wind_farm"].map(normalize_farm_name)
    return bathy


def _haversine_vector(lat: float, lon: float, sample_lats: np.ndarray, sample_lons: np.ndarray) -> np.ndarray:
    radius_km = 6371.0088
    lat1 = np.radians(float(lat))
    lon1 = np.radians(float(lon))
    lat2 = np.radians(sample_lats.astype(float))
    lon2 = np.radians(sample_lons.astype(float))
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat / 2.0) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2.0) ** 2
    return radius_km * 2.0 * np.arctan2(np.sqrt(a), np.sqrt(1.0 - a))


def _nearest_point(points: pd.DataFrame, lat: Any, lon: Any) -> pd.Series | None:
    if points.empty:
        return None
    if pd.isna(lat) or pd.isna(lon):
        centroid = points[points["sample_point_id"].astype(str).eq("farm_centroid")]
        return centroid.iloc[0] if not centroid.empty else points.iloc[0]
    distances = _haversine_vector(
        float(lat),
        float(lon),
        points["lat"].astype(float).to_numpy(),
        points["lon"].astype(float).to_numpy(),
    )
    return points.iloc[int(np.nanargmin(distances))]


def _source_point_metadata(rows: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "sample_point_id",
        "sample_point_type",
        "lat",
        "lon",
        "current_spatial_distance_km",
        "current_depth_m",
        "current_depth_selection_rule",
        "current_product_id",
        "current_dataset_id",
        "current_direction_convention",
        "current_source",
        "current_assignment_method",
        "current_spatial_match_status",
        "provenance_status",
        "emodnet_water_depth_m",
        "depth_warning_le_1m",
        "depth_warning_le_5m",
        "depth_warning_le_10m",
    ]
    return rows[cols].drop_duplicates("sample_point_id").reset_index(drop=True)


def _bathymetry_lookup(bathymetry: pd.DataFrame) -> dict[str, pd.DataFrame]:
    if bathymetry.empty:
        return {}
    lookup: dict[str, pd.DataFrame] = {}
    key_col = "_norm_wind_farm" if "_norm_wind_farm" in bathymetry.columns else "wind_farm"
    for key, group in bathymetry.groupby(key_col, dropna=True, sort=False):
        norm_key = str(key) if key_col == "_norm_wind_farm" else normalize_farm_name(key)
        lookup[norm_key] = group.reset_index(drop=True)
    return lookup


def _bathymetry_points_for_event(
    bathymetry_lookup: dict[str, pd.DataFrame],
    event: pd.Series,
) -> pd.DataFrame:
    for col in ("wind_farm", "farm_id"):
        value = event.get(col)
        if pd.notna(value):
            points = bathymetry_lookup.get(normalize_farm_name(value))
            if points is not None:
                return points
    return pd.DataFrame()


def _missing_candidate(
    event: pd.Series,
    reason: str,
    bathymetry_lookup: dict[str, pd.DataFrame],
) -> dict[str, Any]:
    points = _bathymetry_points_for_event(bathymetry_lookup, event)
    point = _nearest_point(points, event.get("centroid_lat"), event.get("centroid_lon"))
    water_depth = point.get("water_depth_m") if point is not None else pd.NA
    return {
        "dwell_id": event.get("dwell_id"),
        "visit_id": event.get("visit_id"),
        "wind_farm": event.get("wind_farm"),
        "farm_id": event.get("farm_id"),
        "dwell_tier": event.get("dwell_tier"),
        "start_utc": event.get("start_utc"),
        "end_utc": event.get("end_utc"),
        "source": NWS_SOURCE,
        "current_product_id": NWS_CURRENT_PRODUCT_ID,
        "current_dataset_id": NWS_CURRENT_DATASET_ID,
        "sample_point_id": point.get("sample_point_id") if point is not None else pd.NA,
        "sample_point_type": point.get("sample_point_type") if point is not None else pd.NA,
        "source_sample_lat": point.get("lat") if point is not None else pd.NA,
        "source_sample_lon": point.get("lon") if point is not None else pd.NA,
        "source_sample_distance_km": pd.NA,
        "event_window_sample_count": 0,
        "nearest_time_gap_minutes": pd.NA,
        "event_bracketed_by_source_times": False,
        "event_has_in_window_samples": False,
        "temporal_assignment_method": "not_assigned",
        "spatial_assignment_method": "not_assigned",
        "current_u_mean": pd.NA,
        "current_v_mean": pd.NA,
        "current_speed_mean": pd.NA,
        "current_speed_p95": pd.NA,
        "current_direction_to_sin_mean": pd.NA,
        "current_direction_to_cos_mean": pd.NA,
        "current_direction_to_deg_mean": pd.NA,
        "current_direction_convention": CURRENT_DIRECTION_CONVENTION,
        "current_depth_m": pd.NA,
        "current_depth_selection_rule": pd.NA,
        "current_missing_fraction": 1.0,
        "current_missing_reason": reason,
        "provenance_status": reason,
        "emodnet_water_depth_m": water_depth,
        "depth_warning_le_1m": bool(pd.notna(water_depth) and float(water_depth) <= 1.0),
        "depth_warning_le_5m": bool(pd.notna(water_depth) and float(water_depth) <= 5.0),
        "depth_warning_le_10m": bool(pd.notna(water_depth) and float(water_depth) <= 10.0),
        "__row_id": event.get("__row_id"),
    }


def _prepare_sample_series(rows: pd.DataFrame) -> dict[Any, dict[str, Any]]:
    prepared: dict[Any, dict[str, Any]] = {}
    for sample_id, group in rows.sort_values("timestamp_utc").groupby("sample_point_id", sort=False):
        frame = group.reset_index(drop=True)
        timestamps = pd.to_datetime(frame["timestamp_utc"], utc=True, errors="coerce")
        prepared[sample_id] = {
            "frame": frame,
            "time_ns": timestamps.astype("int64").to_numpy(),
            "u": pd.to_numeric(frame["current_u"], errors="coerce").to_numpy(dtype=float),
            "v": pd.to_numeric(frame["current_v"], errors="coerce").to_numpy(dtype=float),
            "speed": pd.to_numeric(frame["current_speed"], errors="coerce").to_numpy(dtype=float),
        }
    return prepared


def _nearest_gap_minutes(time_ns: np.ndarray, midpoint_ns: int) -> float:
    if len(time_ns) == 0:
        return math.nan
    idx = int(np.searchsorted(time_ns, midpoint_ns))
    candidates = []
    if idx < len(time_ns):
        candidates.append(abs(int(time_ns[idx]) - midpoint_ns))
    if idx > 0:
        candidates.append(abs(int(time_ns[idx - 1]) - midpoint_ns))
    return float(min(candidates) / 60_000_000_000.0) if candidates else math.nan


def _aggregate_sample_window(series: dict[str, Any], start: pd.Timestamp, end: pd.Timestamp) -> dict[str, Any]:
    time_ns = series["time_ns"]
    start_ns = int(start.value)
    end_ns = int(end.value)
    midpoint_ns = int(start_ns + (end_ns - start_ns) / 2)
    left = int(np.searchsorted(time_ns, start_ns, side="left"))
    right = int(np.searchsorted(time_ns, end_ns, side="right"))
    window_indices = np.arange(left, right, dtype=int)
    before_idx = int(np.searchsorted(time_ns, start_ns, side="right") - 1)
    after_idx = int(np.searchsorted(time_ns, end_ns, side="left"))
    bracketed = before_idx >= 0 and after_idx < len(time_ns)
    nearest_gap = _nearest_gap_minutes(time_ns, midpoint_ns)

    if len(window_indices) > 0:
        selected = window_indices
        method = "event_window_mean"
        has_window = True
    elif bracketed:
        selected = np.unique(np.array([before_idx, after_idx], dtype=int))
        method = "bracketing_pair_mean"
        has_window = False
    else:
        selected = np.array([], dtype=int)
        method = "not_assigned"
        has_window = False

    if len(selected) == 0:
        return {
            "event_window_sample_count": 0,
            "nearest_time_gap_minutes": nearest_gap,
            "event_bracketed_by_source_times": bracketed,
            "event_has_in_window_samples": False,
            "temporal_assignment_method": method,
            "current_u_mean": math.nan,
            "current_v_mean": math.nan,
            "current_speed_mean": math.nan,
            "current_speed_p95": math.nan,
            "current_direction_to_sin_mean": math.nan,
            "current_direction_to_cos_mean": math.nan,
            "current_direction_to_deg_mean": math.nan,
            "current_missing_fraction": 1.0,
            "current_missing_reason": "no_assignable_current_samples",
        }

    u = series["u"][selected]
    v = series["v"][selected]
    speed = series["speed"][selected]
    valid = np.isfinite(u) & np.isfinite(v)
    missing_fraction = float(1.0 - valid.mean()) if len(valid) else 1.0
    if not valid.any():
        return {
            "event_window_sample_count": int(len(window_indices)),
            "nearest_time_gap_minutes": nearest_gap,
            "event_bracketed_by_source_times": bracketed,
            "event_has_in_window_samples": has_window,
            "temporal_assignment_method": method,
            "current_u_mean": math.nan,
            "current_v_mean": math.nan,
            "current_speed_mean": math.nan,
            "current_speed_p95": math.nan,
            "current_direction_to_sin_mean": math.nan,
            "current_direction_to_cos_mean": math.nan,
            "current_direction_to_deg_mean": math.nan,
            "current_missing_fraction": missing_fraction,
            "current_missing_reason": "current_values_missing",
        }

    u_mean = float(np.nanmean(u))
    v_mean = float(np.nanmean(v))
    speed_mean = float(math.sqrt(u_mean**2 + v_mean**2))
    speed_p95 = float(np.nanpercentile(speed, 95)) if np.isfinite(speed).any() else math.nan
    direction_deg = float((math.degrees(math.atan2(u_mean, v_mean)) + 360.0) % 360.0)
    radians = math.radians(direction_deg)
    return {
        "event_window_sample_count": int(len(window_indices)),
        "nearest_time_gap_minutes": nearest_gap,
        "event_bracketed_by_source_times": bracketed,
        "event_has_in_window_samples": has_window,
        "temporal_assignment_method": method,
        "current_u_mean": u_mean,
        "current_v_mean": v_mean,
        "current_speed_mean": speed_mean,
        "current_speed_p95": speed_p95,
        "current_direction_to_sin_mean": float(math.sin(radians)),
        "current_direction_to_cos_mean": float(math.cos(radians)),
        "current_direction_to_deg_mean": direction_deg,
        "current_missing_fraction": missing_fraction,
        "current_missing_reason": pd.NA if missing_fraction <= 0.05 else "current_values_partially_missing",
    }


def _event_candidate_from_partition(
    event: pd.Series,
    points: pd.DataFrame,
    sample_series: dict[Any, dict[str, Any]],
) -> dict[str, Any]:
    if pd.isna(event.get("start_utc")) or pd.isna(event.get("end_utc")):
        return _missing_candidate(event, "invalid_event_window", {})
    point = _nearest_point(points, event.get("centroid_lat"), event.get("centroid_lon"))
    if point is None:
        return _missing_candidate(event, "no_current_sample_points", {})
    sample_id = point["sample_point_id"]
    if sample_id not in sample_series:
        return _missing_candidate(event, "sample_point_timeseries_missing", {})
    aggregate = _aggregate_sample_window(
        sample_series[sample_id],
        pd.Timestamp(event["start_utc"]),
        pd.Timestamp(event["end_utc"]),
    )
    assigned = pd.notna(aggregate["current_u_mean"]) and pd.notna(aggregate["current_v_mean"])
    missing_reason = aggregate["current_missing_reason"] if not assigned else aggregate["current_missing_reason"]
    if not assigned and pd.isna(missing_reason):
        missing_reason = "current_values_missing"
    return {
        "dwell_id": event.get("dwell_id"),
        "visit_id": event.get("visit_id"),
        "wind_farm": event.get("wind_farm"),
        "farm_id": event.get("farm_id"),
        "dwell_tier": event.get("dwell_tier"),
        "start_utc": event.get("start_utc"),
        "end_utc": event.get("end_utc"),
        "source": NWS_SOURCE,
        "current_product_id": point.get("current_product_id"),
        "current_dataset_id": point.get("current_dataset_id"),
        "sample_point_id": sample_id,
        "sample_point_type": point.get("sample_point_type"),
        "source_sample_lat": point.get("lat"),
        "source_sample_lon": point.get("lon"),
        "source_sample_distance_km": point.get("current_spatial_distance_km"),
        "spatial_assignment_method": "nearest_current_sample_point_to_dwell_centroid",
        "current_direction_convention": point.get("current_direction_convention"),
        "current_depth_m": point.get("current_depth_m"),
        "current_depth_selection_rule": point.get("current_depth_selection_rule"),
        "current_missing_reason": missing_reason,
        "provenance_status": point.get("provenance_status") if assigned else "current_assignment_missing",
        "emodnet_water_depth_m": point.get("emodnet_water_depth_m"),
        "depth_warning_le_1m": bool(point.get("depth_warning_le_1m")),
        "depth_warning_le_5m": bool(point.get("depth_warning_le_5m")),
        "depth_warning_le_10m": bool(point.get("depth_warning_le_10m")),
        "__row_id": event.get("__row_id"),
        **aggregate,
    }


def _load_current_partition(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"NWS current partition not found: {path}")
    rows = _read_parquet_columns(path, CURRENT_ARCHIVE_COLUMNS)
    rows["timestamp_utc"] = pd.to_datetime(rows["timestamp_utc"], utc=True, errors="coerce")
    rows["current_u"] = pd.to_numeric(rows["current_u"], errors="coerce")
    rows["current_v"] = pd.to_numeric(rows["current_v"], errors="coerce")
    rows["current_speed"] = pd.to_numeric(rows["current_speed"], errors="coerce")
    return rows.dropna(subset=["timestamp_utc"]).reset_index(drop=True)


def build_current_event_candidates(
    dwell: pd.DataFrame,
    manifest: pd.DataFrame,
    bathymetry: pd.DataFrame,
) -> pd.DataFrame:
    lookup = _manifest_lookup(manifest)
    bathy_lookup = _bathymetry_lookup(bathymetry)
    candidate_rows: list[dict[str, Any]] = []
    for (farm_key, year), group in dwell.groupby(
        [dwell["wind_farm"].map(normalize_farm_name), "event_year"],
        dropna=False,
        sort=False,
    ):
        if pd.isna(year) or not farm_key:
            for _, event in group.iterrows():
                candidate_rows.append(_missing_candidate(event, "invalid_event_year", bathy_lookup))
            continue
        manifest_row = lookup.get((farm_key, int(year)))
        if manifest_row is None:
            for _, event in group.iterrows():
                candidate_rows.append(_missing_candidate(event, "missing_nws_current_partition", bathy_lookup))
            continue
        partition = Path(str(manifest_row["processed_path"]))
        try:
            current_rows = _load_current_partition(partition)
            points = _source_point_metadata(current_rows)
            sample_series = _prepare_sample_series(current_rows)
            for _, event in group.iterrows():
                candidate_rows.append(_event_candidate_from_partition(event, points, sample_series))
        except Exception as exc:
            reason = f"current_partition_unreadable: {type(exc).__name__}: {exc}"
            for _, event in group.iterrows():
                candidate_rows.append(_missing_candidate(event, reason, bathy_lookup))
    candidates = ensure_candidate_schema(pd.DataFrame(candidate_rows).sort_values("__row_id"))
    return candidates.reset_index(drop=True)


def classify_event_current_confidence(candidate: pd.Series) -> tuple[str, float, bool, str]:
    provenance = str(candidate.get("provenance_status", "")).casefold()
    missing_reason = candidate.get("current_missing_reason")
    has_uv = pd.notna(candidate.get("current_u_mean")) and pd.notna(candidate.get("current_v_mean"))
    if provenance.startswith(("fallback", "simulated", "legacy")):
        return "D_unsuitable", 0.0, False, "Fallback, simulated, or legacy current provenance is banned."
    if not has_uv:
        return "D_unsuitable", 0.0, False, f"No valid event-level u/v current evidence: {missing_reason}."
    if str(candidate.get("source", "")).upper() != NWS_SOURCE:
        return "C_low_confidence", 0.35, False, "Current source is not the accepted NWS hourly archive."
    if str(candidate.get("provenance_status", "")).casefold() != "real_uo_vo":
        return "D_unsuitable", 0.0, False, "Current provenance is not accepted true uo/vo."
    if pd.notna(candidate.get("current_speed_mean")) and float(candidate["current_speed_mean"]) < 0.0:
        return "D_unsuitable", 0.0, False, "Impossible negative current speed."

    distance = float(candidate.get("source_sample_distance_km")) if pd.notna(candidate.get("source_sample_distance_km")) else math.inf
    gap = float(candidate.get("nearest_time_gap_minutes")) if pd.notna(candidate.get("nearest_time_gap_minutes")) else math.inf
    bracketed = bool(candidate.get("event_bracketed_by_source_times"))
    has_window = bool(candidate.get("event_has_in_window_samples"))
    method = str(candidate.get("temporal_assignment_method", ""))
    missing_fraction = (
        float(candidate.get("current_missing_fraction"))
        if pd.notna(candidate.get("current_missing_fraction"))
        else 1.0
    )
    if (
        distance <= MAX_EVENT_SCALE_DISTANCE_KM
        and bracketed
        and (has_window or method == "bracketing_pair_mean")
        and gap <= MAX_EVENT_SCALE_TIME_GAP_MINUTES
        and missing_fraction <= 0.05
    ):
        return "A_event_scale", 1.0, True, "NWS hourly true u/v evidence brackets the dwell window with acceptable spatial and temporal alignment."
    if bracketed and gap <= MAX_EVENT_SCALE_TIME_GAP_MINUTES * 2.0:
        return "B_contextual", 0.7, False, "True NWS u/v evidence is available but event-window or spatial quality is contextual."
    return "C_low_confidence", 0.35, False, "True NWS u/v evidence exists but temporal or spatial alignment is weak."


def build_current_event_confidence(candidates: pd.DataFrame, wave_confidence: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for _, candidate in candidates.iterrows():
        cls, score, has_event_scale, reason = classify_event_current_confidence(candidate)
        rows.append(
            {
                "dwell_id": candidate["dwell_id"],
                "visit_id": candidate["visit_id"],
                "wind_farm": candidate["wind_farm"],
                "farm_id": candidate["farm_id"],
                "dwell_tier": candidate["dwell_tier"],
                "has_event_scale_current": has_event_scale,
                "current_confidence_class": cls,
                "current_confidence_score": score,
                "current_source": candidate["source"],
                "current_u_mean": candidate["current_u_mean"],
                "current_v_mean": candidate["current_v_mean"],
                "current_speed_mean": candidate["current_speed_mean"],
                "current_direction_to_sin_mean": candidate["current_direction_to_sin_mean"],
                "current_direction_to_cos_mean": candidate["current_direction_to_cos_mean"],
                "current_depth_m": candidate["current_depth_m"],
                "event_window_sample_count": candidate["event_window_sample_count"],
                "nearest_time_gap_minutes": candidate["nearest_time_gap_minutes"],
                "event_bracketed_by_source_times": candidate["event_bracketed_by_source_times"],
                "event_has_in_window_samples": candidate["event_has_in_window_samples"],
                "current_missing_reason": candidate["current_missing_reason"],
                "selection_reason": reason,
                "water_depth_m": candidate["emodnet_water_depth_m"],
                "depth_warning_le_1m": candidate["depth_warning_le_1m"],
                "depth_warning_le_5m": candidate["depth_warning_le_5m"],
                "depth_warning_le_10m": candidate["depth_warning_le_10m"],
            }
        )
    confidence = ensure_confidence_schema(pd.DataFrame(rows))
    if not wave_confidence.empty:
        confidence = confidence.drop(columns=["wave_confidence_class"]).merge(
            wave_confidence,
            on=["dwell_id", "visit_id"],
            how="left",
        )
        confidence = ensure_confidence_schema(confidence)
    return confidence


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
    return str(value)


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
        lines.append("| " + " | ".join(_format_value(row[col]) for col in columns) + " |")
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


def validate_current_confidence(
    dwell: pd.DataFrame,
    manifest: pd.DataFrame,
    candidates: pd.DataFrame,
    confidence: pd.DataFrame,
    wave_confidence: pd.DataFrame,
    bathymetry: pd.DataFrame,
) -> dict[str, Any]:
    valid_uv = candidates["current_u_mean"].notna() & candidates["current_v_mean"].notna()
    expected_speed = np.sqrt(
        pd.to_numeric(candidates.loc[valid_uv, "current_u_mean"], errors="coerce") ** 2
        + pd.to_numeric(candidates.loc[valid_uv, "current_v_mean"], errors="coerce") ** 2
    )
    speed_error = (
        float(np.nanmax(np.abs(expected_speed - pd.to_numeric(candidates.loc[valid_uv, "current_speed_mean"], errors="coerce"))))
        if valid_uv.any()
        else math.nan
    )
    direction = pd.to_numeric(candidates.loc[valid_uv, "current_direction_to_deg_mean"], errors="coerce")
    direction_ok = bool(direction.dropna().between(0, 360, inclusive="left").all())
    sin_cos_norm = np.sqrt(
        pd.to_numeric(candidates.loc[valid_uv, "current_direction_to_sin_mean"], errors="coerce") ** 2
        + pd.to_numeric(candidates.loc[valid_uv, "current_direction_to_cos_mean"], errors="coerce") ** 2
    )
    sin_cos_max_error = float(np.nanmax(np.abs(sin_cos_norm - 1.0))) if len(sin_cos_norm) else math.nan
    eligible = candidates["current_missing_reason"].fillna("").astype(str).ne("missing_nws_current_partition")
    tier_a = _is_tier_a(dwell["dwell_tier"])
    confidence_counts = confidence["current_confidence_class"].value_counts(dropna=False).to_dict()
    crosstab = pd.crosstab(
        confidence["current_confidence_class"].fillna("missing"),
        confidence["wave_confidence_class"].fillna("missing"),
    )
    both_high = confidence[
        confidence["current_confidence_class"].eq("A_event_scale")
        & confidence["wave_confidence_class"].eq("A_high")
    ]
    validation = {
        "input_dwell_rows": int(len(dwell)),
        "manifest_rows": int(len(manifest)),
        "nws_current_partitions": int(manifest["processed_path"].nunique()),
        "nws_current_rows": int(manifest["row_count"].sum()) if "row_count" in manifest else 0,
        "fusion_v1_rows": int(len(wave_confidence)),
        "bathymetry_rows": int(len(bathymetry)),
        "candidate_rows": int(len(candidates)),
        "confidence_rows": int(len(confidence)),
        "nws_eligible_events": int(eligible.sum()),
        "valid_current_events": int(valid_uv.sum()),
        "tier_a_events": int(tier_a.sum()),
        "tier_a_valid_current_events": int((tier_a.to_numpy() & valid_uv.to_numpy()).sum()),
        "bracketed_events": int(candidates["event_bracketed_by_source_times"].fillna(False).astype(bool).sum()),
        "in_window_events": int(candidates["event_has_in_window_samples"].fillna(False).astype(bool).sum()),
        "event_scale_current_events": int(confidence["has_event_scale_current"].fillna(False).astype(bool).sum()),
        "confidence_counts": confidence_counts,
        "wave_current_crosstab": crosstab,
        "both_high_wave_current": int(len(both_high)),
        "tier_a_both_high_wave_current": int(_is_tier_a(both_high["dwell_tier"]).sum()) if not both_high.empty else 0,
        "speed_consistency_max_error": speed_error,
        "direction_ok": direction_ok,
        "direction_sin_cos_max_error": sin_cos_max_error,
        "speed_summary": _quantiles(candidates["current_speed_mean"]),
        "speed_p95_summary": _quantiles(candidates["current_speed_p95"]),
        "nearest_gap_summary": _quantiles(candidates["nearest_time_gap_minutes"]),
        "window_sample_summary": _quantiles(candidates["event_window_sample_count"]),
        "spatial_distance_summary": _quantiles(candidates["source_sample_distance_km"]),
        "missing_reason_counts": candidates["current_missing_reason"].fillna("none").value_counts().to_dict(),
    }
    return validation


def build_validation_report(
    dwell: pd.DataFrame,
    manifest: pd.DataFrame,
    candidates: pd.DataFrame,
    confidence: pd.DataFrame,
    validation: dict[str, Any],
    candidate_path: Path,
    confidence_path: Path,
) -> str:
    coverage_by_farm = _count_table(confidence[confidence["has_event_scale_current"].eq(True)], ["wind_farm"])
    coverage_by_tier = _count_table(confidence, ["dwell_tier", "current_confidence_class"])
    by_year = (
        confidence.assign(year=pd.to_datetime(candidates["start_utc"], utc=True, errors="coerce").dt.year)
        .groupby(["year", "current_confidence_class"], dropna=False)
        .size()
        .reset_index(name="event_count")
        .sort_values(["year", "current_confidence_class"])
    )
    farm_year = (
        confidence.assign(year=pd.to_datetime(candidates["start_utc"], utc=True, errors="coerce").dt.year)
        .groupby(["wind_farm", "year", "current_confidence_class"], dropna=False)
        .size()
        .reset_index(name="event_count")
        .sort_values(["event_count"], ascending=False)
    )
    speed = validation["speed_summary"]
    speed_p95 = validation["speed_p95_summary"]
    gap = validation["nearest_gap_summary"]
    samples = validation["window_sample_summary"]
    distance = validation["spatial_distance_summary"]
    confidence_counts = pd.Series(validation["confidence_counts"], name="event_count").reset_index()
    confidence_counts = confidence_counts.rename(columns={"index": "current_confidence_class"})
    missing_counts = pd.Series(validation["missing_reason_counts"], name="event_count").reset_index()
    missing_counts = missing_counts.rename(columns={"index": "current_missing_reason"})
    crosstab = validation["wave_current_crosstab"].reset_index()
    crosstab = crosstab.rename(columns={"current_confidence_class": "current_confidence_class"})
    lines = [
        "# Current Confidence v1 Validation Report",
        "",
        "## Research Design",
        "",
        "Hypotheses: NWS hourly `u/v` currents can be assigned at event scale for NWS-covered normal farm-years; current evidence coverage is source/domain dependent; current severity may later help explain short dwell, repeat attempt, and high-weather success signatures; current confidence should remain separate from wave confidence until Fusion v2.",
        "",
        "Metrics: dwell and Tier A coverage, event-window sample counts, bracketing percentage, nearest time gap, current speed and direction distributions, current variability, confidence class distribution, missingness reasons, and overlap with Fusion v1 wave confidence classes.",
        "",
        "## Executive Conclusion",
        "",
        f"Current Confidence v1 writes one NWS current candidate and one current confidence row for each dwell event. It uses only the accepted local NWS hourly true `uo/vo` archive and keeps missing/non-covered events explicit rather than silently dropping them.",
        "",
        f"- Candidate table: `{candidate_path}`",
        f"- Confidence table: `{confidence_path}`",
        f"- Input dwell rows: {validation['input_dwell_rows']:,}",
        f"- Candidate rows: {validation['candidate_rows']:,}",
        f"- Confidence rows: {validation['confidence_rows']:,}",
        f"- NWS-eligible events: {validation['nws_eligible_events']:,}",
        f"- Event-scale current events: {validation['event_scale_current_events']:,}",
        f"- Tier A valid-current events: {validation['tier_a_valid_current_events']:,} of {validation['tier_a_events']:,}",
        "",
        "## Input Inventory",
        "",
        f"- Dwell rows: {validation['input_dwell_rows']:,}",
        f"- NWS current partitions: {validation['nws_current_partitions']:,}",
        f"- NWS current source rows: {validation['nws_current_rows']:,}",
        f"- Manifest rows accepted: {validation['manifest_rows']:,}",
        f"- Fusion v1 wave confidence rows: {validation['fusion_v1_rows']:,}",
        f"- Bathymetry rows: {validation['bathymetry_rows']:,}",
        "",
        "## Coverage",
        "",
        f"- All dwell events: {validation['input_dwell_rows']:,}",
        f"- NWS eligible events: {validation['nws_eligible_events']:,}",
        f"- Valid current events: {validation['valid_current_events']:,}",
        f"- Tier A events: {validation['tier_a_events']:,}",
        f"- Tier A valid current events: {validation['tier_a_valid_current_events']:,}",
        "",
        "Top farms by event-scale current coverage:",
        "",
    ]
    lines.extend(_markdown_table(coverage_by_farm, ["wind_farm", "event_count"], limit=15))
    lines.extend(["", "Confidence by year:", ""])
    lines.extend(_markdown_table(by_year, ["year", "current_confidence_class", "event_count"]))
    lines.extend(["", "Confidence by dwell tier:", ""])
    lines.extend(_markdown_table(coverage_by_tier, ["dwell_tier", "current_confidence_class", "event_count"]))
    lines.extend(["", "Farm-year confidence counts (top rows):", ""])
    lines.extend(_markdown_table(farm_year, ["wind_farm", "year", "current_confidence_class", "event_count"], limit=25))
    lines.extend(
        [
            "",
            "## Assignment Quality",
            "",
            f"- Bracketed events: {validation['bracketed_events']:,}",
            f"- Events with in-window samples: {validation['in_window_events']:,}",
            f"- Nearest time gap minutes min/p50/p95/max: {gap['min']:.3f} / {gap['p50']:.3f} / {gap['p95']:.3f} / {gap['max']:.3f}",
            f"- Event-window sample count min/p50/p95/max: {samples['min']:.3f} / {samples['p50']:.3f} / {samples['p95']:.3f} / {samples['max']:.3f}",
            f"- Source sample/grid distance km min/p50/p95/max: {distance['min']:.3f} / {distance['p50']:.3f} / {distance['p95']:.3f} / {distance['max']:.3f}",
            "",
            "## Current Physical Checks",
            "",
            f"- Aggregated `current_speed_mean = sqrt(u_mean^2 + v_mean^2)` max error: {validation['speed_consistency_max_error']:.12f}",
            f"- Direction in [0, 360): {validation['direction_ok']}",
            f"- Direction sin/cos unit-vector max error: {validation['direction_sin_cos_max_error']:.12f}",
            f"- Current speed mean min/p50/p95/max: {speed['min']:.3f} / {speed['p50']:.3f} / {speed['p95']:.3f} / {speed['max']:.3f} m/s",
            f"- Current speed p95 min/p50/p95/max: {speed_p95['min']:.3f} / {speed_p95['p50']:.3f} / {speed_p95['p95']:.3f} / {speed_p95['max']:.3f} m/s",
            "",
            "## Current Variability",
            "",
            "This event layer preserves `current_speed_mean` and `current_speed_p95` so later modelling can test whether current severity adds explanatory power beyond waves and bathymetry. Hourly archive-level variability was accepted in the NWS batch validation; event-level variability is represented here by the event-window p95 and short-window bracketing method.",
            "",
            "## Confidence",
            "",
        ]
    )
    lines.extend(_markdown_table(confidence_counts, ["current_confidence_class", "event_count"]))
    lines.extend(["", "Missing reason distribution:", ""])
    lines.extend(_markdown_table(missing_counts, ["current_missing_reason", "event_count"], limit=12))
    lines.extend(["", "## Relation To Wave Confidence", ""])
    lines.extend(_markdown_table(crosstab, list(crosstab.columns)))
    lines.extend(
        [
            "",
            f"- Events with both `A_high` wave confidence and `A_event_scale` current confidence: {validation['both_high_wave_current']:,}",
            f"- Tier A events with both high wave and event-scale current confidence: {validation['tier_a_both_high_wave_current']:,}",
            "",
            "## Research Interpretation",
            "",
            "NWS current evidence is ready for Fusion v2 where the event confidence class is `A_event_scale`; non-covered farm-years remain explicit `D_unsuitable` rows and must not be treated as zero current. Coverage is strongest in the accepted NWS normal farm-years, so Fusion v2 should report source/domain coverage bias by farm and year rather than implying Europe-wide current availability.",
            "",
            "Stress-test farm-years should remain separate because shallow/coastal model warnings were intentionally excluded from the accepted normal NWS current archive. Baltic daily current evidence remains contextual and should not be mixed into this NWS event-scale layer.",
            "",
            "## Recommendation",
            "",
            "Accept Current Confidence v1 if row identity is preserved, physical checks pass, fallback/synthetic provenance is absent, and the confidence distribution keeps missing/non-covered events visible. The next increment should be Fusion v2: wave confidence plus current confidence plus bathymetry event features, without adding new downloads or calibrated probability claims.",
        ]
    )
    return "\n".join(lines) + "\n"


def write_outputs(
    candidates: pd.DataFrame,
    confidence: pd.DataFrame,
    report: str,
    output_dir: Path,
    report_dir: Path,
    overwrite: bool,
) -> tuple[Path, Path, Path]:
    candidate_path = output_dir / CURRENT_EVENT_CANDIDATES_FILENAME
    confidence_path = output_dir / CURRENT_EVENT_CONFIDENCE_FILENAME
    report_path = report_dir / VALIDATION_REPORT_FILENAME
    existing = [path for path in (candidate_path, confidence_path, report_path) if path.exists()]
    if existing and not overwrite:
        raise FileExistsError(
            "Current Confidence v1 outputs already exist; pass overwrite=True to replace: "
            + ", ".join(str(path) for path in existing)
        )
    output_dir.mkdir(parents=True, exist_ok=True)
    report_dir.mkdir(parents=True, exist_ok=True)
    ensure_candidate_schema(candidates).to_parquet(candidate_path, index=False)
    ensure_confidence_schema(confidence).to_parquet(confidence_path, index=False)
    report_path.write_text(report, encoding="utf-8")
    return candidate_path, confidence_path, report_path


def build_current_confidence_v1(
    dwell_weather: Path = DEFAULT_DWELL_WEATHER,
    nws_current_root: Path = DEFAULT_NWS_CURRENT_ROOT,
    nws_current_manifest: Path = DEFAULT_NWS_CURRENT_MANIFEST,
    wave_confidence_path: Path = DEFAULT_WAVE_CONFIDENCE,
    bathymetry_path: Path = DEFAULT_BATHYMETRY,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    report_dir: Path = DEFAULT_REPORT_DIR,
    overwrite: bool = False,
) -> CurrentConfidenceResult:
    candidate_path = output_dir / CURRENT_EVENT_CANDIDATES_FILENAME
    confidence_path = output_dir / CURRENT_EVENT_CONFIDENCE_FILENAME
    report_path = report_dir / VALIDATION_REPORT_FILENAME
    if not overwrite:
        existing = [path for path in (candidate_path, confidence_path, report_path) if path.exists()]
        if existing:
            raise FileExistsError(
                "Current Confidence v1 outputs already exist; pass overwrite=True to replace: "
                + ", ".join(str(path) for path in existing)
            )

    dwell = load_dwell_events(dwell_weather)
    manifest = load_manifest(nws_current_manifest, nws_current_root)
    bathymetry = load_bathymetry(bathymetry_path)
    wave_confidence = load_wave_confidence(wave_confidence_path)

    candidates = build_current_event_candidates(dwell, manifest, bathymetry)
    confidence = build_current_event_confidence(candidates, wave_confidence)
    validation = validate_current_confidence(
        dwell=dwell,
        manifest=manifest,
        candidates=candidates,
        confidence=confidence,
        wave_confidence=wave_confidence,
        bathymetry=bathymetry,
    )
    report = build_validation_report(
        dwell=dwell,
        manifest=manifest,
        candidates=candidates,
        confidence=confidence,
        validation=validation,
        candidate_path=candidate_path,
        confidence_path=confidence_path,
    )
    candidate_path, confidence_path, report_path = write_outputs(
        candidates=candidates,
        confidence=confidence,
        report=report,
        output_dir=output_dir,
        report_dir=report_dir,
        overwrite=overwrite,
    )
    return CurrentConfidenceResult(
        candidate_path=candidate_path,
        confidence_path=confidence_path,
        report_path=report_path,
        candidates=candidates,
        confidence=confidence,
        validation=validation,
    )
