"""Metocean Fusion v1: wave-source agreement and confidence scoring.

Fusion v1 is a research-validation layer over existing local sources. It does
not download current data, import FINO, mutate source archives, rerun NORA3, or
rebuild the final production dwell-weather table.

The key change from Fusion v0 is that source candidates are preserved first:
NORA3, NWS, and Baltic all get event-level candidate rows where the source is
available or an explicit missing reason where it is not. Agreement and
confidence are then derived from those candidate rows.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from itertools import combinations
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from .extract_nws import haversine_distance


DEFAULT_DWELL_WEATHER_INPUT = Path(
    "Data/Processed/ais_dwell_backfill/cross_farm_dwell_weather_features.parquet"
)
DEFAULT_FUSION_V0_INPUT = Path("Data/Processed/metocean/fusion_v0/dwell_metocean_fusion_v0.parquet")
DEFAULT_NWS_ROOT = Path("Data/Processed/metocean/nws_wave_timeseries")
DEFAULT_BALTIC_ROOT = Path("Data/Processed/metocean/baltic_wave_timeseries")
DEFAULT_BATHYMETRY_POINTS = Path(
    "Data/Processed/metocean/bathymetry/site_bathymetry_points.parquet"
)
DEFAULT_OUTPUT_DIR = Path("Data/Processed/metocean/fusion_v1_source_agreement")
DEFAULT_REPORT_DIR = Path("reports/metocean_fusion_v1_source_agreement")

WAVE_SOURCE_CANDIDATES_FILENAME = "wave_source_candidates.parquet"
PAIRWISE_AGREEMENT_FILENAME = "wave_source_pairwise_agreement.parquet"
EVENT_CONFIDENCE_FILENAME = "wave_event_confidence.parquet"
VALIDATION_REPORT_FILENAME = "source_agreement_validation_report.md"

SOURCE_ORDER = ["nora3", "nws", "baltic"]
HS_STRONG_AGREEMENT_M = 0.15
HS_MODERATE_AGREEMENT_M = 0.35
TP_STRONG_AGREEMENT_S = 0.75
TP_MODERATE_AGREEMENT_S = 1.50

BASE_DWELL_COLUMNS = [
    "dwell_id",
    "visit_id",
    "wind_farm",
    "farm_id",
    "dwell_tier",
    "start_utc",
    "end_utc",
    "centroid_lat",
    "centroid_lon",
    "active_hs_mean",
    "active_tp_mean",
    "active_wave_direction_sin_mean",
    "active_wave_direction_cos_mean",
]

REQUIRED_CANDIDATE_COLUMNS = [
    "dwell_id",
    "visit_id",
    "wind_farm",
    "farm_id",
    "dwell_tier",
    "start_utc",
    "end_utc",
    "centroid_lat",
    "centroid_lon",
    "source",
    "source_product",
    "source_native_temporal_resolution_minutes",
    "source_native_spatial_resolution_km",
    "source_domain",
    "source_domain_match",
    "sample_point_id",
    "sample_point_type",
    "source_sample_lat",
    "source_sample_lon",
    "source_sample_distance_km",
    "event_window_sample_count",
    "nearest_time_gap_minutes",
    "event_bracketed_by_source_times",
    "temporal_assignment_method",
    "spatial_assignment_method",
    "hs_mean",
    "tp_mean",
    "wave_direction_sin_mean",
    "wave_direction_cos_mean",
    "variable_completeness_score",
    "source_missing_reason",
    "water_depth_m",
    "shallow_water_flag",
    "coastal_complexity_flag",
    "source_quality_notes",
]

REQUIRED_PAIRWISE_COLUMNS = [
    "dwell_id",
    "wind_farm",
    "dwell_tier",
    "source_a",
    "source_b",
    "hs_a",
    "hs_b",
    "tp_a",
    "tp_b",
    "wave_direction_a_deg",
    "wave_direction_b_deg",
    "hs_diff",
    "hs_abs_diff",
    "tp_diff",
    "tp_abs_diff",
    "direction_circular_diff_deg",
    "source_a_distance_km",
    "source_b_distance_km",
    "source_a_time_gap_min",
    "source_b_time_gap_min",
    "water_depth_m",
    "agreement_class",
]

REQUIRED_CONFIDENCE_COLUMNS = [
    "dwell_id",
    "visit_id",
    "wind_farm",
    "farm_id",
    "dwell_tier",
    "valid_source_count",
    "available_sources",
    "selected_wave_source",
    "selected_hs_mean",
    "selected_tp_mean",
    "selected_wave_direction_sin_mean",
    "selected_wave_direction_cos_mean",
    "weighted_hs_mean",
    "weighted_tp_mean",
    "source_disagreement_hs_range",
    "source_disagreement_tp_range",
    "source_disagreement_direction_range_deg",
    "wave_confidence_score",
    "wave_confidence_class",
    "selection_method",
    "selection_reason",
    "exclude_from_high_confidence_boundary",
    "water_depth_m",
    "shallow_water_flag",
]


@dataclass(frozen=True)
class ArchiveSourceConfig:
    source: str
    root: Path
    source_product: str
    source_native_temporal_resolution_minutes: int
    source_native_spatial_resolution_km: float
    source_domain: str
    timestamp_col: str
    hs_col: str
    tp_col: str
    direction_col: str
    grid_distance_col: str
    grid_lat_col: str
    grid_lon_col: str
    source_file_col: str
    extraction_method_col: str
    spatial_match_status_col: str
    native_tolerance_minutes: int


@dataclass(frozen=True)
class FusionV1Result:
    candidate_path: Path
    pairwise_path: Path
    confidence_path: Path
    report_path: Path
    candidates: pd.DataFrame
    pairwise: pd.DataFrame
    confidence: pd.DataFrame
    validation: dict[str, Any]


NWS_CONFIG = ArchiveSourceConfig(
    source="nws",
    root=DEFAULT_NWS_ROOT,
    source_product="Copernicus NWS Wave Reanalysis / local_nws_wave_reanalysis",
    source_native_temporal_resolution_minutes=180,
    source_native_spatial_resolution_km=1.5,
    source_domain="North West Shelf wave domain; local archive bounded by accepted farm/year partitions",
    timestamp_col="timestamp_utc",
    hs_col="nws_wave_hs",
    tp_col="nws_wave_tp",
    direction_col="nws_wave_dir",
    grid_distance_col="nws_spatial_distance_km",
    grid_lat_col="nws_grid_lat",
    grid_lon_col="nws_grid_lon",
    source_file_col="nws_source_file",
    extraction_method_col="nws_extraction_method",
    spatial_match_status_col="nws_spatial_match_status",
    native_tolerance_minutes=180,
)

BALTIC_CONFIG = ArchiveSourceConfig(
    source="baltic",
    root=DEFAULT_BALTIC_ROOT,
    source_product="BALTICSEA_MULTIYEAR_WAV_003_015 / cmems_mod_bal_wav_my_PT1H-i",
    source_native_temporal_resolution_minutes=60,
    source_native_spatial_resolution_km=1.85,
    source_domain="Baltic Sea Copernicus wave domain; local accepted Baltic farm partitions",
    timestamp_col="timestamp_utc",
    hs_col="baltic_wave_hs",
    tp_col="baltic_wave_tp",
    direction_col="baltic_wave_dir",
    grid_distance_col="baltic_spatial_distance_km",
    grid_lat_col="baltic_grid_lat",
    grid_lon_col="baltic_grid_lon",
    source_file_col="baltic_source_file",
    extraction_method_col="baltic_extraction_method",
    spatial_match_status_col="baltic_spatial_match_status",
    native_tolerance_minutes=60,
)


def _normalise_timestamp(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, utc=True, errors="coerce")


def _source_available(frame: pd.DataFrame) -> pd.Series:
    hs = pd.to_numeric(frame["hs_mean"], errors="coerce")
    tp = pd.to_numeric(frame["tp_mean"], errors="coerce")
    return hs.notna() & tp.notna() & (hs >= 0.0) & (tp > 0.0)


def _valid_pair_mask(frame: pd.DataFrame, hs_col: str, tp_col: str) -> pd.Series:
    hs = pd.to_numeric(frame[hs_col], errors="coerce")
    tp = pd.to_numeric(frame[tp_col], errors="coerce")
    return hs.notna() & tp.notna() & (hs >= 0.0) & (tp > 0.0)


def _direction_degrees(sin_value: Any, cos_value: Any) -> float:
    if pd.isna(sin_value) or pd.isna(cos_value):
        return np.nan
    return float(np.degrees(np.arctan2(float(sin_value), float(cos_value))) % 360.0)


def _circular_diff_deg(left: Any, right: Any) -> float:
    if pd.isna(left) or pd.isna(right):
        return np.nan
    return float(abs((float(left) - float(right) + 180.0) % 360.0 - 180.0))


def _circular_range_deg(values: list[float]) -> float:
    clean = [float(value) for value in values if pd.notna(value)]
    if len(clean) < 2:
        return np.nan
    return float(max(_circular_diff_deg(a, b) for a, b in combinations(clean, 2)))


def _mean_direction_components(direction_degrees: pd.Series) -> tuple[float, float]:
    direction = pd.to_numeric(direction_degrees, errors="coerce").dropna()
    if direction.empty:
        return np.nan, np.nan
    radians = np.deg2rad(direction.to_numpy(dtype=float))
    return float(np.sin(radians).mean()), float(np.cos(radians).mean())


def _variable_completeness(hs: Any, tp: Any, direction_sin: Any, direction_cos: Any) -> float:
    present = 0
    if pd.notna(hs):
        present += 1
    if pd.notna(tp):
        present += 1
    if pd.notna(direction_sin) and pd.notna(direction_cos):
        present += 1
    return float(present / 3.0)


def _year_from_path(path: Path) -> int | None:
    for part in path.parts:
        if part.startswith("year="):
            try:
                return int(part.split("=", 1)[1])
            except ValueError:
                return None
    return None


def _farm_slug_from_path(path: Path) -> str | None:
    for part in path.parts:
        if part.startswith("wind_farm="):
            return part.split("=", 1)[1]
    return None


def _build_partition_index(root: Path) -> dict[tuple[str, int], Path]:
    if not root.exists():
        raise FileNotFoundError(f"Wave source root does not exist: {root}")
    index: dict[tuple[str, int], Path] = {}
    for path in sorted(root.rglob("*.parquet")):
        rel_parts = path.relative_to(root).parts
        if any(part.startswith("_") for part in rel_parts):
            continue
        farm_slug = _farm_slug_from_path(path)
        year = _year_from_path(path)
        if farm_slug is None or year is None:
            continue
        index[(farm_slug, year)] = path
    return index


def _archive_inventory(root: Path) -> dict[str, Any]:
    paths = []
    if root.exists():
        for path in sorted(root.rglob("*.parquet")):
            rel_parts = path.relative_to(root).parts
            if any(part.startswith("_") for part in rel_parts):
                continue
            paths.append(path)
    farms = sorted({farm for path in paths if (farm := _farm_slug_from_path(path))})
    years = sorted({year for path in paths if (year := _year_from_path(path)) is not None})
    row_count = 0
    for path in paths:
        row_count += int(pq.ParquetFile(path).metadata.num_rows)
    return {
        "exists": root.exists(),
        "partition_count": int(len(paths)),
        "farm_count": int(len(farms)),
        "row_count": int(row_count),
        "first_year": int(min(years)) if years else None,
        "last_year": int(max(years)) if years else None,
    }


def _load_dwell_weather(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Dwell-weather table not found: {path}")
    dwell = pd.read_parquet(path)
    missing = sorted(set(BASE_DWELL_COLUMNS) - set(dwell.columns))
    if missing:
        raise ValueError(f"Dwell-weather table missing required columns: {missing}")
    dwell = dwell.copy()
    dwell["_fusion_v1_row_id"] = np.arange(len(dwell), dtype=np.int64)
    dwell["start_utc"] = _normalise_timestamp(dwell["start_utc"])
    dwell["end_utc"] = _normalise_timestamp(dwell["end_utc"])
    dwell["_event_midpoint_utc"] = dwell["start_utc"] + (dwell["end_utc"] - dwell["start_utc"]) / 2
    dwell["_event_year"] = dwell["start_utc"].dt.year
    dwell["_source_farm_name"] = dwell["farm_id"].where(dwell["farm_id"].notna(), dwell["wind_farm"])
    if "active_n_weather_records" not in dwell:
        dwell["active_n_weather_records"] = np.nan
    if "active_source_available" not in dwell:
        dwell["active_source_available"] = dwell["active_hs_mean"].notna() & dwell["active_tp_mean"].notna()
    return dwell


def _load_bathymetry(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Bathymetry point table not found: {path}")
    required = {
        "wind_farm",
        "sample_point_id",
        "sample_point_type",
        "lat",
        "lon",
        "water_depth_m",
        "bathymetry_source",
        "bathymetry_distance_m",
        "bathymetry_spatial_match_status",
    }
    bathymetry = pd.read_parquet(path)
    missing = sorted(required - set(bathymetry.columns))
    if missing:
        raise ValueError(f"Bathymetry point table missing required columns: {missing}")
    return bathymetry.copy()


def _nearest_point_for_events(events: pd.DataFrame, points: pd.DataFrame) -> pd.DataFrame:
    assigned = pd.DataFrame(index=events.index)
    assigned["sample_point_id"] = pd.NA
    assigned["sample_point_type"] = pd.NA
    assigned["sample_lat"] = np.nan
    assigned["sample_lon"] = np.nan
    assigned["event_to_sample_distance_km"] = np.nan
    assigned["assignment_method"] = "missing_farm_points"

    if points.empty or events.empty:
        return assigned

    point_lats = points["lat"].astype(float).to_numpy()
    point_lons = points["lon"].astype(float).to_numpy()
    farm_centroid_points = points[points["sample_point_id"].eq("farm_centroid")]
    fallback_point = farm_centroid_points.iloc[0] if not farm_centroid_points.empty else points.iloc[0]

    for event_index, event in events.iterrows():
        lat = event.get("centroid_lat")
        lon = event.get("centroid_lon")
        if pd.notna(lat) and pd.notna(lon):
            distances = haversine_distance(float(lat), float(lon), point_lats, point_lons)
            nearest_pos = int(np.nanargmin(distances))
            point = points.iloc[nearest_pos]
            distance = float(distances[nearest_pos])
            method = "nearest_point_to_dwell_centroid"
        else:
            point = fallback_point
            distance = np.nan
            method = "farm_centroid_fallback_missing_dwell_centroid"
        assigned.loc[event_index, "sample_point_id"] = point["sample_point_id"]
        assigned.loc[event_index, "sample_point_type"] = point["sample_point_type"]
        assigned.loc[event_index, "sample_lat"] = float(point["lat"])
        assigned.loc[event_index, "sample_lon"] = float(point["lon"])
        assigned.loc[event_index, "event_to_sample_distance_km"] = distance
        assigned.loc[event_index, "assignment_method"] = method
    return assigned


def assign_bathymetry_to_events(dwell: pd.DataFrame, bathymetry: pd.DataFrame) -> pd.DataFrame:
    result = pd.DataFrame(index=dwell.index)
    for column in [
        "water_depth_m",
        "bathymetry_source",
        "bathymetry_distance_m",
        "bathymetry_spatial_match_status",
        "bathymetry_assignment_method",
        "bathymetry_sample_point_id",
        "bathymetry_sample_point_type",
        "bathymetry_sample_lat",
        "bathymetry_sample_lon",
        "bathymetry_event_to_sample_distance_km",
    ]:
        result[column] = pd.NA

    for farm_name, events in dwell.groupby("_source_farm_name", dropna=False):
        points = bathymetry[bathymetry["wind_farm"].eq(farm_name)]
        nearest = _nearest_point_for_events(events, points)
        if points.empty:
            result.loc[events.index, "bathymetry_assignment_method"] = "missing_farm_bathymetry_points"
            result.loc[events.index, "bathymetry_spatial_match_status"] = "missing_farm"
            continue
        joined = nearest.merge(
            points[
                [
                    "sample_point_id",
                    "water_depth_m",
                    "bathymetry_source",
                    "bathymetry_distance_m",
                    "bathymetry_spatial_match_status",
                ]
            ],
            on="sample_point_id",
            how="left",
        )
        result.loc[events.index, "water_depth_m"] = joined["water_depth_m"].to_numpy()
        result.loc[events.index, "bathymetry_source"] = joined["bathymetry_source"].to_numpy()
        result.loc[events.index, "bathymetry_distance_m"] = joined["bathymetry_distance_m"].to_numpy()
        result.loc[events.index, "bathymetry_spatial_match_status"] = joined[
            "bathymetry_spatial_match_status"
        ].to_numpy()
        result.loc[events.index, "bathymetry_assignment_method"] = joined["assignment_method"].to_numpy()
        result.loc[events.index, "bathymetry_sample_point_id"] = joined["sample_point_id"].to_numpy()
        result.loc[events.index, "bathymetry_sample_point_type"] = joined["sample_point_type"].to_numpy()
        result.loc[events.index, "bathymetry_sample_lat"] = joined["sample_lat"].to_numpy()
        result.loc[events.index, "bathymetry_sample_lon"] = joined["sample_lon"].to_numpy()
        result.loc[events.index, "bathymetry_event_to_sample_distance_km"] = joined[
            "event_to_sample_distance_km"
        ].to_numpy()
    return result


def _base_candidate_frame(dwell: pd.DataFrame, bathymetry_assignment: pd.DataFrame) -> pd.DataFrame:
    frame = dwell[
        [
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
    ].copy()
    frame["water_depth_m"] = pd.to_numeric(bathymetry_assignment["water_depth_m"], errors="coerce")
    frame["water_depth_le_1m"] = frame["water_depth_m"] <= 1.0
    frame["water_depth_le_5m"] = frame["water_depth_m"] <= 5.0
    frame["water_depth_le_10m"] = frame["water_depth_m"] <= 10.0
    frame["shallow_water_flag"] = frame["water_depth_le_10m"].fillna(False)
    return frame


def _finish_candidate_columns(frame: pd.DataFrame) -> pd.DataFrame:
    depth = pd.to_numeric(frame["water_depth_m"], errors="coerce")
    sample_distance = pd.to_numeric(frame["source_sample_distance_km"], errors="coerce")
    frame["water_depth_le_1m"] = depth <= 1.0
    frame["water_depth_le_5m"] = depth <= 5.0
    frame["water_depth_le_10m"] = depth <= 10.0
    frame["shallow_water_flag"] = frame["water_depth_le_10m"].fillna(False)
    frame["coastal_complexity_flag"] = (
        frame["shallow_water_flag"] | (sample_distance > 5.0)
    ).fillna(False)
    for column in REQUIRED_CANDIDATE_COLUMNS:
        if column not in frame.columns:
            frame[column] = pd.NA
    preferred = REQUIRED_CANDIDATE_COLUMNS + [
        "water_depth_le_1m",
        "water_depth_le_5m",
        "water_depth_le_10m",
        "source_grid_lat",
        "source_grid_lon",
        "source_grid_distance_km",
        "source_file",
        "source_extraction_method",
        "source_spatial_match_status",
    ]
    return frame[[column for column in preferred if column in frame.columns]]


def build_nora3_candidates(dwell: pd.DataFrame, bathymetry_assignment: pd.DataFrame) -> pd.DataFrame:
    candidates = _base_candidate_frame(dwell, bathymetry_assignment)
    candidates["source"] = "nora3"
    candidates["source_product"] = "MET Norway NORA3 wave hindcast via existing active dwell-weather join"
    candidates["source_native_temporal_resolution_minutes"] = 60
    candidates["source_native_spatial_resolution_km"] = 3.0
    candidates["source_domain"] = "NORA3 regional wave hindcast; existing event-aggregated active fields"
    valid = _valid_pair_mask(dwell, "active_hs_mean", "active_tp_mean")
    candidates["source_domain_match"] = dwell["active_source_available"].fillna(valid).astype(bool)
    candidates["sample_point_id"] = pd.NA
    candidates["sample_point_type"] = pd.NA
    candidates["source_sample_lat"] = np.nan
    candidates["source_sample_lon"] = np.nan
    candidates["source_sample_distance_km"] = np.nan
    candidates["event_window_sample_count"] = pd.to_numeric(
        dwell["active_n_weather_records"], errors="coerce"
    )
    candidates["nearest_time_gap_minutes"] = np.nan
    candidates["event_bracketed_by_source_times"] = pd.NA
    candidates["temporal_assignment_method"] = "nora3_existing_active_event_aggregate"
    candidates["spatial_assignment_method"] = "nora3_existing_join_grid_distance_unavailable"
    candidates["hs_mean"] = pd.to_numeric(dwell["active_hs_mean"], errors="coerce")
    candidates["tp_mean"] = pd.to_numeric(dwell["active_tp_mean"], errors="coerce")
    candidates["wave_direction_sin_mean"] = pd.to_numeric(
        dwell["active_wave_direction_sin_mean"], errors="coerce"
    )
    candidates["wave_direction_cos_mean"] = pd.to_numeric(
        dwell["active_wave_direction_cos_mean"], errors="coerce"
    )
    candidates["variable_completeness_score"] = [
        _variable_completeness(hs, tp, sin_value, cos_value)
        for hs, tp, sin_value, cos_value in candidates[
            ["hs_mean", "tp_mean", "wave_direction_sin_mean", "wave_direction_cos_mean"]
        ].itertuples(index=False, name=None)
    ]
    candidates["source_missing_reason"] = pd.NA
    candidates.loc[~valid, "source_missing_reason"] = "nora3_active_hs_tp_missing"
    candidates["source_quality_notes"] = (
        "pre-aggregated active dwell-weather field; exact NORA3 grid/sample distance is not available"
    )
    candidates["source_grid_lat"] = np.nan
    candidates["source_grid_lon"] = np.nan
    candidates["source_grid_distance_km"] = np.nan
    candidates["source_file"] = pd.NA
    candidates["source_extraction_method"] = "existing_dwell_weather_join"
    candidates["source_spatial_match_status"] = "grid_distance_unavailable"
    return _finish_candidate_columns(candidates)


def _source_points_from_archive(source: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "sample_point_id",
        "sample_point_type",
        "lat",
        "lon",
    ]
    return (
        source[columns]
        .dropna(subset=["sample_point_id", "lat", "lon"])
        .drop_duplicates("sample_point_id")
        .reset_index(drop=True)
    )


def _aggregate_archive_event_source(
    sample_source: pd.DataFrame,
    event: pd.Series,
    config: ArchiveSourceConfig,
) -> dict[str, Any]:
    if sample_source.empty:
        return {
            "hs_mean": np.nan,
            "tp_mean": np.nan,
            "wave_direction_sin_mean": np.nan,
            "wave_direction_cos_mean": np.nan,
            "event_window_sample_count": 0,
            "nearest_time_gap_minutes": np.nan,
            "event_bracketed_by_source_times": False,
            "temporal_assignment_method": pd.NA,
            "source_missing_reason": "source_sample_point_missing",
            "source_domain_match": False,
            "source_grid_lat": np.nan,
            "source_grid_lon": np.nan,
            "source_grid_distance_km": np.nan,
            "source_file": pd.NA,
            "source_extraction_method": pd.NA,
            "source_spatial_match_status": pd.NA,
            "source_quality_notes": "source sample point is missing from archive partition",
        }

    start = event["start_utc"]
    end = event["end_utc"]
    midpoint = event["_event_midpoint_utc"]
    if pd.isna(start) or pd.isna(end) or pd.isna(midpoint):
        return {
            "hs_mean": np.nan,
            "tp_mean": np.nan,
            "wave_direction_sin_mean": np.nan,
            "wave_direction_cos_mean": np.nan,
            "event_window_sample_count": 0,
            "nearest_time_gap_minutes": np.nan,
            "event_bracketed_by_source_times": False,
            "temporal_assignment_method": pd.NA,
            "source_missing_reason": "missing_event_timestamp",
            "source_domain_match": False,
            "source_grid_lat": np.nan,
            "source_grid_lon": np.nan,
            "source_grid_distance_km": np.nan,
            "source_file": pd.NA,
            "source_extraction_method": pd.NA,
            "source_spatial_match_status": pd.NA,
            "source_quality_notes": "event timestamp is missing or invalid",
        }

    timestamps = sample_source[config.timestamp_col]
    deltas = (timestamps - midpoint).abs()
    nearest_time_gap_minutes = float(deltas.min().total_seconds() / 60.0) if not deltas.empty else np.nan
    bracketed = bool((timestamps.min() <= start) and (timestamps.max() >= end)) if not timestamps.empty else False
    in_window = sample_source[timestamps.between(start, end, inclusive="both")]
    event_window_sample_count = int(len(in_window))

    if in_window.empty:
        if pd.isna(nearest_time_gap_minutes) or nearest_time_gap_minutes > config.native_tolerance_minutes:
            return {
                "hs_mean": np.nan,
                "tp_mean": np.nan,
                "wave_direction_sin_mean": np.nan,
                "wave_direction_cos_mean": np.nan,
                "event_window_sample_count": event_window_sample_count,
                "nearest_time_gap_minutes": nearest_time_gap_minutes,
                "event_bracketed_by_source_times": bracketed,
                "temporal_assignment_method": pd.NA,
                "source_missing_reason": "event_time_outside_source_tolerance",
                "source_domain_match": False,
                "source_grid_lat": np.nan,
                "source_grid_lon": np.nan,
                "source_grid_distance_km": np.nan,
                "source_file": pd.NA,
                "source_extraction_method": pd.NA,
                "source_spatial_match_status": pd.NA,
                "source_quality_notes": "no event-window sample and nearest timestamp exceeds tolerance",
            }
        effective = sample_source.loc[[deltas.idxmin()]]
        temporal_assignment_method = (
            f"{config.source}_nearest_sample_midpoint_within_{config.native_tolerance_minutes}min"
        )
    else:
        effective = in_window
        temporal_assignment_method = f"{config.source}_event_window_mean"

    hs = pd.to_numeric(effective[config.hs_col], errors="coerce")
    tp = pd.to_numeric(effective[config.tp_col], errors="coerce")
    pair_mask = hs.notna() & tp.notna() & (hs >= 0.0) & (tp > 0.0)
    metadata_row = effective.iloc[0]
    spatial_status = metadata_row.get(config.spatial_match_status_col)
    source_grid_distance = metadata_row.get(config.grid_distance_col)
    source_domain_match = bool(str(spatial_status).lower() == "ok") if pd.notna(spatial_status) else False
    notes = f"grid_distance_km={source_grid_distance}; spatial_match_status={spatial_status}"

    if not pair_mask.any():
        return {
            "hs_mean": np.nan,
            "tp_mean": np.nan,
            "wave_direction_sin_mean": np.nan,
            "wave_direction_cos_mean": np.nan,
            "event_window_sample_count": event_window_sample_count,
            "nearest_time_gap_minutes": nearest_time_gap_minutes,
            "event_bracketed_by_source_times": bracketed,
            "temporal_assignment_method": temporal_assignment_method,
            "source_missing_reason": "source_values_missing",
            "source_domain_match": source_domain_match,
            "source_grid_lat": metadata_row.get(config.grid_lat_col, np.nan),
            "source_grid_lon": metadata_row.get(config.grid_lon_col, np.nan),
            "source_grid_distance_km": source_grid_distance,
            "source_file": metadata_row.get(config.source_file_col, pd.NA),
            "source_extraction_method": metadata_row.get(config.extraction_method_col, pd.NA),
            "source_spatial_match_status": spatial_status,
            "source_quality_notes": notes,
        }

    dir_sin, dir_cos = _mean_direction_components(effective.loc[pair_mask, config.direction_col])
    return {
        "hs_mean": float(hs.loc[pair_mask].mean()),
        "tp_mean": float(tp.loc[pair_mask].mean()),
        "wave_direction_sin_mean": dir_sin,
        "wave_direction_cos_mean": dir_cos,
        "event_window_sample_count": event_window_sample_count,
        "nearest_time_gap_minutes": nearest_time_gap_minutes,
        "event_bracketed_by_source_times": bracketed,
        "temporal_assignment_method": temporal_assignment_method,
        "source_missing_reason": pd.NA,
        "source_domain_match": source_domain_match,
        "source_grid_lat": metadata_row.get(config.grid_lat_col, np.nan),
        "source_grid_lon": metadata_row.get(config.grid_lon_col, np.nan),
        "source_grid_distance_km": source_grid_distance,
        "source_file": metadata_row.get(config.source_file_col, pd.NA),
        "source_extraction_method": metadata_row.get(config.extraction_method_col, pd.NA),
        "source_spatial_match_status": spatial_status,
        "source_quality_notes": notes,
    }


def build_archive_source_candidates(
    dwell: pd.DataFrame,
    bathymetry_assignment: pd.DataFrame,
    config: ArchiveSourceConfig,
) -> pd.DataFrame:
    file_index = _build_partition_index(config.root)
    candidates = _base_candidate_frame(dwell, bathymetry_assignment)
    candidates["source"] = config.source
    candidates["source_product"] = config.source_product
    candidates["source_native_temporal_resolution_minutes"] = config.source_native_temporal_resolution_minutes
    candidates["source_native_spatial_resolution_km"] = config.source_native_spatial_resolution_km
    candidates["source_domain"] = config.source_domain
    candidates["source_domain_match"] = False
    candidates["sample_point_id"] = pd.NA
    candidates["sample_point_type"] = pd.NA
    candidates["source_sample_lat"] = np.nan
    candidates["source_sample_lon"] = np.nan
    candidates["source_sample_distance_km"] = np.nan
    candidates["event_window_sample_count"] = 0
    candidates["nearest_time_gap_minutes"] = np.nan
    candidates["event_bracketed_by_source_times"] = False
    candidates["temporal_assignment_method"] = pd.NA
    candidates["spatial_assignment_method"] = pd.NA
    candidates["hs_mean"] = np.nan
    candidates["tp_mean"] = np.nan
    candidates["wave_direction_sin_mean"] = np.nan
    candidates["wave_direction_cos_mean"] = np.nan
    candidates["variable_completeness_score"] = 0.0
    candidates["source_missing_reason"] = "source_partition_missing"
    candidates["source_quality_notes"] = "source partition missing for farm/year"
    candidates["source_grid_lat"] = np.nan
    candidates["source_grid_lon"] = np.nan
    candidates["source_grid_distance_km"] = np.nan
    candidates["source_file"] = pd.NA
    candidates["source_extraction_method"] = pd.NA
    candidates["source_spatial_match_status"] = pd.NA

    grouped = dwell.groupby(["wind_farm", "_event_year"], dropna=False)
    read_columns = [
        config.timestamp_col,
        "sample_point_id",
        "sample_point_type",
        "lat",
        "lon",
        config.grid_lat_col,
        config.grid_lon_col,
        config.grid_distance_col,
        config.source_file_col,
        config.extraction_method_col,
        config.spatial_match_status_col,
        config.hs_col,
        config.tp_col,
        config.direction_col,
    ]

    for (farm_slug, year), events in grouped:
        if pd.isna(farm_slug) or pd.isna(year):
            candidates.loc[events.index, "source_missing_reason"] = "missing_event_farm_or_year"
            candidates.loc[events.index, "source_quality_notes"] = "event farm or year missing"
            continue
        partition = file_index.get((str(farm_slug), int(year)))
        if partition is None:
            continue

        source = pd.read_parquet(partition, columns=read_columns)
        source[config.timestamp_col] = _normalise_timestamp(source[config.timestamp_col])
        source = source.sort_values(config.timestamp_col)
        source_points = _source_points_from_archive(source)
        nearest = _nearest_point_for_events(events, source_points)
        candidates.loc[events.index, "sample_point_id"] = nearest["sample_point_id"].to_numpy()
        candidates.loc[events.index, "sample_point_type"] = nearest["sample_point_type"].to_numpy()
        candidates.loc[events.index, "source_sample_lat"] = nearest["sample_lat"].to_numpy()
        candidates.loc[events.index, "source_sample_lon"] = nearest["sample_lon"].to_numpy()
        candidates.loc[events.index, "source_sample_distance_km"] = nearest[
            "event_to_sample_distance_km"
        ].to_numpy()
        candidates.loc[events.index, "spatial_assignment_method"] = nearest["assignment_method"].to_numpy()
        candidates.loc[events.index, "source_missing_reason"] = "source_sample_point_missing"
        candidates.loc[events.index, "source_quality_notes"] = "source sample point missing"

        working = events.join(nearest)
        for sample_point_id, sample_events in working.groupby("sample_point_id", dropna=False):
            if pd.isna(sample_point_id):
                candidates.loc[sample_events.index, "spatial_assignment_method"] = "missing_source_sample_point"
                continue
            sample_source = source[source["sample_point_id"].eq(sample_point_id)]
            for event_index, event in sample_events.iterrows():
                aggregate = _aggregate_archive_event_source(sample_source, event, config)
                for key, value in aggregate.items():
                    candidates.loc[event_index, key] = value
                candidates.loc[event_index, "variable_completeness_score"] = _variable_completeness(
                    aggregate["hs_mean"],
                    aggregate["tp_mean"],
                    aggregate["wave_direction_sin_mean"],
                    aggregate["wave_direction_cos_mean"],
                )

    return _finish_candidate_columns(candidates)


def build_wave_source_candidates(
    dwell: pd.DataFrame,
    bathymetry: pd.DataFrame,
    nws_root: Path = DEFAULT_NWS_ROOT,
    baltic_root: Path = DEFAULT_BALTIC_ROOT,
) -> pd.DataFrame:
    bathymetry_assignment = assign_bathymetry_to_events(dwell, bathymetry)
    nws_config = ArchiveSourceConfig(**{**NWS_CONFIG.__dict__, "root": nws_root})
    baltic_config = ArchiveSourceConfig(**{**BALTIC_CONFIG.__dict__, "root": baltic_root})
    candidates = pd.concat(
        [
            build_nora3_candidates(dwell, bathymetry_assignment),
            build_archive_source_candidates(dwell, bathymetry_assignment, nws_config),
            build_archive_source_candidates(dwell, bathymetry_assignment, baltic_config),
        ],
        ignore_index=True,
    )
    candidates["source"] = pd.Categorical(candidates["source"], categories=SOURCE_ORDER, ordered=True)
    return candidates.sort_values(["dwell_id", "source"]).reset_index(drop=True)


def _agreement_class(hs_abs_diff: Any, tp_abs_diff: Any) -> str:
    if pd.isna(hs_abs_diff) or pd.isna(tp_abs_diff):
        return "insufficient_overlap"
    if hs_abs_diff <= HS_STRONG_AGREEMENT_M and tp_abs_diff <= TP_STRONG_AGREEMENT_S:
        return "strong_agreement"
    if hs_abs_diff <= HS_MODERATE_AGREEMENT_M and tp_abs_diff <= TP_MODERATE_AGREEMENT_S:
        return "moderate_agreement"
    return "weak_agreement"


def compute_pairwise_agreement(candidates: pd.DataFrame) -> pd.DataFrame:
    valid = candidates[_source_available(candidates)].copy()
    rows: list[dict[str, Any]] = []
    for dwell_id, group in valid.groupby("dwell_id", sort=False):
        if len(group) < 2:
            continue
        group = group.sort_values("source", key=lambda s: s.map({source: i for i, source in enumerate(SOURCE_ORDER)}))
        for left_index, right_index in combinations(group.index, 2):
            left = group.loc[left_index]
            right = group.loc[right_index]
            direction_a = _direction_degrees(
                left["wave_direction_sin_mean"], left["wave_direction_cos_mean"]
            )
            direction_b = _direction_degrees(
                right["wave_direction_sin_mean"], right["wave_direction_cos_mean"]
            )
            hs_diff = float(left["hs_mean"]) - float(right["hs_mean"])
            tp_diff = float(left["tp_mean"]) - float(right["tp_mean"])
            hs_abs_diff = abs(hs_diff)
            tp_abs_diff = abs(tp_diff)
            rows.append(
                {
                    "dwell_id": dwell_id,
                    "wind_farm": left["wind_farm"],
                    "dwell_tier": left["dwell_tier"],
                    "source_a": left["source"],
                    "source_b": right["source"],
                    "hs_a": float(left["hs_mean"]),
                    "hs_b": float(right["hs_mean"]),
                    "tp_a": float(left["tp_mean"]),
                    "tp_b": float(right["tp_mean"]),
                    "wave_direction_a_deg": direction_a,
                    "wave_direction_b_deg": direction_b,
                    "hs_diff": hs_diff,
                    "hs_abs_diff": hs_abs_diff,
                    "tp_diff": tp_diff,
                    "tp_abs_diff": tp_abs_diff,
                    "direction_circular_diff_deg": _circular_diff_deg(direction_a, direction_b),
                    "source_a_distance_km": left["source_sample_distance_km"],
                    "source_b_distance_km": right["source_sample_distance_km"],
                    "source_a_time_gap_min": left["nearest_time_gap_minutes"],
                    "source_b_time_gap_min": right["nearest_time_gap_minutes"],
                    "water_depth_m": left["water_depth_m"],
                    "agreement_class": _agreement_class(hs_abs_diff, tp_abs_diff),
                }
            )
    pairwise = pd.DataFrame(rows)
    for column in REQUIRED_PAIRWISE_COLUMNS:
        if column not in pairwise.columns:
            pairwise[column] = pd.NA
    return pairwise[REQUIRED_PAIRWISE_COLUMNS]


def _candidate_quality_score(candidate: pd.Series) -> float:
    if not (
        pd.notna(candidate.get("hs_mean"))
        and pd.notna(candidate.get("tp_mean"))
        and float(candidate.get("hs_mean")) >= 0.0
        and float(candidate.get("tp_mean")) > 0.0
    ):
        return 0.0

    variable_score = float(candidate.get("variable_completeness_score") or 0.0)
    if candidate.get("source") == "nora3":
        temporal_score = 0.75 if pd.notna(candidate.get("event_window_sample_count")) else 0.60
        spatial_score = 0.55
    else:
        native_minutes = float(candidate.get("source_native_temporal_resolution_minutes") or 60.0)
        gap = candidate.get("nearest_time_gap_minutes")
        if pd.isna(gap):
            temporal_score = 0.20
        elif float(gap) <= native_minutes / 2.0:
            temporal_score = 1.00
        elif float(gap) <= native_minutes:
            temporal_score = 0.80
        elif float(gap) <= native_minutes * 2.0:
            temporal_score = 0.50
        else:
            temporal_score = 0.20
        if candidate.get("event_bracketed_by_source_times") is False:
            temporal_score *= 0.80

        distance = candidate.get("source_sample_distance_km")
        if pd.isna(distance):
            spatial_score = 0.30
        elif float(distance) <= 1.0:
            spatial_score = 1.00
        elif float(distance) <= 3.0:
            spatial_score = 0.90
        elif float(distance) <= 5.0:
            spatial_score = 0.75
        elif float(distance) <= 10.0:
            spatial_score = 0.50
        else:
            spatial_score = 0.30

    domain_score = 1.0 if bool(candidate.get("source_domain_match")) else 0.65
    depth_factor = 0.85 if bool(candidate.get("shallow_water_flag")) else 1.0
    return float((0.40 * variable_score + 0.30 * temporal_score + 0.20 * spatial_score + 0.10 * domain_score) * depth_factor)


def _source_disagreement_score(source: str, candidates: pd.DataFrame) -> float:
    row = candidates[candidates["source"].eq(source)].iloc[0]
    others = candidates[~candidates["source"].eq(source)]
    if others.empty:
        return 0.0
    scores = []
    for _, other in others.iterrows():
        hs_norm = abs(float(row["hs_mean"]) - float(other["hs_mean"])) / HS_MODERATE_AGREEMENT_M
        tp_norm = abs(float(row["tp_mean"]) - float(other["tp_mean"])) / TP_MODERATE_AGREEMENT_S
        scores.append((hs_norm + tp_norm) / 2.0)
    return float(np.mean(scores)) if scores else 0.0


def _weighted_value(candidates: pd.DataFrame, value_col: str) -> float:
    weights = []
    values = []
    for _, row in candidates.iterrows():
        value = row.get(value_col)
        if pd.isna(value):
            continue
        quality = _candidate_quality_score(row)
        disagreement = _source_disagreement_score(str(row["source"]), candidates) if len(candidates) > 1 else 0.0
        weight = quality / (1.0 + disagreement)
        if weight <= 0.0:
            continue
        weights.append(weight)
        values.append(float(value))
    if not weights:
        return np.nan
    return float(np.average(values, weights=weights))


def _select_candidate(valid: pd.DataFrame) -> pd.Series:
    if len(valid) == 1:
        return valid.iloc[0]
    ranked = []
    for _, row in valid.iterrows():
        disagreement = _source_disagreement_score(str(row["source"]), valid)
        quality = _candidate_quality_score(row)
        ranked.append((disagreement, -quality, SOURCE_ORDER.index(str(row["source"])), row.name))
    ranked.sort()
    return valid.loc[ranked[0][3]]


def _classify_confidence(
    valid: pd.DataFrame,
    event_pairwise: pd.DataFrame,
    selected: pd.Series | None,
    shallow: bool,
) -> tuple[float, str, str]:
    if valid.empty or selected is None:
        return 0.0, "D_unsuitable", "no valid Hs/Tp pair across NORA3, NWS, or Baltic"

    quality_scores = [_candidate_quality_score(row) for _, row in valid.iterrows()]
    mean_quality = float(np.mean(quality_scores)) if quality_scores else 0.0
    hs_range = float(valid["hs_mean"].max() - valid["hs_mean"].min()) if len(valid) > 1 else 0.0
    tp_range = float(valid["tp_mean"].max() - valid["tp_mean"].min()) if len(valid) > 1 else 0.0
    classes = event_pairwise["agreement_class"].tolist() if not event_pairwise.empty else []
    has_strong = "strong_agreement" in classes
    has_moderate = "moderate_agreement" in classes
    has_weak = "weak_agreement" in classes

    if len(valid) >= 2:
        if (
            has_strong
            and not has_weak
            and hs_range <= HS_MODERATE_AGREEMENT_M
            and tp_range <= TP_MODERATE_AGREEMENT_S
            and not shallow
        ):
            score = max(0.82, min(1.0, 0.72 + 0.28 * mean_quality))
            return (
                score,
                "A_high",
                "at least two independent sources agree strongly and no shallow/depth warning is active",
            )
        if has_strong or has_moderate:
            score = max(0.58, min(0.79, 0.50 + 0.25 * mean_quality))
            if shallow:
                return (
                    min(score, 0.54),
                    "C_low",
                    "sources overlap but shallow/depth warning prevents medium or high confidence",
                )
            return score, "B_medium", "two or more sources show moderate or better agreement"
        return (
            max(0.25, min(0.49, 0.35 + 0.10 * mean_quality)),
            "C_low",
            "multiple valid sources disagree beyond transparent Hs/Tp thresholds",
        )

    selected_quality = _candidate_quality_score(selected)
    if selected_quality >= 0.70 and not shallow:
        return (
            max(0.56, min(0.69, selected_quality)),
            "B_medium",
            "one domain-appropriate source has good spatial/temporal alignment",
        )
    return (
        max(0.20, min(0.49, selected_quality)),
        "C_low",
        "only one fallback or lower-quality source is available",
    )


def score_event_confidence(candidates: pd.DataFrame, pairwise: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    candidate_valid = candidates.assign(_valid_source=_source_available(candidates))
    pairwise_by_dwell = {key: group for key, group in pairwise.groupby("dwell_id", sort=False)}

    for dwell_id, group in candidate_valid.groupby("dwell_id", sort=False):
        base = group.iloc[0]
        valid = group[group["_valid_source"]].copy()
        event_pairwise = pairwise_by_dwell.get(dwell_id, pd.DataFrame(columns=REQUIRED_PAIRWISE_COLUMNS))
        selected = _select_candidate(valid) if not valid.empty else None
        shallow = bool(base.get("shallow_water_flag"))

        hs_range = float(valid["hs_mean"].max() - valid["hs_mean"].min()) if len(valid) > 1 else 0.0 if len(valid) else np.nan
        tp_range = float(valid["tp_mean"].max() - valid["tp_mean"].min()) if len(valid) > 1 else 0.0 if len(valid) else np.nan
        direction_degrees = [
            _direction_degrees(row["wave_direction_sin_mean"], row["wave_direction_cos_mean"])
            for _, row in valid.iterrows()
        ]
        direction_range = _circular_range_deg(direction_degrees)
        score, confidence_class, class_reason = _classify_confidence(valid, event_pairwise, selected, shallow)

        if selected is None:
            selected_source = "missing"
            selected_hs = selected_tp = selected_sin = selected_cos = np.nan
            selection_method = "no_valid_source"
            selected_reason = class_reason
        elif len(valid) == 1:
            selected_source = str(selected["source"])
            selected_hs = selected["hs_mean"]
            selected_tp = selected["tp_mean"]
            selected_sin = selected["wave_direction_sin_mean"]
            selected_cos = selected["wave_direction_cos_mean"]
            selection_method = "single_source_quality_selection"
            selected_reason = f"{class_reason}; selected {selected_source} because it is the only valid source"
        else:
            selected_source = str(selected["source"])
            selected_hs = selected["hs_mean"]
            selected_tp = selected["tp_mean"]
            selected_sin = selected["wave_direction_sin_mean"]
            selected_cos = selected["wave_direction_cos_mean"]
            selection_method = "agreement_centered_quality_selection"
            selected_reason = (
                f"{class_reason}; selected {selected_source} by minimum normalized disagreement "
                "with quality-score tie break, not by Fusion v0 source priority"
            )

        rows.append(
            {
                "dwell_id": dwell_id,
                "visit_id": base["visit_id"],
                "wind_farm": base["wind_farm"],
                "farm_id": base["farm_id"],
                "dwell_tier": base["dwell_tier"],
                "start_utc": base["start_utc"],
                "end_utc": base["end_utc"],
                "valid_source_count": int(len(valid)),
                "available_sources": ",".join(str(value) for value in valid["source"].tolist()),
                "selected_wave_source": selected_source,
                "selected_hs_mean": selected_hs,
                "selected_tp_mean": selected_tp,
                "selected_wave_direction_sin_mean": selected_sin,
                "selected_wave_direction_cos_mean": selected_cos,
                "weighted_hs_mean": _weighted_value(valid, "hs_mean"),
                "weighted_tp_mean": _weighted_value(valid, "tp_mean"),
                "source_disagreement_hs_range": hs_range,
                "source_disagreement_tp_range": tp_range,
                "source_disagreement_direction_range_deg": direction_range,
                "wave_confidence_score": score,
                "wave_confidence_class": confidence_class,
                "selection_method": selection_method,
                "selection_reason": selected_reason,
                "exclude_from_high_confidence_boundary": confidence_class != "A_high",
                "water_depth_m": base["water_depth_m"],
                "shallow_water_flag": shallow,
            }
        )
    confidence = pd.DataFrame(rows)
    for column in REQUIRED_CONFIDENCE_COLUMNS:
        if column not in confidence.columns:
            confidence[column] = pd.NA
    return confidence[
        REQUIRED_CONFIDENCE_COLUMNS
        + [column for column in ["start_utc", "end_utc"] if column in confidence.columns]
    ]


def _metric_summary(values_a: pd.Series, values_b: pd.Series) -> dict[str, float]:
    mask = values_a.notna() & values_b.notna()
    a = values_a.loc[mask].astype(float)
    b = values_b.loc[mask].astype(float)
    if a.empty:
        return {
            "count": 0,
            "r2": np.nan,
            "rmse": np.nan,
            "mae": np.nan,
            "bias": np.nan,
            "median_abs_diff": np.nan,
            "p95_abs_diff": np.nan,
        }
    diff = a - b
    if len(a) > 1 and a.nunique(dropna=True) > 1 and b.nunique(dropna=True) > 1:
        r2 = float(np.corrcoef(a.to_numpy(), b.to_numpy())[0, 1] ** 2)
    else:
        r2 = np.nan
    return {
        "count": int(len(diff)),
        "r2": r2,
        "rmse": float(np.sqrt(np.mean(np.square(diff)))),
        "mae": float(diff.abs().mean()),
        "bias": float(diff.mean()),
        "median_abs_diff": float(diff.abs().median()),
        "p95_abs_diff": float(diff.abs().quantile(0.95)),
    }


def _pairwise_metric_table(pairwise: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for (source_a, source_b), group in pairwise.groupby(["source_a", "source_b"], dropna=False):
        hs = _metric_summary(group["hs_a"], group["hs_b"])
        tp = _metric_summary(group["tp_a"], group["tp_b"])
        direction = pd.to_numeric(group["direction_circular_diff_deg"], errors="coerce").dropna()
        rows.append(
            {
                "source_pair": f"{source_a}_vs_{source_b}",
                "count": int(len(group)),
                "hs_r2": hs["r2"],
                "hs_rmse": hs["rmse"],
                "hs_mae": hs["mae"],
                "hs_bias": hs["bias"],
                "hs_median_abs_diff": hs["median_abs_diff"],
                "hs_p95_abs_diff": hs["p95_abs_diff"],
                "tp_r2": tp["r2"],
                "tp_rmse": tp["rmse"],
                "tp_mae": tp["mae"],
                "tp_bias": tp["bias"],
                "tp_median_abs_diff": tp["median_abs_diff"],
                "tp_p95_abs_diff": tp["p95_abs_diff"],
                "direction_count": int(len(direction)),
                "direction_mae_deg": float(direction.mean()) if not direction.empty else np.nan,
                "direction_p95_abs_diff_deg": float(direction.quantile(0.95)) if not direction.empty else np.nan,
            }
        )
    return pd.DataFrame(rows)


def _format_table(frame: pd.DataFrame, max_rows: int = 20) -> str:
    if frame.empty:
        return "_No rows._"
    return frame.head(max_rows).to_markdown(index=False)


def _tp_boundary(frame: pd.DataFrame, hs_col: str, tp_col: str, label: str) -> pd.DataFrame:
    columns = ["source", "tp_bin_left", "tp_bin_right", "rows", "hs_p50", "hs_p95"]
    tier_a = frame[
        frame["dwell_tier"].eq("Tier A") & frame[hs_col].notna() & frame[tp_col].notna()
    ].copy()
    if tier_a.empty:
        return pd.DataFrame(columns=columns)
    max_tp = max(float(tier_a[tp_col].max()), 0.5)
    bins = np.arange(0.0, math.ceil(max_tp * 2) / 2 + 0.5, 0.5)
    tier_a["tp_bin"] = pd.cut(tier_a[tp_col], bins=bins, right=False)
    rows = []
    for interval, group in tier_a.groupby("tp_bin", observed=True):
        if len(group) < 5:
            continue
        rows.append(
            {
                "source": label,
                "tp_bin_left": float(interval.left),
                "tp_bin_right": float(interval.right),
                "rows": int(len(group)),
                "hs_p50": float(group[hs_col].quantile(0.50)),
                "hs_p95": float(group[hs_col].quantile(0.95)),
            }
        )
    return pd.DataFrame(rows, columns=columns)


def _boundary_sensitivity(fusion_v0: pd.DataFrame, confidence: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    v0_boundary = _tp_boundary(fusion_v0, "fusion_hs_mean", "fusion_tp_mean", "fusion_v0_all")
    v1_all = _tp_boundary(confidence, "selected_hs_mean", "selected_tp_mean", "fusion_v1_selected_all")
    high = confidence[confidence["wave_confidence_class"].eq("A_high")]
    v1_high = _tp_boundary(high, "selected_hs_mean", "selected_tp_mean", "fusion_v1_high_confidence")
    not_shallow = confidence[~confidence["shallow_water_flag"].astype(bool)]
    v1_not_shallow = _tp_boundary(not_shallow, "selected_hs_mean", "selected_tp_mean", "fusion_v1_excluding_shallow")
    combined = pd.concat([v0_boundary, v1_all, v1_high, v1_not_shallow], ignore_index=True)

    deltas = []
    for label, boundary in [
        ("fusion_v1_selected_all", v1_all),
        ("fusion_v1_high_confidence", v1_high),
        ("fusion_v1_excluding_shallow", v1_not_shallow),
    ]:
        merged = v0_boundary.merge(
            boundary,
            on=["tp_bin_left", "tp_bin_right"],
            suffixes=("_v0", "_variant"),
        )
        if merged.empty:
            continue
        merged["variant"] = label
        merged["hs_p95_delta_vs_v0"] = merged["hs_p95_variant"] - merged["hs_p95_v0"]
        deltas.append(
            merged[
                [
                    "variant",
                    "tp_bin_left",
                    "tp_bin_right",
                    "rows_v0",
                    "rows_variant",
                    "hs_p50_v0",
                    "hs_p50_variant",
                    "hs_p95_v0",
                    "hs_p95_variant",
                    "hs_p95_delta_vs_v0",
                ]
            ]
        )
    delta_table = pd.concat(deltas, ignore_index=True) if deltas else pd.DataFrame()
    return combined, delta_table


def _coverage_tables(candidates: pd.DataFrame) -> dict[str, pd.DataFrame]:
    valid = candidates[_source_available(candidates)].copy()
    year = candidates.assign(event_year=pd.to_datetime(candidates["start_utc"], utc=True).dt.year)
    valid_year = valid.assign(event_year=pd.to_datetime(valid["start_utc"], utc=True).dt.year)
    by_source = (
        candidates.groupby("source", observed=False)
        .agg(candidate_rows=("dwell_id", "size"))
        .join(valid.groupby("source", observed=False).agg(valid_candidate_rows=("dwell_id", "size")))
        .fillna(0)
        .reset_index()
    )
    by_source["valid_candidate_rows"] = by_source["valid_candidate_rows"].astype(int)
    by_source["valid_rate"] = by_source["valid_candidate_rows"] / by_source["candidate_rows"]

    by_tier = (
        candidates.groupby(["source", "dwell_tier"], observed=False)
        .agg(candidate_rows=("dwell_id", "size"))
        .join(valid.groupby(["source", "dwell_tier"], observed=False).agg(valid_candidate_rows=("dwell_id", "size")))
        .fillna(0)
        .reset_index()
    )
    by_tier["valid_candidate_rows"] = by_tier["valid_candidate_rows"].astype(int)
    by_tier["valid_rate"] = by_tier["valid_candidate_rows"] / by_tier["candidate_rows"]

    by_farm_year = (
        year.groupby(["source", "farm_id", "event_year"], observed=False, dropna=False)
        .agg(candidate_rows=("dwell_id", "size"))
        .join(
            valid_year.groupby(["source", "farm_id", "event_year"], observed=False, dropna=False).agg(
                valid_candidate_rows=("dwell_id", "size")
            )
        )
        .fillna(0)
        .reset_index()
    )
    by_farm_year["valid_candidate_rows"] = by_farm_year["valid_candidate_rows"].astype(int)
    by_farm_year["valid_rate"] = by_farm_year["valid_candidate_rows"] / by_farm_year["candidate_rows"]
    return {
        "candidate_coverage_by_source": by_source,
        "candidate_coverage_by_source_tier": by_tier,
        "candidate_coverage_by_source_farm_year": by_farm_year,
    }


def _source_overlap_counts(candidates: pd.DataFrame) -> pd.DataFrame:
    valid = candidates[_source_available(candidates)]
    source_sets = valid.groupby("dwell_id")["source"].agg(lambda values: set(map(str, values)))
    rows = []
    for left, right in [("nora3", "nws"), ("nora3", "baltic"), ("nws", "baltic")]:
        rows.append(
            {
                "source_pair": f"{left}_vs_{right}",
                "overlap_count": int(source_sets.map(lambda sources: left in sources and right in sources).sum()),
            }
        )
    return pd.DataFrame(rows)


def _quality_table(candidates: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for source, group in candidates.groupby("source", observed=False):
        distance = pd.to_numeric(group["source_sample_distance_km"], errors="coerce").dropna()
        grid_distance = pd.to_numeric(group.get("source_grid_distance_km"), errors="coerce").dropna()
        time_gap = pd.to_numeric(group["nearest_time_gap_minutes"], errors="coerce").dropna()
        samples = pd.to_numeric(group["event_window_sample_count"], errors="coerce").dropna()
        rows.append(
            {
                "source": source,
                "rows": int(len(group)),
                "sample_distance_p50_km": float(distance.quantile(0.50)) if not distance.empty else np.nan,
                "sample_distance_p95_km": float(distance.quantile(0.95)) if not distance.empty else np.nan,
                "source_grid_distance_p50_km": float(grid_distance.quantile(0.50)) if not grid_distance.empty else np.nan,
                "source_grid_distance_p95_km": float(grid_distance.quantile(0.95)) if not grid_distance.empty else np.nan,
                "nearest_time_gap_p50_min": float(time_gap.quantile(0.50)) if not time_gap.empty else np.nan,
                "nearest_time_gap_p95_min": float(time_gap.quantile(0.95)) if not time_gap.empty else np.nan,
                "event_window_sample_count_p50": float(samples.quantile(0.50)) if not samples.empty else np.nan,
                "event_window_sample_count_p95": float(samples.quantile(0.95)) if not samples.empty else np.nan,
                "not_bracketed_rows": int((group["event_bracketed_by_source_times"] == False).sum()),  # noqa: E712
                "shallow_water_rows": int(group["shallow_water_flag"].astype(bool).sum()),
            }
        )
    return pd.DataFrame(rows)


def _compare_with_v0(fusion_v0: pd.DataFrame, confidence: pd.DataFrame) -> dict[str, Any]:
    merged = confidence.merge(
        fusion_v0[
            [
                "dwell_id",
                "fusion_wave_source",
                "fusion_hs_mean",
                "fusion_tp_mean",
            ]
        ],
        on="dwell_id",
        how="left",
    )
    selected_valid = _valid_pair_mask(merged, "selected_hs_mean", "selected_tp_mean")
    v0_valid = _valid_pair_mask(merged, "fusion_hs_mean", "fusion_tp_mean")
    same_source = merged["selected_wave_source"].eq(merged["fusion_wave_source"])
    supported = same_source & merged["wave_confidence_class"].isin(["A_high", "B_medium"])
    questionable = (
        (~same_source & selected_valid & v0_valid)
        | merged["wave_confidence_class"].isin(["C_low", "D_unsuitable"])
        | (pd.to_numeric(merged["source_disagreement_hs_range"], errors="coerce") > HS_MODERATE_AGREEMENT_M)
    )
    tier_a = merged["dwell_tier"].eq("Tier A")
    return {
        "selected_source_differs_from_v0_count": int((~same_source & selected_valid & v0_valid).sum()),
        "v0_priority_supported_count": int(supported.sum()),
        "v0_priority_questionable_count": int(questionable.sum()),
        "tier_a_v0_valid_rows": int((tier_a & v0_valid).sum()),
        "tier_a_v1_selected_valid_rows": int((tier_a & selected_valid).sum()),
        "tier_a_v1_high_confidence_rows": int((tier_a & merged["wave_confidence_class"].eq("A_high")).sum()),
        "comparison_by_source": (
            merged.groupby(["fusion_wave_source", "selected_wave_source"], dropna=False)
            .size()
            .reset_index(name="rows")
            .sort_values("rows", ascending=False)
        ),
    }


def _write_validation_tables(
    report_dir: Path,
    candidates: pd.DataFrame,
    pairwise: pd.DataFrame,
    confidence: pd.DataFrame,
    fusion_v0: pd.DataFrame,
) -> dict[str, Path]:
    report_dir.mkdir(parents=True, exist_ok=True)
    tables = _coverage_tables(candidates)
    tables["source_overlap_counts"] = _source_overlap_counts(candidates)
    tables["pairwise_agreement_metrics"] = _pairwise_metric_table(pairwise)
    tables["spatial_temporal_quality_by_source"] = _quality_table(candidates)
    tables["confidence_class_distribution"] = (
        confidence["wave_confidence_class"]
        .value_counts(dropna=False)
        .rename_axis("wave_confidence_class")
        .reset_index(name="rows")
    )
    tables["confidence_by_farm"] = (
        confidence.groupby(["farm_id", "wave_confidence_class"], dropna=False)
        .size()
        .reset_index(name="rows")
        .sort_values(["farm_id", "wave_confidence_class"])
    )
    tables["confidence_by_source"] = (
        confidence.groupby(["selected_wave_source", "wave_confidence_class"], dropna=False)
        .size()
        .reset_index(name="rows")
        .sort_values(["selected_wave_source", "wave_confidence_class"])
    )
    tables["confidence_by_tier"] = (
        confidence.groupby(["dwell_tier", "wave_confidence_class"], dropna=False)
        .size()
        .reset_index(name="rows")
        .sort_values(["dwell_tier", "wave_confidence_class"])
    )
    tables["high_disagreement_examples"] = (
        confidence.sort_values(
            ["source_disagreement_hs_range", "source_disagreement_tp_range"],
            ascending=False,
        )
        .head(25)
    )
    comparison = _compare_with_v0(fusion_v0, confidence)
    tables["fusion_v0_vs_v1_source_comparison"] = comparison["comparison_by_source"]
    boundary, boundary_delta = _boundary_sensitivity(fusion_v0, confidence)
    tables["tier_a_hs_tp_boundary_variants"] = boundary
    tables["tier_a_hs_tp_boundary_delta_vs_v0"] = boundary_delta

    paths: dict[str, Path] = {}
    for name, table in tables.items():
        path = report_dir / f"{name}.csv"
        table.to_csv(path, index=False)
        paths[name] = path
    return paths


def validate_fusion_v1(
    candidates: pd.DataFrame,
    pairwise: pd.DataFrame,
    confidence: pd.DataFrame,
    dwell: pd.DataFrame,
    fusion_v0: pd.DataFrame,
    nws_root: Path,
    baltic_root: Path,
    bathymetry: pd.DataFrame,
    output_paths: dict[str, Path],
    report_dir: Path,
) -> tuple[dict[str, Any], dict[str, Path]]:
    candidate_coverage = _coverage_tables(candidates)["candidate_coverage_by_source"]
    overlap_counts = _source_overlap_counts(candidates)
    pairwise_metrics = _pairwise_metric_table(pairwise)
    confidence_counts = (
        confidence["wave_confidence_class"]
        .value_counts(dropna=False)
        .rename_axis("wave_confidence_class")
        .reset_index(name="rows")
    )
    comparison = _compare_with_v0(fusion_v0, confidence)
    _, boundary_delta = _boundary_sensitivity(fusion_v0, confidence)
    nora3_valid = _valid_pair_mask(dwell, "active_hs_mean", "active_tp_mean")
    tables = _write_validation_tables(report_dir, candidates, pairwise, confidence, fusion_v0)
    validation = {
        "input_inventory": {
            "dwell_rows": int(len(dwell)),
            "fusion_v0_rows": int(len(fusion_v0)),
            "nws_archive": _archive_inventory(nws_root),
            "baltic_archive": _archive_inventory(baltic_root),
            "nora3_event_field_valid_rows": int(nora3_valid.sum()),
            "bathymetry_rows": int(len(bathymetry)),
        },
        "output_counts": {
            "candidate_rows": int(len(candidates)),
            "pairwise_rows": int(len(pairwise)),
            "confidence_rows": int(len(confidence)),
            "confidence_row_count_preserved": bool(len(confidence) == len(dwell)),
            "duplicate_confidence_dwell_ids": int(confidence["dwell_id"].duplicated().sum()),
        },
        "candidate_coverage": candidate_coverage,
        "source_overlap_counts": overlap_counts,
        "pairwise_metrics": pairwise_metrics,
        "confidence_class_distribution": confidence_counts,
        "fusion_v0_comparison": comparison,
        "boundary_delta": boundary_delta,
        "required_missing_columns": {
            "candidates": sorted(set(REQUIRED_CANDIDATE_COLUMNS) - set(candidates.columns)),
            "pairwise": sorted(set(REQUIRED_PAIRWISE_COLUMNS) - set(pairwise.columns)),
            "confidence": sorted(set(REQUIRED_CONFIDENCE_COLUMNS) - set(confidence.columns)),
        },
        "accepted": bool(
            len(confidence) == len(dwell)
            and confidence["dwell_id"].duplicated().sum() == 0
            and not sorted(set(REQUIRED_CANDIDATE_COLUMNS) - set(candidates.columns))
            and not sorted(set(REQUIRED_PAIRWISE_COLUMNS) - set(pairwise.columns))
            and not sorted(set(REQUIRED_CONFIDENCE_COLUMNS) - set(confidence.columns))
            and confidence["wave_confidence_class"].notna().all()
        ),
        "output_paths": {key: str(value) for key, value in output_paths.items()},
    }
    return validation, tables


def _render_report(validation: dict[str, Any], table_paths: dict[str, Path]) -> str:
    inventory = validation["input_inventory"]
    counts = validation["output_counts"]
    comparison = validation["fusion_v0_comparison"]
    boundary_delta = validation["boundary_delta"]
    mean_abs_p95 = (
        float(boundary_delta["hs_p95_delta_vs_v0"].abs().mean())
        if not boundary_delta.empty and "hs_p95_delta_vs_v0" in boundary_delta
        else np.nan
    )
    max_abs_p95 = (
        float(boundary_delta["hs_p95_delta_vs_v0"].abs().max())
        if not boundary_delta.empty and "hs_p95_delta_vs_v0" in boundary_delta
        else np.nan
    )

    table_list = "\n".join(f"- `{name}`: `{path}`" for name, path in table_paths.items())
    current_pilot_recommendation = (
        "Proceed to scoped current pilots next, reusing this candidate/agreement/confidence frame for u/v current sources."
        if validation["accepted"]
        else "Do not proceed to current pilots until Fusion v1 output integrity issues are resolved."
    )

    lines = [
        "# Metocean Fusion v1 Source Agreement Validation Report",
        "",
        "Status: completed Fusion v1 source-agreement increment. Existing accepted local sources were read only. No current downloads, FINO import, source archive mutation, NORA3 rerun, source fusion beyond wave-source agreement/confidence, final production dwell-metocean rebuild, or CTV/SOV inference was performed.",
        "",
        "## A. Research Design",
        "",
        "Hypotheses: multi-source agreement improves confidence in event-level wave assignment; high-disagreement events can materially affect the Tier A observed Hs/Tp envelope; shallow/coastal sites show larger disagreement; a confidence-filtered envelope is more defensible than the Fusion v0 priority envelope; and NORA3, NWS, and Baltic strengths are complementary rather than hierarchical.",
        "",
        f"Metrics: source coverage/overlap, Hs/Tp R2, RMSE, MAE, bias, median and p95 absolute difference, circular directional difference where present, event-window temporal alignment, spatial/sample distance, confidence-class distribution, and Tier A Hs/Tp p50/p95 envelope impact. Agreement thresholds are transparent: strong if Hs <= {HS_STRONG_AGREEMENT_M:.2f} m and Tp <= {TP_STRONG_AGREEMENT_S:.2f} s; moderate if Hs <= {HS_MODERATE_AGREEMENT_M:.2f} m and Tp <= {TP_MODERATE_AGREEMENT_S:.2f} s; otherwise weak.",
        "",
        "Weighted Hs/Tp columns are sensitivity diagnostics, not truth. The formula is a quality-weighted average using variable completeness, temporal gap/bracketing, sample distance, domain match, depth warning, and agreement support; selected source remains a traceable source candidate.",
        "",
        "## 1. Executive Conclusion",
        "",
        f"- Fusion v1 accepted: `{validation['accepted']}`",
        f"- Candidate rows: `{counts['candidate_rows']}`; pairwise rows: `{counts['pairwise_rows']}`; event confidence rows: `{counts['confidence_rows']}`",
        f"- Confidence row identity preserved versus dwell input: `{counts['confidence_row_count_preserved']}`; duplicate dwell IDs: `{counts['duplicate_confidence_dwell_ids']}`",
        f"- Fusion v0 selected source differs from v1 for `{comparison['selected_source_differs_from_v0_count']}` valid-overlap events.",
        f"- Tier A valid counts: v0 `{comparison['tier_a_v0_valid_rows']}`, v1 selected `{comparison['tier_a_v1_selected_valid_rows']}`, v1 high-confidence `{comparison['tier_a_v1_high_confidence_rows']}`.",
        f"- Mean absolute Tier A p95 Hs boundary change versus v0 across reported variants: `{mean_abs_p95}` m; max absolute change: `{max_abs_p95}` m.",
        f"- Next increment: {current_pilot_recommendation}",
        "",
        "## 2. Why Fusion v0 Was Insufficient",
        "",
        "Fusion v0 was a coverage resolver: Baltic if available, else NWS if available, else NORA3. That raised Hs/Tp coverage, but it encoded a hierarchy before testing whether overlapping sources agreed. Fusion v1 keeps the source candidates separate, evaluates overlap and disagreement, and assigns confidence before any envelope interpretation.",
        "",
        "## 3. Input Inventory",
        "",
        f"- Dwell rows: `{inventory['dwell_rows']}`",
        f"- Fusion v0 rows: `{inventory['fusion_v0_rows']}`",
        f"- NWS archive: `{inventory['nws_archive']}`",
        f"- Baltic archive: `{inventory['baltic_archive']}`",
        f"- NORA3 active Hs/Tp valid rows: `{inventory['nora3_event_field_valid_rows']}`",
        f"- Bathymetry rows: `{inventory['bathymetry_rows']}`",
        "",
        "## 4. Candidate Coverage",
        "",
        "### Valid Candidate Rows By Source",
        "",
        _format_table(validation["candidate_coverage"]),
        "",
        "### Source Pair Overlap",
        "",
        _format_table(validation["source_overlap_counts"]),
        "",
        "Coverage by source/tier and source/farm/year is written to the detailed validation tables listed below.",
        "",
        "## 5. Pairwise Agreement",
        "",
        _format_table(validation["pairwise_metrics"]),
        "",
        "## 6. Spatial And Temporal Quality",
        "",
        _format_table(pd.read_csv(table_paths["spatial_temporal_quality_by_source"])),
        "",
        "The shallow/depth warning uses `water_depth_m <= 10`; explicit `<=1`, `<=5`, and `<=10` flags are preserved in the candidate table.",
        "",
        "## 7. Confidence Scoring",
        "",
        "### Confidence Class Distribution",
        "",
        _format_table(validation["confidence_class_distribution"]),
        "",
        "Confidence by farm, selected source, and dwell tier, plus high-disagreement examples, are written to the detailed validation tables.",
        "",
        "## 8. Comparison With Fusion v0",
        "",
        f"- Selected sources differ from v0: `{comparison['selected_source_differs_from_v0_count']}`",
        f"- v0 priority supported by v1 A/B confidence and same source: `{comparison['v0_priority_supported_count']}`",
        f"- v0 priority questionable through changed source, low confidence, or high disagreement: `{comparison['v0_priority_questionable_count']}`",
        f"- Tier A v0 valid rows: `{comparison['tier_a_v0_valid_rows']}`",
        f"- Tier A v1 selected valid rows: `{comparison['tier_a_v1_selected_valid_rows']}`",
        f"- Tier A v1 high-confidence rows: `{comparison['tier_a_v1_high_confidence_rows']}`",
        "",
        "### Source Comparison Counts",
        "",
        _format_table(comparison["comparison_by_source"]),
        "",
        "## 9. Workability Envelope Sensitivity",
        "",
        _format_table(boundary_delta, max_rows=30),
        "",
        "The high-confidence and shallow-excluded variants are deliberately narrower evidence sets. They are defensible sensitivity boundaries, not calibrated access probabilities.",
        "",
        "## 10. Research Interpretation",
        "",
        "- Source agreement increases confidence because it exposes which event assignments are supported by independent products rather than only source priority.",
        "- Disagreement should be interpreted by source pair, region, and depth warning before changing workability claims; the detailed tables identify the concentrated examples.",
        "- Fusion v1 can change the provisional Tier A envelope by changing both the selected source and the event subset admitted to high-confidence boundary calculations.",
        "- Before currents are added, the current branch should emit the same candidate, pairwise-agreement, confidence, and missing-reason fields for u/v-derived current speed and direction.",
        "",
        "## 11. Clear Next Increment",
        "",
        current_pilot_recommendation,
        "",
        "Current confidence should reuse the v1 framework: keep source-specific candidates, quantify overlap, score temporal/spatial quality, preserve missing reasons, and only then derive simulator-ready current inputs with provenance and uncertainty.",
        "",
        "## Detailed Validation Tables",
        "",
        table_list,
        "",
        "## Output Paths",
        "",
        "\n".join(f"- {key}: `{value}`" for key, value in validation["output_paths"].items()),
        "",
    ]
    return "\n".join(lines)


def build_metocean_fusion_v1_source_agreement(
    dwell_weather: Path = DEFAULT_DWELL_WEATHER_INPUT,
    fusion_v0: Path = DEFAULT_FUSION_V0_INPUT,
    nws_root: Path = DEFAULT_NWS_ROOT,
    baltic_root: Path = DEFAULT_BALTIC_ROOT,
    bathymetry: Path = DEFAULT_BATHYMETRY_POINTS,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    report_dir: Path = DEFAULT_REPORT_DIR,
    overwrite: bool = False,
) -> FusionV1Result:
    candidate_path = output_dir / WAVE_SOURCE_CANDIDATES_FILENAME
    pairwise_path = output_dir / PAIRWISE_AGREEMENT_FILENAME
    confidence_path = output_dir / EVENT_CONFIDENCE_FILENAME
    report_path = report_dir / VALIDATION_REPORT_FILENAME
    for path in [candidate_path, pairwise_path, confidence_path, report_path]:
        if path.exists() and not overwrite:
            raise FileExistsError(f"Fusion v1 output already exists and overwrite is false: {path}")

    dwell = _load_dwell_weather(dwell_weather)
    fusion_v0_frame = pd.read_parquet(fusion_v0)
    bathymetry_frame = _load_bathymetry(bathymetry)

    candidates = build_wave_source_candidates(
        dwell=dwell,
        bathymetry=bathymetry_frame,
        nws_root=nws_root,
        baltic_root=baltic_root,
    )
    pairwise = compute_pairwise_agreement(candidates)
    confidence = score_event_confidence(candidates, pairwise)

    output_dir.mkdir(parents=True, exist_ok=True)
    candidates.to_parquet(candidate_path, index=False)
    pairwise.to_parquet(pairwise_path, index=False)
    confidence.to_parquet(confidence_path, index=False)

    report_dir.mkdir(parents=True, exist_ok=True)
    output_paths = {
        "candidate_table": candidate_path,
        "pairwise_agreement_table": pairwise_path,
        "event_confidence_table": confidence_path,
        "validation_report": report_path,
    }
    validation, table_paths = validate_fusion_v1(
        candidates=candidates,
        pairwise=pairwise,
        confidence=confidence,
        dwell=dwell,
        fusion_v0=fusion_v0_frame,
        nws_root=nws_root,
        baltic_root=baltic_root,
        bathymetry=bathymetry_frame,
        output_paths=output_paths,
        report_dir=report_dir,
    )
    report_path.write_text(_render_report(validation, table_paths), encoding="utf-8")
    return FusionV1Result(
        candidate_path=candidate_path,
        pairwise_path=pairwise_path,
        confidence_path=confidence_path,
        report_path=report_path,
        candidates=candidates,
        pairwise=pairwise,
        confidence=confidence,
        validation=validation,
    )
