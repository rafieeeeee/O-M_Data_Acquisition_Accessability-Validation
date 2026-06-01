"""Metocean Fusion v0: source-resolved wave plus bathymetry event table.

This increment answers a research question rather than adding another
standalone planning contract: does replacing the current NORA3-only active
weather join with best-available regional waves and static depth materially
change observed dwell-event coverage and the Tier A Hs/Tp envelope?

The implementation is deliberately narrow:

* Wave sources are Baltic, NWS, and existing NORA3 active fields.
* Bathymetry comes from the accepted static site-context point table.
* No currents, FINO import, source archive mutation, interpolation changes, or
  final production dwell-table rebuild are performed.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from .extract_nws import haversine_distance


DEFAULT_DWELL_WEATHER_INPUT = Path(
    "Data/Processed/ais_dwell_backfill/cross_farm_dwell_weather_features.parquet"
)
DEFAULT_NWS_ROOT = Path("Data/Processed/metocean/nws_wave_timeseries")
DEFAULT_BALTIC_ROOT = Path("Data/Processed/metocean/baltic_wave_timeseries")
DEFAULT_BATHYMETRY_POINTS = Path(
    "Data/Processed/metocean/bathymetry/site_bathymetry_points.parquet"
)
DEFAULT_OUTPUT_PATH = Path("Data/Processed/metocean/fusion_v0/dwell_metocean_fusion_v0.parquet")
DEFAULT_REPORT_PATH = Path("reports/metocean_fusion_v0/metocean_fusion_v0_validation_report.md")

REQUIRED_OUTPUT_COLUMNS = [
    "dwell_id",
    "visit_id",
    "wind_farm",
    "farm_id",
    "dwell_tier",
    "start_utc",
    "end_utc",
    "fusion_hs_mean",
    "fusion_tp_mean",
    "fusion_wave_direction_sin_mean",
    "fusion_wave_direction_cos_mean",
    "fusion_wave_source",
    "fusion_wave_assignment_method",
    "fusion_wave_missing_reason",
    "water_depth_m",
    "bathymetry_source",
    "bathymetry_distance_m",
    "bathymetry_spatial_match_status",
    "nora3_hs_mean",
    "nora3_tp_mean",
    "nws_hs_mean",
    "nws_tp_mean",
    "baltic_hs_mean",
    "baltic_tp_mean",
]

BASE_COLUMNS = [
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


@dataclass(frozen=True)
class WaveSourceConfig:
    name: str
    root: Path
    hs_col: str
    tp_col: str
    direction_col: str
    timestamp_col: str
    sample_col: str
    native_tolerance: pd.Timedelta


@dataclass(frozen=True)
class FusionV0Result:
    output_path: Path
    report_path: Path
    output: pd.DataFrame
    validation: dict[str, Any]


NWS_CONFIG = WaveSourceConfig(
    name="nws",
    root=DEFAULT_NWS_ROOT,
    hs_col="nws_wave_hs",
    tp_col="nws_wave_tp",
    direction_col="nws_wave_dir",
    timestamp_col="timestamp_utc",
    sample_col="sample_point_id",
    native_tolerance=pd.Timedelta(hours=3),
)

BALTIC_CONFIG = WaveSourceConfig(
    name="baltic",
    root=DEFAULT_BALTIC_ROOT,
    hs_col="baltic_wave_hs",
    tp_col="baltic_wave_tp",
    direction_col="baltic_wave_dir",
    timestamp_col="timestamp_utc",
    sample_col="sample_point_id",
    native_tolerance=pd.Timedelta(hours=1),
)


def _normalise_timestamp(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, utc=True, errors="coerce")


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


def _load_dwell_weather(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Dwell-weather table not found: {path}")
    dwell = pd.read_parquet(path)
    missing = sorted(set(BASE_COLUMNS) - set(dwell.columns))
    if missing:
        raise ValueError(f"Dwell-weather table missing required columns: {missing}")
    dwell = dwell.copy()
    dwell["_fusion_row_id"] = np.arange(len(dwell), dtype=np.int64)
    dwell["start_utc"] = _normalise_timestamp(dwell["start_utc"])
    dwell["end_utc"] = _normalise_timestamp(dwell["end_utc"])
    dwell["_event_midpoint_utc"] = dwell["start_utc"] + (dwell["end_utc"] - dwell["start_utc"]) / 2
    dwell["_event_year"] = dwell["start_utc"].dt.year
    dwell["_source_farm_name"] = dwell["farm_id"].where(dwell["farm_id"].notna(), dwell["wind_farm"])
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
    assigned["assignment_method"] = "missing_farm_bathymetry_points"

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
            method = "nearest_common_sample_to_dwell_centroid"
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
        result.loc[events.index, "bathymetry_event_to_sample_distance_km"] = joined[
            "event_to_sample_distance_km"
        ].to_numpy()

    return result


def _initial_source_result(index: pd.Index, prefix: str) -> pd.DataFrame:
    result = pd.DataFrame(index=index)
    for col in [
        f"{prefix}_hs_mean",
        f"{prefix}_tp_mean",
        f"{prefix}_wave_direction_sin_mean",
        f"{prefix}_wave_direction_cos_mean",
        f"{prefix}_wave_spatial_distance_km",
    ]:
        result[col] = np.nan
    for col in [
        f"{prefix}_wave_sample_point_id",
        f"{prefix}_wave_assignment_method",
        f"{prefix}_wave_missing_reason",
    ]:
        result[col] = pd.NA
    return result


def _source_sample_assignments(dwell: pd.DataFrame, bathymetry: pd.DataFrame) -> pd.DataFrame:
    assigned = pd.DataFrame(index=dwell.index)
    assigned["source_sample_point_id"] = pd.NA
    assigned["source_spatial_distance_km"] = np.nan

    for farm_name, events in dwell.groupby("_source_farm_name", dropna=False):
        points = bathymetry[bathymetry["wind_farm"].eq(farm_name)]
        nearest = _nearest_point_for_events(events, points)
        assigned.loc[events.index, "source_sample_point_id"] = nearest["sample_point_id"].to_numpy()
        assigned.loc[events.index, "source_spatial_distance_km"] = nearest[
            "event_to_sample_distance_km"
        ].to_numpy()
    return assigned


def _mean_direction_components(direction_degrees: pd.Series) -> tuple[float | None, float | None]:
    direction = pd.to_numeric(direction_degrees, errors="coerce").dropna()
    if direction.empty:
        return None, None
    radians = np.deg2rad(direction.to_numpy(dtype=float))
    return float(np.sin(radians).mean()), float(np.cos(radians).mean())


def _aggregate_event_source(
    source: pd.DataFrame,
    event: pd.Series,
    config: WaveSourceConfig,
) -> dict[str, Any]:
    if source.empty:
        return {
            "hs": np.nan,
            "tp": np.nan,
            "dir_sin": np.nan,
            "dir_cos": np.nan,
            "assignment_method": pd.NA,
            "missing_reason": "source_sample_point_missing",
        }

    start = event["start_utc"]
    end = event["end_utc"]
    midpoint = event["_event_midpoint_utc"]
    if pd.isna(start) or pd.isna(end) or pd.isna(midpoint):
        return {
            "hs": np.nan,
            "tp": np.nan,
            "dir_sin": np.nan,
            "dir_cos": np.nan,
            "assignment_method": pd.NA,
            "missing_reason": "missing_event_timestamp",
        }

    in_window = source[source[config.timestamp_col].between(start, end, inclusive="both")]
    if in_window.empty:
        deltas = (source[config.timestamp_col] - midpoint).abs()
        nearest_index = deltas.idxmin()
        if pd.isna(deltas.loc[nearest_index]) or deltas.loc[nearest_index] > config.native_tolerance:
            return {
                "hs": np.nan,
                "tp": np.nan,
                "dir_sin": np.nan,
                "dir_cos": np.nan,
                "assignment_method": pd.NA,
                "missing_reason": "event_time_outside_source_tolerance",
            }
        in_window = source.loc[[nearest_index]]
        assignment_method = f"{config.name}_nearest_sample_midpoint_within_{int(config.native_tolerance.total_seconds() / 60)}min"
    else:
        assignment_method = f"{config.name}_nearest_sample_interval_mean"

    hs = pd.to_numeric(in_window[config.hs_col], errors="coerce")
    tp = pd.to_numeric(in_window[config.tp_col], errors="coerce")
    pair_mask = hs.notna() & tp.notna()
    if not pair_mask.any():
        return {
            "hs": np.nan,
            "tp": np.nan,
            "dir_sin": np.nan,
            "dir_cos": np.nan,
            "assignment_method": assignment_method,
            "missing_reason": "source_values_missing",
        }

    dir_sin, dir_cos = _mean_direction_components(in_window.loc[pair_mask, config.direction_col])
    return {
        "hs": float(hs.loc[pair_mask].mean()),
        "tp": float(tp.loc[pair_mask].mean()),
        "dir_sin": dir_sin,
        "dir_cos": dir_cos,
        "assignment_method": assignment_method,
        "missing_reason": pd.NA,
    }


def assign_wave_source_to_events(
    dwell: pd.DataFrame,
    bathymetry: pd.DataFrame,
    config: WaveSourceConfig,
) -> pd.DataFrame:
    file_index = _build_partition_index(config.root)
    result = _initial_source_result(dwell.index, config.name)
    sample_assignments = _source_sample_assignments(dwell, bathymetry)
    result[f"{config.name}_wave_sample_point_id"] = sample_assignments["source_sample_point_id"]
    result[f"{config.name}_wave_spatial_distance_km"] = sample_assignments["source_spatial_distance_km"]

    working = dwell.join(sample_assignments)
    missing_year = working["_event_year"].isna()
    result.loc[missing_year, f"{config.name}_wave_missing_reason"] = "missing_event_year"

    grouped = working[~missing_year].groupby(["wind_farm", "_event_year"], dropna=False)
    read_columns = [
        config.timestamp_col,
        config.sample_col,
        config.hs_col,
        config.tp_col,
        config.direction_col,
    ]

    for (farm_slug, year), events in grouped:
        if pd.isna(farm_slug) or pd.isna(year):
            result.loc[events.index, f"{config.name}_wave_missing_reason"] = "missing_event_farm_or_year"
            continue
        partition = file_index.get((str(farm_slug), int(year)))
        if partition is None:
            result.loc[events.index, f"{config.name}_wave_missing_reason"] = "source_partition_missing"
            continue

        source = pd.read_parquet(partition, columns=read_columns)
        source[config.timestamp_col] = _normalise_timestamp(source[config.timestamp_col])
        source = source.sort_values(config.timestamp_col)

        for sample_point_id, sample_events in events.groupby("source_sample_point_id", dropna=False):
            if pd.isna(sample_point_id):
                result.loc[sample_events.index, f"{config.name}_wave_missing_reason"] = (
                    "missing_source_sample_point"
                )
                continue
            sample_source = source[source[config.sample_col].eq(sample_point_id)]
            if sample_source.empty and sample_point_id != "farm_centroid":
                sample_source = source[source[config.sample_col].eq("farm_centroid")]
                result.loc[sample_events.index, f"{config.name}_wave_sample_point_id"] = "farm_centroid"
            for event_index, event in sample_events.iterrows():
                aggregate = _aggregate_event_source(sample_source, event, config)
                result.loc[event_index, f"{config.name}_hs_mean"] = aggregate["hs"]
                result.loc[event_index, f"{config.name}_tp_mean"] = aggregate["tp"]
                result.loc[event_index, f"{config.name}_wave_direction_sin_mean"] = aggregate["dir_sin"]
                result.loc[event_index, f"{config.name}_wave_direction_cos_mean"] = aggregate["dir_cos"]
                result.loc[event_index, f"{config.name}_wave_assignment_method"] = aggregate[
                    "assignment_method"
                ]
                result.loc[event_index, f"{config.name}_wave_missing_reason"] = aggregate["missing_reason"]

    return result


def _source_available(df: pd.DataFrame, prefix: str) -> pd.Series:
    return df[f"{prefix}_hs_mean"].notna() & df[f"{prefix}_tp_mean"].notna()


def _select_fusion_source(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    baltic_available = _source_available(out, "baltic")
    nws_available = _source_available(out, "nws")
    nora3_available = _source_available(out, "nora3")

    out["fusion_wave_source"] = pd.NA
    out.loc[baltic_available, "fusion_wave_source"] = "baltic"
    out.loc[~baltic_available & nws_available, "fusion_wave_source"] = "nws"
    out.loc[~baltic_available & ~nws_available & nora3_available, "fusion_wave_source"] = "nora3"
    out["fusion_wave_source"] = out["fusion_wave_source"].fillna("missing")

    for target, source_map in [
        (
            "fusion_hs_mean",
            {
                "baltic": "baltic_hs_mean",
                "nws": "nws_hs_mean",
                "nora3": "nora3_hs_mean",
            },
        ),
        (
            "fusion_tp_mean",
            {
                "baltic": "baltic_tp_mean",
                "nws": "nws_tp_mean",
                "nora3": "nora3_tp_mean",
            },
        ),
        (
            "fusion_wave_direction_sin_mean",
            {
                "baltic": "baltic_wave_direction_sin_mean",
                "nws": "nws_wave_direction_sin_mean",
                "nora3": "nora3_wave_direction_sin_mean",
            },
        ),
        (
            "fusion_wave_direction_cos_mean",
            {
                "baltic": "baltic_wave_direction_cos_mean",
                "nws": "nws_wave_direction_cos_mean",
                "nora3": "nora3_wave_direction_cos_mean",
            },
        ),
    ]:
        out[target] = np.nan
        for source, source_col in source_map.items():
            mask = out["fusion_wave_source"].eq(source)
            out.loc[mask, target] = pd.to_numeric(out.loc[mask, source_col], errors="coerce")

    out["fusion_wave_assignment_method"] = pd.NA
    out.loc[out["fusion_wave_source"].eq("baltic"), "fusion_wave_assignment_method"] = out.loc[
        out["fusion_wave_source"].eq("baltic"), "baltic_wave_assignment_method"
    ]
    out.loc[out["fusion_wave_source"].eq("nws"), "fusion_wave_assignment_method"] = out.loc[
        out["fusion_wave_source"].eq("nws"), "nws_wave_assignment_method"
    ]
    out.loc[out["fusion_wave_source"].eq("nora3"), "fusion_wave_assignment_method"] = (
        "nora3_existing_active_dwell_weather_join"
    )

    out["fusion_wave_missing_reason"] = pd.NA
    missing = out["fusion_wave_source"].eq("missing")
    out.loc[missing, "fusion_wave_missing_reason"] = "no_wave_source_with_hs_tp_for_event"
    return out


def _coverage_table(df: pd.DataFrame, by: str) -> pd.DataFrame:
    grouped = df.groupby(by, dropna=False)
    rows = []
    for key, group in grouped:
        rows.append(
            {
                by: key,
                "rows": len(group),
                "nora3_hs_tp_rows": int(_source_available(group, "nora3").sum()),
                "fusion_hs_tp_rows": int(
                    group["fusion_hs_mean"].notna().mul(group["fusion_tp_mean"].notna()).sum()
                ),
                "fusion_coverage_rate": float(
                    (group["fusion_hs_mean"].notna() & group["fusion_tp_mean"].notna()).mean()
                ),
                "nora3_coverage_rate": float(_source_available(group, "nora3").mean()),
            }
        )
    return pd.DataFrame(rows).sort_values("rows", ascending=False)


def _threshold_comparison(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    tier_a = df[df["dwell_tier"].eq("Tier A")]
    for threshold in [1.0, 1.5, 2.0, 2.5]:
        for subset_name, subset in [("all_rows", df), ("tier_a", tier_a)]:
            nora3_valid = _source_available(subset, "nora3")
            fusion_valid = subset["fusion_hs_mean"].notna() & subset["fusion_tp_mean"].notna()
            rows.append(
                {
                    "subset": subset_name,
                    "hs_threshold_m": threshold,
                    "nora3_valid_rows": int(nora3_valid.sum()),
                    "nora3_rows_below_threshold": int((subset.loc[nora3_valid, "nora3_hs_mean"] <= threshold).sum()),
                    "fusion_valid_rows": int(fusion_valid.sum()),
                    "fusion_rows_below_threshold": int(
                        (subset.loc[fusion_valid, "fusion_hs_mean"] <= threshold).sum()
                    ),
                }
            )
    return pd.DataFrame(rows)


def _tp_boundary(df: pd.DataFrame, hs_col: str, tp_col: str, label: str) -> pd.DataFrame:
    tier_a = df[df["dwell_tier"].eq("Tier A") & df[hs_col].notna() & df[tp_col].notna()].copy()
    if tier_a.empty:
        return pd.DataFrame(
            columns=["source", "tp_bin_left", "tp_bin_right", "rows", "hs_p50", "hs_p95"]
        )
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
                "rows": len(group),
                "hs_p50": float(group[hs_col].quantile(0.50)),
                "hs_p95": float(group[hs_col].quantile(0.95)),
            }
        )
    return pd.DataFrame(rows)


def _boundary_comparison(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    nora3 = _tp_boundary(df, "nora3_hs_mean", "nora3_tp_mean", "nora3")
    fusion = _tp_boundary(df, "fusion_hs_mean", "fusion_tp_mean", "fusion_v0")
    combined = pd.concat([nora3, fusion], ignore_index=True)
    summary: dict[str, Any] = {
        "nora3_boundary_bins": int(len(nora3)),
        "fusion_boundary_bins": int(len(fusion)),
        "common_boundary_bins": 0,
        "mean_abs_p95_change_m": np.nan,
        "max_abs_p95_change_m": np.nan,
    }
    if not nora3.empty and not fusion.empty:
        merged = nora3.merge(
            fusion,
            on=["tp_bin_left", "tp_bin_right"],
            suffixes=("_nora3", "_fusion"),
        )
        if not merged.empty:
            diff = merged["hs_p95_fusion"] - merged["hs_p95_nora3"]
            summary.update(
                {
                    "common_boundary_bins": int(len(merged)),
                    "mean_abs_p95_change_m": float(diff.abs().mean()),
                    "max_abs_p95_change_m": float(diff.abs().max()),
                }
            )
    return combined, summary


def _overlap_metrics(df: pd.DataFrame) -> pd.DataFrame:
    pairs = [
        ("nora3", "nws"),
        ("nora3", "baltic"),
        ("nws", "baltic"),
    ]
    rows = []
    for left, right in pairs:
        mask = _source_available(df, left) & _source_available(df, right)
        subset = df.loc[mask]
        if subset.empty:
            rows.append(
                {
                    "comparison": f"{left}_minus_{right}",
                    "overlap_rows": 0,
                    "hs_bias": np.nan,
                    "hs_median_abs_diff": np.nan,
                    "hs_rmse": np.nan,
                    "tp_bias": np.nan,
                    "tp_median_abs_diff": np.nan,
                    "tp_rmse": np.nan,
                }
            )
            continue
        hs_diff = subset[f"{left}_hs_mean"].astype(float) - subset[f"{right}_hs_mean"].astype(float)
        tp_diff = subset[f"{left}_tp_mean"].astype(float) - subset[f"{right}_tp_mean"].astype(float)
        rows.append(
            {
                "comparison": f"{left}_minus_{right}",
                "overlap_rows": int(len(subset)),
                "hs_bias": float(hs_diff.mean()),
                "hs_median_abs_diff": float(hs_diff.abs().median()),
                "hs_rmse": float(np.sqrt(np.mean(np.square(hs_diff)))),
                "tp_bias": float(tp_diff.mean()),
                "tp_median_abs_diff": float(tp_diff.abs().median()),
                "tp_rmse": float(np.sqrt(np.mean(np.square(tp_diff)))),
            }
        )
    return pd.DataFrame(rows)


def _physical_checks(df: pd.DataFrame) -> dict[str, Any]:
    direction = df[["fusion_wave_direction_sin_mean", "fusion_wave_direction_cos_mean"]].dropna()
    direction_magnitude = (
        np.sqrt(
            np.square(direction["fusion_wave_direction_sin_mean"].astype(float))
            + np.square(direction["fusion_wave_direction_cos_mean"].astype(float))
        )
        if not direction.empty
        else pd.Series(dtype=float)
    )
    return {
        "fusion_hs_non_negative": bool((df["fusion_hs_mean"].dropna().astype(float) >= 0).all()),
        "fusion_tp_positive_where_non_null": bool((df["fusion_tp_mean"].dropna().astype(float) > 0).all()),
        "fusion_direction_pair_count": int(len(direction)),
        "fusion_direction_sincos_magnitude_le_1": bool((direction_magnitude <= 1.000001).all())
        if len(direction_magnitude)
        else True,
        "water_depth_positive_down_non_negative": bool((df["water_depth_m"].dropna().astype(float) >= 0).all()),
    }


def _write_validation_tables(
    df: pd.DataFrame,
    report_dir: Path,
) -> dict[str, Path]:
    report_dir.mkdir(parents=True, exist_ok=True)
    table_paths = {
        "coverage_by_farm": report_dir / "coverage_by_farm.csv",
        "coverage_by_year": report_dir / "coverage_by_year.csv",
        "coverage_by_tier": report_dir / "coverage_by_tier.csv",
        "bathymetry_shallow_by_farm": report_dir / "bathymetry_shallow_by_farm.csv",
        "source_distribution": report_dir / "source_distribution.csv",
        "source_overlap_comparison": report_dir / "source_overlap_comparison.csv",
        "tier_a_tp_boundary_comparison": report_dir / "tier_a_tp_boundary_comparison.csv",
        "static_threshold_comparison": report_dir / "static_threshold_comparison.csv",
    }

    coverage_farm = _coverage_table(df, "farm_id")
    coverage_year = _coverage_table(df.assign(event_year=df["start_utc"].dt.year), "event_year")
    coverage_tier = _coverage_table(df, "dwell_tier")
    source_distribution = (
        df["fusion_wave_source"].value_counts(dropna=False).rename_axis("fusion_wave_source").reset_index(name="rows")
    )
    source_distribution["rate"] = source_distribution["rows"] / len(df) if len(df) else 0.0
    depth = pd.to_numeric(df["water_depth_m"], errors="coerce")
    shallow = (
        df.assign(_water_depth_m=depth)
        .loc[depth <= 1.0]
        .groupby(["farm_id", "bathymetry_sample_point_id"], dropna=False)
        .agg(
            rows=("dwell_id", "size"),
            min_depth_m=("_water_depth_m", "min"),
            median_depth_m=("_water_depth_m", "median"),
            max_depth_m=("_water_depth_m", "max"),
        )
        .reset_index()
        .sort_values("rows", ascending=False)
    )
    overlap = _overlap_metrics(df)
    boundary, _ = _boundary_comparison(df)
    thresholds = _threshold_comparison(df)

    coverage_farm.to_csv(table_paths["coverage_by_farm"], index=False)
    coverage_year.to_csv(table_paths["coverage_by_year"], index=False)
    coverage_tier.to_csv(table_paths["coverage_by_tier"], index=False)
    shallow.to_csv(table_paths["bathymetry_shallow_by_farm"], index=False)
    source_distribution.to_csv(table_paths["source_distribution"], index=False)
    overlap.to_csv(table_paths["source_overlap_comparison"], index=False)
    boundary.to_csv(table_paths["tier_a_tp_boundary_comparison"], index=False)
    thresholds.to_csv(table_paths["static_threshold_comparison"], index=False)
    return table_paths


def _format_table(df: pd.DataFrame, max_rows: int = 20) -> str:
    if df.empty:
        return "_No rows._"
    trimmed = df.head(max_rows)
    return trimmed.to_markdown(index=False)


def _render_report(validation: dict[str, Any], table_paths: dict[str, Path]) -> str:
    source_distribution = validation["source_distribution"]
    shallow = validation["bathymetry_shallow_by_farm"]
    coverage_comparison = validation["coverage_comparison"]
    bathymetry = validation["bathymetry"]
    physical = validation["physical_checks"]
    boundary_summary = validation["tier_a_boundary_summary"]
    overlap = validation["source_overlap_comparison"]
    threshold = validation["static_threshold_comparison"]

    table_list = "\n".join(f"- `{name}`: `{path}`" for name, path in table_paths.items())

    lines = [
        "# Metocean Fusion v0 Validation Report",
        "",
        "Status: completed Fusion v0 research increment. Existing accepted source archives were read, not mutated. No current download, FINO import, NORA3 rerun, source archive interpolation change, or final production dwell-metocean rebuild was run.",
        "",
        "## A. Research Question And Experiment Design",
        "",
        "**Hypothesis:** replacing the current NORA3-only active weather fields with source-resolved regional waves plus static site depth will improve Hs/Tp event coverage and may alter the observed Tier A Hs/Tp operating envelope.",
        "",
        "**Source priority rules:** Baltic wave if the event farm/year and timestamp are covered; else NWS wave if covered; else existing NORA3 active fields; else mark wave missing with a reason. Source-specific comparator columns are preserved where overlaps exist.",
        "",
        "**Metrics:** row preservation, duplicate dwell IDs, Hs/Tp coverage gain, source distribution, farm/year/tier coverage, source-overlap bias/RMSE, bathymetry completeness, Tier A percentile-boundary change, and static Hs threshold counts.",
        "",
        "**Acceptance gates:** row count equals input; no rows dropped for missing weather; required columns present; no duplicate dwell IDs beyond input state; physical checks pass; bathymetry provenance is populated where depth exists.",
        "",
        "**Caveats:** Fusion v0 is still an observed successful-dwell envelope, not calibrated `P(operation | weather)`. Currents and FINO validation are not included. Regional source assignment uses the nearest accepted common sample point to each dwell centroid and preserves the assignment method.",
        "",
        "## B. Implementation Outputs",
        "",
        f"- input_rows: `{validation['input_row_count']}`",
        f"- output_rows: `{validation['output_row_count']}`",
        f"- row_count_preserved: `{validation['row_count_preserved']}`",
        f"- duplicate_dwell_id_count: `{validation['duplicate_dwell_id_count']}`",
        f"- output_table: `{validation['output_path']}`",
        "",
        "## C. Coverage Comparison",
        "",
        f"- nora3_hs_tp_rows: `{coverage_comparison['nora3_hs_tp_rows']}`",
        f"- nora3_hs_tp_rate: `{coverage_comparison['nora3_hs_tp_rate']:.4f}`",
        f"- fusion_hs_tp_rows: `{coverage_comparison['fusion_hs_tp_rows']}`",
        f"- fusion_hs_tp_rate: `{coverage_comparison['fusion_hs_tp_rate']:.4f}`",
        f"- absolute_gain_rows: `{coverage_comparison['absolute_gain_rows']}`",
        f"- percentage_gain_vs_nora3: `{coverage_comparison['percentage_gain_vs_nora3']:.2f}%`",
        f"- tier_a_nora3_hs_tp_rows: `{coverage_comparison['tier_a_nora3_hs_tp_rows']}`",
        f"- tier_a_fusion_hs_tp_rows: `{coverage_comparison['tier_a_fusion_hs_tp_rows']}`",
        "",
        "## Source Distribution",
        "",
        _format_table(source_distribution),
        "",
        "## Bathymetry Coverage",
        "",
        f"- non_null_depth_rows: `{bathymetry['non_null_depth_rows']}`",
        f"- missing_depth_rows: `{bathymetry['missing_depth_rows']}`",
        f"- shallow_depth_le_1m_rows: `{bathymetry['shallow_depth_le_1m_rows']}`",
        f"- zero_depth_rows: `{bathymetry['zero_depth_rows']}`",
        f"- assignment_method_counts: `{bathymetry['assignment_method_counts']}`",
        "",
        "### Shallow / Zero-Depth Event Rows",
        "",
        "These rows preserve valid EMODnet point-sample provenance but should be reviewed before depth is used as a hard modelling covariate.",
        "",
        _format_table(shallow),
        "",
        "## Physical Checks",
        "",
        "\n".join(f"- {key}: `{value}`" for key, value in physical.items()),
        "",
        "## Source-Overlap Comparison",
        "",
        _format_table(overlap),
        "",
        "## Tier A Hs/Tp Envelope Change",
        "",
        f"- nora3_boundary_bins: `{boundary_summary['nora3_boundary_bins']}`",
        f"- fusion_boundary_bins: `{boundary_summary['fusion_boundary_bins']}`",
        f"- common_boundary_bins: `{boundary_summary['common_boundary_bins']}`",
        f"- mean_abs_p95_change_m: `{boundary_summary['mean_abs_p95_change_m']}`",
        f"- max_abs_p95_change_m: `{boundary_summary['max_abs_p95_change_m']}`",
        "",
        "## Static Hs Threshold Comparison",
        "",
        _format_table(threshold),
        "",
        "## Detailed Validation Tables",
        "",
        table_list,
        "",
        "## Conclusion",
        "",
        f"- coverage_improved: `{coverage_comparison['absolute_gain_rows'] > 0}`",
        f"- material_tier_a_boundary_change_flag: `{validation['material_tier_a_boundary_change_flag']}`",
        "- ready_for_current_v1_integration: `True` if the caveats above are accepted; currents should be added as a separate v1 pilot after true `uo/vo` products are validated.",
        "- next_increment: `Current pilot planner and one-farm/year current pilots, not broad current downloads.`",
        "",
    ]
    return "\n".join(lines)


def validate_fusion_v0(
    output: pd.DataFrame,
    input_row_count: int,
    output_path: Path,
    report_dir: Path,
) -> tuple[dict[str, Any], dict[str, Path]]:
    nora3_available = _source_available(output, "nora3")
    fusion_available = output["fusion_hs_mean"].notna() & output["fusion_tp_mean"].notna()
    tier_a = output["dwell_tier"].eq("Tier A")
    source_distribution = (
        output["fusion_wave_source"].value_counts(dropna=False).rename_axis("fusion_wave_source").reset_index(name="rows")
    )
    source_distribution["rate"] = source_distribution["rows"] / len(output) if len(output) else 0.0
    overlap = _overlap_metrics(output)
    thresholds = _threshold_comparison(output)
    _, boundary_summary = _boundary_comparison(output)
    physical_checks = _physical_checks(output)

    bathymetry_depth = pd.to_numeric(output["water_depth_m"], errors="coerce")
    bathymetry_summary = {
        "non_null_depth_rows": int(bathymetry_depth.notna().sum()),
        "missing_depth_rows": int(bathymetry_depth.isna().sum()),
        "shallow_depth_le_1m_rows": int((bathymetry_depth <= 1.0).sum()),
        "zero_depth_rows": int((bathymetry_depth == 0.0).sum()),
        "assignment_method_counts": output["bathymetry_assignment_method"]
        .value_counts(dropna=False)
        .to_dict(),
    }
    shallow_by_farm = (
        output.assign(_water_depth_m=bathymetry_depth)
        .loc[bathymetry_depth <= 1.0]
        .groupby(["farm_id", "bathymetry_sample_point_id"], dropna=False)
        .agg(
            rows=("dwell_id", "size"),
            min_depth_m=("_water_depth_m", "min"),
            median_depth_m=("_water_depth_m", "median"),
            max_depth_m=("_water_depth_m", "max"),
        )
        .reset_index()
        .sort_values("rows", ascending=False)
    )

    coverage_comparison = {
        "nora3_hs_tp_rows": int(nora3_available.sum()),
        "nora3_hs_tp_rate": float(nora3_available.mean()) if len(output) else 0.0,
        "fusion_hs_tp_rows": int(fusion_available.sum()),
        "fusion_hs_tp_rate": float(fusion_available.mean()) if len(output) else 0.0,
        "absolute_gain_rows": int(fusion_available.sum() - nora3_available.sum()),
        "percentage_gain_vs_nora3": float(
            ((fusion_available.sum() - nora3_available.sum()) / nora3_available.sum()) * 100
        )
        if nora3_available.sum()
        else np.nan,
        "tier_a_nora3_hs_tp_rows": int((nora3_available & tier_a).sum()),
        "tier_a_fusion_hs_tp_rows": int((fusion_available & tier_a).sum()),
    }

    table_paths = _write_validation_tables(output, report_dir)
    required_missing = sorted(set(REQUIRED_OUTPUT_COLUMNS) - set(output.columns))
    validation = {
        "input_row_count": int(input_row_count),
        "output_row_count": int(len(output)),
        "row_count_preserved": bool(input_row_count == len(output)),
        "duplicate_dwell_id_count": int(output["dwell_id"].duplicated().sum()) if "dwell_id" in output else np.nan,
        "required_missing_columns": required_missing,
        "coverage_comparison": coverage_comparison,
        "source_distribution": source_distribution,
        "bathymetry": bathymetry_summary,
        "bathymetry_shallow_by_farm": shallow_by_farm,
        "physical_checks": physical_checks,
        "source_overlap_comparison": overlap,
        "static_threshold_comparison": thresholds,
        "tier_a_boundary_summary": boundary_summary,
        "material_tier_a_boundary_change_flag": bool(
            pd.notna(boundary_summary["mean_abs_p95_change_m"])
            and float(boundary_summary["mean_abs_p95_change_m"]) >= 0.1
        ),
        "output_path": str(output_path),
    }
    return validation, table_paths


def build_metocean_fusion_v0(
    dwell_weather_input: Path = DEFAULT_DWELL_WEATHER_INPUT,
    nws_root: Path = DEFAULT_NWS_ROOT,
    baltic_root: Path = DEFAULT_BALTIC_ROOT,
    bathymetry_points: Path = DEFAULT_BATHYMETRY_POINTS,
    output_path: Path = DEFAULT_OUTPUT_PATH,
    report_path: Path = DEFAULT_REPORT_PATH,
    overwrite: bool = False,
) -> FusionV0Result:
    if output_path.exists() and not overwrite:
        raise FileExistsError(f"Fusion v0 output already exists and overwrite is false: {output_path}")
    if report_path.exists() and not overwrite:
        raise FileExistsError(f"Fusion v0 report already exists and overwrite is false: {report_path}")

    dwell = _load_dwell_weather(dwell_weather_input)
    bathymetry = _load_bathymetry(bathymetry_points)

    bathy_assignment = assign_bathymetry_to_events(dwell, bathymetry)
    nws = assign_wave_source_to_events(dwell, bathymetry, NWS_CONFIG.__class__(**{**NWS_CONFIG.__dict__, "root": nws_root}))
    baltic = assign_wave_source_to_events(
        dwell, bathymetry, BALTIC_CONFIG.__class__(**{**BALTIC_CONFIG.__dict__, "root": baltic_root})
    )

    output = dwell[
        ["dwell_id", "visit_id", "wind_farm", "farm_id", "dwell_tier", "start_utc", "end_utc"]
    ].copy()
    output["nora3_hs_mean"] = dwell["active_hs_mean"]
    output["nora3_tp_mean"] = dwell["active_tp_mean"]
    output["nora3_wave_direction_sin_mean"] = dwell["active_wave_direction_sin_mean"]
    output["nora3_wave_direction_cos_mean"] = dwell["active_wave_direction_cos_mean"]
    output = pd.concat([output, nws, baltic, bathy_assignment], axis=1)
    output = _select_fusion_source(output)

    preferred_columns = REQUIRED_OUTPUT_COLUMNS + [
        "nora3_wave_direction_sin_mean",
        "nora3_wave_direction_cos_mean",
        "nws_wave_direction_sin_mean",
        "nws_wave_direction_cos_mean",
        "baltic_wave_direction_sin_mean",
        "baltic_wave_direction_cos_mean",
        "nws_wave_sample_point_id",
        "nws_wave_spatial_distance_km",
        "nws_wave_assignment_method",
        "nws_wave_missing_reason",
        "baltic_wave_sample_point_id",
        "baltic_wave_spatial_distance_km",
        "baltic_wave_assignment_method",
        "baltic_wave_missing_reason",
        "bathymetry_assignment_method",
        "bathymetry_sample_point_id",
        "bathymetry_sample_point_type",
        "bathymetry_event_to_sample_distance_km",
    ]
    output = output[[column for column in preferred_columns if column in output.columns]]

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output.to_parquet(output_path, index=False)

    report_path.parent.mkdir(parents=True, exist_ok=True)
    validation, table_paths = validate_fusion_v0(
        output=output,
        input_row_count=len(dwell),
        output_path=output_path,
        report_dir=report_path.parent,
    )
    report_path.write_text(_render_report(validation, table_paths), encoding="utf-8")
    return FusionV0Result(
        output_path=output_path,
        report_path=report_path,
        output=output,
        validation=validation,
    )
