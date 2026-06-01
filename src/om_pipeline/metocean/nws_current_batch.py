"""Controlled NWS hourly `uo`/`vo` current batch extraction.

This module builds the first source-specific NWS current archive from the
accepted scale-preflight table. It is intentionally constrained to selected
farm-years, writes a manifest, preserves raw Copernicus subsets in a labelled
pilot cache, and rejects fallback/synthetic current evidence.
"""

from __future__ import annotations

import math
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from .current_pilot_v1 import (
    NWS_PRODUCT,
    _find_coord_name,
    _find_var_name,
    _select_spatial_point,
    _select_surface_depth,
    derive_current_speed_direction,
    farm_slug,
    haversine_distance,
    load_dwell_events_for_farm_year,
    normalize_farm_name,
    summarize_event_scale_suitability,
    validate_real_current_candidates,
)
from .current_scaling_preflight import (
    NWS_CURRENT_DATASET_ID,
    NWS_CURRENT_PRODUCT_ID,
    classify_runtime,
)


DEFAULT_ELIGIBILITY = Path(
    "Data/Processed/metocean/current_pilots/nws_current_scale_eligibility.parquet"
)
DEFAULT_DWELL_WEATHER = Path(
    "Data/Processed/ais_dwell_backfill/cross_farm_dwell_weather_features.parquet"
)
DEFAULT_BATHYMETRY = Path("Data/Processed/metocean/bathymetry/site_bathymetry_points.parquet")
DEFAULT_OUTPUT_ROOT = Path("Data/Processed/metocean/nws_current_timeseries")
DEFAULT_RAW_CACHE_ROOT = Path("Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots")
DEFAULT_REPORT_DIR = Path("reports/current_pilot_v1/nws_batch_top10")

MANIFEST_FILENAME = "manifest.csv"
DRY_RUN_REPORT_FILENAME = "nws_current_batch_dry_run_report.md"
VALIDATION_REPORT_FILENAME = "nws_current_batch_validation_report.md"

CURRENT_DIRECTION_CONVENTION = "flow_to_degrees_clockwise_from_true_north"
NWS_MODEL_MIN_DEPTH_WARNING = "nws_model_min_depth_warning_le_10m"

NWS_CURRENT_BATCH_COLUMNS = [
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
    "current_model_bathymetry_warning",
]

MANIFEST_COLUMNS = [
    "wind_farm",
    "farm_id",
    "year",
    "status",
    "row_count",
    "sample_point_count",
    "timestamp_start",
    "timestamp_end",
    "source_file",
    "processed_path",
    "qa_status",
    "message",
]


@dataclass(frozen=True)
class BatchRunResult:
    selected: pd.DataFrame
    manifest: pd.DataFrame
    dry_run_report_path: Path
    validation_report_path: Path | None
    manifest_path: Path
    output_root: Path
    raw_cache_root: Path


def ensure_batch_schema(df: pd.DataFrame) -> pd.DataFrame:
    for col in NWS_CURRENT_BATCH_COLUMNS:
        if col not in df.columns:
            df[col] = pd.NA
    return df[NWS_CURRENT_BATCH_COLUMNS].copy()


def partition_path(output_root: Path, wind_farm: str, year: int) -> Path:
    return output_root / f"wind_farm={farm_slug(wind_farm)}" / f"year={int(year)}" / "part.parquet"


def raw_cache_path(raw_cache_root: Path, wind_farm: str, year: int, dataset_id: str) -> Path:
    return (
        raw_cache_root
        / f"wind_farm={farm_slug(wind_farm)}"
        / f"year={int(year)}"
        / f"nws_current_{farm_slug(wind_farm)}_{int(year)}_{dataset_id}.nc"
    )


def ensure_raw_cache_guard(raw_cache_root: Path) -> None:
    parts = raw_cache_root.parts
    if "Data" not in parts or "Raw" not in parts or "Metocean" not in parts:
        raise ValueError("Raw cache root must live under Data/Raw/Metocean")
    if raw_cache_root.name.startswith("cmems_raw"):
        raise ValueError("Raw cache root must not use the legacy cmems_raw current cache path")


def load_recommended_farm_years(
    eligibility_path: Path,
    top_n: int | None = 10,
    all_recommended: bool = False,
) -> pd.DataFrame:
    if not eligibility_path.exists():
        raise FileNotFoundError(f"Eligibility table not found: {eligibility_path}")
    eligibility = pd.read_parquet(eligibility_path)
    recommended = eligibility[eligibility["recommended_for_scale"].eq("yes")].copy()
    selected = recommended if all_recommended else recommended.head(int(top_n or 0)).copy()
    if selected.empty:
        raise ValueError("No recommended NWS farm-years found in eligibility table")
    if selected["recommended_for_scale"].ne("yes").any():
        raise ValueError("Stress-test or non-recommended farm-years cannot be selected for this batch")
    selected["selected_rank"] = range(1, len(selected) + 1)
    return selected.reset_index(drop=True)


def load_top_recommended_farm_years(eligibility_path: Path, top_n: int) -> pd.DataFrame:
    return load_recommended_farm_years(
        eligibility_path=eligibility_path,
        top_n=top_n,
        all_recommended=False,
    )


def load_bathymetry_points(bathymetry_path: Path, wind_farm: str) -> pd.DataFrame:
    columns = [
        "wind_farm",
        "sample_point_id",
        "sample_point_type",
        "lat",
        "lon",
        "water_depth_m",
    ]
    schema = pq.read_schema(bathymetry_path).names
    bathy = pd.read_parquet(bathymetry_path, columns=[col for col in columns if col in schema])
    for col in columns:
        if col not in bathy.columns:
            bathy[col] = pd.NA
    normalized = normalize_farm_name(wind_farm)
    points = bathy[bathy["wind_farm"].map(normalize_farm_name).eq(normalized)].copy()
    if points.empty:
        raise FileNotFoundError(f"No bathymetry sample points found for {wind_farm}")
    points["water_depth_m"] = pd.to_numeric(points["water_depth_m"], errors="coerce")
    points = points.sort_values(["sample_point_type", "sample_point_id"]).reset_index(drop=True)
    return points


def subset_bounds(sample_points: pd.DataFrame, pad_degrees: float = 0.03) -> dict[str, float]:
    return {
        "minimum_longitude": float(sample_points["lon"].min() - pad_degrees),
        "maximum_longitude": float(sample_points["lon"].max() + pad_degrees),
        "minimum_latitude": float(sample_points["lat"].min() - pad_degrees),
        "maximum_latitude": float(sample_points["lat"].max() + pad_degrees),
    }


def download_nws_current_subset(
    sample_points: pd.DataFrame,
    year: int,
    raw_path: Path,
    product_id: str,
    dataset_id: str,
    overwrite: bool,
) -> Path:
    """Download a scoped one-farm/year NWS current subset to NetCDF."""
    import copernicusmarine

    raw_path.parent.mkdir(parents=True, exist_ok=True)
    request = {
        "dataset_id": dataset_id,
        "variables": ["uo", "vo"],
        "start_datetime": f"{int(year)}-01-01T00:00:00",
        "end_datetime": f"{int(year)}-12-31T23:59:59",
        "coordinates_selection_method": "outside",
        **subset_bounds(sample_points),
    }
    try:
        response = copernicusmarine.subset(
            output_directory=raw_path.parent,
            output_filename=raw_path.name,
            file_format="netcdf",
            overwrite=overwrite,
            disable_progress_bar=True,
            **request,
        )
        paths = getattr(response, "file_path", None) or getattr(response, "file_paths", None)
        if isinstance(paths, (list, tuple)) and paths:
            return Path(paths[0])
        if paths:
            return Path(paths)
    except ValueError as exc:
        if "numpy.dtype size changed" not in str(exc):
            raise

    # Some local environments can open the Copernicus dataset lazily but fail in
    # the toolbox subset writer because of optional binary dependency drift. The
    # fallback still downloads only the scoped bbox/year and preserves a NetCDF
    # raw cache under the approved pilot path.
    ds = copernicusmarine.open_dataset(**request)
    try:
        ds.to_netcdf(raw_path)
    finally:
        close = getattr(ds, "close", None)
        if callable(close):
            close()
    return raw_path


def open_raw_dataset(raw_path: Path) -> Any:
    import xarray as xr

    return xr.open_dataset(raw_path)


def _time_name(ds: Any) -> str:
    found = _find_coord_name(ds, ("time", "time_counter", "valid_time"))
    if found is None:
        raise ValueError("Dataset has no recognizable time coordinate")
    return found


def _current_var_names(ds: Any) -> tuple[str, str]:
    u_var = _find_var_name(ds, ("uo", "eastward_sea_water_velocity"))
    v_var = _find_var_name(ds, ("vo", "northward_sea_water_velocity"))
    if u_var is None or v_var is None:
        raise ValueError("Dataset does not contain true uo/vo current variables")
    return u_var, v_var


def _depth_selection_rule(point_ds: Any, u_var: str, v_var: str, depth_m: float) -> str:
    variable_dims = set(point_ds[u_var].dims) | set(point_ds[v_var].dims)
    if any(name in variable_dims for name in ("depth", "depthu", "depthv", "deptht", "lev", "elevation")):
        return f"nearest_surface_depth_{depth_m:g}m"
    return "surface_2d_no_depth_dimension"


def _point_rows_from_dataset(
    ds: Any,
    point: pd.Series,
    wind_farm: str,
    farm_id: str,
    year: int,
    product_id: str,
    dataset_id: str,
    source_file: str,
) -> pd.DataFrame:
    point_ds, grid_lat, grid_lon, distance_km, assignment_method = _select_spatial_point(
        ds,
        float(point["lat"]),
        float(point["lon"]),
    )
    u_var, v_var = _current_var_names(point_ds)
    point_ds, current_depth_m = _select_surface_depth(point_ds, u_var, v_var)
    depth_rule = _depth_selection_rule(point_ds, u_var, v_var, current_depth_m)
    time_name = _time_name(point_ds)

    frame = point_ds[[u_var, v_var]].to_dataframe().reset_index()
    frame = frame.rename(columns={time_name: "timestamp_utc", u_var: "current_u", v_var: "current_v"})
    frame["timestamp_utc"] = pd.to_datetime(frame["timestamp_utc"], utc=True, errors="coerce")
    frame = frame.dropna(subset=["timestamp_utc"]).copy()
    frame = frame[frame["timestamp_utc"].dt.year.eq(int(year))].copy()
    frame = frame[["timestamp_utc", "current_u", "current_v"]].copy()
    frame["current_u"] = pd.to_numeric(frame["current_u"], errors="coerce")
    frame["current_v"] = pd.to_numeric(frame["current_v"], errors="coerce")
    speed, direction = derive_current_speed_direction(frame["current_u"], frame["current_v"])
    frame["current_speed"] = speed
    frame["current_direction_to_deg"] = direction

    water_depth = float(point["water_depth_m"]) if pd.notna(point["water_depth_m"]) else math.nan
    frame["wind_farm"] = wind_farm
    frame["farm_id"] = farm_id
    frame["year"] = int(year)
    frame["sample_point_id"] = point["sample_point_id"]
    frame["sample_point_type"] = point["sample_point_type"]
    frame["lat"] = float(point["lat"])
    frame["lon"] = float(point["lon"])
    frame["current_grid_lat"] = grid_lat
    frame["current_grid_lon"] = grid_lon
    frame["current_spatial_distance_km"] = distance_km
    frame["current_direction_convention"] = CURRENT_DIRECTION_CONVENTION
    frame["current_depth_m"] = current_depth_m
    frame["current_depth_selection_rule"] = depth_rule
    frame["current_source"] = "NWS"
    frame["current_product_id"] = product_id
    frame["current_dataset_id"] = dataset_id
    frame["current_native_temporal_resolution_minutes"] = 60
    frame["current_native_spatial_resolution_km"] = 7.0
    frame["current_assignment_method"] = assignment_method
    frame["current_spatial_match_status"] = "ok" if float(distance_km) <= 10.0 else "distant"
    frame["source_file"] = source_file
    frame["provenance_status"] = "real_uo_vo"
    frame["emodnet_water_depth_m"] = water_depth
    frame["depth_warning_le_1m"] = bool(pd.notna(water_depth) and water_depth <= 1.0)
    frame["depth_warning_le_5m"] = bool(pd.notna(water_depth) and water_depth <= 5.0)
    frame["depth_warning_le_10m"] = bool(pd.notna(water_depth) and water_depth <= 10.0)
    frame["current_model_bathymetry_warning"] = (
        NWS_MODEL_MIN_DEPTH_WARNING if frame["depth_warning_le_10m"].iloc[0] else "none"
    )
    return ensure_batch_schema(frame)


def extract_archive_rows_from_raw(
    raw_path: Path,
    sample_points: pd.DataFrame,
    wind_farm: str,
    farm_id: str,
    year: int,
    product_id: str,
    dataset_id: str,
) -> pd.DataFrame:
    ds = open_raw_dataset(raw_path)
    try:
        rows = [
            _point_rows_from_dataset(
                ds=ds,
                point=point,
                wind_farm=wind_farm,
                farm_id=farm_id,
                year=year,
                product_id=product_id,
                dataset_id=dataset_id,
                source_file=str(raw_path),
            )
            for _, point in sample_points.iterrows()
        ]
    finally:
        close = getattr(ds, "close", None)
        if callable(close):
            close()
    if not rows:
        return ensure_batch_schema(pd.DataFrame())
    archive = pd.concat(rows, ignore_index=True)
    archive = archive.sort_values(["sample_point_id", "timestamp_utc"]).reset_index(drop=True)
    return ensure_batch_schema(archive)


def validate_archive_rows(rows: pd.DataFrame) -> dict[str, Any]:
    rows = ensure_batch_schema(rows)
    pilot_compatible = rows.rename(columns={"current_direction_to_deg": "current_direction"}).copy()
    validate_real_current_candidates(pilot_compatible)
    valid_uv = rows["current_u"].notna() & rows["current_v"].notna()
    duplicate_count = int(
        rows.duplicated(["wind_farm", "year", "sample_point_id", "timestamp_utc"]).sum()
    )
    speed_error = math.nan
    if valid_uv.any():
        expected = np.sqrt(rows.loc[valid_uv, "current_u"].astype(float) ** 2 + rows.loc[valid_uv, "current_v"].astype(float) ** 2)
        speed_error = float(np.nanmax(np.abs(expected - rows.loc[valid_uv, "current_speed"].astype(float))))
    direction_ok = bool(rows["current_direction_to_deg"].dropna().between(0, 360, inclusive="left").all())
    provenance_complete = bool(
        rows[
            [
                "current_product_id",
                "current_dataset_id",
                "source_file",
                "provenance_status",
                "current_direction_convention",
                "current_depth_selection_rule",
            ]
        ]
        .notna()
        .all()
        .all()
    )
    cadence_minutes = _cadence_summary(rows)["median_cadence_minutes"]
    qa_passed = (
        len(rows) > 0
        and int(valid_uv.sum()) == len(rows)
        and duplicate_count == 0
        and bool(direction_ok)
        and bool(provenance_complete)
        and float(speed_error) <= 1e-9
        and abs(float(cadence_minutes) - 60.0) <= 1e-6
        and not rows["provenance_status"].fillna("").astype(str).str.casefold().str.startswith(
            ("fallback", "simulated", "legacy")
        ).any()
    )
    return {
        "row_count": int(len(rows)),
        "valid_uv_count": int(valid_uv.sum()),
        "duplicate_count": duplicate_count,
        "speed_consistency_max_error": speed_error,
        "direction_range_min": float(rows["current_direction_to_deg"].min()) if len(rows) else math.nan,
        "direction_range_max": float(rows["current_direction_to_deg"].max()) if len(rows) else math.nan,
        "direction_ok": direction_ok,
        "provenance_complete": provenance_complete,
        "qa_status": "passed" if qa_passed else "failed",
    }


def _cadence_summary(rows: pd.DataFrame) -> dict[str, float]:
    if rows.empty:
        return {"median_cadence_minutes": math.nan, "p95_cadence_minutes": math.nan}
    diffs = (
        rows.sort_values(["sample_point_id", "timestamp_utc"])
        .groupby("sample_point_id")["timestamp_utc"]
        .diff()
        .dt.total_seconds()
        .div(60.0)
        .dropna()
    )
    if diffs.empty:
        return {"median_cadence_minutes": math.nan, "p95_cadence_minutes": math.nan}
    return {
        "median_cadence_minutes": float(np.nanpercentile(diffs, 50)),
        "p95_cadence_minutes": float(np.nanpercentile(diffs, 95)),
    }


def _circular_abs_diff_deg(values: pd.Series) -> pd.Series:
    diff = values.diff().abs()
    return pd.Series(np.minimum(diff, 360.0 - diff), index=values.index)


def summarize_current_variability(rows: pd.DataFrame) -> dict[str, Any]:
    if rows.empty:
        return {
            "current_speed_min": math.nan,
            "current_speed_mean": math.nan,
            "current_speed_p95": math.nan,
            "current_speed_max": math.nan,
            "median_hourly_speed_delta": math.nan,
            "p95_hourly_speed_delta": math.nan,
            "median_hourly_direction_change_deg": math.nan,
            "p95_hourly_direction_change_deg": math.nan,
            "variability_flag": "not_available",
        }
    ordered = rows.sort_values(["sample_point_id", "timestamp_utc"]).copy()
    speed_delta = ordered.groupby("sample_point_id")["current_speed"].diff().abs().dropna()
    direction_delta = (
        ordered.groupby("sample_point_id", group_keys=False)["current_direction_to_deg"]
        .apply(_circular_abs_diff_deg)
        .dropna()
    )
    speed_p95 = float(rows["current_speed"].quantile(0.95))
    p95_speed_delta = float(np.nanpercentile(speed_delta, 95)) if not speed_delta.empty else math.nan
    p95_direction_delta = (
        float(np.nanpercentile(direction_delta, 95)) if not direction_delta.empty else math.nan
    )
    variability_flag = (
        "operationally_plausible_variability"
        if speed_p95 >= 0.25 or p95_speed_delta >= 0.05 or p95_direction_delta >= 45.0
        else "low_variability"
    )
    return {
        "current_speed_min": float(rows["current_speed"].min()),
        "current_speed_mean": float(rows["current_speed"].mean()),
        "current_speed_p95": speed_p95,
        "current_speed_max": float(rows["current_speed"].max()),
        "median_hourly_speed_delta": float(np.nanpercentile(speed_delta, 50)) if not speed_delta.empty else math.nan,
        "p95_hourly_speed_delta": p95_speed_delta,
        "median_hourly_direction_change_deg": float(np.nanpercentile(direction_delta, 50)) if not direction_delta.empty else math.nan,
        "p95_hourly_direction_change_deg": p95_direction_delta,
        "variability_flag": variability_flag,
    }


def summarize_event_scale(rows: pd.DataFrame, dwell_weather: Path, wind_farm: str, year: int) -> dict[str, Any]:
    dwell = load_dwell_events_for_farm_year(dwell_weather, wind_farm, int(year))
    pilot_compatible = rows.rename(columns={"current_direction_to_deg": "current_direction"}).copy()
    return summarize_event_scale_suitability(pilot_compatible, dwell)


def write_archive_partition(rows: pd.DataFrame, path: Path, overwrite: bool) -> None:
    if path.exists() and not overwrite:
        raise FileExistsError(f"NWS current partition already exists: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    ensure_batch_schema(rows).to_parquet(path, index=False)


def manifest_row(
    selected: pd.Series,
    status: str,
    row_count: int,
    sample_point_count: int,
    timestamp_start: str,
    timestamp_end: str,
    source_file: str,
    processed_path: str,
    qa_status: str,
    message: str,
) -> dict[str, Any]:
    return {
        "wind_farm": selected["wind_farm"],
        "farm_id": selected["farm_id"],
        "year": int(selected["year"]),
        "status": status,
        "row_count": int(row_count),
        "sample_point_count": int(sample_point_count),
        "timestamp_start": timestamp_start,
        "timestamp_end": timestamp_end,
        "source_file": source_file,
        "processed_path": processed_path,
        "qa_status": qa_status,
        "message": message,
    }


def build_manifest(rows: list[dict[str, Any]]) -> pd.DataFrame:
    manifest = pd.DataFrame(rows)
    for col in MANIFEST_COLUMNS:
        if col not in manifest.columns:
            manifest[col] = pd.NA
    return manifest[MANIFEST_COLUMNS].copy()


def write_manifest(manifest: pd.DataFrame, output_root: Path) -> Path:
    output_root.mkdir(parents=True, exist_ok=True)
    path = output_root / MANIFEST_FILENAME
    manifest.to_csv(path, index=False)
    return path


def _format_float(value: Any, digits: int = 3) -> str:
    try:
        if pd.isna(value):
            return "NA"
        return f"{float(value):.{digits}f}"
    except (TypeError, ValueError):
        return str(value)


def _markdown_table(df: pd.DataFrame, columns: list[str]) -> list[str]:
    if df.empty:
        return ["No rows."]
    lines = [
        "| " + " | ".join(columns) + " |",
        "| " + " | ".join(["---"] * len(columns)) + " |",
    ]
    for _, row in df.iterrows():
        values = []
        for col in columns:
            value = row[col]
            if isinstance(value, float):
                value = _format_float(value)
            values.append(str(value))
        lines.append("| " + " | ".join(values) + " |")
    return lines


def disk_free_mb(path: Path) -> float:
    probe = path
    while not probe.exists() and probe.parent != probe:
        probe = probe.parent
    usage = shutil.disk_usage(probe)
    return round(usage.free / 1_000_000.0, 1)


def build_dry_run_report(
    selected: pd.DataFrame,
    output_root: Path,
    raw_cache_root: Path,
    product_id: str,
    dataset_id: str,
    tooling_available: bool,
    selection_scope: str,
) -> str:
    total_rows = int(selected["estimated_current_rows"].sum()) if "estimated_current_rows" in selected else 0
    total_processed = float(selected["estimated_processed_size_mb"].sum()) if "estimated_processed_size_mb" in selected else math.nan
    selected = selected.copy()
    selected["processed_path"] = [
        str(partition_path(output_root, row["wind_farm"], int(row["year"]))) for _, row in selected.iterrows()
    ]
    selected["raw_cache_path"] = [
        str(raw_cache_path(raw_cache_root, row["wind_farm"], int(row["year"]), dataset_id))
        for _, row in selected.iterrows()
    ]
    selected["processed_exists"] = selected["processed_path"].map(lambda value: Path(value).exists())
    selected["raw_cache_exists"] = selected["raw_cache_path"].map(lambda value: Path(value).exists())
    years = sorted(selected["year"].astype(int).unique().tolist())
    batch_type = "single-year engineering batch" if len(years) == 1 else "mixed-year scale batch"
    existing_count = int(selected["processed_exists"].sum())
    remaining_count = len(selected) - existing_count
    lines = [
        "# NWS Current Batch v1 Dry-Run Report",
        "",
        "## Research Design",
        "",
        "This batch tests whether the NWS hourly true `uo/vo` method can scale across the accepted normal recommended farm-years while preserving existing validated partitions and excluding stress-test rows.",
        "",
        f"Selection scope: `{selection_scope}`.",
        f"Batch interpretation: `{batch_type}`. Selected years: {', '.join(map(str, years))}.",
        "",
        "## Pre-Run Checks",
        "",
        f"- Product: `{product_id}` / `{dataset_id}`",
        f"- Selected farm-years: {len(selected)}",
        f"- Existing processed farm-years in selected set: {existing_count}",
        f"- Farm-years remaining for extraction in selected set: {remaining_count}",
        f"- Stress-test farm-years selected: {int(selected['recommended_for_scale'].ne('yes').sum())}",
        f"- Estimated current rows: {total_rows:,}",
        f"- Estimated processed size: {_format_float(total_processed, 1)} MB",
        f"- Output root free space: {_format_float(disk_free_mb(output_root), 1)} MB",
        f"- Raw cache root: `{raw_cache_root}`",
        f"- Raw cache root guard: under `Data/Raw/Metocean` and separate from legacy CMEMS CSV cache",
        f"- Copernicus tooling import available: {tooling_available}",
        "",
        "## Selected Farm-Years",
        "",
    ]
    lines.extend(
        _markdown_table(
            selected,
            [
                "selected_rank",
                "wind_farm",
                "year",
                "dwell_count",
                "tier_a_dwell_count",
                "sample_point_count",
                "estimated_current_rows",
                "processed_exists",
                "raw_cache_exists",
            ],
        )
    )
    lines.extend(
        [
            "",
            "## Guardrails",
            "",
            "- Dry-run only; no current download or processed archive write was performed.",
            "- No stress-test farms are selected.",
            "- Baltic and global currents are out of scope.",
            "- Legacy CMEMS current CSVs and fallback/synthetic currents remain banned.",
        ]
    )
    return "\n".join(lines) + "\n"


def write_dry_run_report(
    selected: pd.DataFrame,
    report_dir: Path,
    output_root: Path,
    raw_cache_root: Path,
    product_id: str,
    dataset_id: str,
    tooling_available: bool,
    selection_scope: str,
) -> Path:
    report_dir.mkdir(parents=True, exist_ok=True)
    path = report_dir / DRY_RUN_REPORT_FILENAME
    path.write_text(
        build_dry_run_report(
            selected=selected,
            output_root=output_root,
            raw_cache_root=raw_cache_root,
            product_id=product_id,
            dataset_id=dataset_id,
            tooling_available=tooling_available,
            selection_scope=selection_scope,
        ),
        encoding="utf-8",
    )
    return path


def build_validation_report(
    selected: pd.DataFrame,
    manifest: pd.DataFrame,
    validations: pd.DataFrame,
    event_scale: pd.DataFrame,
    variability: pd.DataFrame,
    output_root: Path,
    raw_cache_root: Path,
    selection_scope: str,
) -> str:
    accepted = manifest[
        manifest["status"].isin(["validated", "skipped_existing"])
        & manifest["qa_status"].eq("passed")
    ]
    processed = manifest[manifest["status"].eq("validated")]
    skipped = manifest[manifest["status"].eq("skipped_existing")]
    failed = manifest[manifest["status"].eq("failed")]
    total_rows = int(accepted["row_count"].sum()) if not accepted.empty else 0
    years = sorted(selected["year"].astype(int).unique().tolist())
    batch_type = "single-year engineering batch" if len(years) == 1 else "mixed-year scale batch"
    lines = [
        "# NWS Current Batch v1 Validation Report",
        "",
        "## Executive Conclusion",
        "",
        f"This NWS current batch is a `{batch_type}` over `{selection_scope}`. It preserves existing accepted partitions, processes missing normal recommended farm-years, and keeps stress-test farm-years out of the source-specific archive.",
        "",
        f"- Farm-years selected: {len(selected)}",
        f"- Farm-years processed this run: {len(processed)}",
        f"- Farm-years skipped existing and revalidated: {len(skipped)}",
        f"- Farm-years accepted in archive: {len(accepted)}",
        f"- Farm-years failed: {len(failed)}",
        f"- Final row count: {total_rows:,}",
        f"- Final partition count: {len(accepted)}",
        f"- Output root: `{output_root}`",
        f"- Raw cache root: `{raw_cache_root}`",
        "",
        "## Manifest Summary",
        "",
    ]
    lines.extend(_markdown_table(manifest, MANIFEST_COLUMNS))
    lines.extend(["", "## Per Farm-Year QA", ""])
    lines.extend(
        _markdown_table(
            validations,
            [
                "wind_farm",
                "year",
                "row_count",
                "sample_point_count",
                "timestamp_start",
                "timestamp_end",
                "median_cadence_minutes",
                "valid_uv_count",
                "duplicate_count",
                "speed_consistency_max_error",
                "direction_ok",
                "provenance_complete",
                "qa_status",
            ],
        )
    )
    lines.extend(["", "## Event-Scale Suitability", ""])
    lines.extend(
        _markdown_table(
            event_scale,
            [
                "wind_farm",
                "year",
                "dwell_event_count",
                "tier_a_dwell_count",
                "events_with_bracketing_current_samples",
                "events_with_window_samples",
                "event_scale_suitable_pct",
                "nearest_time_gap_minutes_p50",
                "nearest_time_gap_minutes_p95",
                "event_window_sample_count_p50",
                "event_window_sample_count_p95",
            ],
        )
    )
    lines.extend(["", "## Current Variability", ""])
    lines.extend(
        _markdown_table(
            variability,
            [
                "wind_farm",
                "year",
                "current_speed_min",
                "current_speed_mean",
                "current_speed_p95",
                "current_speed_max",
                "median_hourly_speed_delta",
                "p95_hourly_speed_delta",
                "median_hourly_direction_change_deg",
                "p95_hourly_direction_change_deg",
                "variability_flag",
            ],
        )
    )
    lines.extend(
        [
            "",
            "## Acceptance",
            "",
            "The batch is acceptable only if each processed farm-year has true non-null `uo/vo`, no fallback/synthetic provenance, hourly UTC cadence, populated source/depth/direction provenance, no duplicate farm-year-sample-timestamp keys, and event-scale bracketing suitable for the selected dwell windows.",
        ]
    )
    return "\n".join(lines) + "\n"


def write_validation_report(
    selected: pd.DataFrame,
    manifest: pd.DataFrame,
    validations: pd.DataFrame,
    event_scale: pd.DataFrame,
    variability: pd.DataFrame,
    report_dir: Path,
    output_root: Path,
    raw_cache_root: Path,
    selection_scope: str,
) -> Path:
    report_dir.mkdir(parents=True, exist_ok=True)
    path = report_dir / VALIDATION_REPORT_FILENAME
    path.write_text(
        build_validation_report(
            selected=selected,
            manifest=manifest,
            validations=validations,
            event_scale=event_scale,
            variability=variability,
            output_root=output_root,
            raw_cache_root=raw_cache_root,
            selection_scope=selection_scope,
        ),
        encoding="utf-8",
    )
    return path


def copernicus_tooling_available() -> bool:
    try:
        import copernicusmarine  # noqa: F401
    except Exception:
        return False
    return True


def run_nws_current_batch(
    eligibility_path: Path = DEFAULT_ELIGIBILITY,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    raw_cache_root: Path = DEFAULT_RAW_CACHE_ROOT,
    report_dir: Path = DEFAULT_REPORT_DIR,
    bathymetry_path: Path = DEFAULT_BATHYMETRY,
    dwell_weather_path: Path = DEFAULT_DWELL_WEATHER,
    top_n: int = 10,
    remaining_recommended: bool = False,
    exclude_existing: bool = False,
    exclude_stress_test: bool = True,
    dry_run: bool = False,
    overwrite: bool = False,
    product_id: str = NWS_CURRENT_PRODUCT_ID,
    dataset_id: str = NWS_CURRENT_DATASET_ID,
) -> BatchRunResult:
    ensure_raw_cache_guard(raw_cache_root)
    if not exclude_stress_test:
        raise ValueError("Stress-test NWS current farm-years are not approved for this batch")
    if exclude_existing and overwrite:
        raise ValueError("--exclude-existing cannot be combined with overwrite")
    selected = load_recommended_farm_years(
        eligibility_path=eligibility_path,
        top_n=top_n,
        all_recommended=remaining_recommended,
    )
    selection_scope = "all_normal_recommended" if remaining_recommended else f"top_{top_n}_normal_recommended"
    tooling_available = copernicus_tooling_available()
    dry_run_report_path = write_dry_run_report(
        selected=selected,
        report_dir=report_dir,
        output_root=output_root,
        raw_cache_root=raw_cache_root,
        product_id=product_id,
        dataset_id=dataset_id,
        tooling_available=tooling_available,
        selection_scope=selection_scope,
    )

    manifest_rows: list[dict[str, Any]] = []
    validation_rows: list[dict[str, Any]] = []
    event_rows: list[dict[str, Any]] = []
    variability_rows: list[dict[str, Any]] = []

    if dry_run:
        for _, row in selected.iterrows():
            processed_path = partition_path(output_root, row["wind_farm"], int(row["year"]))
            source_path = raw_cache_path(raw_cache_root, row["wind_farm"], int(row["year"]), dataset_id)
            manifest_rows.append(
                manifest_row(
                    selected=row,
                    status="planned",
                    row_count=0,
                    sample_point_count=int(row["sample_point_count"]),
                    timestamp_start="",
                    timestamp_end="",
                    source_file=str(source_path),
                    processed_path=str(processed_path),
                    qa_status="not_run",
                    message="dry-run only; no current extraction performed",
                )
            )
        manifest = build_manifest(manifest_rows)
        manifest_path = write_manifest(manifest, output_root)
        return BatchRunResult(
            selected=selected,
            manifest=manifest,
            dry_run_report_path=dry_run_report_path,
            validation_report_path=None,
            manifest_path=manifest_path,
            output_root=output_root,
            raw_cache_root=raw_cache_root,
        )

    for _, row in selected.iterrows():
        wind_farm = str(row["wind_farm"])
        farm_id = str(row["farm_id"])
        year = int(row["year"])
        processed_path = partition_path(output_root, wind_farm, year)
        source_path = raw_cache_path(raw_cache_root, wind_farm, year, dataset_id)
        try:
            sample_points = load_bathymetry_points(bathymetry_path, wind_farm)
            if processed_path.exists() and not overwrite:
                existing = pd.read_parquet(processed_path)
                qa = validate_archive_rows(existing)
                cadence = _cadence_summary(existing)
                variability = summarize_current_variability(existing)
                event_scale = summarize_event_scale(existing, dwell_weather_path, wind_farm, year)
                validation_rows.append(
                    {
                        "wind_farm": wind_farm,
                        "year": year,
                        "row_count": qa["row_count"],
                        "sample_point_count": int(existing["sample_point_id"].nunique()),
                        "timestamp_start": str(existing["timestamp_utc"].min()),
                        "timestamp_end": str(existing["timestamp_utc"].max()),
                        "median_cadence_minutes": cadence["median_cadence_minutes"],
                        "p95_cadence_minutes": cadence["p95_cadence_minutes"],
                        **qa,
                    }
                )
                event_rows.append(
                    {
                        "wind_farm": wind_farm,
                        "year": year,
                        "tier_a_dwell_count": int(row["tier_a_dwell_count"]),
                        **event_scale,
                    }
                )
                variability_rows.append({"wind_farm": wind_farm, "year": year, **variability})
                manifest_rows.append(
                    manifest_row(
                        selected=row,
                        status="skipped_existing",
                        row_count=len(existing),
                        sample_point_count=existing["sample_point_id"].nunique(),
                        timestamp_start=str(existing["timestamp_utc"].min()),
                        timestamp_end=str(existing["timestamp_utc"].max()),
                        source_file=str(existing["source_file"].dropna().iloc[0])
                        if existing["source_file"].notna().any()
                        else str(source_path),
                        processed_path=str(processed_path),
                        qa_status=qa["qa_status"],
                        message="existing partition validated; overwrite is false",
                    )
                )
                continue
            if not source_path.exists() or overwrite:
                source_path = download_nws_current_subset(
                    sample_points=sample_points,
                    year=year,
                    raw_path=source_path,
                    product_id=product_id,
                    dataset_id=dataset_id,
                    overwrite=overwrite,
                )
            archive = extract_archive_rows_from_raw(
                raw_path=source_path,
                sample_points=sample_points,
                wind_farm=wind_farm,
                farm_id=farm_id,
                year=year,
                product_id=product_id,
                dataset_id=dataset_id,
            )
            write_archive_partition(archive, processed_path, overwrite=True)
            qa = validate_archive_rows(archive)
            cadence = _cadence_summary(archive)
            variability = summarize_current_variability(archive)
            event_scale = summarize_event_scale(archive, dwell_weather_path, wind_farm, year)

            validation_rows.append(
                {
                    "wind_farm": wind_farm,
                    "year": year,
                    "row_count": qa["row_count"],
                    "sample_point_count": int(archive["sample_point_id"].nunique()),
                    "timestamp_start": str(archive["timestamp_utc"].min()),
                    "timestamp_end": str(archive["timestamp_utc"].max()),
                    "median_cadence_minutes": cadence["median_cadence_minutes"],
                    "p95_cadence_minutes": cadence["p95_cadence_minutes"],
                    **qa,
                }
            )
            event_rows.append(
                {
                    "wind_farm": wind_farm,
                    "year": year,
                    "tier_a_dwell_count": int(row["tier_a_dwell_count"]),
                    **event_scale,
                }
            )
            variability_rows.append({"wind_farm": wind_farm, "year": year, **variability})
            manifest_rows.append(
                manifest_row(
                    selected=row,
                    status="validated" if qa["qa_status"] == "passed" else "processed",
                    row_count=qa["row_count"],
                    sample_point_count=int(archive["sample_point_id"].nunique()),
                    timestamp_start=str(archive["timestamp_utc"].min()),
                    timestamp_end=str(archive["timestamp_utc"].max()),
                    source_file=str(source_path),
                    processed_path=str(processed_path),
                    qa_status=qa["qa_status"],
                    message="processed and validated" if qa["qa_status"] == "passed" else "processed but QA failed",
                )
            )
        except Exception as exc:  # pragma: no cover - exercised by integration runs
            manifest_rows.append(
                manifest_row(
                    selected=row,
                    status="failed",
                    row_count=0,
                    sample_point_count=int(row.get("sample_point_count", 0) or 0),
                    timestamp_start="",
                    timestamp_end="",
                    source_file=str(source_path),
                    processed_path=str(processed_path),
                    qa_status="failed",
                    message=f"{type(exc).__name__}: {exc}",
                )
            )

    manifest = build_manifest(manifest_rows)
    manifest_path = write_manifest(manifest, output_root)
    validation_report_path = write_validation_report(
        selected=selected,
        manifest=manifest,
        validations=pd.DataFrame(validation_rows),
        event_scale=pd.DataFrame(event_rows),
        variability=pd.DataFrame(variability_rows),
        report_dir=report_dir,
        output_root=output_root,
        raw_cache_root=raw_cache_root,
        selection_scope=selection_scope,
    )
    return BatchRunResult(
        selected=selected,
        manifest=manifest,
        dry_run_report_path=dry_run_report_path,
        validation_report_path=validation_report_path,
        manifest_path=manifest_path,
        output_root=output_root,
        raw_cache_root=raw_cache_root,
    )
