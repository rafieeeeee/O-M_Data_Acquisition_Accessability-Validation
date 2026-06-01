"""Materialize Baltic Copernicus wave subsets into a source-labelled archive.

The materializer preserves the native hourly product cadence. It does not
interpolate onto the 10-minute backbone and it does not create fused or
preferred-source metocean variables.
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import xarray as xr

from .extract_nws import haversine_distance


DEFAULT_RAW_ROOT = Path("Data/Raw/Metocean/CMEMS/BalticSea/Waves")
DEFAULT_OUTPUT_ROOT = Path("Data/Processed/metocean/baltic_wave_timeseries")
DEFAULT_QA_REPORT = Path(
    "analysis/06_rq6_metocean_spatial_resolution/baltic_wave_materialization_qa_report.md"
)
DEFAULT_TURBINE_COORDINATES = Path("Data/Interim/European_Turbine_Coordinates.csv")

REQUIRED_BALTIC_VARIABLES = ["VHM0", "VTPK", "VMDR", "VTM10", "VTM02"]
OPTIONAL_BALTIC_VARIABLES = ["VPED", "VMXL", "VCMX"]
BALTIC_VARIABLE_MAP = {
    "baltic_wave_hs": "VHM0",
    "baltic_wave_tp": "VTPK",
    "baltic_wave_dir": "VMDR",
    "baltic_wave_tm10": "VTM10",
    "baltic_wave_tm02": "VTM02",
}


@dataclass(frozen=True)
class BalticMaterializationResult:
    """Result tables from a Baltic materializer invocation."""

    plan: pd.DataFrame
    qa: pd.DataFrame
    qa_report: Path | None


def normalize_farm_name(value: Any) -> str:
    """Normalize farm labels across raw folders and turbine metadata."""
    asciiish = unicodedata.normalize("NFKD", str(value)).encode("ascii", "ignore")
    text = asciiish.decode("ascii").casefold()
    return re.sub(r"[^a-z0-9]+", "", text)


def farm_slug(value: Any) -> str:
    """Build a stable partition-safe farm label."""
    asciiish = unicodedata.normalize("NFKD", str(value)).encode("ascii", "ignore")
    text = asciiish.decode("ascii")
    text = re.sub(r"[^A-Za-z0-9]+", "_", text).strip("_")
    return text or "unknown_farm"


def list_raw_baltic_wave_files(raw_root: Path) -> pd.DataFrame:
    """Return one row per Baltic raw NetCDF subset file."""
    if not raw_root.exists():
        raise FileNotFoundError(f"Baltic raw root not found: {raw_root}")
    files = sorted(raw_root.glob("*/*.nc")) + sorted(raw_root.glob("*.nc"))
    rows = []
    for path in files:
        raw_farm_dir = path.parent.name if path.parent != raw_root else path.stem
        rows.append(
            {
                "raw_farm_dir": raw_farm_dir,
                "raw_farm_slug": farm_slug(raw_farm_dir),
                "normalized_farm": normalize_farm_name(raw_farm_dir),
                "raw_file": path,
                "raw_file_size_bytes": int(path.stat().st_size),
                "raw_file_modified_utc": pd.Timestamp(path.stat().st_mtime, unit="s", tz="UTC").isoformat(),
            }
        )
    return pd.DataFrame(rows)


def load_baltic_sample_points(
    turbine_coordinates_path: Path,
    raw_farm_names: list[str],
) -> dict[str, list[dict[str, Any]]]:
    """Build farm centroid plus turbine sample points for downloaded raw farms."""
    if not turbine_coordinates_path.exists():
        raise FileNotFoundError(f"Turbine coordinate file not found: {turbine_coordinates_path}")
    turbines = pd.read_csv(turbine_coordinates_path)
    required = {"wind_farm", "latitude", "longitude"}
    missing = sorted(required - set(turbines.columns))
    if missing:
        raise ValueError(f"Turbine coordinate file is missing required columns: {missing}")

    turbines = turbines.copy()
    turbines["normalized_farm"] = turbines["wind_farm"].map(normalize_farm_name)
    lookup = {
        normalized: farm_turbines.reset_index(drop=True)
        for normalized, farm_turbines in turbines.groupby("normalized_farm", dropna=True)
    }

    sample_points: dict[str, list[dict[str, Any]]] = {}
    for raw_farm_name in raw_farm_names:
        normalized = normalize_farm_name(raw_farm_name)
        farm_turbines = lookup.get(normalized)
        if farm_turbines is None or farm_turbines.empty:
            sample_points[normalized] = []
            continue

        canonical_name = str(farm_turbines["wind_farm"].iloc[0])
        points: list[dict[str, Any]] = [
            {
                "wind_farm": canonical_name,
                "sample_point_id": "farm_centroid",
                "sample_point_type": "farm_centroid",
                "lat": float(farm_turbines["latitude"].mean()),
                "lon": float(farm_turbines["longitude"].mean()),
            }
        ]
        for idx, row in farm_turbines.iterrows():
            points.append(
                {
                    "wind_farm": canonical_name,
                    "sample_point_id": f"turbine_{idx:04d}",
                    "sample_point_type": "turbine",
                    "lat": float(row["latitude"]),
                    "lon": float(row["longitude"]),
                }
            )
        sample_points[normalized] = points
    return sample_points


def _time_values(ds: xr.Dataset) -> pd.DatetimeIndex:
    times = pd.to_datetime(ds["time"].values, utc=True)
    return pd.DatetimeIndex(times)


def _dataset_metadata(path: Path) -> dict[str, Any]:
    with xr.open_dataset(path) as ds:
        variables = sorted(str(name) for name in ds.data_vars)
        missing = sorted(set(REQUIRED_BALTIC_VARIABLES) - set(variables))
        times = _time_values(ds)
        resolution_hours = None
        hourly = None
        if len(times) > 1:
            delta_hours = (times[1] - times[0]).total_seconds() / 3600.0
            resolution_hours = float(delta_hours)
            hourly = bool(abs(delta_hours - 1.0) < 1e-6)
        start = times.min() if len(times) else pd.NaT
        end = times.max() if len(times) else pd.NaT
        years = list(range(int(start.year), int(end.year) + 1)) if len(times) else []
        return {
            "variables_present": ",".join(variables),
            "missing_required_variables": ",".join(missing),
            "required_variables_present": not missing,
            "latitude_count": int(ds.sizes.get("latitude", 0)),
            "longitude_count": int(ds.sizes.get("longitude", 0)),
            "time_count": int(ds.sizes.get("time", 0)),
            "time_start_utc": start.isoformat() if len(times) else None,
            "time_end_utc": end.isoformat() if len(times) else None,
            "time_resolution_hours": resolution_hours,
            "native_hourly": hourly,
            "year_count": len(years),
            "years": ",".join(str(year) for year in years),
        }


def build_baltic_materialization_plan(
    raw_root: Path = DEFAULT_RAW_ROOT,
    turbine_coordinates_path: Path = DEFAULT_TURBINE_COORDINATES,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    limit_farms: int | None = None,
    farms: set[str] | None = None,
) -> pd.DataFrame:
    """Inspect raw NetCDFs and build a farm-level materialization plan."""
    raw_files = list_raw_baltic_wave_files(raw_root)
    if raw_files.empty:
        raise ValueError(f"No Baltic NetCDF files found under: {raw_root}")

    if farms:
        normalized_filters = {normalize_farm_name(farm) for farm in farms}
        raw_files = raw_files[raw_files["normalized_farm"].isin(normalized_filters)]
    raw_files = raw_files.sort_values("raw_farm_slug").reset_index(drop=True)
    if raw_files.empty:
        raise ValueError("No Baltic NetCDF files remain after filters.")

    sample_points = load_baltic_sample_points(
        turbine_coordinates_path=turbine_coordinates_path,
        raw_farm_names=[str(value) for value in raw_files["raw_farm_dir"].tolist()],
    )

    rows = []
    for row in raw_files.to_dict("records"):
        metadata = _dataset_metadata(Path(row["raw_file"]))
        points = sample_points.get(row["normalized_farm"], [])
        wind_farm = points[0]["wind_farm"] if points else row["raw_farm_dir"]
        slug = farm_slug(wind_farm)
        years = [int(value) for value in metadata["years"].split(",") if value]
        existing_partitions = []
        for year in years:
            part_path = output_root / f"wind_farm={slug}" / f"year={year}" / "part.parquet"
            if part_path.exists():
                existing_partitions.append(str(part_path))
        rows.append(
            {
                **row,
                **metadata,
                "wind_farm": wind_farm,
                "wind_farm_slug": slug,
                "sample_point_count": int(len(points)),
                "expected_partitions": int(len(years)),
                "expected_rows": int(metadata["time_count"] * len(points)),
                "output_partition_root": str(output_root / f"wind_farm={slug}"),
                "existing_partitions": len(existing_partitions),
                "status": (
                    "ready"
                    if metadata["required_variables_present"] and points
                    else "blocked_missing_variables_or_sample_points"
                ),
            }
        )
    plan = pd.DataFrame(rows)
    if limit_farms is not None:
        plan = (
            plan.sort_values(["expected_rows", "wind_farm_slug"])
            .head(int(limit_farms))
            .reset_index(drop=True)
        )
    return plan


def _find_nearest_valid_grid(
    ds: xr.Dataset,
    lat: float,
    lon: float,
    max_spatial_distance_km: float,
    valid_variable: str = "VHM0",
) -> tuple[float, float, float, int, int, str]:
    lat_values = ds["latitude"].values
    lon_values = ds["longitude"].values
    nearest_lat_idx = int(np.abs(lat_values - lat).argmin())
    nearest_lon_idx = int(np.abs(lon_values - lon).argmin())

    lat_radius_deg = max_spatial_distance_km / 111.0 + 0.05
    lon_radius_deg = max_spatial_distance_km / max(111.0 * np.cos(np.radians(lat)), 1.0) + 0.05
    lat_candidates = np.where(np.abs(lat_values - lat) <= lat_radius_deg)[0]
    lon_candidates = np.where(np.abs(lon_values - lon) <= lon_radius_deg)[0]

    if valid_variable not in ds or len(lat_candidates) == 0 or len(lon_candidates) == 0:
        grid_lat = float(lat_values[nearest_lat_idx])
        grid_lon = float(lon_values[nearest_lon_idx])
        distance = float(haversine_distance(lat, lon, grid_lat, grid_lon))
        return grid_lat, grid_lon, distance, nearest_lat_idx, nearest_lon_idx, "nearest_unchecked"

    sample = ds[valid_variable].isel(
        time=slice(0, min(8, ds.sizes["time"])),
        latitude=lat_candidates,
        longitude=lon_candidates,
    )
    valid_mask = np.isfinite(sample.values).any(axis=0)
    if not valid_mask.any():
        grid_lat = float(lat_values[nearest_lat_idx])
        grid_lon = float(lon_values[nearest_lon_idx])
        distance = float(haversine_distance(lat, lon, grid_lat, grid_lon))
        return grid_lat, grid_lon, distance, nearest_lat_idx, nearest_lon_idx, "no_valid_grid_within_search"

    lat_grid, lon_grid = np.meshgrid(
        lat_values[lat_candidates],
        lon_values[lon_candidates],
        indexing="ij",
    )
    distances = haversine_distance(lat, lon, lat_grid, lon_grid)
    distances = np.where(valid_mask, distances, np.inf)
    local_flat_idx = int(np.nanargmin(distances))
    local_lat_idx, local_lon_idx = np.unravel_index(local_flat_idx, distances.shape)
    lat_idx = int(lat_candidates[local_lat_idx])
    lon_idx = int(lon_candidates[local_lon_idx])
    grid_lat = float(lat_values[lat_idx])
    grid_lon = float(lon_values[lon_idx])
    distance = float(distances[local_lat_idx, local_lon_idx])
    if distance > max_spatial_distance_km:
        return grid_lat, grid_lon, distance, lat_idx, lon_idx, "valid_grid_too_far"
    return grid_lat, grid_lon, distance, lat_idx, lon_idx, "nearest_valid_grid"


def _extract_point_year_timeseries(
    ds_year: xr.Dataset,
    times: pd.DatetimeIndex,
    point: dict[str, Any],
    source_file: Path,
    max_spatial_distance_km: float,
) -> pd.DataFrame:
    grid_lat, grid_lon, distance, lat_idx, lon_idx, grid_method = _find_nearest_valid_grid(
        ds=ds_year,
        lat=float(point["lat"]),
        lon=float(point["lon"]),
        max_spatial_distance_km=max_spatial_distance_km,
    )
    frame = pd.DataFrame(
        {
            "timestamp_utc": times,
            "wind_farm": point["wind_farm"],
            "sample_point_id": point["sample_point_id"],
            "sample_point_type": point["sample_point_type"],
            "lat": float(point["lat"]),
            "lon": float(point["lon"]),
            "baltic_grid_lat": grid_lat,
            "baltic_grid_lon": grid_lon,
            "baltic_spatial_distance_km": distance,
            "baltic_source_file": source_file.name,
            "baltic_extraction_method": f"{grid_method}_hourly_wave_only",
        }
    )

    if distance > max_spatial_distance_km:
        for column in BALTIC_VARIABLE_MAP:
            frame[column] = np.nan
        frame["baltic_spatial_match_status"] = "out_of_bounds_or_too_far"
        return frame

    point_ds = ds_year.isel(latitude=lat_idx, longitude=lon_idx)
    for column, variable in BALTIC_VARIABLE_MAP.items():
        if variable in point_ds:
            values = np.asarray(point_ds[variable].values, dtype="float64")
            if column == "baltic_wave_dir":
                values = np.mod(values, 360.0)
            frame[column] = values
        else:
            frame[column] = np.nan
    frame["baltic_spatial_match_status"] = "ok"
    return frame


def _validate_materialized_frame(frame: pd.DataFrame) -> dict[str, bool | None]:
    if frame.empty:
        return {
            "hs_non_negative": None,
            "tp_positive": None,
            "tm10_positive": None,
            "tm02_positive": None,
            "direction_0_360": None,
            "no_duplicate_sample_times": None,
        }
    checks: dict[str, bool | None] = {}
    hs = frame["baltic_wave_hs"].dropna() if "baltic_wave_hs" in frame else pd.Series(dtype=float)
    tp = frame["baltic_wave_tp"].dropna() if "baltic_wave_tp" in frame else pd.Series(dtype=float)
    tm10 = frame["baltic_wave_tm10"].dropna() if "baltic_wave_tm10" in frame else pd.Series(dtype=float)
    tm02 = frame["baltic_wave_tm02"].dropna() if "baltic_wave_tm02" in frame else pd.Series(dtype=float)
    direction = frame["baltic_wave_dir"].dropna() if "baltic_wave_dir" in frame else pd.Series(dtype=float)
    checks["hs_non_negative"] = bool((hs >= 0).all()) if not hs.empty else None
    checks["tp_positive"] = bool((tp > 0).all()) if not tp.empty else None
    checks["tm10_positive"] = bool((tm10 > 0).all()) if not tm10.empty else None
    checks["tm02_positive"] = bool((tm02 > 0).all()) if not tm02.empty else None
    checks["direction_0_360"] = (
        bool(((direction >= 0) & (direction < 360)).all()) if not direction.empty else None
    )
    duplicate_keys = frame.duplicated(
        subset=["wind_farm", "sample_point_id", "timestamp_utc"],
        keep=False,
    )
    checks["no_duplicate_sample_times"] = bool(not duplicate_keys.any())
    return checks


def materialize_baltic_wave_timeseries(
    raw_root: Path = DEFAULT_RAW_ROOT,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    turbine_coordinates_path: Path = DEFAULT_TURBINE_COORDINATES,
    qa_report: Path | None = DEFAULT_QA_REPORT,
    dry_run: bool = False,
    limit_farms: int | None = None,
    farms: set[str] | None = None,
    overwrite: bool = False,
    max_spatial_distance_km: float = 25.0,
) -> BalticMaterializationResult:
    """Materialize or dry-run the Baltic wave archive."""
    plan = build_baltic_materialization_plan(
        raw_root=raw_root,
        turbine_coordinates_path=turbine_coordinates_path,
        output_root=output_root,
        limit_farms=limit_farms,
        farms=farms,
    )

    if dry_run:
        qa = plan.assign(
            materialization_status=plan["status"].map(
                lambda status: "dry_run_planned" if status == "ready" else "dry_run_blocked"
            ),
            rows_written=0,
        )
        report_path = write_baltic_materialization_qa_report(
            plan=plan,
            qa=qa,
            output_root=output_root,
            qa_report=qa_report,
            dry_run=True,
        )
        return BalticMaterializationResult(plan=plan, qa=qa, qa_report=report_path)

    output_root.mkdir(parents=True, exist_ok=True)
    sample_points_by_farm = load_baltic_sample_points(
        turbine_coordinates_path=turbine_coordinates_path,
        raw_farm_names=[str(value) for value in plan["raw_farm_dir"].tolist()],
    )
    qa_rows: list[dict[str, Any]] = []
    ready_plan = plan[plan["status"] == "ready"].copy()
    if ready_plan.empty:
        raise ValueError("Baltic materialization plan has no ready farms.")

    for farm_row in ready_plan.to_dict("records"):
        raw_file = Path(farm_row["raw_file"])
        points = sample_points_by_farm.get(farm_row["normalized_farm"], [])
        if not points:
            qa_rows.append(
                {
                    "wind_farm": farm_row["wind_farm"],
                    "wind_farm_slug": farm_row["wind_farm_slug"],
                    "year": None,
                    "materialization_status": "missing_sample_points",
                    "rows_written": 0,
                    "output_path": None,
                }
            )
            continue

        with xr.open_dataset(raw_file) as ds:
            all_times = _time_values(ds)
            years = sorted(set(int(year) for year in all_times.year))
            for year in years:
                partition_dir = output_root / f"wind_farm={farm_row['wind_farm_slug']}" / f"year={year}"
                part_path = partition_dir / "part.parquet"
                if part_path.exists() and not overwrite:
                    qa_rows.append(
                        {
                            "wind_farm": farm_row["wind_farm"],
                            "wind_farm_slug": farm_row["wind_farm_slug"],
                            "year": year,
                            "materialization_status": "existing_partition_skipped",
                            "rows_written": 0,
                            "sample_points": len(points),
                            "output_path": str(part_path),
                        }
                    )
                    continue

                mask = all_times.year == year
                time_indices = np.flatnonzero(mask)
                ds_year = ds.isel(time=time_indices)
                year_times = pd.DatetimeIndex(all_times[mask])
                frames = [
                    _extract_point_year_timeseries(
                        ds_year=ds_year,
                        times=year_times,
                        point=point,
                        source_file=raw_file,
                        max_spatial_distance_km=max_spatial_distance_km,
                    )
                    for point in points
                ]
                out = pd.concat(frames, ignore_index=True)
                checks = _validate_materialized_frame(out)
                partition_dir.mkdir(parents=True, exist_ok=True)
                out.to_parquet(part_path, index=False)
                qa_rows.append(
                    {
                        "wind_farm": farm_row["wind_farm"],
                        "wind_farm_slug": farm_row["wind_farm_slug"],
                        "year": year,
                        "materialization_status": "ok",
                        "rows_written": int(len(out)),
                        "sample_points": len(points),
                        "timestamps_per_point": int(out["timestamp_utc"].nunique()),
                        "output_path": str(part_path),
                        **checks,
                    }
                )

    qa = pd.DataFrame(qa_rows)
    report_path = write_baltic_materialization_qa_report(
        plan=plan,
        qa=qa,
        output_root=output_root,
        qa_report=qa_report,
        dry_run=False,
    )
    return BalticMaterializationResult(plan=plan, qa=qa, qa_report=report_path)


def write_baltic_materialization_qa_report(
    plan: pd.DataFrame,
    qa: pd.DataFrame,
    output_root: Path,
    qa_report: Path | None = DEFAULT_QA_REPORT,
    dry_run: bool = False,
) -> Path | None:
    """Write a human-readable QA report for dry-run or materialization."""
    if qa_report is None:
        return None
    qa_report.parent.mkdir(parents=True, exist_ok=True)

    ready = plan[plan["status"] == "ready"] if not plan.empty else plan
    blocked = plan[plan["status"] != "ready"] if not plan.empty else plan
    rows_written = int(qa["rows_written"].sum()) if "rows_written" in qa.columns else 0
    lines = [
        "# Baltic Wave Materialization QA Report",
        "",
        "This report covers the Baltic Copernicus wave archive materializer. "
        "It preserves native hourly data and does not create 10-minute interpolated, fused, current, or final workability variables.",
        "",
        "## Run Mode",
        "",
        f"- Dry run: {dry_run}",
        f"- Output root: `{output_root}`",
        f"- Farms in plan: {len(plan)}",
        f"- Ready farms: {len(ready)}",
        f"- Blocked farms: {len(blocked)}",
        f"- Expected partitions: {int(ready['expected_partitions'].sum()) if not ready.empty else 0}",
        f"- Expected rows if materialized: {int(ready['expected_rows'].sum()) if not ready.empty else 0}",
        f"- Rows written in this run: {rows_written}",
        "",
        "## Variable Contract",
        "",
        "- `VHM0` -> `baltic_wave_hs`",
        "- `VTPK` -> `baltic_wave_tp`",
        "- `VMDR` -> `baltic_wave_dir`",
        "- `VTM10` -> `baltic_wave_tm10`",
        "- `VTM02` -> `baltic_wave_tm02`",
        "- `VSDX`/`VSDY` are not used here because they are Stokes drift, not Eulerian currents.",
        "",
        "## Farm Plan",
        "",
    ]
    if plan.empty:
        lines.append("No farms were planned.")
    else:
        for row in plan.to_dict("records"):
            lines.append(
                f"- **{row['wind_farm']}** (`{row['raw_farm_dir']}`): "
                f"{row['time_start_utc']} to {row['time_end_utc']}; "
                f"{row['time_count']} hourly timestamps; "
                f"{row['sample_point_count']} sample points; "
                f"{row['expected_partitions']} expected partitions; "
                f"{row['expected_rows']} expected rows; status `{row['status']}`"
            )

    lines.extend(["", "## QA Rows", ""])
    if qa.empty:
        lines.append("No QA rows were produced.")
    else:
        status_col = "materialization_status"
        status_counts = qa[status_col].value_counts(dropna=False).to_dict() if status_col in qa else {}
        lines.append(f"- Status counts: `{status_counts}`")
        if not dry_run:
            for row in qa.to_dict("records"):
                lines.append(
                    f"- **{row.get('wind_farm')} {row.get('year')}**: "
                    f"{row.get('materialization_status')}; rows={row.get('rows_written')}; "
                    f"path=`{row.get('output_path')}`"
                )

    lines.extend(
        [
            "",
            "## Guardrails",
            "",
            "- No current downloads were run.",
            "- No NORA3 extraction or consolidation was run.",
            "- No final dwell-metocean feature table was rebuilt.",
            "- No source fusion or preferred-source variables were created.",
            "- Native hourly cadence is preserved for later source-agnostic assignment.",
        ]
    )
    qa_report.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return qa_report
