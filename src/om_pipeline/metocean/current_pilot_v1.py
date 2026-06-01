"""Current Pilot v1: true u/v current candidates and validation.

This module is intentionally scoped to one-farm/year pilots. It does not build
a final current archive, does not rebuild dwell-metocean features, and never
uses the legacy CMEMS fallback CSV/cache path because that path may contain
simulated tidal climatology rather than true Eulerian `uo`/`vo` evidence.
"""

from __future__ import annotations

import math
import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from .extract_nws import haversine_distance


DEFAULT_DWELL_WEATHER_INPUT = Path(
    "Data/Processed/ais_dwell_backfill/cross_farm_dwell_weather_features.parquet"
)
DEFAULT_BALTIC_WAVE_ROOT = Path("Data/Processed/metocean/baltic_wave_timeseries")
DEFAULT_NWS_WAVE_ROOT = Path("Data/Processed/metocean/nws_wave_timeseries")
DEFAULT_OUTPUT_DIR = Path("Data/Processed/metocean/current_pilots")
DEFAULT_REPORT_DIR = Path("reports/current_pilot_v1")
DEFAULT_NWS_INVENTORY = Path("analysis/06_rq6_metocean_spatial_resolution/nws_file_inventory.csv")

BALTIC_CURRENT_CANDIDATES_FILENAME = "baltic_current_candidates.parquet"
NWS_CURRENT_CANDIDATES_FILENAME = "nws_current_candidates.parquet"
PRODUCT_ASSESSMENT_FILENAME = "current_product_assessment.md"
VALIDATION_REPORT_FILENAME = "current_pilot_validation_report.md"

CURRENT_CANDIDATE_COLUMNS = [
    "pilot_id",
    "wind_farm",
    "farm_id",
    "year",
    "sample_point_id",
    "sample_point_type",
    "lat",
    "lon",
    "timestamp_utc",
    "current_u",
    "current_v",
    "current_speed",
    "current_direction",
    "current_depth_m",
    "current_source",
    "current_product_id",
    "current_dataset_id",
    "current_native_temporal_resolution_minutes",
    "current_native_spatial_resolution_km",
    "current_grid_lat",
    "current_grid_lon",
    "current_spatial_distance_km",
    "current_assignment_method",
    "current_spatial_match_status",
    "source_file",
    "provenance_status",
]

BLOCKED_PROVENANCE_PREFIXES = (
    "blocked",
    "dry_run",
    "missing",
    "fallback",
    "simulated",
    "legacy",
)


@dataclass(frozen=True)
class CurrentProductConfig:
    pilot: str
    current_source: str
    product_name: str
    product_id: str
    dataset_id: str
    variables: tuple[str, str]
    native_temporal_resolution_minutes: int
    native_spatial_resolution_km: float
    temporal_coverage: str
    spatial_domain: str
    depth_description: str
    units: str
    access_method: str
    tooling_requirement: str
    storage_estimate: str
    event_scale_suitability: str
    service_url: str
    preferred_wave_root: Path
    output_filename: str
    surface_depth_max_m: float | None


@dataclass(frozen=True)
class CurrentPilotResult:
    pilot: str
    candidate_path: Path
    product_assessment_path: Path
    validation_report_path: Path
    candidates: pd.DataFrame
    validation: dict[str, Any]


BALTIC_PRODUCT = CurrentProductConfig(
    pilot="baltic",
    current_source="Baltic",
    product_name="Copernicus Baltic Sea Physics Reanalysis",
    product_id="BALTICSEA_MULTIYEAR_PHY_003_011",
    dataset_id="cmems_mod_bal_phy_my_P1D-m",
    variables=("uo", "vo"),
    native_temporal_resolution_minutes=1440,
    native_spatial_resolution_km=2.0,
    temporal_coverage="1993-2024 daily/monthly/yearly reanalysis datasets",
    spatial_domain="Baltic Sea, approximately 53.01-65.89N and 9.04-30.21E",
    depth_description="3D physics grid with 56 depth levels; pilot uses nearest-surface uo/vo only",
    units="m s-1 for eastward and northward sea-water velocity",
    access_method="Copernicus Marine Toolbox subset/open_dataset",
    tooling_requirement="copernicusmarine credentials plus xarray/netCDF support",
    storage_estimate="One-farm/year nearest-surface subset is small; candidate parquet usually tens of thousands of rows",
    event_scale_suitability=(
        "Contextual before validation: true u/v is available, but the approved "
        "multi-year reanalysis current dataset is daily, not event-hourly."
    ),
    service_url="https://data.marine.copernicus.eu/product/BALTICSEA_MULTIYEAR_PHY_003_011/services",
    preferred_wave_root=DEFAULT_BALTIC_WAVE_ROOT,
    output_filename=BALTIC_CURRENT_CANDIDATES_FILENAME,
    surface_depth_max_m=5.0,
)

NWS_PRODUCT = CurrentProductConfig(
    pilot="nws",
    current_source="NWS",
    product_name="Copernicus Atlantic-European North West Shelf Ocean Physics Reanalysis",
    product_id="NWSHELF_MULTIYEAR_PHY_004_009",
    dataset_id="cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i",
    variables=("uo", "vo"),
    native_temporal_resolution_minutes=60,
    native_spatial_resolution_km=7.0,
    temporal_coverage="1993-2026 hourly 2D surface currents in the current dataset",
    spatial_domain="North West Shelf, approximately 40.07-65N and -19.89-13E",
    depth_description="2D surface current product for uo/vo; no depth dimension expected",
    units="m s-1 for eastward and northward sea-water velocity",
    access_method="Local annual NetCDF if mounted, otherwise Copernicus Marine Toolbox scoped subset",
    tooling_requirement="local raw NetCDF or copernicusmarine credentials plus xarray/netCDF support",
    storage_estimate="One-farm/year nearest-surface subset is moderate; hourly candidates may be hundreds of thousands of rows",
    event_scale_suitability=(
        "Potentially event-scale where the farm lies in product coverage and hourly "
        "samples bracket dwell windows."
    ),
    service_url="https://data.marine.copernicus.eu/product/NWSHELF_MULTIYEAR_PHY_004_009/services",
    preferred_wave_root=DEFAULT_NWS_WAVE_ROOT,
    output_filename=NWS_CURRENT_CANDIDATES_FILENAME,
    surface_depth_max_m=None,
)

GLOBAL_FALLBACK_ASSESSMENT = {
    "product_name": "Copernicus Global Ocean Physics Reanalysis",
    "product_id": "GLOBAL_MULTIYEAR_PHY_001_030",
    "dataset_id": "cmems_mod_glo_phy_my_0.083deg_P1D-m",
    "variables": "uo, vo",
    "temporal_resolution": "Daily/monthly, depending on dataset part",
    "temporal_coverage": "1993-2026 reanalysis family",
    "spatial_resolution": "1/12 degree global grid",
    "spatial_domain": "Global ocean",
    "depth_levels": "3D, 50 depth levels in the product family",
    "access_method": "Copernicus Marine Toolbox",
    "event_scale_suitability": (
        "Fallback assessment only. Daily/coarser regional fit means it should not "
        "be downloaded before Baltic/NWS regional gaps are proven."
    ),
    "service_url": "https://data.marine.copernicus.eu/product/GLOBAL_MULTIYEAR_PHY_001_030/services",
}

PRODUCTS = {
    "baltic": BALTIC_PRODUCT,
    "nws": NWS_PRODUCT,
}


def farm_slug(value: Any) -> str:
    """Build the same partition-safe farm label used by regional wave archives."""
    asciiish = unicodedata.normalize("NFKD", str(value)).encode("ascii", "ignore")
    text = asciiish.decode("ascii")
    text = re.sub(r"[^A-Za-z0-9]+", "_", text).strip("_")
    return text or "unknown_farm"


def normalize_farm_name(value: Any) -> str:
    """Normalize farm labels for comparing display names, ids, and slugs."""
    asciiish = unicodedata.normalize("NFKD", str(value)).encode("ascii", "ignore")
    text = asciiish.decode("ascii").casefold()
    return re.sub(r"[^a-z0-9]+", "", text)


def derive_current_speed_direction(current_u: pd.Series, current_v: pd.Series) -> tuple[pd.Series, pd.Series]:
    """Return speed and flow-to direction degrees clockwise from true north."""
    speed = np.sqrt(current_u.astype(float) ** 2 + current_v.astype(float) ** 2)
    direction = (np.degrees(np.arctan2(current_u.astype(float), current_v.astype(float))) + 360.0) % 360.0
    return pd.Series(speed, index=current_u.index), pd.Series(direction, index=current_u.index)


def candidate_output_path(output_dir: Path, pilot: str) -> Path:
    return output_dir / PRODUCTS[pilot].output_filename


def ensure_candidate_schema(df: pd.DataFrame) -> pd.DataFrame:
    """Return candidate rows with the required columns in required order."""
    for col in CURRENT_CANDIDATE_COLUMNS:
        if col not in df.columns:
            df[col] = pd.NA
    return df[CURRENT_CANDIDATE_COLUMNS].copy()


def validate_real_current_candidates(candidates: pd.DataFrame) -> None:
    """Reject fallback/simulated/provenance-free rows before accepting output."""
    if candidates.empty:
        return
    provenance = candidates["provenance_status"].fillna("").astype(str).str.casefold()
    bad = provenance.str.startswith(BLOCKED_PROVENANCE_PREFIXES) & (
        candidates["current_u"].notna() | candidates["current_v"].notna()
    )
    if bad.any():
        examples = candidates.loc[bad, "provenance_status"].dropna().astype(str).unique()[:5]
        raise ValueError(
            "Current candidate rows with blocked/fallback/simulated provenance cannot "
            f"carry current values: {', '.join(examples)}"
        )


def write_candidate_table(candidates: pd.DataFrame, path: Path, overwrite: bool = False) -> None:
    if path.exists() and not overwrite:
        raise FileExistsError(f"Current pilot output already exists: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    candidates = ensure_candidate_schema(candidates)
    validate_real_current_candidates(candidates)
    candidates.to_parquet(path, index=False)


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


def _build_wave_partition_index(root: Path) -> dict[tuple[str, int], Path]:
    if not root.exists():
        return {}
    index: dict[tuple[str, int], Path] = {}
    for path in sorted(root.rglob("*.parquet")):
        rel_parts = path.relative_to(root).parts
        if any(part.startswith("_") for part in rel_parts):
            continue
        farm = _farm_slug_from_path(path)
        year = _year_from_path(path)
        if farm is not None and year is not None:
            index[(farm, year)] = path
    return index


def resolve_wave_partition(wave_root: Path, farm: str, year: int) -> Path | None:
    index = _build_wave_partition_index(wave_root)
    if not index:
        return None
    direct = index.get((farm_slug(farm), year))
    if direct is not None:
        return direct
    normalized = normalize_farm_name(farm)
    for (candidate_farm, candidate_year), path in index.items():
        if candidate_year == year and normalize_farm_name(candidate_farm) == normalized:
            return path
    return None


def load_sample_points_from_wave_archive(wave_root: Path, farm: str, year: int) -> pd.DataFrame:
    partition = resolve_wave_partition(wave_root, farm, year)
    if partition is None:
        raise FileNotFoundError(f"No accepted wave archive partition found for {farm} {year} under {wave_root}")
    columns = ["wind_farm", "sample_point_id", "sample_point_type", "lat", "lon"]
    source = pd.read_parquet(partition, columns=columns)
    points = (
        source[columns]
        .drop_duplicates(subset=["sample_point_id", "sample_point_type", "lat", "lon"])
        .sort_values(["sample_point_type", "sample_point_id"])
        .reset_index(drop=True)
    )
    points["wind_farm"] = points["wind_farm"].astype(str)
    return points


def load_dwell_events_for_farm_year(dwell_weather: Path, farm: str, year: int) -> pd.DataFrame:
    columns = [
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
    if not dwell_weather.exists():
        return pd.DataFrame(columns=columns)
    dwell = pd.read_parquet(dwell_weather, columns=[c for c in columns if c in pq.read_schema(dwell_weather).names])
    for col in columns:
        if col not in dwell.columns:
            dwell[col] = pd.NA
    dwell["start_utc"] = pd.to_datetime(dwell["start_utc"], utc=True, errors="coerce")
    dwell["end_utc"] = pd.to_datetime(dwell["end_utc"], utc=True, errors="coerce")
    normalized = normalize_farm_name(farm)
    mask = (
        dwell["start_utc"].dt.year.eq(year)
        & (
            dwell["wind_farm"].map(normalize_farm_name).eq(normalized)
            | dwell["farm_id"].map(normalize_farm_name).eq(normalized)
        )
    )
    return dwell.loc[mask, columns].reset_index(drop=True)


def _source_bbox(sample_points: pd.DataFrame, pad_degrees: float = 0.03) -> dict[str, float]:
    return {
        "minimum_longitude": float(sample_points["lon"].min() - pad_degrees),
        "maximum_longitude": float(sample_points["lon"].max() + pad_degrees),
        "minimum_latitude": float(sample_points["lat"].min() - pad_degrees),
        "maximum_latitude": float(sample_points["lat"].max() + pad_degrees),
    }


def _find_var_name(ds: Any, names: tuple[str, ...] | list[str]) -> str | None:
    available = set(ds.variables) | set(ds.coords)
    for name in names:
        if name in available:
            return name
    lowered = {str(name).casefold(): str(name) for name in available}
    for name in names:
        found = lowered.get(str(name).casefold())
        if found:
            return found
    return None


def _find_coord_name(ds: Any, candidates: tuple[str, ...]) -> str | None:
    return _find_var_name(ds, candidates)


def _nearest_1d_index(values: np.ndarray, target: float) -> int:
    return int(np.nanargmin(np.abs(values.astype(float) - float(target))))


def _select_spatial_point(ds: Any, lat: float, lon: float) -> tuple[Any, float, float, float, str]:
    lat_name = _find_coord_name(ds, ("latitude", "lat", "nav_lat"))
    lon_name = _find_coord_name(ds, ("longitude", "lon", "nav_lon"))
    if lat_name is None or lon_name is None:
        raise ValueError("Dataset has no recognizable latitude/longitude coordinates")

    lat_values = np.asarray(ds[lat_name].values)
    lon_values = np.asarray(ds[lon_name].values)
    if lat_values.ndim == 1 and lon_values.ndim == 1:
        lat_idx = _nearest_1d_index(lat_values, lat)
        lon_idx = _nearest_1d_index(lon_values, lon)
        lat_dim = ds[lat_name].dims[0]
        lon_dim = ds[lon_name].dims[0]
        selected = ds.isel({lat_dim: lat_idx, lon_dim: lon_idx})
        grid_lat = float(lat_values[lat_idx])
        grid_lon = float(lon_values[lon_idx])
        distance = haversine_distance(float(lat), float(lon), grid_lat, grid_lon)
        return selected, grid_lat, grid_lon, float(distance), "nearest_1d_grid"

    if lat_values.shape != lon_values.shape:
        raise ValueError("Curvilinear latitude and longitude coordinate shapes do not match")
    dist = (lat_values.astype(float) - float(lat)) ** 2 + (lon_values.astype(float) - float(lon)) ** 2
    idx = np.unravel_index(int(np.nanargmin(dist)), dist.shape)
    indexers = {dim: pos for dim, pos in zip(ds[lat_name].dims, idx)}
    selected = ds.isel(indexers)
    grid_lat = float(np.asarray(selected[lat_name].values))
    grid_lon = float(np.asarray(selected[lon_name].values))
    distance = haversine_distance(float(lat), float(lon), grid_lat, grid_lon)
    return selected, grid_lat, grid_lon, float(distance), "nearest_curvilinear_grid"


def _select_surface_depth(point_ds: Any, u_var: str, v_var: str) -> tuple[Any, float]:
    variable_dims = set(point_ds[u_var].dims) | set(point_ds[v_var].dims)
    depth_name = None
    for candidate in ("depth", "depthu", "depthv", "deptht", "lev", "elevation"):
        if candidate in point_ds.coords and candidate in variable_dims:
            depth_name = candidate
            break
        if candidate in point_ds.dims and candidate in variable_dims:
            depth_name = candidate
            break
    if depth_name is None:
        return point_ds, 0.0
    values = np.asarray(point_ds[depth_name].values, dtype=float)
    if values.ndim != 1 or len(values) == 0:
        return point_ds, float("nan")
    valid = values[np.isfinite(values)]
    if len(valid) == 0:
        return point_ds, float("nan")
    depth_value = float(valid[np.nanargmin(np.abs(valid))])
    selected = point_ds.sel({depth_name: depth_value}, method="nearest")
    actual = float(np.asarray(selected[depth_name].values))
    return selected, actual


def _candidate_rows_from_point_dataset(
    point_ds: Any,
    sample_point: pd.Series,
    config: CurrentProductConfig,
    year: int,
    farm_id: str,
    pilot_id: str,
    source_file: str,
    grid_lat: float,
    grid_lon: float,
    distance_km: float,
    assignment_method: str,
) -> pd.DataFrame:
    u_var = _find_var_name(point_ds, ("uo", "u", "eastward_sea_water_velocity"))
    v_var = _find_var_name(point_ds, ("vo", "v", "northward_sea_water_velocity"))
    time_name = _find_coord_name(point_ds, ("time", "time_counter", "valid_time"))
    if u_var is None or v_var is None:
        raise ValueError("Dataset does not contain true uo/vo current variables")
    if time_name is None:
        raise ValueError("Dataset has no recognizable time coordinate")

    point_ds, depth_m = _select_surface_depth(point_ds, u_var, v_var)
    frame = point_ds[[u_var, v_var]].to_dataframe().reset_index()
    frame = frame.rename(columns={time_name: "timestamp_utc", u_var: "current_u", v_var: "current_v"})
    frame["timestamp_utc"] = pd.to_datetime(frame["timestamp_utc"], utc=True, errors="coerce")
    frame = frame.dropna(subset=["timestamp_utc"]).copy()
    frame = frame[frame["timestamp_utc"].dt.year.eq(int(year))].copy()

    # Drop non-time coordinate columns after preserving u/v; these vary by product.
    frame = frame[["timestamp_utc", "current_u", "current_v"]].copy()
    frame["current_u"] = pd.to_numeric(frame["current_u"], errors="coerce")
    frame["current_v"] = pd.to_numeric(frame["current_v"], errors="coerce")
    speed, direction = derive_current_speed_direction(frame["current_u"], frame["current_v"])
    frame["current_speed"] = speed
    frame["current_direction"] = direction

    frame["pilot_id"] = pilot_id
    frame["wind_farm"] = str(sample_point["wind_farm"])
    frame["farm_id"] = farm_id
    frame["year"] = int(year)
    frame["sample_point_id"] = sample_point["sample_point_id"]
    frame["sample_point_type"] = sample_point["sample_point_type"]
    frame["lat"] = float(sample_point["lat"])
    frame["lon"] = float(sample_point["lon"])
    frame["current_depth_m"] = depth_m
    frame["current_source"] = config.current_source
    frame["current_product_id"] = config.product_id
    frame["current_dataset_id"] = config.dataset_id
    frame["current_native_temporal_resolution_minutes"] = config.native_temporal_resolution_minutes
    frame["current_native_spatial_resolution_km"] = config.native_spatial_resolution_km
    frame["current_grid_lat"] = grid_lat
    frame["current_grid_lon"] = grid_lon
    frame["current_spatial_distance_km"] = distance_km
    frame["current_assignment_method"] = assignment_method
    frame["current_spatial_match_status"] = "ok"
    frame["source_file"] = source_file
    frame["provenance_status"] = "real_uo_vo"
    return ensure_candidate_schema(frame)


def extract_candidates_from_xarray(
    ds: Any,
    sample_points: pd.DataFrame,
    config: CurrentProductConfig,
    farm: str,
    year: int,
    source_file: str,
) -> pd.DataFrame:
    rows: list[pd.DataFrame] = []
    pilot_id = f"{config.pilot}_{farm_slug(farm)}_{int(year)}"
    farm_id = str(sample_points["wind_farm"].iloc[0]) if not sample_points.empty else farm
    for _, sample_point in sample_points.iterrows():
        point_ds, grid_lat, grid_lon, distance_km, assignment_method = _select_spatial_point(
            ds,
            float(sample_point["lat"]),
            float(sample_point["lon"]),
        )
        rows.append(
            _candidate_rows_from_point_dataset(
                point_ds=point_ds,
                sample_point=sample_point,
                config=config,
                year=year,
                farm_id=farm_id,
                pilot_id=pilot_id,
                source_file=source_file,
                grid_lat=grid_lat,
                grid_lon=grid_lon,
                distance_km=distance_km,
                assignment_method=assignment_method,
            )
        )
    if not rows:
        return ensure_candidate_schema(pd.DataFrame())
    candidates = pd.concat(rows, ignore_index=True)
    candidates = candidates.sort_values(["sample_point_id", "timestamp_utc"]).reset_index(drop=True)
    return ensure_candidate_schema(candidates)


def blocked_candidate_rows(
    sample_points: pd.DataFrame,
    config: CurrentProductConfig,
    farm: str,
    year: int,
    reason: str,
) -> pd.DataFrame:
    pilot_id = f"{config.pilot}_{farm_slug(farm)}_{int(year)}"
    farm_id = str(sample_points["wind_farm"].iloc[0]) if not sample_points.empty else farm
    rows = []
    if sample_points.empty:
        sample_points = pd.DataFrame(
            [
                {
                    "wind_farm": farm,
                    "sample_point_id": "farm_centroid",
                    "sample_point_type": "farm_centroid",
                    "lat": pd.NA,
                    "lon": pd.NA,
                }
            ]
        )
    for _, point in sample_points.iterrows():
        rows.append(
            {
                "pilot_id": pilot_id,
                "wind_farm": point.get("wind_farm", farm),
                "farm_id": farm_id,
                "year": int(year),
                "sample_point_id": point.get("sample_point_id", "unknown"),
                "sample_point_type": point.get("sample_point_type", "unknown"),
                "lat": point.get("lat", pd.NA),
                "lon": point.get("lon", pd.NA),
                "timestamp_utc": pd.NaT,
                "current_u": pd.NA,
                "current_v": pd.NA,
                "current_speed": pd.NA,
                "current_direction": pd.NA,
                "current_depth_m": pd.NA,
                "current_source": config.current_source,
                "current_product_id": config.product_id,
                "current_dataset_id": config.dataset_id,
                "current_native_temporal_resolution_minutes": config.native_temporal_resolution_minutes,
                "current_native_spatial_resolution_km": config.native_spatial_resolution_km,
                "current_grid_lat": pd.NA,
                "current_grid_lon": pd.NA,
                "current_spatial_distance_km": pd.NA,
                "current_assignment_method": "not_assigned",
                "current_spatial_match_status": "blocked",
                "source_file": pd.NA,
                "provenance_status": f"blocked: {reason}",
            }
        )
    return ensure_candidate_schema(pd.DataFrame(rows))


def _open_local_netcdf(path: Path) -> Any:
    import xarray as xr

    return xr.open_dataset(path)


def find_local_nws_current_file(year: int, inventory_path: Path = DEFAULT_NWS_INVENTORY) -> Path | None:
    if not inventory_path.exists():
        return None
    inventory = pd.read_csv(inventory_path)
    if "product_type" in inventory.columns:
        inventory = inventory[inventory["product_type"].astype(str).str.casefold().eq("currents")]
    candidates = inventory[inventory["year"].astype(str).eq(str(int(year)))]
    for value in candidates.get("filepath", pd.Series(dtype=str)).dropna():
        path = Path(str(value))
        if path.exists():
            return path
    return None


def open_copernicus_dataset(config: CurrentProductConfig, sample_points: pd.DataFrame, year: int) -> Any:
    import copernicusmarine

    bbox = _source_bbox(sample_points)
    kwargs: dict[str, Any] = {
        "dataset_id": config.dataset_id,
        "variables": list(config.variables),
        "start_datetime": f"{int(year)}-01-01T00:00:00",
        "end_datetime": f"{int(year)}-12-31T23:59:59",
        "coordinates_selection_method": "outside",
        **bbox,
    }
    if config.surface_depth_max_m is not None:
        kwargs["minimum_depth"] = 0.0
        kwargs["maximum_depth"] = config.surface_depth_max_m
    return copernicusmarine.open_dataset(**kwargs)


def load_or_fetch_current_dataset(
    config: CurrentProductConfig,
    sample_points: pd.DataFrame,
    year: int,
) -> tuple[Any | None, str | None, str | None]:
    """Return dataset, source file label, and blocker reason."""
    if config.pilot == "nws":
        local = find_local_nws_current_file(year)
        if local is not None:
            return _open_local_netcdf(local), str(local), None
    try:
        ds = open_copernicus_dataset(config, sample_points, year)
        return ds, f"copernicusmarine:{config.dataset_id}", None
    except Exception as exc:  # pragma: no cover - exercised by integration environment
        return None, None, f"current source unavailable: {type(exc).__name__}: {exc}"


def _valid_current_rows(candidates: pd.DataFrame) -> pd.DataFrame:
    if candidates.empty:
        return candidates
    return candidates[
        candidates["current_u"].notna()
        & candidates["current_v"].notna()
        & candidates["timestamp_utc"].notna()
        & candidates["provenance_status"].fillna("").astype(str).str.casefold().eq("real_uo_vo")
    ].copy()


def summarize_event_scale_suitability(candidates: pd.DataFrame, dwell_events: pd.DataFrame) -> dict[str, Any]:
    valid = _valid_current_rows(candidates)
    result: dict[str, Any] = {
        "dwell_event_count": int(len(dwell_events)),
        "events_with_bracketing_current_samples": 0,
        "events_with_window_samples": 0,
        "event_scale_suitable_count": 0,
        "event_scale_suitable_pct": 0.0,
        "nearest_time_gap_minutes_p50": math.nan,
        "nearest_time_gap_minutes_p95": math.nan,
        "event_window_sample_count_p50": math.nan,
        "event_window_sample_count_p95": math.nan,
    }
    if valid.empty or dwell_events.empty:
        return result

    sample_points = valid[["sample_point_id", "lat", "lon"]].drop_duplicates("sample_point_id")
    grouped_times = {
        key: group["timestamp_utc"].sort_values().reset_index(drop=True)
        for key, group in valid.groupby("sample_point_id", sort=False)
    }
    gaps: list[float] = []
    counts: list[int] = []
    bracketed = 0
    suitable = 0
    with_window = 0
    native_resolution = float(valid["current_native_temporal_resolution_minutes"].dropna().iloc[0])

    for _, event in dwell_events.iterrows():
        if pd.isna(event["start_utc"]) or pd.isna(event["end_utc"]):
            continue
        distances = sample_points.apply(
            lambda row: haversine_distance(
                float(event["centroid_lat"]),
                float(event["centroid_lon"]),
                float(row["lat"]),
                float(row["lon"]),
            ),
            axis=1,
        )
        sample_id = sample_points.iloc[int(distances.to_numpy().argmin())]["sample_point_id"]
        times = grouped_times[sample_id]
        start = pd.Timestamp(event["start_utc"])
        end = pd.Timestamp(event["end_utc"])
        midpoint = start + (end - start) / 2
        gap = (times - midpoint).abs().dt.total_seconds().min() / 60.0
        window_count = int(((times >= start) & (times <= end)).sum())
        is_bracketed = bool((times <= start).any() and (times >= end).any())
        gaps.append(float(gap))
        counts.append(window_count)
        with_window += int(window_count > 0)
        bracketed += int(is_bracketed)
        suitable += int(native_resolution <= 60.0 and is_bracketed and gap <= native_resolution)

    result.update(
        {
            "events_with_bracketing_current_samples": int(bracketed),
            "events_with_window_samples": int(with_window),
            "event_scale_suitable_count": int(suitable),
            "event_scale_suitable_pct": float(suitable / len(dwell_events)) if len(dwell_events) else 0.0,
            "nearest_time_gap_minutes_p50": float(np.nanpercentile(gaps, 50)) if gaps else math.nan,
            "nearest_time_gap_minutes_p95": float(np.nanpercentile(gaps, 95)) if gaps else math.nan,
            "event_window_sample_count_p50": float(np.nanpercentile(counts, 50)) if counts else math.nan,
            "event_window_sample_count_p95": float(np.nanpercentile(counts, 95)) if counts else math.nan,
        }
    )
    return result


def classify_current_confidence(
    candidates: pd.DataFrame,
    event_scale: dict[str, Any] | None = None,
) -> tuple[str, str]:
    event_scale = event_scale or {}
    valid = _valid_current_rows(candidates)
    provenance = candidates["provenance_status"].fillna("").astype(str).str.casefold() if not candidates.empty else pd.Series(dtype=str)
    if valid.empty:
        return "D_unsuitable", "No true u/v current rows were produced for this pilot."
    if provenance.str.startswith(("fallback", "simulated", "legacy")).any():
        return "D_unsuitable", "Fallback, simulated, or legacy current provenance is not accepted."
    cadence = float(valid["current_native_temporal_resolution_minutes"].dropna().iloc[0])
    p95_distance = float(valid["current_spatial_distance_km"].dropna().quantile(0.95)) if valid["current_spatial_distance_km"].notna().any() else math.inf
    suitable_pct = float(event_scale.get("event_scale_suitable_pct", 0.0) or 0.0)
    bracketed = int(event_scale.get("events_with_bracketing_current_samples", 0) or 0)
    dwell_count = int(event_scale.get("dwell_event_count", 0) or 0)

    if cadence <= 60 and p95_distance <= 10.0 and dwell_count > 0 and suitable_pct >= 0.8:
        return "A_event_scale", "Hourly true u/v current evidence closely matches the farm and brackets most dwell windows."
    if cadence <= 60 and p95_distance <= 15.0 and bracketed > 0:
        return "B_contextual", "Hourly true u/v current evidence is present, but event-window coverage is incomplete."
    if cadence <= 1440 and p95_distance <= 15.0:
        return "B_contextual", "True u/v current evidence is present but cadence is contextual rather than event-hourly."
    return "C_low_confidence", "True u/v current evidence exists, but cadence, spatial distance, or event alignment is weak."


def validate_current_candidates(
    candidates: pd.DataFrame,
    dwell_events: pd.DataFrame,
    config: CurrentProductConfig,
    farm: str,
    year: int,
    candidate_path: Path,
) -> dict[str, Any]:
    valid = _valid_current_rows(candidates)
    event_scale = summarize_event_scale_suitability(candidates, dwell_events)
    confidence_class, confidence_reason = classify_current_confidence(candidates, event_scale)
    speed_error = math.nan
    if not valid.empty:
        expected_speed = np.sqrt(valid["current_u"].astype(float) ** 2 + valid["current_v"].astype(float) ** 2)
        speed_error = float(np.nanmax(np.abs(expected_speed - valid["current_speed"].astype(float))))
    return {
        "pilot": config.pilot,
        "farm": farm,
        "year": int(year),
        "product_id": config.product_id,
        "dataset_id": config.dataset_id,
        "candidate_path": str(candidate_path),
        "ran": bool(not valid.empty),
        "blocked": bool(valid.empty),
        "blocked_reason": None if not valid.empty else "; ".join(candidates["provenance_status"].dropna().astype(str).unique()[:5]),
        "row_count": int(len(candidates)),
        "valid_uv_row_count": int(len(valid)),
        "sample_point_count": int(candidates["sample_point_id"].nunique()) if not candidates.empty else 0,
        "timestamp_min": str(valid["timestamp_utc"].min()) if not valid.empty else "",
        "timestamp_max": str(valid["timestamp_utc"].max()) if not valid.empty else "",
        "native_cadence_minutes": config.native_temporal_resolution_minutes,
        "depth_levels": sorted(valid["current_depth_m"].dropna().astype(float).unique().tolist())[:10] if not valid.empty else [],
        "missing_uv_count": int(candidates[["current_u", "current_v"]].isna().any(axis=1).sum()) if not candidates.empty else 0,
        "speed_consistency_max_error": speed_error,
        "direction_min": float(valid["current_direction"].min()) if not valid.empty else math.nan,
        "direction_max": float(valid["current_direction"].max()) if not valid.empty else math.nan,
        "spatial_distance_km_p50": float(valid["current_spatial_distance_km"].quantile(0.5)) if not valid.empty else math.nan,
        "spatial_distance_km_p95": float(valid["current_spatial_distance_km"].quantile(0.95)) if not valid.empty else math.nan,
        "provenance_complete": bool(
            not valid.empty
            and valid[["current_product_id", "current_dataset_id", "source_file", "provenance_status"]].notna().all().all()
        ),
        "file_size_bytes": int(candidate_path.stat().st_size) if candidate_path.exists() else 0,
        "event_scale": event_scale,
        "current_confidence_class": confidence_class,
        "current_confidence_reason": confidence_reason,
    }


def _format_number(value: Any, digits: int = 3) -> str:
    try:
        if pd.isna(value):
            return "NA"
        return f"{float(value):.{digits}f}"
    except (TypeError, ValueError):
        return str(value)


def build_product_assessment_markdown() -> str:
    lines = [
        "# Current Pilot v1 Product Assessment",
        "",
        "## Executive Assessment",
        "",
        "Current products are piloted before broad download because final workability modelling needs true Eulerian `uo`/`vo` with known cadence, depth, spatial match, provenance, and event-window suitability. Baltic wave `VSDX`/`VSDY` Stokes drift and legacy CMEMS fallback CSVs are explicitly excluded.",
        "",
        "## Candidate Current Sources",
        "",
        "| Source | Product | Product ID | Dataset ID | Variables | Cadence | Coverage | Space/depth | Access | Event-scale suitability |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for config in (BALTIC_PRODUCT, NWS_PRODUCT):
        lines.append(
            "| "
            + " | ".join(
                [
                    config.current_source,
                    f"[{config.product_name}]({config.service_url})",
                    f"`{config.product_id}`",
                    f"`{config.dataset_id}`",
                    ", ".join(config.variables),
                    f"{config.native_temporal_resolution_minutes} min",
                    config.temporal_coverage,
                    f"{config.native_spatial_resolution_km:g} km; {config.depth_description}",
                    config.access_method,
                    config.event_scale_suitability,
                ]
            )
            + " |"
        )
    lines.append(
        "| Global fallback | "
        f"[{GLOBAL_FALLBACK_ASSESSMENT['product_name']}]({GLOBAL_FALLBACK_ASSESSMENT['service_url']}) | "
        f"`{GLOBAL_FALLBACK_ASSESSMENT['product_id']}` | `{GLOBAL_FALLBACK_ASSESSMENT['dataset_id']}` | "
        f"{GLOBAL_FALLBACK_ASSESSMENT['variables']} | {GLOBAL_FALLBACK_ASSESSMENT['temporal_resolution']} | "
        f"{GLOBAL_FALLBACK_ASSESSMENT['temporal_coverage']} | {GLOBAL_FALLBACK_ASSESSMENT['spatial_resolution']}; "
        f"{GLOBAL_FALLBACK_ASSESSMENT['depth_levels']} | {GLOBAL_FALLBACK_ASSESSMENT['access_method']} | "
        f"{GLOBAL_FALLBACK_ASSESSMENT['event_scale_suitability']} |"
    )
    lines.extend(
        [
            "",
            "## Access And Storage Notes",
            "",
            f"- Baltic tooling: {BALTIC_PRODUCT.tooling_requirement}. Estimated storage: {BALTIC_PRODUCT.storage_estimate}.",
            f"- NWS tooling: {NWS_PRODUCT.tooling_requirement}. Estimated storage: {NWS_PRODUCT.storage_estimate}.",
            "- Global fallback is assessment-only in this increment and must not be downloaded without later approval.",
            "",
            "## Acceptance Gates",
            "",
            "- True `uo/current_u` and `vo/current_v` must be present.",
            "- `current_speed` must equal `sqrt(u^2 + v^2)` within numerical tolerance.",
            "- Direction is flow-to degrees clockwise from true north: `degrees(atan2(u, v)) % 360`.",
            "- Timestamps must be UTC-normalized and cadence must be reported.",
            "- Depth level, product ID, dataset ID, source file, and extraction method must be populated.",
            "- No fallback, simulated, or legacy current rows may carry current values.",
        ]
    )
    return "\n".join(lines) + "\n"


def write_product_assessment(report_dir: Path) -> Path:
    report_dir.mkdir(parents=True, exist_ok=True)
    path = report_dir / PRODUCT_ASSESSMENT_FILENAME
    path.write_text(build_product_assessment_markdown(), encoding="utf-8")
    return path


def _load_existing_validation(output_dir: Path, dwell_weather: Path) -> list[dict[str, Any]]:
    validations: list[dict[str, Any]] = []
    for pilot, config in PRODUCTS.items():
        path = candidate_output_path(output_dir, pilot)
        if not path.exists():
            continue
        candidates = pd.read_parquet(path)
        farm = str(candidates["wind_farm"].dropna().iloc[0]) if not candidates["wind_farm"].dropna().empty else pilot
        year = int(candidates["year"].dropna().iloc[0]) if not candidates["year"].dropna().empty else 0
        dwell = load_dwell_events_for_farm_year(dwell_weather, farm, year) if year else pd.DataFrame()
        validations.append(validate_current_candidates(candidates, dwell, config, farm, year, path))
    return validations


def build_validation_report_markdown(validations: list[dict[str, Any]]) -> str:
    lines = [
        "# Current Pilot v1 Validation Report",
        "",
        "## Research Design",
        "",
        "Current matters for RQ1, RQ4, RQ6, and RQ12 because waves alone cannot describe vessel manoeuvring, DP load, approach aborts, or simulator-ready metocean forcing. This increment pilots true Eulerian `u/v` products before broad download so cadence, depth, domain, and provenance failures are visible while the blast radius is still one farm/year.",
        "",
        "Event-scale suitability means that current evidence has real `uo/vo`, documented cadence and depth, close spatial match, product-domain fit, source provenance, no fallback/simulated values, enough samples in the dwell window, small nearest-time gaps, and source timestamps bracketing dwell windows where the cadence permits.",
        "",
        "Acceptance gates: true `uo/vo`; no Stokes drift; no legacy/fallback CSV promotion; UTC timestamps; documented depth; populated product/dataset/source provenance; physically consistent speed/direction; overwrite-safe outputs.",
        "",
        "## Product Metadata Summary",
        "",
        "| Pilot | Product ID | Dataset ID | Native Cadence | Spatial Resolution | Depth | Event-scale Prior |",
        "| --- | --- | --- | ---: | ---: | --- | --- |",
    ]
    for config in (BALTIC_PRODUCT, NWS_PRODUCT):
        lines.append(
            f"| {config.current_source} | `{config.product_id}` | `{config.dataset_id}` | "
            f"{config.native_temporal_resolution_minutes} min | {config.native_spatial_resolution_km:g} km | "
            f"{config.depth_description} | {config.event_scale_suitability} |"
        )
    lines.extend(["", "## Pilot Results", ""])
    if not validations:
        lines.append("No pilot candidate output exists yet.")
    for validation in validations:
        lines.extend(
            [
                f"### {validation['pilot'].upper()} {validation['farm']} {validation['year']}",
                "",
                f"- Status: {'ran' if validation['ran'] else 'blocked'}",
                f"- Blocked reason: {validation['blocked_reason'] or 'not blocked'}",
                f"- Candidate path: `{validation['candidate_path']}`",
                f"- Row count: {validation['row_count']}",
                f"- Valid `u/v` rows: {validation['valid_uv_row_count']}",
                f"- Sample points: {validation['sample_point_count']}",
                f"- Timestamp range: {validation['timestamp_min'] or 'NA'} to {validation['timestamp_max'] or 'NA'}",
                f"- Native cadence: {validation['native_cadence_minutes']} minutes",
                f"- Depth levels used: {validation['depth_levels'] or 'NA'}",
                f"- Missing `u/v` rows: {validation['missing_uv_count']}",
                f"- Speed consistency max error: {_format_number(validation['speed_consistency_max_error'], 8)}",
                f"- Direction range: {_format_number(validation['direction_min'])} to {_format_number(validation['direction_max'])} degrees",
                f"- Spatial distance p50/p95: {_format_number(validation['spatial_distance_km_p50'])} / {_format_number(validation['spatial_distance_km_p95'])} km",
                f"- Provenance complete: {validation['provenance_complete']}",
                f"- File/storage size: {validation['file_size_bytes']} bytes",
                "",
                "Event-scale suitability:",
                f"- Dwell events: {validation['event_scale']['dwell_event_count']}",
                f"- Bracketed events: {validation['event_scale']['events_with_bracketing_current_samples']}",
                f"- Events with in-window samples: {validation['event_scale']['events_with_window_samples']}",
                f"- Nearest gap p50/p95: {_format_number(validation['event_scale']['nearest_time_gap_minutes_p50'])} / {_format_number(validation['event_scale']['nearest_time_gap_minutes_p95'])} minutes",
                f"- Window sample count p50/p95: {_format_number(validation['event_scale']['event_window_sample_count_p50'])} / {_format_number(validation['event_scale']['event_window_sample_count_p95'])}",
                f"- Suitable percentage: {_format_number(validation['event_scale']['event_scale_suitable_pct'] * 100.0, 1)}%",
                f"- Confidence class: `{validation['current_confidence_class']}`",
                f"- Confidence reason: {validation['current_confidence_reason']}",
                "",
            ]
        )
    lines.extend(
        [
            "## Recommendation",
            "",
            "- Scale Baltic currents only if daily/contextual evidence is sufficient for the intended model or an hourly Baltic true-current source is approved separately.",
            "- Scale NWS currents if the scoped NWS pilot produces real hourly `u/v` rows with adequate event-window bracketing and spatial match.",
            "- Keep global currents as fallback assessment only until regional gaps are proven.",
            "- Reuse this candidate/provenance/confidence pattern for any Current v1 agreement layer.",
        ]
    )
    return "\n".join(lines) + "\n"


def write_validation_report(report_dir: Path, validations: list[dict[str, Any]]) -> Path:
    report_dir.mkdir(parents=True, exist_ok=True)
    path = report_dir / VALIDATION_REPORT_FILENAME
    path.write_text(build_validation_report_markdown(validations), encoding="utf-8")
    return path


def build_current_pilot_v1(
    pilot: str,
    farm: str,
    year: int,
    dwell_weather: Path = DEFAULT_DWELL_WEATHER_INPUT,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    report_dir: Path = DEFAULT_REPORT_DIR,
    dry_run: bool = False,
    overwrite: bool = False,
) -> CurrentPilotResult:
    pilot = pilot.casefold()
    if pilot not in PRODUCTS:
        raise ValueError(f"Unsupported pilot: {pilot}")
    config = PRODUCTS[pilot]
    product_assessment_path = write_product_assessment(report_dir)
    candidate_path = candidate_output_path(output_dir, pilot)

    sample_points = load_sample_points_from_wave_archive(config.preferred_wave_root, farm, int(year))
    dwell_events = load_dwell_events_for_farm_year(dwell_weather, farm, int(year))

    if dry_run:
        candidates = blocked_candidate_rows(sample_points, config, farm, year, "dry_run: no source opened")
    else:
        ds, source_file, blocker = load_or_fetch_current_dataset(config, sample_points, int(year))
        if ds is None:
            candidates = blocked_candidate_rows(sample_points, config, farm, year, blocker or "source unavailable")
        else:
            try:
                candidates = extract_candidates_from_xarray(
                    ds=ds,
                    sample_points=sample_points,
                    config=config,
                    farm=farm,
                    year=int(year),
                    source_file=source_file or config.dataset_id,
                )
                if hasattr(ds, "close"):
                    ds.close()
            except Exception as exc:
                if hasattr(ds, "close"):
                    ds.close()
                candidates = blocked_candidate_rows(
                    sample_points,
                    config,
                    farm,
                    year,
                    f"extraction failed: {type(exc).__name__}: {exc}",
                )

    write_candidate_table(candidates, candidate_path, overwrite=overwrite)
    validation = validate_current_candidates(candidates, dwell_events, config, farm, int(year), candidate_path)
    validations = _load_existing_validation(output_dir, dwell_weather)
    if not any(v["pilot"] == validation["pilot"] for v in validations):
        validations.append(validation)
    else:
        validations = [validation if v["pilot"] == validation["pilot"] else v for v in validations]
    validation_report_path = write_validation_report(report_dir, validations)
    return CurrentPilotResult(
        pilot=pilot,
        candidate_path=candidate_path,
        product_assessment_path=product_assessment_path,
        validation_report_path=validation_report_path,
        candidates=candidates,
        validation=validation,
    )
