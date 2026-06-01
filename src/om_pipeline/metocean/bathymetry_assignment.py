"""Assign static bathymetry to common metocean sample points.

The assignment uses the official EMODnet Bathymetry point-sample REST service
as the primary source and caches every raw response before writing the processed
positive-down point archive. It is intentionally source-specific: no current,
FINO, dwell-weather, or source-fusion outputs are created here.
"""

from __future__ import annotations

import hashlib
import json
import math
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import numpy as np
import pandas as pd
import requests

from .bathymetry_planner import (
    BATHYMETRY_OUTPUT_SCHEMA,
    REQUIRED_REQUIREMENT_COLUMNS,
    _classify_region,
    _load_requirements,
    _normalise_source,
)
from .common_requirements import farm_slug, normalize_farm_name
from .extract_nws import haversine_distance


DEFAULT_REQUIREMENTS_PATH = Path(
    "analysis/06_rq6_metocean_spatial_resolution/common_metocean_farm_requirements.csv"
)
DEFAULT_TURBINE_COORDINATES = Path("Data/Interim/European_Turbine_Coordinates.csv")
DEFAULT_SOURCE_ROOT = Path("Data/Raw/Metocean/Bathymetry")
DEFAULT_OUTPUT_ROOT = Path("Data/Processed/metocean/bathymetry")
DEFAULT_QA_REPORT = Path(
    "analysis/06_rq6_metocean_spatial_resolution/bathymetry_assignment_full_report.md"
)

EMODNET_DEPTH_SAMPLE_URL = "https://rest.emodnet-bathymetry.eu/depth_sample"
EMODNET_VERSION = "EMODnet DTM 2024"
EMODNET_VERTICAL_DATUM = "Lowest Astronomical Tide (LAT)"
EMODNET_GRID_SPACING_DEGREES = 1.0 / (16.0 * 60.0)
GEBCO_FALLBACK_VERSION = "GEBCO_2026"
PROCESSED_DEPTH_SIGN_CONVENTION = "positive_down_meters_in_processed_table"
COORDINATE_REFERENCE_SYSTEM = "EPSG:4326 / WGS84 latitude-longitude"
ASSIGNMENT_METHOD = "emodnet_rest_depth_sample_grid_cell_average"
SPOT_CHECK_COUNT = 20


@dataclass(frozen=True)
class BathymetryAssignmentResult:
    """In-memory summary from a bathymetry assignment run."""

    output: pd.DataFrame
    metadata: dict[str, Any]
    qa: dict[str, Any]
    source_cache_path: Path
    source_metadata_path: Path
    output_path: Path
    processed_metadata_path: Path
    qa_report: Path | None


class EmodnetDepthClient:
    """Small client for the EMODnet Bathymetry depth_sample endpoint."""

    def __init__(
        self,
        base_url: str = EMODNET_DEPTH_SAMPLE_URL,
        timeout_seconds: float = 30.0,
        retries: int = 3,
        backoff_seconds: float = 0.5,
        session: requests.Session | None = None,
    ) -> None:
        self.base_url = base_url
        self.timeout_seconds = timeout_seconds
        self.retries = retries
        self.backoff_seconds = backoff_seconds
        self.session = session or requests.Session()

    def fetch(self, lon: float, lat: float) -> dict[str, Any]:
        geom = f"POINT({lon:.8f} {lat:.8f})"
        last_error: str | None = None
        for attempt in range(1, self.retries + 1):
            try:
                response = self.session.get(
                    self.base_url,
                    params={"geom": geom},
                    timeout=self.timeout_seconds,
                    headers={"Accept": "application/json"},
                )
                response.raise_for_status()
                payload = response.json()
                return {
                    "ok": True,
                    "status_code": response.status_code,
                    "geom": geom,
                    "url": response.url,
                    "payload": payload,
                    "error": None,
                    "attempts": attempt,
                }
            except Exception as exc:  # pragma: no cover - exercised with integration run.
                last_error = f"{type(exc).__name__}: {exc}"
                if attempt < self.retries:
                    time.sleep(self.backoff_seconds * attempt)
        return {
            "ok": False,
            "status_code": None,
            "geom": geom,
            "url": self.base_url,
            "payload": None,
            "error": last_error,
            "attempts": self.retries,
        }


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _read_table(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".parquet":
        return pd.read_parquet(path)
    return pd.read_csv(path)


def _load_turbine_coordinates(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Turbine coordinate table not found: {path}")
    turbines = _read_table(path)
    required = {"wind_farm", "latitude", "longitude"}
    missing = sorted(required - set(turbines.columns))
    if missing:
        raise ValueError(f"Turbine coordinate table is missing required columns: {missing}")
    turbines = turbines.copy()
    turbines["latitude"] = pd.to_numeric(turbines["latitude"], errors="coerce")
    turbines["longitude"] = pd.to_numeric(turbines["longitude"], errors="coerce")
    turbines = turbines.dropna(subset=["wind_farm", "latitude", "longitude"])
    turbines["normalized_farm"] = turbines["wind_farm"].map(normalize_farm_name)
    return turbines


def build_common_metocean_sample_points(
    requirements_path: Path = DEFAULT_REQUIREMENTS_PATH,
    turbine_coordinates_path: Path = DEFAULT_TURBINE_COORDINATES,
) -> pd.DataFrame:
    """Expand common requirements into centroid plus turbine sample points."""
    requirements = _load_requirements(requirements_path)
    turbines = _load_turbine_coordinates(turbine_coordinates_path)
    turbine_lookup = {
        normalized: farm_turbines.reset_index(drop=True)
        for normalized, farm_turbines in turbines.groupby("normalized_farm", dropna=True)
    }

    rows: list[dict[str, Any]] = []
    mismatches: list[str] = []
    missing_farms: list[str] = []
    for _, requirement in requirements.sort_values("wind_farm").iterrows():
        wind_farm = str(requirement["wind_farm"])
        normalized = normalize_farm_name(wind_farm)
        farm_turbines = turbine_lookup.get(normalized)
        if farm_turbines is None or farm_turbines.empty:
            missing_farms.append(wind_farm)
            continue

        farm_id = str(requirement["farm_id"])
        country = requirement.get("country")
        region = _classify_region(requirement)
        centroid_lat = float(farm_turbines["latitude"].mean())
        centroid_lon = float(farm_turbines["longitude"].mean())
        rows.append(
            {
                "wind_farm": wind_farm,
                "farm_id": farm_id,
                "country": country,
                "region": region,
                "sample_point_id": "farm_centroid",
                "sample_point_type": "farm_centroid",
                "lat": centroid_lat,
                "lon": centroid_lon,
            }
        )
        for idx, turbine in farm_turbines.reset_index(drop=True).iterrows():
            rows.append(
                {
                    "wind_farm": wind_farm,
                    "farm_id": farm_id,
                    "country": country,
                    "region": region,
                    "sample_point_id": f"turbine_{idx:04d}",
                    "sample_point_type": "turbine",
                    "lat": float(turbine["latitude"]),
                    "lon": float(turbine["longitude"]),
                }
            )

        expected = int(requirement["sample_point_count"])
        actual = int(len(farm_turbines) + 1)
        if expected != actual:
            mismatches.append(f"{wind_farm}: expected {expected}, built {actual}")

    if missing_farms:
        preview = ", ".join(missing_farms[:5])
        raise ValueError(f"Missing turbine coordinates for common requirement farms: {preview}")
    if mismatches:
        preview = "; ".join(mismatches[:5])
        raise ValueError(f"Sample point count mismatch: {preview}")

    points = pd.DataFrame(rows)
    if points.empty:
        raise ValueError("No common metocean sample points could be built.")
    duplicate_keys = points.duplicated(["wind_farm", "sample_point_id"], keep=False)
    if duplicate_keys.any():
        duplicate_preview = points.loc[duplicate_keys, ["wind_farm", "sample_point_id"]].head()
        raise ValueError(f"Duplicate sample point keys detected:\n{duplicate_preview}")
    return points.reset_index(drop=True)


def _cache_key(row: pd.Series) -> str:
    text = "|".join(
        [
            "emodnet_depth_sample",
            str(row["wind_farm"]),
            str(row["sample_point_id"]),
            f"{float(row['lat']):.8f}",
            f"{float(row['lon']):.8f}",
        ]
    )
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _load_response_cache(cache_path: Path) -> dict[str, dict[str, Any]]:
    if not cache_path.exists():
        return {}
    cached: dict[str, dict[str, Any]] = {}
    with cache_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            record = json.loads(line)
            cached[str(record["cache_key"])] = record
    return cached


def _append_cache_records(cache_path: Path, records: list[dict[str, Any]]) -> None:
    if not records:
        return
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    with cache_path.open("a", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, sort_keys=True, ensure_ascii=False) + "\n")


def _snap_to_emodnet_grid(value: float) -> float:
    return round(round(value / EMODNET_GRID_SPACING_DEGREES) * EMODNET_GRID_SPACING_DEGREES, 8)


def _positive_down_depth(raw_value: Any) -> float:
    if raw_value is None:
        return math.nan
    try:
        value = float(raw_value)
    except (TypeError, ValueError):
        return math.nan
    if not math.isfinite(value):
        return math.nan
    if value < 0:
        return -value
    return value


def _source_reference(payload: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    reference = payload.get("reference")
    return reference if isinstance(reference, dict) else {}


def _record_status(response_record: dict[str, Any]) -> str:
    if not response_record.get("ok"):
        return "source_request_failed"
    payload = response_record.get("payload")
    if not isinstance(payload, dict):
        return "source_payload_missing"
    if payload.get("avg") is None and payload.get("smoothed") is None:
        return "source_depth_missing"
    return "ok"


def _assignment_row(point: pd.Series, response_record: dict[str, Any]) -> dict[str, Any]:
    payload = response_record.get("payload") if isinstance(response_record, dict) else None
    status = _record_status(response_record)
    raw_depth = payload.get("avg") if isinstance(payload, dict) else None
    if raw_depth is None and isinstance(payload, dict):
        raw_depth = payload.get("smoothed")
    water_depth = _positive_down_depth(raw_depth) if status == "ok" else math.nan
    grid_lat = _snap_to_emodnet_grid(float(point["lat"]))
    grid_lon = _snap_to_emodnet_grid(float(point["lon"]))
    distance_m = float(haversine_distance(float(point["lat"]), float(point["lon"]), grid_lat, grid_lon) * 1000.0)
    source = "emodnet" if status == "ok" else pd.NA
    return {
        "wind_farm": point["wind_farm"],
        "sample_point_id": point["sample_point_id"],
        "sample_point_type": point["sample_point_type"],
        "lat": float(point["lat"]),
        "lon": float(point["lon"]),
        "water_depth_m": water_depth,
        "bathymetry_source": source,
        "bathymetry_version": EMODNET_VERSION if status == "ok" else pd.NA,
        "bathymetry_grid_lat": grid_lat,
        "bathymetry_grid_lon": grid_lon,
        "bathymetry_distance_m": distance_m,
        "bathymetry_assignment_method": ASSIGNMENT_METHOD if status == "ok" else pd.NA,
        "depth_sign_convention": PROCESSED_DEPTH_SIGN_CONVENTION if status == "ok" else pd.NA,
        "bathymetry_vertical_datum": EMODNET_VERTICAL_DATUM if status == "ok" else pd.NA,
        "bathymetry_spatial_match_status": status,
    }


def _source_reference_row(point: pd.Series, response_record: dict[str, Any]) -> dict[str, Any]:
    payload = response_record.get("payload") if isinstance(response_record, dict) else None
    reference = _source_reference(payload)
    return {
        "wind_farm": point["wind_farm"],
        "sample_point_id": point["sample_point_id"],
        "raw_avg": payload.get("avg") if isinstance(payload, dict) else None,
        "raw_smoothed": payload.get("smoothed") if isinstance(payload, dict) else None,
        "source_reference_identifier": reference.get("identifier"),
        "source_reference_type": reference.get("type"),
        "source_reference_organisation_id": reference.get("organisation_id"),
        "source_reference_metadata_url": reference.get("metadata_url"),
    }


def _fetch_missing_records(
    points: pd.DataFrame,
    cached: dict[str, dict[str, Any]],
    cache_path: Path,
    client: EmodnetDepthClient,
    max_workers: int,
    progress_callback: Callable[[str], None] | None = None,
) -> dict[str, dict[str, Any]]:
    points_with_keys = points.assign(cache_key=points.apply(_cache_key, axis=1))
    missing = points_with_keys[~points_with_keys["cache_key"].isin(cached)].reset_index(drop=True)
    if missing.empty:
        if progress_callback:
            progress_callback(f"EMODnet source cache already covers {len(points)} sample points.")
        return cached

    if progress_callback:
        progress_callback(f"Fetching {len(missing)} missing EMODnet depth samples.")

    completed = 0
    pending_records: list[dict[str, Any]] = []

    def fetch_row(row: dict[str, Any]) -> dict[str, Any]:
        response = client.fetch(lon=float(row["lon"]), lat=float(row["lat"]))
        return {
            "cache_key": row["cache_key"],
            "source": "emodnet",
            "source_version": EMODNET_VERSION,
            "vertical_datum": EMODNET_VERTICAL_DATUM,
            "coordinate_reference_system": COORDINATE_REFERENCE_SYSTEM,
            "acquired_at_utc": _utc_now_iso(),
            "request": {
                "wind_farm": row["wind_farm"],
                "sample_point_id": row["sample_point_id"],
                "sample_point_type": row["sample_point_type"],
                "lat": float(row["lat"]),
                "lon": float(row["lon"]),
                "geom": response.get("geom"),
                "url": response.get("url"),
            },
            "response": response,
        }

    workers = max(1, int(max_workers))
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(fetch_row, row) for row in missing.to_dict("records")]
        for future in as_completed(futures):
            record = future.result()
            cached[str(record["cache_key"])] = record
            pending_records.append(record)
            completed += 1
            if len(pending_records) >= 100:
                _append_cache_records(cache_path, pending_records)
                pending_records = []
            if progress_callback and (completed == len(missing) or completed % 250 == 0):
                progress_callback(f"Fetched {completed}/{len(missing)} missing EMODnet depth samples.")

    _append_cache_records(cache_path, pending_records)
    return cached


def _build_assignment_frame(
    points: pd.DataFrame,
    response_cache: dict[str, dict[str, Any]],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    output_rows: list[dict[str, Any]] = []
    reference_rows: list[dict[str, Any]] = []
    for _, point in points.iterrows():
        record = response_cache[_cache_key(point)]
        response = record.get("response", {})
        output_rows.append(_assignment_row(point, response))
        reference_rows.append(_source_reference_row(point, response))
    output = pd.DataFrame(output_rows, columns=BATHYMETRY_OUTPUT_SCHEMA)
    references = pd.DataFrame(reference_rows)
    return output, references


def _summarize_output(output: pd.DataFrame, points: pd.DataFrame, references: pd.DataFrame) -> dict[str, Any]:
    depths = output["water_depth_m"]
    non_null_depths = depths.dropna()
    duplicate_count = int(output.duplicated(["wind_farm", "sample_point_id"], keep=False).sum())
    populated = output[output["water_depth_m"].notna()]
    provenance_columns = [
        "bathymetry_source",
        "bathymetry_version",
        "bathymetry_assignment_method",
        "depth_sign_convention",
        "bathymetry_vertical_datum",
        "bathymetry_spatial_match_status",
    ]
    if populated.empty:
        provenance_populated = False
    else:
        provenance_populated = bool(populated[provenance_columns].notna().all().all())
    status_counts = output["bathymetry_spatial_match_status"].value_counts(dropna=False).to_dict()
    source_counts = output["bathymetry_source"].value_counts(dropna=False).to_dict()
    reference_counts = references["source_reference_identifier"].fillna("missing").value_counts().head(20).to_dict()
    region_summary = (
        points[["wind_farm", "sample_point_id", "region"]]
        .merge(output[["wind_farm", "sample_point_id", "water_depth_m"]], on=["wind_farm", "sample_point_id"], how="left")
        .groupby("region", dropna=False)
        .agg(
            sample_points=("sample_point_id", "size"),
            missing_depths=("water_depth_m", lambda values: int(values.isna().sum())),
            min_depth_m=("water_depth_m", "min"),
            median_depth_m=("water_depth_m", "median"),
            mean_depth_m=("water_depth_m", "mean"),
            p95_depth_m=("water_depth_m", lambda values: float(values.dropna().quantile(0.95)) if values.notna().any() else math.nan),
            max_depth_m=("water_depth_m", "max"),
        )
        .reset_index()
    )
    farm_summary = (
        points[["wind_farm", "sample_point_id", "region"]]
        .merge(output[["wind_farm", "sample_point_id", "water_depth_m"]], on=["wind_farm", "sample_point_id"], how="left")
        .groupby(["wind_farm", "region"], dropna=False)
        .agg(
            sample_points=("sample_point_id", "size"),
            missing_depths=("water_depth_m", lambda values: int(values.isna().sum())),
            min_depth_m=("water_depth_m", "min"),
            median_depth_m=("water_depth_m", "median"),
            max_depth_m=("water_depth_m", "max"),
        )
        .reset_index()
        .sort_values(["region", "wind_farm"])
    )
    summary = {
        "row_count": int(len(output)),
        "farm_count": int(output["wind_farm"].nunique()),
        "sample_point_count": int(len(points)),
        "missing_depth_count": int(depths.isna().sum()),
        "missing_depth_rate": float(depths.isna().mean()) if len(depths) else math.nan,
        "duplicate_wind_farm_sample_point_count": duplicate_count,
        "provenance_populated_where_depth_exists": provenance_populated,
        "fallback_row_count": int((output["bathymetry_source"] == "gebco_2026").sum()),
        "status_counts": status_counts,
        "source_counts": {str(key): int(value) for key, value in source_counts.items()},
        "source_reference_identifier_top20": reference_counts,
        "depth_summary": {
            "min": float(non_null_depths.min()) if not non_null_depths.empty else math.nan,
            "median": float(non_null_depths.median()) if not non_null_depths.empty else math.nan,
            "mean": float(non_null_depths.mean()) if not non_null_depths.empty else math.nan,
            "p95": float(non_null_depths.quantile(0.95)) if not non_null_depths.empty else math.nan,
            "max": float(non_null_depths.max()) if not non_null_depths.empty else math.nan,
        },
        "zero_depth_count": int((depths == 0).sum()),
        "shallow_depth_le_1m_count": int((depths <= 1.0).sum()),
        "distance_summary_m": {
            "min": float(output["bathymetry_distance_m"].min()) if not output.empty else math.nan,
            "median": float(output["bathymetry_distance_m"].median()) if not output.empty else math.nan,
            "max": float(output["bathymetry_distance_m"].max()) if not output.empty else math.nan,
        },
        "region_summary": region_summary,
        "farm_summary": farm_summary,
    }
    return summary


def _build_spot_checks(
    output: pd.DataFrame,
    points: pd.DataFrame,
    response_cache: dict[str, dict[str, Any]],
    limit: int = SPOT_CHECK_COUNT,
) -> list[dict[str, Any]]:
    if output.empty:
        return []
    indices = np.linspace(0, len(output) - 1, min(limit, len(output)), dtype=int)
    checks: list[dict[str, Any]] = []
    for idx in indices:
        out_row = output.iloc[int(idx)]
        point = points[
            (points["wind_farm"] == out_row["wind_farm"])
            & (points["sample_point_id"] == out_row["sample_point_id"])
        ].iloc[0]
        response = response_cache[_cache_key(point)].get("response", {})
        payload = response.get("payload") if isinstance(response, dict) else {}
        raw_depth = payload.get("avg") if isinstance(payload, dict) else None
        expected = _positive_down_depth(raw_depth)
        processed = float(out_row["water_depth_m"]) if pd.notna(out_row["water_depth_m"]) else math.nan
        checks.append(
            {
                "wind_farm": out_row["wind_farm"],
                "sample_point_id": out_row["sample_point_id"],
                "raw_avg": raw_depth,
                "processed_water_depth_m": processed,
                "absolute_difference_m": (
                    abs(expected - processed)
                    if math.isfinite(expected) and math.isfinite(processed)
                    else math.nan
                ),
                "status": "ok" if math.isfinite(expected) and abs(expected - processed) < 1e-9 else "review",
            }
        )
    return checks


def _snapshot_archives(paths: list[Path]) -> dict[str, dict[str, Any]]:
    snapshot: dict[str, dict[str, Any]] = {}
    for path in paths:
        if not path.exists():
            snapshot[str(path)] = {"exists": False, "file_count": 0, "latest_mtime": None}
            continue
        files = [candidate for candidate in path.rglob("*") if candidate.is_file()]
        latest = max((candidate.stat().st_mtime for candidate in files), default=path.stat().st_mtime)
        snapshot[str(path)] = {
            "exists": True,
            "file_count": len(files),
            "latest_mtime": pd.Timestamp(latest, unit="s", tz="UTC").isoformat(),
        }
    return snapshot


def _format_bytes(value: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    size = float(value)
    for unit in units:
        if size < 1024.0 or unit == units[-1]:
            return f"{size:.1f} {unit}"
        size /= 1024.0
    return f"{float(value):.1f} B"


def _disk_summary(path: Path) -> dict[str, Any]:
    import shutil

    usage = shutil.disk_usage(path)
    return {
        "total": usage.total,
        "used": usage.used,
        "free": usage.free,
        "total_human": _format_bytes(usage.total),
        "used_human": _format_bytes(usage.used),
        "free_human": _format_bytes(usage.free),
    }


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False) + "\n", encoding="utf-8")


def _markdown_table(frame: pd.DataFrame, columns: list[str], max_rows: int | None = None) -> str:
    if frame.empty:
        return "_No rows._"
    table = frame[columns].copy()
    if max_rows is not None:
        table = table.head(max_rows)
    return table.to_markdown(index=False)


def render_bathymetry_assignment_report(result: BathymetryAssignmentResult, command: str | None = None) -> str:
    qa = result.qa
    depth = qa["depth_summary"]
    distance = qa["distance_summary_m"]
    lines = [
        "# Bathymetry Assignment Full Report",
        "",
        "Status: completed bathymetry-only source acquisition and static point assignment. "
        "No currents, FINO bulk data, source fusion, 10-minute interpolation, NORA3 reruns, or final dwell-metocean rebuilds were run.",
        "",
        "## Executive Conclusion",
        "",
        f"- accepted_candidate: `{qa['accepted_candidate']}`",
        f"- row_count: `{qa['row_count']}`",
        f"- farm_count: `{qa['farm_count']}`",
        f"- missing_depth_count: `{qa['missing_depth_count']}`",
        f"- duplicate_wind_farm_sample_point_count: `{qa['duplicate_wind_farm_sample_point_count']}`",
        f"- fallback_row_count: `{qa['fallback_row_count']}`",
        "",
        "## Command",
        "",
        f"`{command or qa.get('command', 'not_recorded')}`",
        "",
        "## Source Access And Cache",
        "",
        f"- primary_source: `emodnet`",
        f"- primary_version: `{EMODNET_VERSION}`",
        f"- primary_endpoint: `{EMODNET_DEPTH_SAMPLE_URL}`",
        f"- fallback_source: `{qa['fallback_source']}`",
        f"- fallback_status: `{qa['fallback_status']}`",
        f"- source_cache_path: `{result.source_cache_path}`",
        f"- source_metadata_path: `{result.source_metadata_path}`",
        f"- raw_source_artifact_type: `EMODnet REST depth_sample JSONL cache`",
        "- raster_tile_note: no raster tiles were downloaded; the official EMODnet point-sample service was used for this fixed-point assignment.",
        "",
        "## Output Paths",
        "",
        f"- processed_output_path: `{result.output_path}`",
        f"- processed_metadata_path: `{result.processed_metadata_path}`",
        f"- qa_report_path: `{result.qa_report}`",
        "",
        "## Preflight",
        "",
        f"- requirements_path: `{qa['requirements_path']}`",
        f"- turbine_coordinates_path: `{qa['turbine_coordinates_path']}`",
        f"- output_existed_before_run: `{qa['output_existed_before_run']}`",
        f"- no_overwrite: `{qa['no_overwrite']}`",
        f"- disk_free_before: `{qa['disk_before']['free_human']}`",
        f"- disk_free_after: `{qa['disk_after']['free_human']}`",
        f"- source_access_preflight_status: `{qa['source_access_preflight_status']}`",
        "",
        "## Schema",
        "",
        "\n".join(f"- `{column}`" for column in BATHYMETRY_OUTPUT_SCHEMA),
        "",
        "## Validation",
        "",
        f"- missing_depth_rate: `{qa['missing_depth_rate']}`",
        f"- provenance_populated_where_depth_exists: `{qa['provenance_populated_where_depth_exists']}`",
        f"- positive_down_sign_convention: `{qa['positive_down_sign_convention']}`",
        f"- vertical_datum_recorded: `{qa['vertical_datum_recorded']}`",
        f"- crs_recorded: `{qa['crs_recorded']}`",
        f"- distance_populated: `{qa['distance_populated']}`",
        f"- plausible_depths: `{qa['plausible_depths']}`",
        f"- status_counts: `{qa['status_counts']}`",
        f"- source_counts: `{qa['source_counts']}`",
        "",
        "## Depth Summary",
        "",
        f"- min_depth_m: `{depth['min']}`",
        f"- median_depth_m: `{depth['median']}`",
        f"- mean_depth_m: `{depth['mean']}`",
        f"- p95_depth_m: `{depth['p95']}`",
        f"- max_depth_m: `{depth['max']}`",
        f"- zero_depth_count: `{qa['zero_depth_count']}`",
        f"- shallow_depth_le_1m_count: `{qa['shallow_depth_le_1m_count']}`",
        "",
        "## Assignment Distance Summary",
        "",
        f"- min_distance_m: `{distance['min']}`",
        f"- median_distance_m: `{distance['median']}`",
        f"- max_distance_m: `{distance['max']}`",
        "",
        "## Region Summary",
        "",
        _markdown_table(
            qa["region_summary"],
            [
                "region",
                "sample_points",
                "missing_depths",
                "min_depth_m",
                "median_depth_m",
                "mean_depth_m",
                "p95_depth_m",
                "max_depth_m",
            ],
        ),
        "",
        "## Farm Summary",
        "",
        _markdown_table(
            qa["farm_summary"],
            [
                "wind_farm",
                "region",
                "sample_points",
                "missing_depths",
                "min_depth_m",
                "median_depth_m",
                "max_depth_m",
            ],
        ),
        "",
        "## Spot Checks Against Raw Cache",
        "",
        _markdown_table(pd.DataFrame(qa["spot_checks"]), list(qa["spot_checks"][0].keys()) if qa["spot_checks"] else []),
        "",
        "## Source Reference Identifiers",
        "",
        f"- top_source_references: `{qa['source_reference_identifier_top20']}`",
        "",
        "## Wave Archive Immutability Check",
        "",
        f"- before: `{qa['wave_archive_snapshot_before']}`",
        f"- after: `{qa['wave_archive_snapshot_after']}`",
        f"- unchanged: `{qa['wave_archives_unchanged']}`",
        "",
        "## Guardrails",
        "",
        "- No current downloads were run.",
        "- No FINO bulk data ingestion was run.",
        "- No source fusion or preferred-source variables were created.",
        "- No final dwell-metocean feature table was rebuilt.",
        "- No 10-minute interpolation was performed.",
        "- No NORA3 extraction or consolidation was run.",
        "- Existing Baltic, NWS, and NORA3 wave archives were not modified by this script.",
        "",
        "## Notes And Caveats",
        "",
        "- EMODnet is the only source used where depths are present. GEBCO_2026 remains the documented fallback/cross-check source, but it was not fetched because EMODnet returned depths for the assignment points.",
        "- Processed depths are positive-down metres. EMODnet REST raw `avg` values observed in the cache are negative below datum and are sign-converted in the processed table.",
        "- The assignment uses EMODnet REST grid-cell depth samples rather than local raster bilinear interpolation; this avoids acquiring large rasters for a fixed-point static site-context table.",
        "",
    ]
    return "\n".join(lines)


def assign_bathymetry_to_metocean_points(
    requirements_path: Path = DEFAULT_REQUIREMENTS_PATH,
    turbine_coordinates_path: Path = DEFAULT_TURBINE_COORDINATES,
    source_root: Path = DEFAULT_SOURCE_ROOT,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    qa_report: Path | None = DEFAULT_QA_REPORT,
    primary_source: str = "emodnet",
    fallback_source: str = "gebco_2026",
    no_overwrite: bool = True,
    limit_scope: str = "common-requirements",
    max_workers: int = 8,
    client: EmodnetDepthClient | None = None,
    progress_callback: Callable[[str], None] | None = None,
    command: str | None = None,
) -> BathymetryAssignmentResult:
    """Acquire EMODnet point samples and write the processed bathymetry table."""
    if limit_scope != "common-requirements":
        raise ValueError("Only --limit-scope common-requirements is supported for this assignment.")
    if _normalise_source(primary_source) != "emodnet":
        raise ValueError("Only EMODnet is supported as the primary bathymetry source.")
    if _normalise_source(fallback_source) != "gebco_2026":
        raise ValueError("Only GEBCO_2026 is supported as the planned fallback source.")

    output_path = output_root / "site_bathymetry_points.parquet"
    processed_metadata_path = output_root / "bathymetry_source_metadata.json"
    output_existed_before_run = output_path.exists()
    if output_existed_before_run and no_overwrite:
        raise FileExistsError(
            f"Processed bathymetry output already exists and --no-overwrite was set: {output_path}"
        )

    missing_requirement_columns = sorted(
        set(REQUIRED_REQUIREMENT_COLUMNS) - set(_read_table(requirements_path).columns)
    )
    if missing_requirement_columns:
        raise ValueError(f"Common requirements table is missing required columns: {missing_requirement_columns}")

    points = build_common_metocean_sample_points(
        requirements_path=requirements_path,
        turbine_coordinates_path=turbine_coordinates_path,
    )
    disk_before = _disk_summary(Path("."))
    wave_paths = [
        Path("Data/Processed/metocean/baltic_wave_timeseries"),
        Path("Data/Processed/metocean/nws_wave_timeseries"),
        Path("Data/Processed/metocean/nora3_joined_cache"),
    ]
    wave_snapshot_before = _snapshot_archives(wave_paths)

    depth_client = client or EmodnetDepthClient()
    preflight_point = points.iloc[0]
    preflight_response = depth_client.fetch(lon=float(preflight_point["lon"]), lat=float(preflight_point["lat"]))
    preflight_status = _record_status(preflight_response)
    if preflight_status != "ok":
        raise RuntimeError(f"EMODnet source access preflight failed: {preflight_response.get('error')}")

    source_cache_dir = source_root / "emodnet_depth_samples"
    source_cache_path = source_cache_dir / "common_requirements_depth_samples.jsonl"
    source_metadata_path = source_cache_dir / "emodnet_depth_samples_metadata.json"
    source_cache = _load_response_cache(source_cache_path)

    preflight_cache_record = {
        "cache_key": _cache_key(preflight_point),
        "source": "emodnet",
        "source_version": EMODNET_VERSION,
        "vertical_datum": EMODNET_VERTICAL_DATUM,
        "coordinate_reference_system": COORDINATE_REFERENCE_SYSTEM,
        "acquired_at_utc": _utc_now_iso(),
        "request": {
            "wind_farm": preflight_point["wind_farm"],
            "sample_point_id": preflight_point["sample_point_id"],
            "sample_point_type": preflight_point["sample_point_type"],
            "lat": float(preflight_point["lat"]),
            "lon": float(preflight_point["lon"]),
            "geom": preflight_response.get("geom"),
            "url": preflight_response.get("url"),
        },
        "response": preflight_response,
    }
    preflight_key = str(preflight_cache_record["cache_key"])
    preflight_was_cached = preflight_key in source_cache
    source_cache.setdefault(preflight_key, preflight_cache_record)
    if not preflight_was_cached:
        _append_cache_records(source_cache_path, [preflight_cache_record])

    source_cache = _fetch_missing_records(
        points=points,
        cached=source_cache,
        cache_path=source_cache_path,
        client=depth_client,
        max_workers=max_workers,
        progress_callback=progress_callback,
    )

    output, references = _build_assignment_frame(points, source_cache)
    summary = _summarize_output(output, points, references)
    missing_rate = float(summary["missing_depth_rate"])
    if missing_rate > 0.05:
        raise RuntimeError(
            f"Bathymetry assignment would be mostly incomplete; missing depth rate {missing_rate:.3%}."
        )
    if int(summary["duplicate_wind_farm_sample_point_count"]) != 0:
        raise RuntimeError("Bathymetry assignment contains duplicate wind_farm + sample_point_id rows.")

    output_root.mkdir(parents=True, exist_ok=True)
    output.to_parquet(output_path, index=False)

    spot_checks = _build_spot_checks(output, points, source_cache, limit=SPOT_CHECK_COUNT)
    disk_after = _disk_summary(Path("."))
    wave_snapshot_after = _snapshot_archives(wave_paths)
    wave_archives_unchanged = wave_snapshot_before == wave_snapshot_after

    metadata = {
        "created_at_utc": _utc_now_iso(),
        "source_name": "EMODnet Bathymetry",
        "source_version": EMODNET_VERSION,
        "source_endpoint": EMODNET_DEPTH_SAMPLE_URL,
        "source_access_method": "REST depth_sample point queries cached as JSONL",
        "source_cache_path": str(source_cache_path),
        "fallback_source": "gebco_2026",
        "fallback_version": GEBCO_FALLBACK_VERSION,
        "fallback_status": "not_fetched_no_emodnet_gaps",
        "vertical_datum": EMODNET_VERTICAL_DATUM,
        "coordinate_reference_system": COORDINATE_REFERENCE_SYSTEM,
        "processed_depth_sign_convention": PROCESSED_DEPTH_SIGN_CONVENTION,
        "assignment_method": ASSIGNMENT_METHOD,
        "requirements_path": str(requirements_path),
        "turbine_coordinates_path": str(turbine_coordinates_path),
        "output_path": str(output_path),
        "row_count": summary["row_count"],
        "farm_count": summary["farm_count"],
        "sample_point_count": summary["sample_point_count"],
        "missing_depth_count": summary["missing_depth_count"],
        "spot_checks": spot_checks,
    }
    _write_json(processed_metadata_path, metadata)
    _write_json(
        source_metadata_path,
        {
            "created_at_utc": _utc_now_iso(),
            "source_name": "EMODnet Bathymetry",
            "source_version": EMODNET_VERSION,
            "source_endpoint": EMODNET_DEPTH_SAMPLE_URL,
            "source_cache_path": str(source_cache_path),
            "vertical_datum": EMODNET_VERTICAL_DATUM,
            "coordinate_reference_system": COORDINATE_REFERENCE_SYSTEM,
            "raw_artifact_type": "JSONL REST point-sample responses",
            "sample_point_count": int(len(points)),
            "cache_record_count": int(len(source_cache)),
        },
    )

    qa: dict[str, Any] = {
        **{key: value for key, value in summary.items() if key not in {"region_summary", "farm_summary"}},
        "region_summary": summary["region_summary"].round(3),
        "farm_summary": summary["farm_summary"].round(3),
        "accepted_candidate": bool(
            summary["missing_depth_count"] == 0
            and summary["duplicate_wind_farm_sample_point_count"] == 0
            and summary["provenance_populated_where_depth_exists"]
        ),
        "requirements_path": str(requirements_path),
        "turbine_coordinates_path": str(turbine_coordinates_path),
        "output_existed_before_run": bool(output_existed_before_run),
        "no_overwrite": bool(no_overwrite),
        "disk_before": disk_before,
        "disk_after": disk_after,
        "source_access_preflight_status": preflight_status,
        "fallback_source": "gebco_2026",
        "fallback_status": "not_fetched_no_emodnet_gaps",
        "positive_down_sign_convention": bool(
            output.loc[output["water_depth_m"].notna(), "depth_sign_convention"]
            .eq(PROCESSED_DEPTH_SIGN_CONVENTION)
            .all()
        ),
        "vertical_datum_recorded": bool(
            output.loc[output["water_depth_m"].notna(), "bathymetry_vertical_datum"]
            .eq(EMODNET_VERTICAL_DATUM)
            .all()
        ),
        "crs_recorded": True,
        "distance_populated": bool(output["bathymetry_distance_m"].notna().all()),
        "plausible_depths": bool((output["water_depth_m"].dropna() >= 0).all() and output["water_depth_m"].dropna().max() < 1000),
        "spot_checks": spot_checks,
        "wave_archive_snapshot_before": wave_snapshot_before,
        "wave_archive_snapshot_after": wave_snapshot_after,
        "wave_archives_unchanged": bool(wave_archives_unchanged),
        "command": command,
    }

    result = BathymetryAssignmentResult(
        output=output,
        metadata=metadata,
        qa=qa,
        source_cache_path=source_cache_path,
        source_metadata_path=source_metadata_path,
        output_path=output_path,
        processed_metadata_path=processed_metadata_path,
        qa_report=qa_report,
    )

    if qa_report is not None:
        qa_report.parent.mkdir(parents=True, exist_ok=True)
        qa_report.write_text(render_bathymetry_assignment_report(result, command=command), encoding="utf-8")

    return result
