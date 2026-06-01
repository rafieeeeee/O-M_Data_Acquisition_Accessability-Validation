"""Common wind-farm metocean requirement planning.

This module is product-agnostic. It derives the farm temporal and spatial
requirements once so regional product adapters can add only source-specific
coverage, variables, output paths, and request formatting.
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd


DEFAULT_ANALYSIS_DIR = Path("analysis/06_rq6_metocean_spatial_resolution")
DEFAULT_TURBINE_COORDINATES = Path("Data/Interim/European_Turbine_Coordinates.csv")
DEFAULT_EVENTS_PATH = Path("Data/Processed/analysis/om_event_table.parquet")
DEFAULT_STUDY_END_DATE = "2024-12-31"
DEFAULT_BUFFER_DEGREES = 0.25

COMMON_REQUIREMENTS_COLUMNS = [
    "wind_farm",
    "farm_id",
    "country",
    "operation_start_date",
    "study_end_date",
    "temporal_start",
    "temporal_end",
    "temporal_start_reason",
    "temporal_end_reason",
    "spatial_basis",
    "buffer_degrees",
    "min_lon",
    "max_lon",
    "min_lat",
    "max_lat",
    "sample_point_strategy",
    "sample_point_count",
    "review_required",
    "notes",
]


@dataclass(frozen=True)
class CommonRequirementPaths:
    csv: Path
    parquet: Path


def normalize_farm_name(value: Any) -> str:
    """Normalize farm labels across metadata, config, and event tables."""
    asciiish = unicodedata.normalize("NFKD", str(value)).encode("ascii", "ignore")
    text = asciiish.decode("ascii").casefold()
    return re.sub(r"[^a-z0-9]+", "", text)


def farm_slug(value: Any) -> str:
    """Build a stable partition-safe farm identifier."""
    asciiish = unicodedata.normalize("NFKD", str(value)).encode("ascii", "ignore")
    text = asciiish.decode("ascii")
    text = re.sub(r"[^A-Za-z0-9]+", "_", text).strip("_")
    return text or "unknown_farm"


def parse_date(value: Any) -> date | None:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    if not text:
        return None
    if re.fullmatch(r"\d{4}", text):
        return date(int(text), 1, 1)
    if re.fullmatch(r"\d{4}-\d{2}", text):
        return datetime.strptime(f"{text}-01", "%Y-%m-%d").date()
    return datetime.strptime(text[:10], "%Y-%m-%d").date()


def date_text(value: date | None) -> str | None:
    return value.isoformat() if value is not None else None


def _coerce_year(value: Any) -> int | None:
    if value is None or pd.isna(value):
        return None
    return int(value)


def _load_events(events_path: Path | None) -> pd.DataFrame:
    if events_path is None or not events_path.exists():
        return pd.DataFrame()

    events = pd.read_parquet(events_path)
    required = {"wind_farm", "start_utc"}
    missing = sorted(required - set(events.columns))
    if missing:
        raise ValueError(f"Event table is missing required columns: {missing}")

    events = events.copy()
    events["start_utc"] = pd.to_datetime(events["start_utc"], utc=True)
    events["event_year"] = events["start_utc"].dt.year
    events["normalized_farm"] = events["wind_farm"].map(normalize_farm_name)
    return events


def _event_summary(events: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "event_wind_farm",
        "normalized_farm",
        "first_ais_event_year_if_available",
        "last_ais_event_year_if_available",
        "ais_event_count_if_available",
        "farm_commissioning_year_if_available",
        "event_min_lon",
        "event_max_lon",
        "event_min_lat",
        "event_max_lat",
    ]
    if events.empty:
        return pd.DataFrame(columns=columns)

    aggregations: dict[str, tuple[str, str]] = {
        "first_ais_event_year_if_available": ("event_year", "min"),
        "last_ais_event_year_if_available": ("event_year", "max"),
        "ais_event_count_if_available": ("wind_farm", "size"),
    }
    if "farm_commissioning_year" in events.columns:
        aggregations["farm_commissioning_year_if_available"] = (
            "farm_commissioning_year",
            "min",
        )
    if {"centroid_lon", "centroid_lat"}.issubset(events.columns):
        aggregations.update(
            {
                "event_min_lon": ("centroid_lon", "min"),
                "event_max_lon": ("centroid_lon", "max"),
                "event_min_lat": ("centroid_lat", "min"),
                "event_max_lat": ("centroid_lat", "max"),
            }
        )

    summary = (
        events.groupby("wind_farm", dropna=True)
        .agg(**aggregations)
        .reset_index()
        .rename(columns={"wind_farm": "event_wind_farm"})
    )
    summary["normalized_farm"] = summary["event_wind_farm"].map(normalize_farm_name)
    for column in columns:
        if column not in summary.columns:
            summary[column] = pd.NA
    return summary[columns]


def _operation_start_from_turbines(farm_turbines: pd.DataFrame) -> tuple[date | None, str]:
    if "operation_start_date" in farm_turbines.columns:
        dates = [
            parsed
            for parsed in (parse_date(value) for value in farm_turbines["operation_start_date"].dropna().unique())
            if parsed is not None
        ]
        if dates:
            return min(dates), "operation_start_date"

    if "commissioning_date" in farm_turbines.columns:
        dates = [
            parsed
            for parsed in (parse_date(value) for value in farm_turbines["commissioning_date"].dropna().unique())
            if parsed is not None
        ]
        if dates:
            return min(dates), "commissioning_date"

    return None, "missing_operation_metadata"


def _common_rows_from_turbines(
    turbines: pd.DataFrame,
    event_lookup: dict[str, dict[str, Any]],
    study_end: date,
    buffer_degrees: float,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    required = {"wind_farm", "latitude", "longitude"}
    missing = sorted(required - set(turbines.columns))
    if missing:
        raise ValueError(f"Turbine coordinate file is missing required columns: {missing}")

    for wind_farm, farm_turbines in turbines.groupby("wind_farm", dropna=True):
        normalized = normalize_farm_name(wind_farm)
        event_info = event_lookup.get(normalized, {})
        operation_start, operation_reason = _operation_start_from_turbines(farm_turbines)
        fallback_notes: list[str] = []

        if operation_start is None:
            event_commissioning_year = _coerce_year(
                event_info.get("farm_commissioning_year_if_available")
            )
            if event_commissioning_year is not None:
                operation_start = date(event_commissioning_year, 1, 1)
                operation_reason = "event_table_farm_commissioning_year"
            else:
                first_ais_year = _coerce_year(
                    event_info.get("first_ais_event_year_if_available")
                )
                if first_ais_year is not None:
                    operation_start = date(first_ais_year, 1, 1)
                    operation_reason = "fallback_first_ais_year"
                    fallback_notes.append(
                        "AIS year used only because no operation or commissioning metadata was available."
                    )

        country = None
        if "country" in farm_turbines.columns:
            countries = sorted(str(value) for value in farm_turbines["country"].dropna().unique())
            country = countries[0] if countries else None

        min_lon = round(float(farm_turbines["longitude"].min()) - buffer_degrees, 6)
        max_lon = round(float(farm_turbines["longitude"].max()) + buffer_degrees, 6)
        min_lat = round(float(farm_turbines["latitude"].min()) - buffer_degrees, 6)
        max_lat = round(float(farm_turbines["latitude"].max()) + buffer_degrees, 6)

        review_required = operation_start is None or operation_start > study_end
        notes = []
        if operation_start is None:
            notes.append("Missing operation/commissioning metadata and AIS fallback evidence.")
        if operation_start is not None and operation_start > study_end:
            notes.append("Operation start is after configured study end date.")
        notes.extend(fallback_notes)

        rows.append(
            {
                "wind_farm": wind_farm,
                "farm_id": farm_slug(wind_farm),
                "country": country,
                "operation_start_date": date_text(operation_start),
                "study_end_date": date_text(study_end),
                "temporal_start": date_text(operation_start),
                "temporal_end": date_text(study_end) if operation_start is not None and operation_start <= study_end else None,
                "temporal_start_reason": operation_reason,
                "temporal_end_reason": "configured_study_end_date",
                "spatial_basis": "turbine_footprint",
                "buffer_degrees": buffer_degrees,
                "min_lon": min_lon,
                "max_lon": max_lon,
                "min_lat": min_lat,
                "max_lat": max_lat,
                "sample_point_strategy": "farm_centroid_and_turbines",
                "sample_point_count": int(len(farm_turbines) + 1),
                "review_required": bool(review_required),
                "notes": " ".join(notes),
                "first_ais_event_year_if_available": event_info.get(
                    "first_ais_event_year_if_available"
                ),
                "last_ais_event_year_if_available": event_info.get(
                    "last_ais_event_year_if_available"
                ),
                "ais_event_count_if_available": int(
                    event_info.get("ais_event_count_if_available", 0) or 0
                ),
            }
        )
    return rows


def _common_rows_from_ais_only(
    events: pd.DataFrame,
    known_normalized_farms: set[str],
    study_end: date,
    buffer_degrees: float,
) -> list[dict[str, Any]]:
    if events.empty or not {"centroid_lon", "centroid_lat"}.issubset(events.columns):
        return []

    rows: list[dict[str, Any]] = []
    for wind_farm, farm_events in events.groupby("wind_farm", dropna=True):
        normalized = normalize_farm_name(wind_farm)
        if normalized in known_normalized_farms:
            continue
        first_ais_year = int(farm_events["event_year"].min())
        temporal_start = date(first_ais_year, 1, 1)
        notes = (
            "AIS fallback row: no central turbine/farm geometry or operation metadata "
            "was available."
        )
        rows.append(
            {
                "wind_farm": wind_farm,
                "farm_id": farm_slug(wind_farm),
                "country": None,
                "operation_start_date": date_text(temporal_start),
                "study_end_date": date_text(study_end),
                "temporal_start": date_text(temporal_start),
                "temporal_end": date_text(study_end) if temporal_start <= study_end else None,
                "temporal_start_reason": "fallback_first_ais_year",
                "temporal_end_reason": "configured_study_end_date",
                "spatial_basis": "fallback_ais_centroids",
                "buffer_degrees": buffer_degrees,
                "min_lon": round(float(farm_events["centroid_lon"].min()) - buffer_degrees, 6),
                "max_lon": round(float(farm_events["centroid_lon"].max()) + buffer_degrees, 6),
                "min_lat": round(float(farm_events["centroid_lat"].min()) - buffer_degrees, 6),
                "max_lat": round(float(farm_events["centroid_lat"].max()) + buffer_degrees, 6),
                "sample_point_strategy": "farm_centroid_from_ais_fallback",
                "sample_point_count": 1,
                "review_required": True,
                "notes": notes,
                "first_ais_event_year_if_available": first_ais_year,
                "last_ais_event_year_if_available": int(farm_events["event_year"].max()),
                "ais_event_count_if_available": int(len(farm_events)),
            }
        )
    return rows


def build_common_metocean_requirements(
    turbine_coordinates: Path = DEFAULT_TURBINE_COORDINATES,
    events_path: Path | None = DEFAULT_EVENTS_PATH,
    study_end_date: str | date = DEFAULT_STUDY_END_DATE,
    buffer_degrees: float = DEFAULT_BUFFER_DEGREES,
    include_ais_only_farms: bool = True,
) -> pd.DataFrame:
    """Build the canonical product-agnostic farm requirements table."""
    study_end = parse_date(study_end_date)
    if study_end is None:
        raise ValueError("study_end_date must be set, e.g. 2024-12-31")

    if not turbine_coordinates.exists():
        raise FileNotFoundError(f"Turbine coordinate file not found: {turbine_coordinates}")
    turbines = pd.read_csv(turbine_coordinates)
    events = _load_events(events_path)
    event_summary = _event_summary(events)
    event_lookup = event_summary.set_index("normalized_farm").to_dict("index")

    rows = _common_rows_from_turbines(
        turbines=turbines,
        event_lookup=event_lookup,
        study_end=study_end,
        buffer_degrees=float(buffer_degrees),
    )
    if include_ais_only_farms:
        known = {normalize_farm_name(name) for name in turbines["wind_farm"].dropna().unique()}
        rows.extend(
            _common_rows_from_ais_only(
                events=events,
                known_normalized_farms=known,
                study_end=study_end,
                buffer_degrees=float(buffer_degrees),
            )
        )

    requirements = pd.DataFrame(rows)
    for column in COMMON_REQUIREMENTS_COLUMNS:
        if column not in requirements.columns:
            requirements[column] = pd.NA
    return requirements.sort_values("wind_farm").reset_index(drop=True)


def common_requirement_paths(
    analysis_dir: Path = DEFAULT_ANALYSIS_DIR,
) -> CommonRequirementPaths:
    return CommonRequirementPaths(
        csv=analysis_dir / "common_metocean_farm_requirements.csv",
        parquet=analysis_dir / "common_metocean_farm_requirements.parquet",
    )


def write_common_metocean_requirements(
    requirements: pd.DataFrame,
    analysis_dir: Path = DEFAULT_ANALYSIS_DIR,
) -> CommonRequirementPaths:
    """Write canonical common requirements as CSV and parquet."""
    paths = common_requirement_paths(analysis_dir)
    paths.csv.parent.mkdir(parents=True, exist_ok=True)
    requirements.to_csv(paths.csv, index=False)
    requirements.to_parquet(paths.parquet, index=False)
    return paths


def load_common_metocean_requirements(path: Path) -> pd.DataFrame:
    """Load common requirements from CSV or parquet."""
    if not path.exists():
        raise FileNotFoundError(f"Common requirements table not found: {path}")
    if path.suffix.lower() == ".parquet":
        return pd.read_parquet(path)
    return pd.read_csv(path)
