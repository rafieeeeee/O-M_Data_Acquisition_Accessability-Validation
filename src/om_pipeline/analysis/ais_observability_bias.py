"""AIS receiver/source observability-bias audit.

This module builds a read-only, audit-first layer for testing whether AIS event
rates may be geographically skewed by source coverage or detectability. It does
not rerun AIS extraction and does not interpret AIS visits as confirmed failures.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
import re
from typing import Any

import numpy as np
import pandas as pd

from om_pipeline.analysis.evidence_readiness import (
    MISSING_SOURCE_STATUS,
    OBSERVED_STATUSES,
    build_farm_metadata,
    contains_ais_only_failure_rate_claim,
)


REQUIRED_EXTERNAL_PROVENANCE_COLUMNS = frozenset(
    {
        "source_name",
        "source_url",
        "access_date",
        "coordinate_reference_system",
        "join_assumptions",
    }
)

DIRECT_RECEIVER_PATTERNS = (
    "receiver",
    "receiver id",
    "receiver lat",
    "receiver lon",
    "ais station",
    "base station",
    "station id",
    "station lat",
    "station lon",
    "terrestrial",
    "satellite",
)
SOURCE_CHANNEL_PATTERNS = (
    "data source type",
    "source file name",
    "source file size",
    "source file modified",
    "source provider",
    "source file",
    "provider",
    "channel",
)
WEAK_PROXY_PATTERNS = (
    "mmsi",
    "ship type",
    "type of mobile",
    "type of position fixing device",
    "latitude",
    "longitude",
    "farm_centroid",
    "country",
    "sea_basin",
    "water_depth",
    "vessel_length",
    "vessel_beam",
    "access_technology",
    "registry_source",
)
VESSEL_METADATA_COLUMNS = (
    "vessel_length_m",
    "vessel_beam_m",
    "vessel_draft_m",
    "vessel_category_enriched",
    "access_technology",
    "registry_source",
    "registry_match_confidence",
)
RAW_BASE_STATION_COLUMNS = (
    "Timestamp",
    "Type of mobile",
    "MMSI",
    "Latitude",
    "Longitude",
    "Type of position fixing device",
    "Data source type",
)
BASE_STATION_CATALOGUE_COLUMNS = (
    "base_station_mmsi",
    "base_station_latitude",
    "base_station_longitude",
    "timestamp_first_seen",
    "timestamp_last_seen",
    "observation_count",
    "position_fixing_device_values",
    "data_source_type_values",
    "source_file_count",
    "source_files_seen",
    "source_months_seen",
    "coordinate_reference_system",
    "evidence_classification",
    "receiver_assignment_available_flag",
    "provenance_note",
)
BASE_STATION_DISTANCE_STRATA = ("country", "sea_basin", "year", "month")
BASE_STATION_DISTANCE_DIAGNOSTIC_COLUMNS = (
    "country",
    "sea_basin",
    "year",
    "month",
    "distance_bin",
    "distance_bin_order",
    "distance_comparison_status",
    "farm_month_count",
    "farm_count",
    "median_distance_to_nearest_observed_base_station_km",
    "turbine_month_denominator",
    "capacity_mw_month_denominator",
    "source_clean_rows_total",
    "clean_rows_per_turbine_month",
    "clean_rows_per_mw_month",
    "success_no_ais_in_bbox_month_count",
    "observed_zero_month_count",
    "observed_zero_rate",
    "ais_dwell_event_count_total",
    "dwell_events_per_turbine_month",
    "dwell_events_per_mw_month",
    "tier_a_count_total",
    "tier_a_per_turbine_month",
    "tier_a_per_mw_month",
    "tier_b_count_total",
    "tier_b_per_turbine_month",
    "tier_b_per_mw_month",
    "tier_c_count_total",
    "tier_c_per_turbine_month",
    "tier_c_per_mw_month",
    "tier_d_count_total",
    "tier_d_per_turbine_month",
    "tier_d_per_mw_month",
)
BASE_STATION_DISTANCE_GRADIENT_COLUMNS = (
    "country",
    "sea_basin",
    "year",
    "month",
    "comparison_method",
    "eligible_distance_bin_count",
    "farm_month_count",
    "farm_count",
    "near_distance_bin",
    "far_distance_bin",
    "near_median_distance_km",
    "far_median_distance_km",
    "clean_rows_per_turbine_month_far_near_ratio",
    "clean_rows_per_mw_month_far_near_ratio",
    "dwell_events_per_turbine_month_far_near_ratio",
    "dwell_events_per_mw_month_far_near_ratio",
    "tier_a_per_turbine_month_far_near_ratio",
    "tier_b_per_turbine_month_far_near_ratio",
    "tier_c_per_turbine_month_far_near_ratio",
    "tier_d_per_turbine_month_far_near_ratio",
    "observed_zero_rate_far_minus_near",
    "source_intensity_declines_with_distance",
    "observed_zero_increases_with_distance",
    "downstream_proxy_declines_with_distance",
    "diagnostic_class",
    "interpretation_guardrail",
)


@dataclass(frozen=True)
class AisObservabilityBiasOutputs:
    """Paths and summary values written by the observability-bias builder."""

    processed_output_dir: Path
    report_output_dir: Path
    files: dict[str, Path]
    validation: dict[str, Any]


def _jsonable(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        return float(value)
    if isinstance(value, np.bool_):
        return bool(value)
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


def _require_columns(df: pd.DataFrame, required: set[str], context: str) -> None:
    missing = sorted(required - set(df.columns))
    if missing:
        raise ValueError(f"{context} is missing required columns: {missing}")


def _safe_share(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    numerator = pd.to_numeric(numerator, errors="coerce")
    denominator = pd.to_numeric(denominator, errors="coerce")
    with np.errstate(divide="ignore", invalid="ignore"):
        result = numerator / denominator
    return result.where(denominator > 0)


def _safe_scalar_ratio(numerator: Any, denominator: Any) -> float:
    numerator_value = pd.to_numeric(pd.Series([numerator]), errors="coerce").iloc[0]
    denominator_value = pd.to_numeric(pd.Series([denominator]), errors="coerce").iloc[0]
    if pd.isna(numerator_value) or pd.isna(denominator_value) or denominator_value <= 0:
        return float("nan")
    return float(numerator_value / denominator_value)


def _first_non_null(series: pd.Series) -> Any:
    non_null = series.dropna()
    if non_null.empty:
        return pd.NA
    return non_null.iloc[0]


def _join_unique(values: pd.Series) -> str:
    unique = sorted({str(value) for value in values.dropna() if str(value).strip()})
    return ";".join(unique)


def _month_label_from_parts(year: pd.Series, month: pd.Series) -> pd.Series:
    dates = pd.to_datetime(
        {"year": year.astype("Int64"), "month": month.astype("Int64"), "day": 1},
        errors="coerce",
    )
    return dates.dt.to_period("M").astype(str)


def _event_month(series: pd.Series) -> pd.Series:
    parsed = pd.to_datetime(series, errors="coerce", utc=True)
    return parsed.dt.tz_convert(None).dt.to_period("M").astype(str)


def _parse_month_label(series: pd.Series) -> pd.Series:
    parsed = pd.to_datetime(series.astype("string"), errors="coerce")
    return parsed.dt.to_period("M").dt.to_timestamp()


def _operational_phase(month: pd.Series, commissioning: pd.Series, steady: pd.Series) -> pd.Series:
    month_ts = _parse_month_label(month)
    commissioning_ts = _parse_month_label(commissioning)
    steady_ts = _parse_month_label(steady)
    phase = pd.Series("unknown_phase", index=month.index, dtype="string")
    known = month_ts.notna() & commissioning_ts.notna() & steady_ts.notna()
    phase.loc[known & (month_ts < commissioning_ts)] = "pre_operational"
    phase.loc[known & (month_ts >= commissioning_ts) & (month_ts < steady_ts)] = (
        "commissioning_ramp_up"
    )
    phase.loc[known & (month_ts >= steady_ts)] = "steady_operational"
    return phase


def _haversine_km(
    lat1: pd.Series,
    lon1: pd.Series,
    lat2: pd.Series,
    lon2: pd.Series,
) -> pd.Series:
    radius_km = 6371.0088
    lat1_rad = np.radians(pd.to_numeric(lat1, errors="coerce"))
    lon1_rad = np.radians(pd.to_numeric(lon1, errors="coerce"))
    lat2_rad = np.radians(pd.to_numeric(lat2, errors="coerce"))
    lon2_rad = np.radians(pd.to_numeric(lon2, errors="coerce"))
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad
    a = np.sin(dlat / 2) ** 2 + np.cos(lat1_rad) * np.cos(lat2_rad) * np.sin(dlon / 2) ** 2
    return radius_km * 2 * np.arcsin(np.sqrt(a))


def _normalize_raw_ais_column_name(column_name: Any) -> str:
    return str(column_name).strip().lstrip("# ").strip()


def _parse_decimal_number(series: pd.Series) -> pd.Series:
    return pd.to_numeric(
        series.astype("string").str.strip().str.replace(",", ".", regex=False),
        errors="coerce",
    )


def _parse_decimal_value(value: Any) -> float | None:
    try:
        parsed = float(str(value).strip().replace(",", "."))
    except (TypeError, ValueError):
        return None
    if not np.isfinite(parsed):
        return None
    return parsed


def _source_month_from_ais_filename(path: Path) -> str | pd.NA:
    match = re.search(r"_(\d{4})_(\d{2})_", path.name)
    if not match:
        return pd.NA
    return f"{match.group(1)}-{match.group(2)}"


def classify_observability_field(column_name: str) -> tuple[str, str, str]:
    """Classify one schema field into the evidence-tier ladder."""
    lowered = column_name.lower().replace("_", " ").strip("# ")
    if any(pattern in lowered for pattern in DIRECT_RECEIVER_PATTERNS):
        return (
            "Tier 1",
            "direct_receiver_evidence",
            "Potential direct receiver/source geometry; inspect values before using.",
        )
    if any(pattern in lowered for pattern in SOURCE_CHANNEL_PATTERNS):
        return (
            "Tier 2",
            "source_channel_evidence",
            "Source/channel metadata; supports source availability but not receiver distance.",
        )
    if any(pattern in lowered for pattern in WEAK_PROXY_PATTERNS):
        return (
            "Tier 3/4",
            "proxy_evidence",
            "Geographic, vessel, or downstream proxy; cannot confirm receiver-distance causality.",
        )
    return ("unclassified", "not_observability_metadata", "Not used as observability metadata.")


def build_receiver_metadata_inventory(
    schema_sources: dict[str, list[str]],
    *,
    external_reference_used: bool = False,
    external_reference_provenance: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Build a field-level receiver/source metadata inventory from table schemas."""
    if external_reference_used:
        validate_external_reference_provenance(external_reference_provenance)

    rows: list[dict[str, Any]] = []
    for source_name, columns in sorted(schema_sources.items()):
        for column in columns:
            tier, category, note = classify_observability_field(column)
            rows.append(
                {
                    "source_name": source_name,
                    "field_name": column,
                    "evidence_tier": tier,
                    "evidence_category": category,
                    "is_direct_receiver_evidence": category == "direct_receiver_evidence",
                    "is_source_channel_evidence": category == "source_channel_evidence",
                    "is_proxy_evidence": category == "proxy_evidence",
                    "external_reference_used": False,
                    "source_url": pd.NA,
                    "access_date": pd.NA,
                    "coordinate_reference_system": pd.NA,
                    "join_assumptions": pd.NA,
                    "notes": note,
                }
            )

    if external_reference_used and external_reference_provenance is not None:
        for _, row in external_reference_provenance.iterrows():
            rows.append(
                {
                    "source_name": row["source_name"],
                    "field_name": "external_receiver_reference",
                    "evidence_tier": "Tier 1 external reference",
                    "evidence_category": "direct_receiver_evidence",
                    "is_direct_receiver_evidence": True,
                    "is_source_channel_evidence": False,
                    "is_proxy_evidence": False,
                    "external_reference_used": True,
                    "source_url": row["source_url"],
                    "access_date": row["access_date"],
                    "coordinate_reference_system": row["coordinate_reference_system"],
                    "join_assumptions": row["join_assumptions"],
                    "notes": "External receiver reference; provenance required and interpretation remains supporting context.",
                }
            )

    columns = [
        "source_name",
        "field_name",
        "evidence_tier",
        "evidence_category",
        "is_direct_receiver_evidence",
        "is_source_channel_evidence",
        "is_proxy_evidence",
        "external_reference_used",
        "source_url",
        "access_date",
        "coordinate_reference_system",
        "join_assumptions",
        "notes",
    ]
    return pd.DataFrame(rows, columns=columns)


def validate_external_reference_provenance(provenance: pd.DataFrame | None) -> None:
    """Require explicit provenance before external receiver/coastline data is used."""
    if provenance is None or provenance.empty:
        raise ValueError("External receiver/coastline references require provenance rows.")
    missing = REQUIRED_EXTERNAL_PROVENANCE_COLUMNS - set(provenance.columns)
    if missing:
        raise ValueError(f"External reference provenance is missing columns: {sorted(missing)}")
    required = list(REQUIRED_EXTERNAL_PROVENANCE_COLUMNS)
    incomplete = provenance[required].isna() | provenance[required].astype("string").eq("")
    if bool(incomplete.any(axis=None)):
        raise ValueError("External reference provenance contains blank required values.")


def read_raw_ais_schema_inventory(raw_ais_root: Path, pattern: str) -> tuple[dict[str, list[str]], dict[str, Any]]:
    """Read raw AIS CSV headers only and return unique schema metadata."""
    schema_sources: dict[str, list[str]] = {}
    files = sorted(raw_ais_root.glob(pattern)) if raw_ais_root.exists() else []
    unique_schemas: dict[tuple[str, ...], int] = {}
    data_source_type_file_count = 0
    for path in files:
        try:
            header = pd.read_csv(path, nrows=0).columns.tolist()
        except Exception:
            continue
        normalized = tuple(column.strip() for column in header)
        unique_schemas[normalized] = unique_schemas.get(normalized, 0) + 1
        if any(column.strip().lstrip("# ").lower() == "data source type" for column in header):
            data_source_type_file_count += 1

    for idx, (schema, count) in enumerate(sorted(unique_schemas.items(), key=lambda item: item[0]), start=1):
        schema_sources[f"raw_ais_schema_{idx}_files_{count}"] = list(schema)

    summary = {
        "raw_ais_file_count": len(files),
        "raw_ais_unique_schema_count": len(unique_schemas),
        "raw_ais_data_source_type_file_count": data_source_type_file_count,
    }
    return schema_sources, summary


def extract_ais_base_station_geometry_catalogue(
    raw_ais_root: Path,
    pattern: str,
    *,
    chunksize: int = 250_000,
) -> pd.DataFrame:
    """Extract observed AIS base-station message geometry from raw AIS CSV rows.

    Danish AIS CSVs can contain `Type of mobile = Base Station` messages with
    station MMSI and station coordinates. These rows provide direct AIS
    base-station geometry reference, but they do not identify which receiver
    captured any vessel message.
    """
    del chunksize
    files = sorted(raw_ais_root.glob(pattern)) if raw_ais_root.exists() else []
    stations: dict[str, dict[str, Any]] = {}
    required = {"Timestamp", "Type of mobile", "MMSI", "Latitude", "Longitude"}

    for path in files:
        try:
            handle = path.open("r", encoding="utf-8", errors="replace", newline="")
        except OSError:
            continue
        with handle:
            header_line = handle.readline()
            if not header_line:
                continue
            try:
                header = next(csv.reader([header_line]))
            except csv.Error:
                continue
            normalized_to_index: dict[str, int] = {}
            for idx, column in enumerate(header):
                normalized_to_index.setdefault(_normalize_raw_ais_column_name(column), idx)
            if not required.issubset(normalized_to_index):
                continue

            source_month = _source_month_from_ais_filename(path)
            for line in handle:
                if "Base Station" not in line:
                    continue
                try:
                    row = next(csv.reader([line]))
                except csv.Error:
                    continue
                if len(row) < len(header):
                    continue
                type_of_mobile = row[normalized_to_index["Type of mobile"]].strip()
                if type_of_mobile.casefold() != "base station":
                    continue
                mmsi = row[normalized_to_index["MMSI"]].strip()
                latitude = _parse_decimal_value(row[normalized_to_index["Latitude"]])
                longitude = _parse_decimal_value(row[normalized_to_index["Longitude"]])
                if not mmsi or latitude is None or longitude is None:
                    continue
                timestamp = pd.to_datetime(
                    row[normalized_to_index["Timestamp"]],
                    errors="coerce",
                    dayfirst=True,
                    utc=True,
                )
                station = stations.setdefault(
                    mmsi,
                    {
                        "base_station_mmsi": mmsi,
                        "latitude_sum": 0.0,
                        "longitude_sum": 0.0,
                        "coordinate_count": 0,
                        "timestamp_first_seen": None,
                        "timestamp_last_seen": None,
                        "observation_count": 0,
                        "position_fixing_device_values": set(),
                        "data_source_type_values": set(),
                        "source_files_seen": set(),
                        "source_months_seen": set(),
                    },
                )
                station["latitude_sum"] += latitude
                station["longitude_sum"] += longitude
                station["coordinate_count"] += 1
                station["observation_count"] += 1
                if pd.notna(timestamp):
                    if station["timestamp_first_seen"] is None or timestamp < station["timestamp_first_seen"]:
                        station["timestamp_first_seen"] = timestamp
                    if station["timestamp_last_seen"] is None or timestamp > station["timestamp_last_seen"]:
                        station["timestamp_last_seen"] = timestamp
                if "Type of position fixing device" in normalized_to_index:
                    value = row[normalized_to_index["Type of position fixing device"]].strip()
                    if value:
                        station["position_fixing_device_values"].add(value)
                if "Data source type" in normalized_to_index:
                    value = row[normalized_to_index["Data source type"]].strip()
                    if value:
                        station["data_source_type_values"].add(value)
                station["source_files_seen"].add(path.name)
                if not pd.isna(source_month):
                    station["source_months_seen"].add(str(source_month))

    if not stations:
        return pd.DataFrame(columns=BASE_STATION_CATALOGUE_COLUMNS)

    rows: list[dict[str, Any]] = []
    for station in stations.values():
        coordinate_count = station["coordinate_count"]
        first_seen = station["timestamp_first_seen"]
        last_seen = station["timestamp_last_seen"]
        rows.append(
            {
                "base_station_mmsi": station["base_station_mmsi"],
                "base_station_latitude": station["latitude_sum"] / coordinate_count,
                "base_station_longitude": station["longitude_sum"] / coordinate_count,
                "timestamp_first_seen": (
                    first_seen.strftime("%Y-%m-%dT%H:%M:%SZ") if first_seen is not None else pd.NA
                ),
                "timestamp_last_seen": (
                    last_seen.strftime("%Y-%m-%dT%H:%M:%SZ") if last_seen is not None else pd.NA
                ),
                "observation_count": station["observation_count"],
                "position_fixing_device_values": ";".join(
                    sorted(station["position_fixing_device_values"])
                ),
                "data_source_type_values": ";".join(sorted(station["data_source_type_values"])),
                "source_file_count": len(station["source_files_seen"]),
                "source_files_seen": ";".join(sorted(station["source_files_seen"])),
                "source_months_seen": ";".join(sorted(station["source_months_seen"])),
                "coordinate_reference_system": "WGS84_AIS_latitude_longitude",
                "evidence_classification": "direct_ais_base_station_geometry_reference",
                "receiver_assignment_available_flag": False,
                "provenance_note": "raw_danish_ais_base_station_messages",
            }
        )
    catalogue = pd.DataFrame(rows, columns=BASE_STATION_CATALOGUE_COLUMNS)
    return catalogue[list(BASE_STATION_CATALOGUE_COLUMNS)].sort_values(
        ["base_station_mmsi"]
    ).reset_index(drop=True)


def build_farm_controls(
    turbine_exposure: pd.DataFrame | None,
    farm_metadata: pd.DataFrame,
) -> pd.DataFrame:
    """Aggregate farm-level controls used for source-aware comparisons."""
    controls = farm_metadata.copy()
    if turbine_exposure is None or turbine_exposure.empty or "farm_id" not in turbine_exposure.columns:
        controls["farm_capacity_mw"] = pd.NA
        controls["farm_centroid_latitude"] = pd.NA
        controls["farm_centroid_longitude"] = pd.NA
        controls["water_depth_m"] = pd.NA
        return controls

    exposure = turbine_exposure.copy()
    agg_spec: dict[str, Any] = {}
    if "rated_capacity_mw" in exposure.columns:
        agg_spec["farm_capacity_mw"] = ("rated_capacity_mw", "sum")
    elif "assigned_turbine_rated_power" in exposure.columns:
        agg_spec["farm_capacity_mw"] = ("assigned_turbine_rated_power", "sum")
    if "farm_centroid_latitude" in exposure.columns:
        agg_spec["farm_centroid_latitude"] = ("farm_centroid_latitude", _first_non_null)
    elif "latitude" in exposure.columns:
        agg_spec["farm_centroid_latitude"] = ("latitude", "mean")
    if "farm_centroid_longitude" in exposure.columns:
        agg_spec["farm_centroid_longitude"] = ("farm_centroid_longitude", _first_non_null)
    elif "longitude" in exposure.columns:
        agg_spec["farm_centroid_longitude"] = ("longitude", "mean")
    if "water_depth_m" in exposure.columns:
        agg_spec["water_depth_m"] = ("water_depth_m", "median")
    if "turbine_id" in exposure.columns:
        agg_spec["turbine_count_from_exposure"] = ("turbine_id", "nunique")

    if agg_spec:
        aggregate = exposure.groupby("farm_id", dropna=False).agg(**agg_spec).reset_index()
        controls = controls.merge(aggregate, on="farm_id", how="left")

    if "turbine_count_from_exposure" in controls.columns:
        controls["turbine_count"] = controls.get("turbine_count", controls["turbine_count_from_exposure"])
        controls["turbine_count"] = controls["turbine_count"].fillna(
            controls["turbine_count_from_exposure"]
        )
        controls = controls.drop(columns=["turbine_count_from_exposure"])

    for column in [
        "farm_capacity_mw",
        "farm_centroid_latitude",
        "farm_centroid_longitude",
        "water_depth_m",
    ]:
        if column not in controls.columns:
            controls[column] = pd.NA
    return controls


def add_external_receiver_distances(
    farm_controls: pd.DataFrame,
    external_receiver_reference: pd.DataFrame | None,
    external_reference_provenance: pd.DataFrame | None,
) -> pd.DataFrame:
    """Attach nearest external receiver distance when a provenanced reference exists."""
    output = farm_controls.copy()
    output["external_receiver_reference_used"] = False
    output["nearest_external_receiver_id"] = pd.NA
    output["nearest_external_receiver_distance_km"] = pd.NA

    if external_receiver_reference is None or external_receiver_reference.empty:
        return output

    validate_external_reference_provenance(external_reference_provenance)
    required = {"receiver_id", "receiver_latitude", "receiver_longitude"}
    _require_columns(external_receiver_reference, required, "external receiver reference")
    if not {"farm_centroid_latitude", "farm_centroid_longitude"}.issubset(output.columns):
        return output

    receivers = external_receiver_reference.dropna(
        subset=["receiver_id", "receiver_latitude", "receiver_longitude"]
    ).copy()
    if receivers.empty:
        return output

    receiver_ids: list[Any] = []
    receiver_distances: list[Any] = []
    for _, farm in output.iterrows():
        if pd.isna(farm.get("farm_centroid_latitude")) or pd.isna(farm.get("farm_centroid_longitude")):
            receiver_ids.append(pd.NA)
            receiver_distances.append(pd.NA)
            continue
        distances = _haversine_km(
            pd.Series([farm["farm_centroid_latitude"]] * len(receivers)),
            pd.Series([farm["farm_centroid_longitude"]] * len(receivers)),
            receivers["receiver_latitude"],
            receivers["receiver_longitude"],
        )
        nearest_idx = distances.astype(float).idxmin()
        receiver_ids.append(receivers.loc[nearest_idx, "receiver_id"])
        receiver_distances.append(float(distances.loc[nearest_idx]))

    output["external_receiver_reference_used"] = True
    output["nearest_external_receiver_id"] = receiver_ids
    output["nearest_external_receiver_distance_km"] = receiver_distances
    return output


def add_observed_base_station_distances(
    farm_controls: pd.DataFrame,
    base_station_catalogue: pd.DataFrame | None,
) -> pd.DataFrame:
    """Attach nearest observed AIS base-station geometry controls by farm.

    The nearest station is a source-geometry control only. It is not evidence
    that the station received any vessel message.
    """
    output = farm_controls.copy()
    output["base_station_geometry_available_flag"] = False
    output["nearest_observed_base_station_mmsi"] = pd.NA
    output["distance_to_nearest_observed_base_station_km"] = pd.NA

    if base_station_catalogue is None or base_station_catalogue.empty:
        return output
    required = {"base_station_mmsi", "base_station_latitude", "base_station_longitude"}
    _require_columns(base_station_catalogue, required, "AIS base-station catalogue")
    if not {"farm_centroid_latitude", "farm_centroid_longitude"}.issubset(output.columns):
        return output

    stations = base_station_catalogue.dropna(
        subset=["base_station_mmsi", "base_station_latitude", "base_station_longitude"]
    ).copy()
    if stations.empty:
        return output

    station_ids: list[Any] = []
    station_distances: list[Any] = []
    for _, farm in output.iterrows():
        if pd.isna(farm.get("farm_centroid_latitude")) or pd.isna(farm.get("farm_centroid_longitude")):
            station_ids.append(pd.NA)
            station_distances.append(pd.NA)
            continue
        distances = _haversine_km(
            pd.Series([farm["farm_centroid_latitude"]] * len(stations)),
            pd.Series([farm["farm_centroid_longitude"]] * len(stations)),
            stations["base_station_latitude"],
            stations["base_station_longitude"],
        )
        nearest_idx = distances.astype(float).idxmin()
        station_ids.append(stations.loc[nearest_idx, "base_station_mmsi"])
        station_distances.append(float(distances.loc[nearest_idx]))

    output["base_station_geometry_available_flag"] = True
    output["nearest_observed_base_station_mmsi"] = station_ids
    output["distance_to_nearest_observed_base_station_km"] = station_distances
    return output


def aggregate_dwell_observability(dwell: pd.DataFrame | None) -> pd.DataFrame:
    """Aggregate downstream dwell/vessel observability proxies by farm-month."""
    if dwell is None or dwell.empty:
        return pd.DataFrame(columns=["farm_id", "month"])
    _require_columns(dwell, {"farm_id", "start_utc"}, "dwell features")
    frame = dwell.copy()
    frame["month"] = _event_month(frame["start_utc"])
    if "mmsi" not in frame.columns:
        frame["mmsi"] = pd.NA
    metadata_cols = [column for column in VESSEL_METADATA_COLUMNS if column in frame.columns]
    frame["has_vessel_metadata"] = (
        frame[metadata_cols].notna().any(axis=1) if metadata_cols else False
    )
    grouped = frame.groupby(["farm_id", "month"], dropna=False)
    summary = grouped.agg(
        dwell_rows_from_feature_table=("farm_id", "size"),
        unique_mmsi_count=("mmsi", "nunique"),
        vessel_metadata_event_count=("has_vessel_metadata", "sum"),
    ).reset_index()
    summary["vessel_metadata_event_share"] = _safe_share(
        summary["vessel_metadata_event_count"], summary["dwell_rows_from_feature_table"]
    )

    counts = (
        frame.dropna(subset=["mmsi"])
        .groupby(["farm_id", "month", "mmsi"], dropna=False)
        .size()
        .rename("mmsi_event_count")
        .reset_index()
    )
    if not counts.empty:
        top = counts.groupby(["farm_id", "month"], as_index=False)["mmsi_event_count"].max()
        total = counts.groupby(["farm_id", "month"], as_index=False)["mmsi_event_count"].sum()
        top = top.merge(total, on=["farm_id", "month"], suffixes=("_top", "_total"))
        top["top_mmsi_concentration"] = _safe_share(
            top["mmsi_event_count_top"], top["mmsi_event_count_total"]
        )
        summary = summary.merge(
            top[["farm_id", "month", "top_mmsi_concentration"]],
            on=["farm_id", "month"],
            how="left",
        )
    else:
        summary["top_mmsi_concentration"] = pd.NA
    return summary


def aggregate_assignment_observability(turbine_events: pd.DataFrame | None) -> pd.DataFrame:
    """Aggregate turbine-assignment confidence by farm-month."""
    if turbine_events is None or turbine_events.empty:
        return pd.DataFrame(columns=["farm_id", "month"])
    _require_columns(turbine_events, {"farm_id", "start_utc"}, "turbine events")
    frame = turbine_events.copy()
    frame["month"] = _event_month(frame["start_utc"])
    frame["high_assignment"] = (
        frame["assignment_confidence"].astype("string").str.casefold().eq("high")
        if "assignment_confidence" in frame.columns
        else False
    )
    summary = frame.groupby(["farm_id", "month"], dropna=False).agg(
        assignment_event_count=("farm_id", "size"),
        high_assignment_event_count=("high_assignment", "sum"),
    ).reset_index()
    summary["high_confidence_assignment_share"] = _safe_share(
        summary["high_assignment_event_count"], summary["assignment_event_count"]
    )
    return summary


def build_farm_month_observability_bias_features(
    manifest: pd.DataFrame,
    farm_controls: pd.DataFrame,
    *,
    dwell: pd.DataFrame | None = None,
    turbine_events: pd.DataFrame | None = None,
    direct_receiver_metadata_available: bool = False,
) -> pd.DataFrame:
    """Build farm-month features for source-aware AIS observability diagnostics."""
    _require_columns(manifest, {"farm_id", "year", "month", "status"}, "AIS manifest")
    matrix = manifest.copy()
    matrix["month"] = _month_label_from_parts(matrix["year"], matrix["month"])
    matrix = matrix.rename(columns={"status": "ais_manifest_status"})
    matrix["observed_source_month_flag"] = matrix["ais_manifest_status"].isin(OBSERVED_STATUSES)
    matrix["skipped_missing_source_flag"] = matrix["ais_manifest_status"].eq(MISSING_SOURCE_STATUS)
    matrix["success_no_ais_in_bbox_flag"] = matrix["ais_manifest_status"].eq(
        "success_no_ais_in_bbox"
    )

    source_columns = {
        "input_rows": "source_input_rows",
        "clean_rows": "source_clean_rows",
        "visit_count": "source_visit_count",
        "dwell_count": "ais_dwell_event_count",
        "tier_a_count": "tier_a_count",
        "tier_b_count": "tier_b_count",
        "tier_c_count": "tier_c_count",
        "tier_d_count": "tier_d_count",
    }
    for source, target in source_columns.items():
        matrix[target] = pd.to_numeric(matrix[source], errors="coerce") if source in matrix else pd.NA
        matrix.loc[matrix["skipped_missing_source_flag"], target] = pd.NA

    matrix["observed_zero_month_flag"] = (
        matrix["observed_source_month_flag"]
        & pd.to_numeric(matrix["ais_dwell_event_count"], errors="coerce").fillna(0).eq(0)
    )
    matrix["source_file_metadata_available"] = (
        matrix.get("source_file_name", pd.Series(pd.NA, index=matrix.index)).notna()
    )
    matrix["direct_receiver_metadata_available"] = direct_receiver_metadata_available

    control_cols = [
        column
        for column in [
            "farm_id",
            "wind_farm",
            "country",
            "sea_basin",
            "commissioning_date",
            "steady_operational_start_month",
            "turbine_count",
            "farm_capacity_mw",
            "farm_centroid_latitude",
            "farm_centroid_longitude",
            "water_depth_m",
            "external_receiver_reference_used",
            "nearest_external_receiver_id",
            "nearest_external_receiver_distance_km",
            "base_station_geometry_available_flag",
            "nearest_observed_base_station_mmsi",
            "distance_to_nearest_observed_base_station_km",
        ]
        if column in farm_controls.columns
    ]
    matrix = matrix.merge(farm_controls[control_cols].drop_duplicates("farm_id"), on="farm_id", how="left")
    if "wind_farm" not in matrix.columns:
        matrix["wind_farm"] = matrix["farm_id"]
    for column in [
        "farm_capacity_mw",
        "farm_centroid_latitude",
        "farm_centroid_longitude",
        "water_depth_m",
        "nearest_external_receiver_distance_km",
        "distance_to_nearest_observed_base_station_km",
    ]:
        if column not in matrix.columns:
            matrix[column] = pd.NA
    for column in ["external_receiver_reference_used", "base_station_geometry_available_flag"]:
        if column not in matrix.columns:
            matrix[column] = False
        matrix[column] = matrix[column].fillna(False).astype(bool)
    if "nearest_external_receiver_id" not in matrix.columns:
        matrix["nearest_external_receiver_id"] = pd.NA
    if "nearest_observed_base_station_mmsi" not in matrix.columns:
        matrix["nearest_observed_base_station_mmsi"] = pd.NA

    matrix["operational_phase"] = _operational_phase(
        matrix["month"],
        matrix.get("commissioning_date", pd.Series(pd.NA, index=matrix.index)),
        matrix.get("steady_operational_start_month", pd.Series(pd.NA, index=matrix.index)),
    )

    dwell_month = aggregate_dwell_observability(dwell)
    matrix = matrix.merge(dwell_month, on=["farm_id", "month"], how="left")
    assignment_month = aggregate_assignment_observability(turbine_events)
    matrix = matrix.merge(assignment_month, on=["farm_id", "month"], how="left")

    for column in [
        "dwell_rows_from_feature_table",
        "unique_mmsi_count",
        "vessel_metadata_event_count",
        "assignment_event_count",
        "high_assignment_event_count",
    ]:
        if column not in matrix.columns:
            matrix[column] = 0
        matrix[column] = matrix[column].fillna(0)
        matrix.loc[matrix["skipped_missing_source_flag"], column] = pd.NA

    matrix["candidate_ping_density_clean_rows"] = pd.to_numeric(
        matrix["source_clean_rows"], errors="coerce"
    )
    matrix["dwell_event_density"] = pd.to_numeric(matrix["ais_dwell_event_count"], errors="coerce")
    matrix["clean_rows_per_turbine"] = _safe_share(
        matrix["candidate_ping_density_clean_rows"],
        pd.to_numeric(matrix.get("turbine_count", pd.Series(np.nan, index=matrix.index)), errors="coerce"),
    )
    matrix["dwell_events_per_turbine"] = _safe_share(
        matrix["dwell_event_density"],
        pd.to_numeric(matrix.get("turbine_count", pd.Series(np.nan, index=matrix.index)), errors="coerce"),
    )
    matrix["dwell_events_per_mw"] = _safe_share(
        matrix["dwell_event_density"],
        pd.to_numeric(matrix["farm_capacity_mw"], errors="coerce"),
    )
    matrix["tier_ab_event_count"] = (
        pd.to_numeric(matrix["tier_a_count"], errors="coerce").fillna(0)
        + pd.to_numeric(matrix["tier_b_count"], errors="coerce").fillna(0)
    )
    matrix.loc[matrix["skipped_missing_source_flag"], "tier_ab_event_count"] = pd.NA
    matrix["observability_evidence_tier"] = np.select(
        [
            matrix["direct_receiver_metadata_available"],
            matrix["base_station_geometry_available_flag"],
        ],
        [
            "Tier 1 per-message receiver/source assignment metadata available",
            "Tier 1 base-station geometry reference plus Tier 2-4 proxies; receiver-distance causality remains unconfirmed",
        ],
        default="Proxy-only: Tier 2-4 evidence; receiver-distance causality cannot be directly tested",
    )
    matrix["offshore_distance_proxy_available"] = False
    matrix["offshore_distance_proxy_km"] = pd.NA

    preferred = [
        "farm_id",
        "wind_farm",
        "year",
        "month",
        "country",
        "sea_basin",
        "commissioning_date",
        "operational_phase",
        "turbine_count",
        "farm_capacity_mw",
        "farm_centroid_latitude",
        "farm_centroid_longitude",
        "water_depth_m",
        "ais_manifest_status",
        "observed_source_month_flag",
        "success_no_ais_in_bbox_flag",
        "skipped_missing_source_flag",
        "observed_zero_month_flag",
        "source_input_rows",
        "source_clean_rows",
        "source_visit_count",
        "ais_dwell_event_count",
        "tier_a_count",
        "tier_b_count",
        "tier_c_count",
        "tier_d_count",
        "tier_ab_event_count",
        "candidate_ping_density_clean_rows",
        "dwell_event_density",
        "clean_rows_per_turbine",
        "dwell_events_per_turbine",
        "dwell_events_per_mw",
        "unique_mmsi_count",
        "top_mmsi_concentration",
        "vessel_metadata_event_share",
        "high_confidence_assignment_share",
        "direct_receiver_metadata_available",
        "base_station_geometry_available_flag",
        "nearest_observed_base_station_mmsi",
        "distance_to_nearest_observed_base_station_km",
        "source_file_metadata_available",
        "external_receiver_reference_used",
        "nearest_external_receiver_id",
        "nearest_external_receiver_distance_km",
        "offshore_distance_proxy_available",
        "offshore_distance_proxy_km",
        "observability_evidence_tier",
    ]
    remaining = [column for column in matrix.columns if column not in preferred]
    return matrix[preferred + remaining]


def build_farm_observability_bias_summary(farm_month: pd.DataFrame) -> pd.DataFrame:
    """Summarize source intensity and downstream event density by farm."""
    frame = farm_month.copy()
    for column in [
        "source_input_rows",
        "source_clean_rows",
        "source_visit_count",
        "ais_dwell_event_count",
        "tier_a_count",
        "tier_b_count",
        "tier_c_count",
        "tier_d_count",
        "tier_ab_event_count",
        "unique_mmsi_count",
    ]:
        frame[column] = pd.to_numeric(frame.get(column, pd.Series(np.nan, index=frame.index)), errors="coerce")

    summary = frame.groupby("farm_id", dropna=False).agg(
        wind_farm=("wind_farm", _first_non_null),
        country=("country", _first_non_null),
        sea_basin=("sea_basin", _first_non_null),
        turbine_count=("turbine_count", _first_non_null),
        farm_capacity_mw=("farm_capacity_mw", _first_non_null),
        farm_centroid_latitude=("farm_centroid_latitude", _first_non_null),
        farm_centroid_longitude=("farm_centroid_longitude", _first_non_null),
        water_depth_m=("water_depth_m", _first_non_null),
        manifest_months=("farm_id", "size"),
        observed_source_months=("observed_source_month_flag", "sum"),
        missing_source_months=("skipped_missing_source_flag", "sum"),
        observed_zero_months=("observed_zero_month_flag", "sum"),
        source_input_rows_total=("source_input_rows", "sum"),
        source_clean_rows_total=("source_clean_rows", "sum"),
        source_visit_count_total=("source_visit_count", "sum"),
        ais_dwell_event_count_total=("ais_dwell_event_count", "sum"),
        tier_a_count_total=("tier_a_count", "sum"),
        tier_b_count_total=("tier_b_count", "sum"),
        tier_c_count_total=("tier_c_count", "sum"),
        tier_d_count_total=("tier_d_count", "sum"),
        tier_ab_event_count_total=("tier_ab_event_count", "sum"),
        unique_mmsi_count_median=("unique_mmsi_count", "median"),
        high_confidence_assignment_share_median=("high_confidence_assignment_share", "median"),
        top_mmsi_concentration_median=("top_mmsi_concentration", "median"),
        vessel_metadata_event_share_median=("vessel_metadata_event_share", "median"),
        direct_receiver_metadata_available=("direct_receiver_metadata_available", "max"),
        base_station_geometry_available_flag=("base_station_geometry_available_flag", "max"),
        nearest_observed_base_station_mmsi=("nearest_observed_base_station_mmsi", _first_non_null),
        distance_to_nearest_observed_base_station_km=(
            "distance_to_nearest_observed_base_station_km",
            "min",
        ),
        external_receiver_reference_used=("external_receiver_reference_used", "max"),
        nearest_external_receiver_distance_km=("nearest_external_receiver_distance_km", "min"),
    ).reset_index()

    summary["source_coverage_share"] = _safe_share(
        summary["observed_source_months"], summary["manifest_months"]
    )
    summary["missing_source_share"] = _safe_share(
        summary["missing_source_months"], summary["manifest_months"]
    )
    summary["observed_zero_share_of_observed"] = _safe_share(
        summary["observed_zero_months"], summary["observed_source_months"]
    )
    summary["clean_rows_per_observed_month"] = _safe_share(
        summary["source_clean_rows_total"], summary["observed_source_months"]
    )
    summary["dwell_events_per_observed_month"] = _safe_share(
        summary["ais_dwell_event_count_total"], summary["observed_source_months"]
    )
    summary["dwell_events_per_turbine_observed_month"] = _safe_share(
        summary["ais_dwell_event_count_total"],
        pd.to_numeric(summary["turbine_count"], errors="coerce") * summary["observed_source_months"],
    )
    summary["dwell_events_per_mw_observed_month"] = _safe_share(
        summary["ais_dwell_event_count_total"],
        pd.to_numeric(summary["farm_capacity_mw"], errors="coerce") * summary["observed_source_months"],
    )
    return summary.sort_values(["sea_basin", "country", "farm_id"]).reset_index(drop=True)


def build_geographic_source_intensity_summary(farm_month: pd.DataFrame) -> pd.DataFrame:
    """Build country/sea-basin source-intensity diagnostics for the report."""
    frame = farm_month.copy()
    for column in ["country", "sea_basin"]:
        frame[column] = frame.get(column, pd.Series("unknown", index=frame.index)).fillna("unknown")
    grouped = frame.groupby(["country", "sea_basin"], dropna=False).agg(
        farm_count=("farm_id", "nunique"),
        farm_month_count=("farm_id", "size"),
        observed_source_months=("observed_source_month_flag", "sum"),
        missing_source_months=("skipped_missing_source_flag", "sum"),
        observed_zero_months=("observed_zero_month_flag", "sum"),
        source_clean_rows_total=("source_clean_rows", "sum"),
        ais_dwell_event_count_total=("ais_dwell_event_count", "sum"),
        tier_ab_event_count_total=("tier_ab_event_count", "sum"),
        median_clean_rows_per_turbine=("clean_rows_per_turbine", "median"),
        median_dwell_events_per_turbine=("dwell_events_per_turbine", "median"),
        median_high_confidence_assignment_share=("high_confidence_assignment_share", "median"),
        median_top_mmsi_concentration=("top_mmsi_concentration", "median"),
        median_vessel_metadata_event_share=("vessel_metadata_event_share", "median"),
        median_distance_to_nearest_observed_base_station_km=(
            "distance_to_nearest_observed_base_station_km",
            "median",
        ),
    ).reset_index()
    grouped["source_coverage_share"] = _safe_share(
        grouped["observed_source_months"], grouped["farm_month_count"]
    )
    grouped["missing_source_share"] = _safe_share(
        grouped["missing_source_months"], grouped["farm_month_count"]
    )
    grouped["observed_zero_share_of_observed"] = _safe_share(
        grouped["observed_zero_months"], grouped["observed_source_months"]
    )
    grouped["clean_rows_per_observed_month"] = _safe_share(
        grouped["source_clean_rows_total"], grouped["observed_source_months"]
    )
    grouped["dwell_events_per_observed_month"] = _safe_share(
        grouped["ais_dwell_event_count_total"], grouped["observed_source_months"]
    )
    return grouped.sort_values(["sea_basin", "country"]).reset_index(drop=True)


def _assign_base_station_distance_bins(stratum: pd.DataFrame) -> pd.DataFrame:
    """Assign balanced distance bins inside one country/basin/month stratum."""
    stratum = stratum.copy()
    stratum["_base_station_distance_km"] = pd.to_numeric(
        stratum["distance_to_nearest_observed_base_station_km"], errors="coerce"
    )
    stratum["distance_bin"] = "missing_base_station_distance"
    stratum["distance_bin_order"] = -1
    stratum["distance_comparison_status"] = "missing_base_station_distance"

    valid_mask = stratum["_base_station_distance_km"].notna()
    valid_count = int(valid_mask.sum())
    if valid_count == 0:
        return stratum

    valid = stratum.loc[valid_mask].sort_values(
        ["_base_station_distance_km", "farm_id"], kind="mergesort"
    )
    if valid_count < 4:
        stratum.loc[valid.index, "distance_bin"] = "insufficient_within_stratum_comparison"
        stratum.loc[valid.index, "distance_bin_order"] = 0
        stratum.loc[
            valid.index, "distance_comparison_status"
        ] = "insufficient_within_stratum_comparison"
        return stratum

    positions = np.arange(valid_count)
    if valid_count >= 8:
        bin_orders = np.minimum((positions * 4 // valid_count) + 1, 4)
        labels = {
            1: "q1_nearest",
            2: "q2_near_mid",
            3: "q3_far_mid",
            4: "q4_farthest",
        }
        status = "eligible_within_stratum_quartile"
    else:
        bin_orders = np.where(positions < valid_count // 2, 1, 2)
        labels = {1: "near", 2: "far"}
        status = "eligible_within_stratum_median_split"

    stratum.loc[valid.index, "distance_bin_order"] = bin_orders
    stratum.loc[valid.index, "distance_bin"] = [labels[int(order)] for order in bin_orders]
    stratum.loc[valid.index, "distance_comparison_status"] = status
    return stratum


def build_base_station_distance_stratum_diagnostic(farm_month: pd.DataFrame) -> pd.DataFrame:
    """Compare AIS source and dwell proxies by base-station distance inside matched strata."""
    required = {
        "farm_id",
        "country",
        "sea_basin",
        "year",
        "month",
        "observed_source_month_flag",
        "skipped_missing_source_flag",
        "success_no_ais_in_bbox_flag",
        "observed_zero_month_flag",
        "distance_to_nearest_observed_base_station_km",
        "source_clean_rows",
        "ais_dwell_event_count",
        "turbine_count",
        "farm_capacity_mw",
        "tier_a_count",
        "tier_b_count",
        "tier_c_count",
        "tier_d_count",
    }
    _require_columns(farm_month, required, "farm-month observability-bias matrix")

    frame = farm_month.copy()
    observed_mask = (
        frame["observed_source_month_flag"].fillna(False).astype(bool)
        & ~frame["skipped_missing_source_flag"].fillna(False).astype(bool)
    )
    frame = frame.loc[observed_mask].copy()
    if frame.empty:
        return pd.DataFrame(columns=BASE_STATION_DISTANCE_DIAGNOSTIC_COLUMNS)

    for column in ["country", "sea_basin"]:
        frame[column] = frame[column].fillna("unknown")
    for column in [
        "source_clean_rows",
        "ais_dwell_event_count",
        "turbine_count",
        "farm_capacity_mw",
        "tier_a_count",
        "tier_b_count",
        "tier_c_count",
        "tier_d_count",
    ]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce").fillna(0)
    frame["_success_no_ais_in_bbox"] = (
        frame["success_no_ais_in_bbox_flag"].fillna(False).astype(bool).astype(int)
    )
    frame["_observed_zero"] = frame["observed_zero_month_flag"].fillna(False).astype(bool).astype(int)

    assigned = pd.concat(
        [
            _assign_base_station_distance_bins(stratum)
            for _, stratum in frame.groupby(list(BASE_STATION_DISTANCE_STRATA), dropna=False)
        ],
        ignore_index=True,
    )
    grouped = assigned.groupby(
        [
            *BASE_STATION_DISTANCE_STRATA,
            "distance_bin",
            "distance_bin_order",
            "distance_comparison_status",
        ],
        dropna=False,
    ).agg(
        farm_month_count=("farm_id", "size"),
        farm_count=("farm_id", "nunique"),
        median_distance_to_nearest_observed_base_station_km=("_base_station_distance_km", "median"),
        turbine_month_denominator=("turbine_count", "sum"),
        capacity_mw_month_denominator=("farm_capacity_mw", "sum"),
        source_clean_rows_total=("source_clean_rows", "sum"),
        success_no_ais_in_bbox_month_count=("_success_no_ais_in_bbox", "sum"),
        observed_zero_month_count=("_observed_zero", "sum"),
        ais_dwell_event_count_total=("ais_dwell_event_count", "sum"),
        tier_a_count_total=("tier_a_count", "sum"),
        tier_b_count_total=("tier_b_count", "sum"),
        tier_c_count_total=("tier_c_count", "sum"),
        tier_d_count_total=("tier_d_count", "sum"),
    ).reset_index()

    grouped["clean_rows_per_turbine_month"] = _safe_share(
        grouped["source_clean_rows_total"], grouped["turbine_month_denominator"]
    )
    grouped["clean_rows_per_mw_month"] = _safe_share(
        grouped["source_clean_rows_total"], grouped["capacity_mw_month_denominator"]
    )
    grouped["observed_zero_rate"] = _safe_share(
        grouped["observed_zero_month_count"], grouped["farm_month_count"]
    )
    grouped["dwell_events_per_turbine_month"] = _safe_share(
        grouped["ais_dwell_event_count_total"], grouped["turbine_month_denominator"]
    )
    grouped["dwell_events_per_mw_month"] = _safe_share(
        grouped["ais_dwell_event_count_total"], grouped["capacity_mw_month_denominator"]
    )
    for tier in ["a", "b", "c", "d"]:
        total = f"tier_{tier}_count_total"
        grouped[f"tier_{tier}_per_turbine_month"] = _safe_share(
            grouped[total], grouped["turbine_month_denominator"]
        )
        grouped[f"tier_{tier}_per_mw_month"] = _safe_share(
            grouped[total], grouped["capacity_mw_month_denominator"]
        )

    return grouped[list(BASE_STATION_DISTANCE_DIAGNOSTIC_COLUMNS)].sort_values(
        [*BASE_STATION_DISTANCE_STRATA, "distance_bin_order", "distance_bin"]
    ).reset_index(drop=True)


def _declines_strongly(ratio: float, threshold: float = 0.8) -> bool:
    return bool(np.isfinite(ratio) and ratio <= threshold)


def build_base_station_distance_gradient_summary(stratum_table: pd.DataFrame) -> pd.DataFrame:
    """Summarize near/far distance gradients for each matched stratum."""
    if stratum_table.empty:
        return pd.DataFrame(columns=BASE_STATION_DISTANCE_GRADIENT_COLUMNS)
    _require_columns(
        stratum_table,
        {
            *BASE_STATION_DISTANCE_STRATA,
            "distance_bin",
            "distance_bin_order",
            "distance_comparison_status",
            "farm_month_count",
            "farm_count",
            "median_distance_to_nearest_observed_base_station_km",
            "clean_rows_per_turbine_month",
            "clean_rows_per_mw_month",
            "observed_zero_rate",
            "dwell_events_per_turbine_month",
            "dwell_events_per_mw_month",
            "tier_a_per_turbine_month",
            "tier_b_per_turbine_month",
            "tier_c_per_turbine_month",
            "tier_d_per_turbine_month",
        },
        "base-station distance stratum diagnostic",
    )

    rows: list[dict[str, Any]] = []
    eligible_statuses = {
        "eligible_within_stratum_quartile",
        "eligible_within_stratum_median_split",
    }
    for keys, group in stratum_table.groupby(list(BASE_STATION_DISTANCE_STRATA), dropna=False):
        key_values = dict(zip(BASE_STATION_DISTANCE_STRATA, keys, strict=True))
        eligible = group[group["distance_comparison_status"].isin(eligible_statuses)].sort_values(
            "distance_bin_order"
        )
        if eligible["distance_bin_order"].nunique() < 2:
            rows.append(
                {
                    **key_values,
                    "comparison_method": _first_non_null(group["distance_comparison_status"]),
                    "eligible_distance_bin_count": int(eligible["distance_bin_order"].nunique()),
                    "farm_month_count": int(group["farm_month_count"].sum()),
                    "farm_count": int(group["farm_count"].sum()),
                    "near_distance_bin": pd.NA,
                    "far_distance_bin": pd.NA,
                    "near_median_distance_km": pd.NA,
                    "far_median_distance_km": pd.NA,
                    "clean_rows_per_turbine_month_far_near_ratio": np.nan,
                    "clean_rows_per_mw_month_far_near_ratio": np.nan,
                    "dwell_events_per_turbine_month_far_near_ratio": np.nan,
                    "dwell_events_per_mw_month_far_near_ratio": np.nan,
                    "tier_a_per_turbine_month_far_near_ratio": np.nan,
                    "tier_b_per_turbine_month_far_near_ratio": np.nan,
                    "tier_c_per_turbine_month_far_near_ratio": np.nan,
                    "tier_d_per_turbine_month_far_near_ratio": np.nan,
                    "observed_zero_rate_far_minus_near": np.nan,
                    "source_intensity_declines_with_distance": False,
                    "observed_zero_increases_with_distance": False,
                    "downstream_proxy_declines_with_distance": False,
                    "diagnostic_class": "insufficient_matched_strata",
                    "interpretation_guardrail": (
                        "Nearest observed base-station distance is source-geometry control only; "
                        "it is not receiver assignment."
                    ),
                }
            )
            continue

        near = eligible.iloc[0]
        far = eligible.iloc[-1]
        clean_turbine_ratio = _safe_scalar_ratio(
            far["clean_rows_per_turbine_month"], near["clean_rows_per_turbine_month"]
        )
        clean_mw_ratio = _safe_scalar_ratio(
            far["clean_rows_per_mw_month"], near["clean_rows_per_mw_month"]
        )
        dwell_turbine_ratio = _safe_scalar_ratio(
            far["dwell_events_per_turbine_month"], near["dwell_events_per_turbine_month"]
        )
        dwell_mw_ratio = _safe_scalar_ratio(
            far["dwell_events_per_mw_month"], near["dwell_events_per_mw_month"]
        )
        tier_ratios = {
            tier: _safe_scalar_ratio(
                far[f"tier_{tier}_per_turbine_month"],
                near[f"tier_{tier}_per_turbine_month"],
            )
            for tier in ["a", "b", "c", "d"]
        }
        zero_delta = (
            pd.to_numeric(pd.Series([far["observed_zero_rate"]]), errors="coerce").iloc[0]
            - pd.to_numeric(pd.Series([near["observed_zero_rate"]]), errors="coerce").iloc[0]
        )
        source_declines = _declines_strongly(clean_turbine_ratio)
        zero_increases = bool(np.isfinite(zero_delta) and zero_delta >= 0.05)
        downstream_declines = _declines_strongly(dwell_turbine_ratio) or any(
            _declines_strongly(ratio) for ratio in tier_ratios.values()
        )
        if source_declines and zero_increases:
            diagnostic_class = "consistent_with_geographic_ais_observability_bias"
        elif downstream_declines and not source_declines:
            diagnostic_class = "downstream_proxy_only_gradient"
        else:
            diagnostic_class = "no_clear_gradient"

        rows.append(
            {
                **key_values,
                "comparison_method": _first_non_null(eligible["distance_comparison_status"]),
                "eligible_distance_bin_count": int(eligible["distance_bin_order"].nunique()),
                "farm_month_count": int(eligible["farm_month_count"].sum()),
                "farm_count": int(eligible["farm_count"].sum()),
                "near_distance_bin": near["distance_bin"],
                "far_distance_bin": far["distance_bin"],
                "near_median_distance_km": near[
                    "median_distance_to_nearest_observed_base_station_km"
                ],
                "far_median_distance_km": far[
                    "median_distance_to_nearest_observed_base_station_km"
                ],
                "clean_rows_per_turbine_month_far_near_ratio": clean_turbine_ratio,
                "clean_rows_per_mw_month_far_near_ratio": clean_mw_ratio,
                "dwell_events_per_turbine_month_far_near_ratio": dwell_turbine_ratio,
                "dwell_events_per_mw_month_far_near_ratio": dwell_mw_ratio,
                "tier_a_per_turbine_month_far_near_ratio": tier_ratios["a"],
                "tier_b_per_turbine_month_far_near_ratio": tier_ratios["b"],
                "tier_c_per_turbine_month_far_near_ratio": tier_ratios["c"],
                "tier_d_per_turbine_month_far_near_ratio": tier_ratios["d"],
                "observed_zero_rate_far_minus_near": zero_delta,
                "source_intensity_declines_with_distance": source_declines,
                "observed_zero_increases_with_distance": zero_increases,
                "downstream_proxy_declines_with_distance": downstream_declines,
                "diagnostic_class": diagnostic_class,
                "interpretation_guardrail": (
                    "Nearest observed base-station distance is source-geometry control only; "
                    "it is not receiver assignment."
                ),
            }
        )

    return pd.DataFrame(rows, columns=BASE_STATION_DISTANCE_GRADIENT_COLUMNS).sort_values(
        [*BASE_STATION_DISTANCE_STRATA]
    ).reset_index(drop=True)


def _base_station_distance_gradient_statement(gradient_summary: pd.DataFrame | None) -> str:
    if gradient_summary is None or gradient_summary.empty:
        return (
            "No base-station distance gradient table was available. Receiver-distance "
            "causality remains untested."
        )
    eligible = gradient_summary[
        gradient_summary["diagnostic_class"].ne("insufficient_matched_strata")
    ]
    if eligible.empty:
        return (
            "Matched within-country/sea-basin/year-month base-station distance strata are "
            "too sparse for a near/far gradient comparison."
        )
    class_counts = eligible["diagnostic_class"].value_counts()
    consistent_count = int(
        class_counts.get("consistent_with_geographic_ais_observability_bias", 0)
    )
    downstream_only_count = int(class_counts.get("downstream_proxy_only_gradient", 0))
    no_clear_count = int(class_counts.get("no_clear_gradient", 0))
    eligible_count = int(len(eligible))
    count_sentence = (
        f"Eligible matched strata: {eligible_count:,}; "
        f"bias-consistent strata: {consistent_count:,}; "
        f"downstream-only strata: {downstream_only_count:,}; "
        f"no-clear strata: {no_clear_count:,}."
    )
    if consistent_count >= 3 and consistent_count / eligible_count >= 0.6:
        return (
            f"{count_sentence} "
            "Within-country/sea-basin/year-month diagnostics show evidence consistent with "
            "geographical AIS observability bias: farther farm-month bins have lower raw "
            "candidate AIS clean-row intensity and higher observed-zero rates in most "
            "eligible strata. This still does not confirm receiver-distance causality."
        )
    if downstream_only_count >= 3 and downstream_only_count / eligible_count >= 0.6:
        return (
            f"{count_sentence} "
            "Within-country/sea-basin/year-month diagnostics show downstream dwell/Tier "
            "gradients without matching clean-row source-intensity gradients. Treat this "
            "as downstream proxy-only evidence, not a source observability finding."
        )
    return (
        f"{count_sentence} "
        "Within-country/sea-basin/year-month diagnostics do not show a clear matched "
        "base-station-distance gradient. This does not prove absence of AIS observability "
        "bias; it only limits this proxy diagnostic."
    )


def build_report_text(
    *,
    farm_month: pd.DataFrame,
    farm_summary: pd.DataFrame,
    receiver_inventory: pd.DataFrame,
    geographic_summary: pd.DataFrame,
    validation: dict[str, Any],
    base_station_catalogue: pd.DataFrame | None = None,
    distance_stratum_diagnostic: pd.DataFrame | None = None,
    distance_gradient_summary: pd.DataFrame | None = None,
) -> str:
    """Render the AIS receiver-distance/source-observability audit report."""
    direct_fields = receiver_inventory[receiver_inventory["is_direct_receiver_evidence"]]
    source_fields = receiver_inventory[receiver_inventory["is_source_channel_evidence"]]
    proxy_fields = receiver_inventory[receiver_inventory["is_proxy_evidence"]]
    per_message_receiver_assignment_available = bool(
        direct_fields[~direct_fields["external_reference_used"]].shape[0] > 0
    )
    base_station_mmsi_count = int(validation.get("base_station_mmsi_count", 0))
    base_station_record_count = int(validation.get("base_station_record_count", 0))
    base_station_catalogue_rows = (
        0 if base_station_catalogue is None else int(len(base_station_catalogue))
    )
    external_reference_used = bool(receiver_inventory["external_reference_used"].any())
    geo_rows = geographic_summary.sort_values("observed_zero_share_of_observed", ascending=False).head(10)
    geo_table = geo_rows.to_markdown(index=False)
    distance_gradient_statement = _base_station_distance_gradient_statement(distance_gradient_summary)
    if distance_gradient_summary is not None and not distance_gradient_summary.empty:
        gradient_rows = distance_gradient_summary.sort_values(
            ["diagnostic_class", "observed_zero_rate_far_minus_near"],
            ascending=[True, False],
        ).head(12)
        gradient_table = gradient_rows[
            [
                "country",
                "sea_basin",
                "year",
                "month",
                "farm_month_count",
                "near_distance_bin",
                "far_distance_bin",
                "clean_rows_per_turbine_month_far_near_ratio",
                "observed_zero_rate_far_minus_near",
                "dwell_events_per_turbine_month_far_near_ratio",
                "diagnostic_class",
            ]
        ].to_markdown(index=False)
    else:
        gradient_table = "No base-station distance gradient rows were generated."
    riskiest_rows = farm_summary.sort_values(
        ["missing_source_share", "observed_zero_share_of_observed"], ascending=False
    ).head(10)
    riskiest_table = riskiest_rows[
        [
            "farm_id",
            "country",
            "sea_basin",
            "observed_source_months",
            "missing_source_share",
            "observed_zero_share_of_observed",
            "clean_rows_per_observed_month",
            "dwell_events_per_observed_month",
        ]
    ].to_markdown(index=False)

    assignment_statement = (
        "Per-vessel-message receiver/source assignment fields are available in the inspected schemas."
        if per_message_receiver_assignment_available
        else "No per-vessel-message receiver station ID or receiver assignment field was found in the inspected local AIS/RQ9 schemas."
    )
    base_station_statement = (
        f"Raw AIS contains `Type of mobile = Base Station` records: {base_station_mmsi_count:,} station MMSIs from {base_station_record_count:,} messages. These provide direct AIS base-station geometry reference for source-geometry controls, not proof of which station received each vessel ping."
        if base_station_mmsi_count
        else "No raw AIS `Type of mobile = Base Station` geometry records were extracted in this run."
    )
    external_statement = (
        "An external receiver reference was used with required provenance; treat resulting distances as supporting context."
        if external_reference_used
        else "No external receiver/coastline reference was used in this run; no receiver-distance field was imputed."
    )

    lines = [
        "# AIS Receiver Distance And Source Observability Audit",
        "",
        "## Scope",
        "",
        "This is an audit-first observability-bias study built from existing local artifacts. It does not rerun AIS extraction, rerun metocean extraction, modify raw/interim/source data, or start new research-question modelling.",
        "",
        "AIS events are treated as observed vessel/dwell/intervention proxies only. A lower AIS event density is not a lower intervention rate, and it is not a lower failure rate.",
        "",
        "## Reproducibility",
        "",
        "- Source of truth: `scripts/build_ais_observability_bias.py` calls `src/om_pipeline/analysis/ais_observability_bias.py` from the repository root.",
        "- Rebuild command: `/opt/anaconda3/bin/python scripts/build_ais_observability_bias.py`.",
        "- Generated matrices under `Data/Processed/analysis/ais_observability_bias/` are derived outputs and may be ignored by git; regenerate them rather than hand-editing them.",
        "- The tracked report and methodology are refreshed from the same builder and should preserve the missingness and no-failure-claim guardrails.",
        "",
        "## Additional Observability Layers",
        "",
        "Build the audit around four evidence tiers:",
        "",
        "Tier 1a: per-message receiver assignment evidence",
        "- vessel ping linked to receiving station ID",
        "- receiver station ID attached to each vessel message",
        "- receiver channel/terrestrial/satellite assignment attached to each vessel message",
        "",
        "Tier 1b: direct AIS source-geometry reference",
        "- `Type of mobile = Base Station` AIS records with station MMSI and latitude/longitude",
        "- source-provider geometry",
        "",
        "Tier 2: source-channel evidence",
        "- `Data source type`",
        "- provider/source system",
        "- source file/month status",
        "- manifest `input_rows` and `clean_rows`",
        "",
        "Tier 3: geographic proxy evidence",
        "- farm centroid latitude/longitude",
        "- distance to nearest external receiver if receiver reference data is allowed and provenance-tracked",
        "- distance to coast/offshore proxy if available",
        "- country",
        "- sea basin",
        "- water depth/bathymetry",
        "",
        "Tier 4: downstream observability proxies",
        "- observed-zero months under source coverage",
        "- dwell/event density",
        "- Tier A/B/C/D counts",
        "- high-confidence assignment share",
        "- top MMSI concentration",
        "- unique MMSI count",
        "- vessel metadata completeness",
        "",
        "Interpretation rule:",
        "Only per-message receiver assignment can directly test receiver-distance bias. AIS base-station geometry is a source-geometry control; Tiers 2-4 can support or weaken a broader AIS observability-bias hypothesis but cannot confirm receiver-distance causality.",
        "",
        "## Receiver/Source Metadata Inventory",
        "",
        f"- {assignment_statement}",
        f"- {base_station_statement}",
        f"- {external_statement}",
        f"- Inspected schema sources: {validation['schema_source_count']}.",
        f"- Raw AIS files inspected by header only: {validation['raw_ais_file_count']}.",
        f"- Raw AIS files with `Data source type`: {validation['raw_ais_data_source_type_file_count']}.",
        f"- Per-message receiver-like schema fields found: {len(direct_fields)}. Source-channel fields found: {len(source_fields)}. Proxy fields found: {len(proxy_fields)}.",
        f"- AIS base-station catalogue rows: {base_station_catalogue_rows:,}.",
        "",
        "`Data source type` is source-channel evidence, not receiver station geometry. `Type of mobile = Base Station` rows are direct AIS base-station geometry reference, but they do not provide per-message receiver assignment. Source file metadata and manifest row counts support source availability/intensity auditing.",
        "",
        "## Source-Intensity Layer",
        "",
        f"- Farm-month rows: {validation['farm_month_rows']:,}.",
        f"- Observed source farm-months: {validation['observed_source_months']:,}.",
        f"- Missing-source farm-months: {validation['missing_source_months']:,}. `skipped_missing_source` is missing source evidence, not zero events.",
        f"- Observed-zero farm-months: {validation['observed_zero_months']:,}. These are counted only where source coverage is observed.",
        f"- Candidate AIS rows are separated from dwell/event rows: total `clean_rows` = {validation['source_clean_rows_total']:,}; total dwell events = {validation['ais_dwell_event_count_total']:,}.",
        "",
        "Primary comparisons should use observed source months only and exclude `skipped_missing_source`. Source intensity (`input_rows`, `clean_rows`) is reported separately from dwell/event density so sparse raw AIS candidate evidence is not conflated with dwell-detection output.",
        "",
        "## Geographic Diagnostics",
        "",
        "The table below is a geographic observability diagnostic, not an operational-performance ranking. Raw sea-basin contrasts must be checked within countries or matched strata before being used as evidence.",
        "",
        geo_table,
        "",
        "## Matched Base-Station Distance Diagnostic",
        "",
        distance_gradient_statement,
        "",
        "The diagnostic below uses observed-source farm-months only, excludes `skipped_missing_source`, bins nearest observed AIS base-station distance within country/sea-basin/year-month strata, and compares source `clean_rows` separately from downstream dwell/Tier proxies. Nearest observed AIS base station remains a source-geometry control only; it is not evidence that the station received any vessel ping.",
        "",
        gradient_table,
        "",
        "## Highest Missingness/Observed-Zero Farms",
        "",
        riskiest_table,
        "",
        "## Interpretation",
        "",
        "- The current local data can test source availability, source intensity, observed-zero frequency, vessel concentration, assignment-confidence proxies, and farm distance to nearest observed AIS base-station geometry.",
        "- The current local data cannot directly test receiver-distance causality unless vessel pings can be linked to receiving station IDs or equivalent per-message receiver assignments.",
        "- Do not infer receiver locations from vessel positions.",
        "- Do not claim the nearest observed base station received a vessel ping.",
        "- Do not treat nearest coast as nearest receiver unless an accepted external reference justifies that assumption.",
        "- Do not compare raw Baltic/North Sea rates without within-country or matched-strata checks.",
        "- Do not call lower AIS event density lower intervention activity or lower failure rate.",
        "",
        "## RQ Answerability Impact",
        "",
        "RQ9 remains blocked for failure claims and causal receiver-distance claims until per-message AIS receiver/source assignment and fault/work-order validation exist. The observed AIS base-station catalogue supports source-geometry controls only. The audit can support evidence-readiness and source-aware sensitivity work, but not confirmed failure-rate inference.",
    ]
    text = "\n".join(lines) + "\n"
    if contains_ais_only_failure_rate_claim(text):
        raise ValueError("Report contains a prohibited AIS-only failure-rate claim.")
    return text


def build_methodology_text() -> str:
    """Render the foundation-level methodology document."""
    return "\n".join(
        [
            "# AIS Receiver Distance Observability Audit Methodology",
            "",
            "## Purpose",
            "",
            "This audit tests whether AIS-derived farm-level event rates may be geographically skewed by source coverage or detectability. It is a data-readiness layer, not a new RQ model and not a failure-rate analysis.",
            "",
            "## Reproducibility",
            "",
            "Source of truth: `scripts/build_ais_observability_bias.py` and `src/om_pipeline/analysis/ais_observability_bias.py`.",
            "",
            "Rebuild from the repository root:",
            "",
            "```bash",
            "/opt/anaconda3/bin/python scripts/build_ais_observability_bias.py",
            "```",
            "",
            "Derived matrices under `Data/Processed/analysis/ais_observability_bias/` may be ignored by git. Rebuild them from existing local artifacts rather than editing them directly.",
            "",
            "## Evidence Tiers",
            "",
            "Tier 1a is per-message receiver assignment evidence: vessel pings linked to receiving station IDs, receiver channel, or equivalent source-provider assignment. Only per-message receiver assignment can directly test receiver-distance bias.",
            "",
            "Tier 1b is direct AIS source-geometry reference: `Type of mobile = Base Station` rows with station MMSI and latitude/longitude, or source-provider geometry. This supports farm distance to nearest observed base station as a source-geometry control, but it does not prove which station received a vessel ping.",
            "",
            "Tier 2 is source-channel evidence: `Data source type`, source-provider fields, source file/month status, and manifest `input_rows`/`clean_rows`. Tier 2 supports source availability and source-intensity diagnostics but not receiver-distance causality.",
            "",
            "Tier 3 is geographic proxy evidence: farm centroid, country, sea basin, water depth, bathymetry, distance to a provenanced external receiver reference, or offshore-distance proxies. These are proxy controls only.",
            "",
            "Tier 4 is downstream observability proxy evidence: observed-zero months under coverage, dwell/event density, Tier A/B/C/D counts, assignment confidence, top-MMSI concentration, unique MMSI count, and vessel metadata completeness.",
            "",
            "## Missingness Semantics",
            "",
            "- `success` and `success_no_ais_in_bbox` are observed source coverage.",
            "- `success_no_ais_in_bbox` is observed zero AIS activity.",
            "- `skipped_missing_source` is missing source evidence and is excluded from observed-zero and event-density denominators.",
            "- Missing per-message receiver assignment is reported as unavailable and is never imputed from vessel positions.",
            "- Observed AIS base-station geometry is reported separately from per-message receiver assignment.",
            "- Missing external receiver/coastline provenance blocks use of external distance references.",
            "",
            "## Matched Base-Station Distance Diagnostic",
            "",
            "Observed-source farm-months can be binned by distance to nearest observed AIS base-station geometry within country/sea-basin/year-month strata. Strata with at least eight farm-months use quartiles; strata with four to seven farm-months use a near/far median split; smaller strata are marked `insufficient_matched_strata`.",
            "",
            "This diagnostic compares source `clean_rows`, observed-zero rates, dwell counts, and Tier A/B/C/D rates. Strong clean-row declines plus observed-zero increases are evidence consistent with geographical AIS observability bias, but still not receiver-distance causality because the nearest observed base station is not a per-message receiving-station assignment.",
            "",
            "## Guardrails",
            "",
            "- AIS dwell/events are candidate intervention proxies, not confirmed failures.",
            "- Absence of AIS events is not absence of activity unless source observability is established.",
            "- Lower AIS event density is not lower intervention activity and not lower failure rate.",
            "- Nearest observed base station is a source-geometry control, not proof of the receiving station for a vessel ping.",
            "- Nearest coast is not a receiver proxy unless explicitly justified by an accepted external reference.",
            "- Raw Baltic/North Sea contrasts must be checked using source-aware, within-country, or matched-strata diagnostics before interpretation.",
        ]
    ) + "\n"


def _validation_summary(
    farm_month: pd.DataFrame,
    farm_summary: pd.DataFrame,
    receiver_inventory: pd.DataFrame,
    raw_schema_summary: dict[str, Any],
    base_station_catalogue: pd.DataFrame,
    distance_stratum_diagnostic: pd.DataFrame,
    distance_gradient_summary: pd.DataFrame,
) -> dict[str, Any]:
    eligible_gradient = distance_gradient_summary[
        distance_gradient_summary["diagnostic_class"].ne("insufficient_matched_strata")
    ] if not distance_gradient_summary.empty else pd.DataFrame()
    return {
        "farm_month_rows": int(len(farm_month)),
        "farm_summary_rows": int(len(farm_summary)),
        "observed_source_months": int(farm_month["observed_source_month_flag"].sum()),
        "missing_source_months": int(farm_month["skipped_missing_source_flag"].sum()),
        "observed_zero_months": int(farm_month["observed_zero_month_flag"].sum()),
        "source_clean_rows_total": int(
            pd.to_numeric(farm_month["source_clean_rows"], errors="coerce").fillna(0).sum()
        ),
        "ais_dwell_event_count_total": int(
            pd.to_numeric(farm_month["ais_dwell_event_count"], errors="coerce").fillna(0).sum()
        ),
        "direct_receiver_field_count": int(receiver_inventory["is_direct_receiver_evidence"].sum()),
        "source_channel_field_count": int(receiver_inventory["is_source_channel_evidence"].sum()),
        "proxy_field_count": int(receiver_inventory["is_proxy_evidence"].sum()),
        "schema_source_count": int(receiver_inventory["source_name"].nunique()),
        "base_station_catalogue_rows": int(len(base_station_catalogue)),
        "base_station_mmsi_count": int(
            base_station_catalogue["base_station_mmsi"].nunique()
            if "base_station_mmsi" in base_station_catalogue.columns
            else 0
        ),
        "base_station_record_count": int(
            pd.to_numeric(
                base_station_catalogue.get(
                    "observation_count",
                    pd.Series(dtype="float64"),
                ),
                errors="coerce",
            )
            .fillna(0)
            .sum()
        ),
        "base_station_geometry_available": bool(not base_station_catalogue.empty),
        "base_station_distance_diagnostic_rows": int(len(distance_stratum_diagnostic)),
        "base_station_distance_gradient_rows": int(len(distance_gradient_summary)),
        "base_station_distance_eligible_strata": int(len(eligible_gradient)),
        "base_station_distance_bias_consistent_strata": int(
            distance_gradient_summary["diagnostic_class"]
            .eq("consistent_with_geographic_ais_observability_bias")
            .sum()
            if "diagnostic_class" in distance_gradient_summary.columns
            else 0
        ),
        "per_message_receiver_assignment_available": bool(
            receiver_inventory[
                receiver_inventory["is_direct_receiver_evidence"]
                & ~receiver_inventory["external_reference_used"]
            ].shape[0]
            > 0
        ),
        **raw_schema_summary,
    }


def build_ais_observability_bias_outputs(
    *,
    project_root: Path,
    processed_output_dir: Path,
    report_output_dir: Path,
    methodology_path: Path,
    manifest_path: Path,
    dwell_path: Path,
    turbine_path: Path,
    farm_intensity_path: Path,
    turbine_intensity_path: Path,
    turbine_exposure_path: Path,
    turbine_events_path: Path,
    raw_ais_root: Path,
    raw_ais_pattern: str = "Farm-Candidates_European-Master_*.csv",
    external_receiver_reference_path: Path | None = None,
    external_reference_provenance_path: Path | None = None,
) -> AisObservabilityBiasOutputs:
    """Build all AIS observability-bias audit outputs from existing artifacts."""
    manifest = pd.read_csv(manifest_path)
    dwell = pd.read_parquet(dwell_path) if dwell_path.exists() else None
    turbines = pd.read_csv(turbine_path) if turbine_path.exists() else None
    farm_intensity = pd.read_csv(farm_intensity_path) if farm_intensity_path.exists() else None
    turbine_intensity = pd.read_csv(turbine_intensity_path) if turbine_intensity_path.exists() else None
    turbine_exposure = pd.read_csv(turbine_exposure_path) if turbine_exposure_path.exists() else None
    turbine_events = pd.read_csv(turbine_events_path) if turbine_events_path.exists() else None

    if turbine_exposure is None:
        raise FileNotFoundError(f"Required turbine exposure input missing: {turbine_exposure_path}")

    external_receiver_reference = (
        pd.read_csv(external_receiver_reference_path)
        if external_receiver_reference_path and external_receiver_reference_path.exists()
        else None
    )
    external_reference_provenance = (
        pd.read_csv(external_reference_provenance_path)
        if external_reference_provenance_path and external_reference_provenance_path.exists()
        else None
    )
    external_reference_used = external_receiver_reference is not None and not external_receiver_reference.empty

    raw_schema_sources, raw_schema_summary = read_raw_ais_schema_inventory(raw_ais_root, raw_ais_pattern)
    base_station_catalogue = extract_ais_base_station_geometry_catalogue(
        raw_ais_root, raw_ais_pattern
    )
    schema_sources: dict[str, list[str]] = {
        "backfill_manifest.csv": manifest.columns.tolist(),
        **raw_schema_sources,
    }
    if dwell is not None:
        schema_sources["cross_farm_dwell_weather_features.parquet"] = dwell.columns.tolist()
    if turbine_exposure is not None:
        schema_sources["turbine_exposure_denominator.csv"] = turbine_exposure.columns.tolist()
    if turbine_events is not None:
        schema_sources["turbine_intervention_events_v0.csv"] = turbine_events.columns.tolist()
    if farm_intensity is not None:
        schema_sources["farm_intervention_intensity.csv"] = farm_intensity.columns.tolist()

    receiver_inventory = build_receiver_metadata_inventory(
        schema_sources,
        external_reference_used=external_reference_used,
        external_reference_provenance=external_reference_provenance,
    )
    direct_receiver_metadata_available = bool(
        receiver_inventory[
            receiver_inventory["is_direct_receiver_evidence"]
            & ~receiver_inventory["external_reference_used"]
        ].shape[0]
    )

    farm_metadata = build_farm_metadata(
        turbines=turbines,
        farm_intensity=farm_intensity,
        turbine_exposure=turbine_exposure,
        turbine_intensity=turbine_intensity,
    )
    farm_controls = build_farm_controls(turbine_exposure, farm_metadata)
    farm_controls = add_observed_base_station_distances(farm_controls, base_station_catalogue)
    farm_controls = add_external_receiver_distances(
        farm_controls, external_receiver_reference, external_reference_provenance
    )
    farm_month = build_farm_month_observability_bias_features(
        manifest,
        farm_controls,
        dwell=dwell,
        turbine_events=turbine_events,
        direct_receiver_metadata_available=direct_receiver_metadata_available,
    )
    farm_summary = build_farm_observability_bias_summary(farm_month)
    geographic_summary = build_geographic_source_intensity_summary(farm_month)
    distance_stratum_diagnostic = build_base_station_distance_stratum_diagnostic(farm_month)
    distance_gradient_summary = build_base_station_distance_gradient_summary(
        distance_stratum_diagnostic
    )
    validation = _validation_summary(
        farm_month,
        farm_summary,
        receiver_inventory,
        raw_schema_summary,
        base_station_catalogue,
        distance_stratum_diagnostic,
        distance_gradient_summary,
    )

    processed_output_dir.mkdir(parents=True, exist_ok=True)
    report_output_dir.mkdir(parents=True, exist_ok=True)
    methodology_path.parent.mkdir(parents=True, exist_ok=True)

    farm_month_path = processed_output_dir / "farm_month_observability_bias_features.csv"
    farm_summary_path = processed_output_dir / "farm_observability_bias_summary.csv"
    receiver_inventory_path = processed_output_dir / "receiver_metadata_inventory.csv"
    base_station_catalogue_path = processed_output_dir / "ais_base_station_geometry_catalogue.csv"
    distance_stratum_path = processed_output_dir / "base_station_distance_stratum_diagnostic.csv"
    distance_gradient_path = processed_output_dir / "base_station_distance_gradient_summary.csv"
    report_path = report_output_dir / "ais_receiver_distance_observability_report.md"

    farm_month.to_csv(farm_month_path, index=False)
    farm_summary.to_csv(farm_summary_path, index=False)
    receiver_inventory.to_csv(receiver_inventory_path, index=False)
    base_station_catalogue.to_csv(base_station_catalogue_path, index=False)
    distance_stratum_diagnostic.to_csv(distance_stratum_path, index=False)
    distance_gradient_summary.to_csv(distance_gradient_path, index=False)
    report_path.write_text(
        build_report_text(
            farm_month=farm_month,
            farm_summary=farm_summary,
            receiver_inventory=receiver_inventory,
            geographic_summary=geographic_summary,
            validation=validation,
            base_station_catalogue=base_station_catalogue,
            distance_stratum_diagnostic=distance_stratum_diagnostic,
            distance_gradient_summary=distance_gradient_summary,
        ),
        encoding="utf-8",
    )
    methodology_path.write_text(build_methodology_text(), encoding="utf-8")

    return AisObservabilityBiasOutputs(
        processed_output_dir=processed_output_dir,
        report_output_dir=report_output_dir,
        files={
            "farm_month_observability_bias_features_csv": farm_month_path,
            "farm_observability_bias_summary_csv": farm_summary_path,
            "receiver_metadata_inventory_csv": receiver_inventory_path,
            "ais_base_station_geometry_catalogue_csv": base_station_catalogue_path,
            "base_station_distance_stratum_diagnostic_csv": distance_stratum_path,
            "base_station_distance_gradient_summary_csv": distance_gradient_path,
            "ais_receiver_distance_observability_report_md": report_path,
            "methodology_md": methodology_path,
        },
        validation={key: _jsonable(value) for key, value in validation.items()},
    )
