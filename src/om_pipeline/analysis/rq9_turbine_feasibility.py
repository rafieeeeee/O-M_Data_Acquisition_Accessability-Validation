"""Turbine-level feasibility v0 for RQ9 maintenance intervention intensity.

This module assigns existing Tier A AIS dwell candidate interventions to the
nearest turbine where the current data support that assignment. It is a
feasibility layer, not a final turbine-level model and not confirmed failure
rate inference.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from om_pipeline.analysis.rq9_intervention_intensity import (
    DEFAULT_RAMP_UP_MONTHS,
    build_farm_operational_metadata,
    read_dwell_table,
    read_turbine_coordinates,
)


ANALYSIS_LABEL = "RQ9 turbine-level maintenance intervention feasibility v0"
TIER_A = "Tier A"
HIGH_CONFIDENCE_DISTANCE_M = 200.0
MEDIUM_CONFIDENCE_DISTANCE_M = 500.0
EARTH_RADIUS_M = 6_371_000.0

TURBINE_EVENT_COLUMNS = [
    "analysis_label",
    "dwell_id",
    "visit_id",
    "source_event_row",
    "farm_id",
    "wind_farm",
    "mmsi",
    "dwell_tier",
    "start_utc",
    "end_utc",
    "duration_min",
    "centroid_lat",
    "centroid_lon",
    "lifecycle_phase",
    "farm_operational_status_at_event",
    "interpretation_period",
    "possible_cross_farm_duplicate",
    "duplicate_group_id",
    "duplicate_farm_ids",
    "duplicate_adjusted_event_weight",
    "nearest_turbine_id",
    "nearest_turbine_source_row",
    "distance_to_turbine_m",
    "assigned_turbine_id",
    "assigned_turbine_source_row",
    "assignment_confidence",
    "assignment_supports_turbine_level",
    "assigned_turbine_latitude",
    "assigned_turbine_longitude",
    "assigned_turbine_country",
    "assigned_turbine_commissioning_date",
    "assigned_turbine_oem_manufacturer",
    "assigned_turbine_type",
    "assigned_turbine_rated_power",
    "assigned_turbine_rotor_diameter",
    "assigned_turbine_hub_height",
]

TURBINE_METADATA_FIELDS = [
    ("turbine_source_id", "Unnamed: 0"),
    ("farm_name", "wind_farm"),
    ("country", "country"),
    ("latitude", "latitude"),
    ("longitude", "longitude"),
    ("commissioning_date", "commissioning_date"),
    ("oem_manufacturer", "oem_manufacturer"),
    ("turbine_model", "turbine_type"),
    ("rated_capacity", "rated_power"),
    ("rotor_diameter", "rotor_diameter"),
    ("hub_height", "hub_height"),
]

DWELL_SUITABILITY_FIELDS = [
    ("event id", ("dwell_id", "visit_id")),
    ("farm id/name", ("farm_id", "wind_farm")),
    ("tier", ("dwell_tier",)),
    ("start/end time", ("start_utc", "end_utc")),
    ("duration", ("duration_min",)),
    ("lat/lon", ("centroid_lat", "centroid_lon")),
    ("MMSI", ("mmsi",)),
    ("duplicate group", ("duplicate_group_id", "possible_cross_farm_duplicate")),
    ("source phase label", ("farm_operational_status_at_event", "interpretation_period")),
]


@dataclass(frozen=True)
class RQ9TurbineFeasibilityOutputs:
    """Paths and summary values written by the turbine feasibility builder."""

    processed_output_dir: Path
    report_output_dir: Path
    files: dict[str, Path]
    validation: dict[str, Any]


def _require_columns(df: pd.DataFrame, required: set[str], context: str) -> None:
    missing = sorted(required - set(df.columns))
    if missing:
        raise ValueError(f"{context} is missing required columns: {missing}")


def _jsonable(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        return float(value)
    if isinstance(value, np.bool_):
        return bool(value)
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
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


def _value_counts_dict(series: pd.Series) -> dict[str, int]:
    return {str(key): int(value) for key, value in series.value_counts(dropna=False).items()}


def _coerce_bool(series: pd.Series) -> pd.Series:
    if pd.api.types.is_bool_dtype(series):
        return series.fillna(False)
    normalized = series.astype("string").str.strip().str.lower()
    return normalized.isin({"true", "1", "yes", "y"})


def _parse_month_start(series: pd.Series) -> pd.Series:
    normalized = series.astype("string").str.strip()
    parsed = pd.to_datetime(normalized, errors="coerce", utc=True)
    parsed = parsed.dt.tz_convert(None)
    return parsed.dt.to_period("M").dt.to_timestamp()


def _format_month_start(series: pd.Series) -> pd.Series:
    parsed = pd.to_datetime(series, errors="coerce")
    return parsed.dt.to_period("M").dt.strftime("%Y-%m")


def _haversine_distance_m(
    event_lat: pd.Series,
    event_lon: pd.Series,
    turbine_lat: np.ndarray,
    turbine_lon: np.ndarray,
) -> np.ndarray:
    event_lat_rad = np.radians(event_lat.to_numpy(dtype=float))[:, None]
    event_lon_rad = np.radians(event_lon.to_numpy(dtype=float))[:, None]
    turbine_lat_rad = np.radians(turbine_lat.astype(float))[None, :]
    turbine_lon_rad = np.radians(turbine_lon.astype(float))[None, :]
    dlat = turbine_lat_rad - event_lat_rad
    dlon = turbine_lon_rad - event_lon_rad
    a = (
        np.sin(dlat / 2.0) ** 2
        + np.cos(event_lat_rad) * np.cos(turbine_lat_rad) * np.sin(dlon / 2.0) ** 2
    )
    return 2.0 * EARTH_RADIUS_M * np.arcsin(np.sqrt(a))


def read_farm_intensity(path: Path) -> pd.DataFrame:
    """Read the existing farm-level RQ9 intensity output."""
    if not path.exists():
        raise FileNotFoundError(f"RQ9 farm intensity input not found: {path}")
    df = pd.read_csv(path)
    _require_columns(df, {"farm_id"}, "RQ9 farm intensity input")
    return df


def prepare_turbine_metadata(turbines: pd.DataFrame) -> pd.DataFrame:
    """Normalize turbine metadata and derive a stable v0 turbine identifier."""
    _require_columns(
        turbines,
        {"wind_farm", "latitude", "longitude"},
        "RQ9 turbine coordinate input",
    )
    working = turbines.copy()
    if "Unnamed: 0" in working.columns:
        working["turbine_source_row"] = working["Unnamed: 0"]
    else:
        working["turbine_source_row"] = working.index
    working["farm_id"] = working["wind_farm"].astype("string")
    working["turbine_source_row"] = working["turbine_source_row"].astype("string")
    working["turbine_id"] = (
        working["farm_id"].str.replace(r"\s+", "_", regex=True)
        + "::"
        + working["turbine_source_row"]
    )
    working["latitude"] = pd.to_numeric(working["latitude"], errors="coerce")
    working["longitude"] = pd.to_numeric(working["longitude"], errors="coerce")
    if "commissioning_date" in working.columns:
        working["turbine_commissioning_month"] = _format_month_start(
            _parse_month_start(working["commissioning_date"])
        )
    else:
        working["turbine_commissioning_month"] = pd.NA
    return working


def _assign_lifecycle_phase(events: pd.DataFrame, farm_metadata: pd.DataFrame) -> pd.DataFrame:
    metadata_columns = [
        "farm_id",
        "farm_commissioning_start_month",
        "farm_commissioning_end_month",
        "steady_operational_start_month",
        "operational_window_known",
    ]
    available = [column for column in metadata_columns if column in farm_metadata.columns]
    working = events.merge(farm_metadata[available], on="farm_id", how="left")
    event_month = _parse_month_start(working["start_utc"]) if "start_utc" in working else pd.NaT
    commissioning_start = pd.to_datetime(
        working.get("farm_commissioning_start_month"),
        errors="coerce",
    )
    steady_start = pd.to_datetime(
        working.get("steady_operational_start_month"),
        errors="coerce",
    )
    known = working.get("operational_window_known")
    if known is None:
        known = pd.Series(False, index=working.index)
    else:
        known = known.fillna(False).astype(bool)

    phase = pd.Series("unknown_phase", index=working.index, dtype="string")
    classifiable = known & event_month.notna() & commissioning_start.notna() & steady_start.notna()
    phase.loc[classifiable & (event_month < commissioning_start)] = "pre_operational"
    phase.loc[classifiable & (event_month >= commissioning_start) & (event_month < steady_start)] = (
        "commissioning_ramp_up"
    )
    phase.loc[classifiable & (event_month >= steady_start)] = "steady_operational"
    working["lifecycle_phase"] = phase
    return working


def build_dwell_column_suitability(dwell: pd.DataFrame) -> pd.DataFrame:
    """Summarize whether existing dwell columns support turbine feasibility v0."""
    rows: list[dict[str, Any]] = []
    total_rows = len(dwell)
    for concept, columns in DWELL_SUITABILITY_FIELDS:
        present_columns = [column for column in columns if column in dwell.columns]
        missing_columns = [column for column in columns if column not in dwell.columns]
        if present_columns:
            non_null = dwell[present_columns].notna().all(axis=1)
            non_null_count = int(non_null.sum())
            non_null_share = float(non_null_count / total_rows) if total_rows else np.nan
        else:
            non_null_count = 0
            non_null_share = np.nan
        rows.append(
            {
                "concept": concept,
                "required_columns": ", ".join(columns),
                "present_columns": ", ".join(present_columns),
                "missing_columns": ", ".join(missing_columns),
                "present": len(missing_columns) == 0,
                "non_null_count": non_null_count,
                "total_rows": total_rows,
                "non_null_share": non_null_share,
            }
        )
    return pd.DataFrame(rows)


def build_turbine_metadata_completeness(turbines: pd.DataFrame) -> pd.DataFrame:
    """Summarize turbine metadata completeness for turbine-level RQ9 questions."""
    total_rows = len(turbines)
    rows: list[dict[str, Any]] = []
    for metadata_field, source_column in TURBINE_METADATA_FIELDS:
        present = source_column in turbines.columns
        if present:
            non_null_count = int(turbines[source_column].notna().sum())
            unique_count = int(turbines[source_column].nunique(dropna=True))
        else:
            non_null_count = 0
            unique_count = 0
        non_null_share = float(non_null_count / total_rows) if total_rows else np.nan
        status = "complete" if present and non_null_count == total_rows else "incomplete"
        rows.append(
            {
                "metadata_field": metadata_field,
                "source_column": source_column,
                "present": present,
                "non_null_count": non_null_count,
                "total_rows": total_rows,
                "non_null_share": non_null_share,
                "unique_count": unique_count,
                "status": status,
            }
        )
    return pd.DataFrame(rows)


def _apply_duplicate_weights(events: pd.DataFrame) -> pd.DataFrame:
    working = events.copy()
    working["duplicate_adjusted_event_weight"] = 1.0
    if {"duplicate_group_id", "possible_cross_farm_duplicate"}.issubset(working.columns):
        duplicate_mask = (
            _coerce_bool(working["possible_cross_farm_duplicate"])
            & working["duplicate_group_id"].notna()
        )
        if duplicate_mask.any():
            farm_counts = (
                working.loc[duplicate_mask]
                .groupby("duplicate_group_id")["farm_id"]
                .transform("nunique")
                .replace(0, 1)
            )
            working.loc[duplicate_mask, "duplicate_adjusted_event_weight"] = 1.0 / farm_counts
    return working


def _assign_nearest_turbines(events: pd.DataFrame, turbines: pd.DataFrame) -> pd.DataFrame:
    working = events.copy()
    assignment_columns = [
        "nearest_turbine_id",
        "nearest_turbine_source_row",
        "distance_to_turbine_m",
        "assigned_turbine_id",
        "assigned_turbine_source_row",
    ]
    for column in assignment_columns:
        working[column] = pd.NA
    working["assignment_confidence"] = "unassigned"
    working["assignment_supports_turbine_level"] = False

    turbines_by_farm = {
        str(farm_id): group.reset_index(drop=True)
        for farm_id, group in turbines.dropna(subset=["farm_id"]).groupby("farm_id")
    }

    for farm_id, event_group in working.groupby("farm_id", dropna=False):
        farm_key = str(farm_id)
        farm_turbines = turbines_by_farm.get(farm_key)
        if farm_turbines is None or farm_turbines.empty:
            continue
        valid_event_mask = (
            event_group["centroid_lat"].notna()
            & event_group["centroid_lon"].notna()
            & farm_turbines["latitude"].notna().any()
            & farm_turbines["longitude"].notna().any()
        )
        valid_events = event_group.loc[valid_event_mask]
        valid_turbines = farm_turbines.dropna(subset=["latitude", "longitude"]).reset_index(
            drop=True
        )
        if valid_events.empty or valid_turbines.empty:
            continue

        distances = _haversine_distance_m(
            valid_events["centroid_lat"],
            valid_events["centroid_lon"],
            valid_turbines["latitude"].to_numpy(),
            valid_turbines["longitude"].to_numpy(),
        )
        nearest_positions = distances.argmin(axis=1)
        nearest_distances = distances[np.arange(len(valid_events)), nearest_positions]
        nearest_turbines = valid_turbines.iloc[nearest_positions].reset_index(drop=True)
        nearest_index = valid_events.index

        working.loc[nearest_index, "nearest_turbine_id"] = nearest_turbines["turbine_id"].to_numpy()
        working.loc[nearest_index, "nearest_turbine_source_row"] = nearest_turbines[
            "turbine_source_row"
        ].to_numpy()
        working.loc[nearest_index, "distance_to_turbine_m"] = nearest_distances

        high_mask = nearest_distances <= HIGH_CONFIDENCE_DISTANCE_M
        medium_mask = (
            (nearest_distances > HIGH_CONFIDENCE_DISTANCE_M)
            & (nearest_distances <= MEDIUM_CONFIDENCE_DISTANCE_M)
        )
        assign_mask = high_mask | medium_mask
        assigned_index = nearest_index[assign_mask]
        working.loc[assigned_index, "assigned_turbine_id"] = nearest_turbines.loc[
            assign_mask,
            "turbine_id",
        ].to_numpy()
        working.loc[assigned_index, "assigned_turbine_source_row"] = nearest_turbines.loc[
            assign_mask,
            "turbine_source_row",
        ].to_numpy()
        working.loc[nearest_index[high_mask], "assignment_confidence"] = "high"
        working.loc[nearest_index[medium_mask], "assignment_confidence"] = "medium"
        working.loc[assigned_index, "assignment_supports_turbine_level"] = True

    return working


def build_turbine_intervention_events(
    dwell: pd.DataFrame,
    turbines: pd.DataFrame,
    ramp_up_months: int = DEFAULT_RAMP_UP_MONTHS,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Assign Tier A candidate intervention events to nearest turbines for v0 feasibility."""
    _require_columns(
        dwell,
        {"farm_id", "dwell_tier", "centroid_lat", "centroid_lon", "duration_min"},
        "RQ9 dwell table",
    )
    turbine_metadata = prepare_turbine_metadata(turbines)
    farm_metadata = build_farm_operational_metadata(turbines, ramp_up_months=ramp_up_months)

    working = dwell.copy()
    working["source_event_row"] = working.index
    working["farm_id"] = working["farm_id"].astype("string")
    working["dwell_tier"] = working["dwell_tier"].astype("string")
    working["centroid_lat"] = pd.to_numeric(working["centroid_lat"], errors="coerce")
    working["centroid_lon"] = pd.to_numeric(working["centroid_lon"], errors="coerce")
    tier_a = working.loc[working["dwell_tier"].eq(TIER_A)].copy()
    tier_a = _assign_lifecycle_phase(tier_a, farm_metadata)
    tier_a = _apply_duplicate_weights(tier_a)
    tier_a = _assign_nearest_turbines(tier_a, turbine_metadata)

    assigned_metadata = turbine_metadata[
        [
            "turbine_id",
            "latitude",
            "longitude",
            "country",
            "commissioning_date",
            "oem_manufacturer",
            "turbine_type",
            "rated_power",
            "rotor_diameter",
            "hub_height",
        ]
    ].rename(
        columns={
            "turbine_id": "assigned_turbine_id",
            "latitude": "assigned_turbine_latitude",
            "longitude": "assigned_turbine_longitude",
            "country": "assigned_turbine_country",
            "commissioning_date": "assigned_turbine_commissioning_date",
            "oem_manufacturer": "assigned_turbine_oem_manufacturer",
            "turbine_type": "assigned_turbine_type",
            "rated_power": "assigned_turbine_rated_power",
            "rotor_diameter": "assigned_turbine_rotor_diameter",
            "hub_height": "assigned_turbine_hub_height",
        }
    )
    tier_a = tier_a.merge(assigned_metadata, on="assigned_turbine_id", how="left")
    tier_a["analysis_label"] = ANALYSIS_LABEL

    for column in TURBINE_EVENT_COLUMNS:
        if column not in tier_a.columns:
            tier_a[column] = pd.NA
    tier_a["assignment_supports_turbine_level"] = tier_a[
        "assignment_supports_turbine_level"
    ].fillna(False).astype(bool)
    tier_a["distance_to_turbine_m"] = pd.to_numeric(
        tier_a["distance_to_turbine_m"],
        errors="coerce",
    )
    output = tier_a[TURBINE_EVENT_COLUMNS].sort_values(
        ["farm_id", "start_utc", "dwell_id"],
        na_position="last",
    )
    output = output.reset_index(drop=True)

    metrics = {
        "dwell_input_rows": int(len(dwell)),
        "tier_a_event_rows": int(len(output)),
        "turbine_input_rows": int(len(turbines)),
        "turbine_farm_count": int(turbines["wind_farm"].nunique())
        if "wind_farm" in turbines
        else None,
        "event_farm_count": int(output["farm_id"].nunique()) if not output.empty else 0,
        "assigned_event_rows": int(output["assignment_supports_turbine_level"].sum()),
        "high_confidence_event_rows": int(output["assignment_confidence"].eq("high").sum()),
        "medium_confidence_event_rows": int(output["assignment_confidence"].eq("medium").sum()),
        "unassigned_event_rows": int(output["assignment_confidence"].eq("unassigned").sum()),
        "duplicate_adjusted_event_weight_total": float(
            output["duplicate_adjusted_event_weight"].sum()
        )
        if not output.empty
        else 0.0,
        "duplicate_group_rows": int(output["duplicate_group_id"].notna().sum())
        if "duplicate_group_id" in output
        else 0,
        "phase_counts": _value_counts_dict(output["lifecycle_phase"]) if not output.empty else {},
        "assignment_confidence_counts": _value_counts_dict(output["assignment_confidence"])
        if not output.empty
        else {},
    }
    return output, metrics


def build_answerability_matrix(
    turbine_events: pd.DataFrame,
    metadata_completeness: pd.DataFrame,
    farm_intensity: pd.DataFrame,
) -> pd.DataFrame:
    """Classify which RQ9/RQ12 turbine-level questions are answerable now."""
    assigned = int(turbine_events["assignment_supports_turbine_level"].sum())
    high = int(turbine_events["assignment_confidence"].eq("high").sum())
    steady_assigned = int(
        (
            turbine_events["assignment_supports_turbine_level"]
            & turbine_events["lifecycle_phase"].eq("steady_operational")
        ).sum()
    )
    farm_count = int(farm_intensity["farm_id"].nunique()) if "farm_id" in farm_intensity else 0
    complete_fields = set(
        metadata_completeness.loc[
            metadata_completeness["status"].eq("complete"),
            "metadata_field",
        ]
    )
    has_oem = "oem_manufacturer" in complete_fields
    has_capacity = "rated_capacity" in complete_fields
    has_commissioning = "commissioning_date" in complete_fields

    rows = [
        {
            "question": "turbine-level intervention intensity",
            "status": "partially ready",
            "required_present": "Tier A event coordinates; turbine coordinates; assignment confidence",
            "required_missing": "turbine-year denominator and validation against work orders/SCADA",
            "sample_size": assigned,
            "confidence_level": "medium" if assigned and high / max(assigned, 1) >= 0.5 else "low",
            "next_increment": "build turbine observed-year denominators and aggregate high/medium assignments",
        },
        {
            "question": "bathtub/age curve",
            "status": "partially ready" if has_commissioning and steady_assigned else "blocked",
            "required_present": "commissioning dates; lifecycle phase labels; assigned steady events",
            "required_missing": "age-band turbine-year denominators and validation labels",
            "sample_size": steady_assigned,
            "confidence_level": "medium" if has_commissioning and steady_assigned else "low",
            "next_increment": "derive turbine age bands over observed months",
        },
        {
            "question": "farm-to-farm comparison",
            "status": "ready",
            "required_present": "farm-level phase-separated intensity and turbine assignment feasibility",
            "required_missing": "minimum-exposure/capping sensitivity for simulator calibration",
            "sample_size": farm_count,
            "confidence_level": "medium",
            "next_increment": "use steady farm-level field with min-observed-year sensitivity",
        },
        {
            "question": "exposure comparison",
            "status": "partially ready",
            "required_present": "assigned turbine events and event-level metocean fields",
            "required_missing": "static turbine exposure features such as edge distance, depth, and long-run exposure",
            "sample_size": assigned,
            "confidence_level": "low",
            "next_increment": "join turbine layout exposure and depth features",
        },
        {
            "question": "Baltic vs North Sea comparison",
            "status": "partially ready",
            "required_present": "country and farm metadata",
            "required_missing": "explicit sea-basin/region mapping",
            "sample_size": assigned,
            "confidence_level": "low",
            "next_increment": "add reviewed Baltic/North Sea farm-region mapping",
        },
        {
            "question": "OEM comparison",
            "status": "partially ready" if has_oem else "blocked",
            "required_present": "OEM/manufacturer metadata and assigned turbine events",
            "required_missing": "OEM turbine-year denominators and metadata harmonization",
            "sample_size": assigned,
            "confidence_level": "medium" if has_oem and assigned else "low",
            "next_increment": "aggregate assigned events and denominators by OEM",
        },
        {
            "question": "turbine capacity comparison",
            "status": "partially ready" if has_capacity else "blocked",
            "required_present": "rated capacity metadata and assigned turbine events",
            "required_missing": "capacity-band turbine-year denominators",
            "sample_size": assigned,
            "confidence_level": "medium" if has_capacity and assigned else "low",
            "next_increment": "aggregate assigned events and denominators by rated-capacity band",
        },
    ]
    return pd.DataFrame(rows)


def _markdown_table(frame: pd.DataFrame, columns: list[str]) -> str:
    if frame.empty:
        return "_No rows._"
    subset = frame[columns].copy()
    return subset.to_markdown(index=False)


def build_turbine_feasibility_report(
    turbine_events: pd.DataFrame,
    metadata_completeness: pd.DataFrame,
    dwell_suitability: pd.DataFrame,
    answerability: pd.DataFrame,
    metrics: dict[str, Any],
) -> str:
    """Render a compact Markdown feasibility report."""
    assignment_counts = turbine_events["assignment_confidence"].value_counts().rename_axis(
        "assignment_confidence"
    )
    phase_counts = turbine_events["lifecycle_phase"].value_counts().rename_axis(
        "lifecycle_phase"
    )
    top_farms = (
        turbine_events.loc[turbine_events["assignment_supports_turbine_level"]]
        .groupby("farm_id")
        .size()
        .sort_values(ascending=False)
        .head(15)
        .reset_index(name="assigned_tier_a_events")
    )
    high_share = (
        metrics["high_confidence_event_rows"] / metrics["assigned_event_rows"]
        if metrics["assigned_event_rows"]
        else np.nan
    )
    sections = [
        "# RQ9 turbine-level intervention feasibility v0",
        "",
        "This report checks whether existing AIS dwell evidence can support a "
        "turbine-level maintenance intervention intensity increment. It does "
        "not infer confirmed failure rate; SCADA, fault-log, work-order, or "
        "equivalent validation remains required.",
        "",
        "## Assignment summary",
        "",
        f"- Tier A event rows inspected: {metrics['tier_a_event_rows']}",
        f"- Events assigned within 500 m: {metrics['assigned_event_rows']}",
        f"- High-confidence events within 200 m: {metrics['high_confidence_event_rows']}",
        f"- Medium-confidence events from 200 to 500 m: {metrics['medium_confidence_event_rows']}",
        f"- Unassigned events over 500 m or missing coordinates: {metrics['unassigned_event_rows']}",
        f"- High-confidence share of assigned events: {high_share:.3f}"
        if not pd.isna(high_share)
        else "- High-confidence share of assigned events: n/a",
        f"- Duplicate-adjusted Tier A event weight total: "
        f"{metrics['duplicate_adjusted_event_weight_total']:.3f}",
        "",
        "## Assignment confidence distribution",
        "",
        assignment_counts.reset_index(name="events").to_markdown(index=False),
        "",
        "## Lifecycle phase distribution",
        "",
        phase_counts.reset_index(name="events").to_markdown(index=False),
        "",
        "## Top farms by assigned Tier A events",
        "",
        _markdown_table(top_farms, ["farm_id", "assigned_tier_a_events"]),
        "",
        "## Dwell/event column suitability",
        "",
        _markdown_table(
            dwell_suitability,
            [
                "concept",
                "present_columns",
                "missing_columns",
                "non_null_share",
            ],
        ),
        "",
        "## Turbine metadata completeness",
        "",
        _markdown_table(
            metadata_completeness,
            [
                "metadata_field",
                "source_column",
                "present",
                "non_null_share",
                "unique_count",
                "status",
            ],
        ),
        "",
        "## Answerability matrix",
        "",
        _markdown_table(
            answerability,
            [
                "question",
                "status",
                "sample_size",
                "confidence_level",
                "required_missing",
                "next_increment",
            ],
        ),
        "",
        "## Recommendation",
        "",
        "The next analysis question to answer first is whether exposed turbines "
        "have higher steady-operational maintenance intervention intensity. "
        "That requires a turbine-year denominator plus static exposure features "
        "before using this v0 assignment as simulator demand evidence.",
        "",
    ]
    return "\n".join(sections)


def build_rq9_turbine_feasibility_outputs(
    dwell_path: Path,
    turbine_path: Path,
    farm_intensity_path: Path,
    processed_output_dir: Path,
    report_output_dir: Path,
    ramp_up_months: int = DEFAULT_RAMP_UP_MONTHS,
) -> RQ9TurbineFeasibilityOutputs:
    """Build turbine feasibility v0 outputs from existing RQ9 inputs."""
    dwell = read_dwell_table(dwell_path)
    turbines = read_turbine_coordinates(turbine_path)
    farm_intensity = read_farm_intensity(farm_intensity_path)

    turbine_events, assignment_metrics = build_turbine_intervention_events(
        dwell,
        turbines,
        ramp_up_months=ramp_up_months,
    )
    metadata_completeness = build_turbine_metadata_completeness(turbines)
    dwell_suitability = build_dwell_column_suitability(dwell)
    answerability = build_answerability_matrix(
        turbine_events,
        metadata_completeness,
        farm_intensity,
    )
    report = build_turbine_feasibility_report(
        turbine_events,
        metadata_completeness,
        dwell_suitability,
        answerability,
        assignment_metrics,
    )

    processed_output_dir.mkdir(parents=True, exist_ok=True)
    report_output_dir.mkdir(parents=True, exist_ok=True)
    event_path = processed_output_dir / "turbine_intervention_events_v0.csv"
    metadata_path = report_output_dir / "turbine_metadata_completeness.csv"
    report_path = report_output_dir / "turbine_level_feasibility_report.md"
    turbine_events.to_csv(event_path, index=False)
    metadata_completeness.to_csv(metadata_path, index=False)
    report_path.write_text(report, encoding="utf-8")

    validation = {
        **assignment_metrics,
        "analysis_label": ANALYSIS_LABEL,
        "farm_intensity_rows": int(len(farm_intensity)),
        "metadata_complete_field_count": int(
            metadata_completeness["status"].eq("complete").sum()
        ),
        "metadata_field_count": int(len(metadata_completeness)),
        "answerability_status_counts": _value_counts_dict(answerability["status"]),
        "dwell_column_suitability": dwell_suitability.to_dict(orient="records"),
        "metadata_completeness": metadata_completeness.to_dict(orient="records"),
        "answerability": answerability.to_dict(orient="records"),
    }
    files = {
        "turbine_intervention_events_v0_csv": event_path,
        "turbine_metadata_completeness_csv": metadata_path,
        "turbine_level_feasibility_report_md": report_path,
    }
    return RQ9TurbineFeasibilityOutputs(
        processed_output_dir=processed_output_dir,
        report_output_dir=report_output_dir,
        files=files,
        validation=_jsonable(validation),
    )
