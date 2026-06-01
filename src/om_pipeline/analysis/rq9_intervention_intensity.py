"""Farm-level RQ9 maintenance intervention intensity builder.

This module estimates farm-level maintenance intervention intensity from
existing AIS dwell outputs and the AIS backfill manifest. It does not infer
confirmed failure rate; that requires SCADA, fault-log, work-order, or
equivalent validation.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


ANALYSIS_LABEL = "RQ9 farm-level maintenance intervention intensity"
OBSERVED_STATUSES = frozenset({"success", "success_no_ais_in_bbox"})
MISSING_SOURCE_STATUS = "skipped_missing_source"
CANDIDATE_TIERS = frozenset({"Tier A", "Tier B"})
DEFAULT_LONG_DWELL_THRESHOLD_MIN = 120.0
DEFAULT_RAMP_UP_MONTHS = 6
RAMP_UP_SENSITIVITY_MONTHS = (0, 6, 12)

FARM_INTENSITY_COLUMNS = [
    "analysis_label",
    "farm_id",
    "turbine_count",
    "farm_commissioning_start_month",
    "farm_commissioning_end_month",
    "ramp_up_months",
    "steady_operational_start_month",
    "operational_start_month",
    "operational_start_source",
    "operational_window_known",
    "total_manifest_months",
    "pre_operational_manifest_months",
    "commissioning_manifest_months",
    "steady_manifest_months",
    "unknown_phase_manifest_months",
    "manifest_months",
    "observed_months",
    "observed_years",
    "commissioning_observed_months",
    "commissioning_observed_years",
    "steady_observed_months",
    "steady_observed_years",
    "unknown_phase_observed_months",
    "unknown_phase_observed_years",
    "success_months",
    "success_no_ais_in_bbox_months",
    "skipped_missing_source_months",
    "other_status_months",
    "coverage_share",
    "first_observed_month",
    "last_observed_month",
    "tier_a_visit_count",
    "tier_b_visit_count",
    "candidate_intervention_count",
    "long_dwell_count",
    "unique_vessel_count",
    "pre_operational_candidate_count",
    "commissioning_candidate_count",
    "steady_candidate_count",
    "unknown_phase_candidate_count",
    "commissioning_long_dwell_count",
    "steady_long_dwell_count",
    "unknown_phase_long_dwell_count",
    "candidate_date_missing_count",
    "duplicate_adjustment_available",
    "duplicate_candidate_row_count",
    "duplicate_group_adjusted_candidate_count",
    "commissioning_duplicate_adjusted_count",
    "steady_duplicate_adjusted_count",
    "unknown_phase_duplicate_adjusted_count",
    "duplicate_adjustment_delta",
    "candidate_interventions_per_observed_farm_year",
    "long_dwell_interventions_per_observed_farm_year",
    "commissioning_intervention_intensity_per_farm_year",
    "steady_intervention_intensity_per_farm_year",
    "confidence_class",
    "recommended_simulator_use",
]


@dataclass(frozen=True)
class RQ9FarmOutputs:
    """Paths and summary values written by the farm-level RQ9 builder."""

    processed_output_dir: Path
    report_output_dir: Path
    files: dict[str, Path]
    validation: dict[str, Any]


OPERATIONAL_METADATA_COLUMNS = [
    "farm_id",
    "turbine_count",
    "farm_commissioning_start_month",
    "farm_commissioning_end_month",
    "ramp_up_months",
    "steady_operational_start_month",
    "operational_start_month",
    "operational_start_source",
    "operational_window_known",
]


def _require_columns(df: pd.DataFrame, required: set[str], context: str) -> None:
    missing = sorted(required - set(df.columns))
    if missing:
        raise ValueError(f"{context} is missing required columns: {missing}")


def _ensure_output_path(path: Path, allowed_roots: list[Path]) -> None:
    resolved = path.resolve()
    for root in allowed_roots:
        try:
            resolved.relative_to(root.resolve())
            return
        except ValueError:
            continue
    roots = ", ".join(str(root) for root in allowed_roots)
    raise ValueError(f"Refusing to write outside approved output roots: {path} not in {roots}")


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


def _month_label(year: pd.Series, month: pd.Series) -> pd.Series:
    dates = pd.to_datetime(
        {"year": year.astype("Int64"), "month": month.astype("Int64"), "day": 1},
        errors="coerce",
    )
    return dates.dt.strftime("%Y-%m")


def _parse_month_start(series: pd.Series) -> pd.Series:
    normalized = series.astype("string").str.strip()
    parsed = pd.to_datetime(normalized, errors="coerce", utc=True)
    parsed = parsed.dt.tz_convert(None)
    return parsed.dt.to_period("M").dt.to_timestamp()


def _format_month_start(series: pd.Series) -> pd.Series:
    parsed = pd.to_datetime(series, errors="coerce")
    return parsed.dt.to_period("M").dt.strftime("%Y-%m")


def _add_months(series: pd.Series, months: int) -> pd.Series:
    parsed = pd.to_datetime(series, errors="coerce")
    periods = parsed.dt.to_period("M")
    shifted = periods + int(months)
    return shifted.dt.to_timestamp()


def _assign_lifecycle_phase(frame: pd.DataFrame, month_column: str) -> pd.Series:
    """Classify a farm-month row into commissioning-aware RQ9 lifecycle phase."""
    month_start = pd.to_datetime(frame[month_column], errors="coerce")
    commissioning_start = pd.to_datetime(
        frame.get("farm_commissioning_start_month"),
        errors="coerce",
    )
    steady_start = pd.to_datetime(
        frame.get("steady_operational_start_month"),
        errors="coerce",
    )
    operational_window_known = frame.get("operational_window_known")
    if operational_window_known is None:
        known = pd.Series(False, index=frame.index)
    else:
        known = operational_window_known.fillna(False).astype(bool)

    phase = pd.Series("unknown_phase", index=frame.index, dtype="string")
    classifiable = (
        known
        & month_start.notna()
        & commissioning_start.notna()
        & steady_start.notna()
    )
    phase.loc[classifiable & (month_start < commissioning_start)] = "pre_operational"
    phase.loc[
        classifiable
        & (month_start >= commissioning_start)
        & (month_start < steady_start)
    ] = "commissioning_ramp_up"
    phase.loc[classifiable & (month_start >= steady_start)] = "steady_operational"
    return phase


def _merge_operational_metadata(
    frame: pd.DataFrame,
    operational_metadata: pd.DataFrame | None,
) -> pd.DataFrame:
    if operational_metadata is None or operational_metadata.empty:
        frame = frame.copy()
        frame["turbine_count"] = pd.NA
        frame["farm_commissioning_start_month"] = pd.NA
        frame["farm_commissioning_end_month"] = pd.NA
        frame["ramp_up_months"] = DEFAULT_RAMP_UP_MONTHS
        frame["steady_operational_start_month"] = pd.NA
        frame["operational_start_month"] = pd.NA
        frame["operational_start_source"] = "missing_turbine_metadata"
        frame["operational_window_known"] = False
        return frame

    metadata_columns = [
        column for column in OPERATIONAL_METADATA_COLUMNS if column in operational_metadata.columns
    ]
    metadata = operational_metadata[metadata_columns].copy()
    metadata["farm_id"] = metadata["farm_id"].astype("string")
    merged = frame.merge(metadata, on="farm_id", how="left")
    merged["operational_start_source"] = merged["operational_start_source"].fillna(
        "missing_turbine_metadata"
    )
    merged["ramp_up_months"] = merged["ramp_up_months"].fillna(DEFAULT_RAMP_UP_MONTHS).astype(int)
    merged["operational_window_known"] = (
        merged["operational_window_known"].where(
            merged["operational_window_known"].notna(),
            False,
        )
        .astype(bool)
    )
    return merged


def _safe_divide(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    denominator = pd.to_numeric(denominator, errors="coerce")
    numerator = pd.to_numeric(numerator, errors="coerce")
    return numerator.where(denominator > 0) / denominator.where(denominator > 0)


def _coerce_bool(series: pd.Series) -> pd.Series:
    if pd.api.types.is_bool_dtype(series):
        return series.fillna(False)
    normalized = series.astype("string").str.strip().str.lower()
    return normalized.isin({"true", "1", "yes", "y"})


def read_dwell_table(path: Path) -> pd.DataFrame:
    """Read the existing weather-joined dwell parquet."""
    if not path.exists():
        raise FileNotFoundError(f"RQ9 dwell input not found: {path}")
    df = pd.read_parquet(path)
    _require_columns(df, {"farm_id", "dwell_tier", "duration_min"}, "RQ9 dwell input")
    return df


def read_manifest(path: Path) -> pd.DataFrame:
    """Read the existing AIS backfill manifest."""
    if not path.exists():
        raise FileNotFoundError(f"RQ9 manifest input not found: {path}")
    df = pd.read_csv(path)
    _require_columns(df, {"farm_id", "year", "month", "status"}, "RQ9 manifest input")
    return df


def read_turbine_coordinates(path: Path) -> pd.DataFrame:
    """Read turbine coordinates for farm counts and operational-window metadata."""
    if not path.exists():
        raise FileNotFoundError(f"RQ9 turbine coordinate input not found: {path}")
    df = pd.read_csv(path)
    _require_columns(df, {"wind_farm"}, "RQ9 turbine coordinate input")
    return df


def build_farm_operational_metadata(
    turbines: pd.DataFrame | None,
    ramp_up_months: int = DEFAULT_RAMP_UP_MONTHS,
) -> pd.DataFrame:
    """Return farm-level turbine counts and operational start metadata."""
    columns = OPERATIONAL_METADATA_COLUMNS
    if turbines is None or turbines.empty:
        return pd.DataFrame(columns=columns)
    _require_columns(turbines, {"wind_farm"}, "RQ9 turbine coordinates")

    working = turbines.dropna(subset=["wind_farm"]).copy()
    working["farm_id"] = working["wind_farm"].astype("string")
    if "commissioning_date" in working.columns:
        working["_commissioning_month"] = _parse_month_start(working["commissioning_date"])
    else:
        working["_commissioning_month"] = pd.NaT

    metadata = (
        working.groupby("farm_id", dropna=False)
        .agg(
            turbine_count=("farm_id", "size"),
            _commissioning_start_dt=("_commissioning_month", "min"),
            _commissioning_end_dt=("_commissioning_month", "max"),
            commissioning_date_parseable_count=("_commissioning_month", "count"),
        )
        .reset_index()
    )
    metadata["farm_commissioning_start_month"] = _format_month_start(
        metadata["_commissioning_start_dt"]
    )
    metadata["farm_commissioning_end_month"] = _format_month_start(
        metadata["_commissioning_end_dt"]
    )
    metadata["operational_window_known"] = (
        metadata["commissioning_date_parseable_count"].fillna(0).astype(int) > 0
    )
    metadata["ramp_up_months"] = int(ramp_up_months)
    metadata["_steady_operational_start_dt"] = _add_months(
        metadata["_commissioning_end_dt"],
        int(ramp_up_months),
    )
    metadata["steady_operational_start_month"] = _format_month_start(
        metadata["_steady_operational_start_dt"]
    )
    metadata["operational_start_month"] = metadata["farm_commissioning_start_month"]
    metadata["operational_start_source"] = np.where(
        metadata["operational_window_known"],
        "turbine_commissioning_date_range",
        "missing_commissioning_date",
    )
    metadata["turbine_count"] = metadata["turbine_count"].astype(int)
    return metadata[columns]


def build_manifest_denominator(
    manifest: pd.DataFrame,
    operational_metadata: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Build farm-level observed month and observed year denominators."""
    _require_columns(manifest, {"farm_id", "year", "month", "status"}, "RQ9 manifest")
    working = manifest.copy()
    working = working.dropna(subset=["farm_id", "year", "month"])
    working["farm_id"] = working["farm_id"].astype("string")
    working["year"] = pd.to_numeric(working["year"], errors="coerce")
    working["month"] = pd.to_numeric(working["month"], errors="coerce")
    working = working.dropna(subset=["year", "month"])
    working["year"] = working["year"].astype(int)
    working["month"] = working["month"].astype(int)
    working["status"] = working["status"].astype("string").fillna("__missing_status__")

    farm_month_status = working.drop_duplicates(["farm_id", "year", "month", "status"])
    status_sets = (
        farm_month_status.groupby(["farm_id", "year", "month"], dropna=False)["status"]
        .agg(lambda values: frozenset(str(value) for value in values))
        .reset_index(name="status_set")
    )
    status_sets = _merge_operational_metadata(status_sets, operational_metadata)
    status_sets["month_label"] = _month_label(status_sets["year"], status_sets["month"])
    status_sets["month_start"] = pd.to_datetime(
        {"year": status_sets["year"], "month": status_sets["month"], "day": 1},
        errors="coerce",
    )
    status_sets["lifecycle_phase"] = _assign_lifecycle_phase(status_sets, "month_start")
    status_sets["is_observed"] = status_sets["status_set"].map(
        lambda statuses: bool(OBSERVED_STATUSES.intersection(statuses))
    )
    status_sets["has_success"] = status_sets["status_set"].map(lambda statuses: "success" in statuses)
    status_sets["has_success_no_ais"] = status_sets["status_set"].map(
        lambda statuses: "success_no_ais_in_bbox" in statuses
    )
    status_sets["has_missing_source"] = status_sets["status_set"].map(
        lambda statuses: MISSING_SOURCE_STATUS in statuses
    )
    status_sets["has_other_status"] = ~(
        status_sets["is_observed"] | status_sets["has_missing_source"]
    )
    status_sets["pre_operational_month"] = status_sets["lifecycle_phase"].eq(
        "pre_operational"
    )
    status_sets["commissioning_month"] = status_sets["lifecycle_phase"].eq(
        "commissioning_ramp_up"
    )
    status_sets["steady_month"] = status_sets["lifecycle_phase"].eq("steady_operational")
    status_sets["unknown_phase_month"] = status_sets["lifecycle_phase"].eq("unknown_phase")
    status_sets["in_reported_denominator"] = ~status_sets["pre_operational_month"]
    status_sets["observed_in_window"] = (
        status_sets["in_reported_denominator"] & status_sets["is_observed"]
    )
    status_sets["commissioning_observed"] = (
        status_sets["commissioning_month"] & status_sets["is_observed"]
    )
    status_sets["steady_observed"] = status_sets["steady_month"] & status_sets["is_observed"]
    status_sets["unknown_phase_observed"] = (
        status_sets["unknown_phase_month"] & status_sets["is_observed"]
    )
    status_sets["success_in_window"] = (
        status_sets["in_reported_denominator"] & status_sets["has_success"]
    )
    status_sets["success_no_ais_in_window"] = (
        status_sets["in_reported_denominator"] & status_sets["has_success_no_ais"]
    )
    status_sets["missing_source_in_window"] = (
        status_sets["in_reported_denominator"] & status_sets["has_missing_source"]
    )
    status_sets["other_status_in_window"] = (
        status_sets["in_reported_denominator"] & status_sets["has_other_status"]
    )

    denominator = (
        status_sets.groupby("farm_id", dropna=False)
        .agg(
            total_manifest_months=("month_label", "nunique"),
            pre_operational_manifest_months=("pre_operational_month", "sum"),
            commissioning_manifest_months=("commissioning_month", "sum"),
            steady_manifest_months=("steady_month", "sum"),
            unknown_phase_manifest_months=("unknown_phase_month", "sum"),
            manifest_months=("in_reported_denominator", "sum"),
            observed_months=("observed_in_window", "sum"),
            commissioning_observed_months=("commissioning_observed", "sum"),
            steady_observed_months=("steady_observed", "sum"),
            unknown_phase_observed_months=("unknown_phase_observed", "sum"),
            success_months=("success_in_window", "sum"),
            success_no_ais_in_bbox_months=("success_no_ais_in_window", "sum"),
            skipped_missing_source_months=("missing_source_in_window", "sum"),
            other_status_months=("other_status_in_window", "sum"),
            first_observed_month=(
                "month_label",
                lambda values: values[
                    status_sets.loc[values.index, "observed_in_window"]
                ].min(),
            ),
            last_observed_month=(
                "month_label",
                lambda values: values[
                    status_sets.loc[values.index, "observed_in_window"]
                ].max(),
            ),
        )
        .reset_index()
    )
    denominator = _merge_operational_metadata(denominator, operational_metadata)
    count_columns = [
        "total_manifest_months",
        "pre_operational_manifest_months",
        "commissioning_manifest_months",
        "steady_manifest_months",
        "unknown_phase_manifest_months",
        "manifest_months",
        "observed_months",
        "commissioning_observed_months",
        "steady_observed_months",
        "unknown_phase_observed_months",
        "success_months",
        "success_no_ais_in_bbox_months",
        "skipped_missing_source_months",
        "other_status_months",
    ]
    for column in count_columns:
        denominator[column] = denominator[column].fillna(0).astype(int)
    denominator["observed_years"] = denominator["observed_months"] / 12.0
    denominator["commissioning_observed_years"] = (
        denominator["commissioning_observed_months"] / 12.0
    )
    denominator["steady_observed_years"] = denominator["steady_observed_months"] / 12.0
    denominator["unknown_phase_observed_years"] = (
        denominator["unknown_phase_observed_months"] / 12.0
    )
    denominator["coverage_share"] = _safe_divide(
        denominator["observed_months"],
        denominator["manifest_months"],
    )
    return denominator


def build_farm_numerators(
    dwell: pd.DataFrame,
    long_dwell_threshold_min: float = DEFAULT_LONG_DWELL_THRESHOLD_MIN,
    operational_metadata: pd.DataFrame | None = None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Aggregate farm-level candidate intervention numerator evidence."""
    _require_columns(dwell, {"farm_id", "dwell_tier", "duration_min"}, "RQ9 dwell table")
    working = dwell.copy()
    working["farm_id"] = working["farm_id"].astype("string")
    working["dwell_tier"] = working["dwell_tier"].astype("string")
    working["duration_min"] = pd.to_numeric(working["duration_min"], errors="coerce")
    candidates = working.loc[working["dwell_tier"].isin(CANDIDATE_TIERS)].copy()
    raw_candidate_count = int(len(candidates))
    candidates = _merge_operational_metadata(candidates, operational_metadata)
    if "start_utc" in candidates.columns:
        candidates["_event_month_start"] = _parse_month_start(candidates["start_utc"])
        candidates["_candidate_date_missing"] = candidates["_event_month_start"].isna()
    else:
        candidates["_event_month_start"] = pd.NaT
        candidates["_candidate_date_missing"] = True
    candidates["lifecycle_phase"] = _assign_lifecycle_phase(
        candidates,
        "_event_month_start",
    )
    phase_count_source = candidates.copy()
    phase_count_source["_pre_operational_candidate"] = phase_count_source[
        "lifecycle_phase"
    ].eq("pre_operational")
    pre_operational_counts = (
        phase_count_source.groupby("farm_id", dropna=False)
        .agg(
            pre_operational_candidate_count=("_pre_operational_candidate", "sum"),
            candidate_date_missing_count=("_candidate_date_missing", "sum"),
        )
        .reset_index()
        if not phase_count_source.empty
        else pd.DataFrame(
            columns=[
                "farm_id",
                "pre_operational_candidate_count",
                "candidate_date_missing_count",
            ]
        )
    )
    candidates = candidates.loc[~candidates["lifecycle_phase"].eq("pre_operational")].copy()

    duplicate_columns_available = {"duplicate_group_id", "possible_cross_farm_duplicate"}.issubset(
        candidates.columns
    )
    candidates["_candidate_weight"] = 1.0
    candidates["_duplicate_candidate"] = False
    if duplicate_columns_available and not candidates.empty:
        duplicate_mask = (
            _coerce_bool(candidates["possible_cross_farm_duplicate"])
            & candidates["duplicate_group_id"].notna()
        )
        candidates["_duplicate_candidate"] = duplicate_mask
        duplicate_farm_counts = (
            candidates.loc[duplicate_mask]
            .groupby("duplicate_group_id")["farm_id"]
            .transform("nunique")
            .replace(0, 1)
        )
        candidates.loc[duplicate_mask, "_candidate_weight"] = 1.0 / duplicate_farm_counts

    if candidates.empty:
        numerators = pd.DataFrame(
            columns=[
                "farm_id",
                "tier_a_visit_count",
                "tier_b_visit_count",
                "candidate_intervention_count",
                "long_dwell_count",
                "unique_vessel_count",
                "duplicate_candidate_row_count",
                "duplicate_group_adjusted_candidate_count",
                "commissioning_candidate_count",
                "steady_candidate_count",
                "unknown_phase_candidate_count",
                "commissioning_long_dwell_count",
                "steady_long_dwell_count",
                "unknown_phase_long_dwell_count",
                "commissioning_duplicate_adjusted_count",
                "steady_duplicate_adjusted_count",
                "unknown_phase_duplicate_adjusted_count",
            ]
        )
    else:
        candidates["_long_dwell_candidate"] = candidates["duration_min"] >= long_dwell_threshold_min
        grouped = candidates.groupby("farm_id", dropna=False)
        numerators = grouped.agg(
            tier_a_visit_count=("dwell_tier", lambda values: int((values == "Tier A").sum())),
            tier_b_visit_count=("dwell_tier", lambda values: int((values == "Tier B").sum())),
            candidate_intervention_count=("dwell_tier", "size"),
            long_dwell_count=(
                "_long_dwell_candidate",
                "sum",
            ),
            unique_vessel_count=(
                "mmsi",
                "nunique",
            )
            if "mmsi" in candidates.columns
            else ("dwell_tier", lambda values: 0),
            duplicate_candidate_row_count=("_duplicate_candidate", "sum"),
            duplicate_group_adjusted_candidate_count=("_candidate_weight", "sum"),
        ).reset_index()

        phase_counts = (
            candidates.groupby(["farm_id", "lifecycle_phase"], dropna=False)
            .size()
            .unstack(fill_value=0)
            .rename(
                columns={
                    "commissioning_ramp_up": "commissioning_candidate_count",
                    "steady_operational": "steady_candidate_count",
                    "unknown_phase": "unknown_phase_candidate_count",
                }
            )
            .reset_index()
        )
        phase_long_counts = (
            candidates.groupby(["farm_id", "lifecycle_phase"], dropna=False)[
                "_long_dwell_candidate"
            ]
            .sum()
            .unstack(fill_value=0)
            .rename(
                columns={
                    "commissioning_ramp_up": "commissioning_long_dwell_count",
                    "steady_operational": "steady_long_dwell_count",
                    "unknown_phase": "unknown_phase_long_dwell_count",
                }
            )
            .reset_index()
        )
        phase_adjusted = (
            candidates.groupby(["farm_id", "lifecycle_phase"], dropna=False)[
                "_candidate_weight"
            ]
            .sum()
            .unstack(fill_value=0.0)
            .rename(
                columns={
                    "commissioning_ramp_up": "commissioning_duplicate_adjusted_count",
                    "steady_operational": "steady_duplicate_adjusted_count",
                    "unknown_phase": "unknown_phase_duplicate_adjusted_count",
                }
            )
            .reset_index()
        )
        for frame in [phase_counts, phase_long_counts, phase_adjusted]:
            numerators = numerators.merge(frame, on="farm_id", how="left")

    numerators = numerators.merge(pre_operational_counts, on="farm_id", how="outer")

    integer_columns = [
        "tier_a_visit_count",
        "tier_b_visit_count",
        "candidate_intervention_count",
        "long_dwell_count",
        "unique_vessel_count",
        "duplicate_candidate_row_count",
        "pre_operational_candidate_count",
        "commissioning_candidate_count",
        "steady_candidate_count",
        "unknown_phase_candidate_count",
        "commissioning_long_dwell_count",
        "steady_long_dwell_count",
        "unknown_phase_long_dwell_count",
        "candidate_date_missing_count",
    ]
    for column in integer_columns:
        if column not in numerators.columns:
            numerators[column] = 0
        numerators[column] = numerators[column].fillna(0).astype(int)

    adjusted_columns = [
        "duplicate_group_adjusted_candidate_count",
        "commissioning_duplicate_adjusted_count",
        "steady_duplicate_adjusted_count",
        "unknown_phase_duplicate_adjusted_count",
    ]
    for column in adjusted_columns:
        if column not in numerators.columns:
            numerators[column] = 0.0
        numerators[column] = pd.to_numeric(numerators[column], errors="coerce").fillna(0.0)

    numerators["duplicate_adjustment_available"] = bool(duplicate_columns_available)
    numerators["duplicate_adjustment_delta"] = (
        numerators["candidate_intervention_count"]
        - numerators["duplicate_group_adjusted_candidate_count"]
    )

    metrics = {
        "candidate_input_rows": raw_candidate_count,
        "operational_candidate_rows": int(len(candidates)),
        "pre_operational_candidate_count": int(
            pre_operational_counts["pre_operational_candidate_count"].sum()
        )
        if "pre_operational_candidate_count" in pre_operational_counts
        else 0,
        "commissioning_candidate_count": int(numerators["commissioning_candidate_count"].sum())
        if "commissioning_candidate_count" in numerators
        else 0,
        "steady_candidate_count": int(numerators["steady_candidate_count"].sum())
        if "steady_candidate_count" in numerators
        else 0,
        "unknown_phase_candidate_count": int(numerators["unknown_phase_candidate_count"].sum())
        if "unknown_phase_candidate_count" in numerators
        else 0,
        "candidate_date_missing_count": int(
            pre_operational_counts["candidate_date_missing_count"].sum()
        )
        if "candidate_date_missing_count" in pre_operational_counts
        else 0,
        "duplicate_adjustment_available": bool(duplicate_columns_available),
        "duplicate_candidate_row_count": int(candidates["_duplicate_candidate"].sum())
        if "_duplicate_candidate" in candidates.columns
        else 0,
        "long_dwell_threshold_min": float(long_dwell_threshold_min),
    }
    return numerators, metrics


def assign_confidence_class(row: pd.Series) -> str:
    """Assign a transparent coverage and evidence confidence class."""
    observed_months = int(row.get("observed_months", 0) or 0)
    coverage_share = row.get("coverage_share")
    candidate_count = int(row.get("steady_candidate_count", 0) or 0)
    operational_window_known = bool(row.get("operational_window_known", False))
    if observed_months <= 0:
        return "low_coverage"
    if not operational_window_known and candidate_count <= 2:
        return "low_signal_ambiguous"
    if pd.isna(coverage_share) or coverage_share < 0.50 or observed_months < 12:
        return "low_coverage"
    if not operational_window_known:
        return "medium_unknown_operational_window"
    if coverage_share < 0.80 or observed_months < 24:
        if candidate_count <= 2:
            return "low_signal_ambiguous"
        return "low_coverage"
    if coverage_share >= 0.80 and observed_months >= 24:
        return "high_observed_signal" if candidate_count > 0 else "high_observed_zero"
    return "low_coverage"


def assign_recommended_simulator_use(row: pd.Series) -> str:
    """Recommend how farm-level evidence can be used by RQ12 simulators."""
    steady_years = row.get("steady_observed_years")
    steady_count = int(row.get("steady_candidate_count", 0) or 0)
    commissioning_count = int(row.get("commissioning_candidate_count", 0) or 0)
    operational_window_known = bool(row.get("operational_window_known", False))
    coverage_share = row.get("coverage_share")
    if pd.isna(steady_years) or float(steady_years) < 1.0:
        return "insufficient_steady_coverage"
    if not operational_window_known or pd.isna(coverage_share) or float(coverage_share) < 0.80:
        return "validation_required"
    if commissioning_count > steady_count and (steady_count <= 2 or commissioning_count >= 10):
        return "commissioning_separate_module"
    if steady_count > 0:
        return "steady_operational_only"
    return "validation_required"


def build_farm_intervention_intensity(
    manifest: pd.DataFrame,
    dwell: pd.DataFrame,
    turbines: pd.DataFrame | None = None,
    long_dwell_threshold_min: float = DEFAULT_LONG_DWELL_THRESHOLD_MIN,
    ramp_up_months: int = DEFAULT_RAMP_UP_MONTHS,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Build farm-level maintenance intervention intensity from existing tables."""
    operational_metadata = build_farm_operational_metadata(
        turbines,
        ramp_up_months=ramp_up_months,
    )
    denominator = build_manifest_denominator(
        manifest,
        operational_metadata=operational_metadata,
    )
    numerators, numerator_metrics = build_farm_numerators(
        dwell,
        long_dwell_threshold_min=long_dwell_threshold_min,
        operational_metadata=operational_metadata,
    )

    all_farms = pd.DataFrame(
        {
            "farm_id": sorted(
                set(denominator["farm_id"].dropna().astype(str))
                | set(numerators["farm_id"].dropna().astype(str))
            )
        }
    )
    result = all_farms.merge(denominator, on="farm_id", how="left")
    result = result.merge(numerators, on="farm_id", how="left")
    result = result.merge(operational_metadata, on="farm_id", how="left", suffixes=("", "_meta"))
    result["analysis_label"] = ANALYSIS_LABEL

    for column in [
        "farm_commissioning_start_month",
        "farm_commissioning_end_month",
        "ramp_up_months",
        "steady_operational_start_month",
        "operational_start_month",
        "operational_start_source",
    ]:
        meta_column = f"{column}_meta"
        if meta_column in result.columns:
            result[column] = result[column].where(result[column].notna(), result[meta_column])
            result = result.drop(columns=[meta_column])
    if "operational_window_known_meta" in result.columns:
        result["operational_window_known"] = result["operational_window_known"].where(
            result["operational_window_known"].notna(),
            result["operational_window_known_meta"],
        )
        result = result.drop(columns=["operational_window_known_meta"])
    if "turbine_count_meta" in result.columns:
        result["turbine_count"] = result["turbine_count"].where(
            result["turbine_count"].notna(),
            result["turbine_count_meta"],
        )
        result = result.drop(columns=["turbine_count_meta"])

    zero_count_columns = [
        "total_manifest_months",
        "pre_operational_manifest_months",
        "commissioning_manifest_months",
        "steady_manifest_months",
        "unknown_phase_manifest_months",
        "manifest_months",
        "observed_months",
        "commissioning_observed_months",
        "steady_observed_months",
        "unknown_phase_observed_months",
        "success_months",
        "success_no_ais_in_bbox_months",
        "skipped_missing_source_months",
        "other_status_months",
        "tier_a_visit_count",
        "tier_b_visit_count",
        "candidate_intervention_count",
        "long_dwell_count",
        "unique_vessel_count",
        "pre_operational_candidate_count",
        "commissioning_candidate_count",
        "steady_candidate_count",
        "unknown_phase_candidate_count",
        "commissioning_long_dwell_count",
        "steady_long_dwell_count",
        "unknown_phase_long_dwell_count",
        "candidate_date_missing_count",
        "duplicate_candidate_row_count",
    ]
    for column in zero_count_columns:
        if column not in result.columns:
            result[column] = 0
        result[column] = result[column].fillna(0).astype(int)

    result["observed_years"] = result["observed_months"] / 12.0
    result["commissioning_observed_years"] = result["commissioning_observed_months"] / 12.0
    result["steady_observed_years"] = result["steady_observed_months"] / 12.0
    result["unknown_phase_observed_years"] = result["unknown_phase_observed_months"] / 12.0
    result["coverage_share"] = _safe_divide(result["observed_months"], result["manifest_months"])
    result["operational_start_source"] = result["operational_start_source"].fillna(
        "missing_turbine_metadata"
    )
    result["operational_window_known"] = result["operational_window_known"].fillna(False).astype(bool)
    result["ramp_up_months"] = result["ramp_up_months"].fillna(ramp_up_months).astype(int)
    result["duplicate_group_adjusted_candidate_count"] = pd.to_numeric(
        result["duplicate_group_adjusted_candidate_count"],
        errors="coerce",
    ).fillna(result["candidate_intervention_count"].astype(float))
    for column in [
        "commissioning_duplicate_adjusted_count",
        "steady_duplicate_adjusted_count",
        "unknown_phase_duplicate_adjusted_count",
    ]:
        if column not in result.columns:
            result[column] = 0.0
        result[column] = pd.to_numeric(result[column], errors="coerce").fillna(0.0)
    if "duplicate_adjustment_available" not in result.columns:
        result["duplicate_adjustment_available"] = bool(
            numerator_metrics["duplicate_adjustment_available"]
        )
    else:
        duplicate_available_default = bool(numerator_metrics["duplicate_adjustment_available"])
        result["duplicate_adjustment_available"] = result["duplicate_adjustment_available"].where(
            result["duplicate_adjustment_available"].notna(),
            duplicate_available_default,
        )
        result["duplicate_adjustment_available"] = result["duplicate_adjustment_available"].astype(
            bool
        )
    result["duplicate_adjustment_delta"] = (
        result["candidate_intervention_count"]
        - result["duplicate_group_adjusted_candidate_count"]
    )
    result["candidate_interventions_per_observed_farm_year"] = _safe_divide(
        result["candidate_intervention_count"],
        result["observed_years"],
    )
    result["long_dwell_interventions_per_observed_farm_year"] = _safe_divide(
        result["long_dwell_count"],
        result["observed_years"],
    )
    result["commissioning_intervention_intensity_per_farm_year"] = _safe_divide(
        result["commissioning_candidate_count"],
        result["commissioning_observed_years"],
    )
    result["steady_intervention_intensity_per_farm_year"] = _safe_divide(
        result["steady_candidate_count"],
        result["steady_observed_years"],
    )
    result["confidence_class"] = result.apply(assign_confidence_class, axis=1)
    result["recommended_simulator_use"] = result.apply(assign_recommended_simulator_use, axis=1)

    for column in ["first_observed_month", "last_observed_month"]:
        if column not in result.columns:
            result[column] = pd.NA
    if "turbine_count" not in result.columns:
        result["turbine_count"] = pd.NA
    result["turbine_count"] = result["turbine_count"].astype("Int64")

    result = result[FARM_INTENSITY_COLUMNS].sort_values("farm_id").reset_index(drop=True)
    return result, numerator_metrics


def build_validation_summary(
    manifest: pd.DataFrame,
    dwell: pd.DataFrame,
    turbines: pd.DataFrame,
    farm_intensity: pd.DataFrame,
    numerator_metrics: dict[str, Any],
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Build a compact validation summary for the RQ9 farm-level outputs."""
    status_counts = _value_counts_dict(manifest["status"]) if "status" in manifest.columns else {}
    status_series = manifest["status"].astype("string") if "status" in manifest.columns else pd.Series([])
    expected_statuses = set(OBSERVED_STATUSES) | {MISSING_SOURCE_STATUS}
    unexpected_statuses = sorted(
        str(status)
        for status in status_series.dropna().unique()
        if str(status) not in expected_statuses
    )
    missing_status_count = int(status_series.isna().sum()) if "status" in manifest.columns else 0

    metrics: dict[str, Any] = {
        "analysis_label": ANALYSIS_LABEL,
        "dwell_input_rows": int(len(dwell)),
        "manifest_input_rows": int(len(manifest)),
        "turbine_input_rows": int(len(turbines)),
        "farm_output_rows": int(len(farm_intensity)),
        "manifest_farm_count": int(manifest["farm_id"].nunique()) if "farm_id" in manifest else None,
        "dwell_farm_count": int(dwell["farm_id"].nunique()) if "farm_id" in dwell else None,
        "turbine_farm_count": int(turbines["wind_farm"].nunique()) if "wind_farm" in turbines else None,
        "operational_window_known_farm_count": int(
            farm_intensity["operational_window_known"].sum()
        ),
        "operational_window_unknown_farm_count": int(
            (~farm_intensity["operational_window_known"]).sum()
        ),
        "observed_years_min": float(farm_intensity["observed_years"].min()),
        "observed_years_median": float(farm_intensity["observed_years"].median()),
        "observed_years_max": float(farm_intensity["observed_years"].max()),
        "confidence_class_counts": _value_counts_dict(farm_intensity["confidence_class"]),
        "total_manifest_months_total": int(farm_intensity["total_manifest_months"].sum()),
        "pre_operational_manifest_months_total": int(
            farm_intensity["pre_operational_manifest_months"].sum()
        ),
        "commissioning_manifest_months_total": int(
            farm_intensity["commissioning_manifest_months"].sum()
        ),
        "steady_manifest_months_total": int(farm_intensity["steady_manifest_months"].sum()),
        "unknown_phase_manifest_months_total": int(
            farm_intensity["unknown_phase_manifest_months"].sum()
        ),
        "observed_months_total": int(farm_intensity["observed_months"].sum()),
        "observed_years_total": float(farm_intensity["observed_years"].sum()),
        "commissioning_observed_months_total": int(
            farm_intensity["commissioning_observed_months"].sum()
        ),
        "commissioning_observed_years_total": float(
            farm_intensity["commissioning_observed_years"].sum()
        ),
        "steady_observed_months_total": int(
            farm_intensity["steady_observed_months"].sum()
        ),
        "steady_observed_years_total": float(
            farm_intensity["steady_observed_years"].sum()
        ),
        "unknown_phase_observed_months_total": int(
            farm_intensity["unknown_phase_observed_months"].sum()
        ),
        "manifest_months_total": int(farm_intensity["manifest_months"].sum()),
        "denominator_coverage_share": float(
            farm_intensity["observed_months"].sum() / farm_intensity["manifest_months"].sum()
        )
        if int(farm_intensity["manifest_months"].sum()) > 0
        else None,
        "success_months_total": int(farm_intensity["success_months"].sum()),
        "success_no_ais_in_bbox_months_total": int(
            farm_intensity["success_no_ais_in_bbox_months"].sum()
        ),
        "skipped_missing_source_months_total": int(
            farm_intensity["skipped_missing_source_months"].sum()
        ),
        "missing_manifest_status_count": missing_status_count,
        "unexpected_manifest_statuses": ",".join(unexpected_statuses),
        "manifest_status_counts": status_counts,
        "tier_a_visit_count_total": int(farm_intensity["tier_a_visit_count"].sum()),
        "tier_b_visit_count_total": int(farm_intensity["tier_b_visit_count"].sum()),
        "candidate_intervention_count_total": int(
            farm_intensity["candidate_intervention_count"].sum()
        ),
        "pre_operational_candidate_count_total": int(
            farm_intensity["pre_operational_candidate_count"].sum()
        ),
        "commissioning_candidate_count_total": int(
            farm_intensity["commissioning_candidate_count"].sum()
        ),
        "steady_candidate_count_total": int(farm_intensity["steady_candidate_count"].sum()),
        "unknown_phase_candidate_count_total": int(
            farm_intensity["unknown_phase_candidate_count"].sum()
        ),
        "candidate_date_missing_count_total": int(
            farm_intensity["candidate_date_missing_count"].sum()
        ),
        "operational_candidate_rows": int(numerator_metrics["operational_candidate_rows"]),
        "long_dwell_count_total": int(farm_intensity["long_dwell_count"].sum()),
        "commissioning_long_dwell_count_total": int(
            farm_intensity["commissioning_long_dwell_count"].sum()
        ),
        "steady_long_dwell_count_total": int(farm_intensity["steady_long_dwell_count"].sum()),
        "unique_candidate_vessel_farm_sum": int(farm_intensity["unique_vessel_count"].sum()),
        "duplicate_adjustment_available": bool(
            numerator_metrics["duplicate_adjustment_available"]
        ),
        "duplicate_candidate_row_count": int(numerator_metrics["duplicate_candidate_row_count"]),
        "duplicate_adjustment_delta_total": float(
            farm_intensity["duplicate_adjustment_delta"].sum()
        ),
        "long_dwell_threshold_min": float(numerator_metrics["long_dwell_threshold_min"]),
        "ramp_up_months": int(farm_intensity["ramp_up_months"].mode().iloc[0])
        if not farm_intensity.empty
        else DEFAULT_RAMP_UP_MONTHS,
        "recommended_simulator_use_counts": _value_counts_dict(
            farm_intensity["recommended_simulator_use"]
        ),
    }
    rows = [{"metric": key, "value": _jsonable(value)} for key, value in metrics.items()]
    return pd.DataFrame(rows), _jsonable(metrics)


def write_methodology_report(
    report_path: Path,
    farm_output_path: Path,
    validation_output_path: Path,
    metrics: dict[str, Any],
) -> None:
    """Write a short methodology report for generated farm-level outputs."""
    lines = [
        "# RQ9 Farm-Level Maintenance Intervention Intensity Report",
        "",
        f"**Analysis label:** {ANALYSIS_LABEL}",
        "",
        "This report describes farm-level maintenance intervention intensity from existing "
        "AIS dwell behaviour and AIS backfill coverage metadata. It is not failure rate. "
        "A vessel visit is not automatically a failure, and true failure-rate inference "
        "requires SCADA, fault logs, work orders, or equivalent validation.",
        "",
        "## Inputs",
        "",
        "- Existing AIS dwell/weather feature table.",
        "- Existing AIS backfill manifest.",
        "- Existing turbine coordinate table, used for farm-level turbine counts and "
        "commissioning-derived lifecycle phases.",
        "- No AIS extraction rerun.",
        "- No metocean extraction rerun.",
        "",
        "## Denominator Policy",
        "",
        "- `success` and `success_no_ais_in_bbox` count as observed months.",
        "- `success_no_ais_in_bbox` is observed zero activity, not missing data.",
        "- `skipped_missing_source` is excluded from the observed denominator.",
        "- When commissioning dates are available, manifest months are split into "
        "`pre_operational`, `commissioning_ramp_up`, and `steady_operational` phases.",
        f"- The default ramp-up buffer is {metrics['ramp_up_months']} months after the "
        "latest parsed turbine commissioning month.",
        "- Pre-operational months are excluded from the operational denominator.",
        "- Commissioning/ramp-up months are reported separately from steady-operational months.",
        "- If commissioning metadata is missing, AIS source coverage is used as a "
        "`unknown_phase` fallback denominator and confidence is lowered.",
        "",
        "## Numerator Policy",
        "",
        "- Tier A and Tier B dwells are candidate intervention evidence, not fault labels.",
        "- Candidate dwell rows are split into pre-operational, commissioning/ramp-up, "
        "steady operational, and unknown phases.",
        "- Pre-operational candidates are retained as `pre_operational_candidate_count`.",
        "- Commissioning/ramp-up candidates are separated from steady operational "
        "candidates because early-life work can reflect testing, handover, snagging, "
        "warranty work, and campaign activity rather than mature maintenance demand.",
        "- Long dwells are Tier A/B candidate interventions at or above the configured duration threshold.",
        "- Duplicate groups are adjusted through derived fractional counts without destructive deletion.",
        "- `steady_intervention_intensity_per_farm_year` is the simulator-facing "
        "provisional source. Commissioning activity should be modelled separately "
        "or excluded from generic mature-operational demand multipliers.",
        "",
        "## Output Inventory",
        "",
        f"- Farm intensity output: `{farm_output_path}`",
        f"- Validation summary: `{validation_output_path}`",
        f"- Methodology report: `{report_path}`",
        "",
        "## Summary Metrics",
        "",
        f"- Farm rows: {metrics['farm_output_rows']}",
        f"- Observed farm-years: {metrics['observed_years_total']:.3f}",
        f"- Commissioning/ramp-up observed farm-years: "
        f"{metrics['commissioning_observed_years_total']:.3f}",
        f"- Steady operational observed farm-years: "
        f"{metrics['steady_observed_years_total']:.3f}",
        f"- Observed farm-years range: {metrics['observed_years_min']:.3f} to "
        f"{metrics['observed_years_max']:.3f}",
        f"- Operational window known farms: {metrics['operational_window_known_farm_count']}",
        f"- Operational window unknown farms: {metrics['operational_window_unknown_farm_count']}",
        f"- Candidate intervention count: {metrics['candidate_intervention_count_total']}",
        f"- Pre-operational candidate count excluded: "
        f"{metrics['pre_operational_candidate_count_total']}",
        f"- Commissioning/ramp-up candidate count: "
        f"{metrics['commissioning_candidate_count_total']}",
        f"- Steady operational candidate count: {metrics['steady_candidate_count_total']}",
        f"- Tier A count: {metrics['tier_a_visit_count_total']}",
        f"- Tier B count: {metrics['tier_b_visit_count_total']}",
        f"- Long dwell count: {metrics['long_dwell_count_total']}",
        f"- Duplicate adjustment available: {metrics['duplicate_adjustment_available']}",
        "",
        "## Guardrails",
        "",
        "- Do not call this output failure rate.",
        "- Do not treat Tier A/B AIS behaviour as confirmed failures.",
        "- Do not use fallback or synthetic current evidence for RQ9 validation.",
        "- Do not start Stage 2 workability work from these outputs.",
    ]
    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _audit_format_value(value: Any) -> str:
    if pd.isna(value):
        return ""
    if isinstance(value, (int, np.integer)):
        return str(int(value))
    if isinstance(value, (float, np.floating)):
        if abs(float(value) - round(float(value))) < 1e-9:
            return str(int(round(float(value))))
        return f"{float(value):.3f}".rstrip("0").rstrip(".")
    return str(value)


def _audit_markdown_table(
    frame: pd.DataFrame,
    columns: list[str] | None = None,
    max_rows: int | None = None,
) -> str:
    if columns is not None:
        frame = frame[columns]
    if max_rows is not None:
        frame = frame.head(max_rows)
    if frame.empty:
        return "_None._"
    headers = list(frame.columns)
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]
    for _, row in frame.iterrows():
        lines.append("| " + " | ".join(_audit_format_value(row[column]) for column in headers) + " |")
    return "\n".join(lines)


def _audit_describe(series: pd.Series) -> pd.DataFrame:
    stats = {
        "count": series.count(),
        "min": series.min(),
        "p05": series.quantile(0.05),
        "p25": series.quantile(0.25),
        "median": series.median(),
        "p75": series.quantile(0.75),
        "p90": series.quantile(0.90),
        "p95": series.quantile(0.95),
        "max": series.max(),
        "total": series.sum(),
    }
    return pd.DataFrame([{"metric": key, "value": value} for key, value in stats.items()])


def _operational_candidate_dwell(dwell: pd.DataFrame, farm_intensity: pd.DataFrame) -> pd.DataFrame:
    _require_columns(dwell, {"farm_id", "dwell_tier", "duration_min"}, "RQ9 dwell table")
    candidates = dwell.loc[dwell["dwell_tier"].isin(CANDIDATE_TIERS)].copy()
    candidates["farm_id"] = candidates["farm_id"].astype("string")
    candidates["duration_min"] = pd.to_numeric(candidates["duration_min"], errors="coerce")
    if "start_utc" in candidates.columns:
        candidates["_event_month_start"] = _parse_month_start(candidates["start_utc"])
    else:
        candidates["_event_month_start"] = pd.NaT

    metadata = farm_intensity[
        [
            "farm_id",
            "farm_commissioning_start_month",
            "farm_commissioning_end_month",
            "steady_operational_start_month",
            "operational_window_known",
        ]
    ].copy()
    metadata["farm_id"] = metadata["farm_id"].astype("string")
    candidates = candidates.merge(metadata, on="farm_id", how="left")
    candidates["lifecycle_phase"] = _assign_lifecycle_phase(candidates, "_event_month_start")
    return candidates.loc[~candidates["lifecycle_phase"].eq("pre_operational")].copy()


def build_sanity_audit_outputs(
    farm_intensity: pd.DataFrame,
    dwell: pd.DataFrame,
    manifest: pd.DataFrame,
    turbines: pd.DataFrame,
    report_output_dir: Path,
    long_dwell_threshold_min: float,
    farm_output_path: Path,
    validation_output_path: Path,
    methodology_report_path: Path,
    ramp_up_sensitivity_months: tuple[int, ...] = RAMP_UP_SENSITIVITY_MONTHS,
) -> dict[str, Path]:
    """Write farm-level sanity audit, top/bottom, and sensitivity outputs."""
    report_output_dir.mkdir(parents=True, exist_ok=True)
    stricter_threshold = max(long_dwell_threshold_min * 2.0, long_dwell_threshold_min + 60.0)
    analysis = farm_intensity.copy()
    analysis["tier_a_rate"] = _safe_divide(
        analysis["tier_a_visit_count"],
        analysis["observed_years"],
    )
    analysis["steady_duplicate_adjusted_rate"] = _safe_divide(
        analysis["steady_duplicate_adjusted_count"],
        analysis["steady_observed_years"],
    )
    analysis["tier_b_share"] = _safe_divide(
        analysis["tier_b_visit_count"],
        analysis["candidate_intervention_count"],
    ).fillna(0.0)

    operational_candidates = _operational_candidate_dwell(dwell, farm_intensity)
    steady_candidates = operational_candidates.loc[
        operational_candidates["lifecycle_phase"].eq("steady_operational")
    ]
    strict_long = (
        steady_candidates.loc[steady_candidates["duration_min"] >= stricter_threshold]
        .groupby("farm_id")
        .size()
        .rename("steady_long_strict_count")
        .reset_index()
    )
    analysis = analysis.merge(strict_long, on="farm_id", how="left")
    analysis["steady_long_strict_count"] = (
        analysis["steady_long_strict_count"].fillna(0).astype(int)
    )
    analysis["steady_long_strict_rate"] = _safe_divide(
        analysis["steady_long_strict_count"],
        analysis["steady_observed_years"],
    )

    rank_columns = [
        "farm_id",
        "turbine_count",
        "farm_commissioning_start_month",
        "farm_commissioning_end_month",
        "steady_operational_start_month",
        "observed_years",
        "commissioning_observed_years",
        "steady_observed_years",
        "coverage_share",
        "candidate_intervention_count",
        "commissioning_candidate_count",
        "steady_candidate_count",
        "commissioning_intervention_intensity_per_farm_year",
        "steady_intervention_intensity_per_farm_year",
        "steady_duplicate_adjusted_count",
        "steady_duplicate_adjusted_rate",
        "pre_operational_candidate_count",
        "confidence_class",
        "recommended_simulator_use",
    ]
    top = analysis.sort_values(
        ["steady_intervention_intensity_per_farm_year", "steady_candidate_count"],
        ascending=[False, False],
        na_position="last",
    ).head(20)
    top = top.copy()
    top.insert(0, "audit_category", "top_20_steady_operational_intensity")
    top.insert(1, "rank", range(1, len(top) + 1))
    bottom = analysis.sort_values(
        ["steady_intervention_intensity_per_farm_year", "steady_candidate_count"],
        ascending=[True, True],
        na_position="last",
    ).head(20)
    bottom = bottom.copy()
    bottom.insert(0, "audit_category", "bottom_20_steady_operational_intensity")
    bottom.insert(1, "rank", range(1, len(bottom) + 1))
    commissioning_top = analysis.sort_values(
        ["commissioning_intervention_intensity_per_farm_year", "commissioning_candidate_count"],
        ascending=[False, False],
        na_position="last",
    ).head(20)
    commissioning_top = commissioning_top.copy()
    commissioning_top.insert(0, "audit_category", "top_20_commissioning_intensity")
    commissioning_top.insert(1, "rank", range(1, len(commissioning_top) + 1))
    top_bottom = pd.concat(
        [
            top[["audit_category", "rank"] + rank_columns],
            bottom[["audit_category", "rank"] + rank_columns],
            commissioning_top[["audit_category", "rank"] + rank_columns],
        ],
        ignore_index=True,
    )

    sensitivity_frames: list[pd.DataFrame] = []
    for sensitivity_ramp_up in ramp_up_sensitivity_months:
        scenario_intensity, _ = build_farm_intervention_intensity(
            manifest=manifest,
            dwell=dwell,
            turbines=turbines,
            long_dwell_threshold_min=long_dwell_threshold_min,
            ramp_up_months=int(sensitivity_ramp_up),
        )
        frame = scenario_intensity[
            [
                "farm_id",
                "farm_commissioning_start_month",
                "farm_commissioning_end_month",
                "steady_operational_start_month",
                "ramp_up_months",
                "observed_years",
                "commissioning_observed_years",
                "steady_observed_years",
                "coverage_share",
                "commissioning_candidate_count",
                "steady_candidate_count",
                "commissioning_intervention_intensity_per_farm_year",
                "steady_intervention_intensity_per_farm_year",
                "confidence_class",
                "recommended_simulator_use",
            ]
        ].copy()
        frame.insert(0, "scenario", f"ramp_up_{int(sensitivity_ramp_up)}_months")
        frame.insert(
            1,
            "scenario_label",
            f"Steady operational starts {int(sensitivity_ramp_up)} months after latest turbine commissioning month",
        )
        frame["notes"] = "Read-only ramp-up sensitivity from existing manifest and dwell tables."
        sensitivity_frames.append(frame)
    sensitivity = pd.concat(sensitivity_frames, ignore_index=True)

    sensitivity_summary_rows = []
    for scenario in sensitivity["scenario"].unique():
        rows = sensitivity.loc[sensitivity["scenario"] == scenario]
        top_row = rows.sort_values(
            "steady_intervention_intensity_per_farm_year",
            ascending=False,
            na_position="last",
        ).iloc[0]
        sensitivity_summary_rows.append(
            {
                "scenario": scenario,
                "ramp_up_months": int(rows["ramp_up_months"].iloc[0]),
                "commissioning_observed_years_total": rows[
                    "commissioning_observed_years"
                ].sum(),
                "steady_observed_years_total": rows["steady_observed_years"].sum(),
                "commissioning_candidate_count_total": rows[
                    "commissioning_candidate_count"
                ].sum(),
                "steady_candidate_count_total": rows["steady_candidate_count"].sum(),
                "steady_mean_rate_per_farm_year": rows[
                    "steady_intervention_intensity_per_farm_year"
                ].mean(),
                "steady_median_rate_per_farm_year": rows[
                    "steady_intervention_intensity_per_farm_year"
                ].median(),
                "steady_p95_rate_per_farm_year": rows[
                    "steady_intervention_intensity_per_farm_year"
                ].quantile(0.95),
                "steady_max_rate_per_farm_year": rows[
                    "steady_intervention_intensity_per_farm_year"
                ].max(),
                "top_farm": top_row["farm_id"],
            }
        )
    sensitivity_summary = pd.DataFrame(sensitivity_summary_rows)

    high_event_count_cutoff = analysis["steady_candidate_count"].quantile(0.90)
    high_event_low_coverage = analysis.loc[
        (analysis["steady_candidate_count"] >= high_event_count_cutoff)
        & (analysis["coverage_share"] < 0.80)
    ].copy()
    high_coverage_near_zero = analysis.loc[
        (analysis["coverage_share"] >= 0.90)
        & (analysis["steady_intervention_intensity_per_farm_year"].fillna(0) <= 0.2)
    ].copy()
    insufficient_steady_coverage = analysis.loc[analysis["steady_observed_months"] < 12].copy()
    implausibly_high = analysis.loc[
        analysis["steady_intervention_intensity_per_farm_year"] > 50.0
    ].copy()
    commissioning_driven = analysis.loc[
        (analysis["commissioning_candidate_count"] > analysis["steady_candidate_count"])
        & (analysis["commissioning_candidate_count"] >= 10)
    ].copy()
    fractional = analysis.loc[
        (analysis["duplicate_group_adjusted_candidate_count"] % 1).abs() > 1e-9
    ].copy()
    high_dup_delta = analysis.sort_values("duplicate_adjustment_delta", ascending=False).head(10)
    unknown_operational = analysis.loc[~analysis["operational_window_known"]].copy()

    current_long_total = analysis["steady_long_dwell_count"].sum()
    strict_long_total = analysis["steady_long_strict_count"].sum()
    strict_drop = current_long_total - strict_long_total
    strict_drop_share = strict_drop / current_long_total if current_long_total else 0.0
    candidate_total = analysis["candidate_intervention_count"].sum()
    steady_candidate_total = analysis["steady_candidate_count"].sum()
    commissioning_candidate_total = analysis["commissioning_candidate_count"].sum()
    tier_b_total = analysis["tier_b_visit_count"].sum()
    tier_b_share = tier_b_total / candidate_total if candidate_total else 0.0
    duplicate_delta_total = analysis["duplicate_adjustment_delta"].sum()
    duplicate_delta_share = duplicate_delta_total / candidate_total if candidate_total else 0.0

    audit_path = report_output_dir / "farm_intervention_intensity_sanity_audit.md"
    top_bottom_path = report_output_dir / "farm_intervention_intensity_top_bottom.csv"
    sensitivity_path = report_output_dir / "farm_intervention_intensity_sensitivity.csv"

    top_bottom.to_csv(top_bottom_path, index=False)
    sensitivity.to_csv(sensitivity_path, index=False)

    report = f"""# RQ9 Farm-Level Maintenance Intervention Intensity Sanity Audit

This audit reviews the farm-level maintenance intervention intensity outputs for simulator readiness. It is not a confirmed fault-driven demand estimate. A vessel visit is not automatically a failure, and Tier A/B dwell evidence remains candidate intervention evidence until SCADA, fault log, work-order, or equivalent validation is linked.

## Scope

- Farm-level only; turbine-level intervention intensity is not implemented here.
- Inputs audited: `{farm_output_path}`, `{validation_output_path}`, `{methodology_report_path}`.
- Farm months and candidate dwells are split into pre-operational, commissioning/ramp-up, steady-operational, and unknown phases.
- Default ramp-up buffer: {int(analysis['ramp_up_months'].mode().iloc[0]) if not analysis.empty else DEFAULT_RAMP_UP_MONTHS} months after the latest parsed turbine commissioning month.
- Ramp-up sensitivity scenarios: {", ".join(str(value) for value in ramp_up_sensitivity_months)} months.
- Stricter long-dwell sensitivity reads the existing dwell feature table in memory through the RQ9 builder. No AIS extraction or metocean extraction was rerun.

## Key Totals

| Metric | Value |
| --- | --- |
| Farm rows | {len(analysis)} |
| Observed farm-years, all non-pre-operational phases | {analysis['observed_years'].sum():.3f} |
| Commissioning/ramp-up observed farm-years | {analysis['commissioning_observed_years'].sum():.3f} |
| Steady operational observed farm-years | {analysis['steady_observed_years'].sum():.3f} |
| Observed farm-years min / median / max | {analysis['observed_years'].min():.3f} / {analysis['observed_years'].median():.3f} / {analysis['observed_years'].max():.3f} |
| Raw candidate interventions, Tier A + Tier B | {int(candidate_total)} |
| Pre-operational candidate rows preserved separately | {int(analysis['pre_operational_candidate_count'].sum())} |
| Commissioning/ramp-up candidate interventions | {int(commissioning_candidate_total)} |
| Steady operational candidate interventions | {int(steady_candidate_total)} |
| Tier A candidate visits | {int(analysis['tier_a_visit_count'].sum())} |
| Tier B candidate visits | {int(tier_b_total)} |
| Tier B share of raw candidates | {tier_b_share:.1%} |
| Current steady long-dwell count | {int(current_long_total)} |
| Stricter steady long-dwell count | {int(strict_long_total)} |
| Duplicate-adjusted candidate total | {analysis['duplicate_group_adjusted_candidate_count'].sum():.3f} |
| Duplicate adjustment delta | {duplicate_delta_total:.3f} ({duplicate_delta_share:.1%} of raw candidates) |

## Top 20 Steady Operational Intensities

{_audit_markdown_table(top, ['farm_id', 'farm_commissioning_end_month', 'steady_operational_start_month', 'steady_observed_years', 'steady_candidate_count', 'steady_intervention_intensity_per_farm_year', 'commissioning_candidate_count', 'pre_operational_candidate_count', 'coverage_share', 'confidence_class', 'recommended_simulator_use'])}

## Bottom 20 Steady Operational Intensities

{_audit_markdown_table(bottom, ['farm_id', 'farm_commissioning_end_month', 'steady_operational_start_month', 'steady_observed_years', 'steady_candidate_count', 'steady_intervention_intensity_per_farm_year', 'commissioning_candidate_count', 'pre_operational_candidate_count', 'coverage_share', 'confidence_class', 'recommended_simulator_use'])}

## Top 20 Commissioning/Ramp-Up Intensities

{_audit_markdown_table(commissioning_top, ['farm_id', 'farm_commissioning_start_month', 'farm_commissioning_end_month', 'commissioning_observed_years', 'commissioning_candidate_count', 'commissioning_intervention_intensity_per_farm_year', 'steady_candidate_count', 'steady_intervention_intensity_per_farm_year', 'coverage_share', 'recommended_simulator_use'])}

## Coverage And Denominator Checks

Observed farm-years now vary by commissioning-derived lifecycle phase. Commissioning/ramp-up months are not part of the steady operational denominator used for provisional simulator demand support.

All non-pre-operational observed farm-years:

{_audit_markdown_table(_audit_describe(analysis['observed_years']))}

Steady operational observed farm-years:

{_audit_markdown_table(_audit_describe(analysis['steady_observed_years']))}

Coverage share distribution after phase-aware operational-window filtering:

{_audit_markdown_table(_audit_describe(analysis['coverage_share'].dropna()))}

Operational-window unknown farms:

{_audit_markdown_table(unknown_operational, ['farm_id', 'observed_years', 'unknown_phase_observed_years', 'unknown_phase_candidate_count', 'coverage_share', 'confidence_class', 'recommended_simulator_use'])}

## Event Count Distributions

Steady operational candidate counts:

{_audit_markdown_table(_audit_describe(analysis['steady_candidate_count']))}

Commissioning/ramp-up candidate counts:

{_audit_markdown_table(_audit_describe(analysis['commissioning_candidate_count']))}

Duplicate-adjusted candidate counts:

{_audit_markdown_table(_audit_describe(analysis['duplicate_group_adjusted_candidate_count']))}

Duplicate adjustment deltas:

{_audit_markdown_table(_audit_describe(analysis['duplicate_adjustment_delta']))}

## Duplicate Adjustment Impact

The duplicate adjustment is non-destructive: raw counts are preserved and duplicate-group adjusted counts are reported separately. Fractional adjusted counts occur on {len(fractional)} farms because duplicate groups can be split across multiple farms. No negative adjustment deltas were found.

Largest duplicate adjustment deltas:

{_audit_markdown_table(high_dup_delta, ['farm_id', 'candidate_intervention_count', 'duplicate_group_adjusted_candidate_count', 'duplicate_adjustment_delta'])}

## Confidence Classes

{_audit_markdown_table(analysis['confidence_class'].value_counts().rename_axis('confidence_class').reset_index(name='farm_count'))}

## Recommended Simulator Use

{_audit_markdown_table(analysis['recommended_simulator_use'].value_counts().rename_axis('recommended_simulator_use').reset_index(name='farm_count'))}

## Ramp-Up Sensitivity Checks

{_audit_markdown_table(sensitivity_summary)}

Tightening steady long dwell from {long_dwell_threshold_min:g} to {stricter_threshold:g} minutes removes {int(strict_drop)} long-dwell candidates ({strict_drop_share:.1%} of the current steady long-dwell numerator). Ramp-up sensitivity should be reviewed before any RQ12 demand multiplier uses the steady operational field.

## Red Flags

### Implausibly High Steady Operational Rates

Farms above 50 steady candidate interventions per observed steady farm-year need manual review before use as absolute simulator demand. They may represent short-denominator effects, residual early-life activity, duplicate-proximal activity, or repeated vessel behavior rather than mature maintenance demand.

{_audit_markdown_table(implausibly_high, ['farm_id', 'farm_commissioning_end_month', 'steady_operational_start_month', 'steady_observed_years', 'steady_candidate_count', 'steady_intervention_intensity_per_farm_year', 'steady_long_dwell_count', 'coverage_share', 'confidence_class', 'recommended_simulator_use'])}

### High Steady Event Counts With Low Coverage

Using steady candidate count >= the 90th percentile ({high_event_count_cutoff:.1f}) and coverage < 80%, these farms have high event evidence but weak observed-source denominator support.

{_audit_markdown_table(high_event_low_coverage, ['farm_id', 'steady_observed_years', 'steady_candidate_count', 'steady_intervention_intensity_per_farm_year', 'coverage_share', 'confidence_class', 'recommended_simulator_use'])}

### Insufficient Steady Operational Coverage

These farms have less than one observed steady operational farm-year. Their commissioning and pre-operational evidence is preserved separately and should not be used as generic mature-operational maintenance signal.

{_audit_markdown_table(insufficient_steady_coverage, ['farm_id', 'steady_operational_start_month', 'steady_manifest_months', 'steady_observed_months', 'steady_candidate_count', 'commissioning_candidate_count', 'pre_operational_candidate_count', 'recommended_simulator_use'])}

### Commissioning-Driven Activity

These farms have more commissioning/ramp-up candidates than steady candidates. That pattern should feed a separate commissioning-demand module or remain excluded from generic mature-operational demand multipliers.

{_audit_markdown_table(commissioning_driven, ['farm_id', 'commissioning_observed_years', 'commissioning_candidate_count', 'commissioning_intervention_intensity_per_farm_year', 'steady_observed_years', 'steady_candidate_count', 'steady_intervention_intensity_per_farm_year', 'recommended_simulator_use'])}

### High Coverage With Zero Or Near-Zero Steady Signal

These farms have coverage >= 90% and <= 0.2 steady candidate interventions per observed steady farm-year. They should not be interpreted as having no maintenance demand without external validation.

{_audit_markdown_table(high_coverage_near_zero, ['farm_id', 'steady_observed_years', 'steady_candidate_count', 'steady_intervention_intensity_per_farm_year', 'coverage_share', 'confidence_class', 'recommended_simulator_use'])}

## Simulator-Use Assessment

The phase-separated output is more suitable as a farm-level maintenance intervention intensity screen and as a relative evidence layer for RQ12 simulator inputs. Only `steady_intervention_intensity_per_farm_year` should be considered for a generic mature-operational demand multiplier, and even then it remains provisional until external SCADA/fault/work-order validation. Commissioning/ramp-up activity should be kept separate.
"""
    audit_path.write_text(report, encoding="utf-8")
    return {
        "sanity_audit_md": audit_path,
        "top_bottom_csv": top_bottom_path,
        "sensitivity_csv": sensitivity_path,
    }


def build_rq9_farm_outputs(
    dwell_path: Path,
    manifest_path: Path,
    turbine_path: Path,
    processed_output_dir: Path,
    report_output_dir: Path,
    long_dwell_threshold_min: float = DEFAULT_LONG_DWELL_THRESHOLD_MIN,
    ramp_up_months: int = DEFAULT_RAMP_UP_MONTHS,
) -> RQ9FarmOutputs:
    """Build farm-level RQ9 maintenance intervention intensity outputs."""
    processed_output_dir.mkdir(parents=True, exist_ok=True)
    report_output_dir.mkdir(parents=True, exist_ok=True)
    allowed_roots = [processed_output_dir, report_output_dir]

    dwell = read_dwell_table(dwell_path)
    manifest = read_manifest(manifest_path)
    turbines = read_turbine_coordinates(turbine_path)

    farm_intensity, numerator_metrics = build_farm_intervention_intensity(
        manifest=manifest,
        dwell=dwell,
        turbines=turbines,
        long_dwell_threshold_min=long_dwell_threshold_min,
        ramp_up_months=ramp_up_months,
    )
    validation_summary, validation_metrics = build_validation_summary(
        manifest=manifest,
        dwell=dwell,
        turbines=turbines,
        farm_intensity=farm_intensity,
        numerator_metrics=numerator_metrics,
    )

    files = {
        "farm_intervention_intensity_csv": processed_output_dir
        / "farm_intervention_intensity.csv",
        "validation_summary_csv": report_output_dir / "validation_summary.csv",
        "methodology_report_md": report_output_dir / "methodology_report.md",
    }
    for path in files.values():
        _ensure_output_path(path, allowed_roots)

    farm_intensity.to_csv(files["farm_intervention_intensity_csv"], index=False)
    validation_summary.to_csv(files["validation_summary_csv"], index=False)
    write_methodology_report(
        report_path=files["methodology_report_md"],
        farm_output_path=files["farm_intervention_intensity_csv"],
        validation_output_path=files["validation_summary_csv"],
        metrics=validation_metrics,
    )
    audit_files = build_sanity_audit_outputs(
        farm_intensity=farm_intensity,
        dwell=dwell,
        manifest=manifest,
        turbines=turbines,
        report_output_dir=report_output_dir,
        long_dwell_threshold_min=long_dwell_threshold_min,
        farm_output_path=files["farm_intervention_intensity_csv"],
        validation_output_path=files["validation_summary_csv"],
        methodology_report_path=files["methodology_report_md"],
    )
    files.update(audit_files)

    return RQ9FarmOutputs(
        processed_output_dir=processed_output_dir,
        report_output_dir=report_output_dir,
        files=files,
        validation=validation_metrics,
    )
