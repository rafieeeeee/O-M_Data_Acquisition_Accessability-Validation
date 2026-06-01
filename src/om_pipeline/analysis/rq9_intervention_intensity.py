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

FARM_INTENSITY_COLUMNS = [
    "analysis_label",
    "farm_id",
    "turbine_count",
    "operational_start_month",
    "operational_start_source",
    "operational_window_known",
    "total_manifest_months",
    "pre_operational_manifest_months",
    "manifest_months",
    "observed_months",
    "observed_years",
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
    "candidate_date_missing_count",
    "duplicate_adjustment_available",
    "duplicate_candidate_row_count",
    "duplicate_group_adjusted_candidate_count",
    "duplicate_adjustment_delta",
    "candidate_interventions_per_observed_farm_year",
    "long_dwell_interventions_per_observed_farm_year",
    "confidence_class",
]


@dataclass(frozen=True)
class RQ9FarmOutputs:
    """Paths and summary values written by the farm-level RQ9 builder."""

    processed_output_dir: Path
    report_output_dir: Path
    files: dict[str, Path]
    validation: dict[str, Any]


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


def _merge_operational_metadata(
    frame: pd.DataFrame,
    operational_metadata: pd.DataFrame | None,
) -> pd.DataFrame:
    if operational_metadata is None or operational_metadata.empty:
        frame = frame.copy()
        frame["operational_start_month"] = pd.NA
        frame["operational_start_source"] = "missing_turbine_metadata"
        frame["operational_window_known"] = False
        return frame

    metadata_columns = [
        "farm_id",
        "operational_start_month",
        "operational_start_source",
        "operational_window_known",
    ]
    metadata = operational_metadata[metadata_columns].copy()
    metadata["farm_id"] = metadata["farm_id"].astype("string")
    merged = frame.merge(metadata, on="farm_id", how="left")
    merged["operational_start_source"] = merged["operational_start_source"].fillna(
        "missing_turbine_metadata"
    )
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


def build_farm_operational_metadata(turbines: pd.DataFrame | None) -> pd.DataFrame:
    """Return farm-level turbine counts and operational start metadata."""
    columns = [
        "farm_id",
        "turbine_count",
        "operational_start_month",
        "operational_start_source",
        "operational_window_known",
    ]
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
            _operational_start_dt=("_commissioning_month", "min"),
            commissioning_date_parseable_count=("_commissioning_month", "count"),
        )
        .reset_index()
    )
    metadata["operational_start_month"] = _format_month_start(
        metadata["_operational_start_dt"]
    )
    metadata["operational_window_known"] = (
        metadata["commissioning_date_parseable_count"].fillna(0).astype(int) > 0
    )
    metadata["operational_start_source"] = np.where(
        metadata["operational_window_known"],
        "turbine_commissioning_date_earliest",
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
    status_sets["_operational_start_dt"] = _parse_month_start(
        status_sets["operational_start_month"]
    )
    status_sets["within_operational_window"] = (
        status_sets["_operational_start_dt"].isna()
        | (status_sets["month_start"] >= status_sets["_operational_start_dt"])
    )
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
    status_sets["observed_in_window"] = (
        status_sets["within_operational_window"] & status_sets["is_observed"]
    )
    status_sets["success_in_window"] = (
        status_sets["within_operational_window"] & status_sets["has_success"]
    )
    status_sets["success_no_ais_in_window"] = (
        status_sets["within_operational_window"] & status_sets["has_success_no_ais"]
    )
    status_sets["missing_source_in_window"] = (
        status_sets["within_operational_window"] & status_sets["has_missing_source"]
    )
    status_sets["other_status_in_window"] = (
        status_sets["within_operational_window"] & status_sets["has_other_status"]
    )
    status_sets["pre_operational_month"] = ~status_sets["within_operational_window"]

    denominator = (
        status_sets.groupby("farm_id", dropna=False)
        .agg(
            total_manifest_months=("month_label", "nunique"),
            pre_operational_manifest_months=("pre_operational_month", "sum"),
            manifest_months=("within_operational_window", "sum"),
            observed_months=("observed_in_window", "sum"),
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
        "manifest_months",
        "observed_months",
        "success_months",
        "success_no_ais_in_bbox_months",
        "skipped_missing_source_months",
        "other_status_months",
    ]
    for column in count_columns:
        denominator[column] = denominator[column].fillna(0).astype(int)
    denominator["observed_years"] = denominator["observed_months"] / 12.0
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
    candidates["_operational_start_dt"] = _parse_month_start(
        candidates["operational_start_month"]
    )
    candidates["_has_operational_start"] = (
        candidates["operational_window_known"] & candidates["_operational_start_dt"].notna()
    )
    candidates["_operational_candidate"] = True
    dated_with_start = candidates["_has_operational_start"] & candidates[
        "_event_month_start"
    ].notna()
    candidates.loc[dated_with_start, "_operational_candidate"] = (
        candidates.loc[dated_with_start, "_event_month_start"]
        >= candidates.loc[dated_with_start, "_operational_start_dt"]
    )
    candidates["_pre_operational_candidate"] = ~candidates["_operational_candidate"]
    pre_operational_counts = (
        candidates.groupby("farm_id", dropna=False)
        .agg(
            pre_operational_candidate_count=("_pre_operational_candidate", "sum"),
            candidate_date_missing_count=("_candidate_date_missing", "sum"),
        )
        .reset_index()
        if not candidates.empty
        else pd.DataFrame(
            columns=[
                "farm_id",
                "pre_operational_candidate_count",
                "candidate_date_missing_count",
            ]
        )
    )
    candidates = candidates.loc[candidates["_operational_candidate"]].copy()

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
            ]
        )
    else:
        grouped = candidates.groupby("farm_id", dropna=False)
        numerators = grouped.agg(
            tier_a_visit_count=("dwell_tier", lambda values: int((values == "Tier A").sum())),
            tier_b_visit_count=("dwell_tier", lambda values: int((values == "Tier B").sum())),
            candidate_intervention_count=("dwell_tier", "size"),
            long_dwell_count=(
                "duration_min",
                lambda values: int((values >= long_dwell_threshold_min).sum()),
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

    numerators = numerators.merge(pre_operational_counts, on="farm_id", how="outer")

    for column in [
        "tier_a_visit_count",
        "tier_b_visit_count",
        "candidate_intervention_count",
        "long_dwell_count",
        "unique_vessel_count",
        "duplicate_candidate_row_count",
        "pre_operational_candidate_count",
        "candidate_date_missing_count",
    ]:
        if column in numerators.columns:
            numerators[column] = numerators[column].fillna(0).astype(int)

    if "duplicate_group_adjusted_candidate_count" in numerators.columns:
        numerators["duplicate_group_adjusted_candidate_count"] = pd.to_numeric(
            numerators["duplicate_group_adjusted_candidate_count"],
            errors="coerce",
        ).fillna(0.0)
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
    candidate_count = int(row.get("candidate_intervention_count", 0) or 0)
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


def build_farm_intervention_intensity(
    manifest: pd.DataFrame,
    dwell: pd.DataFrame,
    turbines: pd.DataFrame | None = None,
    long_dwell_threshold_min: float = DEFAULT_LONG_DWELL_THRESHOLD_MIN,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Build farm-level maintenance intervention intensity from existing tables."""
    operational_metadata = build_farm_operational_metadata(turbines)
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

    for column in ["operational_start_month", "operational_start_source"]:
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
        "manifest_months",
        "observed_months",
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
        "candidate_date_missing_count",
        "duplicate_candidate_row_count",
    ]
    for column in zero_count_columns:
        if column not in result.columns:
            result[column] = 0
        result[column] = result[column].fillna(0).astype(int)

    result["observed_years"] = result["observed_months"] / 12.0
    result["coverage_share"] = _safe_divide(result["observed_months"], result["manifest_months"])
    result["operational_start_source"] = result["operational_start_source"].fillna(
        "missing_turbine_metadata"
    )
    result["operational_window_known"] = result["operational_window_known"].fillna(False).astype(bool)
    result["duplicate_group_adjusted_candidate_count"] = pd.to_numeric(
        result["duplicate_group_adjusted_candidate_count"],
        errors="coerce",
    ).fillna(result["candidate_intervention_count"].astype(float))
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
    result["confidence_class"] = result.apply(assign_confidence_class, axis=1)

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
        "observed_months_total": int(farm_intensity["observed_months"].sum()),
        "observed_years_total": float(farm_intensity["observed_years"].sum()),
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
        "candidate_date_missing_count_total": int(
            farm_intensity["candidate_date_missing_count"].sum()
        ),
        "operational_candidate_rows": int(numerator_metrics["operational_candidate_rows"]),
        "long_dwell_count_total": int(farm_intensity["long_dwell_count"].sum()),
        "unique_candidate_vessel_farm_sum": int(farm_intensity["unique_vessel_count"].sum()),
        "duplicate_adjustment_available": bool(
            numerator_metrics["duplicate_adjustment_available"]
        ),
        "duplicate_candidate_row_count": int(numerator_metrics["duplicate_candidate_row_count"]),
        "duplicate_adjustment_delta_total": float(
            farm_intensity["duplicate_adjustment_delta"].sum()
        ),
        "long_dwell_threshold_min": float(numerator_metrics["long_dwell_threshold_min"]),
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
        "commissioning-derived operational windows.",
        "- No AIS extraction rerun.",
        "- No metocean extraction rerun.",
        "",
        "## Denominator Policy",
        "",
        "- `success` and `success_no_ais_in_bbox` count as observed months.",
        "- `success_no_ais_in_bbox` is observed zero activity, not missing data.",
        "- `skipped_missing_source` is excluded from the observed denominator.",
        "- When commissioning dates are available, manifest months before the farm "
        "operational start month are excluded from the observed denominator.",
        "- If commissioning metadata is missing, AIS source coverage is used as a "
        "fallback denominator and confidence is lowered.",
        "",
        "## Numerator Policy",
        "",
        "- Tier A and Tier B dwells are candidate intervention evidence, not fault labels.",
        "- Candidate dwell rows before the farm operational start month are excluded "
        "from the numerator and retained as `pre_operational_candidate_count`.",
        "- Long dwells are Tier A/B candidate interventions at or above the configured duration threshold.",
        "- Duplicate groups are adjusted through derived fractional counts without destructive deletion.",
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
        f"- Observed farm-years range: {metrics['observed_years_min']:.3f} to "
        f"{metrics['observed_years_max']:.3f}",
        f"- Operational window known farms: {metrics['operational_window_known_farm_count']}",
        f"- Operational window unknown farms: {metrics['operational_window_unknown_farm_count']}",
        f"- Candidate intervention count: {metrics['candidate_intervention_count_total']}",
        f"- Pre-operational candidate count excluded: "
        f"{metrics['pre_operational_candidate_count_total']}",
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
        ["farm_id", "operational_start_month", "operational_window_known"]
    ].copy()
    metadata["farm_id"] = metadata["farm_id"].astype("string")
    candidates = candidates.merge(metadata, on="farm_id", how="left")
    candidates["_operational_start_dt"] = _parse_month_start(
        candidates["operational_start_month"]
    )
    has_start = candidates["operational_window_known"].fillna(False).astype(bool) & candidates[
        "_operational_start_dt"
    ].notna()
    has_event_date = candidates["_event_month_start"].notna()
    eligible = pd.Series(True, index=candidates.index)
    eligible.loc[has_start & has_event_date] = (
        candidates.loc[has_start & has_event_date, "_event_month_start"]
        >= candidates.loc[has_start & has_event_date, "_operational_start_dt"]
    )
    return candidates.loc[eligible].copy()


def build_sanity_audit_outputs(
    farm_intensity: pd.DataFrame,
    dwell: pd.DataFrame,
    report_output_dir: Path,
    long_dwell_threshold_min: float,
    farm_output_path: Path,
    validation_output_path: Path,
    methodology_report_path: Path,
) -> dict[str, Path]:
    """Write farm-level sanity audit, top/bottom, and sensitivity outputs."""
    report_output_dir.mkdir(parents=True, exist_ok=True)
    stricter_threshold = max(long_dwell_threshold_min * 2.0, long_dwell_threshold_min + 60.0)
    analysis = farm_intensity.copy()
    analysis["tier_a_rate"] = _safe_divide(
        analysis["tier_a_visit_count"],
        analysis["observed_years"],
    )
    analysis["duplicate_adjusted_rate"] = _safe_divide(
        analysis["duplicate_group_adjusted_candidate_count"],
        analysis["observed_years"],
    )
    analysis["tier_b_share"] = _safe_divide(
        analysis["tier_b_visit_count"],
        analysis["candidate_intervention_count"],
    ).fillna(0.0)

    operational_candidates = _operational_candidate_dwell(dwell, farm_intensity)
    strict_long = (
        operational_candidates.loc[operational_candidates["duration_min"] >= stricter_threshold]
        .groupby("farm_id")
        .size()
        .rename("long_strict_count")
        .reset_index()
    )
    analysis = analysis.merge(strict_long, on="farm_id", how="left")
    analysis["long_strict_count"] = analysis["long_strict_count"].fillna(0).astype(int)
    analysis["long_strict_rate"] = _safe_divide(
        analysis["long_strict_count"],
        analysis["observed_years"],
    )

    rank_columns = [
        "farm_id",
        "turbine_count",
        "operational_start_month",
        "observed_years",
        "coverage_share",
        "candidate_intervention_count",
        "duplicate_group_adjusted_candidate_count",
        "duplicate_adjustment_delta",
        "candidate_interventions_per_observed_farm_year",
        "pre_operational_candidate_count",
        "confidence_class",
    ]
    top = analysis.sort_values(
        ["candidate_interventions_per_observed_farm_year", "candidate_intervention_count"],
        ascending=[False, False],
        na_position="last",
    ).head(20)
    top = top.copy()
    top.insert(0, "audit_category", "top_20_candidate_intensity")
    top.insert(1, "rank", range(1, len(top) + 1))
    bottom = analysis.sort_values(
        ["candidate_interventions_per_observed_farm_year", "candidate_intervention_count"],
        ascending=[True, True],
        na_position="last",
    ).head(20)
    bottom = bottom.copy()
    bottom.insert(0, "audit_category", "bottom_20_candidate_intensity")
    bottom.insert(1, "rank", range(1, len(bottom) + 1))
    top_bottom = pd.concat(
        [top[["audit_category", "rank"] + rank_columns], bottom[["audit_category", "rank"] + rank_columns]],
        ignore_index=True,
    )

    scenario_specs = [
        (
            "tier_a_only",
            "Tier A candidate visits only",
            np.nan,
            analysis["tier_a_visit_count"],
            "Tier A only candidate intervention evidence.",
        ),
        (
            "tier_a_plus_tier_b",
            "Tier A plus Tier B candidate visits",
            np.nan,
            analysis["candidate_intervention_count"],
            "Current raw numerator for candidate intervention intensity.",
        ),
        (
            f"long_dwell_{long_dwell_threshold_min:g}_min",
            f"Long dwell candidates >= {long_dwell_threshold_min:g} min",
            long_dwell_threshold_min,
            analysis["long_dwell_count"],
            "Current long-dwell numerator from committed farm output.",
        ),
        (
            f"long_dwell_{stricter_threshold:g}_min",
            f"Long dwell candidates >= {stricter_threshold:g} min",
            stricter_threshold,
            analysis["long_strict_count"],
            "Read-only sensitivity from existing dwell feature table; no extraction rerun.",
        ),
    ]
    sensitivity_frames: list[pd.DataFrame] = []
    for scenario, label, threshold, counts, notes in scenario_specs:
        frame = analysis[
            [
                "farm_id",
                "operational_start_month",
                "observed_years",
                "coverage_share",
                "confidence_class",
            ]
        ].copy()
        frame.insert(0, "scenario", scenario)
        frame.insert(1, "scenario_label", label)
        frame["long_dwell_threshold_min"] = threshold
        frame["event_count"] = counts.values
        frame["events_per_observed_farm_year"] = _safe_divide(
            frame["event_count"],
            frame["observed_years"],
        )
        frame["notes"] = notes
        sensitivity_frames.append(frame)
    sensitivity = pd.concat(sensitivity_frames, ignore_index=True)

    sensitivity_summary_rows = []
    for scenario in sensitivity["scenario"].unique():
        rows = sensitivity.loc[sensitivity["scenario"] == scenario]
        top_row = rows.sort_values(
            "events_per_observed_farm_year",
            ascending=False,
            na_position="last",
        ).iloc[0]
        sensitivity_summary_rows.append(
            {
                "scenario": scenario,
                "total_events": rows["event_count"].sum(),
                "mean_rate_per_farm_year": rows["events_per_observed_farm_year"].mean(),
                "median_rate_per_farm_year": rows["events_per_observed_farm_year"].median(),
                "p95_rate_per_farm_year": rows["events_per_observed_farm_year"].quantile(0.95),
                "max_rate_per_farm_year": rows["events_per_observed_farm_year"].max(),
                "top_farm": top_row["farm_id"],
            }
        )
    sensitivity_summary = pd.DataFrame(sensitivity_summary_rows)

    high_event_count_cutoff = analysis["candidate_intervention_count"].quantile(0.90)
    high_event_low_coverage = analysis.loc[
        (analysis["candidate_intervention_count"] >= high_event_count_cutoff)
        & (analysis["coverage_share"] < 0.80)
    ].copy()
    high_coverage_near_zero = analysis.loc[
        (analysis["coverage_share"] >= 0.90)
        & (analysis["candidate_interventions_per_observed_farm_year"].fillna(0) <= 0.2)
    ].copy()
    no_observed_after_start = analysis.loc[analysis["observed_months"] == 0].copy()
    implausibly_high = analysis.loc[
        analysis["candidate_interventions_per_observed_farm_year"] > 50.0
    ].copy()
    fractional = analysis.loc[
        (analysis["duplicate_group_adjusted_candidate_count"] % 1).abs() > 1e-9
    ].copy()
    high_dup_delta = analysis.sort_values("duplicate_adjustment_delta", ascending=False).head(10)
    unknown_operational = analysis.loc[~analysis["operational_window_known"]].copy()

    current_long_total = analysis["long_dwell_count"].sum()
    strict_long_total = analysis["long_strict_count"].sum()
    strict_drop = current_long_total - strict_long_total
    strict_drop_share = strict_drop / current_long_total if current_long_total else 0.0
    candidate_total = analysis["candidate_intervention_count"].sum()
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
- Stricter long-dwell sensitivity reads the existing dwell feature table in memory through the RQ9 builder. No AIS extraction or metocean extraction was rerun.
- Current long-dwell threshold: {long_dwell_threshold_min:g} minutes.
- Stricter long-dwell threshold used for this audit: {stricter_threshold:g} minutes.

## Key Totals

| Metric | Value |
| --- | --- |
| Farm rows | {len(analysis)} |
| Observed farm-years | {analysis['observed_years'].sum():.3f} |
| Observed farm-years min / median / max | {analysis['observed_years'].min():.3f} / {analysis['observed_years'].median():.3f} / {analysis['observed_years'].max():.3f} |
| Raw candidate interventions, Tier A + Tier B | {int(candidate_total)} |
| Pre-operational candidate rows excluded | {int(analysis['pre_operational_candidate_count'].sum())} |
| Tier A candidate visits | {int(analysis['tier_a_visit_count'].sum())} |
| Tier B candidate visits | {int(tier_b_total)} |
| Tier B share of raw candidates | {tier_b_share:.1%} |
| Current long-dwell count | {int(current_long_total)} |
| Stricter long-dwell count | {int(strict_long_total)} |
| Duplicate-adjusted candidate total | {analysis['duplicate_group_adjusted_candidate_count'].sum():.3f} |
| Duplicate adjustment delta | {duplicate_delta_total:.3f} ({duplicate_delta_share:.1%} of raw candidates) |

## Top 20 Candidate Intensities

{_audit_markdown_table(top, ['farm_id', 'operational_start_month', 'observed_years', 'coverage_share', 'candidate_intervention_count', 'candidate_interventions_per_observed_farm_year', 'pre_operational_candidate_count', 'duplicate_group_adjusted_candidate_count', 'duplicate_adjustment_delta', 'confidence_class'])}

## Bottom 20 Candidate Intensities

{_audit_markdown_table(bottom, ['farm_id', 'operational_start_month', 'observed_years', 'coverage_share', 'candidate_intervention_count', 'candidate_interventions_per_observed_farm_year', 'pre_operational_candidate_count', 'duplicate_group_adjusted_candidate_count', 'duplicate_adjustment_delta', 'confidence_class'])}

## Coverage And Denominator Checks

Observed farm-years now vary by farm operational start month. This corrects the v1 global-window issue where every farm had the same 15.0 observed farm-years.

{_audit_markdown_table(_audit_describe(analysis['observed_years']))}

Coverage share distribution after operational-window filtering:

{_audit_markdown_table(_audit_describe(analysis['coverage_share'].dropna()))}

Operational-window unknown farms:

{_audit_markdown_table(unknown_operational, ['farm_id', 'observed_years', 'coverage_share', 'candidate_intervention_count', 'confidence_class'])}

## Event Count Distributions

Raw Tier A/B candidate counts:

{_audit_markdown_table(_audit_describe(analysis['candidate_intervention_count']))}

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

## Sensitivity Checks

{_audit_markdown_table(sensitivity_summary)}

Switching from Tier A + Tier B to Tier A only removes {int(tier_b_total)} candidate visits ({tier_b_share:.1%} of the raw numerator). Tightening long dwell from {long_dwell_threshold_min:g} to {stricter_threshold:g} minutes removes {int(strict_drop)} long-dwell candidates ({strict_drop_share:.1%} of the current long-dwell numerator).

## Red Flags

### Implausibly High Raw Rates

Farms above 50 raw candidate interventions per observed farm-year need manual review before use as absolute simulator demand. They may represent intense operational activity, commissioning-period activity, duplicate-proximal activity, or repeated vessel behavior rather than maintenance demand.

{_audit_markdown_table(implausibly_high, ['farm_id', 'operational_start_month', 'observed_years', 'candidate_intervention_count', 'candidate_interventions_per_observed_farm_year', 'long_dwell_count', 'long_dwell_interventions_per_observed_farm_year', 'coverage_share', 'confidence_class'])}

### High Event Counts With Low Coverage

Using candidate count >= the 90th percentile ({high_event_count_cutoff:.1f}) and coverage < 80%, these farms have high event evidence but weak observed-source denominator support.

{_audit_markdown_table(high_event_low_coverage, ['farm_id', 'operational_start_month', 'observed_years', 'candidate_intervention_count', 'candidate_interventions_per_observed_farm_year', 'coverage_share', 'confidence_class'])}

### No Observed Coverage After Operational Start

These farms have commissioning-derived operational months in the manifest, but none of those months have observed AIS source coverage. Their pre-operational candidates are preserved separately and should not be treated as operational maintenance signal.

{_audit_markdown_table(no_observed_after_start, ['farm_id', 'operational_start_month', 'manifest_months', 'observed_months', 'candidate_intervention_count', 'pre_operational_candidate_count', 'confidence_class'])}

### High Coverage With Zero Or Near-Zero Signal

These farms have coverage >= 90% and <= 0.2 raw candidate interventions per observed farm-year. They should not be interpreted as having no maintenance demand without external validation.

{_audit_markdown_table(high_coverage_near_zero, ['farm_id', 'operational_start_month', 'observed_years', 'candidate_intervention_count', 'candidate_interventions_per_observed_farm_year', 'coverage_share', 'confidence_class'])}

## Simulator-Use Assessment

The corrected output is more plausible as a farm-level maintenance intervention intensity screen and as a relative evidence layer for RQ12 simulator inputs. It still should not be used as a confirmed fault-driven process. Remaining guardrails are operational-window quality for newly commissioned farms, outlier review for very high rates, and external SCADA/fault/work-order validation before calibrating true fault demand.
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
