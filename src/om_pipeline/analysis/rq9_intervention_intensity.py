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
    """Read turbine coordinates for farm-level turbine counts only."""
    if not path.exists():
        raise FileNotFoundError(f"RQ9 turbine coordinate input not found: {path}")
    df = pd.read_csv(path)
    _require_columns(df, {"wind_farm"}, "RQ9 turbine coordinate input")
    return df


def build_manifest_denominator(manifest: pd.DataFrame) -> pd.DataFrame:
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
    status_sets["month_label"] = _month_label(status_sets["year"], status_sets["month"])
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

    denominator = (
        status_sets.groupby("farm_id", dropna=False)
        .agg(
            manifest_months=("month_label", "nunique"),
            observed_months=("is_observed", "sum"),
            success_months=("has_success", "sum"),
            success_no_ais_in_bbox_months=("has_success_no_ais", "sum"),
            skipped_missing_source_months=("has_missing_source", "sum"),
            other_status_months=("has_other_status", "sum"),
            first_observed_month=(
                "month_label",
                lambda values: values[status_sets.loc[values.index, "is_observed"]].min(),
            ),
            last_observed_month=(
                "month_label",
                lambda values: values[status_sets.loc[values.index, "is_observed"]].max(),
            ),
        )
        .reset_index()
    )
    denominator["observed_months"] = denominator["observed_months"].astype(int)
    denominator["observed_years"] = denominator["observed_months"] / 12.0
    denominator["coverage_share"] = _safe_divide(
        denominator["observed_months"],
        denominator["manifest_months"],
    )
    return denominator


def build_farm_turbine_counts(turbines: pd.DataFrame | None) -> pd.DataFrame:
    """Return farm-level turbine counts for contextual output columns."""
    if turbines is None or turbines.empty:
        return pd.DataFrame(columns=["farm_id", "turbine_count"])
    _require_columns(turbines, {"wind_farm"}, "RQ9 turbine coordinates")
    counts = (
        turbines.dropna(subset=["wind_farm"])
        .groupby("wind_farm", dropna=False)
        .size()
        .reset_index(name="turbine_count")
        .rename(columns={"wind_farm": "farm_id"})
    )
    counts["farm_id"] = counts["farm_id"].astype("string")
    counts["turbine_count"] = counts["turbine_count"].astype(int)
    return counts


def build_farm_numerators(
    dwell: pd.DataFrame,
    long_dwell_threshold_min: float = DEFAULT_LONG_DWELL_THRESHOLD_MIN,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Aggregate farm-level candidate intervention numerator evidence."""
    _require_columns(dwell, {"farm_id", "dwell_tier", "duration_min"}, "RQ9 dwell table")
    working = dwell.copy()
    working["farm_id"] = working["farm_id"].astype("string")
    working["dwell_tier"] = working["dwell_tier"].astype("string")
    working["duration_min"] = pd.to_numeric(working["duration_min"], errors="coerce")
    candidates = working.loc[working["dwell_tier"].isin(CANDIDATE_TIERS)].copy()

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

    for column in [
        "tier_a_visit_count",
        "tier_b_visit_count",
        "candidate_intervention_count",
        "long_dwell_count",
        "unique_vessel_count",
        "duplicate_candidate_row_count",
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
        "candidate_input_rows": int(len(candidates)),
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
    if observed_months <= 0:
        return "no_observed_coverage"
    if pd.notna(coverage_share) and coverage_share >= 0.80 and observed_months >= 24:
        return "high_observed_signal" if candidate_count > 0 else "high_observed_zero"
    if pd.notna(coverage_share) and coverage_share >= 0.50 and observed_months >= 12:
        return "medium"
    return "low"


def build_farm_intervention_intensity(
    manifest: pd.DataFrame,
    dwell: pd.DataFrame,
    turbines: pd.DataFrame | None = None,
    long_dwell_threshold_min: float = DEFAULT_LONG_DWELL_THRESHOLD_MIN,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Build farm-level maintenance intervention intensity from existing tables."""
    denominator = build_manifest_denominator(manifest)
    numerators, numerator_metrics = build_farm_numerators(
        dwell,
        long_dwell_threshold_min=long_dwell_threshold_min,
    )
    turbine_counts = build_farm_turbine_counts(turbines)

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
    result = result.merge(turbine_counts, on="farm_id", how="left")
    result["analysis_label"] = ANALYSIS_LABEL

    zero_count_columns = [
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
        "duplicate_candidate_row_count",
    ]
    for column in zero_count_columns:
        if column not in result.columns:
            result[column] = 0
        result[column] = result[column].fillna(0).astype(int)

    result["observed_years"] = result["observed_months"] / 12.0
    result["coverage_share"] = _safe_divide(result["observed_months"], result["manifest_months"])
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
        "- Existing turbine coordinate table, used only for farm-level turbine counts.",
        "- No AIS extraction rerun.",
        "- No metocean extraction rerun.",
        "",
        "## Denominator Policy",
        "",
        "- `success` and `success_no_ais_in_bbox` count as observed months.",
        "- `success_no_ais_in_bbox` is observed zero activity, not missing data.",
        "- `skipped_missing_source` is excluded from the observed denominator.",
        "",
        "## Numerator Policy",
        "",
        "- Tier A and Tier B dwells are candidate intervention evidence, not fault labels.",
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
        f"- Candidate intervention count: {metrics['candidate_intervention_count_total']}",
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

    return RQ9FarmOutputs(
        processed_output_dir=processed_output_dir,
        report_output_dir=report_output_dir,
        files=files,
        validation=validation_metrics,
    )
