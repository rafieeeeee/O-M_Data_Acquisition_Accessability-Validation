"""Turbine denominator and exposure v1 for RQ9 maintenance intervention intensity.

This module uses existing RQ9 turbine-assigned Tier A events, turbine metadata,
and AIS source coverage manifests. It estimates turbine-level maintenance
intervention intensity only; AIS dwell evidence is candidate intervention
evidence and still needs external maintenance validation before simulator use.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from om_pipeline.analysis.rq9_intervention_intensity import (
    DEFAULT_RAMP_UP_MONTHS,
    MISSING_SOURCE_STATUS,
    OBSERVED_STATUSES,
    read_manifest,
    read_turbine_coordinates,
)
from om_pipeline.analysis.rq9_turbine_feasibility import (
    prepare_turbine_metadata,
    read_farm_intensity,
)


ANALYSIS_LABEL = "RQ9 turbine-level maintenance intervention intensity v1"
PRIMARY_SCOPE = "high_confidence_200m"
SENSITIVITY_SCOPE = "high_medium_500m"
REPEAT_VISIT_WINDOW_DAYS = 30
EARTH_RADIUS_M = 6_371_000.0

DENOMINATOR_COLUMNS = [
    "analysis_label",
    "farm_id",
    "wind_farm",
    "turbine_id",
    "turbine_source_row",
    "country",
    "sea_basin",
    "sea_basin_mapping_rule",
    "latitude",
    "longitude",
    "farm_centroid_latitude",
    "farm_centroid_longitude",
    "distance_to_farm_centroid_m",
    "distance_to_farm_centroid_quantile",
    "exposure_group",
    "outer_ring_proxy",
    "farm_turbine_count",
    "oem_manufacturer",
    "turbine_model",
    "rated_capacity_mw",
    "rotor_diameter_m",
    "hub_height_m",
    "commissioning_date",
    "commissioning_month",
    "commissioning_year",
    "steady_operational_start_month",
    "turbine_age_years_at_observation_end",
    "turbine_age_band_at_observation_end",
    "water_depth_m",
    "bathymetry_spatial_match_status",
    "bathymetry_join_status",
    "steady_manifest_months",
    "observed_steady_months",
    "observed_steady_years",
    "success_steady_months",
    "success_no_ais_in_bbox_steady_months",
    "skipped_missing_source_steady_months",
    "other_status_steady_months",
    "steady_coverage_share",
    "first_observed_steady_month",
    "last_observed_steady_month",
    "coverage_class",
    "min_1yr_exposure",
    "min_3yr_exposure",
    "min_5yr_exposure",
]

INTENSITY_COLUMNS = [
    "analysis_label",
    "farm_id",
    "wind_farm",
    "turbine_id",
    "country",
    "sea_basin",
    "sea_basin_mapping_rule",
    "latitude",
    "longitude",
    "exposure_group",
    "outer_ring_proxy",
    "distance_to_farm_centroid_m",
    "distance_to_farm_centroid_quantile",
    "water_depth_m",
    "oem_manufacturer",
    "turbine_model",
    "rated_capacity_mw",
    "rotor_diameter_m",
    "hub_height_m",
    "commissioning_year",
    "turbine_age_band_at_observation_end",
    "observed_steady_months",
    "observed_steady_years",
    "steady_coverage_share",
    "coverage_class",
    "min_1yr_exposure",
    "min_3yr_exposure",
    "min_5yr_exposure",
    "steady_high_event_count",
    "steady_high_duplicate_adjusted_event_count",
    "steady_high_unique_vessel_count",
    "steady_high_repeat_visit_30d_count",
    "steady_high_medium_event_count",
    "steady_high_medium_duplicate_adjusted_event_count",
    "steady_high_medium_unique_vessel_count",
    "steady_high_medium_repeat_visit_30d_count",
    "primary_intervention_intensity_per_steady_turbine_year",
    "sensitivity_intervention_intensity_per_steady_turbine_year",
    "turbine_intervention_confidence_class",
]

EXPOSURE_COMPARISON_COLUMNS = [
    "assignment_scope",
    "comparison_group",
    "turbine_count",
    "eligible_turbine_count_ge_1yr",
    "observed_steady_years",
    "event_count",
    "duplicate_adjusted_event_count",
    "unique_vessel_count_sum",
    "repeat_visit_30d_count",
    "intervention_intensity_per_steady_turbine_year",
    "median_turbine_intervention_intensity",
    "comparison_confidence_class",
]

SEA_BASIN_RULES = [
    {
        "rule_id": "united_kingdom_north_sea",
        "country": "United Kingdom",
        "longitude_rule": "any",
        "sea_basin": "North Sea",
    },
    {
        "rule_id": "netherlands_north_sea",
        "country": "Netherlands",
        "longitude_rule": "any",
        "sea_basin": "North Sea",
    },
    {
        "rule_id": "belgium_north_sea",
        "country": "Belgium",
        "longitude_rule": "any",
        "sea_basin": "North Sea",
    },
    {
        "rule_id": "norway_north_sea",
        "country": "Norway",
        "longitude_rule": "any",
        "sea_basin": "North Sea",
    },
    {
        "rule_id": "germany_baltic",
        "country": "Germany",
        "longitude_rule": "longitude >= 10.0",
        "sea_basin": "Baltic",
    },
    {
        "rule_id": "germany_north_sea",
        "country": "Germany",
        "longitude_rule": "longitude < 10.0",
        "sea_basin": "North Sea",
    },
    {
        "rule_id": "denmark_baltic",
        "country": "Denmark",
        "longitude_rule": "longitude >= 10.0",
        "sea_basin": "Baltic",
    },
    {
        "rule_id": "denmark_north_sea",
        "country": "Denmark",
        "longitude_rule": "longitude < 10.0",
        "sea_basin": "North Sea",
    },
    {
        "rule_id": "sweden_baltic",
        "country": "Sweden",
        "longitude_rule": "any",
        "sea_basin": "Baltic",
    },
    {
        "rule_id": "france_other",
        "country": "France",
        "longitude_rule": "any",
        "sea_basin": "other",
    },
    {
        "rule_id": "missing_country_unknown",
        "country": "__missing__",
        "longitude_rule": "any",
        "sea_basin": "unknown",
    },
    {
        "rule_id": "unmapped_country_unknown",
        "country": "__unmapped__",
        "longitude_rule": "any",
        "sea_basin": "unknown",
    },
]


@dataclass(frozen=True)
class RQ9TurbineExposureOutputs:
    """Paths and summary values written by the turbine exposure builder."""

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
    shifted = parsed.dt.to_period("M") + int(months)
    return shifted.dt.to_timestamp()


def _safe_divide(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    numerator = pd.to_numeric(numerator, errors="coerce")
    denominator = pd.to_numeric(denominator, errors="coerce")
    return numerator.where(denominator > 0) / denominator.where(denominator > 0)


def _month_label(year: pd.Series, month: pd.Series) -> pd.Series:
    dates = pd.to_datetime(
        {"year": year.astype("Int64"), "month": month.astype("Int64"), "day": 1},
        errors="coerce",
    )
    return dates.dt.strftime("%Y-%m")


def _haversine_distance_m(
    lat_a: pd.Series,
    lon_a: pd.Series,
    lat_b: pd.Series,
    lon_b: pd.Series,
) -> pd.Series:
    lat_a_rad = np.radians(pd.to_numeric(lat_a, errors="coerce"))
    lon_a_rad = np.radians(pd.to_numeric(lon_a, errors="coerce"))
    lat_b_rad = np.radians(pd.to_numeric(lat_b, errors="coerce"))
    lon_b_rad = np.radians(pd.to_numeric(lon_b, errors="coerce"))
    dlat = lat_b_rad - lat_a_rad
    dlon = lon_b_rad - lon_a_rad
    a = (
        np.sin(dlat / 2.0) ** 2
        + np.cos(lat_a_rad) * np.cos(lat_b_rad) * np.sin(dlon / 2.0) ** 2
    )
    return pd.Series(2.0 * EARTH_RADIUS_M * np.arcsin(np.sqrt(a)), index=lat_a.index)


def _latest_manifest_month(manifest: pd.DataFrame) -> pd.Timestamp | pd.NaT:
    required = {"year", "month"}
    if not required.issubset(manifest.columns):
        return pd.NaT
    months = pd.to_datetime(
        {
            "year": pd.to_numeric(manifest["year"], errors="coerce").astype("Int64"),
            "month": pd.to_numeric(manifest["month"], errors="coerce").astype("Int64"),
            "day": 1,
        },
        errors="coerce",
    )
    if months.dropna().empty:
        return pd.NaT
    return months.max()


def build_sea_basin_mapping_table() -> pd.DataFrame:
    """Return the explicit country/longitude sea-basin mapping rules."""
    return pd.DataFrame(SEA_BASIN_RULES)


def assign_sea_basin(country: Any, longitude: Any) -> tuple[str, str]:
    """Map turbine location to a coarse sea basin using explicit v1 rules."""
    if pd.isna(country):
        return "unknown", "missing_country_unknown"
    country_text = str(country).strip()
    longitude_value = pd.to_numeric(pd.Series([longitude]), errors="coerce").iloc[0]
    if country_text in {"United Kingdom", "Netherlands", "Belgium", "Norway"}:
        return "North Sea", f"{country_text.lower().replace(' ', '_')}_north_sea"
    if country_text == "Sweden":
        return "Baltic", "sweden_baltic"
    if country_text == "France":
        return "other", "france_other"
    if country_text == "Germany":
        if pd.notna(longitude_value) and float(longitude_value) >= 10.0:
            return "Baltic", "germany_baltic"
        return "North Sea", "germany_north_sea"
    if country_text == "Denmark":
        if pd.notna(longitude_value) and float(longitude_value) >= 10.0:
            return "Baltic", "denmark_baltic"
        return "North Sea", "denmark_north_sea"
    return "unknown", "unmapped_country_unknown"


def _age_band(age_years: Any) -> str:
    if pd.isna(age_years):
        return "unknown"
    value = float(age_years)
    if value < 3.0:
        return "0_to_2_years"
    if value < 8.0:
        return "3_to_7_years"
    if value < 15.0:
        return "8_to_14_years"
    return "15_plus_years"


def _age_years_at_observation_end(
    commissioning_month: pd.Series,
    observation_end_month: pd.Timestamp | pd.NaT,
) -> pd.Series:
    if pd.isna(observation_end_month):
        return pd.Series(np.nan, index=commissioning_month.index)
    commissioning_period = pd.to_datetime(commissioning_month, errors="coerce").dt.to_period("M")
    end_period = pd.Timestamp(observation_end_month).to_period("M")
    values = commissioning_period.map(
        lambda period: (end_period.ordinal - period.ordinal) / 12.0
        if pd.notna(period)
        else np.nan
    )
    return pd.to_numeric(values, errors="coerce")


def _distance_quantile_within_farm(group: pd.DataFrame) -> pd.Series:
    distances = pd.to_numeric(group["distance_to_farm_centroid_m"], errors="coerce")
    output = pd.Series(np.nan, index=group.index, dtype=float)
    valid = distances.notna()
    valid_count = int(valid.sum())
    if valid_count == 0:
        return output
    if valid_count < 4:
        output.loc[valid] = 0.5
        return output
    ranks = distances.loc[valid].rank(method="average")
    output.loc[valid] = (ranks - 1.0) / max(valid_count - 1, 1)
    return output


def _join_bathymetry(turbines: pd.DataFrame, bathymetry: pd.DataFrame | None) -> pd.DataFrame:
    working = turbines.copy()
    bathymetry_columns = [
        "water_depth_m",
        "bathymetry_spatial_match_status",
        "bathymetry_join_status",
    ]
    if bathymetry is None or bathymetry.empty:
        for column in bathymetry_columns:
            working[column] = pd.NA
        working["bathymetry_join_status"] = "bathymetry_missing_future_increment"
        return working

    _require_columns(
        bathymetry,
        {"wind_farm", "sample_point_type", "lat", "lon", "water_depth_m"},
        "Processed bathymetry table",
    )
    points = bathymetry.loc[bathymetry["sample_point_type"].eq("turbine")].copy()
    points["_join_lat"] = pd.to_numeric(points["lat"], errors="coerce").round(6)
    points["_join_lon"] = pd.to_numeric(points["lon"], errors="coerce").round(6)
    working["_join_lat"] = pd.to_numeric(working["latitude"], errors="coerce").round(6)
    working["_join_lon"] = pd.to_numeric(working["longitude"], errors="coerce").round(6)
    join_columns = [
        "wind_farm",
        "_join_lat",
        "_join_lon",
        "water_depth_m",
        "bathymetry_spatial_match_status",
    ]
    working = working.merge(points[join_columns], on=["wind_farm", "_join_lat", "_join_lon"], how="left")
    working["bathymetry_join_status"] = np.where(
        working["water_depth_m"].notna(),
        "matched_processed_bathymetry",
        "missing_processed_bathymetry_match",
    )
    return working.drop(columns=["_join_lat", "_join_lon"])


def build_static_turbine_exposure_features(
    turbines: pd.DataFrame,
    bathymetry: pd.DataFrame | None = None,
    observation_end_month: pd.Timestamp | pd.NaT = pd.NaT,
    ramp_up_months: int = DEFAULT_RAMP_UP_MONTHS,
) -> pd.DataFrame:
    """Build turbine static metadata, radial exposure proxies, and optional depth."""
    metadata = prepare_turbine_metadata(turbines)
    metadata["commissioning_month"] = _format_month_start(
        _parse_month_start(metadata.get("commissioning_date", pd.Series(pd.NA, index=metadata.index)))
    )
    metadata["_commissioning_month_dt"] = pd.to_datetime(
        metadata["commissioning_month"],
        errors="coerce",
    )
    metadata["steady_operational_start_month"] = _format_month_start(
        _add_months(metadata["_commissioning_month_dt"], ramp_up_months)
    )
    metadata["commissioning_year"] = metadata["_commissioning_month_dt"].dt.year.astype("Int64")

    centroid = (
        metadata.groupby("farm_id", dropna=False)
        .agg(
            farm_centroid_latitude=("latitude", "mean"),
            farm_centroid_longitude=("longitude", "mean"),
            farm_turbine_count=("turbine_id", "size"),
        )
        .reset_index()
    )
    metadata = metadata.merge(centroid, on="farm_id", how="left")
    metadata["distance_to_farm_centroid_m"] = _haversine_distance_m(
        metadata["latitude"],
        metadata["longitude"],
        metadata["farm_centroid_latitude"],
        metadata["farm_centroid_longitude"],
    )
    metadata["distance_to_farm_centroid_quantile"] = np.nan
    for _, group in metadata.groupby("farm_id", dropna=False):
        metadata.loc[group.index, "distance_to_farm_centroid_quantile"] = (
            _distance_quantile_within_farm(group)
        )
    metadata["outer_ring_proxy"] = (
        (metadata["farm_turbine_count"] >= 4)
        & (metadata["distance_to_farm_centroid_quantile"] >= 0.75)
    )
    metadata["exposure_group"] = "inner_or_middle_proxy"
    metadata.loc[metadata["farm_turbine_count"] < 4, "exposure_group"] = (
        "small_layout_insufficient"
    )
    metadata.loc[metadata["outer_ring_proxy"], "exposure_group"] = "outer_exposed_proxy"
    metadata.loc[metadata["distance_to_farm_centroid_quantile"].isna(), "exposure_group"] = (
        "unknown_exposure_proxy"
    )

    sea_results = metadata.apply(
        lambda row: assign_sea_basin(row.get("country"), row.get("longitude")),
        axis=1,
        result_type="expand",
    )
    metadata["sea_basin"] = sea_results[0]
    metadata["sea_basin_mapping_rule"] = sea_results[1]
    metadata["turbine_age_years_at_observation_end"] = _age_years_at_observation_end(
        metadata["commissioning_month"],
        observation_end_month,
    )
    metadata["turbine_age_band_at_observation_end"] = metadata[
        "turbine_age_years_at_observation_end"
    ].map(_age_band)

    metadata = _join_bathymetry(metadata, bathymetry)
    metadata["analysis_label"] = ANALYSIS_LABEL
    output = metadata.rename(
        columns={
            "rated_power": "rated_capacity_mw",
            "turbine_type": "turbine_model",
            "rotor_diameter": "rotor_diameter_m",
            "hub_height": "hub_height_m",
        }
    )
    return output


def _build_manifest_month_status(manifest: pd.DataFrame) -> pd.DataFrame:
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
    status_sets["month_start"] = pd.to_datetime(
        {"year": status_sets["year"], "month": status_sets["month"], "day": 1},
        errors="coerce",
    )
    status_sets["is_observed"] = status_sets["status_set"].map(
        lambda statuses: bool(OBSERVED_STATUSES.intersection(statuses))
    )
    status_sets["has_success"] = status_sets["status_set"].map(
        lambda statuses: "success" in statuses
    )
    status_sets["has_success_no_ais"] = status_sets["status_set"].map(
        lambda statuses: "success_no_ais_in_bbox" in statuses
    )
    status_sets["has_missing_source"] = status_sets["status_set"].map(
        lambda statuses: MISSING_SOURCE_STATUS in statuses
    )
    status_sets["has_other_status"] = ~(
        status_sets["is_observed"] | status_sets["has_missing_source"]
    )
    return status_sets


def _assign_coverage_class(row: pd.Series) -> str:
    observed_months = int(row.get("observed_steady_months", 0) or 0)
    manifest_months = int(row.get("steady_manifest_months", 0) or 0)
    share = row.get("steady_coverage_share")
    if pd.isna(row.get("steady_operational_start_month")):
        return "unknown_operational_window"
    if manifest_months <= 0:
        return "no_steady_manifest_window"
    if observed_months <= 0:
        return "no_observed_steady_coverage"
    if pd.notna(share) and float(share) >= 0.80 and observed_months >= 60:
        return "high_coverage"
    if pd.notna(share) and float(share) >= 0.50 and observed_months >= 12:
        return "medium_coverage"
    return "low_coverage"


def build_turbine_denominator(
    manifest: pd.DataFrame,
    turbines: pd.DataFrame,
    bathymetry: pd.DataFrame | None = None,
    ramp_up_months: int = DEFAULT_RAMP_UP_MONTHS,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Build one turbine row with steady-operational observed months/years."""
    observation_end = _latest_manifest_month(manifest)
    exposure = build_static_turbine_exposure_features(
        turbines,
        bathymetry=bathymetry,
        observation_end_month=observation_end,
        ramp_up_months=ramp_up_months,
    )
    status_sets = _build_manifest_month_status(manifest)
    turbine_months = exposure[
        [
            "turbine_id",
            "farm_id",
            "steady_operational_start_month",
        ]
    ].merge(status_sets, on="farm_id", how="left")
    turbine_months["_steady_start_dt"] = pd.to_datetime(
        turbine_months["steady_operational_start_month"],
        errors="coerce",
    )
    turbine_months["_in_steady_window"] = (
        turbine_months["month_start"].notna()
        & turbine_months["_steady_start_dt"].notna()
        & (turbine_months["month_start"] >= turbine_months["_steady_start_dt"])
    )
    is_observed = turbine_months["is_observed"].where(
        turbine_months["is_observed"].notna(),
        False,
    ).astype(bool)
    has_success = turbine_months["has_success"].where(
        turbine_months["has_success"].notna(),
        False,
    ).astype(bool)
    has_success_no_ais = turbine_months["has_success_no_ais"].where(
        turbine_months["has_success_no_ais"].notna(),
        False,
    ).astype(bool)
    has_missing_source = turbine_months["has_missing_source"].where(
        turbine_months["has_missing_source"].notna(),
        False,
    ).astype(bool)
    has_other_status = turbine_months["has_other_status"].where(
        turbine_months["has_other_status"].notna(),
        False,
    ).astype(bool)
    turbine_months["_observed_steady"] = (
        turbine_months["_in_steady_window"] & is_observed
    )
    turbine_months["_success_steady"] = (
        turbine_months["_in_steady_window"] & has_success
    )
    turbine_months["_success_no_ais_steady"] = (
        turbine_months["_in_steady_window"] & has_success_no_ais
    )
    turbine_months["_missing_source_steady"] = (
        turbine_months["_in_steady_window"] & has_missing_source
    )
    turbine_months["_other_status_steady"] = (
        turbine_months["_in_steady_window"] & has_other_status
    )
    turbine_months["_observed_steady_month_label"] = (
        turbine_months["month_label"]
        .where(turbine_months["_observed_steady"], pd.NA)
        .astype("string")
    )

    denominator = (
        turbine_months.groupby("turbine_id", dropna=False)
        .agg(
            steady_manifest_months=("_in_steady_window", "sum"),
            observed_steady_months=("_observed_steady", "sum"),
            success_steady_months=("_success_steady", "sum"),
            success_no_ais_in_bbox_steady_months=("_success_no_ais_steady", "sum"),
            skipped_missing_source_steady_months=("_missing_source_steady", "sum"),
            other_status_steady_months=("_other_status_steady", "sum"),
            first_observed_steady_month=(
                "_observed_steady_month_label",
                lambda values: values.dropna().min() if not values.dropna().empty else pd.NA,
            ),
            last_observed_steady_month=(
                "_observed_steady_month_label",
                lambda values: values.dropna().max() if not values.dropna().empty else pd.NA,
            ),
        )
        .reset_index()
    )
    output = exposure.merge(denominator, on="turbine_id", how="left")
    count_columns = [
        "steady_manifest_months",
        "observed_steady_months",
        "success_steady_months",
        "success_no_ais_in_bbox_steady_months",
        "skipped_missing_source_steady_months",
        "other_status_steady_months",
    ]
    for column in count_columns:
        output[column] = output[column].fillna(0).astype(int)
    output["observed_steady_years"] = output["observed_steady_months"] / 12.0
    output["steady_coverage_share"] = _safe_divide(
        output["observed_steady_months"],
        output["steady_manifest_months"],
    )
    output["coverage_class"] = output.apply(_assign_coverage_class, axis=1)
    output["min_1yr_exposure"] = output["observed_steady_years"] >= 1.0
    output["min_3yr_exposure"] = output["observed_steady_years"] >= 3.0
    output["min_5yr_exposure"] = output["observed_steady_years"] >= 5.0
    for column in DENOMINATOR_COLUMNS:
        if column not in output.columns:
            output[column] = pd.NA
    output = output[DENOMINATOR_COLUMNS].sort_values(["farm_id", "turbine_id"]).reset_index(
        drop=True
    )
    metrics = {
        "turbine_rows": int(len(output)),
        "turbine_farm_count": int(output["farm_id"].nunique()),
        "observed_steady_years_total": float(output["observed_steady_years"].sum()),
        "observed_steady_years_min": float(output["observed_steady_years"].min()),
        "observed_steady_years_median": float(output["observed_steady_years"].median()),
        "observed_steady_years_max": float(output["observed_steady_years"].max()),
        "coverage_class_counts": _value_counts_dict(output["coverage_class"]),
        "success_no_ais_in_bbox_steady_months": int(
            output["success_no_ais_in_bbox_steady_months"].sum()
        ),
        "skipped_missing_source_steady_months": int(
            output["skipped_missing_source_steady_months"].sum()
        ),
        "bathymetry_matched_turbines": int(
            output["bathymetry_join_status"].eq("matched_processed_bathymetry").sum()
        ),
    }
    return output, metrics


def _event_summary_for_scope(
    events: pd.DataFrame,
    assignment_confidences: set[str],
    prefix: str,
) -> pd.DataFrame:
    if events.empty:
        return pd.DataFrame(columns=["turbine_id"])
    working = events.copy()
    working["assignment_confidence"] = working["assignment_confidence"].astype("string")
    working = working.loc[
        working["lifecycle_phase"].eq("steady_operational")
        & working["assignment_confidence"].isin(assignment_confidences)
        & working["assigned_turbine_id"].notna()
    ].copy()
    if working.empty:
        return pd.DataFrame(columns=["turbine_id"])
    working["_event_start"] = pd.to_datetime(working["start_utc"], errors="coerce", utc=True)
    working["_event_weight"] = pd.to_numeric(
        working.get("duplicate_adjusted_event_weight", 1.0),
        errors="coerce",
    ).fillna(1.0)

    rows: list[dict[str, Any]] = []
    for turbine_id, group in working.groupby("assigned_turbine_id", dropna=False):
        sorted_group = group.sort_values("_event_start")
        repeat_mask = (
            sorted_group["_event_start"]
            .diff()
            .dt.total_seconds()
            .div(86_400)
            .le(REPEAT_VISIT_WINDOW_DAYS)
            .fillna(False)
        )
        rows.append(
            {
                "turbine_id": turbine_id,
                f"{prefix}_event_count": int(len(sorted_group)),
                f"{prefix}_duplicate_adjusted_event_count": float(
                    sorted_group["_event_weight"].sum()
                ),
                f"{prefix}_unique_vessel_count": int(sorted_group["mmsi"].nunique())
                if "mmsi" in sorted_group
                else 0,
                f"{prefix}_repeat_visit_30d_count": int(repeat_mask.sum()),
            }
        )
    return pd.DataFrame(rows)


def _assign_turbine_intervention_confidence(row: pd.Series) -> str:
    coverage = row.get("coverage_class")
    count = float(row.get("steady_high_duplicate_adjusted_event_count", 0.0) or 0.0)
    if coverage in {"no_steady_manifest_window", "no_observed_steady_coverage", "low_coverage"}:
        return "low_coverage"
    if coverage == "unknown_operational_window":
        return "medium_unknown_operational_window" if count > 0 else "low_signal_ambiguous"
    if coverage == "high_coverage":
        return "high_observed_signal" if count > 0 else "high_observed_zero"
    if coverage == "medium_coverage":
        return "medium_observed_signal" if count > 0 else "medium_observed_zero"
    return "low_signal_ambiguous"


def build_turbine_intervention_intensity(
    turbine_events: pd.DataFrame,
    turbine_denominator: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Aggregate steady-operational turbine intervention intensity v1."""
    _require_columns(
        turbine_denominator,
        {"turbine_id", "observed_steady_years"},
        "RQ9 turbine denominator",
    )
    _require_columns(
        turbine_events,
        {"assigned_turbine_id", "assignment_confidence", "lifecycle_phase"},
        "RQ9 turbine events v0",
    )
    high = _event_summary_for_scope(
        turbine_events,
        {"high"},
        "steady_high",
    )
    high_medium = _event_summary_for_scope(
        turbine_events,
        {"high", "medium"},
        "steady_high_medium",
    )
    output = turbine_denominator.copy()
    output = output.merge(high, on="turbine_id", how="left")
    output = output.merge(high_medium, on="turbine_id", how="left")
    count_columns = [
        "steady_high_event_count",
        "steady_high_unique_vessel_count",
        "steady_high_repeat_visit_30d_count",
        "steady_high_medium_event_count",
        "steady_high_medium_unique_vessel_count",
        "steady_high_medium_repeat_visit_30d_count",
    ]
    adjusted_columns = [
        "steady_high_duplicate_adjusted_event_count",
        "steady_high_medium_duplicate_adjusted_event_count",
    ]
    for column in count_columns:
        output[column] = output[column].fillna(0).astype(int)
    for column in adjusted_columns:
        output[column] = pd.to_numeric(output[column], errors="coerce").fillna(0.0)
    output["primary_intervention_intensity_per_steady_turbine_year"] = _safe_divide(
        output["steady_high_duplicate_adjusted_event_count"],
        output["observed_steady_years"],
    )
    output["sensitivity_intervention_intensity_per_steady_turbine_year"] = _safe_divide(
        output["steady_high_medium_duplicate_adjusted_event_count"],
        output["observed_steady_years"],
    )
    output["turbine_intervention_confidence_class"] = output.apply(
        _assign_turbine_intervention_confidence,
        axis=1,
    )
    for column in INTENSITY_COLUMNS:
        if column not in output.columns:
            output[column] = pd.NA
    output = output[INTENSITY_COLUMNS].sort_values(["farm_id", "turbine_id"]).reset_index(
        drop=True
    )
    metrics = {
        "turbine_rows": int(len(output)),
        "high_confidence_steady_event_count": int(output["steady_high_event_count"].sum()),
        "high_medium_steady_event_count": int(output["steady_high_medium_event_count"].sum()),
        "high_confidence_duplicate_adjusted_count": float(
            output["steady_high_duplicate_adjusted_event_count"].sum()
        ),
        "high_medium_duplicate_adjusted_count": float(
            output["steady_high_medium_duplicate_adjusted_event_count"].sum()
        ),
        "turbines_with_high_signal": int((output["steady_high_event_count"] > 0).sum()),
        "turbines_with_high_medium_signal": int(
            (output["steady_high_medium_event_count"] > 0).sum()
        ),
        "intervention_confidence_counts": _value_counts_dict(
            output["turbine_intervention_confidence_class"]
        ),
    }
    return output, metrics


def _comparison_confidence_class(row: pd.Series) -> str:
    turbine_count = int(row.get("eligible_turbine_count_ge_1yr", 0) or 0)
    observed_years = float(row.get("observed_steady_years", 0.0) or 0.0)
    events = float(row.get("duplicate_adjusted_event_count", 0.0) or 0.0)
    if turbine_count >= 100 and observed_years >= 500 and events > 0:
        return "high_sample_observed_signal"
    if turbine_count >= 20 and observed_years >= 100:
        return "medium_sample_observed"
    if turbine_count > 0 and observed_years > 0:
        return "low_sample_observed"
    return "insufficient_sample"


def build_exposure_comparison(turbine_intensity: pd.DataFrame) -> pd.DataFrame:
    """Compare outer-ring proxy turbines against inner/middle turbines."""
    rows: list[dict[str, Any]] = []
    scope_specs = [
        (
            PRIMARY_SCOPE,
            "steady_high_event_count",
            "steady_high_duplicate_adjusted_event_count",
            "steady_high_unique_vessel_count",
            "steady_high_repeat_visit_30d_count",
            "primary_intervention_intensity_per_steady_turbine_year",
        ),
        (
            SENSITIVITY_SCOPE,
            "steady_high_medium_event_count",
            "steady_high_medium_duplicate_adjusted_event_count",
            "steady_high_medium_unique_vessel_count",
            "steady_high_medium_repeat_visit_30d_count",
            "sensitivity_intervention_intensity_per_steady_turbine_year",
        ),
    ]
    for (
        scope,
        count_column,
        adjusted_column,
        vessel_column,
        repeat_column,
        intensity_column,
    ) in scope_specs:
        for group_name in ["outer_exposed_proxy", "inner_or_middle_proxy"]:
            subset = turbine_intensity.loc[turbine_intensity["exposure_group"].eq(group_name)]
            observed_years = float(subset["observed_steady_years"].sum()) if not subset.empty else 0.0
            adjusted_count = float(subset[adjusted_column].sum()) if not subset.empty else 0.0
            row = {
                "assignment_scope": scope,
                "comparison_group": group_name,
                "turbine_count": int(len(subset)),
                "eligible_turbine_count_ge_1yr": int(subset["min_1yr_exposure"].sum())
                if not subset.empty
                else 0,
                "observed_steady_years": observed_years,
                "event_count": int(subset[count_column].sum()) if not subset.empty else 0,
                "duplicate_adjusted_event_count": adjusted_count,
                "unique_vessel_count_sum": int(subset[vessel_column].sum())
                if not subset.empty
                else 0,
                "repeat_visit_30d_count": int(subset[repeat_column].sum())
                if not subset.empty
                else 0,
                "intervention_intensity_per_steady_turbine_year": adjusted_count
                / observed_years
                if observed_years > 0
                else np.nan,
                "median_turbine_intervention_intensity": float(
                    subset[intensity_column].median(skipna=True)
                )
                if not subset.empty
                else np.nan,
            }
            row["comparison_confidence_class"] = _comparison_confidence_class(pd.Series(row))
            rows.append(row)
    return pd.DataFrame(rows, columns=EXPOSURE_COMPARISON_COLUMNS)


def _markdown_table(frame: pd.DataFrame, columns: list[str]) -> str:
    if frame.empty:
        return "_No rows._"
    return frame[columns].to_markdown(index=False)


def _ratio_summary(comparison: pd.DataFrame, scope: str) -> str:
    scoped = comparison.loc[comparison["assignment_scope"].eq(scope)]
    values = scoped.set_index("comparison_group")[
        "intervention_intensity_per_steady_turbine_year"
    ]
    if not {"outer_exposed_proxy", "inner_or_middle_proxy"}.issubset(values.index):
        return "not available"
    inner = values.loc["inner_or_middle_proxy"]
    outer = values.loc["outer_exposed_proxy"]
    if pd.isna(inner) or inner <= 0 or pd.isna(outer):
        return "not available"
    return f"{outer / inner:.3f}x outer vs inner/middle"


def build_turbine_exposure_report(
    denominator: pd.DataFrame,
    intensity: pd.DataFrame,
    comparison: pd.DataFrame,
    validation: dict[str, Any],
) -> str:
    """Render turbine exposure denominator and comparison report."""
    denominator_summary = pd.DataFrame(
        [
            {
                "metric": "turbines",
                "value": validation.get("turbine_rows"),
            },
            {
                "metric": "farms",
                "value": validation.get("turbine_farm_count"),
            },
            {
                "metric": "observed steady turbine-years",
                "value": f"{validation.get('observed_steady_years_total', 0.0):.3f}",
            },
            {
                "metric": "median observed steady turbine-years",
                "value": f"{validation.get('observed_steady_years_median', 0.0):.3f}",
            },
            {
                "metric": "processed bathymetry matches",
                "value": validation.get("bathymetry_matched_turbines"),
            },
        ]
    )
    coverage_counts = (
        denominator["coverage_class"].value_counts().rename_axis("coverage_class").reset_index(name="rows")
    )
    sea_counts = denominator["sea_basin"].value_counts().rename_axis("sea_basin").reset_index(
        name="turbines"
    )
    metadata_summary = pd.DataFrame(
        [
            {
                "field": "sea_basin",
                "complete_rows": int(denominator["sea_basin"].notna().sum()),
                "total_rows": int(len(denominator)),
            },
            {
                "field": "oem_manufacturer",
                "complete_rows": int(denominator["oem_manufacturer"].notna().sum()),
                "total_rows": int(len(denominator)),
            },
            {
                "field": "rated_capacity_mw",
                "complete_rows": int(denominator["rated_capacity_mw"].notna().sum()),
                "total_rows": int(len(denominator)),
            },
            {
                "field": "water_depth_m",
                "complete_rows": int(denominator["water_depth_m"].notna().sum()),
                "total_rows": int(len(denominator)),
            },
        ]
    )
    red_flags = [
        "Outer exposure is a farm-layout radial proxy, not a directional wind/wave exposure model.",
        "Tier A assignments outside 200 m are sensitivity evidence, not the primary turbine signal.",
        "Pre-operational and commissioning/ramp-up months and events are excluded from primary steady-operational intensity.",
        "Sea-basin mapping is explicit and coarse; review farm-level basin labels before publication.",
        "Simulator multipliers should remain provisional until linked to SCADA, work orders, or equivalent maintenance records.",
    ]
    red_flags_text = "\n".join(f"- {item}" for item in red_flags)
    mapping_table = build_sea_basin_mapping_table()
    return f"""# RQ9 Turbine Exposure and Denominator v1

This report covers turbine-level maintenance intervention intensity using existing RQ9
Tier A turbine-assigned events and existing source-coverage manifests. It is candidate
maintenance evidence for simulator scoping, not confirmed fault evidence.

## Denominator Summary

{_markdown_table(denominator_summary, ["metric", "value"])}

## Assignment Summary

- Primary high-confidence steady Tier A events: {validation.get("high_confidence_steady_event_count")}
- Sensitivity high+medium steady Tier A events: {validation.get("high_medium_steady_event_count")}
- Primary duplicate-adjusted count: {validation.get("high_confidence_duplicate_adjusted_count", 0.0):.3f}
- Sensitivity duplicate-adjusted count: {validation.get("high_medium_duplicate_adjusted_count", 0.0):.3f}

## Coverage Classes

{_markdown_table(coverage_counts, ["coverage_class", "rows"])}

## Exposure Comparison

Primary result: {_ratio_summary(comparison, PRIMARY_SCOPE)}.
Sensitivity result: {_ratio_summary(comparison, SENSITIVITY_SCOPE)}.

{_markdown_table(comparison, EXPOSURE_COMPARISON_COLUMNS)}

## Sea Basin Distribution

{_markdown_table(sea_counts, ["sea_basin", "turbines"])}

## Metadata Completeness

{_markdown_table(metadata_summary, ["field", "complete_rows", "total_rows"])}

## Sea Basin Mapping Rules

{_markdown_table(mapping_table, ["rule_id", "country", "longitude_rule", "sea_basin"])}

## Red Flags and Guardrails

{red_flags_text}
"""


def build_rq9_turbine_exposure_outputs(
    turbine_events_path: Path,
    turbine_path: Path,
    manifest_path: Path,
    farm_intensity_path: Path,
    processed_output_dir: Path,
    report_output_dir: Path,
    bathymetry_path: Path | None = None,
    ramp_up_months: int = DEFAULT_RAMP_UP_MONTHS,
) -> RQ9TurbineExposureOutputs:
    """Build turbine denominator, intensity, exposure comparison, and report."""
    if not turbine_events_path.exists():
        raise FileNotFoundError(f"RQ9 turbine event input not found: {turbine_events_path}")
    turbine_events = pd.read_csv(turbine_events_path)
    turbines = read_turbine_coordinates(turbine_path)
    manifest = read_manifest(manifest_path)
    farm_intensity = read_farm_intensity(farm_intensity_path)
    bathymetry = None
    if bathymetry_path is not None and bathymetry_path.exists():
        bathymetry = pd.read_parquet(bathymetry_path)

    denominator, denominator_metrics = build_turbine_denominator(
        manifest,
        turbines,
        bathymetry=bathymetry,
        ramp_up_months=ramp_up_months,
    )
    intensity, intensity_metrics = build_turbine_intervention_intensity(
        turbine_events,
        denominator,
    )
    comparison = build_exposure_comparison(intensity)
    validation = {
        **denominator_metrics,
        **intensity_metrics,
        "farm_intensity_rows": int(len(farm_intensity)),
        "farm_intensity_farms": int(farm_intensity["farm_id"].nunique())
        if "farm_id" in farm_intensity
        else 0,
        "sea_basin_counts": _value_counts_dict(denominator["sea_basin"]),
        "oem_complete_turbines": int(denominator["oem_manufacturer"].notna().sum()),
        "capacity_complete_turbines": int(denominator["rated_capacity_mw"].notna().sum()),
        "bathymetry_input_available": bool(bathymetry is not None),
        "exposure_comparison_rows": int(len(comparison)),
    }
    report = build_turbine_exposure_report(denominator, intensity, comparison, validation)

    processed_output_dir.mkdir(parents=True, exist_ok=True)
    report_output_dir.mkdir(parents=True, exist_ok=True)
    files = {
        "turbine_exposure_denominator_csv": processed_output_dir
        / "turbine_exposure_denominator.csv",
        "turbine_intervention_intensity_v1_csv": processed_output_dir
        / "turbine_intervention_intensity_v1.csv",
        "turbine_exposure_comparison_csv": report_output_dir
        / "turbine_exposure_comparison.csv",
        "turbine_exposure_intervention_report_md": report_output_dir
        / "turbine_exposure_intervention_report.md",
    }
    denominator.to_csv(files["turbine_exposure_denominator_csv"], index=False)
    intensity.to_csv(files["turbine_intervention_intensity_v1_csv"], index=False)
    comparison.to_csv(files["turbine_exposure_comparison_csv"], index=False)
    files["turbine_exposure_intervention_report_md"].write_text(report, encoding="utf-8")
    return RQ9TurbineExposureOutputs(
        processed_output_dir=processed_output_dir,
        report_output_dir=report_output_dir,
        files=files,
        validation={key: _jsonable(value) for key, value in validation.items()},
    )
