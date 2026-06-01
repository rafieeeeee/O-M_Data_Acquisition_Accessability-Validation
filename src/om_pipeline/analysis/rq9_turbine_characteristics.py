"""Turbine characteristics comparison for RQ9 intervention intensity.

This module compares steady-operational maintenance intervention intensity
across turbine metadata groups. It uses existing turbine-level RQ9 outputs
only; the evidence remains candidate maintenance intervention evidence rather
than confirmed fault evidence.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


ANALYSIS_LABEL = "RQ9 turbine characteristics maintenance intervention intensity v1"
PRIMARY_SCOPE = "high_confidence_200m"
SENSITIVITY_SCOPE = "high_medium_500m"

EXPOSURE_THRESHOLDS = [
    ("all_turbines", 0.0, "all"),
    ("min_1yr", 1.0, ">=1 steady observed year"),
    ("min_3yr", 3.0, ">=3 steady observed years"),
    ("min_5yr", 5.0, ">=5 steady observed years"),
]

CHARACTERISTIC_SPECS = [
    ("oem_manufacturer", "oem_manufacturer"),
    ("turbine_model", "turbine_model"),
    ("rated_capacity_band", "rated_capacity_band"),
    ("rotor_diameter_band", "rotor_diameter_band"),
    ("hub_height_band", "hub_height_band"),
    ("commissioning_year", "commissioning_year"),
    ("operational_age_band", "turbine_age_band_at_observation_end"),
    ("sea_basin", "sea_basin"),
    ("country", "country"),
    ("farm", "wind_farm"),
]

CHARACTERISTICS_RATE_COLUMNS = [
    "analysis_label",
    "characteristic",
    "characteristic_value",
    "exposure_threshold",
    "exposure_threshold_description",
    "turbine_count",
    "farm_count",
    "oem_count",
    "observed_steady_turbine_years",
    "high_confidence_event_count",
    "high_confidence_duplicate_adjusted_event_count",
    "high_medium_event_count",
    "high_medium_duplicate_adjusted_event_count",
    "primary_raw_intervention_intensity_per_turbine_year",
    "primary_duplicate_adjusted_intervention_intensity_per_turbine_year",
    "sensitivity_raw_intervention_intensity_per_turbine_year",
    "sensitivity_duplicate_adjusted_intervention_intensity_per_turbine_year",
    "zero_event_turbine_count",
    "zero_event_turbine_share",
    "dominant_farm",
    "dominant_farm_turbine_share",
    "dominant_oem",
    "dominant_oem_turbine_share",
    "short_age_turbine_share",
    "interpretation_flag",
]

COMPARISON_COLUMNS = [
    "comparison_focus",
    "primary_rank",
    *CHARACTERISTICS_RATE_COLUMNS,
]


@dataclass(frozen=True)
class RQ9TurbineCharacteristicsOutputs:
    """Paths and summary values written by the characteristics builder."""

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


def _safe_ratio(numerator: float, denominator: float) -> float:
    if denominator <= 0:
        return np.nan
    return float(numerator) / float(denominator)


def capacity_band(value: Any) -> str:
    """Return stable rated-capacity bands in MW."""
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(numeric):
        return "unknown"
    if numeric < 3.0:
        return "lt_3_mw"
    if numeric < 5.0:
        return "3_to_4_9_mw"
    if numeric < 8.0:
        return "5_to_7_9_mw"
    if numeric < 10.0:
        return "8_to_9_9_mw"
    return "10_plus_mw"


def rotor_diameter_band(value: Any) -> str:
    """Return stable rotor-diameter bands in metres."""
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(numeric):
        return "unknown"
    if numeric < 100.0:
        return "lt_100_m"
    if numeric < 130.0:
        return "100_to_129_m"
    if numeric < 160.0:
        return "130_to_159_m"
    if numeric < 180.0:
        return "160_to_179_m"
    return "180_plus_m"


def hub_height_band(value: Any) -> str:
    """Return stable hub-height bands in metres."""
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(numeric):
        return "unknown"
    if numeric < 70.0:
        return "lt_70_m"
    if numeric < 90.0:
        return "70_to_89_m"
    if numeric < 110.0:
        return "90_to_109_m"
    if numeric < 130.0:
        return "110_to_129_m"
    return "130_plus_m"


def add_characteristic_bands(turbine_intensity: pd.DataFrame) -> pd.DataFrame:
    """Add turbine characteristic comparison bands."""
    working = turbine_intensity.copy()
    working["rated_capacity_band"] = working.get("rated_capacity_mw", pd.Series(index=working.index)).map(
        capacity_band
    )
    working["rotor_diameter_band"] = working.get("rotor_diameter_m", pd.Series(index=working.index)).map(
        rotor_diameter_band
    )
    working["hub_height_band"] = working.get("hub_height_m", pd.Series(index=working.index)).map(
        hub_height_band
    )
    working["commissioning_year"] = (
        pd.to_numeric(working.get("commissioning_year", pd.Series(index=working.index)), errors="coerce")
        .astype("Int64")
        .astype("string")
        .fillna("unknown")
    )
    working["turbine_age_band_at_observation_end"] = (
        working.get("turbine_age_band_at_observation_end", pd.Series(index=working.index))
        .astype("string")
        .fillna("unknown")
    )
    for column in ["oem_manufacturer", "turbine_model", "sea_basin", "country", "wind_farm"]:
        working[column] = working.get(column, pd.Series(index=working.index)).astype("string").fillna(
            "unknown"
        )
    return working


def _dominant_value_and_share(group: pd.DataFrame, column: str) -> tuple[str, float]:
    if group.empty or column not in group:
        return "unknown", np.nan
    counts = group[column].astype("string").fillna("unknown").value_counts(dropna=False)
    if counts.empty:
        return "unknown", np.nan
    return str(counts.index[0]), float(counts.iloc[0] / len(group))


def _interpretation_flag(row: dict[str, Any]) -> str:
    turbine_count = int(row["turbine_count"])
    observed_years = float(row["observed_steady_turbine_years"])
    adjusted_events = float(row["high_confidence_duplicate_adjusted_event_count"])
    dominant_farm_share = float(row["dominant_farm_turbine_share"])
    dominant_oem_share = float(row["dominant_oem_turbine_share"])
    short_age_share = float(row["short_age_turbine_share"])
    threshold = str(row["exposure_threshold"])
    characteristic = str(row["characteristic"])

    if turbine_count < 20 or observed_years < 50.0:
        return "insufficient_exposure"
    if short_age_share >= 0.60 or (threshold in {"all_turbines", "min_1yr"} and observed_years / turbine_count < 3.0):
        return "short_operational_age"
    if characteristic != "farm" and dominant_farm_share >= 0.60:
        return "single_farm_dominated"
    if characteristic not in {"oem_manufacturer", "turbine_model", "farm"} and dominant_oem_share >= 0.90:
        return "single_oem_farm_confounded"
    if turbine_count >= 50 and observed_years >= 250.0 and adjusted_events >= 10.0:
        return "robust_enough_for_exploratory_comparison"
    return "insufficient_event_signal"


def _group_rate_row(
    group: pd.DataFrame,
    characteristic: str,
    characteristic_value: str,
    threshold_name: str,
    threshold_description: str,
) -> dict[str, Any]:
    observed_years = float(group["observed_steady_years"].sum())
    high_events = int(group["steady_high_event_count"].sum())
    high_adjusted = float(group["steady_high_duplicate_adjusted_event_count"].sum())
    high_medium_events = int(group["steady_high_medium_event_count"].sum())
    high_medium_adjusted = float(
        group["steady_high_medium_duplicate_adjusted_event_count"].sum()
    )
    dominant_farm, dominant_farm_share = _dominant_value_and_share(group, "wind_farm")
    dominant_oem, dominant_oem_share = _dominant_value_and_share(group, "oem_manufacturer")
    short_age_share = float(
        group["turbine_age_band_at_observation_end"].astype("string").eq("0_to_2_years").mean()
    )
    row = {
        "analysis_label": ANALYSIS_LABEL,
        "characteristic": characteristic,
        "characteristic_value": characteristic_value,
        "exposure_threshold": threshold_name,
        "exposure_threshold_description": threshold_description,
        "turbine_count": int(len(group)),
        "farm_count": int(group["wind_farm"].nunique(dropna=True)),
        "oem_count": int(group["oem_manufacturer"].nunique(dropna=True)),
        "observed_steady_turbine_years": observed_years,
        "high_confidence_event_count": high_events,
        "high_confidence_duplicate_adjusted_event_count": high_adjusted,
        "high_medium_event_count": high_medium_events,
        "high_medium_duplicate_adjusted_event_count": high_medium_adjusted,
        "primary_raw_intervention_intensity_per_turbine_year": _safe_ratio(
            high_events, observed_years
        ),
        "primary_duplicate_adjusted_intervention_intensity_per_turbine_year": _safe_ratio(
            high_adjusted, observed_years
        ),
        "sensitivity_raw_intervention_intensity_per_turbine_year": _safe_ratio(
            high_medium_events, observed_years
        ),
        "sensitivity_duplicate_adjusted_intervention_intensity_per_turbine_year": _safe_ratio(
            high_medium_adjusted, observed_years
        ),
        "zero_event_turbine_count": int(group["steady_high_event_count"].eq(0).sum()),
        "zero_event_turbine_share": float(group["steady_high_event_count"].eq(0).mean()),
        "dominant_farm": dominant_farm,
        "dominant_farm_turbine_share": dominant_farm_share,
        "dominant_oem": dominant_oem,
        "dominant_oem_turbine_share": dominant_oem_share,
        "short_age_turbine_share": short_age_share,
    }
    row["interpretation_flag"] = _interpretation_flag(row)
    return row


def build_characteristics_rates(turbine_intensity: pd.DataFrame) -> pd.DataFrame:
    """Aggregate intervention intensity across turbine characteristic groups."""
    _require_columns(
        turbine_intensity,
        {
            "wind_farm",
            "oem_manufacturer",
            "turbine_model",
            "rated_capacity_mw",
            "rotor_diameter_m",
            "hub_height_m",
            "commissioning_year",
            "turbine_age_band_at_observation_end",
            "sea_basin",
            "country",
            "observed_steady_years",
            "steady_high_event_count",
            "steady_high_duplicate_adjusted_event_count",
            "steady_high_medium_event_count",
            "steady_high_medium_duplicate_adjusted_event_count",
        },
        "RQ9 turbine intervention intensity",
    )
    working = add_characteristic_bands(turbine_intensity)
    rows: list[dict[str, Any]] = []
    for threshold_name, minimum_years, threshold_description in EXPOSURE_THRESHOLDS:
        thresholded = working.loc[
            pd.to_numeric(working["observed_steady_years"], errors="coerce").fillna(0.0)
            >= minimum_years
        ].copy()
        for characteristic, source_column in CHARACTERISTIC_SPECS:
            for value, group in thresholded.groupby(source_column, dropna=False):
                rows.append(
                    _group_rate_row(
                        group,
                        characteristic,
                        str(value),
                        threshold_name,
                        threshold_description,
                    )
                )
    output = pd.DataFrame(rows, columns=CHARACTERISTICS_RATE_COLUMNS)
    return output.sort_values(
        [
            "characteristic",
            "exposure_threshold",
            "primary_duplicate_adjusted_intervention_intensity_per_turbine_year",
            "observed_steady_turbine_years",
        ],
        ascending=[True, True, False, False],
    ).reset_index(drop=True)


def build_characteristics_comparison(rates: pd.DataFrame) -> pd.DataFrame:
    """Build a compact comparison table for thesis-facing review."""
    focus = rates.loc[
        rates["exposure_threshold"].eq("min_3yr")
        & rates["characteristic"].isin(
            [
                "oem_manufacturer",
                "turbine_model",
                "rated_capacity_band",
                "rotor_diameter_band",
                "hub_height_band",
                "operational_age_band",
                "sea_basin",
                "country",
            ]
        )
    ].copy()
    focus = focus.loc[focus["turbine_count"] >= 20].copy()
    focus["primary_rank"] = (
        focus.groupby("characteristic")[
            "primary_duplicate_adjusted_intervention_intensity_per_turbine_year"
        ]
        .rank(method="first", ascending=False)
        .astype(int)
    )
    focus = focus.loc[focus["primary_rank"] <= 10].copy()
    focus["comparison_focus"] = "min_3yr_top_groups_by_primary_duplicate_adjusted_intensity"
    return focus[COMPARISON_COLUMNS].sort_values(
        ["characteristic", "primary_rank", "observed_steady_turbine_years"]
    )


def _markdown_table(frame: pd.DataFrame, columns: list[str]) -> str:
    if frame.empty:
        return "_No rows._"
    return frame[columns].to_markdown(index=False)


def _top_rows_for_report(comparison: pd.DataFrame, characteristic: str, limit: int = 5) -> pd.DataFrame:
    subset = comparison.loc[comparison["characteristic"].eq(characteristic)].copy()
    return subset.sort_values("primary_rank").head(limit)


def build_characteristics_report(
    rates: pd.DataFrame,
    comparison: pd.DataFrame,
    validation: dict[str, Any],
) -> str:
    """Render the turbine characteristics comparison report."""
    summary = pd.DataFrame(
        [
            {"metric": "turbines", "value": validation.get("turbine_rows")},
            {
                "metric": "observed steady turbine-years",
                "value": f"{validation.get('observed_steady_years_total', 0.0):.3f}",
            },
            {
                "metric": "high-confidence steady Tier A events",
                "value": validation.get("high_confidence_event_count"),
            },
            {
                "metric": "high+medium steady Tier A events",
                "value": validation.get("high_medium_event_count"),
            },
            {
                "metric": "robust exploratory rows",
                "value": validation.get("robust_exploratory_rows"),
            },
        ]
    )
    flag_counts = (
        rates["interpretation_flag"]
        .value_counts()
        .rename_axis("interpretation_flag")
        .reset_index(name="rows")
    )
    oem_top = _top_rows_for_report(comparison, "oem_manufacturer")
    model_top = _top_rows_for_report(comparison, "turbine_model")
    capacity_top = _top_rows_for_report(comparison, "rated_capacity_band")
    age_top = _top_rows_for_report(comparison, "operational_age_band")
    basin_top = _top_rows_for_report(comparison, "sea_basin")
    warnings = [
        "High-confidence Tier A within 200 m is the primary evidence scope; high+medium within 500 m is sensitivity only.",
        "Rows marked single-farm dominated or single-OEM/farm confounded should not be interpreted as causal turbine-characteristic effects.",
        "Short operational age groups can reflect commissioning-era observation windows even after ramp-up exclusion.",
        "The output is maintenance intervention intensity, not confirmed fault evidence.",
    ]
    warnings_text = "\n".join(f"- {item}" for item in warnings)
    robust = rates.loc[
        rates["interpretation_flag"].eq("robust_enough_for_exploratory_comparison")
    ]
    robust_characteristics = ", ".join(sorted(robust["characteristic"].unique()))
    if not robust_characteristics:
        robust_characteristics = "none"
    return f"""# RQ9 Turbine Characteristics Comparison v1

This report compares steady-operational maintenance intervention intensity by turbine
characteristics. It uses existing turbine-assigned Tier A evidence and turbine-year
denominators only. It is not confirmed fault evidence.

## Summary

{_markdown_table(summary, ["metric", "value"])}

## Interpretation Flags

{_markdown_table(flag_counts, ["interpretation_flag", "rows"])}

Robust enough for exploratory thesis discussion: {robust_characteristics}.

## Top OEM Groups

{_markdown_table(oem_top, ["characteristic_value", "turbine_count", "observed_steady_turbine_years", "high_confidence_duplicate_adjusted_event_count", "primary_duplicate_adjusted_intervention_intensity_per_turbine_year", "zero_event_turbine_share", "interpretation_flag"])}

## Top Model Groups

{_markdown_table(model_top, ["characteristic_value", "turbine_count", "observed_steady_turbine_years", "high_confidence_duplicate_adjusted_event_count", "primary_duplicate_adjusted_intervention_intensity_per_turbine_year", "zero_event_turbine_share", "interpretation_flag"])}

## Top Capacity Bands

{_markdown_table(capacity_top, ["characteristic_value", "turbine_count", "observed_steady_turbine_years", "high_confidence_duplicate_adjusted_event_count", "primary_duplicate_adjusted_intervention_intensity_per_turbine_year", "zero_event_turbine_share", "interpretation_flag"])}

## Top Operational Age Bands

{_markdown_table(age_top, ["characteristic_value", "turbine_count", "observed_steady_turbine_years", "high_confidence_duplicate_adjusted_event_count", "primary_duplicate_adjusted_intervention_intensity_per_turbine_year", "zero_event_turbine_share", "interpretation_flag"])}

## Sea Basin Comparison

{_markdown_table(basin_top, ["characteristic_value", "turbine_count", "observed_steady_turbine_years", "high_confidence_duplicate_adjusted_event_count", "primary_duplicate_adjusted_intervention_intensity_per_turbine_year", "zero_event_turbine_share", "interpretation_flag"])}

## Guardrails

{warnings_text}
"""


def build_rq9_turbine_characteristics_outputs(
    turbine_intensity_path: Path,
    turbine_denominator_path: Path,
    turbine_events_path: Path,
    turbine_coordinates_path: Path,
    processed_output_dir: Path,
    report_output_dir: Path,
) -> RQ9TurbineCharacteristicsOutputs:
    """Build turbine characteristics rates, comparison table, and report."""
    for path in [
        turbine_intensity_path,
        turbine_denominator_path,
        turbine_events_path,
        turbine_coordinates_path,
    ]:
        if not path.exists():
            raise FileNotFoundError(f"RQ9 turbine characteristics input not found: {path}")

    intensity = pd.read_csv(turbine_intensity_path)
    denominator = pd.read_csv(turbine_denominator_path)
    events = pd.read_csv(turbine_events_path, usecols=lambda column: True)
    turbine_coordinates = pd.read_csv(turbine_coordinates_path, nrows=5)

    rates = build_characteristics_rates(intensity)
    comparison = build_characteristics_comparison(rates)
    validation = {
        "turbine_rows": int(len(intensity)),
        "denominator_rows": int(len(denominator)),
        "event_rows_v0": int(len(events)),
        "turbine_coordinate_columns_seen": sorted(str(column) for column in turbine_coordinates.columns),
        "observed_steady_years_total": float(intensity["observed_steady_years"].sum()),
        "high_confidence_event_count": int(intensity["steady_high_event_count"].sum()),
        "high_medium_event_count": int(intensity["steady_high_medium_event_count"].sum()),
        "rates_rows": int(len(rates)),
        "comparison_rows": int(len(comparison)),
        "robust_exploratory_rows": int(
            rates["interpretation_flag"].eq("robust_enough_for_exploratory_comparison").sum()
        ),
        "interpretation_flag_counts": {
            str(key): int(value)
            for key, value in rates["interpretation_flag"].value_counts().items()
        },
    }
    report = build_characteristics_report(rates, comparison, validation)

    processed_output_dir.mkdir(parents=True, exist_ok=True)
    report_output_dir.mkdir(parents=True, exist_ok=True)
    files = {
        "turbine_characteristics_rates_csv": processed_output_dir
        / "turbine_characteristics_rates.csv",
        "turbine_characteristics_comparison_csv": report_output_dir
        / "turbine_characteristics_comparison.csv",
        "turbine_characteristics_report_md": report_output_dir
        / "turbine_characteristics_report.md",
    }
    rates.to_csv(files["turbine_characteristics_rates_csv"], index=False)
    comparison.to_csv(files["turbine_characteristics_comparison_csv"], index=False)
    files["turbine_characteristics_report_md"].write_text(report, encoding="utf-8")
    return RQ9TurbineCharacteristicsOutputs(
        processed_output_dir=processed_output_dir,
        report_output_dir=report_output_dir,
        files=files,
        validation={key: _jsonable(value) for key, value in validation.items()},
    )
