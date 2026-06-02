"""Evidence-readiness audit for integrated O&M data layers.

The audit is a derived, read-only integration layer. It describes what the
current AIS, turbine, metocean, vessel, geography, and SCADA evidence can
support before new research modelling starts.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
import unicodedata
from typing import Any

import numpy as np
import pandas as pd


OBSERVED_STATUSES = frozenset({"success", "success_no_ais_in_bbox"})
MISSING_SOURCE_STATUS = "skipped_missing_source"
CANDIDATE_TIERS = frozenset({"Tier A", "Tier B"})

DIRECT_AIS_RECEIVER_PATTERNS = (
    "receiver",
    "ais_station",
    "station_id",
    "terrestrial",
    "satellite",
    "base_station",
    "distance_to_receiver",
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

RQ_READINESS_COLUMNS = [
    "rq_number",
    "required_data_layers",
    "available_layers",
    "missing_layers",
    "geographic_bias_risk",
    "observability_bias_risk",
    "answerability",
    "recommended_next_action",
]


@dataclass(frozen=True)
class EvidenceReadinessOutputs:
    """Paths and summary values written by the evidence-readiness builder."""

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


def _month_range(start: Any, end: Any) -> list[str]:
    start_ts = pd.to_datetime(start, errors="coerce")
    end_ts = pd.to_datetime(end, errors="coerce")
    if pd.isna(start_ts) or pd.isna(end_ts):
        return []
    return [str(period) for period in pd.period_range(start_ts, end_ts, freq="M")]


def _safe_share(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    numerator = pd.to_numeric(numerator, errors="coerce")
    denominator = pd.to_numeric(denominator, errors="coerce")
    with np.errstate(divide="ignore", invalid="ignore"):
        result = numerator / denominator
    result = result.where(denominator > 0)
    return result


def _ascii_slug(value: Any) -> str:
    text = "" if pd.isna(value) else str(value)
    normalized = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode()
    normalized = re.sub(r"[^A-Za-z0-9]+", "_", normalized)
    return normalized.strip("_")


def _farm_aliases(farm_id: Any, wind_farm: Any | None = None) -> set[str]:
    values = [farm_id]
    if wind_farm is not None:
        values.append(wind_farm)
    aliases: set[str] = set()
    for value in values:
        if value is None or pd.isna(value):
            continue
        text = str(value)
        aliases.add(text)
        aliases.add(text.replace(" ", "_"))
        aliases.add(text.replace("_", " "))
        aliases.add(_ascii_slug(text))
    return {alias for alias in aliases if alias}


def build_farm_alias_map(*frames: pd.DataFrame | None) -> dict[str, str]:
    """Build a tolerant farm-name alias map from available farm metadata frames."""
    alias_map: dict[str, str] = {}
    for frame in frames:
        if frame is None or frame.empty or "farm_id" not in frame.columns:
            continue
        wind_col = "wind_farm" if "wind_farm" in frame.columns else None
        for _, row in frame.drop_duplicates("farm_id").iterrows():
            farm_id = row["farm_id"]
            wind_farm = row[wind_col] if wind_col else None
            for alias in _farm_aliases(farm_id, wind_farm):
                alias_map.setdefault(alias, farm_id)
    return alias_map


def has_direct_ais_receiver_metadata(*frames: pd.DataFrame | None) -> bool:
    """Return true only when direct AIS detectability metadata is present."""
    for frame in frames:
        if frame is None:
            continue
        for column in frame.columns:
            lowered = column.lower()
            if any(pattern in lowered for pattern in DIRECT_AIS_RECEIVER_PATTERNS):
                return True
    return False


def contains_ais_only_failure_rate_claim(text: str) -> bool:
    """Detect prohibited claims that AIS-only evidence yields failure rates."""
    normalized = re.sub(r"\s+", " ", text.lower())
    prohibited = (
        "ais failure rate",
        "failure rate from ais",
        "failure rates from ais",
        "failure rate derived from ais",
        "ais-only failure rate",
        "ais only failure rate",
    )
    return any(phrase in normalized for phrase in prohibited)


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


def build_farm_metadata(
    turbines: pd.DataFrame | None = None,
    farm_intensity: pd.DataFrame | None = None,
    turbine_exposure: pd.DataFrame | None = None,
    turbine_intensity: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Build one metadata row per farm from the strongest available sources."""
    frames: list[pd.DataFrame] = []

    if turbine_exposure is not None and not turbine_exposure.empty:
        exposure = turbine_exposure.copy()
        _require_columns(exposure, {"farm_id"}, "turbine exposure")
        if "commissioning_month" not in exposure.columns and "commissioning_date" in exposure.columns:
            exposure["commissioning_month"] = exposure["commissioning_date"]
        grouped = exposure.groupby("farm_id", dropna=False).agg(
            wind_farm=("wind_farm", _first_non_null)
            if "wind_farm" in exposure.columns
            else ("farm_id", _first_non_null),
            country=("country", _first_non_null) if "country" in exposure.columns else ("farm_id", _first_non_null),
            sea_basin=("sea_basin", _first_non_null)
            if "sea_basin" in exposure.columns
            else ("farm_id", _first_non_null),
            commissioning_date=("commissioning_month", "min")
            if "commissioning_month" in exposure.columns
            else ("farm_id", _first_non_null),
            steady_operational_start_month=("steady_operational_start_month", _first_non_null)
            if "steady_operational_start_month" in exposure.columns
            else ("farm_id", _first_non_null),
            turbine_count=("turbine_id", "nunique")
            if "turbine_id" in exposure.columns
            else ("farm_id", "size"),
        )
        frames.append(grouped.reset_index())

    if turbine_intensity is not None and not turbine_intensity.empty:
        intensity = turbine_intensity.copy()
        if "farm_id" in intensity.columns:
            grouped = intensity.groupby("farm_id", dropna=False).agg(
                wind_farm=("wind_farm", _first_non_null)
                if "wind_farm" in intensity.columns
                else ("farm_id", _first_non_null),
                country=("country", _first_non_null)
                if "country" in intensity.columns
                else ("farm_id", _first_non_null),
                sea_basin=("sea_basin", _first_non_null)
                if "sea_basin" in intensity.columns
                else ("farm_id", _first_non_null),
                commissioning_date=("commissioning_year", "min")
                if "commissioning_year" in intensity.columns
                else ("farm_id", _first_non_null),
                turbine_count=("turbine_id", "nunique")
                if "turbine_id" in intensity.columns
                else ("farm_id", "size"),
            )
            grouped["commissioning_date"] = grouped["commissioning_date"].apply(
                lambda value: f"{int(value):04d}-01" if pd.notna(value) else pd.NA
            )
            frames.append(grouped.reset_index())

    if farm_intensity is not None and not farm_intensity.empty:
        farms = farm_intensity.copy()
        if "farm_id" in farms.columns:
            keep = [
                column
                for column in [
                    "farm_id",
                    "turbine_count",
                    "farm_commissioning_start_month",
                    "steady_operational_start_month",
                ]
                if column in farms.columns
            ]
            grouped = farms[keep].drop_duplicates("farm_id")
            if "farm_commissioning_start_month" in grouped.columns:
                grouped = grouped.rename(
                    columns={"farm_commissioning_start_month": "commissioning_date"}
                )
            frames.append(grouped)

    if turbines is not None and not turbines.empty:
        raw = turbines.copy()
        if "wind_farm" in raw.columns:
            raw["farm_id"] = raw["wind_farm"]
            grouped = raw.groupby("farm_id", dropna=False).agg(
                wind_farm=("wind_farm", _first_non_null),
                country=("country", _first_non_null)
                if "country" in raw.columns
                else ("farm_id", _first_non_null),
                commissioning_date=("commissioning_date", "min")
                if "commissioning_date" in raw.columns
                else ("farm_id", _first_non_null),
                turbine_count=("wind_farm", "size"),
            )
            frames.append(grouped.reset_index())

    if not frames:
        return pd.DataFrame(columns=["farm_id", "wind_farm", "country", "sea_basin"])

    combined = pd.concat(frames, ignore_index=True, sort=False)
    metadata = combined.groupby("farm_id", dropna=False).agg(_first_non_null).reset_index()
    if "wind_farm" not in metadata.columns:
        metadata["wind_farm"] = metadata["farm_id"]
    if "country" not in metadata.columns:
        metadata["country"] = pd.NA
    if "sea_basin" not in metadata.columns:
        metadata["sea_basin"] = pd.NA
    if "commissioning_date" not in metadata.columns:
        metadata["commissioning_date"] = pd.NA
    if "steady_operational_start_month" not in metadata.columns:
        metadata["steady_operational_start_month"] = pd.NA
    return metadata


def build_archive_year_index(
    root: Path,
    alias_map: dict[str, str],
    availability_column: str,
    source_label: str,
) -> pd.DataFrame:
    """Index available farm-year archive partitions without reading parquet data."""
    rows: list[dict[str, Any]] = []
    if not root.exists():
        return pd.DataFrame(columns=["farm_id", "year", availability_column, f"{availability_column}_source"])

    for part_path in root.glob("wind_farm=*/year=*/part.parquet"):
        wind_part = part_path.parent.parent.name
        year_part = part_path.parent.name
        if not wind_part.startswith("wind_farm=") or not year_part.startswith("year="):
            continue
        slug = wind_part.split("=", 1)[1]
        year_text = year_part.split("=", 1)[1]
        try:
            year = int(year_text)
        except ValueError:
            continue
        farm_id = alias_map.get(slug) or alias_map.get(_ascii_slug(slug)) or slug.replace("_", " ")
        rows.append(
            {
                "farm_id": farm_id,
                "year": year,
                availability_column: True,
                f"{availability_column}_source": source_label,
            }
        )

    if not rows:
        return pd.DataFrame(columns=["farm_id", "year", availability_column, f"{availability_column}_source"])
    indexed = pd.DataFrame(rows).drop_duplicates(["farm_id", "year", f"{availability_column}_source"])
    return (
        indexed.groupby(["farm_id", "year"], as_index=False)
        .agg(
            {
                availability_column: "max",
                f"{availability_column}_source": _join_unique,
            }
        )
    )


def build_current_archive_year_index(manifest_path: Path, alias_map: dict[str, str]) -> pd.DataFrame:
    """Index accepted NWS current farm-years from the existing manifest."""
    columns = ["farm_id", "year", "current_archive_year_available", "current_archive_year_available_source"]
    if not manifest_path.exists():
        return pd.DataFrame(columns=columns)
    manifest = pd.read_csv(manifest_path)
    if "year" not in manifest.columns:
        return pd.DataFrame(columns=columns)
    farm_col = "wind_farm" if "wind_farm" in manifest.columns else None
    slug_col = "farm_slug" if "farm_slug" in manifest.columns else None
    status_col = "status" if "status" in manifest.columns else None
    rows: list[dict[str, Any]] = []
    for _, row in manifest.iterrows():
        if status_col and str(row[status_col]).lower() not in {"validated", "accepted", "passed"}:
            continue
        raw_name = row[farm_col] if farm_col else row.get(slug_col, pd.NA)
        slug = row[slug_col] if slug_col else raw_name
        farm_id = (
            alias_map.get(str(raw_name))
            or alias_map.get(str(slug))
            or alias_map.get(_ascii_slug(raw_name))
            or str(raw_name)
        )
        rows.append(
            {
                "farm_id": farm_id,
                "year": int(row["year"]),
                "current_archive_year_available": True,
                "current_archive_year_available_source": "nws_current_timeseries",
            }
        )
    if not rows:
        return pd.DataFrame(columns=columns)
    return pd.DataFrame(rows).drop_duplicates(["farm_id", "year"])


def build_scada_validation_months(care_root: Path) -> pd.DataFrame:
    """Map local CARE validation windows to de-anonymized farm-months."""
    farm_map = {
        "Wind Farm B": ("Alpha Ventus", "CARE Wind Farm B raw SCADA events"),
        "Wind Farm C": (
            "Trianel Windpark Borkum 1;Trianel Windpark Borkum 2",
            "CARE Wind Farm C processed feature matrix/events",
        ),
    }
    rows: list[dict[str, str]] = []
    for care_name, (farm_names, source) in farm_map.items():
        event_path = care_root / care_name / "event_info.csv"
        if not event_path.exists():
            continue
        events = pd.read_csv(event_path, sep=";")
        if not {"event_start", "event_end"}.issubset(events.columns):
            continue
        for _, event in events.iterrows():
            for month in _month_range(event["event_start"], event["event_end"]):
                for farm_id in farm_names.split(";"):
                    rows.append(
                        {
                            "farm_id": farm_id,
                            "month": month,
                            "scada_validation_available": True,
                            "scada_validation_source": source,
                        }
                    )
    if not rows:
        return pd.DataFrame(
            columns=["farm_id", "month", "scada_validation_available", "scada_validation_source"]
        )
    return (
        pd.DataFrame(rows)
        .drop_duplicates()
        .groupby(["farm_id", "month"], as_index=False)
        .agg(
            scada_validation_available=("scada_validation_available", "max"),
            scada_validation_source=("scada_validation_source", _join_unique),
        )
    )


def _aggregate_dwell_month(dwell: pd.DataFrame | None) -> pd.DataFrame:
    if dwell is None or dwell.empty:
        return pd.DataFrame(columns=["farm_id", "month"])
    _require_columns(dwell, {"farm_id", "start_utc"}, "dwell features")
    frame = dwell.copy()
    frame["month"] = _event_month(frame["start_utc"])
    frame["has_mmsi"] = frame["mmsi"].notna() if "mmsi" in frame.columns else False
    metadata_cols = [column for column in VESSEL_METADATA_COLUMNS if column in frame.columns]
    if metadata_cols:
        frame["has_vessel_metadata"] = frame[metadata_cols].notna().any(axis=1)
        frame["has_access_technology"] = (
            frame["access_technology"].notna() if "access_technology" in frame.columns else False
        )
    else:
        frame["has_vessel_metadata"] = False
        frame["has_access_technology"] = False
    frame["candidate_intervention"] = (
        frame["dwell_tier"].isin(CANDIDATE_TIERS) if "dwell_tier" in frame.columns else False
    )

    grouped = frame.groupby(["farm_id", "month"], dropna=False)
    summary = grouped.agg(
        dwell_rows=("farm_id", "size"),
        unique_mmsi_count=("mmsi", "nunique") if "mmsi" in frame.columns else ("farm_id", "size"),
        vessel_mmsi_available=("has_mmsi", "max"),
        vessel_metadata_available=("has_vessel_metadata", "max"),
        vessel_access_technology_available=("has_access_technology", "max"),
        candidate_intervention_event_count=("candidate_intervention", "sum"),
    ).reset_index()

    if "mmsi" in frame.columns:
        counts = (
            frame.dropna(subset=["mmsi"])
            .groupby(["farm_id", "month", "mmsi"], dropna=False)
            .size()
            .rename("mmsi_event_count")
            .reset_index()
        )
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


def _aggregate_assignment_month(events: pd.DataFrame | None) -> tuple[pd.DataFrame, pd.DataFrame]:
    if events is None or events.empty:
        empty_farm = pd.DataFrame(columns=["farm_id", "month"])
        empty_turbine = pd.DataFrame(columns=["farm_id", "turbine_id", "month"])
        return empty_farm, empty_turbine
    _require_columns(events, {"farm_id", "start_utc"}, "turbine intervention events")
    frame = events.copy()
    frame["month"] = _event_month(frame["start_utc"])
    frame["is_high_assignment"] = (
        frame["assignment_confidence"].astype("string").str.lower().eq("high")
        if "assignment_confidence" in frame.columns
        else False
    )
    frame["supports_turbine_level"] = (
        frame["assignment_supports_turbine_level"].fillna(False).astype(bool)
        if "assignment_supports_turbine_level" in frame.columns
        else False
    )
    frame["candidate_tier_a"] = frame["dwell_tier"].eq("Tier A") if "dwell_tier" in frame.columns else False
    frame["candidate_tier_b"] = frame["dwell_tier"].eq("Tier B") if "dwell_tier" in frame.columns else False
    frame["candidate_tier_c"] = frame["dwell_tier"].eq("Tier C") if "dwell_tier" in frame.columns else False
    frame["candidate_tier_d"] = frame["dwell_tier"].eq("Tier D") if "dwell_tier" in frame.columns else False

    farm = frame.groupby(["farm_id", "month"], dropna=False).agg(
        turbine_assignment_event_count=("farm_id", "size"),
        high_assignment_event_count=("is_high_assignment", "sum"),
        turbine_supported_event_count=("supports_turbine_level", "sum"),
    ).reset_index()
    farm["high_confidence_assignment_share"] = _safe_share(
        farm["high_assignment_event_count"], farm["turbine_assignment_event_count"]
    )

    turbine_id_col = "assigned_turbine_id" if "assigned_turbine_id" in frame.columns else None
    if turbine_id_col is None:
        turbine = pd.DataFrame(columns=["farm_id", "turbine_id", "month"])
    else:
        assigned = frame.dropna(subset=[turbine_id_col]).copy()
        assigned = assigned.rename(columns={turbine_id_col: "turbine_id"})
        turbine = assigned.groupby(["farm_id", "turbine_id", "month"], dropna=False).agg(
            ais_dwell_event_count=("farm_id", "size"),
            tier_a_count=("candidate_tier_a", "sum"),
            tier_b_count=("candidate_tier_b", "sum"),
            tier_c_count=("candidate_tier_c", "sum"),
            tier_d_count=("candidate_tier_d", "sum"),
            high_assignment_event_count=("is_high_assignment", "sum"),
            turbine_supported_event_count=("supports_turbine_level", "sum"),
            unique_mmsi_count=("mmsi", "nunique") if "mmsi" in assigned.columns else ("farm_id", "size"),
        ).reset_index()
        turbine["high_confidence_assignment_share"] = _safe_share(
            turbine["high_assignment_event_count"], turbine["ais_dwell_event_count"]
        )
        if "mmsi" in assigned.columns:
            counts = (
                assigned.dropna(subset=["mmsi"])
                .groupby(["farm_id", "turbine_id", "month", "mmsi"], dropna=False)
                .size()
                .rename("mmsi_event_count")
                .reset_index()
            )
            top = counts.groupby(["farm_id", "turbine_id", "month"], as_index=False)[
                "mmsi_event_count"
            ].max()
            total = counts.groupby(["farm_id", "turbine_id", "month"], as_index=False)[
                "mmsi_event_count"
            ].sum()
            top = top.merge(total, on=["farm_id", "turbine_id", "month"], suffixes=("_top", "_total"))
            top["top_mmsi_concentration"] = _safe_share(
                top["mmsi_event_count_top"], top["mmsi_event_count_total"]
            )
            turbine = turbine.merge(
                top[["farm_id", "turbine_id", "month", "top_mmsi_concentration"]],
                on=["farm_id", "turbine_id", "month"],
                how="left",
            )
        else:
            turbine["top_mmsi_concentration"] = pd.NA
    return farm, turbine


def _aggregate_fusion_month(fusion: pd.DataFrame | None) -> pd.DataFrame:
    if fusion is None or fusion.empty:
        return pd.DataFrame(columns=["farm_id", "month"])
    _require_columns(fusion, {"farm_id", "start_utc"}, "Fusion v2")
    frame = fusion.copy()
    frame["month"] = _event_month(frame["start_utc"])
    bool_columns = [
        "has_wave",
        "has_wind_speed",
        "has_wind_direction",
        "has_current",
        "has_bathymetry",
        "model_ready_wave_only",
        "model_ready_wave_wind",
        "model_ready_wave_current",
        "model_ready_wave_wind_current",
        "model_ready_high_confidence",
    ]
    for column in bool_columns:
        if column not in frame.columns:
            frame[column] = False
        frame[column] = frame[column].fillna(False).astype(bool)
    grouped = frame.groupby(["farm_id", "month"], dropna=False)
    summary = grouped.agg(
        fusion_event_count=("farm_id", "size"),
        metocean_wave_event_count=("has_wave", "sum"),
        metocean_wind_speed_event_count=("has_wind_speed", "sum"),
        metocean_wind_direction_event_count=("has_wind_direction", "sum"),
        metocean_current_event_count=("has_current", "sum"),
        metocean_bathymetry_event_count=("has_bathymetry", "sum"),
        model_ready_wave_only_count=("model_ready_wave_only", "sum"),
        model_ready_wave_wind_count=("model_ready_wave_wind", "sum"),
        model_ready_wave_current_count=("model_ready_wave_current", "sum"),
        model_ready_wave_wind_current_count=("model_ready_wave_wind_current", "sum"),
        model_ready_high_confidence_count=("model_ready_high_confidence", "sum"),
    ).reset_index()
    for source, target in [
        ("metocean_wave_event_count", "wave_event_share"),
        ("metocean_wind_speed_event_count", "wind_speed_event_share"),
        ("metocean_wind_direction_event_count", "wind_direction_event_share"),
        ("metocean_current_event_count", "current_event_share"),
        ("metocean_bathymetry_event_count", "bathymetry_event_share"),
    ]:
        summary[target] = _safe_share(summary[source], summary["fusion_event_count"])
    return summary


def classify_month_confidence(row: pd.Series) -> str:
    """Assign a conservative evidence-readiness confidence class to a month row."""
    if bool(row.get("skipped_missing_source_flag", False)):
        return "D_missing_source"
    if not bool(row.get("observed_source_month_flag", False)):
        return "D_unobserved"

    event_count = row.get("ais_dwell_event_count", 0)
    try:
        event_count = 0 if pd.isna(event_count) else float(event_count)
    except TypeError:
        event_count = 0

    wave = bool(row.get("metocean_wave_available", False))
    wind = bool(row.get("metocean_wind_speed_available", False))
    current = bool(row.get("metocean_current_available", False))
    bathymetry = bool(row.get("metocean_bathymetry_available", False))
    scada = bool(row.get("scada_validation_available", False))
    high_assignment = row.get("high_confidence_assignment_share", np.nan)
    try:
        high_assignment_ready = pd.notna(high_assignment) and float(high_assignment) >= 0.5
    except (TypeError, ValueError):
        high_assignment_ready = False

    if event_count <= 0:
        return "C_observed_zero"
    if scada and wave and wind and bathymetry:
        return "A_local_validated"
    if wave and bathymetry and (wind or current) and high_assignment_ready:
        return "B_integrated_high_assignment"
    if wave and bathymetry and (wind or current):
        return "B_integrated_proxy"
    return "C_partial_proxy"


def build_farm_month_evidence_matrix(
    manifest: pd.DataFrame,
    farm_metadata: pd.DataFrame,
    dwell: pd.DataFrame | None = None,
    fusion_v2: pd.DataFrame | None = None,
    turbine_events: pd.DataFrame | None = None,
    bathymetry: pd.DataFrame | None = None,
    wave_archive_years: pd.DataFrame | None = None,
    current_archive_years: pd.DataFrame | None = None,
    scada_months: pd.DataFrame | None = None,
    direct_ais_receiver_metadata_available: bool = False,
) -> pd.DataFrame:
    """Build the farm-month evidence matrix from existing evidence products."""
    _require_columns(manifest, {"farm_id", "year", "month", "status"}, "AIS manifest")
    matrix = manifest.copy()
    matrix["month"] = _month_label_from_parts(matrix["year"], matrix["month"])
    matrix = matrix.rename(columns={"status": "ais_manifest_status"})
    matrix["observed_source_month_flag"] = matrix["ais_manifest_status"].isin(OBSERVED_STATUSES)
    matrix["skipped_missing_source_flag"] = matrix["ais_manifest_status"].eq(MISSING_SOURCE_STATUS)

    count_columns = {
        "dwell_count": "ais_dwell_event_count",
        "tier_a_count": "tier_a_count",
        "tier_b_count": "tier_b_count",
        "tier_c_count": "tier_c_count",
        "tier_d_count": "tier_d_count",
    }
    for source, target in count_columns.items():
        if source in matrix.columns:
            matrix[target] = pd.to_numeric(matrix[source], errors="coerce")
        else:
            matrix[target] = np.nan
        matrix.loc[matrix["skipped_missing_source_flag"], target] = pd.NA

    matrix["zero_event_despite_coverage"] = (
        matrix["observed_source_month_flag"]
        & pd.to_numeric(matrix["ais_dwell_event_count"], errors="coerce").fillna(0).eq(0)
    )
    matrix["source_file_metadata_available"] = (
        matrix.get("source_file_name", pd.Series(pd.NA, index=matrix.index)).notna()
    )
    matrix["ais_receiver_metadata_available"] = direct_ais_receiver_metadata_available

    meta_cols = [
        column
        for column in [
            "farm_id",
            "wind_farm",
            "country",
            "sea_basin",
            "commissioning_date",
            "steady_operational_start_month",
            "turbine_count",
        ]
        if column in farm_metadata.columns
    ]
    matrix = matrix.merge(farm_metadata[meta_cols].drop_duplicates("farm_id"), on="farm_id", how="left")
    if "wind_farm" not in matrix.columns:
        matrix["wind_farm"] = matrix["farm_id"]
    matrix["operational_phase"] = _operational_phase(
        matrix["month"],
        matrix.get("commissioning_date", pd.Series(pd.NA, index=matrix.index)),
        matrix.get("steady_operational_start_month", pd.Series(pd.NA, index=matrix.index)),
    )

    dwell_month = _aggregate_dwell_month(dwell)
    matrix = matrix.merge(dwell_month, on=["farm_id", "month"], how="left")

    assignment_farm, _ = _aggregate_assignment_month(turbine_events)
    matrix = matrix.merge(assignment_farm, on=["farm_id", "month"], how="left")

    fusion_month = _aggregate_fusion_month(fusion_v2)
    matrix = matrix.merge(fusion_month, on=["farm_id", "month"], how="left")

    if bathymetry is not None and not bathymetry.empty and "wind_farm" in bathymetry.columns:
        bath_farms = pd.DataFrame({"farm_id": bathymetry["wind_farm"].dropna().unique()})
        bath_farms["bathymetry_static_available"] = True
        matrix = matrix.merge(bath_farms, on="farm_id", how="left")
    else:
        matrix["bathymetry_static_available"] = False

    if wave_archive_years is not None and not wave_archive_years.empty:
        matrix = matrix.merge(wave_archive_years, on=["farm_id", "year"], how="left")
    if current_archive_years is not None and not current_archive_years.empty:
        matrix = matrix.merge(current_archive_years, on=["farm_id", "year"], how="left")
    if scada_months is not None and not scada_months.empty:
        matrix = matrix.merge(scada_months, on=["farm_id", "month"], how="left")

    fill_false_columns = [
        "bathymetry_static_available",
        "wave_archive_year_available",
        "current_archive_year_available",
        "scada_validation_available",
        "vessel_mmsi_available",
        "vessel_metadata_available",
        "vessel_access_technology_available",
    ]
    for column in fill_false_columns:
        if column not in matrix.columns:
            matrix[column] = False
        matrix[column] = matrix[column].where(matrix[column].notna(), False).astype(bool)

    for column in [
        "dwell_rows",
        "unique_mmsi_count",
        "candidate_intervention_event_count",
        "turbine_assignment_event_count",
        "high_assignment_event_count",
        "turbine_supported_event_count",
        "fusion_event_count",
        "metocean_wave_event_count",
        "metocean_wind_speed_event_count",
        "metocean_wind_direction_event_count",
        "metocean_current_event_count",
        "metocean_bathymetry_event_count",
        "model_ready_wave_only_count",
        "model_ready_wave_wind_count",
        "model_ready_wave_current_count",
        "model_ready_wave_wind_current_count",
        "model_ready_high_confidence_count",
    ]:
        if column not in matrix.columns:
            matrix[column] = 0
        matrix[column] = matrix[column].fillna(0)

    matrix["intervention_intensity_evidence_flag"] = (
        matrix["observed_source_month_flag"]
        & (
            pd.to_numeric(matrix["tier_a_count"], errors="coerce").fillna(0)
            + pd.to_numeric(matrix["tier_b_count"], errors="coerce").fillna(0)
            > 0
        )
    )
    matrix["metocean_wave_available"] = (
        matrix["wave_archive_year_available"]
        | (pd.to_numeric(matrix["metocean_wave_event_count"], errors="coerce").fillna(0) > 0)
    )
    matrix["metocean_wind_speed_available"] = (
        pd.to_numeric(matrix["metocean_wind_speed_event_count"], errors="coerce").fillna(0) > 0
    )
    matrix["metocean_wind_direction_available"] = (
        pd.to_numeric(matrix["metocean_wind_direction_event_count"], errors="coerce").fillna(0) > 0
    )
    matrix["metocean_current_available"] = (
        matrix["current_archive_year_available"]
        | (pd.to_numeric(matrix["metocean_current_event_count"], errors="coerce").fillna(0) > 0)
    )
    matrix["metocean_bathymetry_available"] = (
        matrix["bathymetry_static_available"]
        | (pd.to_numeric(matrix["metocean_bathymetry_event_count"], errors="coerce").fillna(0) > 0)
    )
    matrix["event_density"] = pd.to_numeric(matrix["ais_dwell_event_count"], errors="coerce")
    matrix["event_density_per_turbine"] = _safe_share(
        matrix["event_density"],
        pd.to_numeric(matrix.get("turbine_count", pd.Series(np.nan, index=matrix.index)), errors="coerce"),
    )
    matrix["confidence_class"] = matrix.apply(classify_month_confidence, axis=1)

    preferred = [
        "farm_id",
        "wind_farm",
        "year",
        "month",
        "country",
        "sea_basin",
        "commissioning_date",
        "operational_phase",
        "ais_manifest_status",
        "observed_source_month_flag",
        "skipped_missing_source_flag",
        "ais_dwell_event_count",
        "tier_a_count",
        "tier_b_count",
        "tier_c_count",
        "tier_d_count",
        "intervention_intensity_evidence_flag",
        "metocean_wave_available",
        "metocean_wind_speed_available",
        "metocean_wind_direction_available",
        "metocean_current_available",
        "metocean_bathymetry_available",
        "scada_validation_available",
        "vessel_mmsi_available",
        "vessel_metadata_available",
        "vessel_access_technology_available",
        "zero_event_despite_coverage",
        "high_confidence_assignment_share",
        "top_mmsi_concentration",
        "event_density",
        "event_density_per_turbine",
        "ais_receiver_metadata_available",
        "source_file_metadata_available",
        "confidence_class",
    ]
    remaining = [column for column in matrix.columns if column not in preferred]
    return matrix[preferred + remaining]


def build_turbine_month_evidence_matrix(
    farm_month_matrix: pd.DataFrame,
    turbine_exposure: pd.DataFrame,
    turbine_intensity: pd.DataFrame | None = None,
    turbine_events: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Expand farm-month readiness to turbine-month rows with assignment evidence."""
    _require_columns(turbine_exposure, {"farm_id", "turbine_id"}, "turbine exposure")
    turbine_cols = [
        column
        for column in [
            "farm_id",
            "wind_farm",
            "turbine_id",
            "country",
            "sea_basin",
            "latitude",
            "longitude",
            "commissioning_date",
            "commissioning_month",
            "steady_operational_start_month",
            "coverage_class",
            "observed_steady_months",
            "observed_steady_years",
            "steady_coverage_share",
            "water_depth_m",
            "bathymetry_join_status",
        ]
        if column in turbine_exposure.columns
    ]
    turbines = turbine_exposure[turbine_cols].drop_duplicates(["farm_id", "turbine_id"]).copy()
    if "commissioning_month" in turbines.columns and "commissioning_date" not in turbines.columns:
        turbines = turbines.rename(columns={"commissioning_month": "commissioning_date"})
    if "commissioning_date" not in turbines.columns and "commissioning_month" in turbines.columns:
        turbines["commissioning_date"] = turbines["commissioning_month"]

    inherited_cols = [
        column
        for column in [
            "farm_id",
            "year",
            "month",
            "ais_manifest_status",
            "observed_source_month_flag",
            "skipped_missing_source_flag",
            "metocean_wave_available",
            "metocean_wind_speed_available",
            "metocean_wind_direction_available",
            "metocean_current_available",
            "metocean_bathymetry_available",
            "scada_validation_available",
            "vessel_mmsi_available",
            "vessel_metadata_available",
            "vessel_access_technology_available",
            "ais_receiver_metadata_available",
            "source_file_metadata_available",
            "zero_event_despite_coverage",
        ]
        if column in farm_month_matrix.columns
    ]
    matrix = farm_month_matrix[inherited_cols].merge(turbines, on="farm_id", how="inner")

    _, turbine_assignment = _aggregate_assignment_month(turbine_events)
    matrix = matrix.merge(
        turbine_assignment,
        on=["farm_id", "turbine_id", "month"],
        how="left",
    )

    if turbine_intensity is not None and not turbine_intensity.empty:
        keep = [
            column
            for column in [
                "farm_id",
                "turbine_id",
                "primary_intervention_intensity_per_steady_turbine_year",
                "sensitivity_intervention_intensity_per_steady_turbine_year",
                "turbine_intervention_confidence_class",
            ]
            if column in turbine_intensity.columns
        ]
        if keep:
            matrix = matrix.merge(
                turbine_intensity[keep].drop_duplicates(["farm_id", "turbine_id"]),
                on=["farm_id", "turbine_id"],
                how="left",
            )

    for column in [
        "ais_dwell_event_count",
        "tier_a_count",
        "tier_b_count",
        "tier_c_count",
        "tier_d_count",
        "high_assignment_event_count",
        "turbine_supported_event_count",
        "unique_mmsi_count",
    ]:
        if column not in matrix.columns:
            matrix[column] = 0
        matrix[column] = matrix[column].fillna(0)
        matrix.loc[matrix["skipped_missing_source_flag"], column] = pd.NA

    matrix["intervention_intensity_evidence_flag"] = (
        matrix["observed_source_month_flag"]
        & pd.to_numeric(matrix["ais_dwell_event_count"], errors="coerce").fillna(0).gt(0)
    )
    matrix["event_density"] = pd.to_numeric(matrix["ais_dwell_event_count"], errors="coerce")
    matrix["operational_phase"] = _operational_phase(
        matrix["month"],
        matrix.get("commissioning_date", pd.Series(pd.NA, index=matrix.index)),
        matrix.get("steady_operational_start_month", pd.Series(pd.NA, index=matrix.index)),
    )
    matrix["confidence_class"] = matrix.apply(classify_month_confidence, axis=1)

    preferred = [
        "farm_id",
        "wind_farm",
        "turbine_id",
        "year",
        "month",
        "country",
        "sea_basin",
        "commissioning_date",
        "operational_phase",
        "ais_manifest_status",
        "observed_source_month_flag",
        "skipped_missing_source_flag",
        "ais_dwell_event_count",
        "tier_a_count",
        "tier_b_count",
        "tier_c_count",
        "tier_d_count",
        "intervention_intensity_evidence_flag",
        "metocean_wave_available",
        "metocean_wind_speed_available",
        "metocean_wind_direction_available",
        "metocean_current_available",
        "metocean_bathymetry_available",
        "scada_validation_available",
        "vessel_mmsi_available",
        "vessel_metadata_available",
        "vessel_access_technology_available",
        "zero_event_despite_coverage",
        "high_confidence_assignment_share",
        "top_mmsi_concentration",
        "event_density",
        "ais_receiver_metadata_available",
        "source_file_metadata_available",
        "confidence_class",
    ]
    remaining = [column for column in matrix.columns if column not in preferred]
    return matrix[preferred + remaining]


def classify_rq_answerability(
    required_layers: list[str],
    available_layers: list[str],
    blocking_layers: list[str] | None = None,
) -> str:
    """Classify an RQ as ready, partial, or blocked from layer availability."""
    blocking = set(blocking_layers or [])
    available = set(available_layers)
    missing = [layer for layer in required_layers if layer not in available]
    if any(layer in blocking for layer in missing):
        return "blocked"
    if missing:
        return "partial"
    return "ready"


def build_rq_readiness_matrix(layer_status: dict[str, bool]) -> pd.DataFrame:
    """Build the RQ readiness matrix from global evidence-layer status."""
    available_layers: list[str] = []
    labels = {
        "ais_manifest": "AIS source manifest",
        "ais_dwell": "AIS dwell/event evidence",
        "metocean_fusion_v2": "Fusion v2 metocean event features",
        "wave": "wave evidence",
        "wind_speed": "wind speed evidence",
        "wind_direction": "wind direction evidence",
        "current": "current evidence",
        "bathymetry": "bathymetry",
        "turbine_metadata": "turbine metadata",
        "vessel_metadata": "vessel registry/access metadata",
        "scada_validation": "SCADA/event validation",
        "direct_ais_receiver_metadata": "direct AIS receiver/source geometry",
        "external_market": "external market/curtailment data",
        "external_oil_gas": "external oil and gas benchmark data",
        "trajectory_tracks": "full trajectory tracks",
        "fault_work_orders": "fault/work-order logs",
        "wake_model": "wake model inputs",
        "solar_light": "solar/light features",
    }
    for key, label in labels.items():
        if layer_status.get(key, False):
            available_layers.append(label)

    rq_specs = [
        {
            "rq_number": "RQ1",
            "required": ["AIS dwell/event evidence", "wave evidence", "SCADA/event validation"],
            "blocking": [],
            "broad_validation_required": True,
            "geo": "medium",
            "obs": "medium",
            "action": "Use observed envelope only; add failed/non-operation denominator before access probability.",
        },
        {
            "rq_number": "RQ2",
            "required": ["AIS dwell/event evidence", "wave evidence", "vessel registry/access metadata"],
            "blocking": ["vessel registry/access metadata"],
            "geo": "medium",
            "obs": "high",
            "action": "Prioritize vessel registry enrichment for length, beam, draft, and access technology.",
        },
        {
            "rq_number": "RQ3",
            "required": ["AIS dwell/event evidence", "vessel registry/access metadata"],
            "blocking": ["vessel registry/access metadata"],
            "geo": "medium",
            "obs": "high",
            "action": "Add authoritative vessel build year and registry provenance before trend claims.",
        },
        {
            "rq_number": "RQ4",
            "required": ["AIS dwell/event evidence", "SCADA/event validation", "Fusion v2 metocean event features"],
            "blocking": [],
            "broad_validation_required": True,
            "geo": "high",
            "obs": "high",
            "action": "Confine to local CARE validation slices until failed/short-attempt labels are broader.",
        },
        {
            "rq_number": "RQ5",
            "required": ["AIS dwell/event evidence", "full trajectory tracks", "vessel registry/access metadata"],
            "blocking": ["full trajectory tracks", "vessel registry/access metadata"],
            "geo": "medium",
            "obs": "high",
            "action": "Recover voyage/port-gap trajectories and registry labels before campaign claims.",
        },
        {
            "rq_number": "RQ6",
            "required": ["Fusion v2 metocean event features", "wave evidence", "wind speed evidence", "current evidence", "bathymetry"],
            "blocking": [],
            "geo": "medium",
            "obs": "medium",
            "action": "Proceed with source-aware sensitivity; keep wind direction and current coverage limitations explicit.",
        },
        {
            "rq_number": "RQ7",
            "required": ["SCADA/event validation", "external market/curtailment data"],
            "blocking": ["external market/curtailment data"],
            "geo": "high",
            "obs": "high",
            "action": "Add curtailment/market source and restrict current work to validation design.",
        },
        {
            "rq_number": "RQ8",
            "required": ["SCADA/event validation", "wake model inputs", "turbine metadata"],
            "blocking": ["wake model inputs"],
            "geo": "medium",
            "obs": "high",
            "action": "Define wake/value inputs and broaden SCADA linkage beyond local CARE slices.",
        },
        {
            "rq_number": "RQ9",
            "required": ["AIS dwell/event evidence", "turbine metadata", "direct AIS receiver/source geometry", "fault/work-order logs"],
            "blocking": ["fault/work-order logs"],
            "geo": "high",
            "obs": "high",
            "action": "Keep as intervention-intensity evidence; add receiver/source controls and fault/work-order validation.",
        },
        {
            "rq_number": "RQ10",
            "required": ["AIS dwell/event evidence", "external oil and gas benchmark data"],
            "blocking": ["external oil and gas benchmark data"],
            "geo": "high",
            "obs": "medium",
            "action": "Identify accepted O&G benchmark source before comparison.",
        },
        {
            "rq_number": "RQ11",
            "required": ["AIS dwell/event evidence", "solar/light features", "vessel registry/access metadata"],
            "blocking": [],
            "geo": "medium",
            "obs": "medium",
            "action": "Add deterministic light features and registry enrichment before safety-practice interpretation.",
        },
        {
            "rq_number": "RQ12",
            "required": ["AIS dwell/event evidence", "Fusion v2 metocean event features", "vessel registry/access metadata", "SCADA/event validation"],
            "blocking": [],
            "geo": "medium",
            "obs": "high",
            "action": "Run simulator ablations only after vessel metadata and validation labels are explicit.",
        },
    ]

    rows: list[dict[str, Any]] = []
    for spec in rq_specs:
        required = spec["required"]
        present = [layer for layer in required if layer in available_layers]
        missing = [layer for layer in required if layer not in available_layers]
        answerability = classify_rq_answerability(required, present, spec.get("blocking", []))
        if (
            answerability == "ready"
            and spec.get("broad_validation_required", False)
            and not layer_status.get("broad_scada_validation", False)
        ):
            answerability = "partial"
            missing = [*missing, "broad SCADA/non-operation denominator"]
        rows.append(
            {
                "rq_number": spec["rq_number"],
                "required_data_layers": "; ".join(required),
                "available_layers": "; ".join(present),
                "missing_layers": "; ".join(missing),
                "geographic_bias_risk": spec["geo"],
                "observability_bias_risk": spec["obs"],
                "answerability": answerability,
                "recommended_next_action": spec["action"],
            }
        )
    return pd.DataFrame(rows, columns=RQ_READINESS_COLUMNS)


def build_geographic_coverage_summary(farm_month: pd.DataFrame) -> pd.DataFrame:
    """Summarize readiness and missingness by geography."""
    group_cols = ["country", "sea_basin"]
    frame = farm_month.copy()
    for column in ["country", "sea_basin"]:
        if column not in frame.columns:
            frame[column] = "unknown"
        frame[column] = frame[column].fillna("unknown")
    numeric_cols = [
        "ais_dwell_event_count",
        "tier_a_count",
        "tier_b_count",
        "tier_c_count",
        "tier_d_count",
    ]
    for column in numeric_cols:
        frame[column] = pd.to_numeric(frame.get(column, 0), errors="coerce").fillna(0)
    summary = frame.groupby(group_cols, dropna=False).agg(
        farm_count=("farm_id", "nunique"),
        farm_month_count=("farm_id", "size"),
        observed_source_months=("observed_source_month_flag", "sum"),
        skipped_missing_source_months=("skipped_missing_source_flag", "sum"),
        observed_zero_months=("zero_event_despite_coverage", "sum"),
        ais_dwell_event_count=("ais_dwell_event_count", "sum"),
        tier_a_count=("tier_a_count", "sum"),
        tier_b_count=("tier_b_count", "sum"),
        tier_c_count=("tier_c_count", "sum"),
        tier_d_count=("tier_d_count", "sum"),
        wave_available_months=("metocean_wave_available", "sum"),
        wind_speed_available_months=("metocean_wind_speed_available", "sum"),
        wind_direction_available_months=("metocean_wind_direction_available", "sum"),
        current_available_months=("metocean_current_available", "sum"),
        bathymetry_available_months=("metocean_bathymetry_available", "sum"),
        scada_validation_available_months=("scada_validation_available", "sum"),
        vessel_metadata_available_months=("vessel_metadata_available", "sum"),
        median_high_confidence_assignment_share=("high_confidence_assignment_share", "median"),
        median_top_mmsi_concentration=("top_mmsi_concentration", "median"),
    ).reset_index()
    summary["coverage_share"] = _safe_share(
        summary["observed_source_months"], summary["farm_month_count"]
    )
    summary["missing_source_share"] = _safe_share(
        summary["skipped_missing_source_months"], summary["farm_month_count"]
    )
    summary["observed_zero_share"] = _safe_share(
        summary["observed_zero_months"], summary["observed_source_months"]
    )
    return summary.sort_values(["sea_basin", "country"]).reset_index(drop=True)


def build_report_text(
    farm_month: pd.DataFrame,
    turbine_month: pd.DataFrame,
    geographic_summary: pd.DataFrame,
    rq_readiness: pd.DataFrame,
    validation: dict[str, Any],
) -> str:
    """Render the data limitations and observability report."""
    answerability_counts = rq_readiness["answerability"].value_counts().to_dict()
    confidence_counts = farm_month["confidence_class"].value_counts().to_dict()
    turbine_confidence_counts = turbine_month["confidence_class"].value_counts().to_dict()
    geography_rows = geographic_summary.sort_values("missing_source_share", ascending=False).head(8)
    geo_table = geography_rows.to_markdown(index=False)
    rq_table = rq_readiness[["rq_number", "answerability", "missing_layers", "recommended_next_action"]].to_markdown(index=False)

    lines = [
        "# Data Limitations And Observability Report",
        "",
        "## Scope",
        "",
        "This first evidence-readiness audit integrates existing outputs only. It did not rerun AIS extraction, rerun metocean extraction, modify raw/interim/source data, delete data, or start new RQ modelling.",
        "",
        "AIS visits are treated as observation and candidate intervention evidence. They are not confirmed failures; confirmation requires SCADA, fault-log, work-order, or equivalent validation.",
        "",
        "## Inputs Inspected",
        "",
        "- AIS dwell/weather feature table and AIS backfill manifest.",
        "- Raw AIS European Master CSV schemas for vessel/source columns.",
        "- European turbine coordinates and RQ9 farm/turbine intervention-intensity outputs.",
        "- Fusion v2, Wind Confidence v1, Current Confidence v1, wave archive partitions, and EMODnet bathymetry outputs.",
        "- CARE Wind Farm B/C SCADA event files, Wind Farm C feature matrix, and event aggregates where present.",
        "",
        "## Coverage Findings",
        "",
        f"- Farm-month matrix rows: {validation['farm_month_rows']:,}.",
        f"- Turbine-month matrix rows: {validation['turbine_month_rows']:,}.",
        f"- Observed AIS source farm-months: {validation['observed_source_months']:,}.",
        f"- Missing-source farm-months: {validation['skipped_missing_source_months']:,}. These are missing evidence, not zero-event months.",
        f"- Observed-zero farm-months: {validation['observed_zero_months']:,}. These have observed source coverage but no dwell/event evidence in the manifest.",
        f"- Farm-month confidence classes: {confidence_counts}.",
        f"- Turbine-month confidence classes: {turbine_confidence_counts}.",
        "",
        "## Geographic Missingness",
        "",
        "Source coverage is uneven by country and sea basin. The summary below is an observability audit, not a ranking of operational performance.",
        "",
        geo_table,
        "",
        "## Vessel Metadata And AIS Observability",
        "",
        f"- MMSI is present for dwell rows, but integrated vessel enrichment is available in {validation['vessel_metadata_available_months']:,} farm-months.",
        "- The current dwell/Fusion/RQ9 tables do not carry direct AIS receiver station, terrestrial/satellite channel, receiver coordinates, or receiver-distance fields.",
        "- Top-MMSI concentration and high-confidence turbine assignment share are retained as indirect observability proxies only.",
        "- Raw AIS schemas include vessel-name/type/dimension/source columns, but those registry fields are not yet populated in the integrated dwell evidence layer.",
        "",
        "## Metocean Completeness",
        "",
        f"- Wave availability months: {validation['wave_available_months']:,}.",
        f"- Wind speed availability months: {validation['wind_speed_available_months']:,}.",
        f"- Wind direction availability months: {validation['wind_direction_available_months']:,}. Wind direction remains sparse and sensitivity-only.",
        f"- Current availability months: {validation['current_available_months']:,}. Current coverage is source/domain dependent and must not be treated as zero current when missing.",
        f"- Bathymetry availability months: {validation['bathymetry_available_months']:,}. Static bathymetry is broad, but shallow/coastal warnings still require interpretation.",
        "",
        "## SCADA Validation Availability",
        "",
        f"- SCADA validation farm-months: {validation['scada_validation_available_months']:,}.",
        "- Validation is localized to CARE Wind Farm B/C mappings, not a Europe-wide denominator.",
        "- Wind Farm C has a processed feature matrix and event aggregates; Wind Farm B/C raw CARE event datasets are present.",
        "",
        "## RQ Readiness Summary",
        "",
        f"- Answerability counts: {answerability_counts}.",
        "",
        rq_table,
        "",
        "## Guardrails",
        "",
        "- Do not call AIS visits failures.",
        "- Do not interpret skipped source months as zero-event months.",
        "- Do not treat missing current or wind direction as zero physical values.",
        "- Do not convert sea-basin contrasts into reliability claims without receiver/source geometry and validation labels.",
    ]
    text = "\n".join(lines) + "\n"
    if contains_ais_only_failure_rate_claim(text):
        raise ValueError("Report contains a prohibited AIS-only failure-rate claim.")
    return text


def _validation_summary(
    farm_month: pd.DataFrame,
    turbine_month: pd.DataFrame,
    rq_readiness: pd.DataFrame,
) -> dict[str, Any]:
    return {
        "farm_month_rows": int(len(farm_month)),
        "turbine_month_rows": int(len(turbine_month)),
        "observed_source_months": int(farm_month["observed_source_month_flag"].sum()),
        "skipped_missing_source_months": int(farm_month["skipped_missing_source_flag"].sum()),
        "observed_zero_months": int(farm_month["zero_event_despite_coverage"].sum()),
        "wave_available_months": int(farm_month["metocean_wave_available"].sum()),
        "wind_speed_available_months": int(farm_month["metocean_wind_speed_available"].sum()),
        "wind_direction_available_months": int(farm_month["metocean_wind_direction_available"].sum()),
        "current_available_months": int(farm_month["metocean_current_available"].sum()),
        "bathymetry_available_months": int(farm_month["metocean_bathymetry_available"].sum()),
        "scada_validation_available_months": int(farm_month["scada_validation_available"].sum()),
        "vessel_metadata_available_months": int(farm_month["vessel_metadata_available"].sum()),
        "rq_answerability_counts": {
            str(key): int(value)
            for key, value in rq_readiness["answerability"].value_counts().items()
        },
    }


def build_evidence_readiness_outputs(
    *,
    project_root: Path,
    processed_output_dir: Path,
    report_output_dir: Path,
    methodology_path: Path,
    dwell_path: Path,
    manifest_path: Path,
    turbine_path: Path,
    farm_intensity_path: Path,
    turbine_intensity_path: Path,
    turbine_exposure_path: Path,
    turbine_events_path: Path,
    fusion_v2_path: Path,
    bathymetry_path: Path,
    current_manifest_path: Path,
    nws_wave_root: Path,
    baltic_wave_root: Path,
    care_root: Path,
) -> EvidenceReadinessOutputs:
    """Build all first-pass evidence-readiness outputs."""
    manifest = pd.read_csv(manifest_path)
    turbines = pd.read_csv(turbine_path) if turbine_path.exists() else None
    farm_intensity = pd.read_csv(farm_intensity_path) if farm_intensity_path.exists() else None
    turbine_intensity = pd.read_csv(turbine_intensity_path) if turbine_intensity_path.exists() else None
    turbine_exposure = pd.read_csv(turbine_exposure_path) if turbine_exposure_path.exists() else None
    turbine_events = pd.read_csv(turbine_events_path) if turbine_events_path.exists() else None
    dwell = pd.read_parquet(dwell_path) if dwell_path.exists() else None
    fusion_v2 = pd.read_parquet(fusion_v2_path) if fusion_v2_path.exists() else None
    bathymetry = pd.read_parquet(bathymetry_path) if bathymetry_path.exists() else None

    if turbine_exposure is None:
        raise FileNotFoundError(f"Required turbine exposure input missing: {turbine_exposure_path}")

    farm_metadata = build_farm_metadata(
        turbines=turbines,
        farm_intensity=farm_intensity,
        turbine_exposure=turbine_exposure,
        turbine_intensity=turbine_intensity,
    )
    alias_map = build_farm_alias_map(farm_metadata, dwell, fusion_v2, turbine_exposure, turbine_intensity)
    wave_nws = build_archive_year_index(
        nws_wave_root, alias_map, "wave_archive_year_available", "nws_wave_timeseries"
    )
    wave_baltic = build_archive_year_index(
        baltic_wave_root, alias_map, "wave_archive_year_available", "baltic_wave_timeseries"
    )
    wave_archive_years = pd.concat([wave_nws, wave_baltic], ignore_index=True, sort=False)
    if not wave_archive_years.empty:
        wave_archive_years = (
            wave_archive_years.groupby(["farm_id", "year"], as_index=False)
            .agg(
                wave_archive_year_available=("wave_archive_year_available", "max"),
                wave_archive_year_available_source=("wave_archive_year_available_source", _join_unique),
            )
        )
    current_archive_years = build_current_archive_year_index(current_manifest_path, alias_map)
    scada_months = build_scada_validation_months(care_root)
    direct_receiver_available = has_direct_ais_receiver_metadata(
        manifest, dwell, fusion_v2, turbine_events, turbine_exposure
    )

    farm_month = build_farm_month_evidence_matrix(
        manifest,
        farm_metadata,
        dwell=dwell,
        fusion_v2=fusion_v2,
        turbine_events=turbine_events,
        bathymetry=bathymetry,
        wave_archive_years=wave_archive_years,
        current_archive_years=current_archive_years,
        scada_months=scada_months,
        direct_ais_receiver_metadata_available=direct_receiver_available,
    )
    turbine_month = build_turbine_month_evidence_matrix(
        farm_month,
        turbine_exposure=turbine_exposure,
        turbine_intensity=turbine_intensity,
        turbine_events=turbine_events,
    )

    layer_status = {
        "ais_manifest": manifest_path.exists(),
        "ais_dwell": dwell is not None and not dwell.empty,
        "metocean_fusion_v2": fusion_v2 is not None and not fusion_v2.empty,
        "wave": bool(farm_month["metocean_wave_available"].any()),
        "wind_speed": bool(farm_month["metocean_wind_speed_available"].any()),
        "wind_direction": bool(farm_month["metocean_wind_direction_available"].sum() > 100),
        "current": bool(farm_month["metocean_current_available"].any()),
        "bathymetry": bool(farm_month["metocean_bathymetry_available"].any()),
        "turbine_metadata": turbine_exposure is not None and not turbine_exposure.empty,
        "vessel_metadata": bool(farm_month["vessel_metadata_available"].any()),
        "scada_validation": bool(farm_month["scada_validation_available"].any()),
        "direct_ais_receiver_metadata": direct_receiver_available,
        "external_market": False,
        "external_oil_gas": False,
        "trajectory_tracks": False,
        "fault_work_orders": False,
        "wake_model": False,
        "solar_light": False,
    }
    rq_readiness = build_rq_readiness_matrix(layer_status)
    geographic_summary = build_geographic_coverage_summary(farm_month)
    rq_answerability = rq_readiness[
        ["rq_number", "answerability", "missing_layers", "recommended_next_action"]
    ].copy()
    validation = _validation_summary(farm_month, turbine_month, rq_readiness)

    processed_output_dir.mkdir(parents=True, exist_ok=True)
    report_output_dir.mkdir(parents=True, exist_ok=True)
    methodology_path.parent.mkdir(parents=True, exist_ok=True)

    farm_matrix_path = processed_output_dir / "farm_month_evidence_matrix.csv"
    turbine_matrix_path = processed_output_dir / "turbine_month_evidence_matrix.csv"
    rq_matrix_path = processed_output_dir / "rq_readiness_matrix.csv"
    report_path = report_output_dir / "data_limitations_and_observability_report.md"
    geo_summary_path = report_output_dir / "geographic_coverage_summary.csv"
    rq_summary_path = report_output_dir / "rq_answerability_summary.csv"

    farm_month.to_csv(farm_matrix_path, index=False)
    turbine_month.to_csv(turbine_matrix_path, index=False)
    rq_readiness.to_csv(rq_matrix_path, index=False)
    geographic_summary.to_csv(geo_summary_path, index=False)
    rq_answerability.to_csv(rq_summary_path, index=False)
    report_path.write_text(
        build_report_text(farm_month, turbine_month, geographic_summary, rq_readiness, validation),
        encoding="utf-8",
    )

    return EvidenceReadinessOutputs(
        processed_output_dir=processed_output_dir,
        report_output_dir=report_output_dir,
        files={
            "farm_month_evidence_matrix_csv": farm_matrix_path,
            "turbine_month_evidence_matrix_csv": turbine_matrix_path,
            "rq_readiness_matrix_csv": rq_matrix_path,
            "data_limitations_report_md": report_path,
            "geographic_coverage_summary_csv": geo_summary_path,
            "rq_answerability_summary_csv": rq_summary_path,
            "methodology_md": methodology_path,
        },
        validation={key: _jsonable(value) for key, value in validation.items()},
    )
