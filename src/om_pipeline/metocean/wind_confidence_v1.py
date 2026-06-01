"""Wind Confidence v1: event-level NORA3 wind evidence.

This increment formalises the wind fields that already exist in the
dwell-weather table. It keeps wind speed and direction confidence separate,
because the accepted event table contains broad NORA3 10 m wind-speed coverage
but only sparse wind-direction coverage.

The implementation is intentionally read-only with respect to NORA3 caches. It
does not download wind data, repair direction files, rebuild dwell weather, or
create Fusion v2.
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow.parquet as pq


DEFAULT_DWELL_WEATHER = Path(
    "Data/Processed/ais_dwell_backfill/cross_farm_dwell_weather_features.parquet"
)
DEFAULT_NORA3_JOINED_CACHE = Path("Data/Processed/metocean/nora3_joined_cache")
DEFAULT_NORA3_RAW_CACHE = Path("Data/Raw/Metocean/NORA3")
DEFAULT_WAVE_CONFIDENCE = Path(
    "Data/Processed/metocean/fusion_v1_source_agreement/wave_event_confidence.parquet"
)
DEFAULT_CURRENT_CONFIDENCE = Path(
    "Data/Processed/metocean/current_confidence_v1/current_event_confidence.parquet"
)
DEFAULT_OUTPUT_DIR = Path("Data/Processed/metocean/wind_confidence_v1")
DEFAULT_REPORT_DIR = Path("reports/wind_confidence_v1")

WIND_EVENT_CANDIDATES_FILENAME = "wind_event_candidates.parquet"
WIND_EVENT_CONFIDENCE_FILENAME = "wind_event_confidence.parquet"
VALIDATION_REPORT_FILENAME = "wind_confidence_validation_report.md"

WIND_SOURCE = "NORA3"
WIND_PRODUCT = "NORA3 Atmospheric wind_hourly_v2 existing dwell-weather active fields"
WIND_HEIGHT_M = 10.0
WIND_DIRECTION_CONVENTION = "meteorological_from_degrees_clockwise_from_true_north"
TEMPORAL_ASSIGNMENT_METHOD = "existing_dwell_weather_active_window_aggregation"
SPATIAL_ASSIGNMENT_METHOD = "existing_nora3_nearest_coordinate_cache"
WIND_SPEED_P95_METHOD_NOTE = (
    "The existing dwell-weather table stores active wind mean and max, not "
    "per-event wind-speed p95. Wind v1 preserves the active max in the "
    "`wind_speed_p95` slot as an upper-window diagnostic until a targeted "
    "per-sample NORA3 wind repair/reaggregation is approved."
)

DWELL_WIND_COLUMNS = [
    "dwell_id",
    "visit_id",
    "wind_farm",
    "farm_id",
    "dwell_tier",
    "start_utc",
    "end_utc",
    "active_wind_speed_mean",
    "active_wind_speed_max",
    "active_wind_direction_sin_mean",
    "active_wind_direction_cos_mean",
    "active_n_weather_records",
    "active_weather_missing_fraction",
    "active_source_available",
]

WIND_EVENT_CANDIDATE_COLUMNS = [
    "dwell_id",
    "visit_id",
    "wind_farm",
    "farm_id",
    "dwell_tier",
    "start_utc",
    "end_utc",
    "wind_source",
    "wind_product",
    "wind_height_m",
    "wind_speed_mean",
    "wind_speed_p95",
    "wind_direction_sin_mean",
    "wind_direction_cos_mean",
    "wind_direction_deg_mean",
    "wind_direction_convention",
    "event_window_sample_count",
    "nearest_time_gap_minutes",
    "temporal_assignment_method",
    "spatial_assignment_method",
    "wind_missing_fraction",
    "wind_missing_reason",
    "provenance_status",
]

WIND_EVENT_CONFIDENCE_COLUMNS = [
    "dwell_id",
    "visit_id",
    "wind_farm",
    "farm_id",
    "dwell_tier",
    "has_wind_speed",
    "has_wind_direction",
    "wind_confidence_class",
    "wind_confidence_score",
    "wind_source",
    "wind_speed_mean",
    "wind_speed_p95",
    "wind_direction_sin_mean",
    "wind_direction_cos_mean",
    "wind_direction_deg_mean",
    "wind_height_m",
    "wind_missing_reason",
    "selection_reason",
    "wave_confidence_class",
    "current_confidence_class",
]

RAW_WIND_PATTERN = re.compile(
    r"^nora3_wind_raw_(?P<lat>-?\d+(?:\.\d+)?)_(?P<lon>-?\d+(?:\.\d+)?)_"
    r"(?P<year>\d{4})_(?P<month>\d{2})\.csv$"
)


@dataclass(frozen=True)
class WindConfidenceResult:
    candidate_path: Path
    confidence_path: Path
    report_path: Path
    candidates: pd.DataFrame
    confidence: pd.DataFrame
    validation: dict[str, Any]


def ensure_candidate_schema(frame: pd.DataFrame) -> pd.DataFrame:
    for column in WIND_EVENT_CANDIDATE_COLUMNS:
        if column not in frame.columns:
            frame[column] = pd.NA
    return frame[WIND_EVENT_CANDIDATE_COLUMNS].copy()


def ensure_confidence_schema(frame: pd.DataFrame) -> pd.DataFrame:
    for column in WIND_EVENT_CONFIDENCE_COLUMNS:
        if column not in frame.columns:
            frame[column] = pd.NA
    return frame[WIND_EVENT_CONFIDENCE_COLUMNS].copy()


def _read_parquet_columns(path: Path, columns: list[str]) -> pd.DataFrame:
    schema = pq.read_schema(path).names
    available = [column for column in columns if column in schema]
    frame = pd.read_parquet(path, columns=available)
    for column in columns:
        if column not in frame.columns:
            frame[column] = pd.NA
    return frame[columns].copy()


def load_dwell_wind_events(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Dwell-weather table not found: {path}")
    dwell = _read_parquet_columns(path, DWELL_WIND_COLUMNS)
    dwell["start_utc"] = pd.to_datetime(dwell["start_utc"], utc=True, errors="coerce")
    dwell["end_utc"] = pd.to_datetime(dwell["end_utc"], utc=True, errors="coerce")
    dwell["__row_id"] = np.arange(len(dwell), dtype=np.int64)
    return dwell


def load_wave_confidence(path: Path) -> pd.DataFrame:
    columns = ["dwell_id", "visit_id", "wave_confidence_class"]
    if not path.exists():
        return pd.DataFrame(columns=columns)
    return _read_parquet_columns(path, columns).drop_duplicates(["dwell_id", "visit_id"])


def load_current_confidence(path: Path) -> pd.DataFrame:
    columns = ["dwell_id", "visit_id", "current_confidence_class"]
    if not path.exists():
        return pd.DataFrame(columns=columns)
    return _read_parquet_columns(path, columns).drop_duplicates(["dwell_id", "visit_id"])


def _numeric(value: Any) -> float:
    if pd.isna(value):
        return math.nan
    try:
        return float(value)
    except (TypeError, ValueError):
        return math.nan


def _direction_from_sin_cos(sin_value: Any, cos_value: Any) -> tuple[float, bool, float]:
    sin_num = _numeric(sin_value)
    cos_num = _numeric(cos_value)
    if not np.isfinite(sin_num) or not np.isfinite(cos_num):
        return math.nan, False, math.nan
    norm = float(math.sqrt(sin_num**2 + cos_num**2))
    if norm <= 0.0:
        return math.nan, False, norm
    direction = float((math.degrees(math.atan2(sin_num, cos_num)) + 360.0) % 360.0)
    return direction, 0.5 <= norm <= 1.05, norm


def wind_missing_reason(row: pd.Series, has_speed: bool, has_direction: bool, direction_valid: bool) -> str | pd._libs.missing.NAType:
    if not has_speed:
        records = _numeric(row.get("active_n_weather_records"))
        source_available = bool(row.get("active_source_available")) if pd.notna(row.get("active_source_available")) else False
        if not source_available or not np.isfinite(records) or records <= 0:
            return "no_active_nora3_wind_records"
        return "wind_speed_missing_in_active_fields"
    if not has_direction:
        return "wind_direction_missing_in_existing_active_fields"
    if not direction_valid:
        return "wind_direction_sin_cos_invalid_norm"
    return pd.NA


def build_wind_event_candidates(dwell: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for _, event in dwell.iterrows():
        speed = _numeric(event.get("active_wind_speed_mean"))
        speed_p95_proxy = _numeric(event.get("active_wind_speed_max"))
        has_speed = np.isfinite(speed)
        direction_deg, direction_valid, _ = _direction_from_sin_cos(
            event.get("active_wind_direction_sin_mean"),
            event.get("active_wind_direction_cos_mean"),
        )
        direction_rad = math.radians(direction_deg) if direction_valid else math.nan
        direction_sin = float(math.sin(direction_rad)) if direction_valid else pd.NA
        direction_cos = float(math.cos(direction_rad)) if direction_valid else pd.NA
        has_direction_values = (
            pd.notna(event.get("active_wind_direction_sin_mean"))
            and pd.notna(event.get("active_wind_direction_cos_mean"))
        )
        reason = wind_missing_reason(event, has_speed, has_direction_values, direction_valid)
        assigned = has_speed or has_direction_values
        rows.append(
            {
                "dwell_id": event.get("dwell_id"),
                "visit_id": event.get("visit_id"),
                "wind_farm": event.get("wind_farm"),
                "farm_id": event.get("farm_id"),
                "dwell_tier": event.get("dwell_tier"),
                "start_utc": event.get("start_utc"),
                "end_utc": event.get("end_utc"),
                "wind_source": WIND_SOURCE,
                "wind_product": WIND_PRODUCT,
                "wind_height_m": WIND_HEIGHT_M,
                "wind_speed_mean": speed if has_speed else pd.NA,
                "wind_speed_p95": speed_p95_proxy if np.isfinite(speed_p95_proxy) else pd.NA,
                "wind_direction_sin_mean": direction_sin,
                "wind_direction_cos_mean": direction_cos,
                "wind_direction_deg_mean": direction_deg if direction_valid else pd.NA,
                "wind_direction_convention": WIND_DIRECTION_CONVENTION,
                "event_window_sample_count": event.get("active_n_weather_records"),
                "nearest_time_gap_minutes": pd.NA,
                "temporal_assignment_method": TEMPORAL_ASSIGNMENT_METHOD if assigned else "not_assigned",
                "spatial_assignment_method": SPATIAL_ASSIGNMENT_METHOD if assigned else "not_assigned",
                "wind_missing_fraction": 0.0 if has_speed else 1.0,
                "wind_missing_reason": reason,
                "provenance_status": "existing_nora3_active_wind_fields" if assigned else "missing_nora3_active_wind_fields",
                "__row_id": event.get("__row_id"),
            }
        )
    candidates = pd.DataFrame(rows).sort_values("__row_id").reset_index(drop=True)
    return ensure_candidate_schema(candidates)


def classify_wind_confidence(candidate: pd.Series) -> tuple[str, float, bool, bool, str]:
    provenance = str(candidate.get("provenance_status", "")).casefold()
    if provenance.startswith(("fallback", "simulated", "legacy")):
        return "D_unsuitable", 0.0, False, False, "Fallback, simulated, or legacy wind provenance is banned."

    speed = _numeric(candidate.get("wind_speed_mean"))
    has_speed = np.isfinite(speed)
    if not has_speed:
        return (
            "D_unsuitable",
            0.0,
            False,
            False,
            f"No accepted event-level wind speed evidence: {candidate.get('wind_missing_reason')}.",
        )
    if speed < 0.0 or speed > 75.0:
        return "D_unsuitable", 0.0, False, False, "Impossible wind speed outside 0-75 m/s physical QA range."
    if str(candidate.get("wind_source", "")).upper() != WIND_SOURCE:
        return "C_low_confidence", 0.35, True, False, "Wind source is not the accepted NORA3 event aggregate."
    if "nora3" not in provenance:
        return "C_low_confidence", 0.35, True, False, "Wind provenance is weak or unspecified."

    direction_deg, direction_valid, _ = _direction_from_sin_cos(
        candidate.get("wind_direction_sin_mean"),
        candidate.get("wind_direction_cos_mean"),
    )
    has_direction = direction_valid and np.isfinite(direction_deg)
    if has_direction:
        return (
            "A_speed_direction",
            1.0,
            True,
            True,
            "NORA3 active-window wind speed and direction are present with explicit meteorological-from convention.",
        )
    return (
        "B_speed_only",
        0.75,
        True,
        False,
        "NORA3 active-window wind speed is present, but wind direction is missing or untrusted.",
    )


def build_wind_event_confidence(
    candidates: pd.DataFrame,
    wave_confidence: pd.DataFrame,
    current_confidence: pd.DataFrame,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for _, candidate in candidates.iterrows():
        cls, score, has_speed, has_direction, reason = classify_wind_confidence(candidate)
        rows.append(
            {
                "dwell_id": candidate["dwell_id"],
                "visit_id": candidate["visit_id"],
                "wind_farm": candidate["wind_farm"],
                "farm_id": candidate["farm_id"],
                "dwell_tier": candidate["dwell_tier"],
                "has_wind_speed": has_speed,
                "has_wind_direction": has_direction,
                "wind_confidence_class": cls,
                "wind_confidence_score": score,
                "wind_source": candidate["wind_source"],
                "wind_speed_mean": candidate["wind_speed_mean"],
                "wind_speed_p95": candidate["wind_speed_p95"],
                "wind_direction_sin_mean": candidate["wind_direction_sin_mean"],
                "wind_direction_cos_mean": candidate["wind_direction_cos_mean"],
                "wind_direction_deg_mean": candidate["wind_direction_deg_mean"],
                "wind_height_m": candidate["wind_height_m"],
                "wind_missing_reason": candidate["wind_missing_reason"],
                "selection_reason": reason,
            }
        )
    confidence = ensure_confidence_schema(pd.DataFrame(rows))
    if not wave_confidence.empty:
        confidence = confidence.drop(columns=["wave_confidence_class"]).merge(
            wave_confidence,
            on=["dwell_id", "visit_id"],
            how="left",
        )
    if not current_confidence.empty:
        confidence = confidence.drop(columns=["current_confidence_class"]).merge(
            current_confidence,
            on=["dwell_id", "visit_id"],
            how="left",
        )
    return ensure_confidence_schema(confidence)


def inventory_joined_cache(cache_root: Path) -> dict[str, Any]:
    inventory: dict[str, Any] = {
        "exists": cache_root.exists(),
        "manifest_rows": 0,
        "manifest_year_min": pd.NA,
        "manifest_year_max": pd.NA,
        "parquet_files": 0,
        "parquet_rows": 0,
        "schema_columns": [],
        "wind_speed_fields": [],
        "wind_direction_fields": [],
    }
    if not cache_root.exists():
        return inventory
    manifest_path = cache_root / "manifest.csv"
    if manifest_path.exists():
        manifest = pd.read_csv(manifest_path)
        inventory["manifest_rows"] = int(len(manifest))
        if "year" in manifest:
            years = pd.to_numeric(manifest["year"], errors="coerce").dropna()
            if not years.empty:
                inventory["manifest_year_min"] = int(years.min())
                inventory["manifest_year_max"] = int(years.max())
    schema_columns: set[str] = set()
    row_count = 0
    parquet_files = list(cache_root.glob("batch_id=*/data.parquet"))
    for path in parquet_files:
        parquet_file = pq.ParquetFile(path)
        row_count += int(parquet_file.metadata.num_rows)
        schema_columns.update(parquet_file.schema.names)
    inventory["parquet_files"] = int(len(parquet_files))
    inventory["parquet_rows"] = int(row_count)
    inventory["schema_columns"] = sorted(schema_columns)
    inventory["wind_speed_fields"] = sorted(
        column for column in schema_columns if "wind" in column.lower() and "speed" in column.lower()
    )
    inventory["wind_direction_fields"] = sorted(
        column for column in schema_columns if "wind" in column.lower() and "direction" in column.lower()
    )
    return inventory


def inventory_raw_wind_cache(raw_root: Path) -> dict[str, Any]:
    inventory: dict[str, Any] = {
        "exists": raw_root.exists(),
        "wind_raw_files": 0,
        "speed_only_files": 0,
        "speed_direction_files": 0,
        "direction_capable_year_min": pd.NA,
        "direction_capable_year_max": pd.NA,
        "year_min": pd.NA,
        "year_max": pd.NA,
        "schema_counts": {},
    }
    if not raw_root.exists():
        return inventory
    files = sorted(raw_root.glob("nora3_wind_raw_*.csv"))
    inventory["wind_raw_files"] = int(len(files))
    years: list[int] = []
    direction_years: list[int] = []
    schema_counts: dict[tuple[str, ...], int] = {}
    for path in files:
        match = RAW_WIND_PATTERN.match(path.name)
        year = int(match.group("year")) if match else None
        if year is not None:
            years.append(year)
        try:
            columns = tuple(pd.read_csv(path, nrows=0).columns)
        except Exception as exc:  # pragma: no cover - defensive inventory branch
            columns = (type(exc).__name__,)
        schema_counts[columns] = schema_counts.get(columns, 0) + 1
        has_speed = any("wind_speed" in column.lower() or "speed" in column.lower() for column in columns)
        has_direction = any("wind_direction" in column.lower() or "direction" in column.lower() for column in columns)
        if has_speed and has_direction:
            inventory["speed_direction_files"] += 1
            if year is not None:
                direction_years.append(year)
        elif has_speed:
            inventory["speed_only_files"] += 1
    if years:
        inventory["year_min"] = int(min(years))
        inventory["year_max"] = int(max(years))
    if direction_years:
        inventory["direction_capable_year_min"] = int(min(direction_years))
        inventory["direction_capable_year_max"] = int(max(direction_years))
    inventory["schema_counts"] = {" | ".join(key): value for key, value in schema_counts.items()}
    return inventory


def _is_tier_a(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str).str.casefold().str.contains("a")


def _quantiles(series: pd.Series) -> dict[str, float]:
    values = pd.to_numeric(series, errors="coerce").dropna()
    if values.empty:
        return {"min": math.nan, "p50": math.nan, "p95": math.nan, "max": math.nan}
    return {
        "min": float(values.min()),
        "p50": float(values.quantile(0.50)),
        "p95": float(values.quantile(0.95)),
        "max": float(values.max()),
    }


def _format_value(value: Any, digits: int = 3) -> str:
    if isinstance(value, (bool, np.bool_)):
        return str(bool(value))
    try:
        if pd.isna(value):
            return "NA"
        if isinstance(value, (int, np.integer)):
            return str(int(value))
        if isinstance(value, (float, np.floating)):
            return f"{float(value):.{digits}f}"
    except (TypeError, ValueError):
        pass
    return str(value).replace("|", "\\|")


def _markdown_table(frame: pd.DataFrame, columns: list[str], limit: int | None = None) -> list[str]:
    if frame.empty:
        return ["No rows."]
    subset = frame[columns].copy()
    if limit is not None:
        subset = subset.head(limit)
    lines = [
        "| " + " | ".join(columns) + " |",
        "| " + " | ".join(["---"] * len(columns)) + " |",
    ]
    for _, row in subset.iterrows():
        lines.append("| " + " | ".join(_format_value(row[column]) for column in columns) + " |")
    if limit is not None and len(frame) > limit:
        lines.append(f"| ... | {len(frame) - limit} more rows omitted |" + " |" * max(0, len(columns) - 2))
    return lines


def _count_table(frame: pd.DataFrame, group_cols: list[str], value_name: str = "event_count") -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=group_cols + [value_name])
    return (
        frame.groupby(group_cols, dropna=False)
        .size()
        .reset_index(name=value_name)
        .sort_values(value_name, ascending=False)
    )


def validate_wind_confidence(
    dwell: pd.DataFrame,
    candidates: pd.DataFrame,
    confidence: pd.DataFrame,
    wave_confidence: pd.DataFrame,
    current_confidence: pd.DataFrame,
    joined_inventory: dict[str, Any],
    raw_inventory: dict[str, Any],
) -> dict[str, Any]:
    has_speed = confidence["has_wind_speed"].fillna(False).astype(bool)
    has_direction = confidence["has_wind_direction"].fillna(False).astype(bool)
    tier_a = _is_tier_a(confidence["dwell_tier"])
    direction_values = pd.to_numeric(candidates.loc[has_direction, "wind_direction_deg_mean"], errors="coerce")
    direction_ok = bool(direction_values.dropna().between(0, 360, inclusive="left").all())
    sin_cos_norm = np.sqrt(
        pd.to_numeric(candidates.loc[has_direction, "wind_direction_sin_mean"], errors="coerce") ** 2
        + pd.to_numeric(candidates.loc[has_direction, "wind_direction_cos_mean"], errors="coerce") ** 2
    )
    sin_cos_max_error = float(np.nanmax(np.abs(sin_cos_norm - 1.0))) if len(sin_cos_norm) else math.nan
    speed_values = pd.to_numeric(candidates["wind_speed_mean"], errors="coerce")
    impossible_speed_count = int(((speed_values < 0.0) | (speed_values > 75.0)).sum())
    wave_crosstab = pd.crosstab(
        confidence["wind_confidence_class"].fillna("missing"),
        confidence["wave_confidence_class"].fillna("missing"),
    )
    current_crosstab = pd.crosstab(
        confidence["wind_confidence_class"].fillna("missing"),
        confidence["current_confidence_class"].fillna("missing"),
    )
    validation = {
        "input_dwell_rows": int(len(dwell)),
        "candidate_rows": int(len(candidates)),
        "confidence_rows": int(len(confidence)),
        "fusion_v1_wave_rows": int(len(wave_confidence)),
        "current_confidence_rows": int(len(current_confidence)),
        "wind_speed_events": int(has_speed.sum()),
        "wind_direction_events": int(has_direction.sum()),
        "tier_a_events": int(tier_a.sum()),
        "tier_a_wind_speed_events": int((tier_a & has_speed).sum()),
        "tier_a_wind_direction_events": int((tier_a & has_direction).sum()),
        "confidence_counts": confidence["wind_confidence_class"].value_counts(dropna=False).to_dict(),
        "missing_reason_counts": candidates["wind_missing_reason"].fillna("none").value_counts().to_dict(),
        "speed_summary": _quantiles(candidates["wind_speed_mean"]),
        "speed_upper_summary": _quantiles(candidates["wind_speed_p95"]),
        "impossible_speed_count": impossible_speed_count,
        "direction_ok": direction_ok,
        "direction_sin_cos_max_error": sin_cos_max_error,
        "wave_wind_crosstab": wave_crosstab,
        "current_wind_crosstab": current_crosstab,
        "both_high_wave_wind": int(
            (
                confidence["wind_confidence_class"].eq("A_speed_direction")
                & confidence["wave_confidence_class"].eq("A_high")
            ).sum()
        ),
        "speed_ready_wave_high": int(
            (
                confidence["wind_confidence_class"].isin(["A_speed_direction", "B_speed_only"])
                & confidence["wave_confidence_class"].eq("A_high")
            ).sum()
        ),
        "speed_ready_current_event_scale": int(
            (
                confidence["wind_confidence_class"].isin(["A_speed_direction", "B_speed_only"])
                & confidence["current_confidence_class"].eq("A_event_scale")
            ).sum()
        ),
        "joined_cache_inventory": joined_inventory,
        "raw_cache_inventory": raw_inventory,
    }
    return validation


def build_validation_report(
    dwell: pd.DataFrame,
    candidates: pd.DataFrame,
    confidence: pd.DataFrame,
    validation: dict[str, Any],
    candidate_path: Path,
    confidence_path: Path,
) -> str:
    confidence_counts = pd.Series(validation["confidence_counts"], name="event_count").reset_index()
    confidence_counts = confidence_counts.rename(columns={"index": "wind_confidence_class"})
    missing_counts = pd.Series(validation["missing_reason_counts"], name="event_count").reset_index()
    missing_counts = missing_counts.rename(columns={"index": "wind_missing_reason"})
    by_tier = _count_table(confidence, ["dwell_tier", "wind_confidence_class"])
    by_farm = _count_table(confidence, ["wind_farm", "wind_confidence_class"])
    by_year = (
        confidence.assign(year=pd.to_datetime(candidates["start_utc"], utc=True, errors="coerce").dt.year)
        .groupby(["year", "wind_confidence_class"], dropna=False)
        .size()
        .reset_index(name="event_count")
        .sort_values(["year", "wind_confidence_class"])
    )
    farm_year = (
        confidence.assign(year=pd.to_datetime(candidates["start_utc"], utc=True, errors="coerce").dt.year)
        .groupby(["wind_farm", "year", "wind_confidence_class"], dropna=False)
        .size()
        .reset_index(name="event_count")
        .sort_values("event_count", ascending=False)
    )
    wave_crosstab = validation["wave_wind_crosstab"].reset_index()
    current_crosstab = validation["current_wind_crosstab"].reset_index()
    speed = validation["speed_summary"]
    speed_upper = validation["speed_upper_summary"]
    joined = validation["joined_cache_inventory"]
    raw = validation["raw_cache_inventory"]
    schema_counts = pd.Series(raw.get("schema_counts", {}), name="file_count").reset_index()
    schema_counts = schema_counts.rename(columns={"index": "schema"})

    lines = [
        "# Wind Confidence v1 Validation Report",
        "",
        "## Research Design",
        "",
        "Hypotheses: NORA3 wind speed evidence is usable for many dwell events; wind direction coverage is weak and must be separated from wind speed; wind speed alone may improve later modelling beyond waves/currents; and wind direction should not be trusted unless provenance and coverage are explicit.",
        "",
        "Metrics: wind speed coverage, wind direction coverage, farm/year/tier coverage, temporal alignment quality from existing event aggregates, raw and processed cache schema evidence, missingness reasons, physical range checks, and relation to wave/current confidence classes.",
        "",
        "## Executive Conclusion",
        "",
        "Wind Confidence v1 writes one NORA3 wind candidate and one wind confidence row for every dwell event using existing local evidence only. The main result is a split evidence layer: wind speed is broadly modelling-ready, while wind direction is sparse and should remain pending targeted repair.",
        "",
        f"- Candidate table: `{candidate_path}`",
        f"- Confidence table: `{confidence_path}`",
        f"- Input dwell rows: {validation['input_dwell_rows']:,}",
        f"- Candidate rows: {validation['candidate_rows']:,}",
        f"- Confidence rows: {validation['confidence_rows']:,}",
        f"- Wind speed events: {validation['wind_speed_events']:,}",
        f"- Wind direction events: {validation['wind_direction_events']:,}",
        f"- Tier A wind speed events: {validation['tier_a_wind_speed_events']:,} of {validation['tier_a_events']:,}",
        f"- Tier A wind direction events: {validation['tier_a_wind_direction_events']:,} of {validation['tier_a_events']:,}",
        "",
        "## Input Inventory",
        "",
        f"- Dwell rows: {validation['input_dwell_rows']:,}",
        f"- Fusion v1 wave confidence rows: {validation['fusion_v1_wave_rows']:,}",
        f"- Current confidence rows: {validation['current_confidence_rows']:,}",
        f"- NORA3 joined-cache manifest rows: {joined.get('manifest_rows', 0):,}",
        f"- NORA3 joined-cache parquet files: {joined.get('parquet_files', 0):,}",
        f"- NORA3 joined-cache parquet rows: {joined.get('parquet_rows', 0):,}",
        f"- NORA3 joined-cache year range: {joined.get('manifest_year_min')} to {joined.get('manifest_year_max')}",
        f"- Raw NORA3 wind files inspected: {raw.get('wind_raw_files', 0):,}",
        f"- Raw NORA3 wind year range: {raw.get('year_min')} to {raw.get('year_max')}",
        f"- Joined-cache wind speed fields: `{', '.join(joined.get('wind_speed_fields', []))}`",
        f"- Joined-cache wind direction fields: `{', '.join(joined.get('wind_direction_fields', []))}`",
        "",
        "Raw wind cache schema counts:",
        "",
    ]
    lines.extend(_markdown_table(schema_counts, ["schema", "file_count"], limit=8))
    lines.extend(
        [
            "",
            "## Coverage",
            "",
            f"- Events with wind speed: {validation['wind_speed_events']:,} / {validation['input_dwell_rows']:,}",
            f"- Events with wind direction: {validation['wind_direction_events']:,} / {validation['input_dwell_rows']:,}",
            f"- Tier A wind speed coverage: {validation['tier_a_wind_speed_events']:,} / {validation['tier_a_events']:,}",
            f"- Tier A wind direction coverage: {validation['tier_a_wind_direction_events']:,} / {validation['tier_a_events']:,}",
            "",
            "Confidence by dwell tier:",
            "",
        ]
    )
    lines.extend(_markdown_table(by_tier, ["dwell_tier", "wind_confidence_class", "event_count"]))
    lines.extend(["", "Confidence by year:", ""])
    lines.extend(_markdown_table(by_year, ["year", "wind_confidence_class", "event_count"]))
    lines.extend(["", "Farm/year confidence counts (top rows):", ""])
    lines.extend(_markdown_table(farm_year, ["wind_farm", "year", "wind_confidence_class", "event_count"], limit=25))
    lines.extend(["", "Farm confidence counts (top rows):", ""])
    lines.extend(_markdown_table(by_farm, ["wind_farm", "wind_confidence_class", "event_count"], limit=25))
    lines.extend(
        [
            "",
            "## Physical QA",
            "",
            f"- Wind speed mean min/p50/p95/max: {speed['min']:.3f} / {speed['p50']:.3f} / {speed['p95']:.3f} / {speed['max']:.3f} m/s",
            f"- Wind upper-window diagnostic min/p50/p95/max: {speed_upper['min']:.3f} / {speed_upper['p50']:.3f} / {speed_upper['p95']:.3f} / {speed_upper['max']:.3f} m/s",
            f"- Impossible wind speed rows outside 0-75 m/s: {validation['impossible_speed_count']:,}",
            f"- Direction degrees in [0, 360): {validation['direction_ok']}",
            f"- Direction sin/cos unit-vector max error: {validation['direction_sin_cos_max_error']:.12f}",
            f"- Direction convention: `{WIND_DIRECTION_CONVENTION}`",
            "- Direction sin/cos outputs are unit-circle projections of the accepted event mean direction; missing direction remains null.",
            "",
            "Note: " + WIND_SPEED_P95_METHOD_NOTE,
            "",
            "## Missingness Diagnosis",
            "",
        ]
    )
    lines.extend(_markdown_table(missing_counts, ["wind_missing_reason", "event_count"], limit=12))
    lines.extend(
        [
            "",
            "Most raw NORA3 wind cache files in this local archive are speed-only. Direction-capable raw files exist but are a small minority, and the current dwell-weather table contains only sparse active wind-direction aggregates. Therefore direction missingness is treated as an upstream acquisition/schema coverage issue rather than a zero-direction value.",
            "",
            "## Confidence",
            "",
        ]
    )
    lines.extend(_markdown_table(confidence_counts, ["wind_confidence_class", "event_count"]))
    lines.extend(["", "Wind confidence versus wave confidence:", ""])
    lines.extend(_markdown_table(wave_crosstab, list(wave_crosstab.columns)))
    lines.extend(["", "Wind confidence versus current confidence:", ""])
    lines.extend(_markdown_table(current_crosstab, list(current_crosstab.columns)))
    lines.extend(
        [
            "",
            f"- Events with both `A_speed_direction` wind and `A_high` wave confidence: {validation['both_high_wave_wind']:,}",
            f"- Events with speed-ready wind and `A_high` wave confidence: {validation['speed_ready_wave_high']:,}",
            f"- Events with speed-ready wind and `A_event_scale` current confidence: {validation['speed_ready_current_event_scale']:,}",
            "",
            "## Research Interpretation",
            "",
            "Wind speed is ready for Fusion v2 as a 10 m NORA3 active-window event feature with explicit `B_speed_only` confidence for most usable rows. Wind direction is not ready as a broad modelling feature: the valid event count is too small, and missing direction must not be treated as calm, zero, or aligned wind.",
            "",
            "Fusion v2 should include wind speed and wind confidence, while either leaving wind direction nullable or treating it as a narrow sensitivity-only feature. The real source-resolved Fusion v2 should therefore combine wave confidence, wind confidence, current confidence, and bathymetry, with wind direction flagged as repair-pending.",
            "",
            "## Recommendation",
            "",
            "Accept Wind Confidence v1 if row identity is preserved, speed physical QA passes, direction missingness is explicit, and no synthetic/fallback wind evidence is introduced. Proceed to Fusion v2 with wind speed included and wind direction marked pending targeted NORA3 repair. Run a targeted wind-direction repair only if later modelling or simulator inputs require directional wind effects beyond speed.",
        ]
    )
    return "\n".join(lines) + "\n"


def write_outputs(
    candidates: pd.DataFrame,
    confidence: pd.DataFrame,
    report: str,
    output_dir: Path,
    report_dir: Path,
    overwrite: bool,
) -> tuple[Path, Path, Path]:
    candidate_path = output_dir / WIND_EVENT_CANDIDATES_FILENAME
    confidence_path = output_dir / WIND_EVENT_CONFIDENCE_FILENAME
    report_path = report_dir / VALIDATION_REPORT_FILENAME
    existing = [path for path in (candidate_path, confidence_path, report_path) if path.exists()]
    if existing and not overwrite:
        raise FileExistsError(
            "Wind Confidence v1 outputs already exist; pass overwrite=True to replace: "
            + ", ".join(str(path) for path in existing)
        )
    output_dir.mkdir(parents=True, exist_ok=True)
    report_dir.mkdir(parents=True, exist_ok=True)
    ensure_candidate_schema(candidates).to_parquet(candidate_path, index=False)
    ensure_confidence_schema(confidence).to_parquet(confidence_path, index=False)
    report_path.write_text(report, encoding="utf-8")
    return candidate_path, confidence_path, report_path


def build_wind_confidence_v1(
    dwell_weather: Path = DEFAULT_DWELL_WEATHER,
    nora3_joined_cache: Path = DEFAULT_NORA3_JOINED_CACHE,
    nora3_raw_cache: Path = DEFAULT_NORA3_RAW_CACHE,
    wave_confidence_path: Path = DEFAULT_WAVE_CONFIDENCE,
    current_confidence_path: Path = DEFAULT_CURRENT_CONFIDENCE,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    report_dir: Path = DEFAULT_REPORT_DIR,
    overwrite: bool = False,
) -> WindConfidenceResult:
    candidate_path = output_dir / WIND_EVENT_CANDIDATES_FILENAME
    confidence_path = output_dir / WIND_EVENT_CONFIDENCE_FILENAME
    report_path = report_dir / VALIDATION_REPORT_FILENAME
    if not overwrite:
        existing = [path for path in (candidate_path, confidence_path, report_path) if path.exists()]
        if existing:
            raise FileExistsError(
                "Wind Confidence v1 outputs already exist; pass overwrite=True to replace: "
                + ", ".join(str(path) for path in existing)
            )

    dwell = load_dwell_wind_events(dwell_weather)
    wave_confidence = load_wave_confidence(wave_confidence_path)
    current_confidence = load_current_confidence(current_confidence_path)
    joined_inventory = inventory_joined_cache(nora3_joined_cache)
    raw_inventory = inventory_raw_wind_cache(nora3_raw_cache)

    candidates = build_wind_event_candidates(dwell)
    confidence = build_wind_event_confidence(candidates, wave_confidence, current_confidence)
    validation = validate_wind_confidence(
        dwell=dwell,
        candidates=candidates,
        confidence=confidence,
        wave_confidence=wave_confidence,
        current_confidence=current_confidence,
        joined_inventory=joined_inventory,
        raw_inventory=raw_inventory,
    )
    report = build_validation_report(
        dwell=dwell,
        candidates=candidates,
        confidence=confidence,
        validation=validation,
        candidate_path=candidate_path,
        confidence_path=confidence_path,
    )
    candidate_path, confidence_path, report_path = write_outputs(
        candidates=candidates,
        confidence=confidence,
        report=report,
        output_dir=output_dir,
        report_dir=report_dir,
        overwrite=overwrite,
    )
    return WindConfidenceResult(
        candidate_path=candidate_path,
        confidence_path=confidence_path,
        report_path=report_path,
        candidates=candidates,
        confidence=confidence,
        validation=validation,
    )
