"""Inspect native FINO CSV/ASCII exports before any import.

The inspector is intentionally report-only. It helps validate a small manual
BSH Insitu export by detecting delimiters, timestamp cadence, likely FINO wave
columns, units, QC/status fields, and physical-range warnings. It does not
create a processed FINO archive or source-fuse observations.
"""

from __future__ import annotations

import csv
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


DEFAULT_OUTPUT_REPORT = Path(
    "analysis/06_rq6_metocean_spatial_resolution/fino_export_inspection_report.md"
)

CANONICAL_IMPORT_SCHEMA = [
    "station_id",
    "timestamp_utc",
    "fino_hs",
    "fino_tp",
    "fino_wave_direction",
    "variable_units",
    "qc_flag",
    "source_file",
    "access_method",
    "parser_version",
]

PARSER_VERSION = "fino_export_inspector_v1"

DELIMITER_CANDIDATES = [
    ("comma", ",", ","),
    ("semicolon", ";", ";"),
    ("tab", "\t", "\t"),
    ("whitespace", r"\s+", None),
]

TIMESTAMP_PATTERNS = [
    "timestamp",
    "datetime",
    "date_time",
    "date time",
    "time_utc",
    "utc",
    "zeit",
    "datum",
    "time",
    "date",
]

HS_PATTERNS = [
    "hs",
    "h_s",
    "hm0",
    "swh",
    "significantwaveheight",
    "significant_wave_height",
    "waveheight",
    "wave_height",
]

TP_PATTERNS = [
    "tp",
    "t_p",
    "tpeak",
    "peakperiod",
    "peak_period",
    "peakwaveperiod",
    "peak_wave_period",
]

DIRECTION_PATTERNS = [
    "theta",
    "mdir",
    "mwd",
    "wave_direction",
    "wavedirection",
    "wave_dir",
    "wavedir",
    "thq",
    "direction",
    "dir",
    "richtung",
]

QC_PATTERNS = [
    "qc",
    "quality",
    "qualitaet",
    "qualitat",
    "flag",
    "status",
    "datenstatus",
    "validation",
]


@dataclass(frozen=True)
class DelimiterDetection:
    label: str
    pandas_sep: str
    csv_delimiter: str | None
    header_line_index: int
    header_columns: list[str]


@dataclass(frozen=True)
class TimestampInspection:
    timestamp_column: str | None
    timestamp_source_columns: list[str]
    parsed_non_null_count: int
    parse_failure_count: int
    timestamp_start_utc: str | None
    timestamp_end_utc: str | None
    inferred_cadence: str | None
    ten_minute_cadence: bool
    duplicate_timestamp_count: int
    cadence_gap_count: int
    timezone_handling: str


@dataclass(frozen=True)
class FinoExportInspectionResult:
    input_path: Path
    station_id: str
    row_count: int
    columns: list[str]
    delimiter: DelimiterDetection
    timestamp: TimestampInspection
    canonical_mapping: dict[str, str]
    unit_mapping: dict[str, str]
    qc_columns: list[str]
    unknown_columns: list[str]
    missingness: pd.DataFrame
    numeric_ranges: pd.DataFrame
    warnings: list[str]
    safe_for_small_import_pilot: bool
    output_report: Path | None


def _normalise_column_name(name: str) -> str:
    clean = re.sub(r"\[[^\]]*\]|\([^)]*\)", "", str(name))
    clean = clean.replace("µ", "u")
    clean = re.sub(r"[^0-9a-zA-Z]+", "_", clean).strip("_").lower()
    return clean


def _compact_column_name(name: str) -> str:
    return re.sub(r"[^0-9a-zA-Z]+", "", _normalise_column_name(name))


def _split_line(line: str, label: str, csv_delimiter: str | None) -> list[str]:
    stripped = line.strip().lstrip("\ufeff")
    if not stripped:
        return []
    if label == "whitespace":
        return [token.strip().strip('"').strip("'") for token in re.split(r"\s+", stripped) if token.strip()]
    try:
        return [token.strip().strip('"').strip("'") for token in next(csv.reader([stripped], delimiter=csv_delimiter or ","))]
    except csv.Error:
        return []


def _column_score(tokens: list[str]) -> int:
    if len(tokens) < 2:
        return -100
    normalised = {_normalise_column_name(token) for token in tokens}
    compact = {_compact_column_name(token) for token in tokens}
    alpha_tokens = sum(bool(re.search(r"[A-Za-z]", token)) for token in tokens)
    score = len(tokens) + alpha_tokens
    score += 10 if any(pattern in normalised or pattern in compact for pattern in TIMESTAMP_PATTERNS) else 0
    score += 5 if any(pattern in normalised or pattern in compact for pattern in HS_PATTERNS) else 0
    score += 5 if any(pattern in normalised or pattern in compact for pattern in TP_PATTERNS) else 0
    score += 5 if any(pattern in normalised or pattern in compact for pattern in DIRECTION_PATTERNS) else 0
    score += 3 if any(any(pattern in column for pattern in QC_PATTERNS) for column in normalised) else 0
    return score


def detect_delimiter_and_header(lines: list[str]) -> DelimiterDetection:
    best: tuple[int, str, str, str | None, int, list[str]] | None = None

    for index, line in enumerate(lines[:100]):
        if not line.strip() or line.lstrip().startswith(("#", "//")):
            continue
        for label, pandas_sep, csv_delimiter in DELIMITER_CANDIDATES:
            tokens = _split_line(line, label, csv_delimiter)
            score = _column_score(tokens)
            if best is None or score > best[0]:
                best = (score, label, pandas_sep, csv_delimiter, index, tokens)

    if best is None or best[0] < 0:
        raise ValueError("Could not identify a tabular FINO export header.")

    _, label, pandas_sep, csv_delimiter, header_line_index, header_columns = best
    return DelimiterDetection(
        label=label,
        pandas_sep=pandas_sep,
        csv_delimiter=csv_delimiter,
        header_line_index=header_line_index,
        header_columns=header_columns,
    )


def _extract_unit(column: str) -> str | None:
    match = re.search(r"\[([^\]]+)\]|\(([^)]+)\)", str(column))
    if not match:
        return None
    return (match.group(1) or match.group(2) or "").strip() or None


def _looks_like_unit_row(row: pd.Series, timestamp_column: str | None) -> bool:
    if row.empty:
        return False
    values = [str(value).strip() for value in row.tolist() if pd.notna(value) and str(value).strip()]
    if not values:
        return False
    if timestamp_column and timestamp_column in row:
        parsed = pd.to_datetime(pd.Series([row[timestamp_column]]), errors="coerce", utc=True)
        if parsed.notna().iloc[0]:
            return False
    unit_like = 0
    for value in values:
        if re.fullmatch(r"(?i)(m|meter|metre|s|sec|second|deg|degree|°|utc|date|time|flag|qc|status|-)", value):
            unit_like += 1
    return unit_like >= max(2, int(0.5 * len(values)))


def _read_export_table(path: Path, delimiter: DelimiterDetection, encoding: str) -> pd.DataFrame:
    sep = delimiter.pandas_sep
    kwargs: dict[str, Any] = {
        "sep": sep,
        "skiprows": delimiter.header_line_index,
        "engine": "python",
        "encoding": encoding,
        "comment": "#",
    }
    if delimiter.label != "whitespace":
        kwargs["sep"] = delimiter.csv_delimiter
    table = pd.read_csv(path, **kwargs)
    table.columns = [str(column).strip().lstrip("\ufeff") for column in table.columns]
    table = table.dropna(how="all")
    table = table.reset_index(drop=True)
    return table


def _find_timestamp_column(columns: list[str], explicit_timestamp_column: str | None) -> str | None:
    if explicit_timestamp_column:
        if explicit_timestamp_column not in columns:
            raise ValueError(f"Explicit timestamp column not found: {explicit_timestamp_column}")
        return explicit_timestamp_column

    for column in columns:
        normalised = _normalise_column_name(column)
        compact = _compact_column_name(column)
        if normalised in TIMESTAMP_PATTERNS or compact in TIMESTAMP_PATTERNS:
            return column
    for column in columns:
        normalised = _normalise_column_name(column)
        compact = _compact_column_name(column)
        if "time" in normalised or "date" in normalised or "zeit" in normalised or "datum" in normalised:
            return column
        if "time" in compact or "date" in compact or "zeit" in compact or "datum" in compact:
            return column
    return None


def _find_date_time_columns(columns: list[str]) -> tuple[str | None, str | None]:
    date_column: str | None = None
    time_column: str | None = None
    for column in columns:
        normalised = _normalise_column_name(column)
        compact = _compact_column_name(column)
        if date_column is None and (normalised in {"date", "datum"} or compact in {"date", "datum"}):
            date_column = column
        if time_column is None and (normalised in {"time", "zeit"} or compact in {"time", "zeit"}):
            time_column = column
    return date_column, time_column


def _parse_timestamps(
    table: pd.DataFrame,
    explicit_timestamp_column: str | None,
) -> tuple[pd.Series, str | None, list[str], str]:
    timestamp_column: str | None
    source_columns: list[str] = []

    if explicit_timestamp_column:
        timestamp_column = _find_timestamp_column(list(table.columns), explicit_timestamp_column)
        raw = table[timestamp_column].astype(str)
        source_columns = [timestamp_column]
    else:
        date_column, time_column = _find_date_time_columns(list(table.columns))
        if date_column and time_column:
            raw = table[date_column].astype(str).str.strip() + " " + table[time_column].astype(str).str.strip()
            timestamp_column = None
            source_columns = [date_column, time_column]
        else:
            timestamp_column = _find_timestamp_column(list(table.columns), None)
            if timestamp_column:
                raw = table[timestamp_column].astype(str)
                source_columns = [timestamp_column]
            else:
                raise ValueError("Could not detect a timestamp column. Pass --timestamp-column for this export.")

    raw_clean = raw.str.strip()
    parsed_default = pd.to_datetime(raw_clean, errors="coerce", utc=True)
    parsed_dayfirst = pd.to_datetime(raw_clean, errors="coerce", utc=True, dayfirst=True)
    parsed = parsed_dayfirst if parsed_dayfirst.notna().sum() > parsed_default.notna().sum() else parsed_default

    timezone_aware_strings = raw_clean.str.contains(
        r"(?i)(?:Z|UTC|[+-]\d{2}:?\d{2}|CET|CEST|GMT)", regex=True, na=False
    ).any()
    timezone_handling = (
        "timestamps include timezone markers and are normalized to UTC"
        if timezone_aware_strings
        else "timestamps are timezone-naive in the native export; inspector assumes UTC for the pilot plan"
    )
    return parsed, timestamp_column, source_columns, timezone_handling


def _infer_cadence(timestamps: pd.Series) -> tuple[str | None, bool, int]:
    valid = pd.Series(timestamps.dropna().sort_values().unique())
    if len(valid) < 2:
        return None, False, 0
    diffs = valid.diff().dropna()
    if diffs.empty:
        return None, False, 0
    mode = diffs.mode().iloc[0] if not diffs.mode().empty else diffs.median()
    cadence = pd.to_timedelta(mode)
    ten_minute = cadence == pd.Timedelta(minutes=10)
    gap_threshold = cadence * 1.5
    gap_count = int((diffs > gap_threshold).sum())
    return str(cadence), bool(ten_minute), gap_count


def _matches_any(column: str, patterns: list[str]) -> bool:
    normalised = _normalise_column_name(column)
    compact = _compact_column_name(column)
    candidates = {normalised, compact}
    return any(pattern in candidates or pattern in normalised or pattern in compact for pattern in patterns)


def _select_first_column(columns: list[str], patterns: list[str]) -> str | None:
    exact_matches = [
        column
        for column in columns
        if _normalise_column_name(column) in patterns or _compact_column_name(column) in patterns
    ]
    if exact_matches:
        return exact_matches[0]
    partial_matches = [column for column in columns if _matches_any(column, patterns)]
    return partial_matches[0] if partial_matches else None


def _to_numeric(series: pd.Series) -> pd.Series:
    if pd.api.types.is_numeric_dtype(series):
        return pd.to_numeric(series, errors="coerce")
    cleaned = (
        series.astype(str)
        .str.strip()
        .replace({"": np.nan, "nan": np.nan, "NaN": np.nan, "NA": np.nan, "N/A": np.nan})
        .str.replace(",", ".", regex=False)
    )
    return pd.to_numeric(cleaned, errors="coerce")


def _build_missingness_and_ranges(
    table: pd.DataFrame,
    mapping: dict[str, str],
) -> tuple[pd.DataFrame, pd.DataFrame, list[str]]:
    rows = []
    ranges = []
    warnings: list[str] = []

    for raw_column, canonical in mapping.items():
        if canonical == "timestamp_utc":
            continue
        numeric = _to_numeric(table[raw_column])
        non_null = int(numeric.notna().sum())
        null_count = int(numeric.isna().sum())
        null_rate = float(null_count / len(table)) if len(table) else 0.0
        rows.append(
            {
                "raw_column": raw_column,
                "canonical_column": canonical,
                "non_null_count": non_null,
                "null_count": null_count,
                "null_rate": null_rate,
            }
        )
        ranges.append(
            {
                "raw_column": raw_column,
                "canonical_column": canonical,
                "min": float(numeric.min()) if non_null else np.nan,
                "max": float(numeric.max()) if non_null else np.nan,
                "mean": float(numeric.mean()) if non_null else np.nan,
            }
        )
        if canonical == "fino_hs" and (numeric.dropna() < 0).any():
            warnings.append(f"{raw_column}: Hs contains negative values.")
        if canonical == "fino_tp" and (numeric.dropna() <= 0).any():
            warnings.append(f"{raw_column}: Tp contains non-positive values.")
        if canonical == "fino_wave_direction":
            direction = numeric.dropna()
            if ((direction < 0) | (direction >= 360)).any():
                warnings.append(f"{raw_column}: wave direction contains values outside [0, 360).")

    return pd.DataFrame(rows), pd.DataFrame(ranges), warnings


def _format_dataframe(df: pd.DataFrame) -> str:
    if df.empty:
        return "_None._"
    return df.to_markdown(index=False)


def _render_report(result: FinoExportInspectionResult) -> str:
    timestamp = result.timestamp
    mapping_rows = pd.DataFrame(
        [
            {"raw_column": raw, "canonical_column": canonical, "unit": result.unit_mapping.get(raw, "")}
            for raw, canonical in result.canonical_mapping.items()
        ]
    )
    unit_rows = pd.DataFrame(
        [{"raw_column": raw, "unit": unit} for raw, unit in result.unit_mapping.items()]
    )

    lines = [
        "# FINO Native Export Inspection Report",
        "",
        "Status: dry-run inspection only. No FINO bulk import, processed FINO archive, current download, source fusion, 10-minute interpolation, NORA3 rerun, or final dwell-metocean rebuild was run.",
        "",
        "## Executive Conclusion",
        "",
        f"- station_id: `{result.station_id}`",
        f"- input_file: `{result.input_path}`",
        f"- row_count: `{result.row_count}`",
        f"- column_count: `{len(result.columns)}`",
        f"- inferred_delimiter: `{result.delimiter.label}`",
        f"- timestamp_parse_failures: `{timestamp.parse_failure_count}`",
        f"- inferred_cadence: `{timestamp.inferred_cadence}`",
        f"- ten_minute_cadence: `{timestamp.ten_minute_cadence}`",
        f"- duplicate_timestamp_count: `{timestamp.duplicate_timestamp_count}`",
        f"- safe_for_small_import_pilot: `{result.safe_for_small_import_pilot}`",
        "",
        "## Input Shape",
        "",
        f"- header_line_index_zero_based: `{result.delimiter.header_line_index}`",
        f"- columns: `{', '.join(result.columns)}`",
        "",
        "## Timestamp Inspection",
        "",
        f"- timestamp_column: `{timestamp.timestamp_column}`",
        f"- timestamp_source_columns: `{', '.join(timestamp.timestamp_source_columns)}`",
        f"- timestamp_start_utc: `{timestamp.timestamp_start_utc}`",
        f"- timestamp_end_utc: `{timestamp.timestamp_end_utc}`",
        f"- parsed_non_null_count: `{timestamp.parsed_non_null_count}`",
        f"- parse_failure_count: `{timestamp.parse_failure_count}`",
        f"- cadence_gap_count: `{timestamp.cadence_gap_count}`",
        f"- timezone_handling: {timestamp.timezone_handling}",
        "",
        "## Canonical Mapping Proposal",
        "",
        _format_dataframe(mapping_rows),
        "",
        "## Unit Detection",
        "",
        _format_dataframe(unit_rows),
        "",
        "## QC / Status Columns",
        "",
        "- " + "\n- ".join(result.qc_columns) if result.qc_columns else "_None detected._",
        "",
        "## Unknown Columns To Preserve",
        "",
        "- " + "\n- ".join(result.unknown_columns) if result.unknown_columns else "_None._",
        "",
        "## Missingness By Mapped Variable",
        "",
        _format_dataframe(result.missingness),
        "",
        "## Numeric Ranges By Mapped Variable",
        "",
        _format_dataframe(result.numeric_ranges),
        "",
        "## Proposed Later Import Schema",
        "",
        "- " + "\n- ".join(CANONICAL_IMPORT_SCHEMA),
        "",
        "## Warnings",
        "",
        "- " + "\n- ".join(result.warnings) if result.warnings else "_No warnings._",
        "",
        "## Do-Not-Do",
        "",
        "- Do not bulk-import FINO until this report has been reviewed.",
        "- Do not write a processed FINO parquet archive from inspection mode.",
        "- Do not scrape the BSH portal or store credentials in the repo.",
        "- Do not treat FINO as an automatic farm-wide source.",
        "- Do not run current downloads, source fusion, or final dwell-table rebuilds from this task.",
        "",
    ]
    return "\n".join(lines)


def inspect_fino_export(
    input_path: Path,
    station_id: str,
    output_report: Path | None = None,
    timestamp_column: str | None = None,
    delimiter: str | None = None,
    encoding: str = "utf-8-sig",
    dry_run: bool = True,
) -> FinoExportInspectionResult:
    """Inspect a single native FINO export and optionally write a Markdown report."""

    if not dry_run:
        raise ValueError("FINO export inspection is dry-run/report-only in this implementation.")
    if not input_path.exists():
        raise FileNotFoundError(f"FINO export file not found: {input_path}")

    lines = input_path.read_text(encoding=encoding, errors="replace").splitlines()
    detected = detect_delimiter_and_header(lines)
    if delimiter:
        delimiter_lookup = {label: (label, pandas_sep, csv_delimiter) for label, pandas_sep, csv_delimiter in DELIMITER_CANDIDATES}
        if delimiter not in delimiter_lookup:
            raise ValueError(f"Unsupported delimiter override: {delimiter}")
        label, pandas_sep, csv_delimiter = delimiter_lookup[delimiter]
        detected = DelimiterDetection(
            label=label,
            pandas_sep=pandas_sep,
            csv_delimiter=csv_delimiter,
            header_line_index=detected.header_line_index,
            header_columns=detected.header_columns,
        )

    table = _read_export_table(input_path, detected, encoding=encoding)
    detected_timestamp_column = _find_timestamp_column(list(table.columns), timestamp_column)
    if len(table) and _looks_like_unit_row(table.iloc[0], detected_timestamp_column):
        table = table.iloc[1:].reset_index(drop=True)

    parsed_timestamps, detected_timestamp_column, timestamp_source_columns, timezone_handling = _parse_timestamps(
        table, timestamp_column
    )

    parsed_non_null_count = int(parsed_timestamps.notna().sum())
    parse_failure_count = int(parsed_timestamps.isna().sum())
    duplicate_timestamp_count = int(parsed_timestamps.duplicated().sum())
    cadence, ten_minute_cadence, cadence_gap_count = _infer_cadence(parsed_timestamps)

    canonical_mapping: dict[str, str] = {}
    if detected_timestamp_column:
        canonical_mapping[detected_timestamp_column] = "timestamp_utc"
    elif timestamp_source_columns:
        canonical_mapping[" + ".join(timestamp_source_columns)] = "timestamp_utc"

    hs_column = _select_first_column(list(table.columns), HS_PATTERNS)
    tp_column = _select_first_column(list(table.columns), TP_PATTERNS)
    direction_column = _select_first_column(list(table.columns), DIRECTION_PATTERNS)
    if hs_column:
        canonical_mapping[hs_column] = "fino_hs"
    if tp_column:
        canonical_mapping[tp_column] = "fino_tp"
    if direction_column:
        canonical_mapping[direction_column] = "fino_wave_direction"

    qc_columns = [column for column in table.columns if _matches_any(column, QC_PATTERNS)]
    unit_mapping = {
        column: unit
        for column in table.columns
        if (unit := _extract_unit(column)) is not None
    }

    mapped_raw_columns = set(canonical_mapping)
    timestamp_source_set = set(timestamp_source_columns)
    unknown_columns = [
        column
        for column in table.columns
        if column not in mapped_raw_columns
        and column not in timestamp_source_set
        and column not in qc_columns
    ]

    missingness, numeric_ranges, physical_warnings = _build_missingness_and_ranges(
        table, {raw: canonical for raw, canonical in canonical_mapping.items() if canonical != "timestamp_utc"}
    )

    warnings = list(physical_warnings)
    if parse_failure_count:
        warnings.append(f"{parse_failure_count} rows have unparseable timestamps.")
    if duplicate_timestamp_count:
        warnings.append(f"{duplicate_timestamp_count} duplicate timestamps detected.")
    if not ten_minute_cadence:
        warnings.append("Native cadence is not confirmed as 10 minutes.")
    if hs_column is None:
        warnings.append("No likely Hs/significant wave height column detected.")
    if tp_column is None:
        warnings.append("No likely Tp/peak period column detected.")
    if direction_column is None:
        warnings.append("No likely wave-direction/theta column detected.")
    if direction_column and _normalise_column_name(direction_column) in {"direction", "richtung"}:
        warnings.append(
            f"{direction_column}: generic direction column mapped to wave direction; confirm portal metadata before import."
        )
    if not qc_columns:
        warnings.append("No QC/status/flag columns detected; confirm whether the export includes quality metadata.")

    valid_timestamp_rate = parsed_non_null_count / len(table) if len(table) else 0.0
    required_wave_columns_present = hs_column is not None and tp_column is not None
    no_fatal_physical_warnings = not any(
        "negative values" in warning
        or "non-positive values" in warning
        or "outside [0, 360)" in warning
        for warning in warnings
    )
    safe_for_small_import_pilot = bool(
        len(table)
        and valid_timestamp_rate >= 0.95
        and required_wave_columns_present
        and no_fatal_physical_warnings
    )

    timestamp_inspection = TimestampInspection(
        timestamp_column=detected_timestamp_column,
        timestamp_source_columns=timestamp_source_columns,
        parsed_non_null_count=parsed_non_null_count,
        parse_failure_count=parse_failure_count,
        timestamp_start_utc=parsed_timestamps.min().isoformat() if parsed_non_null_count else None,
        timestamp_end_utc=parsed_timestamps.max().isoformat() if parsed_non_null_count else None,
        inferred_cadence=cadence,
        ten_minute_cadence=ten_minute_cadence,
        duplicate_timestamp_count=duplicate_timestamp_count,
        cadence_gap_count=cadence_gap_count,
        timezone_handling=timezone_handling,
    )

    result = FinoExportInspectionResult(
        input_path=input_path,
        station_id=station_id,
        row_count=int(len(table)),
        columns=list(table.columns),
        delimiter=detected,
        timestamp=timestamp_inspection,
        canonical_mapping=canonical_mapping,
        unit_mapping=unit_mapping,
        qc_columns=qc_columns,
        unknown_columns=unknown_columns,
        missingness=missingness,
        numeric_ranges=numeric_ranges,
        warnings=warnings,
        safe_for_small_import_pilot=safe_for_small_import_pilot,
        output_report=output_report,
    )

    if output_report is not None:
        output_report.parent.mkdir(parents=True, exist_ok=True)
        output_report.write_text(_render_report(result), encoding="utf-8")

    return result
