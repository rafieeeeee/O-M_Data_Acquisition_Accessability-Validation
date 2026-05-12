"""QA summaries for identified O&M dwell event outputs."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional, Tuple, Union

import numpy as np
import pandas as pd


DEFAULT_JUMP_SPEED_KNOTS = 45.0
SUSPECT_TYPE_KEYWORDS = (
    "cargo",
    "tanker",
    "fishing",
    "pleasure",
    "sailing",
    "passenger",
    "wing in ground",
    "military",
    "unknown",
    "undefined",
)


@dataclass(frozen=True)
class EventQaConfig:
    """Thresholds for concise pilot QA checks."""

    top_mmsi_limit: int = 10
    suspect_limit: int = 20
    jump_speed_knots: float = DEFAULT_JUMP_SPEED_KNOTS
    min_reasonable_length_m: float = 5.0
    max_reasonable_length_m: float = 180.0


def audit_event_outputs(
    events_csv: Union[str, Path],
    registry_csv: Union[str, Path],
    config: Optional[EventQaConfig] = None,
) -> dict[str, Any]:
    """Load event and registry CSVs and return a JSON-friendly QA summary."""

    config = config or EventQaConfig()
    events = _read_csv(events_csv)
    registry = _read_csv(registry_csv)

    events = _normalise_columns(events)
    registry = _normalise_columns(registry)
    events = _coerce_event_columns(events)
    registry = _coerce_registry_columns(registry)

    return {
        "inputs": {
            "events_csv": str(events_csv),
            "registry_csv": str(registry_csv),
        },
        "row_counts": {
            "events": int(len(events)),
            "registry_rows": int(len(registry)),
            "unique_event_mmsi": _safe_nunique(events, "mmsi"),
            "unique_registry_mmsi": _safe_nunique(registry, "mmsi"),
        },
        "duration_min": _numeric_distribution(events, "duration_min"),
        "min_dist_m": _numeric_distribution(events, "min_dist"),
        "events_by_wind_farm": _events_by_wind_farm(events),
        "top_mmsis_by_dwell_time": _top_mmsis_by_dwell_time(events, registry, config.top_mmsi_limit),
        "suspect_vessels": _suspect_vessels(events, registry, config),
        "impossible_event_jumps": _impossible_event_jumps(events, config.jump_speed_knots),
    }


def format_qa_summary(summary: dict[str, Any]) -> str:
    """Format a QA summary as plain text without losing JSON-friendly keys."""

    lines = [
        "Pilot event QA summary",
        f"Events: {summary['row_counts']['events']:,}",
        f"Registry rows: {summary['row_counts']['registry_rows']:,}",
        _format_distribution("Duration min", summary["duration_min"]),
        _format_distribution("Min distance m", summary["min_dist_m"]),
        "",
        "Events by wind farm:",
    ]
    lines.extend(_format_records(summary["events_by_wind_farm"], ("wind_farm", "event_count", "total_duration_min")))
    lines.extend(["", "Top MMSIs by dwell time:"])
    lines.extend(
        _format_records(
            summary["top_mmsis_by_dwell_time"],
            ("mmsi", "name", "ship_type", "total_dwell_min", "event_count"),
        )
    )

    suspect = summary["suspect_vessels"]
    lines.extend(
        [
            "",
            "Suspect vessel classes/lengths:",
            f"Suspect rows: {suspect['suspect_count']:,}",
        ]
    )
    lines.extend(_format_records(suspect["examples"], ("mmsi", "name", "ship_type", "length_m", "reason")))

    jumps = summary["impossible_event_jumps"]
    lines.extend(["", "Impossible event jumps:"])
    if not jumps["available"]:
        lines.append(f"Skipped: {jumps['reason']}")
    else:
        lines.append(f"Flagged jumps: {jumps['jump_count']:,}")
        lines.extend(
            _format_records(
                jumps["examples"],
                ("mmsi", "previous_found_id", "next_found_id", "gap_min", "required_speed_knots", "reason"),
            )
        )

    return "\n".join(lines)


def _read_csv(path: Union[str, Path]) -> pd.DataFrame:
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(path)
    return pd.read_csv(path)


def _normalise_columns(frame: pd.DataFrame) -> pd.DataFrame:
    frame = frame.copy()
    frame.columns = [_normalise_column_name(column) for column in frame.columns]
    return frame


def _normalise_column_name(column: object) -> str:
    return str(column).strip().lower().replace(" ", "_").replace("-", "_")


def _coerce_event_columns(events: pd.DataFrame) -> pd.DataFrame:
    events = events.copy()
    _rename_first(events, ("ship_type", "shiptype"), "ship_type")
    _rename_first(events, ("dist",), "min_dist")
    for column in ("duration_min", "min_dist", "length", "latitude", "longitude", "lat", "lon"):
        if column in events:
            events[column] = pd.to_numeric(events[column], errors="coerce")
    for column in ("start", "end"):
        if column in events:
            events[column] = pd.to_datetime(events[column], errors="coerce")
    if "duration_min" not in events and {"start", "end"}.issubset(events.columns):
        events["duration_min"] = (events["end"] - events["start"]).dt.total_seconds() / 60
    return events


def _coerce_registry_columns(registry: pd.DataFrame) -> pd.DataFrame:
    registry = registry.copy()
    _rename_first(registry, ("total_dwell_time", "dwell_time", "duration_min"), "total_dwell_time")
    _rename_first(registry, ("event_count", "events"), "event_count")
    _rename_first(registry, ("ship_type", "shiptype"), "ship_type")
    for column in ("total_dwell_time", "event_count", "length"):
        if column in registry:
            registry[column] = pd.to_numeric(registry[column], errors="coerce")
    return registry


def _rename_first(frame: pd.DataFrame, candidates: tuple[str, ...], target: str) -> None:
    if target in frame:
        return
    for candidate in candidates:
        if candidate in frame:
            frame.rename(columns={candidate: target}, inplace=True)
            return


def _safe_nunique(frame: pd.DataFrame, column: str) -> int:
    if column not in frame:
        return 0
    return int(frame[column].nunique(dropna=True))


def _numeric_distribution(frame: pd.DataFrame, column: str) -> dict[str, Any]:
    if column not in frame:
        return {"available": False, "reason": f"missing column: {column}"}
    series = pd.to_numeric(frame[column], errors="coerce").dropna()
    if series.empty:
        return {"available": False, "reason": f"no numeric values in {column}"}
    quantiles = series.quantile([0.05, 0.25, 0.5, 0.75, 0.95])
    return {
        "available": True,
        "count": int(series.count()),
        "min": _json_number(series.min()),
        "p05": _json_number(quantiles.loc[0.05]),
        "p25": _json_number(quantiles.loc[0.25]),
        "median": _json_number(quantiles.loc[0.5]),
        "p75": _json_number(quantiles.loc[0.75]),
        "p95": _json_number(quantiles.loc[0.95]),
        "max": _json_number(series.max()),
        "mean": _json_number(series.mean()),
    }


def _events_by_wind_farm(events: pd.DataFrame) -> list[dict[str, Any]]:
    if "wind_farm" not in events:
        return []
    grouped = events.groupby("wind_farm", dropna=False).agg(
        event_count=("wind_farm", "size"),
        total_duration_min=("duration_min", "sum") if "duration_min" in events else ("wind_farm", "size"),
        unique_mmsi=("mmsi", "nunique") if "mmsi" in events else ("wind_farm", "size"),
    )
    grouped = grouped.sort_values(["event_count", "total_duration_min"], ascending=False).reset_index()
    return _records(grouped)


def _top_mmsis_by_dwell_time(
    events: pd.DataFrame,
    registry: pd.DataFrame,
    limit: int,
) -> list[dict[str, Any]]:
    source = registry if {"mmsi", "total_dwell_time"}.issubset(registry.columns) else events
    if "mmsi" not in source:
        return []

    if source is registry:
        grouped = registry.groupby("mmsi", dropna=False).agg(
            total_dwell_min=("total_dwell_time", "sum"),
            event_count=("event_count", "sum") if "event_count" in registry else ("mmsi", "size"),
            name=("name", _first_present) if "name" in registry else ("mmsi", _blank),
            ship_type=("ship_type", _first_present) if "ship_type" in registry else ("mmsi", _blank),
            length_m=("length", "max") if "length" in registry else ("mmsi", _nan),
        )
    elif "duration_min" in events:
        grouped = events.groupby("mmsi", dropna=False).agg(
            total_dwell_min=("duration_min", "sum"),
            event_count=("mmsi", "size"),
            name=("name", _first_present) if "name" in events else ("mmsi", _blank),
            ship_type=("ship_type", _first_present) if "ship_type" in events else ("mmsi", _blank),
            length_m=("length", "max") if "length" in events else ("mmsi", _nan),
        )
    else:
        return []

    return _records(grouped.sort_values("total_dwell_min", ascending=False).head(limit).reset_index())


def _suspect_vessels(
    events: pd.DataFrame,
    registry: pd.DataFrame,
    config: EventQaConfig,
) -> dict[str, Any]:
    source = registry if len(registry) else events
    if "mmsi" not in source:
        return {"suspect_count": 0, "reason_counts": {}, "examples": []}

    columns = [column for column in ("mmsi", "name", "ship_type", "length") if column in source]
    vessels = source[columns].drop_duplicates().copy()
    if "ship_type" not in vessels:
        vessels["ship_type"] = ""
    if "length" not in vessels:
        vessels["length"] = np.nan

    vessels["reason"] = vessels.apply(lambda row: _suspect_reason(row, config), axis=1)
    suspects = vessels[vessels["reason"] != ""].copy()
    if suspects.empty:
        return {"suspect_count": 0, "reason_counts": {}, "examples": []}

    suspects.rename(columns={"length": "length_m"}, inplace=True)
    reason_counts = suspects["reason"].value_counts().sort_index().to_dict()
    examples = suspects.sort_values(["reason", "mmsi"]).head(config.suspect_limit)
    return {
        "suspect_count": int(len(suspects)),
        "reason_counts": {str(key): int(value) for key, value in reason_counts.items()},
        "examples": _records(examples),
    }


def _suspect_reason(row: pd.Series, config: EventQaConfig) -> str:
    reasons = []
    ship_type = str(row.get("ship_type", "")).strip().lower()
    length = pd.to_numeric(row.get("length"), errors="coerce")

    if not ship_type or ship_type in {"nan", "none"}:
        reasons.append("missing ship_type")
    elif any(keyword in ship_type for keyword in SUSPECT_TYPE_KEYWORDS):
        reasons.append("suspect ship_type")

    if pd.isna(length):
        reasons.append("missing length")
    elif length < config.min_reasonable_length_m:
        reasons.append("length below reasonable vessel range")
    elif length > config.max_reasonable_length_m:
        reasons.append("length above reasonable vessel range")

    return "; ".join(reasons)


def _impossible_event_jumps(events: pd.DataFrame, max_speed_knots: float) -> dict[str, Any]:
    required = {"mmsi", "start", "end"}
    if not required.issubset(events.columns):
        missing = ", ".join(sorted(required - set(events.columns)))
        return {"available": False, "reason": f"missing columns: {missing}", "jump_count": 0, "examples": []}

    usable = events.dropna(subset=["mmsi", "start", "end"]).copy()
    if usable.empty:
        return {"available": False, "reason": "no events with mmsi/start/end", "jump_count": 0, "examples": []}

    lat_col, lon_col = _event_coordinate_columns(usable)
    has_coordinates = lat_col is not None and lon_col is not None
    usable = usable.sort_values(["mmsi", "start", "end"])
    jumps = []

    for mmsi, group in usable.groupby("mmsi", dropna=False):
        previous_rows = group.iloc[:-1]
        next_rows = group.iloc[1:]
        for previous, current in zip(previous_rows.to_dict("records"), next_rows.to_dict("records")):
            gap_min = (current["start"] - previous["end"]).total_seconds() / 60
            base = {
                "mmsi": mmsi,
                "previous_found_id": previous.get("found_id"),
                "next_found_id": current.get("found_id"),
                "previous_end": previous["end"],
                "next_start": current["start"],
                "gap_min": gap_min,
            }
            if gap_min < 0:
                jumps.append({**base, "required_speed_knots": None, "reason": "overlapping events"})
                continue
            if gap_min == 0 and _changed_location(previous, current, lat_col, lon_col):
                jumps.append({**base, "required_speed_knots": None, "reason": "zero-time location change"})
                continue
            if has_coordinates and gap_min > 0:
                distance_nm = _haversine_nm(previous[lon_col], previous[lat_col], current[lon_col], current[lat_col])
                required_speed = distance_nm / (gap_min / 60)
                if required_speed > max_speed_knots:
                    jumps.append(
                        {
                            **base,
                            "distance_nm": distance_nm,
                            "required_speed_knots": required_speed,
                            "reason": "required speed exceeds threshold",
                        }
                    )

    available = has_coordinates or bool(jumps)
    reason = "" if available else "need coordinate columns for speed-based jumps; no overlapping events found"
    return {
        "available": available,
        "reason": reason,
        "jump_count": int(len(jumps)),
        "threshold_knots": float(max_speed_knots),
        "examples": _records(pd.DataFrame(jumps).head(20)) if jumps else [],
    }


def _event_coordinate_columns(events: pd.DataFrame) -> Tuple[Optional[str], Optional[str]]:
    candidates = (
        ("latitude", "longitude"),
        ("lat", "lon"),
        ("start_lat", "start_lon"),
        ("event_lat", "event_lon"),
    )
    for lat_col, lon_col in candidates:
        if {lat_col, lon_col}.issubset(events.columns):
            events[lat_col] = pd.to_numeric(events[lat_col], errors="coerce")
            events[lon_col] = pd.to_numeric(events[lon_col], errors="coerce")
            if events[[lat_col, lon_col]].notna().any().all():
                return lat_col, lon_col
    return None, None


def _changed_location(
    previous: dict[str, Any],
    current: dict[str, Any],
    lat_col: Optional[str],
    lon_col: Optional[str],
) -> bool:
    if lat_col is not None and lon_col is not None:
        return previous.get(lat_col) != current.get(lat_col) or previous.get(lon_col) != current.get(lon_col)
    return previous.get("found_id") != current.get("found_id")


def _haversine_nm(lon1: float, lat1: float, lon2: float, lat2: float) -> float:
    lon1, lat1, lon2, lat2 = map(np.radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
    meters = 6371000 * 2 * np.arcsin(np.sqrt(a))
    return float(meters / 1852)


def _first_present(series: pd.Series) -> Any:
    present = series.dropna()
    present = present[present.astype(str).str.strip() != ""]
    return present.iloc[0] if len(present) else ""


def _blank(series: pd.Series) -> str:
    return ""


def _nan(series: pd.Series) -> float:
    return np.nan


def _json_number(value: Any) -> Optional[Union[float, int]]:
    if pd.isna(value):
        return None
    value = float(value)
    return int(value) if value.is_integer() else value


def _records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    records = frame.replace({np.nan: None}).to_dict("records")
    return [{str(key): _json_value(value) for key, value in record.items()} for record in records]


def _json_value(value: Any) -> Any:
    if pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        return _json_number(value)
    return value


def _format_distribution(label: str, distribution: dict[str, Any]) -> str:
    if not distribution["available"]:
        return f"{label}: skipped ({distribution['reason']})"
    return (
        f"{label}: count={distribution['count']:,}, "
        f"min={distribution['min']}, median={distribution['median']}, "
        f"p95={distribution['p95']}, max={distribution['max']}"
    )


def _format_records(records: list[dict[str, Any]], keys: tuple[str, ...]) -> list[str]:
    if not records:
        return ["  none"]
    lines = []
    for record in records:
        parts = [f"{key}={record.get(key)}" for key in keys if key in record]
        lines.append(f"  - {', '.join(parts)}")
    return lines
