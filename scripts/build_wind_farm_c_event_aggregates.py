"""
Build event-level aggregates for CARE Wind Farm C.

This script collapses the 10-minute Wind Farm C feature matrix into one row per
CARE event for baseline modeling. Wind Farm C is treated as the current
high-confidence working mapping to Trianel Windpark Borkum I+II with a 0-year
temporal shift; see docs/wind-farm-c-current-state.md for caveats.

Input:
  Data/Processed/wind_farm_c_feature_matrix.parquet

Output:
  Data/Processed/wind_farm_c_event_aggregates.parquet

Usage:
  python scripts/build_wind_farm_c_event_aggregates.py [--force]
"""

from __future__ import annotations

import argparse
import math
import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
INPUT_PARQUET = PROJECT_ROOT / "Data" / "Processed" / "wind_farm_c_feature_matrix.parquet"
OUTPUT_PARQUET = PROJECT_ROOT / "Data" / "Processed" / "wind_farm_c_event_aggregates.parquet"

SCALAR_COLUMNS = [
    "hs",
    "tp",
    "wind_speed_10m",
    "wind_speed_100m",
    "current_speed",
]

ANGULAR_COLUMNS = [
    "wave_direction",
    "wind_direction_10m",
    "wind_direction_100m",
    "current_direction",
]

STATUS_VALUES = [0, 1, 2, 3, 4, 5]
LABEL_VALUES = [
    "unknown",
    "maintenance_success",
    "standby_weather",
    "attempted_transfer",
]


def _valid_angles_degrees(values: pd.Series) -> np.ndarray:
    angles = pd.to_numeric(values, errors="coerce").dropna().to_numpy(dtype=float)
    if len(angles) == 0:
        return angles
    return np.mod(angles, 360.0)


def circular_mean_degrees(values: pd.Series) -> float:
    """Return circular mean in degrees on [0, 360), or NaN for all-missing input."""
    angles = _valid_angles_degrees(values)
    if len(angles) == 0:
        return math.nan

    radians = np.deg2rad(angles)
    x_bar = np.cos(radians).mean()
    y_bar = np.sin(radians).mean()

    if np.isclose(x_bar, 0.0) and np.isclose(y_bar, 0.0):
        return math.nan

    return float(np.mod(np.rad2deg(np.arctan2(y_bar, x_bar)), 360.0))


def circular_variance(values: pd.Series) -> float:
    """
    Return circular variance/spread in [0, 1].

    0 means all directions are aligned; 1 means the resultant vector length is
    zero, such as a uniform distribution around the circle.
    """
    angles = _valid_angles_degrees(values)
    if len(angles) == 0:
        return math.nan

    radians = np.deg2rad(angles)
    x_bar = np.cos(radians).mean()
    y_bar = np.sin(radians).mean()
    resultant_length = math.sqrt(float(x_bar**2 + y_bar**2))
    return float(1.0 - resultant_length)


def _safe_share(mask: pd.Series, denominator: int) -> float:
    if denominator == 0:
        return math.nan
    return float(mask.sum() / denominator)


def _dominant_label(labels: pd.Series) -> str:
    counts = labels.fillna("unknown").value_counts()
    if counts.empty:
        return "unknown"
    return str(counts.index[0])


def aggregate_feature_matrix(feature_df: pd.DataFrame) -> pd.DataFrame:
    """Collapse the 10-minute Wind Farm C feature matrix to one row per event."""
    required = {"timestamp", "event_id", "asset_id", "event_label_care", "label"}
    missing = required - set(feature_df.columns)
    if missing:
        raise ValueError(f"Feature matrix missing required columns: {sorted(missing)}")

    df = feature_df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    event_rows = []
    for (event_id, asset_id), group in df.groupby(["event_id", "asset_id"], sort=True):
        group = group.sort_values("timestamp")
        row_count = len(group)

        event_row = {
            "event_id": int(event_id),
            "asset_id": int(asset_id),
            "event_label_care": str(group["event_label_care"].dropna().iloc[0])
            if group["event_label_care"].notna().any()
            else "",
            "event_label_model": _dominant_label(group["label"]),
            "row_count_10min": int(row_count),
            "event_start": group["timestamp"].min(),
            "event_end": group["timestamp"].max(),
            "duration_hours": float(row_count / 6.0),
        }

        for col in SCALAR_COLUMNS:
            if col in group.columns:
                values = pd.to_numeric(group[col], errors="coerce")
                event_row[f"{col}_mean"] = float(values.mean())
                event_row[f"{col}_max"] = float(values.max())
                event_row[f"{col}_min"] = float(values.min())
                event_row[f"{col}_std"] = float(values.std(ddof=0))
                event_row[f"{col}_null_share"] = _safe_share(values.isna(), row_count)
            else:
                for suffix in ["mean", "max", "min", "std", "null_share"]:
                    event_row[f"{col}_{suffix}"] = math.nan

        for col in ANGULAR_COLUMNS:
            if col in group.columns:
                values = group[col]
                event_row[f"{col}_circular_mean"] = circular_mean_degrees(values)
                event_row[f"{col}_circular_variance"] = circular_variance(values)
                event_row[f"{col}_null_share"] = _safe_share(values.isna(), row_count)
            else:
                event_row[f"{col}_circular_mean"] = math.nan
                event_row[f"{col}_circular_variance"] = math.nan
                event_row[f"{col}_null_share"] = math.nan

        status = pd.to_numeric(group.get("status_type_id"), errors="coerce")
        for value in STATUS_VALUES:
            event_row[f"share_status_{value}"] = _safe_share(status == value, row_count)
        event_row["share_status_nan"] = _safe_share(status.isna(), row_count)
        known_status_mask = status.isna()
        for value in STATUS_VALUES:
            known_status_mask = known_status_mask | (status == value)
        event_row["share_status_other"] = _safe_share(~known_status_mask, row_count)

        labels = group["label"].fillna("unknown").astype(str)
        for label in LABEL_VALUES:
            event_row[f"share_label_{label}"] = _safe_share(labels == label, row_count)
        event_row["share_label_other"] = _safe_share(~labels.isin(LABEL_VALUES), row_count)

        event_rows.append(event_row)

    result = pd.DataFrame(event_rows)
    return result.sort_values(["event_id", "asset_id"]).reset_index(drop=True)


def _print_summary(event_df: pd.DataFrame, output_path: Path) -> None:
    print("\n" + "=" * 68)
    print("WIND FARM C EVENT AGGREGATES — SUMMARY")
    print("=" * 68)
    print(f"  Events             : {len(event_df):,}")
    print(f"  Columns            : {len(event_df.columns):,}")
    print(f"  Date span          : {event_df['event_start'].min()} → {event_df['event_end'].max()}")
    print(f"  Output             : {output_path}")

    print("\n  CARE event labels:")
    for label, count in event_df["event_label_care"].value_counts().items():
        print(f"    {label:<25} {count:>5}")

    print("\n  Dominant handshake labels:")
    for label, count in event_df["event_label_model"].value_counts().items():
        print(f"    {label:<25} {count:>5}")

    null_cols = event_df.columns[event_df.isna().any()].tolist()
    if null_cols:
        print("\n  Columns with NaN values:")
        for col in null_cols:
            print(f"    {col:<40} {event_df[col].isna().mean() * 100:.1f}%")
    else:
        print("\n  Columns with NaN values: none")
    print("=" * 68)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build Wind Farm C event-level aggregates.")
    parser.add_argument("--input", default=str(INPUT_PARQUET), help="Input 10-minute feature matrix Parquet.")
    parser.add_argument("--output", default=str(OUTPUT_PARQUET), help="Output event aggregate Parquet.")
    parser.add_argument("--force", action="store_true", help="Overwrite output if it already exists.")
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)

    if not input_path.exists():
        print(f"[ERROR] Input feature matrix not found: {input_path}")
        print("Run scripts/build_wind_farm_c_feature_matrix.py --force first.")
        sys.exit(1)

    if output_path.exists() and not args.force:
        print(f"[SKIP] Output already exists: {output_path}")
        print("Re-run with --force to overwrite.")
        sys.exit(0)

    print(f"Loading feature matrix: {input_path}")
    feature_df = pd.read_parquet(input_path)
    event_df = aggregate_feature_matrix(feature_df)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    event_df.to_parquet(output_path, index=False)
    _print_summary(event_df, output_path)


if __name__ == "__main__":
    main()
