"""
scripts/build_wind_farm_c_feature_matrix.py
--------------------------------------------
Builds the first production feature matrix for Wind Farm C (Trianel Borkum I+II).

This script joins:
  - CARE Wind Farm C event_info (gives us event windows + asset IDs)
  - SCADA status lookup via SCADAHandshake (0-year shift, direct timestamps)
  - 10-minute metocean backbone (hs, tp, wave_direction, wind, current)
    from Data/Processed/metocean/ if available, else stubs NaN columns.

Output:
  Data/Processed/wind_farm_c_feature_matrix.parquet

Schema:
  timestamp, asset_id, event_id, event_label_care,
  hs, tp, wave_direction,
  wind_speed_10m, wind_direction_10m,
  wind_speed_100m, wind_direction_100m,
  current_speed, current_direction,
  status_type_id, label

Usage:
    python scripts/build_wind_farm_c_feature_matrix.py [--max-events N]
"""

import os
import sys
import argparse
import pandas as pd
import numpy as np
from glob import glob

# Ensure src package is importable
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(PROJECT_ROOT, "src"))

from om_pipeline.analysis.scada_handshake import SCADAHandshake

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
CARE_BASE_DIR   = os.path.join(PROJECT_ROOT, "Data", "CARE_To_Compare")
WIND_FARM_C_DIR = os.path.join(CARE_BASE_DIR, "Wind Farm C")
EVENT_INFO_PATH = os.path.join(WIND_FARM_C_DIR, "event_info.csv")
METOCEAN_DIR    = os.path.join(PROJECT_ROOT, "Data", "Processed", "metocean")
OUTPUT_DIR      = os.path.join(PROJECT_ROOT, "Data", "Processed")
OUTPUT_PARQUET  = os.path.join(OUTPUT_DIR, "wind_farm_c_feature_matrix.parquet")

METOCEAN_COLS = [
    "hs", "tp", "wave_direction",
    "wind_speed_10m", "wind_direction_10m",
    "wind_speed_100m", "wind_direction_100m",
    "current_speed", "current_direction",
]

FEATURE_COLS = [
    "timestamp", "asset_id", "event_id", "event_label_care",
    *METOCEAN_COLS,
    "status_type_id", "label",
]


# ---------------------------------------------------------------------------
# Metocean loader
# ---------------------------------------------------------------------------

def load_metocean_index() -> pd.DataFrame | None:
    """
    Attempts to load metocean backbone CSVs from Data/Processed/metocean/.
    Returns a DataFrame indexed by timestamp, or None if no files found.
    """
    csv_files = glob(os.path.join(METOCEAN_DIR, "*.csv"))
    if not csv_files:
        print(f"  [INFO] No metocean CSV files found in {METOCEAN_DIR}. "
              "Metocean columns will be NaN.")
        return None

    parts = []
    for f in csv_files:
        try:
            df = pd.read_csv(f, parse_dates=["timestamp_10min"])
            parts.append(df)
        except Exception as e:
            print(f"  [WARN] Could not load {f}: {e}")

    if not parts:
        return None

    metocean = pd.concat(parts, ignore_index=True)
    metocean = metocean.drop_duplicates(subset=["timestamp_10min"])
    metocean = metocean.set_index("timestamp_10min").sort_index()
    avail = [c for c in METOCEAN_COLS if c in metocean.columns]
    print(f"  [INFO] Metocean index loaded: {len(metocean):,} rows, columns: {avail}")
    return metocean[avail] if avail else None


def lookup_metocean(metocean_index: pd.DataFrame | None,
                    timestamps: pd.DatetimeIndex) -> pd.DataFrame:
    """Nearest-neighbour metocean lookup (tolerance ±10 min)."""
    stub = pd.DataFrame(index=timestamps, columns=METOCEAN_COLS, dtype=float)
    if metocean_index is None:
        return stub

    tolerance = pd.Timedelta("10min")
    for ts in timestamps:
        pos = metocean_index.index.get_indexer([ts], method="nearest", tolerance=tolerance)
        if pos[0] != -1:
            stub.loc[ts] = metocean_index.iloc[pos[0]]
    return stub


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Build Wind Farm C production feature matrix."
    )
    parser.add_argument(
        "--max-events", type=int, default=None,
        help="Limit to first N events (default: all)."
    )
    args = parser.parse_args()

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # 1. Load event_info
    print(f"Loading event_info: {EVENT_INFO_PATH}")
    event_info = pd.read_csv(EVENT_INFO_PATH, sep=";")
    event_info.columns = [c.strip() for c in event_info.columns]
    event_info["event_start"] = pd.to_datetime(event_info["event_start"])
    event_info["event_end"]   = pd.to_datetime(event_info["event_end"])

    if args.max_events:
        event_info = event_info.head(args.max_events)
        print(f"Limiting to {len(event_info)} events (--max-events).")

    # 2. Load metocean index
    print(f"\nLoading metocean backbone from: {METOCEAN_DIR}")
    metocean_index = load_metocean_index()

    # 3. Init handshaker
    handshaker = SCADAHandshake(care_base_dir=CARE_BASE_DIR)

    # 4. Build feature rows
    all_rows = []
    skipped = 0

    for _, ev in event_info.iterrows():
        event_id   = int(ev["event_id"])
        asset_id   = int(ev["asset_id"])
        ev_label   = str(ev["event_label"])
        ev_start   = ev["event_start"]
        ev_end     = ev["event_end"]

        scada_path = os.path.join(WIND_FARM_C_DIR, "datasets", f"{event_id}.csv")
        if not os.path.exists(scada_path):
            print(f"  [SKIP] event_id={event_id} — no SCADA file")
            skipped += 1
            continue

        # Build 10-min backbone for this event window
        backbone_ts = pd.date_range(
            start=ev_start.floor("10min"),
            end=ev_end.ceil("10min"),
            freq="10min",
        )

        duration_min = (ev_end - ev_start).total_seconds() / 60.0

        # Construct pseudo-joined rows for handshake
        rows_for_handshake = pd.DataFrame({
            "timestamp_10min": backbone_ts,
            "wind_farm":       "Wind Farm C",
            "event_id":        float(event_id),
            "duration_min":    duration_min,
            "min_dist":        50.0,  # Conservative: vessel at foundation
        })

        labelled = handshaker.apply_handshake(rows_for_handshake)

        # Metocean join
        mo_df = lookup_metocean(metocean_index, pd.DatetimeIndex(backbone_ts))

        for i, ts in enumerate(backbone_ts):
            lr = labelled.iloc[i] if i < len(labelled) else None
            mr = mo_df.iloc[i] if i < len(mo_df) else None

            feature_row = {
                "timestamp":          ts,
                "asset_id":           asset_id,
                "event_id":           event_id,
                "event_label_care":   ev_label,
                "status_type_id":     lr["status_type_id"] if lr is not None else np.nan,
                "label":              lr["label"] if lr is not None else "unknown",
            }
            if mr is not None:
                for col in METOCEAN_COLS:
                    feature_row[col] = mr.get(col, np.nan)
            else:
                for col in METOCEAN_COLS:
                    feature_row[col] = np.nan

            all_rows.append(feature_row)

    if not all_rows:
        print("\n[ERROR] No feature rows produced. Check event_info and SCADA files.")
        sys.exit(1)

    # 5. Assemble final DataFrame
    feature_df = pd.DataFrame(all_rows)[FEATURE_COLS]
    feature_df["timestamp"] = pd.to_datetime(feature_df["timestamp"])
    feature_df = feature_df.sort_values(["event_id", "timestamp"]).reset_index(drop=True)

    # 6. Write Parquet
    feature_df.to_parquet(OUTPUT_PARQUET, index=False)
    print(f"\nFeature matrix written → {OUTPUT_PARQUET}")

    # 7. Summary
    total = len(feature_df)
    matched = feature_df["status_type_id"].notna().sum()
    label_dist = feature_df["label"].value_counts()

    print("\n" + "=" * 60)
    print("WIND FARM C FEATURE MATRIX — SUMMARY")
    print("=" * 60)
    print(f"  Events processed  : {len(event_info) - skipped}/{len(event_info)}")
    print(f"  Skipped (no SCADA): {skipped}")
    print(f"  Total rows        : {total:,}")
    print(f"  SCADA matched     : {matched:,} ({matched/total*100:.1f}%)")
    print(f"  Date span         : {feature_df['timestamp'].min()} → {feature_df['timestamp'].max()}")
    print(f"  Schema            : {list(feature_df.columns)}")
    print("\n  Label distribution:")
    for lbl, cnt in label_dist.items():
        print(f"    {lbl:<25} {cnt:>6}  ({cnt/total*100:.1f}%)")
    print("=" * 60)
    print(f"\n  Output: {OUTPUT_PARQUET}")


if __name__ == "__main__":
    main()
