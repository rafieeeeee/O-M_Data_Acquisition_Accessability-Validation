"""
scripts/build_wind_farm_c_feature_matrix.py
--------------------------------------------
Builds the production feature matrix for Wind Farm C (Trianel Borkum I+II).

This script joins:
  - CARE Wind Farm C event_info (gives us event windows + asset IDs)
  - SCADA status lookup via SCADAHandshake (0-year shift, direct timestamps)
  - 10-minute metocean backbone from Data/Processed/metocean/
    Expected: wind_farm_c_borkum_metocean_10min.csv (produced by
    scripts/extract_wind_farm_c_metocean.py). Falls back to NaN stubs if absent.

Metocean join strategy:
  A single vectorised pd.merge_asof (tolerance = 10 min, direction = nearest)
  is used rather than per-row lookups. This keeps rebuild time in seconds.

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
    python scripts/build_wind_farm_c_feature_matrix.py [--max-events N] [--force]
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

# Preferred single-file output from extract_wind_farm_c_metocean.py
BORKUM_METOCEAN_FILE = os.path.join(
    METOCEAN_DIR, "wind_farm_c_borkum_metocean_10min.csv"
)


def load_metocean_df() -> pd.DataFrame | None:
    """
    Loads the Borkum metocean backbone into a DataFrame sorted by
    timestamp_10min, ready for merge_asof.

    Priority:
      1. wind_farm_c_borkum_metocean_10min.csv  (canonical output)
      2. Any other *.csv in Data/Processed/metocean/  (fallback glob)
    Returns None (with a warning) when no CSV is found.
    """
    # Priority 1: canonical file
    if os.path.exists(BORKUM_METOCEAN_FILE):
        print(f"  [INFO] Loading canonical metocean file: {BORKUM_METOCEAN_FILE}")
        df = pd.read_csv(BORKUM_METOCEAN_FILE, parse_dates=["timestamp_10min"])
    else:
        # Priority 2: any CSV in the metocean dir
        csv_files = glob(os.path.join(METOCEAN_DIR, "*.csv"))
        if not csv_files:
            print(f"  [WARN] No metocean CSV found in {METOCEAN_DIR}. "
                  "Run scripts/extract_wind_farm_c_metocean.py first. "
                  "Metocean columns will be NaN.")
            return None
        parts = []
        for f in csv_files:
            try:
                parts.append(pd.read_csv(f, parse_dates=["timestamp_10min"]))
            except Exception as e:
                print(f"  [WARN] Could not load {f}: {e}")
        if not parts:
            return None
        df = pd.concat(parts, ignore_index=True)

    avail = [c for c in METOCEAN_COLS if c in df.columns]
    if not avail:
        print("  [WARN] Metocean file has no recognised parameter columns. "
              "Check schema.")
        return None

    df = df.drop_duplicates(subset=["timestamp_10min"]).sort_values("timestamp_10min")
    df = df[["timestamp_10min"] + avail].reset_index(drop=True)
    print(f"  [INFO] Metocean loaded: {len(df):,} rows, columns: {avail}")
    return df


def join_metocean_vectorised(
    backbone_df: pd.DataFrame,
    metocean_df: pd.DataFrame | None,
) -> pd.DataFrame:
    """
    Vectorised nearest-neighbour metocean join using pd.merge_asof.
    Tolerance = 10 minutes. Falls back to NaN columns when metocean_df is None.

    IMPORTANT: Do NOT pre-seed NaN stubs for METOCEAN_COLS on backbone_df
    before calling merge_asof. If both sides carry the same column names,
    pandas produces _x/_y suffixed duplicates and the real values are lost.
    Instead, NaN stubs are added only AFTER the merge for any col still absent.
    """
    if metocean_df is None:
        # Pure fallback — add NaN stubs and return immediately
        for col in METOCEAN_COLS:
            if col not in backbone_df.columns:
                backbone_df[col] = np.nan
        return backbone_df

    mo = metocean_df.copy()
    mo["timestamp_10min"] = pd.to_datetime(mo["timestamp_10min"])
    backbone_df["timestamp"] = pd.to_datetime(backbone_df["timestamp"])

    avail_cols = [c for c in METOCEAN_COLS if c in mo.columns]

    # Strip any pre-existing metocean columns from the backbone before merging
    # to prevent _x/_y suffix collision in merge_asof.
    backbone_clean = backbone_df.drop(
        columns=[c for c in avail_cols if c in backbone_df.columns],
        errors="ignore",
    ).sort_values("timestamp")

    mo_sorted = mo[["timestamp_10min"] + avail_cols].sort_values("timestamp_10min")

    merged = pd.merge_asof(
        backbone_clean,
        mo_sorted,
        left_on="timestamp",
        right_on="timestamp_10min",
        direction="nearest",
        tolerance=pd.Timedelta("10min"),
    )

    # Drop the extra key column introduced by merge_asof
    if "timestamp_10min" in merged.columns:
        merged = merged.drop(columns=["timestamp_10min"])

    # Fallback NaN stubs for any metocean column not in the metocean file
    for col in METOCEAN_COLS:
        if col not in merged.columns:
            merged[col] = np.nan

    # Restore original sort order
    merged = merged.sort_values(["event_id", "timestamp"]).reset_index(drop=True)
    return merged


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
    parser.add_argument(
        "--force", action="store_true",
        help="Overwrite existing Parquet even if it exists."
    )
    args = parser.parse_args()

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    if os.path.exists(OUTPUT_PARQUET) and not args.force:
        print(f"[SKIP] Output already exists: {OUTPUT_PARQUET}")
        print("Re-run with --force to overwrite.")
        sys.exit(0)

    # 1. Load event_info
    print(f"Loading event_info: {EVENT_INFO_PATH}")
    event_info = pd.read_csv(EVENT_INFO_PATH, sep=";")
    event_info.columns = [c.strip() for c in event_info.columns]
    event_info["event_start"] = pd.to_datetime(event_info["event_start"])
    event_info["event_end"]   = pd.to_datetime(event_info["event_end"])

    if args.max_events:
        event_info = event_info.head(args.max_events)
        print(f"Limiting to {len(event_info)} events (--max-events).")

    # 2. Load metocean backbone (single vectorised join later)
    print(f"\nLoading metocean backbone from: {METOCEAN_DIR}")
    metocean_df = load_metocean_df()

    # 3. Init handshaker
    handshaker = SCADAHandshake(care_base_dir=CARE_BASE_DIR)

    # 4. Build SCADA-labelled backbone rows (batch — no metocean yet)
    all_backbone_rows = []
    skipped = 0

    for _, ev in event_info.iterrows():
        event_id  = int(ev["event_id"])
        asset_id  = int(ev["asset_id"])
        ev_label  = str(ev["event_label"])
        ev_start  = ev["event_start"]
        ev_end    = ev["event_end"]

        scada_path = os.path.join(WIND_FARM_C_DIR, "datasets", f"{event_id}.csv")
        if not os.path.exists(scada_path):
            print(f"  [SKIP] event_id={event_id} — no SCADA file")
            skipped += 1
            continue

        backbone_ts  = pd.date_range(
            start=ev_start.floor("10min"),
            end=ev_end.ceil("10min"),
            freq="10min",
        )
        duration_min = (ev_end - ev_start).total_seconds() / 60.0

        rows_for_handshake = pd.DataFrame({
            "timestamp_10min": backbone_ts,
            "wind_farm":       "Wind Farm C",
            "event_id":        float(event_id),
            "duration_min":    duration_min,
            "min_dist":        50.0,
        })

        labelled = handshaker.apply_handshake(rows_for_handshake)

        labelled["timestamp"]        = backbone_ts
        labelled["asset_id"]         = asset_id
        labelled["event_label_care"] = ev_label
        all_backbone_rows.append(labelled)

    if not all_backbone_rows:
        print("\n[ERROR] No backbone rows produced. Check event_info and SCADA files.")
        sys.exit(1)

    # 5. Concatenate backbone
    backbone_df = pd.concat(all_backbone_rows, ignore_index=True)
    backbone_df["event_id"] = backbone_df["event_id"].astype(int)
    backbone_df["timestamp"] = pd.to_datetime(backbone_df["timestamp"])

    # 6. Vectorised metocean join
    print(f"\nJoining metocean ({len(backbone_df):,} backbone rows)...")
    feature_df = join_metocean_vectorised(backbone_df, metocean_df)

    # 7. Select and order final columns
    final_cols = [c for c in FEATURE_COLS if c in feature_df.columns]
    feature_df = feature_df[final_cols].sort_values(
        ["event_id", "timestamp"]
    ).reset_index(drop=True)

    # 8. Write Parquet
    feature_df.to_parquet(OUTPUT_PARQUET, index=False)
    print(f"\nFeature matrix written → {OUTPUT_PARQUET}")

    # 9. QA summary
    total   = len(feature_df)
    matched = feature_df["status_type_id"].notna().sum()
    label_dist = feature_df["label"].value_counts()

    metocean_null_rates = {
        col: feature_df[col].isna().mean() * 100
        for col in METOCEAN_COLS
        if col in feature_df.columns
    }

    print("\n" + "=" * 60)
    print("WIND FARM C FEATURE MATRIX — SUMMARY")
    print("=" * 60)
    print(f"  Events processed  : {len(event_info) - skipped}/{len(event_info)}")
    print(f"  Skipped (no SCADA): {skipped}")
    print(f"  Total rows        : {total:,}")
    print(f"  SCADA matched     : {matched:,} ({matched/total*100:.1f}%)")
    print(f"  Date span         : {feature_df['timestamp'].min()} → "
          f"{feature_df['timestamp'].max()}")
    print(f"  Schema            : {list(feature_df.columns)}")
    print("\n  Label distribution:")
    for lbl, cnt in label_dist.items():
        print(f"    {lbl:<25} {cnt:>6}  ({cnt/total*100:.1f}%)")
    print("\n  Metocean null rates:")
    for col, rate in metocean_null_rates.items():
        flag = " ⚠  (run extract_wind_farm_c_metocean.py)" if rate > 95 else ""
        print(f"    {col:<35} {rate:.1f}%{flag}")
    print("=" * 60)
    print(f"\n  Output: {OUTPUT_PARQUET}")


if __name__ == "__main__":
    main()
