"""
scripts/extract_wind_farm_c_metocean.py
----------------------------------------
CARE-specific Borkum metocean extractor.

Unlike scripts/extract_metocean.py (which is driven by AIS dwell events in
the DuckDB catalog), this script is driven directly by the CARE event_info.csv
time windows for Wind Farm C (Trianel Borkum I+II).

Why a separate script:
  The standard extractor calls get_connection() and queries dwell_events, which
  returns empty for Wind Farm C because no AIS dwell events have been cataloged
  yet. The CARE feature matrix is event-info driven, not AIS-dwell driven.

Approach:
  1. Read event_info.csv to determine the full date span: 2023-01-08 → 2024-01-06.
  2. Build a list of unique calendar months covering that span.
  3. For each month, fetch NORA3 wave, NORA3 wind, and CMEMS current data at
     the Borkum centroid (54.05N, 6.46E). Month-level caching is inherited from
     the underlying fetch functions, so re-runs are cheap.
  4. Merge wave + wind + current into a single hourly dataframe.
  5. Upscale to 10-minute resolution using MetoceanIngestor.upscale_to_10min().
  6. Clip to the exact CARE event span + 2h padding, deduplicate, and save.

Output:
  Data/Processed/metocean/wind_farm_c_borkum_metocean_10min.csv

Columns:
  timestamp_10min, lat, lon, hs, tp, wave_direction,
  wind_speed_10m, wind_direction_10m, wind_speed_100m, wind_direction_100m,
  current_speed, current_direction, source, interpolation_method

Usage:
  python scripts/extract_wind_farm_c_metocean.py [--dry-run]
"""

import os
import sys
import argparse
import pandas as pd
import numpy as np

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(PROJECT_ROOT, "src"))

from om_pipeline.ingestion.nora3 import fetch_nora3_point, fetch_nora3_wind
from om_pipeline.ingestion.cmems import fetch_cmems_current
from om_pipeline.ingestion.metocean import MetoceanIngestor

# ---------------------------------------------------------------------------
# Constants — Borkum centroid confirmed in ADR 003
# ---------------------------------------------------------------------------
BORKUM_LAT = 54.05
BORKUM_LON = 6.46

EVENT_INFO_PATH = os.path.join(
    PROJECT_ROOT, "Data", "CARE_To_Compare", "Wind Farm C", "event_info.csv"
)
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "Data", "Processed", "metocean")
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "wind_farm_c_borkum_metocean_10min.csv")

# 2-hour pad inherited from fetch functions; we clip to this for the final output
CLIP_PAD = pd.Timedelta("2h")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def iter_months(start: pd.Timestamp, end: pd.Timestamp):
    """Yield the first day of each calendar month in [start, end]."""
    current = start.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    while current <= end:
        yield current
        # Advance one month
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)


def month_end(month_start: pd.Timestamp) -> pd.Timestamp:
    """Return the last moment of a calendar month."""
    if month_start.month == 12:
        next_month = month_start.replace(year=month_start.year + 1, month=1)
    else:
        next_month = month_start.replace(month=month_start.month + 1)
    return next_month - pd.Timedelta(seconds=1)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Extract NORA3+CMEMS metocean backbone for Wind Farm C (Borkum) "
                    "from CARE event windows."
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print the planned fetch months and exit without downloading."
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Overwrite existing output file even if it already exists."
    )
    args = parser.parse_args()

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # 1. Load event_info to determine the full CARE date span
    print(f"Reading event_info: {EVENT_INFO_PATH}")
    event_info = pd.read_csv(EVENT_INFO_PATH, sep=";")
    event_info["event_start"] = pd.to_datetime(event_info["event_start"])
    event_info["event_end"]   = pd.to_datetime(event_info["event_end"])

    global_start = event_info["event_start"].min()
    global_end   = event_info["event_end"].max()
    print(f"CARE event span: {global_start} → {global_end}")

    # 2. Enumerate unique calendar months
    months = list(iter_months(global_start, global_end))
    print(f"Calendar months to fetch: {len(months)}")
    for m in months:
        print(f"  {m.strftime('%Y-%m')}")

    if args.dry_run:
        print("\n[DRY RUN] No data fetched. Exiting.")
        return

    # 3. Check if output already exists
    if os.path.exists(OUTPUT_FILE) and not args.force:
        print(f"\n[SKIP] Output already exists: {OUTPUT_FILE}")
        print("Re-run with --force to overwrite.")
        return

    # 4. Fetch per-month, merge, upscale
    ingestor = MetoceanIngestor()
    all_10min = []
    fetch_errors = []

    for month_start in months:
        m_end = month_end(month_start)
        label = month_start.strftime("%Y-%m")
        print(f"\n{'=' * 55}")
        print(f"Processing month: {label}")
        print(f"  Coordinate: {BORKUM_LAT}N, {BORKUM_LON}E")

        # 4a. NORA3 wave
        waves_df = fetch_nora3_point(
            lat=BORKUM_LAT,
            lon=BORKUM_LON,
            time_start=month_start,
            time_end=m_end,
        )

        if waves_df.empty:
            print(f"  [WARN] No NORA3 wave data for {label}. Skipping month.")
            fetch_errors.append(f"{label}: NORA3 wave empty")
            continue

        # Normalise column name if needed (thq → wave_direction)
        for alias in ["thq", "mwd", "dir"]:
            if alias in waves_df.columns and "wave_direction" not in waves_df.columns:
                waves_df = waves_df.rename(columns={alias: "wave_direction"})

        print(f"  NORA3 wave   : {len(waves_df)} hourly rows")

        # 4b. NORA3 wind
        wind_df = fetch_nora3_wind(
            lat=BORKUM_LAT,
            lon=BORKUM_LON,
            time_start=month_start,
            time_end=m_end,
        )
        print(f"  NORA3 wind   : {len(wind_df)} hourly rows" if not wind_df.empty
              else "  NORA3 wind   : [EMPTY — NaN columns will be used]")

        # 4c. CMEMS current
        current_df = fetch_cmems_current(
            lat=BORKUM_LAT,
            lon=BORKUM_LON,
            time_start=month_start,
            time_end=m_end,
        )
        print(f"  CMEMS current: {len(current_df)} hourly rows" if not current_df.empty
              else "  CMEMS current: [EMPTY/fallback — tidal climatology will be used]")

        # 4d. Merge on time
        merged = waves_df.copy()

        wind_cols = ["wind_speed_10m", "wind_direction_10m",
                     "wind_speed_100m", "wind_direction_100m"]
        if not wind_df.empty and all(c in wind_df.columns for c in wind_cols):
            merged = pd.merge(
                merged,
                wind_df[["time"] + wind_cols],
                on="time", how="left"
            )
        else:
            for c in wind_cols:
                merged[c] = np.nan

        current_cols = ["current_speed", "current_direction"]
        if not current_df.empty and all(c in current_df.columns for c in current_cols):
            merged = pd.merge(
                merged,
                current_df[["time"] + current_cols],
                on="time", how="left"
            )
        else:
            for c in current_cols:
                merged[c] = np.nan

        # 4e. Upscale to 10-minute
        upscaled = ingestor.upscale_to_10min(merged)

        if upscaled.empty:
            print(f"  [WARN] Upscaling produced empty result for {label}.")
            fetch_errors.append(f"{label}: upscale empty")
            continue

        # Rename 'time' → 'timestamp_10min' to match the pipeline schema
        if "time" in upscaled.columns:
            upscaled = upscaled.rename(columns={"time": "timestamp_10min"})

        upscaled["lat"]    = BORKUM_LAT
        upscaled["lon"]    = BORKUM_LON
        upscaled["source"] = "NORA3+CMEMS"
        upscaled["interpolation_method"] = "cubic_scalar+circular_vector"

        print(f"  → {len(upscaled)} rows at 10-min resolution")
        all_10min.append(upscaled)

    # 5. Combine all months
    if not all_10min:
        print("\n[ERROR] No metocean data produced. Check network/THREDDS availability.")
        if fetch_errors:
            print("Errors encountered:")
            for e in fetch_errors:
                print(f"  {e}")
        sys.exit(1)

    combined = pd.concat(all_10min, ignore_index=True)
    combined["timestamp_10min"] = pd.to_datetime(combined["timestamp_10min"])

    # 6. Clip to the exact CARE span (+ CLIP_PAD for boundary bracketing)
    clip_start = global_start - CLIP_PAD
    clip_end   = global_end   + CLIP_PAD
    combined = combined[
        (combined["timestamp_10min"] >= clip_start) &
        (combined["timestamp_10min"] <= clip_end)
    ].copy()

    # 7. Deduplicate (months may overlap at boundaries due to 2h padding)
    before_dedup = len(combined)
    combined = combined.drop_duplicates(subset=["timestamp_10min"]).sort_values("timestamp_10min")
    after_dedup = len(combined)
    if before_dedup != after_dedup:
        print(f"\nDeduplicated: {before_dedup} → {after_dedup} rows "
              f"({before_dedup - after_dedup} duplicates removed at month boundaries)")

    # 8. Write output
    combined.to_csv(OUTPUT_FILE, index=False)

    # 9. Summary
    metocean_cols = [
        "hs", "tp", "wave_direction",
        "wind_speed_10m", "wind_direction_10m",
        "wind_speed_100m", "wind_direction_100m",
        "current_speed", "current_direction",
    ]
    null_rates = {
        col: combined[col].isna().mean() * 100
        for col in metocean_cols
        if col in combined.columns
    }

    print(f"\n{'=' * 55}")
    print("WIND FARM C METOCEAN EXTRACTION — COMPLETE")
    print(f"{'=' * 55}")
    print(f"  Output file   : {OUTPUT_FILE}")
    print(f"  Total rows    : {len(combined):,}")
    print(f"  Date span     : {combined['timestamp_10min'].min()} → {combined['timestamp_10min'].max()}")
    print(f"  Months fetched: {len(months)} ({len(fetch_errors)} errors)")
    print("\n  Null rates by column:")
    for col, rate in null_rates.items():
        flag = " ⚠" if rate > 5 else ""
        print(f"    {col:<35} {rate:.1f}%{flag}")

    if fetch_errors:
        print("\n  Fetch errors (review manually):")
        for e in fetch_errors:
            print(f"    {e}")

    print(f"\n  Next: python scripts/build_wind_farm_c_feature_matrix.py")
    print(f"        (will now join metocean columns from {OUTPUT_FILE})")


if __name__ == "__main__":
    main()
