"""Continuous time-lagged cross-correlation for CAREtoCompare Wind Farm C."""

from __future__ import annotations

import argparse
import re
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from om_pipeline.analysis.research.care_wind_farm_confirmation import (
    WIND_COLUMNS,
    build_hourly_scada,
    load_wind_farm_c_scada,
)
from om_pipeline.ingestion.nora3 import fetch_nora3_wind


@dataclass(frozen=True)
class TlccResult:
    scada_column: str
    lag_hours: int
    overlap_hours: int
    pearson_r: float
    scada_start: pd.Timestamp
    scada_end: pd.Timestamp

    @property
    def lag(self) -> pd.Timedelta:
        return pd.Timedelta(hours=self.lag_hours)

    @property
    def true_start(self) -> pd.Timestamp:
        return self.scada_start - self.lag

    @property
    def true_end(self) -> pd.Timestamp:
        return self.scada_end - self.lag


def fetch_missing_nora_months(
    lat: float,
    lon: float,
    start: str,
    end: str,
    *,
    nora_dir: Path,
) -> None:
    wanted_months = pd.period_range(start=start, end=end, freq="M")
    for month in wanted_months:
        cache_path = nora_dir / f"nora3_wind_raw_{lat:.2f}_{lon:.2f}_{month.year}_{month.month:02d}.csv"
        if cache_path.exists():
            continue
        month_start = month.to_timestamp()
        month_end = (month + 1).to_timestamp() - pd.Timedelta(hours=1)
        fetch_nora3_wind(lat, lon, month_start, month_end)


def load_nora_multi_year(nora_dir: Path, lat: float, lon: float) -> pd.Series:
    pattern = re.compile(
        rf"nora3_wind_raw_{lat:.2f}_{lon:.2f}_(\d{{4}})_(\d{{2}})\.csv"
    )
    frames = []
    for path in sorted(nora_dir.glob(f"nora3_wind_raw_{lat:.2f}_{lon:.2f}_*.csv")):
        if not pattern.match(path.name):
            continue
        frame = pd.read_csv(path, parse_dates=["time"])
        frames.append(frame[["time", "wind_speed_100m"]])
    if not frames:
        return pd.Series(dtype=float)
    data = (
        pd.concat(frames, ignore_index=True)
        .drop_duplicates("time")
        .sort_values("time")
        .set_index("time")["wind_speed_100m"]
    )
    return data.resample("1h").mean().dropna()


def continuous_lag_scan(
    scada_hourly: pd.Series,
    nora_hourly: pd.Series,
    *,
    min_overlap_hours: int,
    top_n: int,
) -> pd.DataFrame:
    scada_start = scada_hourly.index.min()
    scada_end = scada_hourly.index.max()
    nora_start = nora_hourly.index.min()
    nora_end = nora_hourly.index.max()

    min_lag = int(np.floor((scada_start - nora_end) / pd.Timedelta(hours=1)))
    max_lag = int(np.ceil((scada_end - nora_start) / pd.Timedelta(hours=1)))
    rows = []

    scada_frame = scada_hourly.rename("scada_wind_speed").to_frame()
    for lag_hours in range(min_lag, max_lag + 1):
        shifted_nora = nora_hourly.copy()
        shifted_nora.index = shifted_nora.index + pd.Timedelta(hours=lag_hours)
        joined = scada_frame.join(shifted_nora.rename("nora_wind_speed"), how="inner").dropna()
        if len(joined) < min_overlap_hours:
            continue
        corr = joined["scada_wind_speed"].corr(joined["nora_wind_speed"])
        if np.isfinite(corr):
            rows.append(
                {
                    "lag_hours": lag_hours,
                    "overlap_hours": len(joined),
                    "pearson_r": float(corr),
                    "lag_days": lag_hours / 24,
                }
            )
    ranking = pd.DataFrame(rows).sort_values("pearson_r", ascending=False)
    ranking["rank"] = np.arange(1, len(ranking) + 1)
    return ranking.head(top_n)


def describe_lag(lag: pd.Timedelta) -> str:
    sign = "-" if lag < pd.Timedelta(0) else ""
    total_hours = abs(int(lag / pd.Timedelta(hours=1)))
    years = total_hours // (365 * 24)
    rem = total_hours % (365 * 24)
    days = rem // 24
    hours = rem % 24
    return f"{sign}{years} years, {days} days, {hours} hours"


def raw_ais_availability(raw_ais_dir: Path, true_start: pd.Timestamp, true_end: pd.Timestamp) -> pd.DataFrame:
    months = pd.period_range(true_start.to_period("M"), true_end.to_period("M"), freq="M")
    files = [path.name for path in raw_ais_dir.glob("*.csv")]
    rows = []
    for month in months:
        token = f"{month.year}_{month.month:02d}"
        matching = [name for name in files if token in name]
        rows.append(
            {
                "month": str(month),
                "raw_ais_files": ";".join(sorted(matching)),
                "has_farm_candidate_slice": any("Farm-Candidates" in name for name in matching),
                "has_german_bight_or_north_sea_slice": any(
                    ("German-Bight" in name or "German_North_Sea" in name) for name in matching
                ),
            }
        )
    return pd.DataFrame(rows)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--care-dir", default="Data/CARE_To_Compare")
    parser.add_argument("--nora-dir", default="Data/Raw/Metocean/NORA3")
    parser.add_argument("--raw-ais-dir", default="Data/Raw/AIS")
    parser.add_argument("--lat", type=float, default=54.05)
    parser.add_argument("--lon", type=float, default=6.46)
    parser.add_argument("--fetch-start", default="2015-01")
    parser.add_argument("--fetch-end", default="2023-12")
    parser.add_argument("--fetch-missing", action="store_true")
    parser.add_argument("--min-overlap-hours", type=int, default=24 * 21)
    parser.add_argument("--top-n", type=int, default=100)
    parser.add_argument("--out-dir", default="reports/care_wind_farm_c_confirmation")
    args = parser.parse_args()

    nora_dir = Path(args.nora_dir)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    if args.fetch_missing:
        fetch_missing_nora_months(
            args.lat,
            args.lon,
            args.fetch_start,
            args.fetch_end,
            nora_dir=nora_dir,
        )

    scada, _, _ = load_wind_farm_c_scada(Path(args.care_dir))
    nora_hourly = load_nora_multi_year(nora_dir, args.lat, args.lon)
    if nora_hourly.empty:
        raise SystemExit("No local NORA3 wind cache found for requested coordinate.")

    all_rankings = []
    best_results = []
    for column in WIND_COLUMNS:
        scada_hourly = build_hourly_scada(scada, column)
        ranking = continuous_lag_scan(
            scada_hourly,
            nora_hourly,
            min_overlap_hours=args.min_overlap_hours,
            top_n=args.top_n,
        )
        ranking.insert(0, "scada_column", column)
        all_rankings.append(ranking)
        if not ranking.empty:
            best = ranking.iloc[0]
            best_results.append(
                TlccResult(
                    scada_column=column,
                    lag_hours=int(best["lag_hours"]),
                    overlap_hours=int(best["overlap_hours"]),
                    pearson_r=float(best["pearson_r"]),
                    scada_start=scada_hourly.index.min(),
                    scada_end=scada_hourly.index.max(),
                )
            )

    ranking_table = pd.concat(all_rankings, ignore_index=True)
    ranking_table.to_csv(out_dir / "tlcc_borkum_hourly_lag_ranking.csv", index=False)

    summary_rows = []
    for result in sorted(best_results, key=lambda r: r.pearson_r, reverse=True):
        summary_rows.append(
            {
                "scada_column": result.scada_column,
                "lag_hours": result.lag_hours,
                "lag_description_365d_years": describe_lag(result.lag),
                "overlap_hours": result.overlap_hours,
                "pearson_r": result.pearson_r,
                "scada_start": result.scada_start,
                "scada_end": result.scada_end,
                "derived_true_start": result.true_start,
                "derived_true_end": result.true_end,
            }
        )
    summary = pd.DataFrame(summary_rows)
    summary.to_csv(out_dir / "tlcc_borkum_summary.csv", index=False)

    if not best_results:
        raise SystemExit("No lag met the minimum-overlap requirement.")
    best_result = max(best_results, key=lambda r: r.pearson_r)
    ais = raw_ais_availability(Path(args.raw_ais_dir), best_result.true_start, best_result.true_end)
    ais.to_csv(out_dir / "tlcc_raw_ais_availability.csv", index=False)

    print("TLCC Borkum result")
    print(summary.to_string(index=False))
    print("\nRaw AIS availability for best true window")
    print(ais.to_string(index=False, max_colwidth=100))


if __name__ == "__main__":
    main()
