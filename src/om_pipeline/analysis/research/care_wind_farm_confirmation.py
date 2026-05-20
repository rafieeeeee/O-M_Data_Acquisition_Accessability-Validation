"""Reproduce CAREtoCompare Wind Farm C de-anonymization checks.

This module is intentionally research-oriented: it writes compact CSV evidence
tables for the methodology in docs/care-wind-farm-confirmation-methodology.md.
"""

from __future__ import annotations

import argparse
import math
import re
import zipfile
from dataclasses import dataclass
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd


SCADA_COLUMNS = [
    "time_stamp",
    "asset_id",
    "status_type_id",
    "train_test",
    "wind_speed_235_avg",
    "wind_speed_236_avg",
    "wind_speed_237_avg",
    "power_6_avg",
    "sensor_123_avg",
    "sensor_124_avg",
    "sensor_125_avg",
    "sensor_126_avg",
]

WIND_COLUMNS = ["wind_speed_236_avg", "wind_speed_237_avg"]
RELATIVE_WIND_COLUMNS = [
    "sensor_123_avg",
    "sensor_124_avg",
    "sensor_125_avg",
    "sensor_126_avg",
]


@dataclass(frozen=True)
class Candidate:
    name: str
    lat: float
    lon: float


def _read_registry(registry_path: Path) -> pd.DataFrame:
    registry = pd.read_csv(registry_path)
    if len(registry.columns) == 1:
        registry = pd.read_csv(registry_path, sep=",")
    registry["latitude"] = pd.to_numeric(registry["latitude"], errors="coerce")
    registry["longitude"] = pd.to_numeric(registry["longitude"], errors="coerce")
    return registry


def _nearest_farm_name(lat: float, lon: float, registry: pd.DataFrame) -> str:
    centroids = (
        registry.groupby("wind_farm")
        .agg(lat=("latitude", "mean"), lon=("longitude", "mean"))
        .dropna()
        .reset_index()
    )
    distances = (centroids["lat"] - lat) ** 2 + (centroids["lon"] - lon) ** 2
    return str(centroids.loc[distances.idxmin(), "wind_farm"])


def discover_nora_candidates(nora_dir: Path, registry: pd.DataFrame) -> list[Candidate]:
    pattern = re.compile(
        r"nora3_wind_raw_(-?\d+\.\d+)_(-?\d+\.\d+)_(\d{4})_(\d{2})\.csv"
    )
    coords: set[tuple[float, float]] = set()
    for path in nora_dir.glob("nora3_wind_raw_*.csv"):
        match = pattern.match(path.name)
        if not match:
            continue
        coords.add((float(match.group(1)), float(match.group(2))))

    candidates = []
    seen_names: dict[str, int] = {}
    for lat, lon in sorted(coords):
        name = _nearest_farm_name(lat, lon, registry)
        seen_names[name] = seen_names.get(name, 0) + 1
        if seen_names[name] > 1:
            name = f"{name} ({lat:.2f},{lon:.2f})"
        candidates.append(Candidate(name=name, lat=lat, lon=lon))
    return candidates


def load_wind_farm_c_scada(care_dir: Path) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    dataset_dir = care_dir / "Wind Farm C" / "datasets"
    inventory_rows = []
    frames = []
    for path in sorted(dataset_dir.glob("*.csv"), key=lambda p: int(p.stem)):
        frame = pd.read_csv(path, sep=";", usecols=SCADA_COLUMNS)
        frame["time_stamp"] = pd.to_datetime(frame["time_stamp"])
        frame["event_file"] = path.name
        frames.append(frame)
        inventory_rows.append(
            {
                "event_file": path.name,
                "rows": len(frame),
                "asset_id": int(frame["asset_id"].iloc[0]),
                "first_timestamp": frame["time_stamp"].min(),
                "last_timestamp": frame["time_stamp"].max(),
                "wind_speed_columns": ",".join(
                    col for col in WIND_COLUMNS if col in frame.columns
                ),
                "power_columns": "power_6_avg",
                "relative_wind_columns": ",".join(RELATIVE_WIND_COLUMNS),
                "power_6_min": frame["power_6_avg"].min(),
                "power_6_max": frame["power_6_avg"].max(),
                "wind_speed_236_min": frame["wind_speed_236_avg"].min(),
                "wind_speed_236_max": frame["wind_speed_236_avg"].max(),
            }
        )

    raw = pd.concat(frames, ignore_index=True)
    grouped = (
        raw.groupby(["time_stamp", "asset_id"], as_index=False)
        .agg(
            {
                "status_type_id": "median",
                "wind_speed_235_avg": "mean",
                "wind_speed_236_avg": "mean",
                "wind_speed_237_avg": "mean",
                "power_6_avg": "mean",
                "sensor_123_avg": "mean",
                "sensor_124_avg": "mean",
                "sensor_125_avg": "mean",
                "sensor_126_avg": "mean",
            }
        )
        .sort_values(["asset_id", "time_stamp"])
    )
    inventory = pd.DataFrame(inventory_rows)
    asset_coverage = (
        inventory.groupby("asset_id")
        .agg(
            files=("event_file", "count"),
            rows=("rows", "sum"),
            first_timestamp=("first_timestamp", "min"),
            last_timestamp=("last_timestamp", "max"),
        )
        .reset_index()
    )
    return grouped, inventory, asset_coverage


def build_hourly_scada(scada: pd.DataFrame, wind_col: str) -> pd.Series:
    valid = scada[["time_stamp", wind_col]].copy()
    valid = valid[(valid[wind_col] >= 0.0) & (valid[wind_col] <= 45.0)]
    series = valid.groupby("time_stamp")[wind_col].mean().sort_index()
    return series.resample("1h").mean().dropna()


def load_nora_series(nora_dir: Path, candidate: Candidate) -> pd.DataFrame:
    files = sorted(
        nora_dir.glob(f"nora3_wind_raw_{candidate.lat:.2f}_{candidate.lon:.2f}_*.csv")
    )
    if not files:
        return pd.DataFrame()
    frames = []
    for path in files:
        frame = pd.read_csv(path)
        frame["time"] = pd.to_datetime(frame["time"])
        frames.append(frame)
    return (
        pd.concat(frames, ignore_index=True)
        .drop_duplicates("time")
        .sort_values("time")
        .set_index("time")
    )


def nora_cache_coverage(candidates: list[Candidate], nora_dir: Path) -> pd.DataFrame:
    rows = []
    for candidate in candidates:
        files = sorted(
            nora_dir.glob(f"nora3_wind_raw_{candidate.lat:.2f}_{candidate.lon:.2f}_*.csv")
        )
        months = []
        for path in files:
            match = re.search(r"_(\d{4})_(\d{2})\.csv$", path.name)
            if match:
                months.append(f"{match.group(1)}-{match.group(2)}")
        rows.append(
            {
                "candidate": candidate.name,
                "lat": candidate.lat,
                "lon": candidate.lon,
                "cached_months": ",".join(sorted(set(months))),
                "cached_month_count": len(set(months)),
            }
        )
    return pd.DataFrame(rows)


def pearson_join(
    scada_hourly: pd.Series,
    nora: pd.DataFrame,
    year_shift: int,
) -> tuple[int, float]:
    if nora.empty:
        return 0, np.nan
    shifted = scada_hourly.copy()
    shifted.index = shifted.index - pd.DateOffset(years=year_shift)
    joined = pd.concat(
        [shifted.rename("scada_wind_speed"), nora["wind_speed_100m"]],
        axis=1,
        join="inner",
    ).dropna()
    if len(joined) < 48:
        return len(joined), np.nan
    return len(joined), float(joined["scada_wind_speed"].corr(joined["wind_speed_100m"]))


def correlation_scan(
    scada: pd.DataFrame,
    candidates: list[Candidate],
    nora_dir: Path,
    shifts: range,
) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
    nora_cache = {c.name: load_nora_series(nora_dir, c) for c in candidates}
    rows = []
    for wind_col in WIND_COLUMNS:
        scada_hourly = build_hourly_scada(scada, wind_col)
        for candidate in candidates:
            nora = nora_cache[candidate.name]
            for shift in shifts:
                overlap, corr = pearson_join(scada_hourly, nora, shift)
                rows.append(
                    {
                        "candidate": candidate.name,
                        "lat": candidate.lat,
                        "lon": candidate.lon,
                        "scada_column": wind_col,
                        "year_shift": shift,
                        "overlap_hours": overlap,
                        "pearson_r": corr,
                    }
                )
    ranking = pd.DataFrame(rows)
    ranking = ranking.sort_values(
        ["scada_column", "pearson_r", "overlap_hours"],
        ascending=[True, False, False],
        na_position="last",
    )
    ranking["rank"] = ranking.groupby("scada_column")["pearson_r"].rank(
        method="first", ascending=False
    )
    return ranking, nora_cache


def monthly_window_rankings(
    scada: pd.DataFrame,
    candidates: list[Candidate],
    nora_cache: dict[str, pd.DataFrame],
    year_shift: int,
    wind_col: str,
) -> pd.DataFrame:
    scada_hourly = build_hourly_scada(scada, wind_col)
    shifted = scada_hourly.copy()
    shifted.index = shifted.index - pd.DateOffset(years=year_shift)
    shifted = shifted.rename("scada_wind_speed")
    rows = []
    for candidate in candidates:
        nora = nora_cache[candidate.name]
        if nora.empty:
            continue
        joined = pd.concat([shifted, nora["wind_speed_100m"]], axis=1, join="inner").dropna()
        if joined.empty:
            continue
        for month, window in joined.groupby(pd.Grouper(freq="MS")):
            if len(window) < 48:
                continue
            rows.append(
                {
                    "month": month.date().isoformat(),
                    "candidate": candidate.name,
                    "lat": candidate.lat,
                    "lon": candidate.lon,
                    "scada_column": wind_col,
                    "year_shift": year_shift,
                    "overlap_hours": len(window),
                    "pearson_r": float(
                        window["scada_wind_speed"].corr(window["wind_speed_100m"])
                    ),
                }
            )
    ranking = pd.DataFrame(rows)
    if ranking.empty:
        return ranking
    ranking["rank"] = ranking.groupby(["month", "scada_column"])["pearson_r"].rank(
        method="first", ascending=False
    )
    return ranking.sort_values(["month", "rank", "candidate"])


def bootstrap_win_rates(
    window_rankings: pd.DataFrame, *, iterations: int = 5000, seed: int = 1729
) -> pd.DataFrame:
    if window_rankings.empty:
        return pd.DataFrame()
    best_per_month = window_rankings.sort_values("pearson_r", ascending=False).groupby(
        "month", as_index=False
    ).first()
    months = best_per_month["month"].to_numpy()
    winners = dict(zip(best_per_month["month"], best_per_month["candidate"]))
    rng = np.random.default_rng(seed)
    candidates = sorted(window_rankings["candidate"].unique())
    counts = {candidate: 0 for candidate in candidates}
    for _ in range(iterations):
        sample = rng.choice(months, size=len(months), replace=True)
        values, value_counts = np.unique([winners[month] for month in sample], return_counts=True)
        winner = values[value_counts.argmax()]
        counts[str(winner)] += 1
    return pd.DataFrame(
        {
            "candidate": list(counts.keys()),
            "bootstrap_win_rate": [counts[c] / iterations for c in counts],
            "windows": len(months),
            "iterations": iterations,
            "seed": seed,
        }
    ).sort_values("bootstrap_win_rate", ascending=False)


def load_power_curve(curve_zip: Path, curve_name: str) -> pd.DataFrame:
    with zipfile.ZipFile(curve_zip) as archive:
        with archive.open(f"power_curves/{curve_name}.csv") as handle:
            curve = pd.read_csv(handle)
    curve["power_norm"] = curve["power"] / curve["power"].max()
    return curve


def _fit_curve_metrics(binned: pd.DataFrame, curve: pd.DataFrame) -> dict[str, float]:
    predicted = np.interp(binned["wind_speed_mid"], curve["ws"], curve["power_norm"])
    observed = binned["power_norm"]
    residual = observed - predicted
    ss_res = float(np.sum(residual**2))
    ss_tot = float(np.sum((observed - observed.mean()) ** 2))
    r2 = np.nan if ss_tot == 0 else 1 - ss_res / ss_tot
    rated_bins = binned.loc[binned["power_norm"] >= 0.95, "wind_speed_mid"]
    return {
        "binned_mae": float(np.mean(np.abs(residual))),
        "rmse": float(np.sqrt(np.mean(residual**2))),
        "r2": float(r2),
        "inferred_rated_wind_speed": float(rated_bins.min()) if not rated_bins.empty else np.nan,
        "plateau_power_mean": float(
            binned.loc[binned["wind_speed_mid"].between(14, 25), "power_norm"].mean()
        ),
    }


def turbine_fingerprint(scada: pd.DataFrame, curve_zip: Path) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    curves = {
        "M5000-116_5MW": load_power_curve(curve_zip, "M5000-116"),
        "Senvion_6.2M152_6.15MW": load_power_curve(curve_zip, "6.2M152"),
    }
    rows = []
    binned_rows = []
    valid = scada[
        (scada["status_type_id"].isin([0, 1]))
        & (scada["wind_speed_236_avg"].between(0, 35))
        & (scada["power_6_avg"].between(-0.05, 1.2))
    ].copy()
    valid["power_norm"] = valid["power_6_avg"].clip(0, 1.05)
    valid["wind_bin"] = (valid["wind_speed_236_avg"] * 2).round() / 2.0
    for asset_id, asset in valid.groupby("asset_id"):
        binned = (
            asset.groupby("wind_bin")
            .agg(
                wind_speed_mid=("wind_bin", "mean"),
                power_norm=("power_norm", "mean"),
                samples=("power_norm", "count"),
            )
            .reset_index(drop=True)
        )
        binned = binned[(binned["samples"] >= 30) & binned["wind_speed_mid"].between(4, 25)]
        if len(binned) < 12:
            continue
        for _, row in binned.iterrows():
            binned_rows.append(
                {
                    "asset_id": asset_id,
                    "wind_speed_mid": row["wind_speed_mid"],
                    "power_norm": row["power_norm"],
                    "samples": row["samples"],
                }
            )
        for curve_name, curve in curves.items():
            metrics = _fit_curve_metrics(binned, curve)
            metrics.update(
                {
                    "asset_id": asset_id,
                    "curve": curve_name,
                    "samples": int(asset["power_norm"].notna().sum()),
                    "power_min": float(asset["power_6_avg"].min()),
                    "power_max": float(asset["power_6_avg"].max()),
                }
            )
            rows.append(metrics)
    asset_fits = pd.DataFrame(rows)
    if not asset_fits.empty:
        asset_fits["preferred_curve"] = asset_fits.loc[
            asset_fits.groupby("asset_id")["binned_mae"].idxmin(), ["asset_id", "curve"]
        ].set_index("asset_id")["curve"].reindex(asset_fits["asset_id"]).to_numpy()
    farm_binned = (
        pd.DataFrame(binned_rows)
        .groupby("wind_speed_mid")
        .agg(power_norm=("power_norm", "mean"), samples=("samples", "sum"))
        .reset_index()
    )
    farm_rows = []
    for curve_name, curve in curves.items():
        metrics = _fit_curve_metrics(farm_binned, curve)
        metrics.update({"curve": curve_name, "assets": scada["asset_id"].nunique()})
        farm_rows.append(metrics)
    return asset_fits, pd.DataFrame(farm_rows), pd.DataFrame(binned_rows)


def directional_checks(
    scada: pd.DataFrame,
    candidates: list[Candidate],
    nora_cache: dict[str, pd.DataFrame],
    year_shift: int,
    wind_col: str,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    sectors = ["N", "NE", "E", "SE", "S", "SW", "W", "NW"]
    rose_rows = []
    sector_rows = []
    scada_hourly = build_hourly_scada(scada, wind_col)
    shifted = scada_hourly.copy()
    shifted.index = shifted.index - pd.DateOffset(years=year_shift)
    shifted = shifted.rename("scada_wind_speed")
    for candidate in candidates:
        nora = nora_cache[candidate.name]
        if nora.empty:
            continue
        nora = nora.copy()
        nora["sector"] = pd.cut(
            ((nora["wind_direction_100m"] + 22.5) % 360),
            bins=[0, 45, 90, 135, 180, 225, 270, 315, 360],
            labels=sectors,
            include_lowest=True,
        )
        rose = nora["sector"].value_counts(normalize=True).reindex(sectors).fillna(0)
        for sector, share in rose.items():
            rose_rows.append(
                {
                    "candidate": candidate.name,
                    "lat": candidate.lat,
                    "lon": candidate.lon,
                    "sector": sector,
                    "share": float(share),
                }
            )
        joined = pd.concat(
            [shifted, nora[["wind_speed_100m", "sector"]]], axis=1, join="inner"
        ).dropna()
        for sector, window in joined.groupby("sector", observed=True):
            if len(window) < 24:
                corr = np.nan
            else:
                corr = float(window["scada_wind_speed"].corr(window["wind_speed_100m"]))
            sector_rows.append(
                {
                    "candidate": candidate.name,
                    "sector": sector,
                    "overlap_hours": len(window),
                    "pearson_r": corr,
                }
            )

    rel_rows = []
    for column in RELATIVE_WIND_COLUMNS:
        values = scada[column].dropna()
        angles = np.deg2rad(values % 360)
        sin_mean = np.sin(angles).mean()
        cos_mean = np.cos(angles).mean()
        resultant = math.sqrt(sin_mean**2 + cos_mean**2)
        rel_rows.append(
            {
                "column": column,
                "samples": int(values.size),
                "ordinary_mean_deg": float(values.mean()),
                "ordinary_std_deg": float(values.std()),
                "circular_mean_deg": float((math.degrees(math.atan2(sin_mean, cos_mean)) + 360) % 360),
                "circular_std_deg": float(math.degrees(math.sqrt(-2 * math.log(max(resultant, 1e-12))))),
            }
        )
    return pd.DataFrame(rose_rows), pd.DataFrame(rel_rows), pd.DataFrame(sector_rows)


def registry_crosscheck(registry: pd.DataFrame) -> pd.DataFrame:
    farms = ["Trianel Windpark Borkum 1", "Trianel Windpark Borkum 2"]
    subset = registry[registry["wind_farm"].isin(farms)].copy()
    return (
        subset.groupby("wind_farm")
        .agg(
            turbine_count=("wind_farm", "size"),
            manufacturer=("oem_manufacturer", lambda s: "; ".join(sorted(s.dropna().unique()))),
            turbine_type=("turbine_type", lambda s: "; ".join(sorted(s.dropna().unique()))),
            rated_power=("rated_power", lambda s: "; ".join(map(str, sorted(s.dropna().unique())))),
            rotor_diameter=("rotor_diameter", lambda s: "; ".join(map(str, sorted(s.dropna().unique())))),
            commissioning_date=("commissioning_date", lambda s: "; ".join(sorted(s.dropna().unique()))),
            centroid_lat=("latitude", "mean"),
            centroid_lon=("longitude", "mean"),
        )
        .reset_index()
    )


def ais_scada_cooccurrence(
    db_path: Path,
    scada: pd.DataFrame,
    controls: list[str],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    con = duckdb.connect(str(db_path), read_only=True)
    farms = ["Trianel Windpark Borkum 1", "Trianel Windpark Borkum 2"] + controls
    events = con.sql(
        """
        select MMSI, Name, "Ship type" as ship_type, wind_farm, event_id, start, "end" as event_end,
               duration_min, length, min_dist
        from dwell_events
        where wind_farm in ({})
          and start >= timestamp '2021-01-01'
          and start < timestamp '2023-02-01'
        """.format(",".join(["?"] * len(farms))),
        params=farms,
    ).fetchdf()
    if events.empty:
        return events, pd.DataFrame()

    scada_status = scada[["time_stamp", "status_type_id", "power_6_avg", "wind_speed_236_avg"]].copy()
    scada_status = (
        scada_status.groupby("time_stamp")
        .agg(
            status_mode=("status_type_id", lambda s: s.mode().iloc[0] if not s.mode().empty else np.nan),
            service_share=("status_type_id", lambda s: float((s == 3).mean())),
            downtime_share=("status_type_id", lambda s: float((s == 4).mean())),
            avg_power=("power_6_avg", "mean"),
            avg_wind_speed=("wind_speed_236_avg", "mean"),
        )
        .reset_index()
        .sort_values("time_stamp")
    )
    rows = []
    for _, event in events.iterrows():
        lookup = pd.Timestamp(event["start"]) + pd.DateOffset(years=1)
        nearest_pos = scada_status["time_stamp"].searchsorted(lookup)
        candidates = []
        for pos in [nearest_pos - 1, nearest_pos]:
            if 0 <= pos < len(scada_status):
                candidates.append(scada_status.iloc[pos])
        if candidates:
            nearest = min(candidates, key=lambda row: abs(row["time_stamp"] - lookup))
            delta_min = abs((nearest["time_stamp"] - lookup).total_seconds()) / 60.0
            if delta_min <= 10:
                status_mode = nearest["status_mode"]
                service_share = nearest["service_share"]
                downtime_share = nearest["downtime_share"]
                avg_power = nearest["avg_power"]
                avg_wind = nearest["avg_wind_speed"]
                nearest_scada_timestamp = nearest["time_stamp"]
            else:
                status_mode = np.nan
                service_share = np.nan
                downtime_share = np.nan
                avg_power = np.nan
                avg_wind = np.nan
                nearest_scada_timestamp = pd.NaT
        else:
            status_mode = np.nan
            service_share = np.nan
            downtime_share = np.nan
            avg_power = np.nan
            avg_wind = np.nan
            nearest_scada_timestamp = pd.NaT
        rows.append(
            {
                "wind_farm": event["wind_farm"],
                "event_id": event["event_id"],
                "MMSI": event["MMSI"],
                "Name": event["Name"],
                "start": event["start"],
                "end": event["event_end"],
                "duration_min": event["duration_min"],
                "length": event["length"],
                "nearest_scada_timestamp": nearest_scada_timestamp,
                "status_mode": status_mode,
                "service_share": service_share,
                "downtime_share": downtime_share,
                "avg_power": avg_power,
                "avg_wind_speed": avg_wind,
            }
        )
    result = pd.DataFrame(rows)
    summary = (
        result.groupby("wind_farm")
        .agg(
            events=("event_id", "count"),
            matched_scada_windows=("nearest_scada_timestamp", lambda s: int(s.notna().sum())),
            service_or_downtime_matches=(
                "status_mode",
                lambda s: int(s.isin([3, 4]).sum()),
            ),
            large_vessel_events=("length", lambda s: int((s > 60).sum())),
            median_service_share=("service_share", "median"),
            median_downtime_share=("downtime_share", "median"),
        )
        .reset_index()
    )
    missing = [farm for farm in farms if farm not in set(summary["wind_farm"])]
    if missing:
        summary = pd.concat(
            [
                summary,
                pd.DataFrame(
                    {
                        "wind_farm": missing,
                        "events": 0,
                        "matched_scada_windows": 0,
                        "service_or_downtime_matches": 0,
                        "large_vessel_events": 0,
                        "median_service_share": np.nan,
                        "median_downtime_share": np.nan,
                    }
                ),
            ],
            ignore_index=True,
        )
    return result, summary


def write_power_curve_plot(
    out_path: Path,
    farm_binned: pd.DataFrame,
    curve_zip: Path,
) -> None:
    import matplotlib.pyplot as plt

    m5000 = load_power_curve(curve_zip, "M5000-116")
    s62 = load_power_curve(curve_zip, "6.2M152")
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.scatter(
        farm_binned["wind_speed_mid"],
        farm_binned["power_norm"],
        s=np.clip(farm_binned["samples"] / 100, 8, 80),
        alpha=0.6,
        label="Wind Farm C empirical bins",
    )
    ax.plot(m5000["ws"], m5000["power_norm"], label="M5000-116 normalized")
    ax.plot(s62["ws"], s62["power_norm"], label="6.2M152 normalized")
    ax.set_xlabel("Wind speed (m/s)")
    ax.set_ylabel("Normalized active power")
    ax.set_title("CAREtoCompare Wind Farm C empirical power curve")
    ax.grid(True, alpha=0.3)
    ax.legend()
    fig.tight_layout()
    fig.savefig(out_path, dpi=160)
    plt.close(fig)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--care-dir", default="Data/CARE_To_Compare")
    parser.add_argument("--nora-dir", default="Data/Raw/Metocean/NORA3")
    parser.add_argument(
        "--registry",
        default="Data/Raw/Open European offshore wind turbine database/20251218_eww_opendatabase.csv",
    )
    parser.add_argument(
        "--curve-zip",
        default="Data/Raw/Open European offshore wind turbine database/power_curves.zip",
    )
    parser.add_argument("--catalog", default="Data/catalog.duckdb")
    parser.add_argument("--out-dir", default="reports/care_wind_farm_c_confirmation")
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    registry = _read_registry(Path(args.registry))
    scada, inventory, asset_coverage = load_wind_farm_c_scada(Path(args.care_dir))
    candidates = discover_nora_candidates(Path(args.nora_dir), registry)
    nora_cache_coverage(candidates, Path(args.nora_dir)).to_csv(
        out_dir / "nora_candidate_cache_coverage.csv", index=False
    )
    ranking, nora_cache = correlation_scan(scada, candidates, Path(args.nora_dir), range(0, 11))
    ranking.to_csv(out_dir / "candidate_year_shift_ranking.csv", index=False)
    inventory.to_csv(out_dir / "scada_inventory.csv", index=False)
    asset_coverage.to_csv(out_dir / "asset_coverage.csv", index=False)

    best_col = "wind_speed_236_avg"
    bootstrap = pd.DataFrame()
    for shift in [0, 1]:
        window_rankings = monthly_window_rankings(scada, candidates, nora_cache, shift, best_col)
        window_rankings.to_csv(out_dir / f"monthly_window_rankings_shift{shift}.csv", index=False)
        shift_bootstrap = bootstrap_win_rates(window_rankings)
        shift_bootstrap.to_csv(out_dir / f"bootstrap_win_rates_shift{shift}.csv", index=False)
        if shift == 1:
            bootstrap = shift_bootstrap

    asset_fits, farm_fits, binned_power = turbine_fingerprint(scada, Path(args.curve_zip))
    asset_fits.to_csv(out_dir / "turbine_fingerprint_asset_fits.csv", index=False)
    farm_fits.to_csv(out_dir / "turbine_fingerprint_farm_fits.csv", index=False)
    binned_power.to_csv(out_dir / "empirical_power_curve_bins.csv", index=False)
    write_power_curve_plot(out_dir / "empirical_power_curve.png", binned_power, Path(args.curve_zip))

    wind_rose, rel_stats, sector_corr = directional_checks(
        scada, candidates, nora_cache, 1, best_col
    )
    wind_rose.to_csv(out_dir / "nora_wind_rose.csv", index=False)
    rel_stats.to_csv(out_dir / "scada_relative_wind_stats.csv", index=False)
    sector_corr.to_csv(out_dir / "sector_stratified_correlations_shift1.csv", index=False)

    registry_table = registry_crosscheck(registry)
    registry_table.to_csv(out_dir / "borkum_registry_crosscheck.csv", index=False)

    ais_events, ais_summary = ais_scada_cooccurrence(
        Path(args.catalog),
        scada,
        controls=["Alpha Ventus", "Borkum Riffgrund 1", "Borkum Riffgrund 2", "Global Tech I"],
    )
    ais_events.to_csv(out_dir / "ais_scada_cooccurrence_events.csv", index=False)
    ais_summary.to_csv(out_dir / "ais_scada_cooccurrence_summary.csv", index=False)

    print(f"Wrote confirmation evidence to {out_dir}")
    print("Top candidate/shift rows:")
    print(ranking.head(20).to_string(index=False))
    print("\nFarm fingerprint:")
    print(farm_fits.to_string(index=False))
    print("\nBootstrap:")
    print(bootstrap.head(10).to_string(index=False))
    print("\nAIS summary:")
    print(ais_summary.to_string(index=False))


if __name__ == "__main__":
    main()
