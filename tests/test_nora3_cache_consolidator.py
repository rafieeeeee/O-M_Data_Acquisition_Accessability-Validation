from pathlib import Path
import os
import time

import pandas as pd

from om_pipeline.metocean.nora3_cache_consolidator import (
    consolidate_nora3_cache,
    discover_cache_pairs,
)


def _write_cache_pair(cache_dir: Path, lat: str, lon: str, year: int, month: int) -> None:
    wave = pd.DataFrame(
        {
            "time": pd.date_range(f"{year}-{month:02d}-01", periods=2, freq="1h"),
            "hs": [1.0, 1.1],
            "tp": [5.0, 5.1],
            "wave_direction": [270.0, 280.0],
        }
    )
    wind = pd.DataFrame(
        {
            "time": pd.date_range(f"{year}-{month:02d}-01", periods=2, freq="1h"),
            "wind_speed_10m": [8.0, 8.5],
            "wind_direction_10m": [180.0, 190.0],
        }
    )
    wave.to_csv(cache_dir / f"nora3_raw_{lat}_{lon}_{year}_{month:02d}.csv", index=False)
    wind.to_csv(cache_dir / f"nora3_wind_raw_{lat}_{lon}_{year}_{month:02d}.csv", index=False)


def _write_cache_pair_with_coordinates(cache_dir: Path, lat: str, lon: str, year: int, month: int) -> None:
    wave = pd.DataFrame(
        {
            "time": pd.date_range(f"{year}-{month:02d}-01", periods=2, freq="1h"),
            "hs": [1.0, 1.1],
            "tp": [5.0, 5.1],
            "wave_direction": [270.0, 280.0],
            "lat": [float(lat), float(lat)],
            "lon": [float(lon), float(lon)],
        }
    )
    wind = pd.DataFrame(
        {
            "time": pd.date_range(f"{year}-{month:02d}-01", periods=2, freq="1h"),
            "wind_speed_10m": [8.0, 8.5],
        }
    )
    wave.to_csv(cache_dir / f"nora3_raw_{lat}_{lon}_{year}_{month:02d}.csv", index=False)
    wind.to_csv(cache_dir / f"nora3_wind_raw_{lat}_{lon}_{year}_{month:02d}.csv", index=False)


def _age_files(cache_dir: Path, seconds_ago: int = 300) -> None:
    target_time = time.time() - seconds_ago
    for path in cache_dir.glob("*.csv"):
        os.utime(path, (target_time, target_time))


def test_discover_cache_pairs_skips_fresh_and_incomplete(tmp_path):
    _write_cache_pair(tmp_path, "54.30", "5.80", 2024, 1)
    _write_cache_pair(tmp_path, "54.31", "5.81", 2024, 2)
    (tmp_path / "nora3_raw_54.32_5.82_2024_03.csv").write_text("time,hs,tp,wave_direction\n")

    _age_files(tmp_path, seconds_ago=300)
    fresh = tmp_path / "nora3_wind_raw_54.31_5.81_2024_02.csv"
    now = time.time()
    os.utime(fresh, (now, now))

    pairs, incomplete_count, fresh_count = discover_cache_pairs(
        tmp_path,
        stable_seconds=120,
        now=now,
    )

    assert [pair.pair_id for pair in pairs] == ["54.30_5.80_2024_01"]
    assert incomplete_count == 1
    assert fresh_count == 1


def test_consolidate_nora3_cache_writes_batches_and_manifest(tmp_path):
    cache_dir = tmp_path / "cache"
    output_dir = tmp_path / "out"
    cache_dir.mkdir()

    _write_cache_pair(cache_dir, "54.30", "5.80", 2024, 1)
    _write_cache_pair(cache_dir, "54.31", "5.81", 2024, 2)
    _write_cache_pair(cache_dir, "54.32", "5.82", 2024, 3)
    _age_files(cache_dir, seconds_ago=300)

    summary = consolidate_nora3_cache(
        cache_dir=cache_dir,
        output_dir=output_dir,
        batch_size=2,
        stable_seconds=120,
    )

    assert summary.written_pairs == 3
    assert summary.written_batches == 2

    manifest = pd.read_csv(output_dir / "manifest.csv")
    assert set(manifest["pair_id"]) == {
        "54.30_5.80_2024_01",
        "54.31_5.81_2024_02",
        "54.32_5.82_2024_03",
    }

    first_batch = pd.read_parquet(output_dir / "batch_id=000001" / "data.parquet")
    assert {"hs", "tp", "wind_speed_10m", "wind_direction_10m"}.issubset(first_batch.columns)
    assert first_batch["source"].eq("NORA3").all()

    second_summary = consolidate_nora3_cache(
        cache_dir=cache_dir,
        output_dir=output_dir,
        batch_size=2,
        stable_seconds=120,
    )
    assert second_summary.written_pairs == 0
    assert second_summary.already_processed_pairs == 3


def test_consolidate_nora3_cache_normalizes_existing_coordinate_columns(tmp_path):
    cache_dir = tmp_path / "cache"
    output_dir = tmp_path / "out"
    cache_dir.mkdir()

    _write_cache_pair_with_coordinates(cache_dir, "52.6357", "1.7935", 2024, 7)
    _age_files(cache_dir, seconds_ago=300)

    summary = consolidate_nora3_cache(
        cache_dir=cache_dir,
        output_dir=output_dir,
        batch_size=10,
        stable_seconds=120,
    )

    assert summary.written_pairs == 1

    batch = pd.read_parquet(output_dir / "batch_id=000001" / "data.parquet")
    assert batch["lat"].eq(52.6357).all()
    assert batch["lon"].eq(1.7935).all()
