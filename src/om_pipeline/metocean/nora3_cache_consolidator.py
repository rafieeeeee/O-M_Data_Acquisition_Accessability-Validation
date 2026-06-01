"""Read-only consolidation of completed NORA3 wave and wind cache files."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os
import re
import time
from typing import Iterable, Optional

import pandas as pd

from om_pipeline.common.paths import PROCESSED_DIR
from om_pipeline.ingestion.nora3 import NORA3_CACHE_DIR


DEFAULT_OUTPUT_DIR = Path(PROCESSED_DIR) / "metocean" / "nora3_joined_cache"
DEFAULT_MANIFEST_NAME = "manifest.csv"
EXPECTED_JOINED_COLUMNS = [
    "nora3_pair_id",
    "lat",
    "lon",
    "year",
    "month",
    "time",
    "hs",
    "tp",
    "wave_direction",
    "wind_speed_10m",
    "wind_direction_10m",
    "source",
    "wave_cache_file",
    "wind_cache_file",
]

WAVE_CACHE_PATTERN = re.compile(
    r"^nora3_raw_(?P<lat>-?\d+(?:\.\d+)?)_(?P<lon>-?\d+(?:\.\d+)?)_"
    r"(?P<year>\d{4})_(?P<month>\d{2})\.csv$"
)
WIND_CACHE_PATTERN = re.compile(
    r"^nora3_wind_raw_(?P<lat>-?\d+(?:\.\d+)?)_(?P<lon>-?\d+(?:\.\d+)?)_"
    r"(?P<year>\d{4})_(?P<month>\d{2})\.csv$"
)


@dataclass(frozen=True)
class Nora3CacheFile:
    path: Path
    lat_text: str
    lon_text: str
    year: int
    month: int
    mtime: float
    size_bytes: int

    @property
    def key(self) -> tuple[str, str, int, int]:
        return (self.lat_text, self.lon_text, self.year, self.month)


@dataclass(frozen=True)
class Nora3CachePair:
    wave: Nora3CacheFile
    wind: Nora3CacheFile

    @property
    def key(self) -> tuple[str, str, int, int]:
        return self.wave.key

    @property
    def pair_id(self) -> str:
        lat, lon, year, month = self.key
        return f"{lat}_{lon}_{year}_{month:02d}"

    @property
    def newest_mtime(self) -> float:
        return max(self.wave.mtime, self.wind.mtime)


@dataclass(frozen=True)
class ConsolidationSummary:
    eligible_pairs: int
    already_processed_pairs: int
    written_pairs: int
    written_batches: int
    skipped_incomplete_pairs: int
    skipped_fresh_pairs: int
    output_dir: Path
    manifest_path: Path


def _parse_cache_file(path: Path, pattern: re.Pattern[str]) -> Optional[Nora3CacheFile]:
    match = pattern.match(path.name)
    if not match:
        return None
    stat = path.stat()
    return Nora3CacheFile(
        path=path,
        lat_text=match.group("lat"),
        lon_text=match.group("lon"),
        year=int(match.group("year")),
        month=int(match.group("month")),
        mtime=stat.st_mtime,
        size_bytes=stat.st_size,
    )


def _discover_by_kind(cache_dir: Path, pattern: re.Pattern[str]) -> dict[tuple[str, str, int, int], Nora3CacheFile]:
    files: dict[tuple[str, str, int, int], Nora3CacheFile] = {}
    for path in cache_dir.glob("*.csv"):
        parsed = _parse_cache_file(path, pattern)
        if parsed is not None and parsed.size_bytes > 0:
            files[parsed.key] = parsed
    return files


def discover_cache_pairs(
    cache_dir: Path,
    stable_seconds: int = 120,
    now: Optional[float] = None,
) -> tuple[list[Nora3CachePair], int, int]:
    """Return complete wave/wind pairs old enough to read safely.

    The downloader writes CSVs directly to the NORA3 cache. To avoid racing it,
    this function only emits pairs where both files exist, are non-empty, and
    neither file has been modified within ``stable_seconds``.
    """

    now = time.time() if now is None else now
    cutoff = now - stable_seconds
    waves = _discover_by_kind(cache_dir, WAVE_CACHE_PATTERN)
    winds = _discover_by_kind(cache_dir, WIND_CACHE_PATTERN)

    complete_keys = set(waves).intersection(winds)
    incomplete_count = len(set(waves).symmetric_difference(winds))

    stable_pairs: list[Nora3CachePair] = []
    fresh_count = 0
    for key in sorted(complete_keys, key=lambda item: (item[2], item[3], float(item[0]), float(item[1]))):
        pair = Nora3CachePair(wave=waves[key], wind=winds[key])
        if pair.newest_mtime > cutoff:
            fresh_count += 1
            continue
        stable_pairs.append(pair)

    return stable_pairs, incomplete_count, fresh_count


def load_processed_pair_ids(manifest_path: Path) -> set[str]:
    if not manifest_path.exists():
        return set()
    manifest = pd.read_csv(manifest_path)
    if manifest.empty or "pair_id" not in manifest.columns:
        return set()
    if "status" in manifest.columns:
        manifest = manifest[manifest["status"] == "success"]
    return set(manifest["pair_id"].astype(str))


def _next_batch_number(manifest_path: Path) -> int:
    if not manifest_path.exists():
        return 1
    manifest = pd.read_csv(manifest_path)
    if manifest.empty or "batch_id" not in manifest.columns:
        return 1
    batch_ids = pd.to_numeric(manifest["batch_id"], errors="coerce").dropna()
    if batch_ids.empty:
        return 1
    return int(batch_ids.max()) + 1


def _atomic_write_parquet(df: pd.DataFrame, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = output_path.with_name(f".{output_path.name}.tmp-{os.getpid()}")
    df.to_parquet(tmp_path, index=False)
    os.replace(tmp_path, output_path)


def _atomic_write_csv(df: pd.DataFrame, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = output_path.with_name(f".{output_path.name}.tmp-{os.getpid()}")
    df.to_csv(tmp_path, index=False)
    os.replace(tmp_path, output_path)


def _read_pair(pair: Nora3CachePair) -> pd.DataFrame:
    wave = pd.read_csv(pair.wave.path, parse_dates=["time"])
    wind = pd.read_csv(pair.wind.path, parse_dates=["time"])

    wave = wave.drop_duplicates("time").sort_values("time")
    wind = wind.drop_duplicates("time").sort_values("time")
    joined = pd.merge(wave, wind, on="time", how="outer", suffixes=("", "_wind"))
    joined = joined.sort_values("time").reset_index(drop=True)

    lat, lon, year, month = pair.key
    # Some legacy/raw cache files already carry coordinate columns. Normalize
    # those values instead of attempting duplicate inserts.
    joined["lat"] = float(lat)
    joined["lon"] = float(lon)
    joined["year"] = year
    joined["month"] = month
    joined["nora3_pair_id"] = pair.pair_id
    joined["source"] = "NORA3"
    joined["wave_cache_file"] = pair.wave.path.name
    joined["wind_cache_file"] = pair.wind.path.name
    for column in EXPECTED_JOINED_COLUMNS:
        if column not in joined.columns:
            joined[column] = pd.NA
    extra_columns = [column for column in joined.columns if column not in EXPECTED_JOINED_COLUMNS]
    return joined[EXPECTED_JOINED_COLUMNS + extra_columns]


def _append_manifest(manifest_path: Path, rows: list[dict[str, object]]) -> None:
    new_entries = pd.DataFrame(rows)
    if manifest_path.exists():
        existing = pd.read_csv(manifest_path)
        combined = pd.concat([existing, new_entries], ignore_index=True)
    else:
        combined = new_entries
    _atomic_write_csv(combined, manifest_path)


def _chunks(items: list[Nora3CachePair], size: int) -> Iterable[list[Nora3CachePair]]:
    for start in range(0, len(items), size):
        yield items[start : start + size]


def consolidate_nora3_cache(
    cache_dir: Path | str = Path(NORA3_CACHE_DIR),
    output_dir: Path | str = DEFAULT_OUTPUT_DIR,
    batch_size: int = 100,
    stable_seconds: int = 120,
    max_batches: Optional[int] = None,
    dry_run: bool = False,
) -> ConsolidationSummary:
    """Consolidate completed NORA3 wave/wind cache pairs into parquet batches."""

    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    if stable_seconds < 0:
        raise ValueError("stable_seconds must be zero or positive")

    cache_dir = Path(cache_dir)
    output_dir = Path(output_dir)
    manifest_path = output_dir / DEFAULT_MANIFEST_NAME

    pairs, incomplete_count, fresh_count = discover_cache_pairs(cache_dir, stable_seconds=stable_seconds)
    processed_ids = load_processed_pair_ids(manifest_path)
    new_pairs = [pair for pair in pairs if pair.pair_id not in processed_ids]

    if max_batches is not None:
        if max_batches < 0:
            raise ValueError("max_batches must be zero or positive")
        new_pairs = new_pairs[: batch_size * max_batches]

    written_pairs = 0
    written_batches = 0

    if dry_run or not new_pairs:
        return ConsolidationSummary(
            eligible_pairs=len(pairs),
            already_processed_pairs=len(processed_ids.intersection({pair.pair_id for pair in pairs})),
            written_pairs=0,
            written_batches=0,
            skipped_incomplete_pairs=incomplete_count,
            skipped_fresh_pairs=fresh_count,
            output_dir=output_dir,
            manifest_path=manifest_path,
        )

    next_batch = _next_batch_number(manifest_path)
    for batch_offset, batch_pairs in enumerate(_chunks(new_pairs, batch_size)):
        batch_id = next_batch + batch_offset
        frames = [_read_pair(pair) for pair in batch_pairs]
        batch_df = pd.concat(frames, ignore_index=True)
        output_path = output_dir / f"batch_id={batch_id:06d}" / "data.parquet"
        _atomic_write_parquet(batch_df, output_path)

        processed_at = pd.Timestamp.utcnow().isoformat()
        manifest_rows = []
        for pair in batch_pairs:
            manifest_rows.append(
                {
                    "pair_id": pair.pair_id,
                    "batch_id": batch_id,
                    "status": "success",
                    "lat": float(pair.wave.lat_text),
                    "lon": float(pair.wave.lon_text),
                    "year": pair.wave.year,
                    "month": pair.wave.month,
                    "wave_cache_file": str(pair.wave.path),
                    "wind_cache_file": str(pair.wind.path),
                    "wave_mtime": pair.wave.mtime,
                    "wind_mtime": pair.wind.mtime,
                    "wave_size_bytes": pair.wave.size_bytes,
                    "wind_size_bytes": pair.wind.size_bytes,
                    "output_path": str(output_path),
                    "processed_at": processed_at,
                }
            )
        _append_manifest(manifest_path, manifest_rows)
        written_pairs += len(batch_pairs)
        written_batches += 1

    return ConsolidationSummary(
        eligible_pairs=len(pairs),
        already_processed_pairs=len(processed_ids.intersection({pair.pair_id for pair in pairs})),
        written_pairs=written_pairs,
        written_batches=written_batches,
        skipped_incomplete_pairs=incomplete_count,
        skipped_fresh_pairs=fresh_count,
        output_dir=output_dir,
        manifest_path=manifest_path,
    )
