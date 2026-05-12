import csv
import os
import time
from dataclasses import dataclass
from pathlib import Path

from ..common.paths import AIS_RAW_DIR, INTERIM_DIR
from ..identification.dwell_events import identify_vessels
from ..ingestion.ais import list_month_keys, stream_and_filter


QUARTERLY_MONTHS = (1, 4, 7, 10)
ALL_MONTHS = tuple(range(1, 13))


@dataclass(frozen=True)
class SliceSpec:
    year: int
    month: int


def farm_candidate_base_name(year, month, region_name, max_sog, buffer_nm):
    region_suffix = region_name.replace("_", "-").title()
    sog_suffix = f"_SogMax{max_sog}" if max_sog is not None else ""
    return f"Farm-Candidates_{region_suffix}_{year}_{month:02d}{sog_suffix}_Buffer{buffer_nm}nm"


def slice_paths(year, month, region_name="european_master", max_sog=2.0, buffer_nm=2.0):
    base_name = farm_candidate_base_name(year, month, region_name, max_sog, buffer_nm)
    raw_path = Path(AIS_RAW_DIR) / f"{base_name}.csv"
    events_path = Path(INTERIM_DIR) / f"OM_Events_{base_name}.csv"
    registry_path = Path(INTERIM_DIR) / f"Fleet_Registry_{base_name}.csv"
    return raw_path, events_path, registry_path


def build_schedule(start_year, end_year, phase="all"):
    """Return quarterly slices first, then missing-month backfill slices."""
    years = range(end_year, start_year - 1, -1)
    quarterly = [SliceSpec(year, month) for year in years for month in QUARTERLY_MONTHS]
    backfill = [
        SliceSpec(year, month)
        for year in years
        for month in ALL_MONTHS
        if month not in QUARTERLY_MONTHS
    ]
    if phase == "quarterly":
        return quarterly
    if phase == "backfill":
        return backfill
    return quarterly + backfill


def append_manifest(manifest_path, row):
    manifest_path = Path(manifest_path)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "timestamp",
        "year",
        "month",
        "stage",
        "status",
        "attempt",
        "raw_path",
        "events_path",
        "registry_path",
        "message",
    ]
    write_header = not manifest_path.exists()
    with manifest_path.open("a", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        writer.writerow({name: row.get(name, "") for name in fieldnames})


def log_slice(manifest_path, spec, stage, status, attempt, paths, message=""):
    raw_path, events_path, registry_path = paths
    append_manifest(
        manifest_path,
        {
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "year": spec.year,
            "month": f"{spec.month:02d}",
            "stage": stage,
            "status": status,
            "attempt": attempt,
            "raw_path": raw_path,
            "events_path": events_path,
            "registry_path": registry_path,
            "message": message,
        },
    )


def run_with_retries(action, retries, sleep_seconds, on_attempt):
    last_error = None
    for attempt in range(1, retries + 1):
        on_attempt(attempt)
        try:
            return action(attempt)
        except Exception as exc:
            last_error = exc
            if attempt < retries:
                time.sleep(sleep_seconds * attempt)
    raise last_error


def list_keys_with_retries(spec, retries, sleep_seconds, manifest_path, paths):
    """Return DMA keys for a slice, retrying transient catalogue/network failures."""
    def list_action(attempt):
        keys = list_month_keys(spec.year, spec.month)
        log_slice(manifest_path, spec, "list_keys", "success", attempt, paths, f"{len(keys)} keys")
        return keys

    def list_attempt(attempt):
        log_slice(manifest_path, spec, "list_keys", "attempt", attempt, paths)

    return run_with_retries(list_action, retries, sleep_seconds, list_attempt)


def process_slice(
    spec,
    *,
    region_name="european_master",
    max_sog=2.0,
    buffer_nm=2.0,
    turbine_file=None,
    chunk_size=500000,
    progress_interval=5,
    retries=3,
    retry_sleep_seconds=60,
    manifest_path=None,
    force_raw=False,
    force_identification=False,
    dry_run=False,
):
    paths = slice_paths(spec.year, spec.month, region_name, max_sog, buffer_nm)
    raw_path, events_path, registry_path = paths
    manifest_path = manifest_path or Path(INTERIM_DIR) / "ais_backfill_manifest.csv"

    print(f"\n=== {spec.year}-{spec.month:02d} ===")
    print(f"Raw:      {raw_path}")
    print(f"Events:   {events_path}")
    print(f"Registry: {registry_path}")

    if dry_run:
        status = "exists" if raw_path.exists() and events_path.exists() and registry_path.exists() else "planned"
        log_slice(manifest_path, spec, "dry_run", status, 0, paths)
        return status

    if force_raw or not raw_path.exists():
        try:
            keys = list_keys_with_retries(spec, retries, retry_sleep_seconds, manifest_path, paths)
        except Exception as exc:
            log_slice(manifest_path, spec, "list_keys", "failed", retries, paths, str(exc))
            print(f"FAILED key listing for {spec.year}-{spec.month:02d}: {exc}")
            return "failed_list_keys"

        if not keys:
            message = "No DMA ZIP files found for this year-month."
            print(message)
            log_slice(manifest_path, spec, "stream", "skipped_no_keys", 0, paths, message)
            return "skipped_no_keys"

        def stream_action(attempt):
            result = stream_and_filter(
                spec.year,
                spec.month,
                region_name=region_name,
                max_sog=max_sog,
                mode="farm_candidate",
                buffer_nm=buffer_nm,
                turbine_file=turbine_file,
            )
            if not result or not raw_path.exists():
                raise RuntimeError("stream_and_filter completed without creating the expected raw file")
            log_slice(manifest_path, spec, "stream", "success", attempt, paths)
            return result

        def stream_attempt(attempt):
            log_slice(manifest_path, spec, "stream", "attempt", attempt, paths)

        try:
            run_with_retries(stream_action, retries, retry_sleep_seconds, stream_attempt)
        except Exception as exc:
            log_slice(manifest_path, spec, "stream", "failed", retries, paths, str(exc))
            print(f"FAILED stream for {spec.year}-{spec.month:02d}: {exc}")
            return "failed_stream"
    else:
        print("Raw file already exists; skipping stream.")
        log_slice(manifest_path, spec, "stream", "skipped_exists", 0, paths)

    if force_identification or not (events_path.exists() and registry_path.exists()):
        def identify_action(attempt):
            result = identify_vessels(
                os.fspath(raw_path),
                turbine_file or os.path.join(INTERIM_DIR, "European_Turbine_Coordinates.csv"),
                chunk_size=chunk_size,
                progress_interval=progress_interval,
            )
            if not events_path.exists() or not registry_path.exists():
                raise RuntimeError("identify_vessels completed without creating expected outputs")
            log_slice(manifest_path, spec, "identify", "success", attempt, paths)
            return result

        def identify_attempt(attempt):
            log_slice(manifest_path, spec, "identify", "attempt", attempt, paths)

        try:
            run_with_retries(identify_action, retries, retry_sleep_seconds, identify_attempt)
        except Exception as exc:
            log_slice(manifest_path, spec, "identify", "failed", retries, paths, str(exc))
            print(f"FAILED identification for {spec.year}-{spec.month:02d}: {exc}")
            return "failed_identify"
    else:
        print("Event and registry files already exist; skipping identification.")
        log_slice(manifest_path, spec, "identify", "skipped_exists", 0, paths)

    return "success"


def run_backfill(
    *,
    start_year=2010,
    end_year=2025,
    phase="all",
    region_name="european_master",
    max_sog=2.0,
    buffer_nm=2.0,
    turbine_file=None,
    chunk_size=500000,
    progress_interval=5,
    retries=3,
    retry_sleep_seconds=60,
    manifest_path=None,
    force_raw=False,
    force_identification=False,
    limit=None,
    dry_run=False,
):
    schedule = build_schedule(start_year, end_year, phase=phase)
    if limit is not None:
        schedule = schedule[:limit]

    manifest_path = manifest_path or Path(INTERIM_DIR) / "ais_backfill_manifest.csv"
    print(f"Scheduled {len(schedule)} slice(s), phase={phase}, years={start_year}-{end_year}.")
    print(f"Manifest: {manifest_path}")

    counts = {}
    for spec in schedule:
        status = process_slice(
            spec,
            region_name=region_name,
            max_sog=max_sog,
            buffer_nm=buffer_nm,
            turbine_file=turbine_file,
            chunk_size=chunk_size,
            progress_interval=progress_interval,
            retries=retries,
            retry_sleep_seconds=retry_sleep_seconds,
            manifest_path=manifest_path,
            force_raw=force_raw,
            force_identification=force_identification,
            dry_run=dry_run,
        )
        counts[status] = counts.get(status, 0) + 1

    print("\nBackfill summary:")
    for status, count in sorted(counts.items()):
        print(f"  {status}: {count}")
    return counts
