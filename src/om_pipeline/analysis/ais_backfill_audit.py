from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import csv

import pandas as pd


STAGE_ORDER = {
    "dry_run": -1,
    "list_keys": 0,
    "stream": 1,
    "identify": 2,
}

STATUS_RANK = {
    "success": 5,
    "skipped_exists": 5,
    "skipped_no_keys": 4,
    "exists": 3,
    "failed": 1,
    "attempt": 0,
    "planned": 0,
}


@dataclass(frozen=True)
class AisAuditSummary:
    manifest_rows: int
    effective_slice_rows: int
    effective_status_counts: dict[str, int]
    effective_stage_counts: dict[str, int]
    unresolved_slices: list[dict[str, object]]
    total_event_files: int
    total_event_rows: int
    total_registry_files: int
    total_registry_rows: int
    event_class_counts: dict[str, int]
    unique_event_vessels: int


def _read_manifest(manifest_path: Path | str) -> pd.DataFrame:
    manifest = pd.read_csv(manifest_path)
    if manifest.empty:
        return manifest
    manifest = manifest.copy()
    manifest["timestamp"] = pd.to_datetime(manifest["timestamp"], errors="coerce")
    manifest["year"] = pd.to_numeric(manifest["year"], errors="coerce").astype("Int64")
    manifest["month"] = pd.to_numeric(manifest["month"], errors="coerce").astype("Int64")
    manifest["stage_order"] = manifest["stage"].map(STAGE_ORDER).fillna(-1)
    manifest["status_rank"] = manifest["status"].map(STATUS_RANK).fillna(0)
    return manifest


def compute_effective_slice_state(manifest: pd.DataFrame) -> pd.DataFrame:
    if manifest.empty:
        return manifest.copy()

    latest_stage_rows = (
        manifest.sort_values("timestamp")
        .groupby(["year", "month", "stage"], as_index=False)
        .tail(1)
    )

    effective = (
        latest_stage_rows.sort_values(
            ["year", "month", "stage_order", "status_rank", "timestamp"]
        )
        .groupby(["year", "month"], as_index=False)
        .tail(1)
        .sort_values(["year", "month"])
        .reset_index(drop=True)
    )
    return effective


def _count_csv_rows(path: Path) -> int:
    with path.open("r", encoding="utf-8", errors="replace", newline="") as handle:
        return max(sum(1 for _ in handle) - 1, 0)


def summarize_event_outputs(interim_dir: Path | str) -> tuple[int, int, int, int, dict[str, int], int]:
    interim_dir = Path(interim_dir)
    event_files = sorted(interim_dir.glob("OM_Events_*.csv"))
    registry_files = sorted(interim_dir.glob("Fleet_Registry_*.csv"))

    total_event_rows = 0
    total_registry_rows = 0
    event_class_counts: dict[str, int] = {}
    unique_event_vessels: set[str] = set()

    for path in event_files:
        total_event_rows += _count_csv_rows(path)
        with path.open("r", encoding="utf-8", errors="replace", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                event_class = row.get("event_class") or "Unknown"
                event_class_counts[event_class] = event_class_counts.get(event_class, 0) + 1
                vessel_id = row.get("MMSI") or row.get("vessel_id")
                if vessel_id:
                    unique_event_vessels.add(str(vessel_id))

    for path in registry_files:
        total_registry_rows += _count_csv_rows(path)

    return (
        len(event_files),
        total_event_rows,
        len(registry_files),
        total_registry_rows,
        dict(sorted(event_class_counts.items())),
        len(unique_event_vessels),
    )


def build_ais_audit_summary(
    manifest_path: Path | str,
    interim_dir: Path | str,
) -> AisAuditSummary:
    manifest = _read_manifest(manifest_path)
    effective = compute_effective_slice_state(manifest)

    unresolved = effective[~effective["status"].isin(["success", "skipped_exists", "skipped_no_keys"])]
    unresolved_rows = [
        {
            "year": int(row["year"]),
            "month": int(row["month"]),
            "stage": row["stage"],
            "status": row["status"],
            "message": row.get("message", ""),
        }
        for _, row in unresolved.iterrows()
    ]

    (
        total_event_files,
        total_event_rows,
        total_registry_files,
        total_registry_rows,
        event_class_counts,
        unique_event_vessels,
    ) = summarize_event_outputs(interim_dir)

    return AisAuditSummary(
        manifest_rows=len(manifest),
        effective_slice_rows=len(effective),
        effective_status_counts={
            str(key): int(value)
            for key, value in effective["status"].value_counts().sort_index().items()
        },
        effective_stage_counts={
            str(key): int(value)
            for key, value in effective["stage"].value_counts().sort_index().items()
        },
        unresolved_slices=unresolved_rows,
        total_event_files=total_event_files,
        total_event_rows=total_event_rows,
        total_registry_files=total_registry_files,
        total_registry_rows=total_registry_rows,
        event_class_counts=event_class_counts,
        unique_event_vessels=unique_event_vessels,
    )
