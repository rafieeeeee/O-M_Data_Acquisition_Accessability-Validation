import pandas as pd

from om_pipeline.analysis.ais_backfill_audit import (
    build_ais_audit_summary,
    compute_effective_slice_state,
)


def test_compute_effective_slice_state_ignores_stale_dry_run_rows():
    manifest = pd.DataFrame(
        [
            {
                "timestamp": "2026-05-01 00:00:00",
                "year": 2024,
                "month": 11,
                "stage": "dry_run",
                "status": "planned",
                "message": "",
                "stage_order": -1,
                "status_rank": 0,
            },
            {
                "timestamp": "2026-05-02 00:00:00",
                "year": 2024,
                "month": 11,
                "stage": "list_keys",
                "status": "success",
                "message": "30 keys",
                "stage_order": 0,
                "status_rank": 5,
            },
            {
                "timestamp": "2026-05-02 00:01:00",
                "year": 2024,
                "month": 11,
                "stage": "stream",
                "status": "failed",
                "message": "dns failure",
                "stage_order": 1,
                "status_rank": 1,
            },
            {
                "timestamp": "2026-05-03 00:00:00",
                "year": 2024,
                "month": 10,
                "stage": "identify",
                "status": "skipped_exists",
                "message": "",
                "stage_order": 2,
                "status_rank": 5,
            },
        ]
    )

    effective = compute_effective_slice_state(manifest)
    assert len(effective) == 2

    nov = effective[(effective["year"] == 2024) & (effective["month"] == 11)].iloc[0]
    assert nov["stage"] == "stream"
    assert nov["status"] == "failed"

    oct_row = effective[(effective["year"] == 2024) & (effective["month"] == 10)].iloc[0]
    assert oct_row["stage"] == "identify"
    assert oct_row["status"] == "skipped_exists"


def test_build_ais_audit_summary_counts_event_rows_and_unresolved_slices(tmp_path):
    manifest_path = tmp_path / "ais_backfill_manifest.csv"
    interim_dir = tmp_path / "Data" / "Interim"
    interim_dir.mkdir(parents=True)

    pd.DataFrame(
        [
            {
                "timestamp": "2026-05-01 00:00:00",
                "year": 2024,
                "month": 11,
                "stage": "stream",
                "status": "failed",
                "attempt": 5,
                "raw_path": "raw.csv",
                "events_path": "events.csv",
                "registry_path": "registry.csv",
                "message": "dns failure",
            },
            {
                "timestamp": "2026-05-01 00:00:00",
                "year": 2024,
                "month": 10,
                "stage": "identify",
                "status": "success",
                "attempt": 1,
                "raw_path": "raw.csv",
                "events_path": "events.csv",
                "registry_path": "registry.csv",
                "message": "",
            },
        ]
    ).to_csv(manifest_path, index=False)

    pd.DataFrame(
        [
            {"MMSI": 1, "event_class": "Transfer"},
            {"MMSI": 2, "event_class": "Extended"},
            {"MMSI": 1, "event_class": "Transfer"},
        ]
    ).to_csv(interim_dir / "OM_Events_test.csv", index=False)
    pd.DataFrame([{"MMSI": 1}, {"MMSI": 2}]).to_csv(
        interim_dir / "Fleet_Registry_test.csv", index=False
    )

    summary = build_ais_audit_summary(manifest_path, interim_dir)
    assert summary.manifest_rows == 2
    assert summary.effective_slice_rows == 2
    assert summary.effective_status_counts == {"failed": 1, "success": 1}
    assert summary.total_event_files == 1
    assert summary.total_event_rows == 3
    assert summary.total_registry_files == 1
    assert summary.total_registry_rows == 2
    assert summary.event_class_counts == {"Extended": 1, "Transfer": 2}
    assert summary.unique_event_vessels == 2
    assert len(summary.unresolved_slices) == 1
    assert summary.unresolved_slices[0]["month"] == 11
