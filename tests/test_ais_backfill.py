from pathlib import Path

from om_pipeline.analysis.ais_backfill import (
    QUARTERLY_MONTHS,
    SliceSpec,
    build_schedule,
    farm_candidate_base_name,
    log_slice,
)


def test_build_schedule_runs_quarterly_newest_first_then_backfill():
    schedule = build_schedule(2024, 2025, phase="all")

    assert schedule[:8] == [
        SliceSpec(2025, 1),
        SliceSpec(2025, 4),
        SliceSpec(2025, 7),
        SliceSpec(2025, 10),
        SliceSpec(2024, 1),
        SliceSpec(2024, 4),
        SliceSpec(2024, 7),
        SliceSpec(2024, 10),
    ]
    assert all(spec.month in QUARTERLY_MONTHS for spec in schedule[:8])
    assert schedule[8] == SliceSpec(2025, 2)
    assert schedule[-1] == SliceSpec(2024, 12)


def test_build_schedule_can_select_only_quarterly_or_backfill():
    assert build_schedule(2024, 2024, phase="quarterly") == [
        SliceSpec(2024, 1),
        SliceSpec(2024, 4),
        SliceSpec(2024, 7),
        SliceSpec(2024, 10),
    ]
    assert build_schedule(2024, 2024, phase="backfill") == [
        SliceSpec(2024, 2),
        SliceSpec(2024, 3),
        SliceSpec(2024, 5),
        SliceSpec(2024, 6),
        SliceSpec(2024, 8),
        SliceSpec(2024, 9),
        SliceSpec(2024, 11),
        SliceSpec(2024, 12),
    ]


def test_farm_candidate_base_name_matches_existing_naming_contract():
    assert (
        farm_candidate_base_name(2024, 7, "european_master", 2.0, 2.0)
        == "Farm-Candidates_European-Master_2024_07_SogMax2.0_Buffer2.0nm"
    )


def test_log_slice_writes_resumable_manifest(tmp_path):
    manifest = tmp_path / "manifest.csv"
    paths = (
        Path("Data/Raw/AIS/example.csv"),
        Path("Data/Interim/OM_Events_example.csv"),
        Path("Data/Interim/Fleet_Registry_example.csv"),
    )

    log_slice(manifest, SliceSpec(2024, 7), "stream", "success", 1, paths, "ok")
    text = manifest.read_text()

    assert "year,month,stage,status" in text
    assert "2024,07,stream,success,1" in text
    assert "ok" in text
