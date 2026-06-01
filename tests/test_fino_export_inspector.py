from pathlib import Path

import pytest

from om_pipeline.metocean.fino_export_inspector import (
    CANONICAL_IMPORT_SCHEMA,
    inspect_fino_export,
)


def test_inspector_detects_semicolon_fino_wave_export(tmp_path):
    export = tmp_path / "fino1_wave.csv"
    export.write_text(
        "\n".join(
            [
                "Timestamp;Hs [m];Tp [s];Theta [deg];QC;Comment",
                "2022-01-01 00:00;1.2;6.5;210;0;ok",
                "2022-01-01 00:10;1.3;6.7;211;0;ok",
                "2022-01-01 00:20;1.4;6.8;212;0;ok",
            ]
        ),
        encoding="utf-8",
    )
    report = tmp_path / "report.md"

    result = inspect_fino_export(export, "FINO1", output_report=report, dry_run=True)

    assert result.row_count == 3
    assert result.delimiter.label == "semicolon"
    assert result.timestamp.ten_minute_cadence is True
    assert result.timestamp.duplicate_timestamp_count == 0
    assert result.canonical_mapping["Hs [m]"] == "fino_hs"
    assert result.canonical_mapping["Tp [s]"] == "fino_tp"
    assert result.canonical_mapping["Theta [deg]"] == "fino_wave_direction"
    assert result.unit_mapping["Hs [m]"] == "m"
    assert result.qc_columns == ["QC"]
    assert result.safe_for_small_import_pilot is True
    assert report.exists()
    assert "dry-run inspection only" in report.read_text(encoding="utf-8")
    assert "fino_hs" in CANONICAL_IMPORT_SCHEMA


def test_inspector_handles_tab_export_with_metadata_lines(tmp_path):
    export = tmp_path / "fino1_wave_ascii.txt"
    export.write_text(
        "\n".join(
            [
                "# BSH Insitu export",
                "# Station: FINO1",
                "DateTime\tWaveHeight[m]\tPeakPeriod[s]\tWaveDir[deg]\tStatus",
                "2022-01-01T00:00:00Z\t1.2\t6.5\t210\tA",
                "2022-01-01T00:10:00Z\t1.3\t6.6\t211\tA",
            ]
        ),
        encoding="utf-8",
    )

    result = inspect_fino_export(export, "FINO1", dry_run=True)

    assert result.delimiter.label == "tab"
    assert result.timestamp.timestamp_start_utc == "2022-01-01T00:00:00+00:00"
    assert result.canonical_mapping["WaveHeight[m]"] == "fino_hs"
    assert result.canonical_mapping["PeakPeriod[s]"] == "fino_tp"
    assert result.canonical_mapping["WaveDir[deg]"] == "fino_wave_direction"
    assert result.qc_columns == ["Status"]


def test_inspector_supports_separate_date_time_columns(tmp_path):
    export = tmp_path / "fino1_space_ascii.txt"
    export.write_text(
        "\n".join(
            [
                "Date Time Hs Tp Direction Flag",
                "2022-01-01 00:00 1.2 6.5 210 0",
                "2022-01-01 00:10 1.3 6.6 211 0",
            ]
        ),
        encoding="utf-8",
    )

    result = inspect_fino_export(export, "FINO1", delimiter="whitespace", dry_run=True)

    assert result.delimiter.label == "whitespace"
    assert result.timestamp.timestamp_source_columns == ["Date", "Time"]
    assert result.timestamp.ten_minute_cadence is True
    assert result.canonical_mapping["Hs"] == "fino_hs"


def test_inspector_reports_physical_and_duplicate_warnings(tmp_path):
    export = tmp_path / "bad_fino.csv"
    export.write_text(
        "\n".join(
            [
                "timestamp,Hs,Tp,theta",
                "2022-01-01 00:00,-1.0,6.5,210",
                "2022-01-01 00:00,1.2,0,361",
            ]
        ),
        encoding="utf-8",
    )

    result = inspect_fino_export(export, "FINO1", dry_run=True)

    assert result.timestamp.duplicate_timestamp_count == 1
    assert result.safe_for_small_import_pilot is False
    warning_text = "\n".join(result.warnings)
    assert "negative values" in warning_text
    assert "non-positive values" in warning_text
    assert "outside [0, 360)" in warning_text
    assert "duplicate timestamps" in warning_text


def test_inspector_blocks_non_dry_run(tmp_path):
    export = tmp_path / "fino.csv"
    export.write_text("timestamp,Hs,Tp\n2022-01-01 00:00,1.0,5.0\n", encoding="utf-8")

    with pytest.raises(ValueError, match="dry-run"):
        inspect_fino_export(export, "FINO1", dry_run=False)


def test_inspector_missing_input_fails_cleanly(tmp_path):
    with pytest.raises(FileNotFoundError, match="FINO export file"):
        inspect_fino_export(Path(tmp_path / "missing.csv"), "FINO1", dry_run=True)
