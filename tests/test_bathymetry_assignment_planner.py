import pandas as pd
import pytest

from om_pipeline.metocean.bathymetry_planner import (
    BATHYMETRY_OUTPUT_SCHEMA,
    build_bathymetry_assignment_plan,
    plan_bathymetry_assignment,
)


def _write_requirements(path):
    pd.DataFrame(
        [
            {
                "wind_farm": "Example Baltic Farm",
                "farm_id": "Example_Baltic_Farm",
                "country": "Germany",
                "min_lon": 13.0,
                "max_lon": 13.5,
                "min_lat": 54.2,
                "max_lat": 54.7,
                "sample_point_strategy": "farm_centroid_and_turbines",
                "sample_point_count": 3,
                "review_required": False,
            },
            {
                "wind_farm": "Example UK Farm",
                "farm_id": "Example_UK_Farm",
                "country": "United Kingdom",
                "min_lon": -3.5,
                "max_lon": -3.0,
                "min_lat": 53.7,
                "max_lat": 54.2,
                "sample_point_strategy": "farm_centroid_and_turbines",
                "sample_point_count": 2,
                "review_required": False,
            },
        ]
    ).to_csv(path, index=False)


def test_bathymetry_planner_dry_run_writes_only_report(tmp_path):
    requirements = tmp_path / "requirements.csv"
    output_dir = tmp_path / "bathymetry"
    report = tmp_path / "bathymetry_report.md"
    _write_requirements(requirements)

    result = plan_bathymetry_assignment(
        requirements_path=requirements,
        output_root=output_dir,
        qa_report=report,
        dry_run=True,
    )

    assert report.exists()
    assert not output_dir.exists()
    assert result.summary["farm_count"] == 2
    assert result.summary["sample_point_count"] == 5
    assert result.summary["primary_source"] == "emodnet"
    assert result.summary["fallback_source"] == "gebco_2026"
    assert result.output_schema == BATHYMETRY_OUTPUT_SCHEMA
    text = report.read_text(encoding="utf-8")
    assert "QA report only" in text
    assert "site_bathymetry_points.parquet" in text


def test_bathymetry_planner_records_source_choice_and_schema(tmp_path):
    requirements = tmp_path / "requirements.csv"
    _write_requirements(requirements)

    result = build_bathymetry_assignment_plan(
        requirements_path=requirements,
        output_root=tmp_path / "out",
    )

    assert set(result.farm_plan["planned_primary_source"]) == {"emodnet"}
    assert set(result.farm_plan["planned_fallback_source"]) == {"gebco_2026"}
    assert "Baltic / Belt Seas" in set(result.farm_plan["region"])
    assert "UK shelf / Irish Sea" in set(result.farm_plan["region"])
    assert "bathymetry_vertical_datum" in result.output_schema
    assert result.summary["depth_sign_convention"] == "positive_down_meters_in_processed_table"


def test_bathymetry_planner_is_overwrite_safe_in_dry_run(tmp_path):
    requirements = tmp_path / "requirements.csv"
    output_dir = tmp_path / "bathymetry"
    existing = output_dir / "site_bathymetry_points.parquet"
    report = tmp_path / "report.md"
    _write_requirements(requirements)
    output_dir.mkdir()
    existing.write_text("existing output", encoding="utf-8")

    result = plan_bathymetry_assignment(
        requirements_path=requirements,
        output_root=output_dir,
        qa_report=report,
        dry_run=True,
    )

    assert existing.read_text(encoding="utf-8") == "existing output"
    assert result.summary["output_exists"]
    assert result.summary["overwrite_policy"] == "preserve_existing"
    assert "preserve_existing" in report.read_text(encoding="utf-8")


def test_bathymetry_planner_missing_file_fails_cleanly(tmp_path):
    with pytest.raises(FileNotFoundError, match="Common metocean requirements table"):
        build_bathymetry_assignment_plan(requirements_path=tmp_path / "missing.csv")


def test_bathymetry_planner_missing_columns_fail_cleanly(tmp_path):
    requirements = tmp_path / "requirements.csv"
    pd.DataFrame([{"wind_farm": "Incomplete Farm"}]).to_csv(requirements, index=False)

    with pytest.raises(ValueError, match="missing required columns"):
        build_bathymetry_assignment_plan(requirements_path=requirements)


def test_bathymetry_planner_blocks_non_dry_run(tmp_path):
    requirements = tmp_path / "requirements.csv"
    _write_requirements(requirements)

    with pytest.raises(ValueError, match="dry-run mode only"):
        plan_bathymetry_assignment(
            requirements_path=requirements,
            output_root=tmp_path / "out",
            qa_report=None,
            dry_run=False,
        )
