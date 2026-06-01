from pathlib import Path

import pandas as pd

from om_pipeline.analysis.rq9_intervention_intensity import (
    ANALYSIS_LABEL,
    FARM_INTENSITY_COLUMNS,
    build_farm_intervention_intensity,
    build_farm_operational_metadata,
    build_manifest_denominator,
    build_rq9_farm_outputs,
)


def _sample_manifest() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "farm_id": [
                "Alpha",
                "Alpha",
                "Alpha",
                "Alpha",
                "Alpha",
                "Alpha",
                "Bravo",
                "Bravo",
                "Bravo",
                "Charlie",
                "Delta",
            ],
            "year": [2020] * 11,
            "month": [1, 2, 3, 9, 10, 11, 1, 2, 8, 1, 1],
            "status": [
                "success",
                "success_no_ais_in_bbox",
                "success",
                "success",
                "skipped_missing_source",
                "success_no_ais_in_bbox",
                "success_no_ais_in_bbox",
                "skipped_missing_source",
                "success",
                "skipped_missing_source",
                "success_no_ais_in_bbox",
            ],
        }
    )


def _sample_dwell() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "farm_id": [
                "Alpha",
                "Alpha",
                "Alpha",
                "Alpha",
                "Alpha",
                "Alpha",
                "Bravo",
                "Bravo",
            ],
            "dwell_tier": [
                "Tier A",
                "Tier A",
                "Tier B",
                "Tier A",
                "Tier B",
                "Tier D",
                "Tier A",
                "Tier B",
            ],
            "duration_min": [300.0, 30.0, 150.0, 300.0, 90.0, 300.0, 200.0, 90.0],
            "mmsi": [999, 111, 222, 444, None, 333, 111, None],
            "start_utc": [
                "2020-01-15T00:00:00Z",
                "2020-02-01T00:00:00Z",
                "2020-03-02T00:00:00Z",
                "2020-09-03T00:00:00Z",
                "2020-10-01T00:00:00Z",
                "2020-09-05T00:00:00Z",
                "2020-01-10T00:00:00Z",
                "2020-08-11T00:00:00Z",
            ],
            "possible_cross_farm_duplicate": [
                False,
                True,
                False,
                False,
                False,
                False,
                True,
                False,
            ],
            "duplicate_group_id": [None, "dup-1", None, None, None, None, "dup-1", None],
        }
    )


def _sample_turbines() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "wind_farm": ["Alpha", "Alpha", "Bravo", "TurbineOnly"],
            "latitude": [1.0, 1.1, 2.0, 9.0],
            "longitude": [3.0, 3.1, 4.0, 9.0],
            "commissioning_date": ["2020-02", "2020-03", "2020-01", "2020-01"],
        }
    )


def test_manifest_denominator_counts_phase_observed_zero_and_excludes_missing_source():
    metadata = build_farm_operational_metadata(_sample_turbines(), ramp_up_months=6)
    denominator = build_manifest_denominator(
        _sample_manifest(),
        operational_metadata=metadata,
    ).set_index("farm_id")

    assert denominator.loc["Alpha", "total_manifest_months"] == 6
    assert denominator.loc["Alpha", "pre_operational_manifest_months"] == 1
    assert denominator.loc["Alpha", "commissioning_manifest_months"] == 2
    assert denominator.loc["Alpha", "steady_manifest_months"] == 3
    assert denominator.loc["Alpha", "manifest_months"] == 5
    assert denominator.loc["Alpha", "commissioning_observed_months"] == 2
    assert denominator.loc["Alpha", "steady_observed_months"] == 2
    assert denominator.loc["Alpha", "observed_months"] == 4
    assert denominator.loc["Alpha", "success_no_ais_in_bbox_months"] == 2
    assert denominator.loc["Alpha", "skipped_missing_source_months"] == 1
    assert denominator.loc["Alpha", "steady_observed_years"] == 2 / 12

    assert denominator.loc["Bravo", "commissioning_observed_months"] == 1
    assert denominator.loc["Bravo", "steady_observed_months"] == 1
    assert denominator.loc["Charlie", "observed_months"] == 0
    assert denominator.loc["Charlie", "skipped_missing_source_months"] == 1
    assert denominator.loc["Delta", "unknown_phase_observed_months"] == 1
    assert denominator.loc["Delta", "success_no_ais_in_bbox_months"] == 1


def test_farm_numerators_split_ramp_up_from_steady_and_preserve_duplicates():
    dwell = _sample_dwell()
    original_rows = len(dwell)

    intensity, _ = build_farm_intervention_intensity(
        manifest=_sample_manifest(),
        dwell=dwell,
        turbines=_sample_turbines(),
        long_dwell_threshold_min=120.0,
        ramp_up_months=6,
    )
    by_farm = intensity.set_index("farm_id")

    assert len(dwell) == original_rows
    assert by_farm.loc["Alpha", "pre_operational_candidate_count"] == 1
    assert by_farm.loc["Alpha", "commissioning_candidate_count"] == 2
    assert by_farm.loc["Alpha", "steady_candidate_count"] == 2
    assert by_farm.loc["Alpha", "candidate_intervention_count"] == 4
    assert by_farm.loc["Alpha", "commissioning_long_dwell_count"] == 1
    assert by_farm.loc["Alpha", "steady_long_dwell_count"] == 1
    assert by_farm.loc["Alpha", "long_dwell_count"] == 2
    assert by_farm.loc["Alpha", "unique_vessel_count"] == 3

    assert by_farm.loc["Bravo", "commissioning_candidate_count"] == 1
    assert by_farm.loc["Bravo", "steady_candidate_count"] == 1
    assert by_farm.loc["Bravo", "steady_long_dwell_count"] == 0
    assert by_farm.loc["Bravo", "unique_vessel_count"] == 1

    assert by_farm.loc["Alpha", "duplicate_adjustment_available"]
    assert by_farm.loc["Alpha", "duplicate_group_adjusted_candidate_count"] == 3.5
    assert by_farm.loc["Alpha", "commissioning_duplicate_adjusted_count"] == 1.5
    assert by_farm.loc["Alpha", "steady_duplicate_adjusted_count"] == 2.0
    assert by_farm.loc["Bravo", "duplicate_group_adjusted_candidate_count"] == 1.5
    assert by_farm.loc["Alpha", "duplicate_adjustment_delta"] == 0.5


def test_ramp_up_months_sensitivity_changes_phase_classification():
    no_ramp, _ = build_farm_intervention_intensity(
        manifest=_sample_manifest(),
        dwell=_sample_dwell(),
        turbines=_sample_turbines(),
        ramp_up_months=0,
    )
    long_ramp, _ = build_farm_intervention_intensity(
        manifest=_sample_manifest(),
        dwell=_sample_dwell(),
        turbines=_sample_turbines(),
        ramp_up_months=12,
    )

    no_ramp_alpha = no_ramp.set_index("farm_id").loc["Alpha"]
    long_ramp_alpha = long_ramp.set_index("farm_id").loc["Alpha"]

    assert no_ramp_alpha["steady_operational_start_month"] == "2020-03"
    assert no_ramp_alpha["steady_candidate_count"] == 3
    assert long_ramp_alpha["steady_operational_start_month"] == "2021-03"
    assert long_ramp_alpha["steady_candidate_count"] == 0
    assert long_ramp_alpha["commissioning_candidate_count"] == 4


def test_output_schema_observed_zero_and_simulator_use_behaviour():
    intensity, _ = build_farm_intervention_intensity(
        manifest=_sample_manifest(),
        dwell=_sample_dwell(),
        turbines=_sample_turbines(),
        long_dwell_threshold_min=120.0,
        ramp_up_months=6,
    )

    assert list(intensity.columns) == FARM_INTENSITY_COLUMNS
    assert set(intensity["analysis_label"]) == {ANALYSIS_LABEL}
    assert "TurbineOnly" not in set(intensity["farm_id"])

    by_farm = intensity.set_index("farm_id")
    assert by_farm.loc["Alpha", "steady_observed_months"] == 2
    assert by_farm.loc["Alpha", "steady_intervention_intensity_per_farm_year"] == 12.0
    assert by_farm.loc["Charlie", "observed_months"] == 0
    assert pd.isna(by_farm.loc["Charlie", "candidate_interventions_per_observed_farm_year"])
    assert by_farm.loc["Charlie", "confidence_class"] == "low_coverage"
    assert by_farm.loc["Delta", "observed_months"] == 1
    assert by_farm.loc["Delta", "unknown_phase_observed_months"] == 1
    assert by_farm.loc["Delta", "candidate_intervention_count"] == 0
    assert by_farm.loc["Delta", "candidate_interventions_per_observed_farm_year"] == 0.0
    assert by_farm.loc["Delta", "recommended_simulator_use"] == "insufficient_steady_coverage"


def test_missing_commissioning_data_lowers_confidence():
    intensity, _ = build_farm_intervention_intensity(
        manifest=_sample_manifest(),
        dwell=_sample_dwell(),
        turbines=_sample_turbines(),
        long_dwell_threshold_min=120.0,
        ramp_up_months=6,
    )

    by_farm = intensity.set_index("farm_id")
    assert by_farm.loc["Delta", "operational_start_source"] == "missing_turbine_metadata"
    assert not by_farm.loc["Delta", "operational_window_known"]
    assert by_farm.loc["Delta", "confidence_class"] == "low_signal_ambiguous"
    assert set(intensity["confidence_class"]) != {"high_observed_signal"}


def test_build_rq9_farm_outputs_writes_expected_files(tmp_path: Path):
    dwell_path = tmp_path / "dwell.parquet"
    manifest_path = tmp_path / "manifest.csv"
    turbine_path = tmp_path / "turbines.csv"
    processed_dir = tmp_path / "processed"
    report_dir = tmp_path / "reports"

    _sample_dwell().to_parquet(dwell_path, index=False)
    _sample_manifest().to_csv(manifest_path, index=False)
    _sample_turbines().to_csv(turbine_path, index=False)

    outputs = build_rq9_farm_outputs(
        dwell_path=dwell_path,
        manifest_path=manifest_path,
        turbine_path=turbine_path,
        processed_output_dir=processed_dir,
        report_output_dir=report_dir,
        long_dwell_threshold_min=120.0,
        ramp_up_months=6,
    )

    for path in outputs.files.values():
        assert path.exists()

    farm_output = pd.read_csv(outputs.files["farm_intervention_intensity_csv"])
    assert list(farm_output.columns) == FARM_INTENSITY_COLUMNS
    assert outputs.validation["farm_output_rows"] == 4
    assert outputs.validation["candidate_intervention_count_total"] == 6
    assert outputs.validation["pre_operational_candidate_count_total"] == 1
    assert outputs.validation["commissioning_candidate_count_total"] == 3
    assert outputs.validation["steady_candidate_count_total"] == 3
    assert outputs.validation["observed_years_total"] == 7 / 12
    assert outputs.validation["commissioning_observed_years_total"] == 3 / 12
    assert outputs.validation["steady_observed_years_total"] == 3 / 12

    report_text = outputs.files["methodology_report_md"].read_text(encoding="utf-8")
    assert "intervention intensity" in report_text
    assert "not failure rate" in report_text
    assert "commissioning/ramp-up" in report_text
    assert "A vessel visit is not automatically a failure" in report_text
