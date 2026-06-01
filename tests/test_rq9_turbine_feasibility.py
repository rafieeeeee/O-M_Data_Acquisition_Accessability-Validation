from pathlib import Path

import pandas as pd

from om_pipeline.analysis.rq9_turbine_feasibility import (
    ANALYSIS_LABEL,
    TURBINE_EVENT_COLUMNS,
    build_dwell_column_suitability,
    build_rq9_turbine_feasibility_outputs,
    build_turbine_intervention_events,
    build_turbine_metadata_completeness,
    build_turbine_feasibility_report,
    build_answerability_matrix,
)
from om_pipeline.analysis.rq9_turbine_exposure import (
    DENOMINATOR_COLUMNS,
    INTENSITY_COLUMNS,
    assign_sea_basin,
    build_exposure_comparison,
    build_static_turbine_exposure_features,
    build_turbine_denominator,
    build_turbine_exposure_report,
    build_turbine_intervention_intensity,
)
from om_pipeline.analysis.rq9_turbine_characteristics import (
    build_characteristics_rates,
    build_characteristics_report,
    capacity_band,
    hub_height_band,
    rotor_diameter_band,
)


def _sample_turbines() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "Unnamed: 0": [10, 11, 20],
            "wind_farm": ["Alpha", "Alpha", "Bravo"],
            "oem_manufacturer": ["MakerA", "MakerA", "MakerB"],
            "latitude": [52.0, 52.01, 53.0],
            "longitude": [4.0, 4.0, 5.0],
            "country": ["Netherlands", "Netherlands", "Germany"],
            "rated_power": [8.0, 8.0, 6.0],
            "rotor_diameter": [160.0, 160.0, 150.0],
            "hub_height": [100.0, 100.0, 90.0],
            "turbine_type": ["A-8", "A-8", "B-6"],
            "commissioning_date": ["2020-01", "2020-01", "2020-06"],
        }
    )


def _sample_dwell() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "dwell_id": ["dw-high", "dw-medium", "dw-far", "dw-tier-b", "dw-bravo"],
            "visit_id": ["v1", "v2", "v3", "v4", "v5"],
            "farm_id": ["Alpha", "Alpha", "Alpha", "Alpha", "Bravo"],
            "wind_farm": ["Alpha", "Alpha", "Alpha", "Alpha", "Bravo"],
            "mmsi": [111, 222, 333, 444, 555],
            "dwell_tier": ["Tier A", "Tier A", "Tier A", "Tier B", "Tier A"],
            "start_utc": [
                "2020-02-01T00:00:00Z",
                "2020-08-01T00:00:00Z",
                "2020-09-01T00:00:00Z",
                "2020-09-01T00:00:00Z",
                "2021-01-01T00:00:00Z",
            ],
            "end_utc": [
                "2020-02-01T02:00:00Z",
                "2020-08-01T02:00:00Z",
                "2020-09-01T02:00:00Z",
                "2020-09-01T02:00:00Z",
                "2021-01-01T02:00:00Z",
            ],
            "duration_min": [120.0, 130.0, 140.0, 150.0, 160.0],
            "centroid_lat": [52.0, 52.003, 52.02, 52.0, 53.0],
            "centroid_lon": [4.0, 4.0, 4.0, 4.0, 5.0],
            "possible_cross_farm_duplicate": [False, True, False, False, True],
            "duplicate_group_id": [None, "dup-1", None, None, "dup-1"],
            "duplicate_farm_ids": [None, "Alpha,Bravo", None, None, "Alpha,Bravo"],
            "farm_operational_status_at_event": [
                "commissioning",
                "operational",
                "operational",
                "operational",
                "operational",
            ],
            "interpretation_period": [
                "commissioning",
                "steady",
                "steady",
                "steady",
                "steady",
            ],
        }
    )


def _sample_farm_intensity() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "farm_id": ["Alpha", "Bravo"],
            "steady_intervention_intensity_per_farm_year": [1.0, 2.0],
        }
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
            ],
            "year": [2020, 2020, 2020, 2020, 2020, 2020, 2020, 2021],
            "month": [1, 6, 7, 8, 9, 10, 11, 1],
            "status": [
                "success",
                "success",
                "success",
                "success_no_ais_in_bbox",
                "skipped_missing_source",
                "source_parse_error",
                "success",
                "success",
            ],
        }
    )


def test_nearest_turbine_assignment_confidence_and_tier_a_only():
    events, metrics = build_turbine_intervention_events(
        _sample_dwell(),
        _sample_turbines(),
        ramp_up_months=6,
    )
    by_id = events.set_index("dwell_id")

    assert list(events.columns) == TURBINE_EVENT_COLUMNS
    assert set(events["analysis_label"]) == {ANALYSIS_LABEL}
    assert set(events["dwell_tier"]) == {"Tier A"}
    assert "dw-tier-b" not in set(events["dwell_id"])
    assert by_id.loc["dw-high", "assignment_confidence"] == "high"
    assert by_id.loc["dw-high", "assignment_supports_turbine_level"]
    assert by_id.loc["dw-medium", "assignment_confidence"] == "medium"
    assert by_id.loc["dw-medium", "assignment_supports_turbine_level"]
    assert metrics["tier_a_event_rows"] == 4
    assert metrics["assigned_event_rows"] == 3


def test_no_turbine_assignment_outside_threshold():
    events, _ = build_turbine_intervention_events(
        _sample_dwell(),
        _sample_turbines(),
        ramp_up_months=6,
    )
    far = events.set_index("dwell_id").loc["dw-far"]

    assert far["assignment_confidence"] == "unassigned"
    assert not far["assignment_supports_turbine_level"]
    assert pd.isna(far["assigned_turbine_id"])
    assert far["distance_to_turbine_m"] > 500.0
    assert pd.notna(far["nearest_turbine_id"])


def test_duplicate_groups_are_preserved_non_destructively():
    events, _ = build_turbine_intervention_events(
        _sample_dwell(),
        _sample_turbines(),
        ramp_up_months=6,
    )
    by_id = events.set_index("dwell_id")

    assert "dup-1" in set(events["duplicate_group_id"].dropna())
    assert by_id.loc["dw-medium", "duplicate_adjusted_event_weight"] == 0.5
    assert by_id.loc["dw-bravo", "duplicate_adjusted_event_weight"] == 0.5
    assert by_id.loc["dw-high", "duplicate_adjusted_event_weight"] == 1.0


def test_metadata_completeness_and_dwell_suitability_summaries():
    metadata = build_turbine_metadata_completeness(_sample_turbines())
    suitability = build_dwell_column_suitability(_sample_dwell())

    assert metadata.set_index("metadata_field").loc["oem_manufacturer", "status"] == "complete"
    assert metadata.set_index("metadata_field").loc["rated_capacity", "non_null_share"] == 1.0
    assert suitability.set_index("concept").loc["lat/lon", "present"]
    assert suitability.set_index("concept").loc["duplicate group", "non_null_share"] < 1.0


def test_report_and_outputs_use_intervention_terminology(tmp_path: Path):
    dwell = _sample_dwell()
    turbines = _sample_turbines()
    events, metrics = build_turbine_intervention_events(dwell, turbines)
    metadata = build_turbine_metadata_completeness(turbines)
    suitability = build_dwell_column_suitability(dwell)
    answerability = build_answerability_matrix(events, metadata, _sample_farm_intensity())
    report = build_turbine_feasibility_report(
        events,
        metadata,
        suitability,
        answerability,
        metrics,
    )

    assert "maintenance intervention intensity" in report
    assert "failure_rate" not in report
    assert all("failure" not in column for column in events.columns)

    dwell_path = tmp_path / "dwell.parquet"
    turbine_path = tmp_path / "turbines.csv"
    farm_path = tmp_path / "farm.csv"
    dwell.to_parquet(dwell_path, index=False)
    turbines.to_csv(turbine_path, index=False)
    _sample_farm_intensity().to_csv(farm_path, index=False)

    outputs = build_rq9_turbine_feasibility_outputs(
        dwell_path=dwell_path,
        turbine_path=turbine_path,
        farm_intensity_path=farm_path,
        processed_output_dir=tmp_path / "processed",
        report_output_dir=tmp_path / "reports",
    )

    for path in outputs.files.values():
        assert path.exists()
    written_events = pd.read_csv(outputs.files["turbine_intervention_events_v0_csv"])
    assert list(written_events.columns) == TURBINE_EVENT_COLUMNS
    assert outputs.validation["answerability_status_counts"]["partially ready"] >= 1


def test_turbine_denominator_excludes_pre_operational_and_ramp_up_months():
    denominator, metrics = build_turbine_denominator(
        _sample_manifest(),
        _sample_turbines(),
        ramp_up_months=6,
    )
    alpha = denominator.loc[denominator["farm_id"].eq("Alpha")]

    assert list(denominator.columns) == DENOMINATOR_COLUMNS
    assert set(alpha["observed_steady_months"]) == {2}
    assert set(alpha["steady_manifest_months"]) == {4}
    assert set(alpha["success_steady_months"]) == {1}
    assert set(alpha["success_no_ais_in_bbox_steady_months"]) == {1}
    assert set(alpha["skipped_missing_source_steady_months"]) == {1}
    assert set(alpha["first_observed_steady_month"]) == {"2020-07"}
    assert metrics["success_no_ais_in_bbox_steady_months"] == 2


def test_turbine_high_confidence_primary_and_medium_sensitivity_are_separate():
    events, _ = build_turbine_intervention_events(
        _sample_dwell(),
        _sample_turbines(),
        ramp_up_months=6,
    )
    denominator, _ = build_turbine_denominator(
        _sample_manifest(),
        _sample_turbines(),
        ramp_up_months=6,
    )
    intensity, metrics = build_turbine_intervention_intensity(events, denominator)
    alpha_10 = intensity.set_index("turbine_id").loc["Alpha::10"]

    assert list(intensity.columns) == INTENSITY_COLUMNS
    assert alpha_10["steady_high_event_count"] == 0
    assert alpha_10["steady_high_medium_event_count"] == 1
    assert metrics["high_medium_steady_event_count"] > metrics["high_confidence_steady_event_count"]


def test_exposure_quantile_and_sea_basin_unknown_mapping():
    turbines = pd.DataFrame(
        {
            "Unnamed: 0": [1, 2, 3, 4],
            "wind_farm": ["Layout", "Layout", "Layout", "Layout"],
            "latitude": [55.0, 55.0, 55.01, 55.02],
            "longitude": [8.0, 8.01, 8.0, 8.0],
            "country": ["Denmark", "Denmark", "Denmark", "Denmark"],
            "commissioning_date": ["2020-01", "2020-01", "2020-01", "2020-01"],
        }
    )
    features = build_static_turbine_exposure_features(turbines)

    assert features["distance_to_farm_centroid_quantile"].max() == 1.0
    assert "outer_exposed_proxy" in set(features["exposure_group"])
    assert assign_sea_basin("Atlantis", 0.0) == ("unknown", "unmapped_country_unknown")
    assert assign_sea_basin(None, 0.0) == ("unknown", "missing_country_unknown")


def test_exposure_report_and_outputs_use_intervention_terminology():
    events, _ = build_turbine_intervention_events(
        _sample_dwell(),
        _sample_turbines(),
        ramp_up_months=6,
    )
    denominator, denominator_metrics = build_turbine_denominator(
        _sample_manifest(),
        _sample_turbines(),
        ramp_up_months=6,
    )
    intensity, intensity_metrics = build_turbine_intervention_intensity(events, denominator)
    comparison = build_exposure_comparison(intensity)
    report = build_turbine_exposure_report(
        denominator,
        intensity,
        comparison,
        {**denominator_metrics, **intensity_metrics},
    )

    assert "maintenance intervention intensity" in report
    assert "failure_rate" not in report
    assert "failure rate" not in report.lower()
    assert all("failure_rate" not in column for column in denominator.columns)
    assert all("failure_rate" not in column for column in intensity.columns)


def _sample_turbine_intensity_for_characteristics() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "wind_farm": ["Alpha", "Alpha", "Bravo", "Bravo", "Charlie"],
            "oem_manufacturer": ["MakerA", "MakerA", "MakerB", "MakerB", "MakerC"],
            "turbine_model": ["A-8", "A-8", "B-6", "B-6", "C-12"],
            "rated_capacity_mw": [8.0, 8.0, 6.0, 6.0, 12.0],
            "rotor_diameter_m": [164.0, 164.0, 150.0, 150.0, 220.0],
            "hub_height_m": [108.0, 108.0, 95.0, 95.0, 140.0],
            "commissioning_year": [2018, 2018, 2020, 2020, 2024],
            "turbine_age_band_at_observation_end": [
                "3_to_7_years",
                "3_to_7_years",
                "3_to_7_years",
                "3_to_7_years",
                "0_to_2_years",
            ],
            "sea_basin": ["North Sea", "North Sea", "Baltic", "Baltic", "other"],
            "country": ["Netherlands", "Netherlands", "Germany", "Germany", "France"],
            "observed_steady_years": [5.0, 5.0, 3.0, 0.5, 0.2],
            "steady_high_event_count": [2, 0, 1, 1, 0],
            "steady_high_duplicate_adjusted_event_count": [2.0, 0.0, 0.5, 1.0, 0.0],
            "steady_high_medium_event_count": [3, 1, 2, 1, 0],
            "steady_high_medium_duplicate_adjusted_event_count": [
                3.0,
                1.0,
                1.5,
                1.0,
                0.0,
            ],
        }
    )


def test_characteristic_banding_helpers():
    assert capacity_band(2.9) == "lt_3_mw"
    assert capacity_band(3.6) == "3_to_4_9_mw"
    assert capacity_band(6.0) == "5_to_7_9_mw"
    assert capacity_band(8.4) == "8_to_9_9_mw"
    assert capacity_band(14.0) == "10_plus_mw"
    assert capacity_band(None) == "unknown"
    assert rotor_diameter_band(99.0) == "lt_100_m"
    assert rotor_diameter_band(126.0) == "100_to_129_m"
    assert rotor_diameter_band(150.0) == "130_to_159_m"
    assert rotor_diameter_band(164.0) == "160_to_179_m"
    assert rotor_diameter_band(222.0) == "180_plus_m"
    assert hub_height_band(68.0) == "lt_70_m"
    assert hub_height_band(80.0) == "70_to_89_m"
    assert hub_height_band(100.0) == "90_to_109_m"
    assert hub_height_band(120.0) == "110_to_129_m"
    assert hub_height_band(140.0) == "130_plus_m"


def test_characteristics_rates_apply_minimum_exposure_and_rates():
    rates = build_characteristics_rates(_sample_turbine_intensity_for_characteristics())
    by_key = rates.set_index(["characteristic", "characteristic_value", "exposure_threshold"])
    maker_a = by_key.loc[("oem_manufacturer", "MakerA", "min_3yr")]
    maker_b = by_key.loc[("oem_manufacturer", "MakerB", "min_3yr")]

    assert maker_a["turbine_count"] == 2
    assert maker_a["observed_steady_turbine_years"] == 10.0
    assert maker_a["high_confidence_event_count"] == 2
    assert maker_a["primary_duplicate_adjusted_intervention_intensity_per_turbine_year"] == 0.2
    assert maker_a["sensitivity_duplicate_adjusted_intervention_intensity_per_turbine_year"] == 0.4
    assert maker_a["zero_event_turbine_count"] == 1
    assert maker_a["zero_event_turbine_share"] == 0.5
    assert maker_b["turbine_count"] == 1
    assert "MakerC" not in set(
        rates.loc[
            rates["characteristic"].eq("oem_manufacturer")
            & rates["exposure_threshold"].eq("min_3yr"),
            "characteristic_value",
        ]
    )


def test_characteristics_report_uses_intervention_terminology():
    rates = build_characteristics_rates(_sample_turbine_intensity_for_characteristics())
    report = build_characteristics_report(
        rates,
        rates.head(0).assign(
            comparison_focus=pd.Series(dtype="string"),
            primary_rank=pd.Series(dtype="Int64"),
        ),
        {
            "turbine_rows": 5,
            "observed_steady_years_total": 13.7,
            "high_confidence_event_count": 4,
            "high_medium_event_count": 7,
            "robust_exploratory_rows": 0,
        },
    )

    assert "maintenance intervention intensity" in report
    assert "failure_rate" not in report
    assert "failure rate" not in report.lower()
