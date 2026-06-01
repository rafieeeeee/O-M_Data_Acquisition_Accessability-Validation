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
