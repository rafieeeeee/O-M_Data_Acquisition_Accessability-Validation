import pandas as pd

from om_pipeline.analysis.provisional_stage1_hs_tp import (
    PROVISIONAL_STAGE1_LABEL,
    build_provisional_stage1_outputs,
    build_stage1_subsets,
    compute_tp_boundary_table,
    static_threshold_comparison,
)


def _sample_dwell_weather() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "dwell_id": ["d1", "d2", "d3", "d4", "d5"],
            "visit_id": ["v1", "v2", "v3", "v4", "v5"],
            "mmsi": [1, 2, 3, 4, 5],
            "dwell_tier": ["Tier A", "Tier A", "Tier B", "Tier A", "Tier D"],
            "wind_farm": ["A", "A", "B", "C", "C"],
            "start_utc": pd.to_datetime(
                [
                    "2020-01-01T00:00:00Z",
                    "2020-02-01T00:00:00Z",
                    "2021-03-01T00:00:00Z",
                    "2021-04-01T00:00:00Z",
                    "2021-05-01T00:00:00Z",
                ]
            ),
            "end_utc": pd.to_datetime(
                [
                    "2020-01-01T00:30:00Z",
                    "2020-02-01T00:30:00Z",
                    "2021-03-01T00:30:00Z",
                    "2021-04-01T00:30:00Z",
                    "2021-05-01T00:30:00Z",
                ]
            ),
            "active_hs_mean": [0.6, 1.2, 1.5, None, 2.5],
            "active_tp_mean": [4.0, 5.5, 6.0, 7.0, 8.0],
        }
    )


def test_stage1_filters_and_tables_are_observed_envelope_only():
    df = _sample_dwell_weather()

    primary, sensitivity = build_stage1_subsets(df)
    assert len(primary) == 2
    assert len(sensitivity) == 4
    assert set(primary["dwell_tier"]) == {"Tier A"}

    boundary = compute_tp_boundary_table(primary, "primary_tier_a")
    assert "hs_p95" in boundary.columns
    assert (boundary["analysis_label"] == PROVISIONAL_STAGE1_LABEL).all()

    threshold = static_threshold_comparison(primary, sensitivity)
    assert "share_at_or_below_threshold" in threshold.columns
    assert (threshold["analysis_label"] == PROVISIONAL_STAGE1_LABEL).all()


def test_build_provisional_stage1_outputs_writes_labelled_artifacts(tmp_path):
    input_path = tmp_path / "dwell_weather.parquet"
    report_dir = tmp_path / "reports"
    processed_dir = tmp_path / "processed"
    _sample_dwell_weather().to_parquet(input_path, index=False)

    outputs = build_provisional_stage1_outputs(
        input_path=input_path,
        report_output_dir=report_dir,
        processed_output_dir=processed_dir,
    )

    assert outputs.validation["primary_summary"]["rows"] == 2
    assert outputs.validation["sensitivity_summary"]["rows"] == 4
    for path in outputs.files.values():
        assert path.exists()

    primary = pd.read_parquet(outputs.files["primary_clean_parquet"])
    assert set(primary["analysis_label"]) == {PROVISIONAL_STAGE1_LABEL}

    report_text = outputs.files["coverage_report_md"].read_text(encoding="utf-8")
    assert PROVISIONAL_STAGE1_LABEL in report_text
    assert "P(operation | weather)" in report_text
    assert "not a probability model" in report_text
