import pandas as pd

from om_pipeline.analysis.evidence_readiness import (
    build_farm_metadata,
    build_geographic_coverage_summary,
    build_farm_month_evidence_matrix,
    build_report_text,
    build_rq_readiness_matrix,
    build_turbine_month_evidence_matrix,
    classify_rq_answerability,
    contains_ais_only_failure_rate_claim,
)


def _manifest() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "farm_id": ["Alpha", "Alpha", "Alpha", "Bravo"],
            "year": [2020, 2020, 2020, 2020],
            "month": [1, 2, 3, 1],
            "status": [
                "success",
                "success_no_ais_in_bbox",
                "skipped_missing_source",
                "success",
            ],
            "dwell_count": [2, 0, 0, 1],
            "tier_a_count": [1, 0, 0, 0],
            "tier_b_count": [1, 0, 0, 0],
            "tier_c_count": [0, 0, 0, 0],
            "tier_d_count": [0, 0, 0, 1],
            "source_file_name": ["raw-a.csv", "raw-b.csv", None, "raw-c.csv"],
        }
    )


def _turbine_exposure() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "farm_id": ["Alpha", "Alpha", "Bravo"],
            "wind_farm": ["Alpha", "Alpha", "Bravo"],
            "turbine_id": ["Alpha::0", "Alpha::1", "Bravo::0"],
            "country": ["DE", "DE", "DK"],
            "sea_basin": ["North Sea", "North Sea", "Baltic"],
            "commissioning_date": ["2019-01", "2019-01", "2019-01"],
            "commissioning_month": ["2019-01", "2019-01", "2019-01"],
            "steady_operational_start_month": ["2019-07", "2019-07", "2019-07"],
        }
    )


def _dwell() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "farm_id": ["Alpha", "Alpha", "Bravo"],
            "wind_farm": ["Alpha", "Alpha", "Bravo"],
            "mmsi": [111, 222, 333],
            "dwell_tier": ["Tier A", "Tier B", "Tier D"],
            "start_utc": [
                "2020-01-05T00:00:00Z",
                "2020-01-06T00:00:00Z",
                "2020-01-07T00:00:00Z",
            ],
            "vessel_length_m": [None, None, None],
            "vessel_beam_m": [None, None, None],
            "access_technology": [None, None, None],
            "registry_source": [None, None, None],
        }
    )


def _fusion() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "farm_id": ["Alpha", "Alpha", "Bravo"],
            "wind_farm": ["Alpha", "Alpha", "Bravo"],
            "dwell_id": ["d1", "d2", "d3"],
            "start_utc": [
                "2020-01-05T00:00:00Z",
                "2020-01-06T00:00:00Z",
                "2020-01-07T00:00:00Z",
            ],
            "has_wave": [True, True, False],
            "has_wind_speed": [True, True, False],
            "has_wind_direction": [False, False, False],
            "has_current": [False, False, False],
            "has_bathymetry": [True, True, False],
            "model_ready_wave_only": [True, True, False],
            "model_ready_wave_wind": [True, True, False],
            "model_ready_wave_current": [False, False, False],
            "model_ready_wave_wind_current": [False, False, False],
            "model_ready_high_confidence": [False, False, False],
        }
    )


def _events() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "farm_id": ["Alpha", "Alpha"],
            "assigned_turbine_id": ["Alpha::0", "Alpha::1"],
            "mmsi": [111, 222],
            "dwell_tier": ["Tier A", "Tier B"],
            "start_utc": ["2020-01-05T00:00:00Z", "2020-01-06T00:00:00Z"],
            "assignment_confidence": ["high", "medium"],
            "assignment_supports_turbine_level": [True, True],
        }
    )


def _farm_matrix() -> pd.DataFrame:
    metadata = build_farm_metadata(turbine_exposure=_turbine_exposure())
    return build_farm_month_evidence_matrix(
        _manifest(),
        metadata,
        dwell=_dwell(),
        fusion_v2=_fusion(),
        turbine_events=_events(),
        bathymetry=pd.DataFrame({"wind_farm": ["Alpha"], "water_depth_m": [30.0]}),
        wave_archive_years=pd.DataFrame(
            {
                "farm_id": ["Alpha"],
                "year": [2020],
                "wave_archive_year_available": [True],
                "wave_archive_year_available_source": ["nws_wave_timeseries"],
            }
        ),
        current_archive_years=pd.DataFrame(
            {
                "farm_id": ["Alpha"],
                "year": [2020],
                "current_archive_year_available": [False],
                "current_archive_year_available_source": [""],
            }
        ),
        scada_months=pd.DataFrame(
            {
                "farm_id": ["Alpha"],
                "month": ["2020-01"],
                "scada_validation_available": [True],
                "scada_validation_source": ["test_scada"],
            }
        ),
    )


def test_success_and_success_no_ais_in_bbox_count_as_observed_coverage():
    matrix = _farm_matrix().set_index(["farm_id", "month"])

    assert matrix.loc[("Alpha", "2020-01"), "observed_source_month_flag"]
    assert matrix.loc[("Alpha", "2020-02"), "observed_source_month_flag"]
    assert matrix.loc[("Alpha", "2020-02"), "zero_event_despite_coverage"]
    assert matrix.loc[("Alpha", "2020-02"), "ais_dwell_event_count"] == 0


def test_skipped_missing_source_is_missing_not_zero():
    matrix = _farm_matrix().set_index(["farm_id", "month"])

    assert matrix.loc[("Alpha", "2020-03"), "skipped_missing_source_flag"]
    assert pd.isna(matrix.loc[("Alpha", "2020-03"), "ais_dwell_event_count"])
    assert matrix.loc[("Alpha", "2020-03"), "confidence_class"] == "D_missing_source"


def test_missing_metocean_vessel_and_scada_layers_lower_readiness():
    matrix = _farm_matrix().set_index(["farm_id", "month"])

    alpha = matrix.loc[("Alpha", "2020-01")]
    bravo = matrix.loc[("Bravo", "2020-01")]
    assert alpha["confidence_class"] == "A_local_validated"
    assert bravo["confidence_class"] == "C_partial_proxy"
    assert not bravo["metocean_wave_available"]
    assert not bravo["vessel_metadata_available"]
    assert not bravo["scada_validation_available"]


def test_turbine_month_matrix_preserves_missing_source_and_assignment_counts():
    farm_matrix = _farm_matrix()
    turbine_matrix = build_turbine_month_evidence_matrix(
        farm_matrix,
        turbine_exposure=_turbine_exposure(),
        turbine_events=_events(),
    ).set_index(["farm_id", "turbine_id", "month"])

    assert turbine_matrix.loc[("Alpha", "Alpha::0", "2020-01"), "ais_dwell_event_count"] == 1
    assert pd.isna(turbine_matrix.loc[("Alpha", "Alpha::0", "2020-03"), "ais_dwell_event_count"])
    assert turbine_matrix.loc[("Alpha", "Alpha::0", "2020-03"), "confidence_class"] == "D_missing_source"


def test_rq_readiness_classification_works():
    assert classify_rq_answerability(["ais", "metocean"], ["ais", "metocean"]) == "ready"
    assert classify_rq_answerability(["ais", "metocean"], ["ais"]) == "partial"
    assert (
        classify_rq_answerability(["ais", "vessel"], ["ais"], blocking_layers=["vessel"])
        == "blocked"
    )

    rq = build_rq_readiness_matrix(
        {
            "ais_manifest": True,
            "ais_dwell": True,
            "metocean_fusion_v2": True,
            "wave": True,
            "wind_speed": True,
            "current": True,
            "bathymetry": True,
            "turbine_metadata": True,
            "vessel_metadata": False,
            "scada_validation": False,
            "direct_ais_receiver_metadata": False,
            "fault_work_orders": False,
        }
    ).set_index("rq_number")

    assert rq.loc["RQ6", "answerability"] == "ready"
    assert rq.loc["RQ2", "answerability"] == "blocked"
    assert rq.loc["RQ9", "answerability"] == "blocked"


def test_no_failure_rate_claim_is_made_from_ais_only_evidence():
    allowed_boundary = (
        "AIS visits are candidate intervention evidence, not confirmed failures. "
        "SCADA or work-order validation is required before any failure interpretation."
    )
    prohibited = "AIS failure rate is estimated from vessel dwell counts."

    assert not contains_ais_only_failure_rate_claim(allowed_boundary)
    assert contains_ais_only_failure_rate_claim(prohibited)


def test_report_preserves_reproducibility_and_missingness_semantics():
    farm_matrix = _farm_matrix()
    turbine_matrix = build_turbine_month_evidence_matrix(
        farm_matrix,
        turbine_exposure=_turbine_exposure(),
        turbine_events=_events(),
    )
    rq_matrix = build_rq_readiness_matrix(
        {
            "ais_manifest": True,
            "ais_dwell": True,
            "metocean_fusion_v2": True,
            "wave": True,
            "wind_speed": True,
            "current": True,
            "bathymetry": True,
            "turbine_metadata": True,
            "vessel_metadata": False,
            "scada_validation": True,
            "direct_ais_receiver_metadata": False,
            "fault_work_orders": False,
        }
    )
    validation = {
        "farm_month_rows": len(farm_matrix),
        "turbine_month_rows": len(turbine_matrix),
        "observed_source_months": int(farm_matrix["observed_source_month_flag"].sum()),
        "skipped_missing_source_months": int(farm_matrix["skipped_missing_source_flag"].sum()),
        "observed_zero_months": int(farm_matrix["zero_event_despite_coverage"].sum()),
        "vessel_metadata_available_months": int(farm_matrix["vessel_metadata_available"].sum()),
        "wave_available_months": int(farm_matrix["metocean_wave_available"].sum()),
        "wind_speed_available_months": int(farm_matrix["metocean_wind_speed_available"].sum()),
        "wind_direction_available_months": int(farm_matrix["metocean_wind_direction_available"].sum()),
        "current_available_months": int(farm_matrix["metocean_current_available"].sum()),
        "bathymetry_available_months": int(farm_matrix["metocean_bathymetry_available"].sum()),
        "scada_validation_available_months": int(farm_matrix["scada_validation_available"].sum()),
    }

    report = build_report_text(
        farm_matrix,
        turbine_matrix,
        build_geographic_coverage_summary(farm_matrix),
        rq_matrix,
        validation,
    )

    assert "/opt/anaconda3/bin/python scripts/build_evidence_readiness.py" in report
    assert "`success` and `success_no_ais_in_bbox` are observed AIS source coverage" in report
    assert "`success_no_ais_in_bbox` is observed zero-event evidence" in report
    assert "`skipped_missing_source` is missing source evidence" in report
    assert "RQ6 is ready only for source-aware metocean sensitivity/readiness work" in report
    assert "RQ9 remains blocked for failure claims" in report
    assert "Validation is localized to CARE Wind Farm B/C mappings" in report
    assert not contains_ais_only_failure_rate_claim(report)
