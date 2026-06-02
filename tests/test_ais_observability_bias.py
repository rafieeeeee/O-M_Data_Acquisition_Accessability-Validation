from pathlib import Path

import pandas as pd
import pytest

from om_pipeline.analysis.ais_observability_bias import (
    add_observed_base_station_distances,
    build_base_station_distance_gradient_summary,
    build_base_station_distance_stratum_diagnostic,
    build_farm_controls,
    build_farm_month_observability_bias_features,
    build_farm_observability_bias_summary,
    build_geographic_source_intensity_summary,
    build_receiver_metadata_inventory,
    build_report_text,
    classify_observability_field,
    extract_ais_base_station_geometry_catalogue,
    validate_external_reference_provenance,
)
from om_pipeline.analysis.evidence_readiness import (
    build_farm_metadata,
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
            "input_rows": [100, 50, None, 10],
            "clean_rows": [90, 48, None, 8],
            "visit_count": [2, 0, None, 1],
            "dwell_count": [2, 0, None, 1],
            "tier_a_count": [1, 0, None, 0],
            "tier_b_count": [1, 0, None, 0],
            "tier_c_count": [0, 0, None, 0],
            "tier_d_count": [0, 0, None, 1],
            "source_file_name": ["a.csv", "b.csv", None, "c.csv"],
        }
    )


def _turbine_exposure() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "farm_id": ["Alpha", "Alpha", "Bravo"],
            "wind_farm": ["Alpha", "Alpha", "Bravo"],
            "turbine_id": ["A-1", "A-2", "B-1"],
            "country": ["DE", "DE", "DK"],
            "sea_basin": ["North Sea", "North Sea", "Baltic"],
            "rated_capacity_mw": [5.0, 5.0, 8.0],
            "farm_centroid_latitude": [54.0, 54.0, 55.0],
            "farm_centroid_longitude": [7.0, 7.0, 13.0],
            "water_depth_m": [30.0, 32.0, 20.0],
            "commissioning_date": ["2019-01", "2019-01", "2019-01"],
            "commissioning_month": ["2019-01", "2019-01", "2019-01"],
            "steady_operational_start_month": ["2019-07", "2019-07", "2019-07"],
        }
    )


def _dwell() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "farm_id": ["Alpha", "Alpha", "Bravo"],
            "start_utc": [
                "2020-01-05T00:00:00Z",
                "2020-01-06T00:00:00Z",
                "2020-01-07T00:00:00Z",
            ],
            "mmsi": [111, 222, 333],
            "vessel_length_m": [30.0, None, None],
            "access_technology": ["CTV", None, None],
            "registry_source": ["test_registry", None, None],
        }
    )


def _events() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "farm_id": ["Alpha", "Alpha", "Bravo"],
            "start_utc": [
                "2020-01-05T00:00:00Z",
                "2020-01-06T00:00:00Z",
                "2020-01-07T00:00:00Z",
            ],
            "assignment_confidence": ["high", "medium", "low"],
        }
    )


def _farm_month() -> pd.DataFrame:
    metadata = build_farm_metadata(turbine_exposure=_turbine_exposure())
    controls = build_farm_controls(_turbine_exposure(), metadata)
    return build_farm_month_observability_bias_features(
        _manifest(),
        controls,
        dwell=_dwell(),
        turbine_events=_events(),
        direct_receiver_metadata_available=False,
    )


def _distance_diagnostic_farm_month(
    *,
    clean_rows: list[int] | None = None,
    dwell_counts: list[int] | None = None,
    observed_zero: list[bool] | None = None,
    statuses: list[str] | None = None,
    farm_count: int = 8,
) -> pd.DataFrame:
    clean_rows = clean_rows or [100, 90, 80, 70, 30, 20, 10, 5]
    dwell_counts = dwell_counts or [8, 7, 6, 5, 2, 1, 0, 0]
    observed_zero = observed_zero or [False, False, False, False, False, True, True, True]
    statuses = statuses or ["success"] * farm_count
    rows = []
    for index in range(farm_count):
        status = statuses[index]
        rows.append(
            {
                "farm_id": f"Farm-{index:02d}",
                "country": "DK",
                "sea_basin": "Baltic",
                "year": 2020,
                "month": "2020-01",
                "observed_source_month_flag": status
                in {"success", "success_no_ais_in_bbox"},
                "success_no_ais_in_bbox_flag": status == "success_no_ais_in_bbox",
                "skipped_missing_source_flag": status == "skipped_missing_source",
                "observed_zero_month_flag": observed_zero[index],
                "distance_to_nearest_observed_base_station_km": float(index + 1) * 10.0,
                "source_clean_rows": clean_rows[index],
                "ais_dwell_event_count": dwell_counts[index],
                "turbine_count": 1,
                "farm_capacity_mw": 10.0,
                "tier_a_count": dwell_counts[index],
                "tier_b_count": max(dwell_counts[index] - 1, 0),
                "tier_c_count": 0,
                "tier_d_count": 0,
            }
        )
    return pd.DataFrame(rows)


def test_receiver_metadata_detection_classifies_direct_and_source_fields():
    assert classify_observability_field("receiver_station_id")[1] == "direct_receiver_evidence"
    assert classify_observability_field("Data source type")[1] == "source_channel_evidence"

    inventory = build_receiver_metadata_inventory(
        {
            "raw.csv": ["Timestamp", "Data source type", "MMSI"],
            "receiver.csv": ["receiver_station_id", "receiver_latitude"],
        }
    )

    assert inventory["is_direct_receiver_evidence"].sum() == 2
    assert inventory["is_source_channel_evidence"].sum() == 1


def test_missing_receiver_metadata_is_unavailable_not_imputed():
    matrix = _farm_month()

    assert not matrix["direct_receiver_metadata_available"].any()
    assert matrix["nearest_external_receiver_distance_km"].isna().all()
    assert matrix["observability_evidence_tier"].str.contains("Proxy-only").all()


def test_external_receiver_reference_requires_complete_provenance():
    with pytest.raises(ValueError, match="provenance"):
        build_receiver_metadata_inventory(
            {"raw.csv": ["Data source type"]},
            external_reference_used=True,
            external_reference_provenance=None,
        )

    incomplete = pd.DataFrame({"source_name": ["example"]})
    with pytest.raises(ValueError, match="missing columns"):
        validate_external_reference_provenance(incomplete)


def test_base_station_catalogue_extracts_decimal_comma_rows(tmp_path: Path):
    raw_file = tmp_path / "Farm-Candidates_European-Master_2020_01_SogMax2.0_Buffer2.0nm.csv"
    raw_file.write_text(
        "\n".join(
            [
                "Timestamp,Type of mobile,MMSI,Latitude,Longitude,Type of position fixing device,Data source type",
                '01/01/2020 00:00:00,Base Station,2053506,"55,000000","12,000000",GPS,AIS',
                '01/01/2020 00:06:00,Base Station,2053506,"55,000200","12,000200",GPS,AIS',
                '01/01/2020 00:07:00,Class A,123456789,"55,100000","12,100000",GPS,AIS',
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    catalogue = extract_ais_base_station_geometry_catalogue(
        tmp_path, "Farm-Candidates_European-Master_*.csv"
    )

    assert len(catalogue) == 1
    station = catalogue.iloc[0]
    assert station["base_station_mmsi"] == "2053506"
    assert station["observation_count"] == 2
    assert station["timestamp_first_seen"] == "2020-01-01T00:00:00Z"
    assert station["timestamp_last_seen"] == "2020-01-01T00:06:00Z"
    assert station["position_fixing_device_values"] == "GPS"
    assert station["data_source_type_values"] == "AIS"
    assert station["source_months_seen"] == "2020-01"
    assert station["evidence_classification"] == "direct_ais_base_station_geometry_reference"
    assert not station["receiver_assignment_available_flag"]


def test_nearest_observed_base_station_distance_is_source_geometry_control():
    metadata = build_farm_metadata(turbine_exposure=_turbine_exposure())
    controls = build_farm_controls(_turbine_exposure(), metadata)
    catalogue = pd.DataFrame(
        {
            "base_station_mmsi": ["station-alpha", "station-bravo"],
            "base_station_latitude": [54.0, 55.0],
            "base_station_longitude": [7.0, 13.0],
        }
    )

    with_distances = add_observed_base_station_distances(controls, catalogue).set_index("farm_id")

    assert with_distances.loc["Alpha", "base_station_geometry_available_flag"]
    assert with_distances.loc["Alpha", "nearest_observed_base_station_mmsi"] == "station-alpha"
    assert with_distances.loc["Alpha", "distance_to_nearest_observed_base_station_km"] == 0
    assert with_distances.loc["Bravo", "nearest_observed_base_station_mmsi"] == "station-bravo"


def test_base_station_geometry_does_not_become_receiver_assignment_claim():
    metadata = build_farm_metadata(turbine_exposure=_turbine_exposure())
    controls = build_farm_controls(_turbine_exposure(), metadata)
    controls = add_observed_base_station_distances(
        controls,
        pd.DataFrame(
            {
                "base_station_mmsi": ["station-alpha"],
                "base_station_latitude": [54.0],
                "base_station_longitude": [7.0],
            }
        ),
    )

    matrix = build_farm_month_observability_bias_features(
        _manifest(),
        controls,
        dwell=_dwell(),
        turbine_events=_events(),
        direct_receiver_metadata_available=False,
    )

    assert matrix["base_station_geometry_available_flag"].all()
    assert not matrix["direct_receiver_metadata_available"].any()
    assert matrix["observability_evidence_tier"].str.contains(
        "receiver-distance causality remains unconfirmed"
    ).all()


def test_missing_source_months_are_excluded_from_zero_and_density_denominators():
    matrix = _farm_month().set_index(["farm_id", "month"])

    assert matrix.loc[("Alpha", "2020-02"), "observed_source_month_flag"]
    assert matrix.loc[("Alpha", "2020-02"), "observed_zero_month_flag"]
    assert matrix.loc[("Alpha", "2020-03"), "skipped_missing_source_flag"]
    assert not matrix.loc[("Alpha", "2020-03"), "observed_zero_month_flag"]
    assert pd.isna(matrix.loc[("Alpha", "2020-03"), "dwell_event_density"])
    assert pd.isna(matrix.loc[("Alpha", "2020-03"), "candidate_ping_density_clean_rows"])

    summary = build_farm_observability_bias_summary(_farm_month()).set_index("farm_id")
    assert summary.loc["Alpha", "observed_source_months"] == 2
    assert summary.loc["Alpha", "missing_source_months"] == 1
    assert summary.loc["Alpha", "observed_zero_months"] == 1
    assert summary.loc["Alpha", "observed_zero_share_of_observed"] == 0.5


def test_source_intensity_is_separated_from_dwell_event_density():
    matrix = _farm_month().set_index(["farm_id", "month"])

    assert matrix.loc[("Alpha", "2020-01"), "candidate_ping_density_clean_rows"] == 90
    assert matrix.loc[("Alpha", "2020-01"), "dwell_event_density"] == 2
    assert matrix.loc[("Alpha", "2020-01"), "clean_rows_per_turbine"] == 45
    assert matrix.loc[("Alpha", "2020-01"), "dwell_events_per_mw"] == 0.2


def test_controls_are_preserved_for_farm_month_summaries():
    matrix = _farm_month().set_index(["farm_id", "month"])

    alpha = matrix.loc[("Alpha", "2020-01")]
    assert alpha["country"] == "DE"
    assert alpha["sea_basin"] == "North Sea"
    assert alpha["turbine_count"] == 2
    assert alpha["farm_capacity_mw"] == 10
    assert alpha["water_depth_m"] == 31


def test_base_station_distance_diagnostic_uses_observed_source_months_only():
    statuses = [
        "success",
        "success_no_ais_in_bbox",
        "skipped_missing_source",
        "success",
        "success",
        "success",
        "success",
        "success",
    ]
    matrix = _distance_diagnostic_farm_month(statuses=statuses)

    diagnostic = build_base_station_distance_stratum_diagnostic(matrix)

    assert diagnostic["farm_month_count"].sum() == 7
    assert diagnostic["success_no_ais_in_bbox_month_count"].sum() == 1
    assert diagnostic["observed_zero_month_count"].sum() == 3
    assert diagnostic["source_clean_rows_total"].sum() == 100 + 90 + 70 + 30 + 20 + 10 + 5
    assert "skipped_missing_source" not in diagnostic.to_string()


def test_base_station_distance_bins_are_assigned_within_matched_strata():
    matrix = _distance_diagnostic_farm_month()

    diagnostic = build_base_station_distance_stratum_diagnostic(matrix)

    assert diagnostic["distance_bin"].tolist() == [
        "q1_nearest",
        "q2_near_mid",
        "q3_far_mid",
        "q4_farthest",
    ]
    assert diagnostic["distance_comparison_status"].eq(
        "eligible_within_stratum_quartile"
    ).all()
    assert diagnostic["farm_month_count"].sum() == 8


def test_small_distance_strata_are_flagged_not_overinterpreted():
    matrix = _distance_diagnostic_farm_month(farm_count=3)

    diagnostic = build_base_station_distance_stratum_diagnostic(matrix)
    gradient = build_base_station_distance_gradient_summary(diagnostic)

    assert diagnostic["distance_bin"].iloc[0] == "insufficient_within_stratum_comparison"
    assert gradient["diagnostic_class"].iloc[0] == "insufficient_matched_strata"


def test_base_station_distance_gradient_classifies_source_observability_signal():
    matrix = _distance_diagnostic_farm_month()
    diagnostic = build_base_station_distance_stratum_diagnostic(matrix)

    gradient = build_base_station_distance_gradient_summary(diagnostic)
    row = gradient.iloc[0]

    assert row["diagnostic_class"] == "consistent_with_geographic_ais_observability_bias"
    assert row["source_intensity_declines_with_distance"]
    assert row["observed_zero_increases_with_distance"]
    assert row["clean_rows_per_turbine_month_far_near_ratio"] < 0.8
    assert row["observed_zero_rate_far_minus_near"] > 0


def test_downstream_only_gradient_does_not_become_source_observability_claim():
    matrix = _distance_diagnostic_farm_month(
        clean_rows=[100, 100, 100, 100, 100, 100, 100, 100],
        dwell_counts=[8, 7, 6, 5, 2, 1, 0, 0],
        observed_zero=[False, False, False, False, False, False, False, False],
    )
    diagnostic = build_base_station_distance_stratum_diagnostic(matrix)

    gradient = build_base_station_distance_gradient_summary(diagnostic)
    row = gradient.iloc[0]

    assert row["diagnostic_class"] == "downstream_proxy_only_gradient"
    assert not row["source_intensity_declines_with_distance"]
    assert row["downstream_proxy_declines_with_distance"]


def test_report_preserves_guardrails_and_no_failure_rate_claim():
    matrix = _farm_month()
    summary = build_farm_observability_bias_summary(matrix)
    inventory = build_receiver_metadata_inventory(
        {
            "raw_ais_schema": ["Timestamp", "Data source type", "MMSI"],
            "manifest": ["source_file_name", "input_rows", "clean_rows"],
        }
    )
    validation = {
        "farm_month_rows": len(matrix),
        "observed_source_months": int(matrix["observed_source_month_flag"].sum()),
        "missing_source_months": int(matrix["skipped_missing_source_flag"].sum()),
        "observed_zero_months": int(matrix["observed_zero_month_flag"].sum()),
        "source_clean_rows_total": int(matrix["source_clean_rows"].fillna(0).sum()),
        "ais_dwell_event_count_total": int(matrix["ais_dwell_event_count"].fillna(0).sum()),
        "schema_source_count": int(inventory["source_name"].nunique()),
        "raw_ais_file_count": 2,
        "raw_ais_data_source_type_file_count": 2,
        "base_station_mmsi_count": 1,
        "base_station_record_count": 3,
        "base_station_catalogue_rows": 1,
    }
    base_station_catalogue = pd.DataFrame(
        {
            "base_station_mmsi": ["station-alpha"],
            "base_station_latitude": [54.0],
            "base_station_longitude": [7.0],
            "observation_count": [3],
        }
    )
    distance_diagnostic = build_base_station_distance_stratum_diagnostic(
        _distance_diagnostic_farm_month()
    )
    distance_gradient = build_base_station_distance_gradient_summary(distance_diagnostic)
    report = build_report_text(
        farm_month=matrix,
        farm_summary=summary,
        receiver_inventory=inventory,
        geographic_summary=build_geographic_source_intensity_summary(matrix),
        validation=validation,
        base_station_catalogue=base_station_catalogue,
        distance_stratum_diagnostic=distance_diagnostic,
        distance_gradient_summary=distance_gradient,
    )

    assert "Only per-message receiver assignment can directly test receiver-distance bias" in report
    assert "No per-vessel-message receiver station ID or receiver assignment field was found" in report
    assert "`Type of mobile = Base Station` records" in report
    assert "not proof of which station received each vessel ping" in report
    assert "`skipped_missing_source` is missing source evidence, not zero events" in report
    assert "Do not infer receiver locations from vessel positions" in report
    assert "Do not claim the nearest observed base station received a vessel ping" in report
    assert "Candidate AIS rows are separated from dwell/event rows" in report
    assert "Matched Base-Station Distance Diagnostic" in report
    assert "evidence consistent with geographical AIS observability bias" not in report
    assert "Nearest observed AIS base station remains a source-geometry control only" in report
    assert not contains_ais_only_failure_rate_claim(report)
