import pytest
import pandas as pd
import numpy as np
from src.om_pipeline.analysis.workability import (
    WorkabilitySurfaceBin,
    WorkabilitySurfaceSpec,
    build_workability_surface_table,
    classify_vessel,
    compute_tp_bin_statistics,
    generate_workability_lookup_matrix,
    hs_tp_surface_spec,
)

def test_classify_vessel():
    # Test NaN and invalid cases
    assert classify_vessel(np.nan) == 'Unclassified (Missing Length)'
    assert classify_vessel(0.0) == 'Unclassified (Missing Length)'
    assert classify_vessel(-10.0) == 'Unclassified (Missing Length)'
    
    # Test CTV case
    assert classify_vessel(20.0) == 'CTV (<40m)'
    assert classify_vessel(39.9) == 'CTV (<40m)'
    
    # Test Medium case
    assert classify_vessel(40.0) == 'Medium-sized Vessel'
    assert classify_vessel(50.0) == 'Medium-sized Vessel'
    assert classify_vessel(59.9) == 'Medium-sized Vessel'
    
    # Test SOV case
    assert classify_vessel(60.0) == 'SOV (>=60m)'
    assert classify_vessel(85.0) == 'SOV (>=60m)'

def test_compute_tp_bin_statistics():
    # Create mock dwell events DataFrame
    mock_data = pd.DataFrame({
        'dwell_tier': ['Tier A'] * 10 + ['Tier B'] * 2,
        'vessel_class': ['CTV (<40m)'] * 6 + ['SOV (>=60m)'] * 6,
        'active_hs_mean': [0.5, 0.6, 0.7, 0.8, 1.2, 1.5, 1.8, 2.0, 2.2, 2.5, 3.0, 3.5],
        'active_tp_mean': [3.5, 3.8, 4.2, 4.5, 5.2, 5.8, 6.2, 6.5, 7.2, 8.5, 9.5, 11.0]
    })
    
    # Run overall stats (computes stats strictly for Tier A dwells)
    stats = compute_tp_bin_statistics(mock_data)
    assert not stats.empty
    assert 'tp_bin' in stats.columns
    assert 'count' in stats.columns
    assert 'boundary_hs' in stats.columns
    
    # Filter stats to specific Tp bins
    bin_3_4 = stats[stats['tp_bin'] == '(3, 4]'].iloc[0]
    assert bin_3_4['count'] == 2 # 3.5, 3.8 are Tier A
    assert np.isclose(bin_3_4['median_hs'], 0.55)
    
    # Run with vessel class filter
    ctv_stats = compute_tp_bin_statistics(mock_data, "CTV (<40m)")
    assert not ctv_stats.empty
    # CTV stats should only count Tier A dwells with CTV class (index 0-5)
    assert ctv_stats['count'].sum() == 6

def test_generate_workability_lookup_matrix():
    # Create mock dwells
    np.random.seed(42)
    n_rows = 50
    mock_data = pd.DataFrame({
        'dwell_tier': ['Tier A'] * n_rows,
        'vessel_class': ['CTV (<40m)'] * n_rows,
        'active_hs_mean': np.random.uniform(0.5, 2.5, n_rows),
        'active_tp_mean': np.random.uniform(3.0, 10.0, n_rows)
    })
    
    matrix = generate_workability_lookup_matrix(mock_data)
    assert not matrix.empty
    assert matrix.index.name == 'hs'
    assert len(matrix.index) == 51 # 0.0 to 5.0
    assert len(matrix.columns) == 27 # 2.0 to 15.0
    
    # Assert probabilities are valid floats between 0.0 and 1.0
    for col in matrix.columns:
        vals = matrix[col].dropna()
        assert (vals >= 0.0).all()
        assert (vals <= 1.0).all()


def test_hs_tp_surface_is_default_preset():
    mock_data = pd.DataFrame({
        'dwell_tier': ['Tier A', 'Tier A', 'Tier B'],
        'vessel_class': ['CTV (<40m)', 'SOV (>=60m)', 'CTV (<40m)'],
        'active_hs_mean': [0.5, 1.1, 2.0],
        'active_tp_mean': [4.0, 5.5, 8.0],
    })

    surface = build_workability_surface_table(mock_data)

    assert hs_tp_surface_spec().name == "hs_tp"
    assert set(surface["surface_name"]) == {"hs_tp"}
    assert set(surface["feature_columns"]) == {"active_hs_mean|active_tp_mean"}
    assert {"hs_bin", "tp_bin", "observed_count"}.issubset(surface.columns)
    assert surface["complete_feature_rows"].iloc[0] == 2


def test_custom_surface_supports_hs_tp_wind_speed_dimensions():
    mock_data = pd.DataFrame({
        'dwell_tier': ['Tier A', 'Tier A', 'Tier A', 'Tier B'],
        'vessel_class': ['CTV (<40m)', 'CTV (<40m)', 'SOV (>=60m)', 'SOV (>=60m)'],
        'active_hs_mean': [0.5, 1.1, 1.7, 2.0],
        'active_tp_mean': [4.0, 5.5, 6.2, 8.0],
        'active_wind_speed_mean': [6.0, 8.5, 12.0, 15.0],
    })
    spec = WorkabilitySurfaceSpec(
        name="hs_tp_wind_speed",
        bins=(
            WorkabilitySurfaceBin("active_hs_mean", (0.0, 1.0, 2.0), name="hs", unit="m"),
            WorkabilitySurfaceBin("active_tp_mean", (0.0, 5.0, 10.0), name="tp", unit="s"),
            WorkabilitySurfaceBin(
                "active_wind_speed_mean",
                (0.0, 10.0, 20.0),
                name="wind_speed",
                unit="m/s",
            ),
        ),
        grouping_columns=("vessel_class",),
    )

    surface = build_workability_surface_table(mock_data, spec)

    assert set(surface["surface_name"]) == {"hs_tp_wind_speed"}
    assert set(surface["feature_columns"]) == {
        "active_hs_mean|active_tp_mean|active_wind_speed_mean"
    }
    assert {
        "vessel_class",
        "hs_bin",
        "tp_bin",
        "wind_speed_bin",
        "wind_speed_feature_column",
        "observed_count",
    }.issubset(surface.columns)
    assert surface["observed_count"].sum() == 3


def test_current_speed_nulls_are_excluded_not_zero_filled():
    mock_data = pd.DataFrame({
        'dwell_tier': ['Tier A', 'Tier A', 'Tier A'],
        'active_hs_mean': [0.4, 0.8, 1.2],
        'active_tp_mean': [4.0, 5.0, 6.0],
        'active_current_speed_mean': [None, 0.35, None],
    })
    spec = WorkabilitySurfaceSpec(
        name="hs_tp_current_speed",
        bins=(
            WorkabilitySurfaceBin("active_hs_mean", (0.0, 1.0, 2.0), name="hs", unit="m"),
            WorkabilitySurfaceBin("active_tp_mean", (0.0, 5.0, 10.0), name="tp", unit="s"),
            WorkabilitySurfaceBin(
                "active_current_speed_mean",
                (0.0, 0.5, 1.0),
                name="current_speed",
                unit="m/s",
            ),
        ),
    )

    surface = build_workability_surface_table(mock_data, spec)

    assert surface["excluded_null_feature_rows"].iloc[0] == 2
    assert surface["complete_feature_rows"].iloc[0] == 1
    assert surface["observed_count"].sum() == 1
    observed = surface[surface["observed_count"] > 0]
    assert observed["current_speed_bin_left"].min() == 0.0
    assert observed["current_speed_bin_right"].max() == 0.5
