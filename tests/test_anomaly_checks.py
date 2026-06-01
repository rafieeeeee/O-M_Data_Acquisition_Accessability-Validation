"""
test_anomaly_checks.py

Unit tests for anomaly_checks.py using isolated, synthetic DataFrame fixtures and manifests.
"""

import pytest
import pandas as pd
import numpy as np
from pathlib import Path

from om_pipeline.dashboard.anomaly_checks import run_anomaly_checks, DEFAULT_THRESHOLDS

@pytest.fixture
def mock_manifest_dir(tmp_path):
    """Sets up a mock directory structure with a synthetic processed backfill manifest."""
    manifest_dir = tmp_path / "Data/Processed/ais_dwell_backfill/logs"
    manifest_dir.mkdir(parents=True, exist_ok=True)
    return tmp_path

@pytest.fixture
def base_healthy_row():
    """Returns a single dictionary representing a perfectly healthy operational row."""
    return {
        "farm_id": "Wikinger",
        "farm_name": "Wikinger",
        "year": 2020,
        "month": 1,
        "is_target_cluster": True,
        "raw_source_status": "exists",
        "raw_source_rows": 1000,
        "raw_source_file_path": "AIS/Farm-Candidates_European-Master_2020_01_SogMax2.0_Buffer2.0nm.csv",
        "raw_source_file_size_mb": 15.5,
        "filtered_ais_status": "exists",
        "filtered_ais_rows": 800,
        "visits_status": "exists",
        "visits_count": 10,
        "dwells_status": "exists",
        "dwells_count": 8,
        "tier_a_count": 4,
        "tier_b_count": 2,
        "tier_c_count": 2,
        "tier_d_count": 0,
        "duplicate_status": "calculated",
        "duplicate_flag_count": 0,
        "weather_join_status": "joined",
        "weather_join_count": 8,
        "weather_missing_fraction": 0.0,
        "validation_status": "exists",
        "validation_event_count": 8,
        "fleet_registry_file_exists": True,
        "fleet_registry_vessel_count": 2,
        "qa_map_status": "exists",
        "qa_map_count": 1
    }

def test_empty_input():
    """Asserts that passing an empty DataFrame returns a clean, empty output structure."""
    df_empty = pd.DataFrame()
    df_res = run_anomaly_checks(df_empty)
    assert len(df_res) == 0
    assert list(df_res.columns) == [
        "Severity", "Wind Farm", "Year", "Month", 
        "Anomaly Type", "Evidence", "Suggested Action"
    ]

def test_healthy_partition(base_healthy_row, mock_manifest_dir):
    """Asserts that a perfectly normal, operational partition yields zero anomalies."""
    df_index = pd.DataFrame([base_healthy_row])
    df_res = run_anomaly_checks(df_index, root_path=mock_manifest_dir)
    assert len(df_res) == 0

def test_check_1_missing_raw_source(base_healthy_row, mock_manifest_dir):
    """Asserts missing raw source is flagged as Warning if operational & target, or Info if non-target."""
    # Partition is in 2020-01 (operational for Wikinger which is 2018-10)
    row_target = base_healthy_row.copy()
    row_target["raw_source_status"] = "missing"
    row_target["is_target_cluster"] = True
    
    df_res = run_anomaly_checks(pd.DataFrame([row_target]), root_path=mock_manifest_dir)
    assert len(df_res) == 1
    assert df_res.iloc[0]["Anomaly Type"] == "Missing Raw Source"
    assert df_res.iloc[0]["Severity"] == "Warning"
    
    # Non-target cluster flag -> Info
    row_nontarget = row_target.copy()
    row_nontarget["is_target_cluster"] = False
    row_nontarget["farm_id"] = "Nordsee_Ost"  # Nordsee Ost operational 2015-05
    row_nontarget["farm_name"] = "Nordsee Ost"
    df_res_nt = run_anomaly_checks(pd.DataFrame([row_nontarget]), root_path=mock_manifest_dir)
    assert len(df_res_nt) == 1
    assert df_res_nt.iloc[0]["Severity"] == "Info"

def test_check_1_missing_raw_scope_aware(base_healthy_row, mock_manifest_dir):
    """Asserts that missing raw source checks are scope-aware (pre-operational is ignored)."""
    # Wikinger operational date is 2018-10. Let's test 2015-05 (pre-operational).
    row_old = base_healthy_row.copy()
    row_old["year"] = 2015
    row_old["month"] = 5
    row_old["raw_source_status"] = "missing"
    
    df_res = run_anomaly_checks(pd.DataFrame([row_old]), root_path=mock_manifest_dir)
    assert len(df_res) == 0  # pre-operational slot is gracefully ignored!

def test_check_2_interrupted_filtered_ais(base_healthy_row, mock_manifest_dir):
    """Asserts interrupted filtered AIS slices on valid raw files are flagged as Critical if manifest says success."""
    row = base_healthy_row.copy()
    row["filtered_ais_status"] = "interrupted"
    
    # Write mock manifest showing success
    df_man = pd.DataFrame([{
        "farm_id": "Wikinger", "year": 2020, "month": 1, "status": "success"
    }])
    df_man.to_csv(mock_manifest_dir / "Data/Processed/ais_dwell_backfill/logs/backfill_manifest.csv", index=False)
    
    df_res = run_anomaly_checks(pd.DataFrame([row]), root_path=mock_manifest_dir)
    assert len(df_res) == 1
    assert df_res.iloc[0]["Anomaly Type"] == "Interrupted Filtered AIS"
    assert df_res.iloc[0]["Severity"] == "Critical"

def test_check_3_raw_exists_visits_missing_critical(base_healthy_row, mock_manifest_dir):
    """Asserts raw source exists but visits are missing yields Critical anomaly if manifest status is success."""
    row = base_healthy_row.copy()
    row["visits_status"] = "missing"
    row["visits_count"] = 0
    
    # Write mock manifest showing success
    df_man = pd.DataFrame([{
        "farm_id": "Wikinger", "year": 2020, "month": 1, "status": "success"
    }])
    df_man.to_csv(mock_manifest_dir / "Data/Processed/ais_dwell_backfill/logs/backfill_manifest.csv", index=False)
    
    df_res = run_anomaly_checks(pd.DataFrame([row]), root_path=mock_manifest_dir)
    assert len(df_res) == 1
    assert df_res.iloc[0]["Anomaly Type"] == "Missing Visits Output"
    assert df_res.iloc[0]["Severity"] == "Critical"

def test_check_3_raw_exists_visits_missing_warning(base_healthy_row, mock_manifest_dir):
    """Asserts raw source exists but visits are missing yields Warning (backlog) if manifest has no record."""
    row = base_healthy_row.copy()
    row["visits_status"] = "missing"
    row["visits_count"] = 0
    
    # No manifest loaded -> Treated as unprocessed backlog
    df_res = run_anomaly_checks(pd.DataFrame([row]), root_path=mock_manifest_dir)
    assert len(df_res) == 1
    assert df_res.iloc[0]["Anomaly Type"] == "Missing Visits Output"
    assert df_res.iloc[0]["Severity"] == "Warning"  # Warning because Wikinger is target-cluster!

def test_check_3_raw_exists_visits_missing_ignored(base_healthy_row, mock_manifest_dir):
    """Asserts raw source exists but visits are missing is completely ignored if manifest status is success_no_ais_in_bbox."""
    row = base_healthy_row.copy()
    row["visits_status"] = "missing"
    row["visits_count"] = 0
    
    # Write mock manifest showing success_no_ais_in_bbox (expected empty bbox)
    df_man = pd.DataFrame([{
        "farm_id": "Wikinger", "year": 2020, "month": 1, "status": "success_no_ais_in_bbox"
    }])
    df_man.to_csv(mock_manifest_dir / "Data/Processed/ais_dwell_backfill/logs/backfill_manifest.csv", index=False)
    
    df_res = run_anomaly_checks(pd.DataFrame([row]), root_path=mock_manifest_dir)
    assert len(df_res) == 0  # Ignored by design because manifest reports expected empty bounding box!

def test_check_4_visits_exist_dwells_missing(base_healthy_row, mock_manifest_dir):
    """Asserts visits exist but dwells are missing yields Critical anomaly if manifest status is success."""
    row = base_healthy_row.copy()
    row["dwells_status"] = "missing"
    row["dwells_count"] = 0
    
    df_man = pd.DataFrame([{
        "farm_id": "Wikinger", "year": 2020, "month": 1, "status": "success"
    }])
    df_man.to_csv(mock_manifest_dir / "Data/Processed/ais_dwell_backfill/logs/backfill_manifest.csv", index=False)
    
    df_res = run_anomaly_checks(pd.DataFrame([row]), root_path=mock_manifest_dir)
    assert len(df_res) == 1
    assert df_res.iloc[0]["Anomaly Type"] == "Missing Dwells Output"
    assert df_res.iloc[0]["Severity"] == "Critical"

def test_check_5_dwells_exist_weather_missing(base_healthy_row, mock_manifest_dir):
    """Asserts missing weather join for active dwells is a Warning for <=2024 or Info for >2024."""
    # Case 1: 2020-01 (<= 2024 NORA3 availability) -> Warning
    row_2020 = base_healthy_row.copy()
    row_2020["weather_join_status"] = "missing"
    row_2020["weather_join_count"] = 0
    
    df_res = run_anomaly_checks(pd.DataFrame([row_2020]), root_path=mock_manifest_dir)
    assert len(df_res) == 1
    assert df_res.iloc[0]["Anomaly Type"] == "Missing Weather Join"
    assert df_res.iloc[0]["Severity"] == "Warning"
    
    # Case 2: 2025-05 (> 2024 optional metocean status) -> Info
    row_2025 = row_2020.copy()
    row_2025["year"] = 2025
    df_res_2025 = run_anomaly_checks(pd.DataFrame([row_2025]), root_path=mock_manifest_dir)
    assert len(df_res_2025) == 1
    assert df_res_2025.iloc[0]["Severity"] == "Info"

def test_check_6_high_duplicate_flags(base_healthy_row, mock_manifest_dir):
    """Asserts high duplicate rates >30% yield Warning anomaly."""
    row = base_healthy_row.copy()
    row["dwells_count"] = 10
    row["duplicate_flag_count"] = 4  # 40% duplicate rate
    
    df_res = run_anomaly_checks(pd.DataFrame([row]), root_path=mock_manifest_dir)
    assert len(df_res) == 1
    assert df_res.iloc[0]["Anomaly Type"] == "High Duplicate Share"
    assert df_res.iloc[0]["Severity"] == "Warning"

def test_check_7_high_tier_d_share(base_healthy_row, mock_manifest_dir):
    """Asserts high Tier D transit/drifting shares >40% yield Info anomaly."""
    row = base_healthy_row.copy()
    row["dwells_count"] = 10
    row["tier_d_count"] = 5  # 50% Tier D share
    
    df_res = run_anomaly_checks(pd.DataFrame([row]), root_path=mock_manifest_dir)
    assert len(df_res) == 1
    assert df_res.iloc[0]["Anomaly Type"] == "High Tier D Share"
    assert df_res.iloc[0]["Severity"] == "Info"

def test_check_8_high_dwell_to_visit_ratio(base_healthy_row, mock_manifest_dir):
    """Asserts unusually high dwell-to-visit ratio >2.0x yields Warning anomaly."""
    row = base_healthy_row.copy()
    row["visits_count"] = 5
    row["dwells_count"] = 15  # 3.0x ratio
    
    df_res = run_anomaly_checks(pd.DataFrame([row]), root_path=mock_manifest_dir)
    assert len(df_res) == 1
    assert df_res.iloc[0]["Anomaly Type"] == "High Dwell-to-Visit Ratio"
    assert df_res.iloc[0]["Severity"] == "Warning"

def test_check_9_zero_row_output(base_healthy_row, mock_manifest_dir):
    """Asserts source exists but pipeline phase contains 0 rows yields Warning anomaly."""
    row = base_healthy_row.copy()
    row["dwells_count"] = 0
    
    df_res = run_anomaly_checks(pd.DataFrame([row]), root_path=mock_manifest_dir)
    assert len(df_res) == 1
    assert df_res.iloc[0]["Anomaly Type"] == "Zero-Row Pipeline Output"
    assert df_res.iloc[0]["Severity"] == "Warning"

def test_check_10_high_weather_missingness(base_healthy_row, mock_manifest_dir):
    """Asserts joined weather features with >25% missing fraction yield Warning anomaly."""
    row = base_healthy_row.copy()
    row["weather_missing_fraction"] = 0.35  # 35% weather missingness
    
    df_res = run_anomaly_checks(pd.DataFrame([row]), root_path=mock_manifest_dir)
    assert len(df_res) == 1
    assert df_res.iloc[0]["Anomaly Type"] == "High Weather Missingness"
    assert df_res.iloc[0]["Severity"] == "Warning"

def test_threshold_overrides(base_healthy_row, mock_manifest_dir):
    """Asserts threshold override parameters successfully adjust anomaly trigger points."""
    row = base_healthy_row.copy()
    row["dwells_count"] = 10
    row["duplicate_flag_count"] = 2  # 20% duplicate rate (healthy under default 30%)
    
    # Check 1: Default thresholds (No anomaly)
    df_res_def = run_anomaly_checks(pd.DataFrame([row]), root_path=mock_manifest_dir)
    assert len(df_res_def) == 0
    
    # Check 2: Custom threshold override (duplicate_threshold=0.1) -> Anomaly flagged!
    df_res_override = run_anomaly_checks(
        pd.DataFrame([row]), 
        thresholds={"duplicate_threshold": 0.1},
        root_path=mock_manifest_dir
    )
    assert len(df_res_override) == 1
    assert df_res_override.iloc[0]["Anomaly Type"] == "High Duplicate Share"
