"""
test_qa_viewer.py

Unit tests for qa_viewer.py utilizing isolated mock workspaces under pytest.
"""

import sys
from pathlib import Path
import pandas as pd
import pytest

from om_pipeline.dashboard.qa_viewer import load_qa_samples, resolve_qa_image_path, QA_SCHEMA_COLUMNS

@pytest.fixture
def mock_qa_workspace(tmp_path):
    """Sets up a synthetic directory tree mocking visual QA catalogs and maps."""
    qa_dir = tmp_path / "reports/ais_dwell"
    qa_dir.mkdir(parents=True, exist_ok=True)
    
    # 1. Write visual_qa_sample.csv
    df1 = pd.DataFrame({
        "qa_id": ["qa_dw_1"],
        "farm_id": ["Nordsee Ost"],
        "visit_id": ["v1"],
        "dwell_id": ["d1"],
        "vessel_id": [211000000],
        "dwell_tier": ["Tier B"],
        "start_utc": ["2020-01-01 00:00:00"],
        "end_utc": ["2020-01-01 01:00:00"],
        "duration_min": [60.0],
        "map_path": ["reports/ais_dwell/qa_maps/qa_dw_1.png"],
        "automated_reason": ["Tier sample: Tier B"]
    })
    df1.to_csv(qa_dir / "visual_qa_sample.csv", index=False)
    
    # 2. Write wikinger_visual_qa_sample.csv
    df2 = pd.DataFrame({
        "qa_id": ["qa_dw_2"],
        "farm_id": ["Wikinger"],
        "visit_id": ["v2"],
        "dwell_id": ["d2"],
        "vessel_id": [211000000],
        "dwell_tier": ["Tier A"],
        "start_utc": ["2020-01-01 02:00:00"],
        "end_utc": ["2020-01-01 03:00:00"],
        "duration_min": [60.0],
        "map_path": ["reports/ais_dwell/qa_maps_backfill/qa_dw_2.png"],
        "automated_reason": ["Tier sample: Tier A"]
    })
    df2.to_csv(qa_dir / "wikinger_visual_qa_sample.csv", index=False)
    
    # 3. Create dummy image files
    map_dir1 = qa_dir / "qa_maps"
    map_dir1.mkdir(parents=True, exist_ok=True)
    (map_dir1 / "qa_dw_1.png").touch()
    
    map_dir2 = qa_dir / "qa_maps_backfill"
    map_dir2.mkdir(parents=True, exist_ok=True)
    (map_dir2 / "qa_dw_2.png").touch()
    
    return tmp_path

def test_load_qa_samples_empty(tmp_path):
    """Asserts that an empty isolated workspace returns a clean empty catalog with correct schema."""
    df_res = load_qa_samples(tmp_path)
    assert len(df_res) == 0
    assert list(df_res.columns) == QA_SCHEMA_COLUMNS

def test_load_qa_samples_consolidation(mock_qa_workspace):
    """Asserts that both CSV samples are consolidated and duplicate rows dropped."""
    df_res = load_qa_samples(mock_qa_workspace)
    assert len(df_res) == 2
    assert set(df_res["qa_id"]) == {"qa_dw_1", "qa_dw_2"}
    assert list(df_res.columns) == QA_SCHEMA_COLUMNS
    # Verify metadata fields mapped
    assert df_res.loc[df_res["qa_id"] == "qa_dw_1", "dwell_tier"].values[0] == "Tier B"
    assert df_res.loc[df_res["qa_id"] == "qa_dw_2", "dwell_tier"].values[0] == "Tier A"

def test_resolve_qa_image_path_success(mock_qa_workspace):
    """Asserts that resolve_qa_image_path correctly returns absolute Path for existing files."""
    df_res = load_qa_samples(mock_qa_workspace)
    rel_path_1 = df_res.loc[df_res["qa_id"] == "qa_dw_1", "map_path"].values[0]
    
    abs_path_1 = resolve_qa_image_path(mock_qa_workspace, rel_path_1)
    assert abs_path_1 is not None
    assert abs_path_1.exists()
    assert abs_path_1.is_file()
    assert abs_path_1 == (mock_qa_workspace / "reports/ais_dwell/qa_maps/qa_dw_1.png").resolve()

def test_resolve_qa_image_path_missing(mock_qa_workspace):
    """Asserts that resolve_qa_image_path returns None for missing image files."""
    abs_path = resolve_qa_image_path(mock_qa_workspace, "reports/ais_dwell/qa_maps/missing.png")
    assert abs_path is None
    
    # Test malformed input string
    assert resolve_qa_image_path(mock_qa_workspace, None) is None
    assert resolve_qa_image_path(mock_qa_workspace, "") is None
