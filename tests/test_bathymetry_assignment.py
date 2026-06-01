from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from om_pipeline.metocean.bathymetry_assignment import (
    BATHYMETRY_OUTPUT_SCHEMA,
    assign_bathymetry_to_metocean_points,
    build_common_metocean_sample_points,
)


class FakeDepthClient:
    def __init__(self):
        self.calls = []

    def fetch(self, lon: float, lat: float):
        self.calls.append((lon, lat))
        raw_depth = -(20.0 + abs(float(lon)) + abs(float(lat)) / 100.0)
        return {
            "ok": True,
            "status_code": 200,
            "geom": f"POINT({lon:.8f} {lat:.8f})",
            "url": "https://example.test/depth_sample",
            "payload": {
                "avg": raw_depth,
                "smoothed": raw_depth + 0.1,
                "reference": {
                    "organisation_id": 574,
                    "identifier": "TEST-REF",
                    "type": "CDI",
                    "metadata_url": "https://example.test/metadata",
                },
            },
            "error": None,
            "attempts": 1,
        }


def _write_inputs(tmp_path: Path) -> tuple[Path, Path]:
    requirements = tmp_path / "requirements.csv"
    turbines = tmp_path / "turbines.csv"
    pd.DataFrame(
        [
            {
                "wind_farm": "Example Farm",
                "farm_id": "Example_Farm",
                "country": "Germany",
                "min_lon": 6.0,
                "max_lon": 6.2,
                "min_lat": 54.0,
                "max_lat": 54.2,
                "sample_point_strategy": "farm_centroid_and_turbines",
                "sample_point_count": 3,
                "review_required": False,
            }
        ]
    ).to_csv(requirements, index=False)
    pd.DataFrame(
        [
            {
                "wind_farm": "Example Farm",
                "latitude": 54.0,
                "longitude": 6.0,
                "country": "Germany",
            },
            {
                "wind_farm": "Example Farm",
                "latitude": 54.2,
                "longitude": 6.2,
                "country": "Germany",
            },
        ]
    ).to_csv(turbines, index=False)
    return requirements, turbines


def test_build_common_metocean_sample_points_expands_centroid_and_turbines(tmp_path):
    requirements, turbines = _write_inputs(tmp_path)

    points = build_common_metocean_sample_points(
        requirements_path=requirements,
        turbine_coordinates_path=turbines,
    )

    assert len(points) == 3
    assert list(points["sample_point_id"]) == ["farm_centroid", "turbine_0000", "turbine_0001"]
    centroid = points.iloc[0]
    assert centroid["sample_point_type"] == "farm_centroid"
    assert centroid["lat"] == pytest.approx(54.1)
    assert centroid["lon"] == pytest.approx(6.1)


def test_assign_bathymetry_writes_processed_table_metadata_cache_and_report(tmp_path):
    requirements, turbines = _write_inputs(tmp_path)
    source_root = tmp_path / "raw_bathymetry"
    output_dir = tmp_path / "processed_bathymetry"
    report = tmp_path / "report.md"
    client = FakeDepthClient()

    result = assign_bathymetry_to_metocean_points(
        requirements_path=requirements,
        turbine_coordinates_path=turbines,
        source_root=source_root,
        output_root=output_dir,
        qa_report=report,
        no_overwrite=True,
        client=client,
        max_workers=2,
    )

    assert result.output_path.exists()
    assert result.processed_metadata_path.exists()
    assert result.source_cache_path.exists()
    assert report.exists()
    written = pd.read_parquet(result.output_path)
    assert list(written.columns) == BATHYMETRY_OUTPUT_SCHEMA
    assert len(written) == 3
    assert written["water_depth_m"].notna().all()
    assert (written["water_depth_m"] > 0).all()
    assert set(written["bathymetry_source"]) == {"emodnet"}
    assert written.duplicated(["wind_farm", "sample_point_id"]).sum() == 0
    assert result.qa["missing_depth_count"] == 0
    assert result.qa["fallback_row_count"] == 0
    assert "Bathymetry Assignment Full Report" in report.read_text(encoding="utf-8")


def test_assignment_no_overwrite_blocks_before_fetching(tmp_path):
    requirements, turbines = _write_inputs(tmp_path)
    output_dir = tmp_path / "processed_bathymetry"
    output_dir.mkdir()
    (output_dir / "site_bathymetry_points.parquet").write_text("existing", encoding="utf-8")
    client = FakeDepthClient()

    with pytest.raises(FileExistsError, match="already exists"):
        assign_bathymetry_to_metocean_points(
            requirements_path=requirements,
            turbine_coordinates_path=turbines,
            source_root=tmp_path / "raw_bathymetry",
            output_root=output_dir,
            qa_report=None,
            no_overwrite=True,
            client=client,
        )

    assert client.calls == []


def test_assignment_reuses_existing_source_cache(tmp_path):
    requirements, turbines = _write_inputs(tmp_path)
    source_root = tmp_path / "raw_bathymetry"
    output_dir = tmp_path / "processed_bathymetry"
    client = FakeDepthClient()

    first = assign_bathymetry_to_metocean_points(
        requirements_path=requirements,
        turbine_coordinates_path=turbines,
        source_root=source_root,
        output_root=output_dir,
        qa_report=None,
        no_overwrite=False,
        client=client,
        max_workers=2,
    )
    first_call_count = len(client.calls)

    second_client = FakeDepthClient()
    second = assign_bathymetry_to_metocean_points(
        requirements_path=requirements,
        turbine_coordinates_path=turbines,
        source_root=source_root,
        output_root=output_dir,
        qa_report=None,
        no_overwrite=False,
        client=second_client,
        max_workers=2,
    )

    assert first_call_count == 3
    assert len(second_client.calls) == 1  # preflight source access only
    assert len(pd.read_parquet(second.output_path)) == len(pd.read_parquet(first.output_path))
    assert len(second.source_cache_path.read_text(encoding="utf-8").strip().splitlines()) == 3


def test_assignment_metadata_json_is_parseable(tmp_path):
    requirements, turbines = _write_inputs(tmp_path)

    result = assign_bathymetry_to_metocean_points(
        requirements_path=requirements,
        turbine_coordinates_path=turbines,
        source_root=tmp_path / "raw_bathymetry",
        output_root=tmp_path / "processed_bathymetry",
        qa_report=None,
        no_overwrite=True,
        client=FakeDepthClient(),
    )

    metadata = json.loads(result.processed_metadata_path.read_text(encoding="utf-8"))
    assert metadata["source_name"] == "EMODnet Bathymetry"
    assert metadata["fallback_status"] == "not_fetched_no_emodnet_gaps"
    assert metadata["processed_depth_sign_convention"] == "positive_down_meters_in_processed_table"
