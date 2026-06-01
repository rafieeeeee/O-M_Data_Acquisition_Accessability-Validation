import numpy as np
import pandas as pd
import xarray as xr

from om_pipeline.metocean.baltic_wave_materializer import (
    BALTIC_VARIABLE_MAP,
    build_baltic_materialization_plan,
    materialize_baltic_wave_timeseries,
)


def _write_tiny_baltic_raw(raw_root):
    farm_dir = raw_root / "Farm_A"
    farm_dir.mkdir(parents=True)
    path = farm_dir / "farm_a_wave.nc"
    times = pd.date_range("2020-01-01T00:00:00Z", periods=3, freq="h")
    latitudes = np.array([54.0, 54.1])
    longitudes = np.array([13.0, 13.1])
    shape = (len(times), len(latitudes), len(longitudes))
    data_vars = {}
    for idx, source_var in enumerate(BALTIC_VARIABLE_MAP.values(), start=1):
        data_vars[source_var] = (
            ("time", "latitude", "longitude"),
            np.full(shape, float(idx)),
        )
    ds = xr.Dataset(
        data_vars=data_vars,
        coords={
            "time": times.tz_convert(None).to_numpy(),
            "latitude": latitudes,
            "longitude": longitudes,
        },
    )
    ds.to_netcdf(path)
    return path


def _write_turbines(path):
    pd.DataFrame(
        [
            {
                "wind_farm": "Farm A",
                "latitude": 54.01,
                "longitude": 13.01,
                "commissioning_date": "2020-01",
            }
        ]
    ).to_csv(path, index=False)


def test_baltic_materialization_dry_run_reports_without_partition_writes(tmp_path):
    raw_root = tmp_path / "raw"
    output_root = tmp_path / "processed"
    turbines = tmp_path / "turbines.csv"
    report = tmp_path / "qa.md"
    _write_tiny_baltic_raw(raw_root)
    _write_turbines(turbines)

    result = materialize_baltic_wave_timeseries(
        raw_root=raw_root,
        output_root=output_root,
        turbine_coordinates_path=turbines,
        qa_report=report,
        dry_run=True,
    )

    assert not output_root.exists()
    assert report.exists()
    assert result.plan.iloc[0]["sample_point_count"] == 2
    assert result.plan.iloc[0]["expected_partitions"] == 1
    assert result.plan.iloc[0]["expected_rows"] == 6
    assert set(result.qa["materialization_status"]) == {"dry_run_planned"}


def test_baltic_materialization_writes_hourly_source_labelled_partition(tmp_path):
    raw_root = tmp_path / "raw"
    output_root = tmp_path / "processed"
    turbines = tmp_path / "turbines.csv"
    report = tmp_path / "qa.md"
    _write_tiny_baltic_raw(raw_root)
    _write_turbines(turbines)

    plan = build_baltic_materialization_plan(
        raw_root=raw_root,
        turbine_coordinates_path=turbines,
        output_root=output_root,
    )
    assert plan.iloc[0]["status"] == "ready"

    result = materialize_baltic_wave_timeseries(
        raw_root=raw_root,
        output_root=output_root,
        turbine_coordinates_path=turbines,
        qa_report=report,
        dry_run=False,
    )

    assert set(result.qa["materialization_status"]) == {"ok"}
    partition = output_root / "wind_farm=Farm_A" / "year=2020" / "part.parquet"
    assert partition.exists()
    out = pd.read_parquet(partition)
    assert len(out) == 6
    assert out["timestamp_utc"].nunique() == 3
    assert set(BALTIC_VARIABLE_MAP).issubset(out.columns)
    assert set(out["baltic_extraction_method"]) == {"nearest_valid_grid_hourly_wave_only"}
    assert (out["baltic_wave_dir"] >= 0).all()
    assert (out["baltic_wave_dir"] < 360).all()
    assert report.exists()
