import importlib.util
from pathlib import Path

import pytest

from om_pipeline.common.database import get_connection, list_views, register_data_source


def _write_csv(path, rows):
    path.write_text("\n".join(rows) + "\n", encoding="utf-8")


def _load_init_catalog():
    script_path = Path(__file__).resolve().parents[1] / "scripts" / "init_catalog.py"
    spec = importlib.util.spec_from_file_location("init_catalog_script", script_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.init_catalog


def test_register_data_source_creates_queryable_csv_view(tmp_path):
    csv_path = tmp_path / "events.csv"
    _write_csv(csv_path, ["event_id,MMSI", "1,123456789"])

    con = get_connection(catalog_path=tmp_path / "catalog.duckdb")
    try:
        register_data_source(con, "dwell_events", csv_path)

        assert list_views(con) == [("dwell_events",)]
        assert con.execute("SELECT event_id, MMSI FROM dwell_events").fetchall() == [
            (1, 123456789)
        ]
    finally:
        con.close()


def test_register_data_source_escapes_source_paths(tmp_path):
    csv_path = tmp_path / "events'pilot.csv"
    _write_csv(csv_path, ["event_id,MMSI", "2,987654321"])

    con = get_connection(catalog_path=tmp_path / "catalog.duckdb")
    try:
        register_data_source(con, "dwell_events", csv_path)

        assert con.execute("SELECT event_id FROM dwell_events").fetchone() == (2,)
    finally:
        con.close()


@pytest.mark.parametrize("view_name", ["bad-name", "1bad", "events; DROP TABLE x"])
def test_register_data_source_rejects_unsafe_view_names(tmp_path, view_name):
    csv_path = tmp_path / "events.csv"
    _write_csv(csv_path, ["event_id", "1"])

    con = get_connection(catalog_path=tmp_path / "catalog.duckdb")
    try:
        with pytest.raises(ValueError, match="Unsafe DuckDB view name"):
            register_data_source(con, view_name, csv_path)
    finally:
        con.close()


def test_register_data_source_reports_missing_files(tmp_path):
    con = get_connection(catalog_path=tmp_path / "catalog.duckdb")
    try:
        with pytest.raises(FileNotFoundError, match="Data source file not found"):
            register_data_source(con, "dwell_events", tmp_path / "missing.csv")
    finally:
        con.close()


def test_init_catalog_registers_pilot_outputs_and_optional_raw_ais(tmp_path):
    turbine_csv = tmp_path / "European_Turbine_Coordinates.csv"
    events_csv = tmp_path / "OM_Events_Test_2024_07.csv"
    registry_csv = tmp_path / "Fleet_Registry_Test_2024_07.csv"
    raw_ais_csv = tmp_path / "AIS_Test_2024_07.csv"
    catalog_path = tmp_path / "catalog.duckdb"

    _write_csv(turbine_csv, ["found_id,latitude,longitude", "AV01,54.0,6.0"])
    _write_csv(events_csv, ["event_id,MMSI,found_id", "1,123456789,AV01"])
    _write_csv(registry_csv, ["MMSI,wind_farm,Event_Count", "123456789,Alpha Ventus,1"])
    _write_csv(raw_ais_csv, ["MMSI,Latitude,Longitude,SOG", "123456789,54.0,6.0,0.1"])

    init_catalog = _load_init_catalog()
    registered_count = init_catalog(
        catalog_path=catalog_path,
        turbine_csv=turbine_csv,
        events_glob=str(tmp_path / "OM_Events_*.csv"),
        registry_glob=str(tmp_path / "Fleet_Registry_*.csv"),
        raw_ais_glob=raw_ais_csv,
        include_raw_ais=True,
    )

    con = get_connection(catalog_path=catalog_path)
    try:
        assert registered_count == 4
        assert list_views(con) == [
            ("ais_raw",),
            ("dwell_events",),
            ("fleet_registry",),
            ("turbines",),
        ]
        assert con.execute("SELECT COUNT(*) FROM turbines").fetchone() == (1,)
        assert con.execute("SELECT COUNT(*) FROM ais_raw").fetchone() == (1,)
    finally:
        con.close()


def test_init_catalog_reports_missing_required_sources(tmp_path):
    init_catalog = _load_init_catalog()

    with pytest.raises(FileNotFoundError, match="missing required source files"):
        init_catalog(
            catalog_path=tmp_path / "catalog.duckdb",
            turbine_csv=tmp_path / "missing_turbines.csv",
            events_glob=str(tmp_path / "OM_Events_*.csv"),
            registry_glob=str(tmp_path / "Fleet_Registry_*.csv"),
        )
