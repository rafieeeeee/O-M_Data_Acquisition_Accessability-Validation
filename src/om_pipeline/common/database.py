import glob
import os
import re
from pathlib import Path

import duckdb

from .paths import DATA_DIR

CATALOG_PATH = os.path.join(DATA_DIR, "catalog.duckdb")

_SAFE_VIEW_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def get_connection(read_only=False, catalog_path=None):
    """Get a connection to the DuckDB catalog."""
    db_path = os.fspath(catalog_path or CATALOG_PATH)
    if not read_only:
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(db_path, read_only=read_only)


def quote_view_name(view_name):
    """Return a safely quoted DuckDB view name."""
    if not isinstance(view_name, str) or not _SAFE_VIEW_NAME.fullmatch(view_name):
        raise ValueError(
            "Unsafe DuckDB view name. Use letters, numbers, and underscores, "
            "starting with a letter or underscore."
        )
    return f'"{view_name}"'


def _sql_string_literal(value):
    return "'" + os.fspath(value).replace("'", "''") + "'"


def _source_matches(file_path):
    path_text = os.fspath(file_path)
    if glob.has_magic(path_text):
        return sorted(glob.glob(path_text))
    if os.path.exists(path_text):
        return [path_text]
    return []


def _source_extension(matches):
    extensions = {Path(match).suffix.lower() for match in matches}
    if len(extensions) != 1:
        raise ValueError(
            "DuckDB view registration expects sources with one shared extension; "
            f"found {sorted(extensions)}"
        )
    return extensions.pop()

def register_data_source(con, table_name, file_path):
    """
    Register a CSV or Parquet file as a DuckDB view.
    This allows querying the files using SQL without importing them into the DB.
    """
    matches = _source_matches(file_path)
    if not matches:
        raise FileNotFoundError(
            f"Data source file not found for view '{table_name}': {file_path}"
        )

    quoted_view_name = quote_view_name(table_name)
    source_literal = _sql_string_literal(file_path)
    ext = _source_extension(matches)
    if ext == '.csv':
        con.execute(
            f"CREATE OR REPLACE VIEW {quoted_view_name} AS "
            f"SELECT * FROM read_csv_auto({source_literal}, union_by_name=true)"
        )
    elif ext == '.parquet':
        con.execute(
            f"CREATE OR REPLACE VIEW {quoted_view_name} AS "
            f"SELECT * FROM read_parquet({source_literal})"
        )
    else:
        raise ValueError(f"Unsupported file extension for DuckDB registration: {ext}")

    print(f"Registered view '{table_name}' for {file_path}")
    return table_name

def list_views(con):
    """List all registered views in the catalog."""
    return con.execute(
        "SELECT view_name FROM duckdb_views() "
        "WHERE schema_name='main' AND NOT internal "
        "ORDER BY view_name"
    ).fetchall()
