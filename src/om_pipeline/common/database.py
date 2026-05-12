import duckdb
import os
from .paths import DATA_DIR

CATALOG_PATH = os.path.join(DATA_DIR, "catalog.duckdb")

def get_connection(read_only=False):
    """Get a connection to the DuckDB catalog."""
    return duckdb.connect(CATALOG_PATH, read_only=read_only)

def register_data_source(con, table_name, file_path):
    """
    Register a CSV or Parquet file as a DuckDB view.
    This allows querying the files using SQL without importing them into the DB.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Data source file not found: {file_path}")
        
    ext = os.path.splitext(file_path)[1].lower()
    if ext == '.csv':
        con.execute(f"CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM read_csv_auto('{file_path}')")
    elif ext == '.parquet':
        con.execute(f"CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM read_parquet('{file_path}')")
    else:
        raise ValueError(f"Unsupported file extension for DuckDB registration: {ext}")
    
    print(f"Registered view '{table_name}' for {file_path}")

def list_views(con):
    """List all registered views in the catalog."""
    return con.execute("SELECT table_name FROM information_schema.views WHERE table_schema='main'").fetchall()
