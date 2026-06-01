#!/usr/bin/env python3
import os
import sys
import duckdb
from pathlib import Path
import argparse

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from om_pipeline.common.paths import DATA_DIR
from om_pipeline.analysis.plot_workability import (
    generate_workability_ecdf,
    generate_workability_boxplots,
    generate_oss_control_matrix
)

DB_PATH = os.path.join(DATA_DIR, "catalog.duckdb")
OUTPUT_DIR = os.path.join(DATA_DIR, "Processed", "figures")

def main():
    parser = argparse.ArgumentParser(description="Generate Empirical Workability Surfaces")
    parser.add_argument("--db-path", default=DB_PATH, help="Path to DuckDB catalog")
    parser.add_argument("--out-dir", default=OUTPUT_DIR, help="Output directory for plots")
    args = parser.parse_args()
    
    if not os.path.exists(args.db_path):
        print(f"Error: Database {args.db_path} not found.")
        sys.exit(1)
        
    print(f"Connecting to catalog {args.db_path}...")
    con = duckdb.connect(args.db_path, read_only=True)
    
    # Query the unified backbone view (assuming it's called unified_backbone or similar)
    # We will try to fetch the dwell_events and join with metocean or just use a consolidated view if it exists.
    # Since this is a scaffold, we handle missing tables gracefully.
    try:
        tables = [t[0] for t in con.execute("SHOW TABLES").fetchall()]
        
        target_table = "processed_backbone" if "processed_backbone" in tables else "dwell_events"
        print(f"Extracting data from {target_table}...")
        
        query = f"""
            SELECT * FROM {target_table} 
            WHERE vessel_class IN ('CTV', 'SOV')
              AND dwell_tier IN ('Tier A', 'Tier C')
        """
        df = con.execute(query).df()
        
        if df.empty:
            print("No matching records found to plot.")
            sys.exit(0)
            
        print(f"Loaded {len(df)} records. Generating Workability ECDF and Boxplots...")
        generate_workability_ecdf(df, args.out_dir)
        generate_workability_boxplots(df, args.out_dir)
        
        print("Generating OSS Control Matrix...")
        oss_matrix = generate_oss_control_matrix(df)
        if not oss_matrix.empty:
            oss_csv = os.path.join(args.out_dir, "oss_control_matrix.csv")
            oss_matrix.to_csv(oss_csv, index=False)
            print(f"Saved OSS control matrix to {oss_csv}")
            
        print("All analytical outputs successfully scaffolded!")
        
    except Exception as e:
        print(f"Analysis failed: {e}")
        sys.exit(1)
    finally:
        con.close()

if __name__ == "__main__":
    main()
