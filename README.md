# O&M Data Acquisition & Accessibility

This project implements an empirical data pipeline to derive multi-parameter operational limits for offshore wind Operations and Maintenance (O&M) vessels. It aims to replace static heuristics (e.g., $H_s < 1.5m$) with dynamic, vessel-aware workability surfaces using AIS, metocean hindcasts, and structural sensor data.

## 🤖 Agent Context Layer
This repository is optimized for AI coding agents.
- **[AGENTS.md](AGENTS.md)**: Rules of engagement and workflow mandates.
- **[CONTEXT.md](CONTEXT.md)**: Deep dive into the O&M domain and pipeline logic.
- **[docs/README.md](docs/README.md)**: Map of planning, methodology, provenance, and decision documents.
- **[docs/adr/](docs/adr/)**: Architectural Decision Records.

## 📂 Project Structure
- `src/om_pipeline/`: Core logic package (ingestion, identification, analysis).
- `scripts/`: Thin CLI wrappers for running the pipeline.
- `configs/`: Externalized configurations (bounding boxes, time slices).
- `docs/`: Roadmaps, ADRs, and data provenance.
- `Data/`: Multi-tiered data storage (Raw, Interim, Processed) - *Git ignored*.

## 🚀 Setup & Execution

1. **Environment:** Ensure Python 3.x is installed and install the package in editable mode:
   ```bash
   pip install -e .
   ```
2. **Data Initialization:** Prepare the turbine coordinate database:
   ```bash
   python3 scripts/prepare_turbine_data.py
   ```
3. **Run Ingestion:** Stream and filter AIS data for a specific month:
   ```bash
   python3 scripts/stream_ais_filter.py 2024 7 --region european_master --mode farm_candidate --max-sog 2.0
   ```
4. **Identify Vessels:** Process the filtered AIS to find O&M vessels:
   ```bash
   python3 scripts/identify_vessels_at_scale.py Data/Raw/AIS/Farm-Candidates_European-Master_2024_07_SogMax2.0_Buffer2.0nm.csv
   ```
5. **Run the resumable backfill:** Process farm-candidate slices with manifest logging:
   ```bash
   python3 scripts/backfill_ais_slices.py --start-year 2010 --end-year 2020 --phase quarterly --mode farm_candidate --turbine-file Data/Interim/European_Turbine_Coordinates.csv
   ```
6. **Extract and QA the wave-only NORA3 backbone:** After the DuckDB catalog has dwell events and turbines registered:
   ```bash
   python3 scripts/extract_metocean.py --dry-run
   python3 scripts/qa_metocean_backbone.py
   ```

For the long-term project vision, see **[docs/roadmap.md](docs/roadmap.md)**.
