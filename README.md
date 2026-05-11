# O&M Data Acquisition & Accessibility

This project implements an empirical data pipeline to derive multi-parameter operational limits for offshore wind Operations and Maintenance (O&M) vessels. It aims to replace static heuristics (e.g., $H_s < 1.5m$) with dynamic, vessel-aware workability surfaces using AIS, metocean hindcasts, and structural sensor data.

## 🤖 Agent Context Layer
This repository is optimized for AI coding agents.
- **[AGENTS.md](AGENTS.md)**: Rules of engagement and workflow mandates.
- **[CONTEXT.md](CONTEXT.md)**: Deep dive into the O&M domain and pipeline logic.
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
   python3 scripts/stream_ais_filter.py 2024 7
   ```
4. **Identify Vessels:** Process the filtered AIS to find O&M vessels:
   ```bash
   python3 scripts/identify_vessels_at_scale.py Data/Raw/AIS/European_Waters_2024_07.csv
   ```

For the long-term project vision, see **[docs/roadmap.md](docs/roadmap.md)**.
