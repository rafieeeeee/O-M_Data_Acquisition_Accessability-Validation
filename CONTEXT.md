# Project Context: O&M Data Acquisition & Validation

This project builds an empirical data pipeline to derive operational limits for offshore wind Operations and Maintenance (O&M) vessels. It replaces static heuristics with dynamic, vessel-aware workability surfaces.

## Domain Concepts

### AIS (Automatic Identification System)
- **Source:** Danish Maritime Authority (DMA) S3 archives.
- **Data Structure:** Longitudinal study (2009–2024), focusing on Jan/July "slices."
- **Filtering:** 
    - **Regional:** European Master Box (46.5N–60.0N, -4.5E–15.0E).
    - **Proximity:** 100m radius around turbine foundations.
    - **Dwell Events:** Defined as stationary behavior (SOG < 0.5 kn) for duration >= 15 min.

### The "10-Minute Backbone"
- A rigid temporal grid where AIS, metocean (NORA3), and structural sensor (RAVE) data are synchronized.
- **Target Status:** Labeling events as "Success" (Active Maintenance) or "Wait-on-Weather" (WoW) using SCADA/DPR ground truth.

### Offshore Wind Infrastructure
- **Open European Offshore Wind Turbine Database:** The primary source for turbine coordinates and farm boundaries.
- **Alpha Ventus (AV):** The primary focus area for validation due to the RAVE research archive.

## Data Pipeline Architecture

### Funnel Approach (Hybrid)
The pipeline operates in two modes to balance storage efficiency with auditability:
1. **Mode A (Regional Slice):** Streams and filters large monthly archives into regional CSVs (e.g., German Bight). Retains all traffic below a speed threshold. Used for validation months.
2. **Mode B (Farm-Candidate Extraction):** Extracts only AIS pings within a configurable buffer (e.g., 2nm) of known wind farm bounding boxes. Used for standard longitudinal slices to minimize disk footprint.

### Processing Tiers
1. **Tier 1 (Raw):** Ingested AIS CSVs (from Mode A or Mode B). Stored in `Data/Raw/`.
2. **Tier 2 (Interim):** Identified candidate O&M vessels and consolidated dwell events. Stored in `Data/Interim/`.
3. **Tier 3 (Processed):** Synchronized AIS, Metocean, and SCADA features in the "10-Minute Backbone". Stored in `Data/Processed/`.
4. **Catalog Layer:** A local **DuckDB** database (`Data/catalog.duckdb`) provides a SQL interface for cross-slice analysis and feature engineering.

## Metocean & Synchronization
- **FINO1:** 10-minute ground-truth wave spectra ($H_s, T_p, \theta$).
- **NORA3 Extraction:** Hourly 3km hindcast data is pulled from MET Norway's THREDDS server. The current implemented backbone extracts wave parameters only: significant wave height (`hs`), peak period (`tp`), and wave direction (`wave_direction`). Extraction is driven by a local DuckDB catalog, fetching exact calendar month blocks (plus a 2-hour padding overlap) to maximize local cache hits for events occurring in the same temporal window.
- **Future Metocean Scope:** Wind and current are required for the final workability model but are intentionally deferred until the wave-only NORA3 backbone has passed QA. Add wind as speed/direction or vector components (`u10`, `v10`) and current as speed/direction or vector components if a reliable hindcast/source is available. Preserve the same cache/interpolation contract when extending the schema.
- **Upscaling Strategy:** NORA3 hourly arrays are upscaled to 10-minute intervals using **Cubic Splines** for scalar variables ($H_s, T_p$) and **Circular Vector Interpolation** for the wave direction ($\theta$). 
- **Backbone Join:** The extraction of the Metocean backbone (`Metocean_NORA3_Backbone.csv`) is completely separated from the AIS join (`events + metocean`). This allows for rigorous row-count and boundary QA on the metocean arrays before initiating the complex event-level synchronization.
- **SCADA Handshake:** Taxonomy-based labeling (Success, Standby, Aborted) by cross-referencing vessel proximity with turbine status.

## Ingestion Logic & Safety
- **Robust Headers:** Uses a synonym-based resolver (`Latitude`, `Longitude`, `SOG`) to handle multi-year DMA schema variations.
- **Numeric Normalization:** Handles comma decimal delimiters common in European datasets.
- **Counter-based Validation:** Each ingestion cycle tracks detailed metrics (scanned, kept, malformed skips) for provenance.
- **Provenance:** Detailed pilot run metadata is preserved in `docs/provenance.md`.

## Technical Constraints
- **Storage:** Data is stored locally on high-speed SSDs. Large archives are streamed to minimize disk footprint.
- **Privacy:** Vessel names and MMSIs are handled for research but should not be exposed in public-facing summaries if they contain sensitive operational metadata.
