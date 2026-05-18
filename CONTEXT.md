# Project Context: O&M Data Acquisition & Validation

This project builds an empirical data pipeline to derive operational limits for offshore wind Operations and Maintenance (O&M) vessels. It replaces static heuristics with dynamic, vessel-aware workability surfaces, specializing in Service Operation Vessel (SOV) workability.

## Domain Concepts

### AIS (Automatic Identification System)
- **Source:** Danish Maritime Authority (DMA) S3 archives.
- **Data Structure:** Longitudinal study currently implemented for 2010-2020 farm-candidate slices. The runner prioritizes quarterly coverage (`Jan/Apr/Jul/Oct`) before filling remaining months.
- **Filtering:** 
    - **Regional:** European Master Box (46.5N–60.0N, -4.5E–15.0E).
    - **Proximity:** 100m radius around turbine foundations.
    - **Dwell Events:** Defined as stationary behavior (SOG < 0.5 kn) for duration >= 15 min.

### The "10-Minute Backbone"
- A rigid temporal grid where AIS, metocean (NORA3), and structural sensor (RAVE) data are synchronized.
- **Target Status:** Labeling events as "Success" (Active Maintenance) or "Wait-on-Weather" (WoW) using SCADA/DPR ground truth.

### Offshore Wind Infrastructure
- **Open European Offshore Wind Turbine Database:** The primary source for turbine coordinates and farm boundaries.
- **Wikinger Wind Farm:** The primary focus area and pilot site for validation, selected for its high volume of Service Operation Vessel (SOV) activity (vessels > 60m with DP capabilities), replacing the older, CTV-dominated Alpha Ventus target.

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
- **NORA3 Wave Ingestion:** Hourly 3km wave hindcast data is pulled from MET Norway's THREDDS server. The backbone extracts wave parameters: significant wave height (`hs`), peak period (`tp`), and wave direction (`wave_direction`). The wave ingestion utilizes spatial coordinate rounding to 2 decimal places and month-level caching to share raw files across close-proximity turbines.
- **Atmospheric Wind Ingestion:** Wind speed and direction at both 10m and 100m hub heights are extracted from MET Norway's NORA3 Atmospheric hindcast (`nora3_subset_atmos/wind_hourly_v2`). It retrieves variables (`wind_speed`, `wind_direction`) at height dimensions `10` and `100` and saves them locally with coordinate caching.
- **CMEMS Current Ingestion:** Ocean surface current speed and direction are extracted using the Copernicus Marine Toolbox API (`copernicusmarine` client) targeting dataset `cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i` to fetch horizontal components `uo` and `vo`. The system integrates a physically consistent semi-diurnal tidal rotation climatology fallback when offline or credentials are not supplied.
- **Generalized Upscaling & QA:** The metocean ingestor dynamically processes all metocean parameters to upscale hourly records to the 10-minute backbone: Cubic Spline interpolation is used for all scalar columns (e.g. `hs`, `tp`, `wind_speed_10m`, `wind_speed_100m`, `current_speed`) and Circular Vector Interpolation is used for all angular columns (e.g. `wave_direction`, `wind_direction_10m`, `wind_direction_100m`, `current_direction`). The QA gate enforces strict [0, 360) angular bounds and zero nulls.
- **SCADA Handshake:** Taxonomy-based labeling (Success, Standby, Aborted) by cross-referencing vessel proximity with turbine status.


## Ingestion Logic & Safety
- **Robust Headers:** Uses a synonym-based resolver (`Latitude`, `Longitude`, `SOG`) to handle multi-year DMA schema variations.
- **Numeric Normalization:** Handles comma decimal delimiters common in European datasets.
- **Counter-based Validation:** Each ingestion cycle tracks detailed metrics (scanned, kept, malformed skips) for provenance.
- **Provenance:** Detailed pilot run metadata is preserved in `docs/provenance.md`.

## Technical Constraints
- **Storage:** Data is stored locally on high-speed SSDs. Large archives are streamed to minimize disk footprint.
- **Privacy:** Vessel names and MMSIs are handled for research but should not be exposed in public-facing summaries if they contain sensitive operational metadata.
