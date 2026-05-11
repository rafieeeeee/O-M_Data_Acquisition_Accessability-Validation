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

### Funnel Approach
1. **Tier 1 (Raw):** Stream and filter 10GB+ monthly archives into ~1GB regional CSVs.
2. **Tier 2 (Interim):** Identify candidate O&M vessels using farm-level bounding boxes.
3. **Tier 3 (Processed):** Validate via foundation proximity to build the "Fleet Registry" and "Event Matrix."

## Technical Constraints
- **Storage:** Data is stored locally on high-speed SSDs. Large archives are streamed to minimize disk footprint.
- **Privacy:** Vessel names and MMSIs are handled for research but should not be exposed in public-facing summaries if they contain sensitive operational metadata.
