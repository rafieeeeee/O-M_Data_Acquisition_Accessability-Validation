# Metocean Data Acquisition Guide

This document details the acquisition paths for FINO1 and NORA3 wave data to support joining AIS dwell events with environmental conditions.

## 1. FINO1 (In-Situ Ground Truth)

FINO1 provides high-fidelity, 10-minute wave records at the Alpha Ventus pilot site.

- **Access Method:** BSH specialist procedure "Insitu".
- **Portal:** [BSH Service Portal](https://fino.bsh.de/) (Direct access via [Insitu Portal](https://fino.bsh.de/))
- **Credentials:** Requires registration on the **BSH-Login** system and specific request for "Insitu" specialist procedure access.
- **Variables:**
    - `Hs`: Significant wave height (m).
    - `Tp`: Peak wave period (s).
    - `theta`: Mean wave direction (degrees, from which waves arrive).
- **Format:** CSV or ASCII (preferred for 10-minute alignment).
- **Resolution:** 10-minute intervals.
- **License:** Free for research and scientific purposes; requires source acknowledgment (BSH and FINO project). **Note:** Final license terms require confirmation after BSH Insitu access is granted.

## 2. NORA3 (Regional Hindcast)

NORA3 provides regional wave and atmospheric fields. It is our primary source for scale-up across the European fleet.

- **Access Method:** MET Norway THREDDS Data Server via OPeNDAP Point Extraction.
- **Endpoints:**
    - **2024+ Monthly Subsets:** `nora3_subset_wave/wave_tser/` (Preferred for speed and reliability).
    - **Legacy Global Aggregation:** `windsurfer/mywavewam3km_files/aggregate/nora3_wave_agg.nc`.
- **Variables (Current):**
    - `hs`: Significant wave height (m).
    - `tp`: Peak wave period (s).
    - `thq`: Mean wave direction (degrees).
- **Spatial Strategy:** 
    - **Foundation-Level Extraction:** We do NOT download regional grids. We perform 1D time-series extraction for specific grid cells containing turbine foundations (`found_id`).
    - **Rotated Grid Handling:** 2024+ monthly files use a curvilinear grid (`rlat`/`rlon`). The pipeline uses a 2D nearest-neighbor search to map foundation lat/lon to grid indices.
- **Server Etiquette & Caching (CRITICAL):**
    - **Monthly Caching Policy:** To prevent redundant server load, raw NORA3 data is cached locally as CSVs (e.g., `nora3_raw_{lat}_{lon}_{YYYY_MM}.csv`).
    - **Throttling:** Requests are serialized. The pipeline fetches data for one foundation-month group at a time.
    - **Avoidance of Bulk Scans:** Extraction is strictly event-driven. We only fetch data for foundations and months where O&M activity has been identified in the AIS catalog.
- **Interpolation:** 1-hour hourly data is upscaled to the 10-minute backbone using cubic spline (scalars) and circular vector interpolation (directions).

## 3. Environment Expansion Roadmap (Wind & Current)

Final workability surfaces require additional parameters beyond wave state.

### Wind (NORA3 Atmospheric)
- **Source:** MET Norway NORA3 Atmospheric hindcast.
- **Target Variables:** Wind speed and direction at 10m and 100m (hub height).
- **Status:** Planned. Needs verification of `tser` monthly subset availability to match wave extraction speed.

### Current (CMEMS)
- **Source:** Copernicus Marine Service (CMEMS).
- **Product:** Atlantic-European North West Shelf - Ocean Physics Hindcast (NEMO).
- **Target Variables:** Surface current speed and direction.
- **Status:** Required for Stage 2. Implementation will require a separate CMEMS API client and authentication.

## 4. Operational Synchronization

The **AIS Backfill** (2010–2025) and **Metocean Extraction** are currently disjointed processes to manage system load and server etiquette.

1. **AIS Runner Phase:** Identifies foundations and months of interest across the 15-year backfill.
2. **Metocean Trigger Phase:** Triggered manually or at milestones to "fill" the environmental data for the newly identified events.
3. **Backbone Join:** The final step merges the 10-minute AIS event sequences with the 10-minute Metocean backbone.

## 4. Blockers & Action Items
- [ ] **Human Action Required:** Register for a BSH-Login account and request "Insitu" access.
- [ ] **Technical Task:** Implement the circular interpolation utility for NORA3 upscaling.
