# Metocean Data Acquisition Guide

This document details the acquisition paths and current storage state for FINO1, NORA3, NWS, and Baltic wave data used to join AIS dwell events with environmental conditions.

## 1. FINO1/2/3 (In-Situ Validation)

FINO1 provides high-fidelity, 10-minute wave records at the Alpha Ventus pilot site. FINO2 and FINO3 extend the validation-station network into the Baltic/Kriegers Flak area and the German North Sea/Sylt cluster.

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
- **Planning script:** `scripts/plan_fino_metadata_access.py`
- **Core logic:** `src/om_pipeline/metocean/fino_metadata_planner.py`
- **Planning report:** `analysis/06_rq6_metocean_spatial_resolution/fino_metadata_access_plan.md`
- **Decision record:** `docs/adr/0023-fino-validation-access-planning.md`
- **Native export inspector:** `scripts/inspect_fino_export.py`
- **Inspection logic:** `src/om_pipeline/metocean/fino_export_inspector.py`
- **Inspection decision record:** `docs/adr/0024-fino-native-export-inspection.md`
- **Current local status:** `Data/Raw/Metocean/FINO1/` exists as an empty placeholder; no usable local FINO1/2/3 time-series archive exists yet.
- **Guardrail:** FINO1/2/3 are validation/baseline stations, not automatic farm-wide source assignments. The planner writes only the access/report artifact and the inspector writes only a native-export inspection report. Neither tool downloads FINO data, imports time series, source-fuses variables, interpolates to 10 minutes, handles credentials, scrapes the BSH portal, or rebuilds dwell-metocean features.

## 2. NORA3 (Regional Hindcast)

NORA3 provides regional wave and atmospheric fields. It is our primary source for scale-up across the European fleet.

- **Access Method:** MET Norway THREDDS Data Server via OPeNDAP Point Extraction.
- **Endpoints:**
    - **Legacy Global Aggregation:** `windsurfer/mywavewam3km_files/aggregate/nora3_wave_agg.nc` (current default for the 2010-2020 Wikinger SOV study).
    - **2024+ Monthly Subsets:** `nora3_subset_wave/wave_tser/` (available as an explicit override if the study window shifts back to recent years).
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

### Current Local Storage State
- **Raw cache root:** `Data/Raw/Metocean/NORA3/`
- **Checkpointed joined cache root:** `Data/Processed/metocean/nora3_joined_cache/`
- **Operational mode:** The long-running downloader writes raw wave and wind CSV sidecars. `scripts/consolidate_nora3_cache.py` can checkpoint stable wave/wind pairs into parquet batches without interrupting the downloader.

## 3. Environment Expansion Roadmap (Wind & Current)

Final workability surfaces require additional parameters beyond wave state.

### Wind (NORA3 Atmospheric)
- **Source:** MET Norway NORA3 Atmospheric hindcast.
- **Target Variables:** Wind speed and direction at 10m and 100m (hub height).
- **Status:** Wind Confidence v1 is accepted as an event-level evidence layer from existing local NORA3/dwell-weather fields. Outputs live under `Data/Processed/metocean/wind_confidence_v1/`, with validation at `reports/wind_confidence_v1/wind_confidence_validation_report.md`. The layer records `75,380` speed-ready dwell events but only `197` speed+direction events, so wind speed is ready for Fusion v2 while wind direction remains nullable/repair-pending. The local raw NORA3 wind cache inventory contains `33,521` speed-only files and `464` speed+direction files; no new NORA3 downloads or broad direction repair were run.

### Current (CMEMS)
- **Source:** Copernicus Marine Service (CMEMS).
- **Product:** Atlantic-European North West Shelf - Ocean Physics Hindcast (NEMO).
- **Target Variables:** True Eulerian `uo` and `vo`; speed and flow-to direction are derived locally.
- **Status:** NWS current pilot, normal recommended scale, and Current Confidence v1 are accepted for source-specific and event-level evidence. The processed NWS hourly current archive covers `125` non-stress-test farm-years under `Data/Processed/metocean/nws_current_timeseries/`, with the manifest at `Data/Processed/metocean/nws_current_timeseries/manifest.csv` and validation under `reports/current_pilot_v1/`. Current Confidence v1 attaches that archive to dwell events at `Data/Processed/metocean/current_confidence_v1/` and reports `16,307` event-scale current assignments from `92,660` dwell events. Baltic historical true-current evidence remains daily/contextual unless a separate historical hourly source is approved. Legacy CMEMS CSV/fallback current paths remain banned as research evidence.

### Fusion v2 Event Features
- **Source layers:** Wave Confidence v1, Wind Confidence v1, Current Confidence v1, and EMODnet bathymetry.
- **Output table:** `Data/Processed/metocean/fusion_v2/dwell_metocean_fusion_v2.parquet`
- **Validation report:** `reports/metocean_fusion_v2/fusion_v2_validation_report.md`
- **Status:** accepted as the first combined source-resolved metocean event feature table. Fusion v2 preserves all `92,660` dwell rows, keeps wave/wind/current/bathymetry confidence fields separate, and adds modelling-readiness flags for wave-only, wave+wind, wave+current, wave+wind+current, and high-confidence multivariate subsets.
- **Guardrails:** Fusion v2 does not download data, repair wind direction, include stress-test current farm-years, promote Baltic daily current to event-scale evidence, import FINO, rerun NORA3, promote legacy CMEMS currents, infer vessel roles, or make calibrated `P(operation | weather)` claims. Wind direction remains nullable/sensitivity-only; missing current remains null rather than zero.

## 4. Copernicus NWS Wave Backbone

NWS is no longer only a planning branch. A processed hindcast archive now exists locally.

- **Raw source inventory:** documented in `analysis/06_rq6_metocean_spatial_resolution/nws_file_inventory.md`
- **Raw annual source location:** external 4TB drive paths referenced by the inventory and extraction plan
- **Processed archive root:** `Data/Processed/metocean/nws_wave_timeseries/`
- **Current coverage:** `112` farms and `1,169` farm-year parquet partitions
- **QA authority:** `analysis/06_rq6_metocean_spatial_resolution/nws_wave_full_extraction_qa_report.md`
- **Important caveat:** there is no mirrored `Data/Raw/Metocean/NWS/` folder in this repository, so raw-source discoverability is weaker than for Baltic and NORA3.

## 5. Baltic Copernicus Wave Backbone

Baltic subset downloads are present locally and the processed native-hourly continuous archive has been materialized and accepted.

- **Product:** `BALTICSEA_MULTIYEAR_WAV_003_015` / `cmems_mod_bal_wav_my_PT1H-i`
- **Reviewed raw subset root:** `Data/Raw/Metocean/CMEMS/BalticSea/Waves/`
- **Current raw farm count:** `16`
- **Processed archive root:** `Data/Processed/metocean/baltic_wave_timeseries/`
- **Current processed status:** accepted; `16` farms, `238` partitions, `73,866,720` rows
- **QA authority:** `analysis/06_rq6_metocean_spatial_resolution/baltic_wave_materialization_acceptance_report.md`
- **Planning authority:** `analysis/06_rq6_metocean_spatial_resolution/baltic_download_plan_summary.md`
- **Naming caveat:** one downloaded farm directory uses `Arkona-Becken_Südost`, while the planning table uses the normalized identifier `Arkona_Becken_Sudost`.

## 6. Bathymetry Site-Context Planning And Assignment

Bathymetry is the static site-context layer for common metocean sample points.
It is source-specific and must not rebuild or fuse dwell-metocean features.

- **Planning script:** `scripts/plan_bathymetry_assignment.py`
- **Core logic:** `src/om_pipeline/metocean/bathymetry_planner.py`
- **Assignment script:** `scripts/assign_bathymetry_to_metocean_points.py`
- **Assignment logic:** `src/om_pipeline/metocean/bathymetry_assignment.py`
- **Input requirements:** `analysis/06_rq6_metocean_spatial_resolution/common_metocean_farm_requirements.csv`
- **Primary source:** EMODnet Bathymetry DTM through the official `depth_sample` REST endpoint
- **Fallback/cross-check source:** `GEBCO_2026`
- **Dry-run QA report:** `analysis/06_rq6_metocean_spatial_resolution/bathymetry_assignment_pilot_report.md`
- **Full assignment QA report:** `analysis/06_rq6_metocean_spatial_resolution/bathymetry_assignment_full_report.md`
- **Raw/cache source data:** `Data/Raw/Metocean/Bathymetry/emodnet_depth_samples/`
- **Processed output contract:** `Data/Processed/metocean/bathymetry/site_bathymetry_points.parquet`
- **Metadata:** `Data/Processed/metocean/bathymetry/bathymetry_source_metadata.json`
- **Current processed status:** accepted candidate; `119` farms, `6,642` sample points, zero missing depths, zero duplicate farm/sample keys
- **Decision record:** `docs/adr/0022-bathymetry-site-context-assignment.md`
- **Guardrail:** the assignment writes only bathymetry source cache, metadata, the static point table, and QA report. It does not download currents, ingest FINO, mutate wave archives, interpolate to 10 minutes, source-fuse, or rebuild dwell-metocean features.

## 7. Operational Synchronization

The **AIS Backfill** (2010–2025) and **Metocean Extraction** are currently disjointed processes to manage system load and server etiquette.

1. **AIS Runner Phase:** Identifies foundations and months of interest across the 15-year backfill.
2. **Metocean Trigger Phase:** Triggered manually or at milestones to "fill" the environmental data for the newly identified events.
3. **Backbone Join:** The final step merges the 10-minute AIS event sequences with the 10-minute Metocean backbone.

## 8. Current Gaps & Action Items
- [ ] **Human Action Required:** Register for a BSH-Login account and request "Insitu" access.
- [x] **Technical Task:** Implement and run the FINO metadata/access planning dry-run under `analysis/06_rq6_metocean_spatial_resolution/fino_metadata_access_plan.md`.
- [x] **Technical Task:** Implement the dry-run FINO native export inspector for one small manually exported CSV/ASCII file.
- [ ] **Technical Task:** After BSH Insitu access is approved, run `scripts/inspect_fino_export.py` on one small native FINO1 export and document exact variable names, measurement heights/depths, QC fields, file format, and licence/source acknowledgement wording before any bulk import.
- [x] **Technical Task:** Materialize the Baltic continuous archive under `Data/Processed/metocean/baltic_wave_timeseries/` from the reviewed raw subset downloads.
- [x] **Technical Task:** Implement and run the bathymetry assignment planning dry-run under `analysis/06_rq6_metocean_spatial_resolution/bathymetry_assignment_pilot_report.md`.
- [x] **Technical Task:** Implement the EMODnet point-sample bathymetry assignment wrapper for `site_bathymetry_points.parquet`.
- [x] **Technical Task:** Run the full EMODnet point-sample bathymetry assignment for all common metocean sample points.
- [ ] **Technical Task:** Review shallow/coastal depth rows in the full bathymetry QA report and decide whether later slope/roughness features require separate raster/tile acquisition.
- [ ] **Technical Task:** Decide whether to mirror or symlink the external NWS annual NetCDF source files into a repository-visible `Data/Raw/Metocean/NWS/` structure for clearer storage hygiene.
- [x] **Technical Task:** Complete real NWS current evidence for normal recommended farm-years and attach it to dwell events through Current Confidence v1.
- [x] **Technical Task:** Build Fusion v2 from wave confidence, wind confidence, current confidence, and bathymetry.
- [ ] **Technical Task:** Run Stage 2 modelling sensitivity comparing wave-only, wave+wind, wave+current, and wave+wind+current subsets from Fusion v2.
