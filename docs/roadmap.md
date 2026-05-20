# Roadmap: Empirical Multi-Parameter Operational Limits for 15MW+ Offshore Wind O&M

## Thesis Objective
To replace legacy, single-parameter vessel workability heuristics (e.g., $H_s < 1.5m$) with a dynamic, multi-parameter ( $H_s, T_p, \theta$, wind, current ) non-linear boundary surface. This model will be "vessel-aware," incorporating dimensions and technology (Gangway/DP) to optimize fleet sizing and O&M costs for 15MW+ offshore wind assets.

---

## Phase 0: Project Architecture & Local Ingestion
**Goal:** Establish a standardized directory structure and discovery protocols for local data ingestion.

- [x] **Workspace Organization:** Define a structured directory hierarchy (`/Data/Raw`, `/Data/Processed`, `/Data/Interim`) to handle multi-source ingestion.
- [x] **Reproducibility Foundation:** Root `README.md`, `requirements.txt`, and `.gitignore` established.
- [x] **Format Discovery:** Verified DMA AIS (CSV), NORA3 (NetCDF), and FINO1 schemas.
- [x] **Turbine Data Preparation:** `Scripts/prepare_turbine_data.py` created to regenerate interim coordinate files.
- [x] **Lightweight Analytics Setup:** Configure a local **DuckDB** catalog for exploratory spatial queries and pipeline view registration.

---

## Phase 1: Data Acquisition (Wikinger SOV Pilot)
**Goal:** Acquire high-fidelity historical data for the modern, SOV-heavy Wikinger wind farm pilot site.

- [/] **Logistics (AIS):** Download and process raw terrestrial AIS data from the **Danish Maritime Authority (DMA)**.
    - [x] **Automated Pipeline:** Hardened `stream_ais_filter.py` (Python-native HTTP/ZIP) and `identify_vessels_at_scale.py` (Event-based) implemented.
    - [x] **Repository Restructuring:** Implemented `src/om_pipeline` and agent-centric context layer.
    - [/] **Longitudinal Sampling:** Resumable farm-candidate backfill runner configured for the 2010–2020 historical period. Quarterly slices (`Jan/Apr/Jul/Oct`) are run first, followed by the remaining months to capture full seasonal transitions.
- [ ] **Environment (In-Situ):** Access relevant Baltic/North Sea database for wave spectra.
- [x] **Environment (Hindcast):** Pull wave-only **NORA3** 3km-resolution NetCDF data via MET Norway's legacy aggregate OPeNDAP endpoint (`windsurfer/mywavewam3km_files/aggregate/nora3_wave_agg.nc`) for the 2010-2020 Wikinger SOV study. The ingestion utilizes spatial coordinate rounding to 2 decimal places and month-level caching to share raw files across close-proximity turbines, with monitoring required because the aggregate endpoint is prone to timeouts.
- [ ] **Operations (SCADA/Structural/DPR):** Obtain Wikinger daily progress reports (DPRs) or validation datasets for workability validation.

---

## Phase 2: The "10-Minute Backbone" Data Engineering
**Goal:** Synchronize disparate data streams into a unified training matrix.

- [x] **Event-Based Identification:** 
    - [x] Identify "Dwell Events" (SOG < 0.5 kn, Proximity < 100m, Duration >= 15 min).
    - [x] Catalog registered via DuckDB for SQL analysis.
- [x] **Metocean Extraction & Upscaling:**
    - [x] Implement cache-aware NORA3 THREDDS extraction.
    - [x] Implement Cubic Spline (scalars) and Circular Vector (direction) upscaling to 10-minutes.
    - [x] Extend the metocean backbone beyond wave-only NORA3 parameters to include wind and current once the wave backbone passes QA.
    - [x] Run `extract_metocean.py` across the completed event catalog to generate the expanded wave, wind, and current backbone.
- [x] **Data Quality Assurance (QA):**
    - [x] Run row count, schema, missing-value, duplicate, span-continuity, and alignment sanity checks on the NORA3 Backbone. Passing this wave-only QA is a strict gate before wind/current expansion or AIS + metocean join work.
- [x] **The AIS + Metocean Join:**
    - [x] Create a formal module in `src/om_pipeline/` to merge `dwell_events` and the 10-minute backbone based on `found_id` and timestamps.
- [/] **The "SCADA Handshake" & Feature Engineering:**
    - [x] **Wind Farm C (CAREtoCompare / Trianel Borkum I+II):** De-anonymized with confirmed 0-year temporal shift. `SCADAHandshake` applies timestamps directly. Labels validated via 8-test empirical campaign. See `docs/adr/003-care-de-anonymization.md`.
    - [x] **Wind Farm B (CAREtoCompare / Alpha Ventus):** De-anonymized with confirmed 0-year temporal shift.
    - [ ] **Wikinger strategic log sourcing (BLOCKED):** Pivot from legacy Alpha Ventus RAVE archives to secure Wikinger-specific Daily Progress Reports (DPRs) or SCADA logs to enable high-fidelity vessel status handshakes.
    - [ ] Compute event-level aggregates (mean/max Hs, Tp, wind, current, direction, and relative profiles).
- [ ] **Feature Matrix Construction — Wind Farm C (ACTIVE MILESTONE):** Build the first production feature matrix for Wind Farm C (Trianel Borkum I+II). Produce a joined table with:
    - `[timestamp | vessel | hs | tp | wave_direction | wind_speed | wind_direction | current_speed | current_direction | status_type_id | label]`
    - Output: `Data/Processed/wind_farm_c_feature_matrix.parquet` (Parquet preferred for scale).
    - QA report: `reports/care_wind_farm_c_confirmation/`
- [ ] **Feature Matrix Construction — Wikinger (BLOCKED):** Requires Wikinger SCADA/DPR data. Unblocks after Wikinger log sourcing.
- [ ] **Master Feature Matrix:** Merge Wind Farm B, C, and Wikinger slices into a unified training CSV/Parquet:
    - `[Timestamp | Vessel_Specs | Hs | Tp | Wave_Direction | Wind | Current | Vessel_Heading | Target_Status]`

---

## Phase 3: Machine Learning & Boundary Mapping
**Goal:** Derive the non-linear workability surface.

- [ ] **Baseline Model:** Train a **Random Forest / XGBoost** classifier on the German Bight master dataset.
- [ ] **Feature Importance:** Quantify the impact of $T_p$ (Period) and $\theta$ (Direction) vs. $H_s$.
- [ ] **Vessel Sensitivity Analysis:** 
    - Determine how the workability surface shifts as Vessel Length increases from 60m (CTV) to 120m (SOV).
- [ ] **Model Validation:** Cross-validate against Daily Progress Reports (DPRs) from the **Marine Data Exchange (MDE)**.

---

## Phase 4: Scaling & Impact Modeling
**Goal:** Apply findings to next-generation 15MW+ assets and US Eastern Seaboard deployment.

- [ ] **US East Coast Transfer:** Apply the validated model to **MarineCadastre** AIS and **NOAA** buoy data for US lease areas.
- [ ] **15MW Extrapolation:** Use the "Vessel-Aware" ML model to predict operational uptime for 15MW-class SOVs using theoretical dimensions.
- [ ] **Monte Carlo Integration:** 
    - Replace the $H_s = 1.5m$ hard-cut in a fleet simulation (e.g., using `OpenOA` or a custom simulation).
- [ ] **Economic Synthesis:** Calculate the **LCoE reduction** resulting from optimized vessel dispatch and reduced downtime.
