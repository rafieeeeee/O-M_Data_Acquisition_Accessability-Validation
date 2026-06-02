# Roadmap: Empirical Multi-Parameter Operational Limits for 15MW+ Offshore Wind O&M

## Thesis Objective
To replace legacy, single-parameter vessel workability heuristics (e.g., $H_s < 1.5m$) with dynamic, multi-parameter observed workability surfaces over waves, wind speed, current speed, vessel, task, and site context. Directional variables remain secondary until coverage supports them; wind direction is currently too sparse for broad modelling.

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
    - [x] **Longitudinal Sampling:** Resumable farm-candidate backfill runner configured and executed for the 2010–2020 historical period. Quarterly slices (`Jan/Apr/Jul/Oct`) completed for the Wikinger/Baltic cluster, capturing 527 raw dwells (383 deduplicated behavioral events).
- [ ] **Environment (In-Situ):** Access relevant Baltic/North Sea database for wave spectra.
- [x] **Environment (Hindcast):** Pull wave-only **NORA3** 3km-resolution NetCDF data via MET Norway's legacy aggregate OPeNDAP endpoint (`windsurfer/mywavewam3km_files/aggregate/nora3_wave_agg.nc`) for the 2010-2020 Wikinger SOV study. The ingestion utilizes spatial coordinate rounding to 2 decimal places and month-level caching to share raw files across close-proximity turbines, with monitoring required because the aggregate endpoint is prone to timeouts.
    - [x] **NORA3 Sidecar Consolidation:** Added a read-only sidecar checkpoint flow that joins stable raw NORA3 wave/wind cache pairs into `Data/Processed/metocean/nora3_joined_cache/` while the downloader continues writing monthly CSVs.
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
    - [x] Extend the metocean evidence stack beyond wave-only NORA3 parameters to include accepted wind and true-current confidence layers once the wave backbone passes QA.
    - [x] Preserve the historical expanded wave/wind/current backbone as local generated evidence, while treating accepted true-current evidence as the NWS `uo`/`vo` archive plus Current Confidence v1.
- [x] **Data Quality Assurance (QA):**
    - [x] Run row count, schema, missing-value, duplicate, span-continuity, and alignment sanity checks on the NORA3 Backbone. Passing this wave-only QA is a strict gate before wind/current expansion or AIS + metocean join work.
- [x] **NWS Continuous Wave Backbone:**
    - [x] Planned NWS extraction from common farm requirements rather than event-only windows.
    - [x] Materialized the processed farm/sample-point archive under `Data/Processed/metocean/nws_wave_timeseries/`.
    - [x] Validated `1,169` farm-year partitions across `112` farms; raw annual NetCDF source files remain on the external 4TB drive referenced by the inventory docs.
- [/] **Baltic Continuous Wave Backbone:**
    - [x] Planned Baltic extraction from the same common farm requirements table.
    - [x] Downloaded reviewed raw subset files for `16` in-scope Baltic farms under `Data/Raw/Metocean/CMEMS/BalticSea/Waves/`.
    - [x] Materialized and accepted the processed archive target `Data/Processed/metocean/baltic_wave_timeseries/` with `238` partitions and `73,866,720` rows.
- [x] **Wave Confidence Layer:**
    - [x] Built Fusion v1 source-agreement evidence for NORA3, NWS, and Baltic waves.
    - [x] Preserved source disagreement and confidence classes rather than using the v0 source-priority resolver.
- [x] **Current Confidence Layer:**
    - [x] Piloted and scaled NWS hourly true `uo`/`vo` currents for `125` normal recommended farm-years.
    - [x] Attached NWS current evidence to dwell events in Current Confidence v1 with `16,307` event-scale current assignments.
- [x] **Wind Confidence Layer:**
    - [x] Formalized existing NORA3 active-window wind evidence in Wind Confidence v1.
    - [x] Accepted wind speed as modelling-ready for `75,380` events while keeping sparse wind direction nullable/sensitivity-only.
- [x] **Fusion v2 Multi-Parameter Event Features:**
    - [x] Built `Data/Processed/metocean/fusion_v2/dwell_metocean_fusion_v2.parquet`.
    - [x] Preserved all `92,660` dwell rows and joined wave confidence, wind confidence, current confidence, and bathymetry.
    - [x] Validated `13,207` wave+wind+current rows and `9,337` high-confidence multivariate rows.
- [x] **Stage 1 Observed/Provisional Workability Surface:**
    - [x] Preserved a configurable workability surface engine with $H_s \times T_p$ as the default preset rather than the closed workability definition.
    - [x] Kept Stage 1 labelled as observed/provisional and not calibrated `P(operation | weather)`.
- [x] **The AIS + Metocean Join:**
    - [x] Create a formal module in `src/om_pipeline/` to merge `dwell_events` and the 10-minute backbone based on `found_id` and timestamps.
- [/] **The "SCADA Handshake" & Feature Engineering:**
    - [x] **Wind Farm C (CAREtoCompare / Trianel Borkum I+II):** De-anonymized with confirmed 0-year temporal shift. `SCADAHandshake` applies timestamps directly. Labels validated via 8-test empirical campaign. See `docs/adr/003-care-de-anonymization.md`.
    - [x] **Wind Farm B (CAREtoCompare / Alpha Ventus):** De-anonymized with confirmed 0-year temporal shift.
    - [ ] **Wikinger strategic log sourcing (BLOCKED):** Pivot from legacy Alpha Ventus RAVE archives to secure Wikinger-specific Daily Progress Reports (DPRs) or SCADA logs to enable high-fidelity vessel status handshakes.
    - [ ] Compute event-level aggregates (mean/max Hs, Tp, wind, current, direction, and relative profiles).
- [x] **Feature Matrix Construction — Wind Farm C:** Built the first production feature matrix for Wind Farm C using the Trianel Borkum I+II working mapping. Produced a joined table with:
    - `[timestamp | vessel | hs | tp | wave_direction | wind_speed | wind_direction | current_speed | current_direction | status_type_id | label]`
    - Output: `Data/Processed/wind_farm_c_feature_matrix.parquet` (local generated artifact; not committed).
    - QA report: `reports/care_wind_farm_c_confirmation/wfc_feature_matrix_qa.md`
    - Caveat: historical generated artifact only; current columns from legacy synthetic paths are not accepted research evidence and must not be used for Fusion v2 or Stage 2.
- [x] **Wind Farm C Event-Level Aggregation:** Collapsed the 10-minute matrix to one row per CARE event with mean/max/std metocean features, circular directional statistics, SCADA status shares, handshake label shares, and event-level target labels.
    - Script: `scripts/build_wind_farm_c_event_aggregates.py`
    - Output: `Data/Processed/wind_farm_c_event_aggregates.parquet` (local generated artifact; not committed).
    - Current local run: 58 events, 58 columns, 31 CARE normal / 27 CARE anomaly, no aggregate NaNs.
- [ ] **Wind Farm C External Cross-Checks:** Replace synthetic `min_dist=50m` with real AIS dwell proximity where catalog coverage exists, and rebuild current features only from accepted true `uo`/`vo` products.
- [ ] **Feature Matrix Construction — Wikinger (BLOCKED):** Requires Wikinger SCADA/DPR data. Unblocks after Wikinger log sourcing.
- [ ] **Master Feature Matrix:** Merge Wind Farm B, C, and Wikinger slices into a unified training CSV/Parquet:
    - `[Timestamp | Vessel_Specs | Hs | Tp | Wave_Direction | Wind | Current | Vessel_Heading | Target_Status]`
- [ ] **Stage 2 Fusion v2 Sensitivity:** Not started. Use Fusion v2 to compare observed envelopes for wave-only, wave+wind speed, wave+current, and wave+wind+current subsets before any calibrated probability model.

---

## Phase 3: Machine Learning & Boundary Mapping
**Goal:** Derive the non-linear workability surface.

    - [x] **Wind Farm C Baseline Models:** Implemented dual-task cross-validated modeling pipeline (`scripts/train_wind_farm_c_baseline.py`). We implemented a baseline diagnostic modeling pipeline. Task A shows promising but leakage-prone diagnostic separability. Task B shows high ROC-AUC ranking under extreme class imbalance, but default-threshold classification fails for Random Forest and the target remains a proxy because AIS proximity is synthetic. Results are exploratory and should guide the next grouped 10-minute modeling experiment, not be treated as thesis-grade evidence yet.
    - [x] **Wind Farm C 10-Minute Grouped Feasibility Study:** Successfully established a leakage-safe, event-grouped validation framework (`scripts/train_wind_farm_c_10min_grouped.py`). The study confirms that naive row-level CV substantially overstates performance (F1 0.899 vs 0.682). Verdict: **Methodological Success / Predictive Inconclusive**. Metocean-only features provide weak/unstable signal under proxy labels, and LOEO sensitivity shows significant event-level instability. This justifies the pivot to richer operational context (AIS/CMEMS) in the next phase. See `reports/baseline_models/ten_min_grouped_README.md`.
- [/] **Feature Importance:** Quantify the impact of $T_p$ (Period) and $\theta$ (Direction) vs. $H_s$.
    - [x] **Wind Farm C Importances:** Analyzed MDI feature importances across all classifiers; significant wave height ($H_s$) and wind speed dominate. Also audited physical separability using Cliff's Delta and event-normalized medians.
- [ ] **Vessel Sensitivity Analysis:** 
    - Determine how the workability surface shifts as Vessel Length increases from 60m (CTV) to 120m (SOV).
- [ ] **Model Validation:** Cross-validate against Daily Progress Reports (DPRs) from the **Marine Data Exchange (MDE)**.

---

## Phase 4: Cross-Farm AIS Dwell Atlas (Pilot)
**Goal:** Develop a scalable, cross-farm behavioral atlas to characterize operational limits using AIS dwells as proxies.

- [x] **Spatial Engine Implementation:**
    - [x] Developed `om_pipeline.spatial.bounds` for UTM-projected farm and context geometries.
    - [x] Implemented precision asset-proximity checks using `scipy.spatial.KDTree` (200m Tier A threshold).
- [x] **Track Segmentation & Dwell Detection:**
    - [x] Developed `ais_visit_extractor` to segment tracks into visits based on 5km context buffers and AIS gaps.
    - [x] Developed `ais_dwell_detector` with a 4-tier unsupervised geometric taxonomy (Asset-Proximal, Farm-Internal, Operational-Dwell, Context-Holding).
- [x] **Phase-Based Metocean Join:**
    - [x] Implemented `dwell_weather_join` to capture exposure across four critical phases: approach, active-dwell, departure, and matched non-dwell comparator.
    - [x] Calculated the **weather-exposure difference** between dwell and matched non-dwell windows.
- [x] **Validation Island (Wind Farm C):**
    - [x] Aligned AIS dwell candidates with CARE SCADA anomalies for Trianel Borkum I+II.
    - [x] Results: Directionally consistent correspondence found in 3 of 4 available dwells, but validation is underpowered due to local AIS coverage gaps.
- [x] **Cross-Farm Synthesis:**
    - [x] Generated the `Cross-Farm Dwell Atlas: Taxonomy & Behavioural Summary` and `Metocean Exposure Report`.
    - [x] Framework **implemented and pilot-verified across three farms**.
    - [x] Verdict: **Methodological extension successful / atlas pilot underpowered but scalable.**
- [x] **RQ9 Maintenance Intervention Intensity Feasibility:**
    - [x] Built farm-level phase-separated AIS-derived maintenance intervention intensity with commissioning/ramp-up and steady-operational periods separated.
    - [x] Built turbine-level feasibility, denominator/exposure v1, and turbine characteristics comparison v1 using high-confidence Tier A `<=200 m` assignment as primary evidence and high+medium `<=500 m` as sensitivity only.
    - [x] Audited the Baltic/North Sea contrast and preserved the key caveat: `sea_basin` is a geographic grouping, not a physical exposure metric or confirmed reliability signal.
    - [x] Added an AIS observability bias audit showing no per-vessel-message receiver assignment in current RQ9 tables; raw `Type of mobile = Base Station` AIS records provide observed base-station geometry as a source-geometry control, but vessel pings still need receiver-station linkage before basin claims are thesis-safe.
    - [ ] Next RQ9 research step: test whether intervention intensity varies by turbine age/capacity/OEM after controlling for farm, basin, and observability.

---

## Phase 5: Scaling & Impact Modeling
**Goal:** Apply findings to next-generation 15MW+ assets and US Eastern Seaboard deployment.

- [x] **Baltic Cluster Scaling (Wikinger):** 
    - [x] Implemented resumable backfill runner with cross-farm duplicate detection.
    - [x] Completed 10-year quarterly backfill (2010-2020) across 5 farms.
    - [x] Enriched with 4-phase NORA3 metocean exposure.
    - [x] Verdict: **Methodological extension successful / atlas pilot underpowered but scalable.**
- [ ] **US East Coast Transfer:** Apply the validated model to **MarineCadastre** AIS and **NOAA** buoy data for US lease areas.
- [ ] **15MW Extrapolation:** Use the "Vessel-Aware" ML model to predict operational uptime for 15MW-class SOVs using theoretical dimensions.
- [ ] **Monte Carlo Integration:** 
    - Replace the $H_s = 1.5m$ hard-cut in a fleet simulation (e.g., using `OpenOA` or a custom simulation).
- [ ] **Economic Synthesis:** Calculate the **LCoE reduction** resulting from optimized vessel dispatch and reduced downtime.
