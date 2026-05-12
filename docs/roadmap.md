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
- [ ] **Lightweight Analytics Setup:** Configure a local **DuckDB** or **SQLite** environment for initial exploratory spatial queries.

---

## Phase 1: Data Acquisition (First Strike: German Bight)
**Goal:** Acquire ground-truth data from the world's most instrumented offshore wind cluster.

- [/] **Logistics (AIS):** Download and process raw terrestrial AIS data from the **Danish Maritime Authority (DMA)**.
    - [x] **Automated Pipeline:** Hardened `stream_ais_filter.py` (Python-native HTTP/ZIP) and `identify_vessels_at_scale.py` (Event-based) implemented.
    - [x] **Repository Restructuring:** Implemented `src/om_pipeline` and agent-centric context layer.
    - [ ] **Longitudinal Sampling:** Complete 6-month "Time Slices" from 2009 to 2024 (Jan & July of each year).
- [ ] **Environment (In-Situ):** Access the **FINO1** database for 10-minute ground-truth wave spectra ( $H_s, T_p, \text{Direction}$ ).
- [ ] **Environment (Hindcast):** Pull **NORA3** 3km-resolution NetCDF files for regional validation and scaling.
- [ ] **Operations (SCADA/Structural):** 
    - Apply for the **RAVE (Research at Alpha Ventus)** archive access.
    - Download the **EDP "CARE to Compare"** dataset for anomaly/fault trigger training.

---

## Phase 2: The "10-Minute Backbone" Data Engineering
**Goal:** Synchronize disparate data streams into a unified training matrix.

- [x] **Event-Based Identification:** 
    - [x] Identify "Dwell Events" (SOG < 0.5 kn, Proximity < 100m, Duration >= 15 min).
    - [x] Catalog registered via DuckDB for SQL analysis.
- [ ] **Metocean Extraction & Upscaling:**
    - [x] Implement cache-aware NORA3 THREDDS extraction.
    - [x] Implement Cubic Spline (scalars) and Circular Vector (direction) upscaling to 10-minutes.
    - [ ] Extend the metocean backbone beyond wave-only NORA3 parameters to include wind and current once the wave backbone passes QA.
    - [ ] Run `extract_metocean.py` across the full seasonal dataset to generate the backbone.
- [ ] **Data Quality Assurance (QA):**
    - [ ] Run row count, schema, and alignment sanity checks on the NORA3 Backbone.
- [ ] **The AIS + Metocean Join:**
    - [ ] Create a formal module in `src/om_pipeline/` to merge `dwell_events` and the 10-minute backbone based on `found_id` and timestamps.
- [ ] **The "SCADA Handshake" & Feature Engineering:** 
    - Use RAVE/DPR data to label events as **Success** (Active Maintenance) or **Wait-on-Weather (WoW)**.
    - Compute event-level aggregates (mean/max Hs, Tp, direction).
- [ ] **Feature Matrix Construction:** Create a master CSV/Parquet file containing:
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
