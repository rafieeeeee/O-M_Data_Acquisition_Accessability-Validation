# Start Here: Developer & Agent Entry Point

Welcome to the **O&M Data Acquisition & Accessibility** workspace. This directory is your starting point for understanding the repository layout, system architecture, and current execution status.

Rather than duplicating codebase context, this folder serves as a **highly structured index and navigation hub** to orient humans and AI agents with limited context windows.

---

## 🗺️ Documentation Directory Map

The ultimate authority on all domain and engineering contracts is maintained in the root context files and the `docs/` directory. Use the links below to navigate to the detailed records:

### 1. Root-Level Mandates
* **[CONTEXT.md](../CONTEXT.md)**: The absolute domain authority. Details Automatic Identification System (AIS) extraction models, the 10-minute Temporal backbone, metocean interpolation, and SCADA de-anonymization.
* **[AGENTS.md](../AGENTS.md)**: The core mandates, rules of engagement, and workflow requirements for semi-autonomous development.

### 2. Operational Planning & Design Notes
* **[docs/README.md](../docs/README.md)**: The global documentation map of the repository.
* **[docs/roadmap.md](../docs/roadmap.md)**: Phase-by-phase timeline of milestones, thesis objectives, and accomplished/blocked tasks.
* **[docs/thesis-methodology.md](../docs/thesis-methodology.md)**: Academic-facing explanations of spatial filters, sampling methods, and validation constraints.
* **[docs/provenance.md](../docs/provenance.md)**: Source database lineage and pilot run metadata.
* **[docs/adr/](../docs/adr/)**: Architectural Decision Records (ADRs) tracking significant design modifications.
* **[docs/adr/0014-european-dwell-harvest.md](../docs/adr/0014-european-dwell-harvest.md)**: Current decision record for broad European/German dwell-event harvesting from the turbine coordinate table.
* **[docs/adr/0016-empirical-workability-surface-modeling.md](../docs/adr/0016-empirical-workability-surface-modeling.md)**: Current Stage 1 workability surface contract; $H_s \times T_p$ is the default preset, not the closed model.
* **[docs/adr/0019-common-regional-metocean-extraction-methodology.md](../docs/adr/0019-common-regional-metocean-extraction-methodology.md)**: Decision record for the shared NWS/Baltic continuous metocean requirements planner.
* **[docs/adr/0029-metocean-fusion-v2-multiparameter-event-features.md](../docs/adr/0029-metocean-fusion-v2-multiparameter-event-features.md)**: Decision record for the accepted Fusion v2 multi-parameter event feature table.
* **[COMMON_METOCEAN_EXTRACTION_METHODOLOGY.md](../analysis/06_rq6_metocean_spatial_resolution/COMMON_METOCEAN_EXTRACTION_METHODOLOGY.md)**: Authoritative executable contract for Agent 2 / NWS and Agent 3 / Baltic metocean planning.
* **[docs/metocean-acquisition.md](../docs/metocean-acquisition.md)**: Current storage map and acquisition status for NORA3, NWS, and Baltic wave archives.
* **[docs/agent-handoff-metocean-fusion-v2.md](../docs/agent-handoff-metocean-fusion-v2.md)**: Current handover for Stage 2 modelling after Fusion v2.

### 3. Current State Highlights

These are the fastest orientation facts for the current repository state:

* **European farm-candidate AIS raw slices:** `180` month files currently exist under `Data/Raw/AIS/`.
* **European dwell backfill:** `4,328` parquet month partitions exist across `113` farms under `data/processed/ais_dwell_backfill/dwells/`.
* **NWS wave backbone:** complete processed archive exists under `Data/Processed/metocean/nws_wave_timeseries/` with `112` farms and `1,169` farm-year partitions.
* **Baltic wave backbone:** reviewed raw Copernicus subsets exist under `Data/Raw/Metocean/CMEMS/BalticSea/Waves/` for `16` farms and the processed native-hourly archive is accepted under `Data/Processed/metocean/baltic_wave_timeseries/` with `238` partitions and `73,866,720` rows.
* **Bathymetry site context:** EMODnet point-sample assignment exists under `Data/Processed/metocean/bathymetry/site_bathymetry_points.parquet` with `119` farms and `6,642` common metocean sample points.
* **Wave confidence:** Fusion v1 source-agreement output exists under `Data/Processed/metocean/fusion_v1_source_agreement/`; validation lives under `reports/metocean_fusion_v1_source_agreement/`.
* **Current confidence:** NWS event-scale true `uo`/`vo` current confidence exists under `Data/Processed/metocean/current_confidence_v1/` with `16,307` event-scale current assignments.
* **Wind confidence:** NORA3 wind confidence exists under `Data/Processed/metocean/wind_confidence_v1/` with `75,380` wind-speed-ready events and only `197` direction-ready events.
* **Metocean Fusion v2:** accepted multi-parameter event feature table exists under `Data/Processed/metocean/fusion_v2/dwell_metocean_fusion_v2.parquet`; validation lives under `reports/metocean_fusion_v2/`.
* **Stage 1 workability:** observed/provisional $H_s \times T_p$ surface exists as the default preset in a configurable workability engine.
* **Stage 2 modelling:** not started. The next branch should compare Fusion v2 wave-only, wave+wind speed, wave+current, and wave+wind+current subsets before any calibrated probability model.
* **FINO validation planning:** dry-run access and station-to-farm proximity planning exists at `analysis/06_rq6_metocean_spatial_resolution/fino_metadata_access_plan.md`; a report-only native export inspector exists at `scripts/inspect_fino_export.py`; no FINO time-series import has been run.
* **NORA3 sidecar cache:** joined checkpoint batches are written to `Data/Processed/metocean/nora3_joined_cache/` while the downloader is active.

---

## 📂 Start-Here Index Kit

To quickly ramp up on current progress and prepare for immediate work, navigate through the companion index files in this directory:

* **[01_project_state_summary.md](01_project_state_summary.md)**: Audit map of completed pipeline runs, current manifests, and metocean archive status.
* **[02_incremental_dev_guide.md](02_incremental_dev_guide.md)**: Playbook for monitoring and extending the broad European dwell harvest without breaking resumability.
* **[03_domain_map_cheat_sheet.md](03_domain_map_cheat_sheet.md)**: Rapid index of spatial boundaries, verified dwell tier thresholds, and SCADA shifts.
