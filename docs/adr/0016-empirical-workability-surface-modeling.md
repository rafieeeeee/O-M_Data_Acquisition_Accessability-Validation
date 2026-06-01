# ADR 0016: Empirical Workability Surface Modeling

## Status
Approved; clarified by ADR 0021 and the Slice 4 configurable surface refactor.

## Context
O&M simulation models traditionally use simplified binary limits (e.g. significant wave height $H_s < 1.5\text{ m}$ for Crew Transfer Vessels and $H_s < 2.5\text{ m}$ for Service Operation Vessels) to decide whether offshore wind turbines can be accessed. However, in real-world operations, vessel workability is governed by a complex, non-linear multi-parameter relationship, where wave period ($T_p$) plays an equal or greater role in motion response and safe boarding than wave height alone.

To address Research Question 1 (RQ1) of the EngD Research Plan, we need to replace these static limits with empirical, multi-parameter operating envelopes derived from actual historical operations. Calibrated access probabilities remain a later target and require an explicit denominator such as non-operation weather windows, failed attempts, or SCADA/WoW labels. Stage 1 now exists as an observed/provisional surface; Stage 2 has not started.

## Decision
We will implement an empirical workability surface modeling engine with the following design:

1. **Modular Architecture:** Core mathematical, binning, and percentiles logic is placed in `src/om_pipeline/analysis/workability.py`. A thin command-line interface is provided in `scripts/build_workability_surface.py`.
2. **Behavioral Proxy Definition:**
   - Successful observed operations are identified from Tier A (Asset-Proximal) dwells in our consolidated database (`dwell_events`) and matched metocean time series (`Data/Processed/cross_farm_dwell_weather_features.parquet`).
   - We will also support analysis of the SCADA-handshaked feature matrix (`Data/Processed/wind_farm_c_feature_matrix.parquet`) which has explicit `maintenance_success` labels.
3. **Configurable Surface Representation:**
   - The base surface engine accepts configurable feature columns, bin definitions, optional grouping columns, and long-form simulator-ready output.
   - The default Stage 1 preset is $H_s \times T_p$: wave period ($T_p$) is binned into discrete intervals reflecting sea-state physics: `[0, 3]`, `(3, 4]`, `(4, 5]`, `(5, 6]`, `(6, 7]`, `(7, 8]`, `(8, 10]`, and `(10, 15]` seconds.
   - For each default preset bin, we calculate the **95th percentile significant wave height ($H_s$)** of successful O&M events to define an observed successful-dwell upper envelope, filtering out anomalous weather/GPS drift outliers.
   - Later sensitivity views may add wind speed, true current speed, vessel, task, and site grouping dimensions through the same surface spec. Missing current remains null and must never be treated as zero; wind direction remains too sparse for primary predictors.
4. **Exploratory Vessel-Size Sensitivity:**
   - Provisional analyses may stratify observed envelopes by `vessel_length_m` bands as exploratory sensitivity only. These bands must not be labelled CTV/SOV unless validated by an external registry or authoritative vessel taxonomy.
5. **Lookup / Envelope Matrix:**
   - For provisional Stage 1, we will construct an $H_s \times T_p$ grid (0.1m $H_s$ resolution, 0.5s $T_p$ resolution) as an empirical observed-envelope or relative-workability heuristic derived from successful Tier A dwell events. It must not be labelled as calibrated $P(\text{Operation} | H_s, T_p)$ until non-operation weather windows, failed attempts, or SCADA/WoW labels are added.
   - Fusion v2 is the accepted/provisional feature layer for the next Stage 2 sensitivity branch, not a final model.

## Consequences
- The EngD O&M simulation tool can import Stage 1 tables only as provisional observed-envelope heuristics until a calibrated denominator is available.
- Provides a clean, publication-ready visualization (`empirical_hs_tp_workability_surface.png`) demonstrating the observed non-linear relationship between $H_s$ and $T_p$ for the thesis, with provisional labelling preserved.
- Prevents $H_s \times T_p$ from becoming the closed architecture: it is a default preset within a configurable surface contract.
