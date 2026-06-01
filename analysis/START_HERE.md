# START HERE — EngD O&M Empirical Engine

## Purpose
This analysis directory supports the EngD research programme converting AIS, metocean, vessel, and SCADA evidence into simulator-ready O&M behavioural modules.

The central objective is to replace generic static assumptions such as “CTV works below 1.5 m Hs” or “SOV works below 2.5 m Hs” with evidence-based observed workability surfaces and, later, calibrated operating models that reflect how vessels actually behave offshore.

## Current Status
* **Phase A Data Foundation:** Complete.
* **Phase A.1 Hardening & QA:** Complete (documented in `analysis/00_data_foundation/foundation_qa_report.md` and `analysis/00_data_foundation/om_event_table_metadata.json`).
* **Stage 1 Workability:** Exists as an observed/provisional surface. $H_s \times T_p$ is the default preset, not the closed definition of workability.
* **Stage 2 Modelling:** Not started. The next branch should use Fusion v2 to compare wave-only, wave+wind speed, wave+current, and wave+wind+current evidence slices before any calibrated probability model.
* **Fusion v2:** Accepted/provisional event feature layer with `92,660` dwell rows, `75,380` wind-speed-ready rows, `16,307` event-scale current rows, and `13,207` wave+wind+current rows.

## Canonical Input Tables
* **Stage 2 feature layer:** `Data/Processed/metocean/fusion_v2/dwell_metocean_fusion_v2.parquet`
* **Stage 1 observed surface source:** `Data/Processed/ais_dwell_backfill/cross_farm_dwell_weather_features.parquet`
* **Wave confidence:** `Data/Processed/metocean/fusion_v1_source_agreement/wave_event_confidence.parquet`
* **Wind confidence:** `Data/Processed/metocean/wind_confidence_v1/wind_event_confidence.parquet`
* **Current confidence:** `Data/Processed/metocean/current_confidence_v1/current_event_confidence.parquet`
* **Legacy pilot table:** `Data/Processed/analysis/om_event_table.parquet` is historical context only and should not define new modelling scope.

## How to Work
1. Read this file.
2. Read `DATA_ACCESS.md` to understand variables and table joins.
3. Read the relevant RQ folder (`01_rq1_workability_envelope` to `11_rq11_safety_working_practices`).
4. **Do not start Stage 2 modelling** until branch reconciliation and documentation readiness are complete.
5. Write notebooks strictly inside the relevant RQ folder.
6. Put reproducible scripts beside the notebook only after exploratory logic is reviewed.

## Current Approved Next Activity
* Documentation readiness and branch reconciliation.
* Stage 2 design only after the cleanup branch is reconciled and a clean Stage 2 branch is approved.

## Red-line Rules
> [!CAUTION]
> **1. No Mega-Scripts:** Maintain modularity. Keep analysis files focused on their respective folders.
>
> **2. No Unbiased Probability Models from Successful Events Only:** Do not call any fit $P(\text{operation} \mid \text{weather})$ unless control/non-operation windows are explicitly constructed.
>
> **3. No CTV/SOV/HLV Inference:** Treat vessel physical properties as continuous dimensions (`vessel_length_m`, `vessel_draft_m`) or raw DMA registry text labels. Do not use dynamic/heuristic classification rules.
>
> **4. No Synthetic Current:** Model current effects only from accepted true `uo`/`vo` evidence. Missing current remains missing/null and must never be interpreted as zero current.
>
> **5. Wind Direction Quarantine:** Wind speed may be used in Stage 2 sensitivity. Wind direction is too sparse for broad modelling and must not be a primary predictor until a targeted repair is approved.
>
> **6. No Economic Dispatch Analysis:** Wait for SCADA and wake models to be formally verified.
