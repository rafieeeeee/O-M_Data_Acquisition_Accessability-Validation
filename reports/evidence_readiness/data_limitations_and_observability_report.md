# Data Limitations And Observability Report

## Scope

This first evidence-readiness audit integrates existing outputs only. It did not rerun AIS extraction, rerun metocean extraction, modify raw/interim/source data, delete data, or start new RQ modelling.

AIS visits are treated as observation and candidate intervention evidence. They are not confirmed failures; confirmation requires SCADA, fault-log, work-order, or equivalent validation.

## Inputs Inspected

- AIS dwell/weather feature table and AIS backfill manifest.
- Raw AIS European Master CSV schemas for vessel/source columns.
- European turbine coordinates and RQ9 farm/turbine intervention-intensity outputs.
- Fusion v2, Wind Confidence v1, Current Confidence v1, wave archive partitions, and EMODnet bathymetry outputs.
- CARE Wind Farm B/C SCADA event files, Wind Farm C feature matrix, and event aggregates where present.

## Coverage Findings

- Farm-month matrix rows: 22,052.
- Turbine-month matrix rows: 1,270,500.
- Observed AIS source farm-months: 20,646.
- Missing-source farm-months: 1,406. These are missing evidence, not zero-event months.
- Observed-zero farm-months: 16,221. These have observed source coverage but no dwell/event evidence in the manifest.
- Farm-month confidence classes: {'C_observed_zero': 16221, 'B_integrated_proxy': 3367, 'D_missing_source': 1406, 'B_integrated_high_assignment': 811, 'C_partial_proxy': 226, 'A_local_validated': 21}.
- Turbine-month confidence classes: {'C_observed_zero': 1180775, 'D_missing_source': 80990, 'B_integrated_proxy': 5312, 'B_integrated_high_assignment': 3386, 'C_partial_proxy': 35, 'A_local_validated': 2}.

## Geographic Missingness

Source coverage is uneven by country and sea basin. The summary below is an observability audit, not a ranking of operational performance.

| country        | sea_basin   |   farm_count |   farm_month_count |   observed_source_months |   skipped_missing_source_months |   observed_zero_months |   ais_dwell_event_count |   tier_a_count |   tier_b_count |   tier_c_count |   tier_d_count |   wave_available_months |   wind_speed_available_months |   wind_direction_available_months |   current_available_months |   bathymetry_available_months |   scada_validation_available_months |   vessel_metadata_available_months |   median_high_confidence_assignment_share |   median_top_mmsi_concentration |   coverage_share |   missing_source_share |   observed_zero_share |
|:---------------|:------------|-------------:|-------------------:|-------------------------:|--------------------------------:|-----------------------:|------------------------:|---------------:|---------------:|---------------:|---------------:|------------------------:|------------------------------:|----------------------------------:|---------------------------:|------------------------------:|------------------------------------:|-----------------------------------:|------------------------------------------:|--------------------------------:|-----------------:|-----------------------:|----------------------:|
| Germany        | Baltic      |            6 |               1508 |                     1386 |                             122 |                    740 |                    8968 |           3983 |            340 |            577 |           4068 |                     909 |                           644 |                                 0 |                          0 |                          1508 |                                   0 |                                  0 |                                  0.169492 |                        0.5      |         0.919098 |              0.0809019 |              0.533911 |
| Denmark        | Baltic      |           10 |               1920 |                     1800 |                             120 |                    830 |                   36782 |           3002 |            573 |            405 |          32802 |                    1749 |                           964 |                                 0 |                          0 |                          1920 |                                   0 |                                  0 |                                  0.230769 |                        0.333333 |         0.9375   |              0.0625    |              0.461111 |
| Sweden         | Baltic      |            1 |                192 |                      180 |                              12 |                     89 |                     450 |            310 |              0 |              5 |            135 |                     182 |                            88 |                                 0 |                          0 |                           192 |                                   0 |                                  0 |                                  1        |                        1        |         0.9375   |              0.0625    |              0.494444 |
| Belgium        | North Sea   |           10 |               1920 |                     1800 |                             120 |                   1694 |                     449 |             94 |             25 |             51 |            279 |                    1126 |                            88 |                                 0 |                         12 |                          1920 |                                   0 |                                  0 |                                  0.2      |                        0.666667 |         0.9375   |              0.0625    |              0.941111 |
| Denmark        | North Sea   |            7 |               1344 |                     1260 |                              84 |                    595 |                   26747 |           2197 |             61 |            229 |          24260 |                     827 |                           652 |                                 0 |                        220 |                          1344 |                                   0 |                                  0 |                                  0.125    |                        0.25     |         0.9375   |              0.0625    |              0.472222 |
| Germany        | North Sea   |           24 |               4608 |                     4320 |                             288 |                   3019 |                   16090 |           4578 |           1114 |            883 |           9515 |                    2559 |                          1185 |                                49 |                       1059 |                          4608 |                                  39 |                                  0 |                                  0.166667 |                        0.5      |         0.9375   |              0.0625    |              0.698843 |
| Netherlands    | North Sea   |            8 |               1536 |                     1440 |                              96 |                   1299 |                     709 |             78 |             46 |             66 |            519 |                     793 |                           132 |                                 0 |                         48 |                          1536 |                                   0 |                                  0 |                                  0.157895 |                        0.5      |         0.9375   |              0.0625    |              0.902083 |
| United Kingdom | North Sea   |           44 |               8448 |                     7920 |                             528 |                   7425 |                    2978 |           1210 |            184 |            152 |           1432 |                    5370 |                           459 |                                 0 |                        132 |                          8448 |                                   0 |                                  0 |                                  0.390476 |                        0.5      |         0.9375   |              0.0625    |              0.9375   |

## Vessel Metadata And AIS Observability

- MMSI is present for dwell rows, but integrated vessel enrichment is available in 0 farm-months.
- The current dwell/Fusion/RQ9 tables do not carry direct AIS receiver station, terrestrial/satellite channel, receiver coordinates, or receiver-distance fields.
- Top-MMSI concentration and high-confidence turbine assignment share are retained as indirect observability proxies only.
- Raw AIS schemas include vessel-name/type/dimension/source columns, but those registry fields are not yet populated in the integrated dwell evidence layer.

## Metocean Completeness

- Wave availability months: 13,585.
- Wind speed availability months: 4,222.
- Wind direction availability months: 49. Wind direction remains sparse and sensitivity-only.
- Current availability months: 1,471. Current coverage is source/domain dependent and must not be treated as zero current when missing.
- Bathymetry availability months: 22,052. Static bathymetry is broad, but shallow/coastal warnings still require interpretation.

## SCADA Validation Availability

- SCADA validation farm-months: 39.
- Validation is localized to CARE Wind Farm B/C mappings, not a Europe-wide denominator.
- Wind Farm C has a processed feature matrix and event aggregates; Wind Farm B/C raw CARE event datasets are present.

## RQ Readiness Summary

- Answerability counts: {'blocked': 7, 'partial': 4, 'ready': 1}.

| rq_number   | answerability   | missing_layers                                             | recommended_next_action                                                                                |
|:------------|:----------------|:-----------------------------------------------------------|:-------------------------------------------------------------------------------------------------------|
| RQ1         | partial         | broad SCADA/non-operation denominator                      | Use observed envelope only; add failed/non-operation denominator before access probability.            |
| RQ2         | blocked         | vessel registry/access metadata                            | Prioritize vessel registry enrichment for length, beam, draft, and access technology.                  |
| RQ3         | blocked         | vessel registry/access metadata                            | Add authoritative vessel build year and registry provenance before trend claims.                       |
| RQ4         | partial         | broad SCADA/non-operation denominator                      | Confine to local CARE validation slices until failed/short-attempt labels are broader.                 |
| RQ5         | blocked         | full trajectory tracks; vessel registry/access metadata    | Recover voyage/port-gap trajectories and registry labels before campaign claims.                       |
| RQ6         | ready           |                                                            | Proceed with source-aware sensitivity; keep wind direction and current coverage limitations explicit.  |
| RQ7         | blocked         | external market/curtailment data                           | Add curtailment/market source and restrict current work to validation design.                          |
| RQ8         | blocked         | wake model inputs                                          | Define wake/value inputs and broaden SCADA linkage beyond local CARE slices.                           |
| RQ9         | blocked         | direct AIS receiver/source geometry; fault/work-order logs | Keep as intervention-intensity evidence; add receiver/source controls and fault/work-order validation. |
| RQ10        | blocked         | external oil and gas benchmark data                        | Identify accepted O&G benchmark source before comparison.                                              |
| RQ11        | partial         | solar/light features; vessel registry/access metadata      | Add deterministic light features and registry enrichment before safety-practice interpretation.        |
| RQ12        | partial         | vessel registry/access metadata                            | Run simulator ablations only after vessel metadata and validation labels are explicit.                 |

## Guardrails

- Do not call AIS visits failures.
- Do not interpret skipped source months as zero-event months.
- Do not treat missing current or wind direction as zero physical values.
- Do not convert sea-basin contrasts into reliability claims without receiver/source geometry and validation labels.
