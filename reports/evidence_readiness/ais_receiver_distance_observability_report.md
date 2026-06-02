# AIS Receiver Distance And Source Observability Audit

## Scope

This is an audit-first observability-bias study built from existing local artifacts. It does not rerun AIS extraction, rerun metocean extraction, modify raw/interim/source data, or start new research-question modelling.

AIS events are treated as observed vessel/dwell/intervention proxies only. A lower AIS event density is not a lower intervention rate, and it is not a lower failure rate.

## Reproducibility

- Source of truth: `scripts/build_ais_observability_bias.py` calls `src/om_pipeline/analysis/ais_observability_bias.py` from the repository root.
- Rebuild command: `/opt/anaconda3/bin/python scripts/build_ais_observability_bias.py`.
- Generated matrices under `Data/Processed/analysis/ais_observability_bias/` are derived outputs and may be ignored by git; regenerate them rather than hand-editing them.
- The tracked report and methodology are refreshed from the same builder and should preserve the missingness and no-failure-claim guardrails.

## Additional Observability Layers

Build the audit around four evidence tiers:

Tier 1a: per-message receiver assignment evidence
- vessel ping linked to receiving station ID
- receiver station ID attached to each vessel message
- receiver channel/terrestrial/satellite assignment attached to each vessel message

Tier 1b: direct AIS source-geometry reference
- `Type of mobile = Base Station` AIS records with station MMSI and latitude/longitude
- source-provider geometry

Tier 2: source-channel evidence
- `Data source type`
- provider/source system
- source file/month status
- manifest `input_rows` and `clean_rows`

Tier 3: geographic proxy evidence
- farm centroid latitude/longitude
- distance to nearest external receiver if receiver reference data is allowed and provenance-tracked
- distance to coast/offshore proxy if available
- country
- sea basin
- water depth/bathymetry

Tier 4: downstream observability proxies
- observed-zero months under source coverage
- dwell/event density
- Tier A/B/C/D counts
- high-confidence assignment share
- top MMSI concentration
- unique MMSI count
- vessel metadata completeness

Interpretation rule:
Only per-message receiver assignment can directly test receiver-distance bias. AIS base-station geometry is a source-geometry control; Tiers 2-4 can support or weaken a broader AIS observability-bias hypothesis but cannot confirm receiver-distance causality.

## Receiver/Source Metadata Inventory

- No per-vessel-message receiver station ID or receiver assignment field was found in the inspected local AIS/RQ9 schemas.
- Raw AIS contains `Type of mobile = Base Station` records: 2 station MMSIs from 11,596 messages. These provide direct AIS base-station geometry reference for source-geometry controls, not proof of which station received each vessel ping.
- No external receiver/coastline reference was used in this run; no receiver-distance field was imputed.
- Inspected schema sources: 9.
- Raw AIS files inspected by header only: 181.
- Raw AIS files with `Data source type`: 181.
- Per-message receiver-like schema fields found: 0. Source-channel fields found: 7. Proxy fields found: 34.
- AIS base-station catalogue rows: 2.

`Data source type` is source-channel evidence, not receiver station geometry. `Type of mobile = Base Station` rows are direct AIS base-station geometry reference, but they do not provide per-message receiver assignment. Source file metadata and manifest row counts support source availability/intensity auditing.

## Source-Intensity Layer

- Farm-month rows: 22,052.
- Observed source farm-months: 20,646.
- Missing-source farm-months: 1,406. `skipped_missing_source` is missing source evidence, not zero events.
- Observed-zero farm-months: 16,221. These are counted only where source coverage is observed.
- Candidate AIS rows are separated from dwell/event rows: total `clean_rows` = 520,693,432; total dwell events = 93,187.

Primary comparisons should use observed source months only and exclude `skipped_missing_source`. Source intensity (`input_rows`, `clean_rows`) is reported separately from dwell/event density so sparse raw AIS candidate evidence is not conflated with dwell-detection output.

## Geographic Diagnostics

The table below is a geographic observability diagnostic, not an operational-performance ranking. Raw sea-basin contrasts must be checked within countries or matched strata before being used as evidence.

| country        | sea_basin   |   farm_count |   farm_month_count |   observed_source_months |   missing_source_months |   observed_zero_months |   source_clean_rows_total |   ais_dwell_event_count_total |   tier_ab_event_count_total |   median_clean_rows_per_turbine |   median_dwell_events_per_turbine | median_high_confidence_assignment_share   |   median_top_mmsi_concentration |   median_vessel_metadata_event_share |   median_distance_to_nearest_observed_base_station_km |   source_coverage_share |   missing_source_share |   observed_zero_share_of_observed |   clean_rows_per_observed_month |   dwell_events_per_observed_month |
|:---------------|:------------|-------------:|-------------------:|-------------------------:|------------------------:|-----------------------:|--------------------------:|------------------------------:|----------------------------:|--------------------------------:|----------------------------------:|:------------------------------------------|--------------------------------:|-------------------------------------:|------------------------------------------------------:|------------------------:|-----------------------:|----------------------------------:|--------------------------------:|----------------------------------:|
| France         | other       |            3 |                576 |                      540 |                      36 |                    530 |            2876           |                            14 |                           2 |                          0      |                         0         | <NA>                                      |                        1        |                                    0 |                                              494.391  |                0.9375   |              0.0625    |                          0.981481 |                         5.32593 |                         0.0259259 |
| Belgium        | North Sea   |           10 |               1920 |                     1800 |                     120 |                   1694 |          340289           |                           449 |                         119 |                          0      |                         0         | 0.2                                       |                        0.666667 |                                    0 |                                                8.8177 |                0.9375   |              0.0625    |                          0.941111 |                       189.049   |                         0.249444  |
| United Kingdom | North Sea   |           44 |               8448 |                     7920 |                     528 |                   7425 |               1.0838e+06  |                          2978 |                        1394 |                          0      |                         0         | 0.3904761904761905                        |                        0.5      |                                    0 |                                              435.446  |                0.9375   |              0.0625    |                          0.9375   |                       136.843   |                         0.37601   |
| Netherlands    | North Sea   |            8 |               1536 |                     1440 |                      96 |                   1299 |          369051           |                           709 |                         124 |                          0      |                         0         | 0.15789473684210525                       |                        0.5      |                                    0 |                                              129.661  |                0.9375   |              0.0625    |                          0.902083 |                       256.285   |                         0.492361  |
| Germany        | North Sea   |           24 |               4608 |                     4320 |                     288 |                   3019 |               2.08263e+07 |                         16090 |                        5692 |                          0      |                         0         | 0.16666666666666666                       |                        0.5      |                                    0 |                                              360.438  |                0.9375   |              0.0625    |                          0.698843 |                      4820.91    |                         3.72454   |
| Germany        | Baltic      |            6 |               1508 |                     1386 |                     122 |                    740 |               6.01669e+07 |                          8968 |                        4323 |                         12.8924 |                         0         | 0.1694915254237288                        |                        0.5      |                                    0 |                                               56.8958 |                0.919098 |              0.0809019 |                          0.533911 |                     43410.5     |                         6.47042   |
| Sweden         | Baltic      |            1 |                192 |                      180 |                      12 |                     89 |               3.51407e+06 |                           450 |                         310 |                         54.0938 |                         0.0208333 | 1.0                                       |                        1        |                                    0 |                                               62.2409 |                0.9375   |              0.0625    |                          0.494444 |                     19522.6     |                         2.5       |
| Denmark        | North Sea   |            7 |               1344 |                     1260 |                      84 |                    595 |               2.94501e+08 |                         26747 |                        2258 |                        205.853  |                         0.0125    | 0.125                                     |                        0.25     |                                    0 |                                              360.62   |                0.9375   |              0.0625    |                          0.472222 |                    233731       |                        21.2278    |
| Denmark        | Baltic      |           10 |               1920 |                     1800 |                     120 |                    830 |               1.39889e+08 |                         36782 |                        3575 |                         34.3345 |                         0.0138889 | 0.23076923076923078                       |                        0.333333 |                                    0 |                                              132.659  |                0.9375   |              0.0625    |                          0.461111 |                     77716.3     |                        20.4344    |

## Matched Base-Station Distance Diagnostic

Eligible matched strata: 1,260; bias-consistent strata: 116; downstream-only strata: 169; no-clear strata: 975. Within-country/sea-basin/year-month diagnostics do not show a clear matched base-station-distance gradient. This does not prove absence of AIS observability bias; it only limits this proxy diagnostic.

The diagnostic below uses observed-source farm-months only, excludes `skipped_missing_source`, bins nearest observed AIS base-station distance within country/sea-basin/year-month strata, and compares source `clean_rows` separately from downstream dwell/Tier proxies. Nearest observed AIS base station remains a source-geometry control only; it is not evidence that the station received any vessel ping.

| country     | sea_basin   |   year | month   |   farm_month_count | near_distance_bin   | far_distance_bin   |   clean_rows_per_turbine_month_far_near_ratio |   observed_zero_rate_far_minus_near |   dwell_events_per_turbine_month_far_near_ratio | diagnostic_class                                  |
|:------------|:------------|-------:|:--------|-------------------:|:--------------------|:-------------------|----------------------------------------------:|------------------------------------:|------------------------------------------------:|:--------------------------------------------------|
| Belgium     | North Sea   |   2024 | 2024-08 |                 10 | q1_nearest          | q4_farthest        |                                     0.24      |                            1        |                                        0        | consistent_with_geographic_ais_observability_bias |
| Netherlands | North Sea   |   2012 | 2012-04 |                  8 | q1_nearest          | q4_farthest        |                                     0.052823  |                            1        |                                        0        | consistent_with_geographic_ais_observability_bias |
| Germany     | Baltic      |   2010 | 2010-04 |                 11 | q1_nearest          | q4_farthest        |                                     0.0565746 |                            0.666667 |                                        0        | consistent_with_geographic_ais_observability_bias |
| Germany     | North Sea   |   2011 | 2011-08 |                 24 | q1_nearest          | q4_farthest        |                                     0.257733  |                            0.666667 |                                        0        | consistent_with_geographic_ais_observability_bias |
| Germany     | Baltic      |   2012 | 2012-06 |                  6 | near                | far                |                                     0.145097  |                            0.666667 |                                        0.207947 | consistent_with_geographic_ais_observability_bias |
| Denmark     | Baltic      |   2010 | 2010-02 |                 10 | q1_nearest          | q4_farthest        |                                     0.481333  |                            0.5      |                                        1.23913  | consistent_with_geographic_ais_observability_bias |
| Denmark     | Baltic      |   2010 | 2010-03 |                 10 | q1_nearest          | q4_farthest        |                                     0.726805  |                            0.5      |                                        1.30797  | consistent_with_geographic_ais_observability_bias |
| Denmark     | Baltic      |   2010 | 2010-11 |                 10 | q1_nearest          | q4_farthest        |                                     0.729297  |                            0.5      |                                        0.953177 | consistent_with_geographic_ais_observability_bias |
| Denmark     | Baltic      |   2011 | 2011-02 |                 10 | q1_nearest          | q4_farthest        |                                     0.671226  |                            0.5      |                                        2.22011  | consistent_with_geographic_ais_observability_bias |
| Denmark     | Baltic      |   2012 | 2012-11 |                 10 | q1_nearest          | q4_farthest        |                                     0.587862  |                            0.5      |                                        1.47826  | consistent_with_geographic_ais_observability_bias |
| Denmark     | Baltic      |   2022 | 2022-02 |                 10 | q1_nearest          | q4_farthest        |                                     0.207892  |                            0.5      |                                        0.431002 | consistent_with_geographic_ais_observability_bias |
| Denmark     | Baltic      |   2022 | 2022-11 |                 10 | q1_nearest          | q4_farthest        |                                     0.234468  |                            0.5      |                                        0.447464 | consistent_with_geographic_ais_observability_bias |

## Highest Missingness/Observed-Zero Farms

| farm_id                | country        | sea_basin   |   observed_source_months |   missing_source_share |   observed_zero_share_of_observed |   clean_rows_per_observed_month |   dwell_events_per_observed_month |
|:-----------------------|:---------------|:------------|-------------------------:|-----------------------:|----------------------------------:|--------------------------------:|----------------------------------:|
| Baltic Eagle           | Germany        | Baltic      |                      239 |              0.0842912 |                          0.615063 |                     66482.6     |                        7.61088    |
| Wikinger               | Germany        | Baltic      |                      239 |              0.0842912 |                          0.523013 |                     62483.5     |                        9.02092    |
| Arkona-Becken Südost   | Germany        | Baltic      |                      239 |              0.0842912 |                          0.405858 |                     59523.5     |                       10.5523     |
| EnBW Windpark Baltic 2 | Germany        | Baltic      |                      239 |              0.0842912 |                          0.380753 |                     14348.1     |                        4.43933    |
| EnBW Windpark Baltic 1 | Germany        | Baltic      |                      250 |              0.0808824 |                          0.616    |                      5608.61    |                        2.024      |
| Kincardine             | United Kingdom | North Sea   |                      180 |              0.0625    |                          0.994444 |                         2.83333 |                        0.00555556 |
| Fécamp                 | France         | other       |                      180 |              0.0625    |                          0.983333 |                         1.59444 |                        0.0222222  |
| Saint-Brieuc           | France         | other       |                      180 |              0.0625    |                          0.983333 |                         4.57222 |                        0.0222222  |
| Saint-Nazaire          | France         | other       |                      180 |              0.0625    |                          0.977778 |                         9.81111 |                        0.0333333  |
| Seagreen               | United Kingdom | North Sea   |                      180 |              0.0625    |                          0.972222 |                        14.75    |                        0.0555556  |

## Interpretation

- The current local data can test source availability, source intensity, observed-zero frequency, vessel concentration, assignment-confidence proxies, and farm distance to nearest observed AIS base-station geometry.
- The current local data cannot directly test receiver-distance causality unless vessel pings can be linked to receiving station IDs or equivalent per-message receiver assignments.
- Do not infer receiver locations from vessel positions.
- Do not claim the nearest observed base station received a vessel ping.
- Do not treat nearest coast as nearest receiver unless an accepted external reference justifies that assumption.
- Do not compare raw Baltic/North Sea rates without within-country or matched-strata checks.
- Do not call lower AIS event density lower intervention activity or lower failure rate.

## RQ Answerability Impact

RQ9 remains blocked for failure claims and causal receiver-distance claims until per-message AIS receiver/source assignment and fault/work-order validation exist. The observed AIS base-station catalogue supports source-geometry controls only. The audit can support evidence-readiness and source-aware sensitivity work, but not confirmed failure-rate inference.
