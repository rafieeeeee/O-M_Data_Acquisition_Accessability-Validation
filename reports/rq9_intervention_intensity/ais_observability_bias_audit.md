# RQ9 AIS Observability Bias Audit

This audit adds an AIS observability correction layer before treating the Baltic/North Sea contrast as an operational or reliability signal. It uses existing RQ9 outputs only. It reports AIS-derived intervention intensity, not confirmed failure rate.

## Direct AIS receiver/source metadata

Direct receiver-distance controls are not available in the current RQ9 evidence tables. The available columns were checked for receiver ID, station ID, AIS source type, terrestrial/satellite flag, receiver coordinates, and message-source fields:

- turbine_intervention_events_v0.csv: none
- turbine_intervention_intensity_v1.csv: none
- turbine_exposure_denominator.csv: none
- cross_farm_dwell_weather_features.parquet: none
- backfill_manifest.csv: none

Source-like fields exist, but they are not receiver geometry fields:

- cross_farm_dwell_weather_features.parquet: ['current_source', 'current_source_available', 'registry_source', 'approach_source_available', 'active_source_available', 'departure_source_available', 'comparator_source_available']
- backfill_manifest.csv: ['source_file_name', 'source_file_size_bytes', 'source_file_modified_time']

`backfill_manifest.csv` contains source-file names, sizes, and modified timestamps. Those fields support source availability auditing, but they do not identify receiver station, terrestrial/satellite channel, or distance from receiver.

The dwell table contains `near_substation`, but that is an asset-proximity flag, not an AIS receiver station field.

## Proxy observability controls used

Because receiver-distance metadata is absent, this audit uses proxy controls already available locally:

- farm centroid latitude/longitude,
- country and sea basin,
- observed source coverage share,
- `success_no_ais_in_bbox` observed-zero share,
- `skipped_missing_source` share,
- Tier A high-confidence assignment share,
- high+medium assignment sensitivity,
- top-5 MMSI concentration,
- zero-event turbine share,
- dwell duration and Tier A/B evidence from existing dwell outputs.

No external receiver, coastline, or port data was downloaded or derived.

## Basin summary under available controls

| control                                | sea_basin   |   farm_count |   turbine_count |   observed_steady_turbine_years |   primary_intervention_intensity_per_steady_turbine_year |   sensitivity_intervention_intensity_per_steady_turbine_year |   baltic_to_north_primary_ratio |   baltic_to_north_sensitivity_ratio |   median_source_coverage_share |   median_tier_a_high_assignment_share |   median_high_confidence_top5_mmsi_share |
|:---------------------------------------|:------------|-------------:|----------------:|--------------------------------:|---------------------------------------------------------:|-------------------------------------------------------------:|--------------------------------:|------------------------------------:|-------------------------------:|--------------------------------------:|-----------------------------------------:|
| baseline_all_available_proxy_controls  | Baltic      |           17 |             755 |                        6987.667 |                                                    0.248 |                                                        0.628 |                           6.315 |                               5.725 |                          0.931 |                                 0.305 |                                    0.965 |
| baseline_all_available_proxy_controls  | North Sea   |           99 |            5555 |                       43293.333 |                                                    0.039 |                                                        0.110 |                           6.315 |                               5.725 |                          0.897 |                                 0.318 |                                    1.000 |
| farm_source_coverage_ge_0_8            | Baltic      |           14 |             606 |                        6759.667 |                                                    0.223 |                                                        0.583 |                           5.749 |                               5.400 |                          0.934 |                                 0.318 |                                    0.965 |
| farm_source_coverage_ge_0_8            | North Sea   |           79 |            4571 |                       42018.000 |                                                    0.039 |                                                        0.108 |                           5.749 |                               5.400 |                          0.901 |                                 0.327 |                                    1.000 |
| exclude_high_observed_zero_share       | Baltic      |           16 |             705 |                        6987.667 |                                                    0.248 |                                                        0.628 |                           4.506 |                               4.040 |                          0.931 |                                 0.305 |                                    0.965 |
| exclude_high_observed_zero_share       | North Sea   |           65 |            3894 |                       29858.583 |                                                    0.055 |                                                        0.155 |                           4.506 |                               4.040 |                          0.896 |                                 0.292 |                                    1.000 |
| exclude_high_missing_source_share      | Baltic      |           14 |             606 |                        6759.667 |                                                    0.223 |                                                        0.583 |                           5.734 |                               5.296 |                          0.934 |                                 0.318 |                                    0.965 |
| exclude_high_missing_source_share      | North Sea   |           69 |            3970 |                       39432.917 |                                                    0.039 |                                                        0.110 |                           5.734 |                               5.296 |                          0.904 |                                 0.333 |                                    1.000 |
| high_assignment_quality_farms          | Baltic      |            8 |             312 |                        3687.333 |                                                    0.279 |                                                        0.509 |                           8.645 |                               8.079 |                          0.937 |                                 0.651 |                                    0.980 |
| high_assignment_quality_farms          | North Sea   |           37 |            2215 |                       23434.083 |                                                    0.032 |                                                        0.063 |                           8.645 |                               8.079 |                          0.924 |                                 0.552 |                                    1.000 |
| lower_mmsi_concentration_farms         | Baltic      |           17 |             755 |                        6987.667 |                                                    0.248 |                                                        0.628 |                           6.315 |                               5.725 |                          0.931 |                                 0.305 |                                    0.965 |
| lower_mmsi_concentration_farms         | North Sea   |           99 |            5555 |                       43293.333 |                                                    0.039 |                                                        0.110 |                           6.315 |                               5.725 |                          0.897 |                                 0.318 |                                    1.000 |
| matched_source_coverage_quartiles      | Baltic      |           16 |             705 |                        6987.667 |                                                    0.248 |                                                        0.628 |                           6.315 |                               5.725 |                          0.931 |                                 0.305 |                                    0.965 |
| matched_source_coverage_quartiles      | North Sea   |           92 |            5493 |                       43293.333 |                                                    0.039 |                                                        0.110 |                           6.315 |                               5.725 |                          0.897 |                                 0.318 |                                    1.000 |
| matched_observed_zero_share_quartiles  | Baltic      |           16 |             705 |                        6987.667 |                                                    0.248 |                                                        0.628 |                           2.695 |                               2.357 |                          0.931 |                                 0.305 |                                    0.965 |
| matched_observed_zero_share_quartiles  | North Sea   |           37 |            2414 |                       16625.750 |                                                    0.092 |                                                        0.266 |                           2.695 |                               2.357 |                          0.888 |                                 0.247 |                                    0.900 |
| matched_missing_source_share_quartiles | Baltic      |           16 |             705 |                        6987.667 |                                                    0.248 |                                                        0.628 |                           6.315 |                               5.725 |                          0.931 |                                 0.305 |                                    0.965 |
| matched_missing_source_share_quartiles | North Sea   |           92 |            5493 |                       43293.333 |                                                    0.039 |                                                        0.110 |                           6.315 |                               5.725 |                          0.897 |                                 0.318 |                                    1.000 |
| matched_assignment_quality_quartiles   | Baltic      |           16 |             705 |                        6987.667 |                                                    0.248 |                                                        0.628 |                           5.721 |                               5.187 |                          0.931 |                                 0.305 |                                    0.965 |
| matched_assignment_quality_quartiles   | North Sea   |           74 |            4601 |                       39221.750 |                                                    0.043 |                                                        0.121 |                           5.721 |                               5.187 |                          0.901 |                                 0.318 |                                    1.000 |
| matched_mmsi_concentration_quartiles   | Baltic      |           15 |             701 |                        6927.667 |                                                    0.250 |                                                        0.633 |                           5.409 |                               4.915 |                          0.929 |                                 0.345 |                                    0.965 |
| matched_mmsi_concentration_quartiles   | North Sea   |           64 |            4063 |                       36763.333 |                                                    0.046 |                                                        0.129 |                           5.409 |                               4.915 |                          0.902 |                                 0.367 |                                    1.000 |

Baseline Baltic/North Sea primary AIS-derived intervention-intensity ratio: 6.315.
Baseline high+medium sensitivity ratio: 5.725.

## Interpretation

The Baltic/North Sea gap remains visible under available proxy observability controls, but this is not enough to treat `sea_basin` as a causal operational or reliability signal. The most important missing control is direct AIS receiver/source geometry: receiver station, terrestrial/satellite flag, receiver coordinates, or equivalent detectability metadata.

The proxy controls show that the gap is not erased by source-status coverage, observed-zero-month share, skipped-source share, assignment-quality filtering, or MMSI-concentration filtering. However, these controls are indirect. They cannot prove equivalent detectability of short CTV-style visits, vessel AIS behaviour, or receiver network density across basins.

## CTV/SOV observability bias

CTV/SOV bias is plausible but not proven unless registry/access data supports it. The current dwell table contains vessel enrichment fields, but `vessel_category_enriched`, `access_technology`, and vessel dimensions are effectively unavailable for the event population. This means we can test MMSI concentration and dwell behaviour, but we cannot yet label the access mode safely.

## Thesis-safety assessment

The basin comparison is not thesis-safe as a standalone operational/reliability finding. It is thesis-safe as a measurement and observability finding:

> Baltic-labelled turbines show a stronger AIS-derived intervention-intensity proxy than North Sea-labelled turbines, but the contrast remains entangled with farm/country composition, vessel concentration, assignment geometry, and unresolved AIS observability bias.

## Extra data needed

To make the basin contrast robust, add one or more of:

- AIS receiver station ID or source channel per message or visit,
- terrestrial versus satellite AIS flag,
- receiver coordinates or distance-to-nearest receiver,
- stable distance-to-coast/offshore-distance feature from an accepted local source,
- vessel registry enrichment for top MMSIs, including length, beam, vessel class, access technology, registry source, and confidence,
- SCADA/fault/work-order overlap to separate maintenance intervention evidence from confirmed failure evidence.

## Output files

- `reports/rq9_intervention_intensity/ais_observability_by_farm.csv`
- `reports/rq9_intervention_intensity/basin_observability_control_summary.csv`
- `reports/rq9_intervention_intensity/ais_observability_bias_audit.md`
