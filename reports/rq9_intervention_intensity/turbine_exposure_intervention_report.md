# RQ9 Turbine Exposure and Denominator v1

This report covers turbine-level maintenance intervention intensity using existing RQ9
Tier A turbine-assigned events and existing source-coverage manifests. It is candidate
maintenance evidence for simulator scoping, not confirmed fault evidence.

## Denominator Summary

| metric                               |     value |
|:-------------------------------------|----------:|
| turbines                             |  6523     |
| farms                                |   119     |
| observed steady turbine-years        | 50454.2   |
| median observed steady turbine-years |     7.917 |
| processed bathymetry matches         |  6523     |

## Assignment Summary

- Primary high-confidence steady Tier A events: 3452
- Sensitivity high+medium steady Tier A events: 9219
- Primary duplicate-adjusted count: 3432.167
- Sensitivity duplicate-adjusted count: 9130.333

## Coverage Classes

| coverage_class              |   rows |
|:----------------------------|-------:|
| high_coverage               |   4576 |
| medium_coverage             |   1228 |
| low_coverage                |    384 |
| no_observed_steady_coverage |    223 |
| no_steady_manifest_window   |    112 |

## Exposure Comparison

Primary result: 0.515x outer vs inner/middle.
Sensitivity result: 0.369x outer vs inner/middle.

| assignment_scope     | comparison_group      |   turbine_count |   eligible_turbine_count_ge_1yr |   observed_steady_years |   event_count |   duplicate_adjusted_event_count |   unique_vessel_count_sum |   repeat_visit_30d_count |   intervention_intensity_per_steady_turbine_year |   median_turbine_intervention_intensity | comparison_confidence_class   |
|:---------------------|:----------------------|----------------:|--------------------------------:|------------------------:|--------------:|---------------------------------:|--------------------------:|-------------------------:|-------------------------------------------------:|----------------------------------------:|:------------------------------|
| high_confidence_200m | outer_exposed_proxy   |            1658 |                            1476 |                 12831.2 |           516 |                           513    |                       372 |                      132 |                                        0.0399805 |                                       0 | high_sample_observed_signal   |
| high_confidence_200m | inner_or_middle_proxy |            4854 |                            4325 |                 37585.2 |          2935 |                          2918.17 |                      1876 |                      729 |                                        0.0776413 |                                       0 | high_sample_observed_signal   |
| high_medium_500m     | outer_exposed_proxy   |            1658 |                            1476 |                 12831.2 |          1033 |                          1021.5  |                       723 |                      273 |                                        0.0796103 |                                       0 | high_sample_observed_signal   |
| high_medium_500m     | inner_or_middle_proxy |            4854 |                            4325 |                 37585.2 |          8185 |                          8107.83 |                      4131 |                     3349 |                                        0.215718  |                                       0 | high_sample_observed_signal   |

## Sea Basin Distribution

| sea_basin   |   turbines |
|:------------|-----------:|
| North Sea   |       5555 |
| Baltic      |        755 |
| other       |        213 |

## Metadata Completeness

| field             |   complete_rows |   total_rows |
|:------------------|----------------:|-------------:|
| sea_basin         |            6523 |         6523 |
| oem_manufacturer  |            6523 |         6523 |
| rated_capacity_mw |            6523 |         6523 |
| water_depth_m     |            6523 |         6523 |

## Sea Basin Mapping Rules

| rule_id                  | country        | longitude_rule    | sea_basin   |
|:-------------------------|:---------------|:------------------|:------------|
| united_kingdom_north_sea | United Kingdom | any               | North Sea   |
| netherlands_north_sea    | Netherlands    | any               | North Sea   |
| belgium_north_sea        | Belgium        | any               | North Sea   |
| norway_north_sea         | Norway         | any               | North Sea   |
| germany_baltic           | Germany        | longitude >= 10.0 | Baltic      |
| germany_north_sea        | Germany        | longitude < 10.0  | North Sea   |
| denmark_baltic           | Denmark        | longitude >= 10.0 | Baltic      |
| denmark_north_sea        | Denmark        | longitude < 10.0  | North Sea   |
| sweden_baltic            | Sweden         | any               | Baltic      |
| france_other             | France         | any               | other       |
| missing_country_unknown  | __missing__    | any               | unknown     |
| unmapped_country_unknown | __unmapped__   | any               | unknown     |

## Red Flags and Guardrails

- Outer exposure is a farm-layout radial proxy, not a directional wind/wave exposure model.
- Tier A assignments outside 200 m are sensitivity evidence, not the primary turbine signal.
- Pre-operational and commissioning/ramp-up months and events are excluded from primary steady-operational intensity.
- Sea-basin mapping is explicit and coarse; review farm-level basin labels before publication.
- Simulator multipliers should remain provisional until linked to SCADA, work orders, or equivalent maintenance records.
