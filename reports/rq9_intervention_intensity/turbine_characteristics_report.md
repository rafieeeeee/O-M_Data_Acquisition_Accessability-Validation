# RQ9 Turbine Characteristics Comparison v1

This report compares steady-operational maintenance intervention intensity by turbine
characteristics. It uses existing turbine-assigned Tier A evidence and turbine-year
denominators only. It is not confirmed fault evidence.

## Summary

| metric                               |   value |
|:-------------------------------------|--------:|
| turbines                             |  6523   |
| observed steady turbine-years        | 50454.2 |
| high-confidence steady Tier A events |  3452   |
| high+medium steady Tier A events     |  9219   |
| robust exploratory rows              |   266   |

## Interpretation Flags

| interpretation_flag                      |   rows |
|:-----------------------------------------|-------:|
| robust_enough_for_exploratory_comparison |    266 |
| insufficient_event_signal                |    229 |
| insufficient_exposure                    |    109 |
| single_farm_dominated                    |     98 |
| short_operational_age                    |     32 |
| single_oem_farm_confounded               |      6 |

Robust enough for exploratory thesis discussion: commissioning_year, country, farm, hub_height_band, oem_manufacturer, operational_age_band, rated_capacity_band, rotor_diameter_band, sea_basin, turbine_model.

## Top OEM Groups

| characteristic_value   |   turbine_count |   observed_steady_turbine_years |   high_confidence_duplicate_adjusted_event_count |   primary_duplicate_adjusted_intervention_intensity_per_turbine_year |   zero_event_turbine_share | interpretation_flag                      |
|:-----------------------|----------------:|--------------------------------:|-------------------------------------------------:|---------------------------------------------------------------------:|---------------------------:|:-----------------------------------------|
| Bonus                  |             102 |                         1530    |                                           278    |                                                            0.181699  |                   0.303922 | single_farm_dominated                    |
| Adwen                  |             196 |                         1553.83 |                                           223.5  |                                                            0.143838  |                   0.561224 | robust_enough_for_exploratory_comparison |
| GE Energy              |              66 |                          341    |                                            46    |                                                            0.134897  |                   0.5      | single_farm_dominated                    |
| Senvion                |             242 |                         2118.67 |                                           213.5  |                                                            0.100771  |                   0.607438 | robust_enough_for_exploratory_comparison |
| Siemens                |            2604 |                        26062.4  |                                          1901.17 |                                                            0.0729467 |                   0.715054 | robust_enough_for_exploratory_comparison |

## Top Model Groups

| characteristic_value   |   turbine_count |   observed_steady_turbine_years |   high_confidence_duplicate_adjusted_event_count |   primary_duplicate_adjusted_intervention_intensity_per_turbine_year |   zero_event_turbine_share | interpretation_flag                      |
|:-----------------------|----------------:|--------------------------------:|-------------------------------------------------:|---------------------------------------------------------------------:|---------------------------:|:-----------------------------------------|
| 6.2M152                |              32 |                         130.667 |                                             64.5 |                                                             0.493622 |                   0.40625  | single_farm_dominated                    |
| AD 5-135               |              70 |                         408.333 |                                            161   |                                                             0.394286 |                   0.342857 | single_farm_dominated                    |
| B82/2300               |              82 |                        1230     |                                            271   |                                                             0.220325 |                   0.219512 | single_farm_dominated                    |
| SWT-2.3-93             |             282 |                        3905.25  |                                            733.5 |                                                             0.187824 |                   0.329787 | robust_enough_for_exploratory_comparison |
| Haliade 150-6MW        |              66 |                         341     |                                             46   |                                                             0.134897 |                   0.5      | single_farm_dominated                    |

## Top Capacity Bands

| characteristic_value   |   turbine_count |   observed_steady_turbine_years |   high_confidence_duplicate_adjusted_event_count |   primary_duplicate_adjusted_intervention_intensity_per_turbine_year |   zero_event_turbine_share | interpretation_flag                      |
|:-----------------------|----------------:|--------------------------------:|-------------------------------------------------:|---------------------------------------------------------------------:|---------------------------:|:-----------------------------------------|
| lt_3_mw                |             599 |                         8660.25 |                                         1309.5   |                                                            0.151208  |                   0.419032 | robust_enough_for_exploratory_comparison |
| 5_to_7_9_mw            |            1602 |                        11668.7  |                                          759     |                                                            0.065046  |                   0.786517 | robust_enough_for_exploratory_comparison |
| 3_to_4_9_mw            |            2533 |                        26276.5  |                                          997.667 |                                                            0.037968  |                   0.798658 | robust_enough_for_exploratory_comparison |
| 8_to_9_9_mw            |             520 |                         2448.17 |                                           76.5   |                                                            0.0312479 |                   0.898077 | robust_enough_for_exploratory_comparison |

## Top Operational Age Bands

| characteristic_value   |   turbine_count |   observed_steady_turbine_years |   high_confidence_duplicate_adjusted_event_count |   primary_duplicate_adjusted_intervention_intensity_per_turbine_year |   zero_event_turbine_share | interpretation_flag                      |
|:-----------------------|----------------:|--------------------------------:|-------------------------------------------------:|---------------------------------------------------------------------:|---------------------------:|:-----------------------------------------|
| 15_plus_years          |            1125 |                         16386.1 |                                          1361.83 |                                                            0.0831091 |                   0.628444 | robust_enough_for_exploratory_comparison |
| 3_to_7_years           |            1435 |                          7222.5 |                                           456    |                                                            0.063136  |                   0.855749 | robust_enough_for_exploratory_comparison |
| 8_to_14_years          |            2694 |                         25445   |                                          1324.83 |                                                            0.0520665 |                   0.766889 | robust_enough_for_exploratory_comparison |

## Sea Basin Comparison

| characteristic_value   |   turbine_count |   observed_steady_turbine_years |   high_confidence_duplicate_adjusted_event_count |   primary_duplicate_adjusted_intervention_intensity_per_turbine_year |   zero_event_turbine_share | interpretation_flag                      |
|:-----------------------|----------------:|--------------------------------:|-------------------------------------------------:|---------------------------------------------------------------------:|---------------------------:|:-----------------------------------------|
| Baltic                 |             606 |                         6759.67 |                                          1508.5  |                                                            0.223162  |                   0.262376 | robust_enough_for_exploratory_comparison |
| North Sea              |            4648 |                        42293.9  |                                          1634.17 |                                                            0.0386383 |                   0.826592 | robust_enough_for_exploratory_comparison |

## Guardrails

- High-confidence Tier A within 200 m is the primary evidence scope; high+medium within 500 m is sensitivity only.
- Rows marked single-farm dominated or single-OEM/farm confounded should not be interpreted as causal turbine-characteristic effects.
- Short operational age groups can reflect commissioning-era observation windows even after ramp-up exclusion.
- The output is maintenance intervention intensity, not confirmed fault evidence.
