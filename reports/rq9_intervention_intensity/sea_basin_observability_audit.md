# RQ9 Sea-Basin Observability Audit

This audit compares Baltic and North Sea steady-operational maintenance intervention intensity using existing RQ9 outputs and AIS dwell evidence. It does not treat AIS visits as confirmed faults, and it does not infer CTV/SOV labels unless the enrichment fields already support that label.

## Basin Denominators And Event Signal

| sea_basin   |   farm_count |   turbine_count |   observed_steady_turbine_years |   high_confidence_steady_tier_a_event_count |   high_confidence_steady_tier_a_duplicate_adjusted_event_count |   high_medium_steady_tier_a_event_count |   high_confidence_intervention_intensity_per_turbine_year |   high_medium_intervention_intensity_per_turbine_year |   zero_event_turbine_share_high_confidence |   high_coverage_zero_event_turbine_share |
|:------------|-------------:|----------------:|--------------------------------:|--------------------------------------------:|---------------------------------------------------------------:|----------------------------------------:|----------------------------------------------------------:|------------------------------------------------------:|-------------------------------------------:|-----------------------------------------:|
| Baltic      |           17 |             755 |                         6987.67 |                                        1733 |                                                        1732.5  |                                    4388 |                                                     0.248 |                                                 0.628 |                                      0.307 |                                    0.262 |
| North Sea   |           99 |            5555 |                        43293.3  |                                        1719 |                                                        1699.67 |                                    4831 |                                                     0.039 |                                                 0.11  |                                      0.848 |                                    0.809 |

The Baltic high-confidence duplicate-adjusted intensity is 6.32x the North Sea value. Under the high+medium sensitivity it is 5.73x the North Sea value.

## Dominance Checks

| sea_basin   | largest_farm_by_high_events   |   largest_farm_high_event_share | top2_farms_by_high_events   |   top2_farms_high_event_share | largest_oem_by_high_events   |   largest_oem_high_event_share | largest_country_by_high_events   |   largest_country_high_event_share |   top1_vessel_by_high_events |   top1_vessel_high_event_share |   top5_vessels_high_event_share |
|:------------|:------------------------------|--------------------------------:|:----------------------------|------------------------------:|:-----------------------------|-------------------------------:|:---------------------------------|-----------------------------------:|-----------------------------:|-------------------------------:|--------------------------------:|
| Baltic      | Lillgrund                     |                           0.155 | Lillgrund; Rodsand_II       |                         0.294 | Siemens                      |                          0.597 | Denmark                          |                              0.557 |                    219016663 |                          0.125 |                           0.385 |
| North Sea   | Horns_Rev_I                   |                           0.141 | Horns_Rev_I; Horns_Rev_II   |                         0.227 | Siemens                      |                          0.513 | Germany                          |                              0.613 |                    218319000 |                          0.048 |                           0.186 |

The Baltic signal is not controlled by only the top five vessels or top two farms, but farm/OEM/country structure remains material.

## Vessel Observability

The table below reports all dwell tiers mapped to each basin, not only assigned turbine events. Vessel category and access technology are raw enrichment fields, not inferred CTV/SOV labels.

| sea_basin   |   unique_mmsi_count_all_dwell |   all_dwell_event_count |   tier_a_all_dwell_event_count |   tier_b_all_dwell_event_count |   tier_c_all_dwell_event_count |   tier_d_all_dwell_event_count |   median_dwell_duration_min_all_tiers |   share_dwell_duration_le_60min |   share_dwell_duration_le_120min |   vessel_length_non_null_share | median_vessel_length_m   |   tier_a_high_assignment_share |   tier_a_high_medium_assignment_share |
|:------------|------------------------------:|------------------------:|-------------------------------:|-------------------------------:|-------------------------------:|-------------------------------:|--------------------------------------:|--------------------------------:|---------------------------------:|-------------------------------:|:-------------------------|-------------------------------:|--------------------------------------:|
| Baltic      |                          4489 |                   45673 |                           7107 |                            869 |                            963 |                          36734 |                                425.9  |                           0.083 |                            0.249 |                              0 |                          |                          0.331 |                                 0.839 |
| North Sea   |                          3126 |                   46973 |                           8157 |                           1430 |                           1381 |                          36005 |                                501.45 |                           0.066 |                            0.174 |                              0 |                          |                          0.276 |                                 0.775 |

### Raw Vessel Category Distribution

Baltic:

| vessel_category_enriched   |   share |
|:---------------------------|--------:|
| missing                    |       1 |

North Sea:

| vessel_category_enriched   |   share |
|:---------------------------|--------:|
| missing                    |       1 |

### Access Technology Distribution

Baltic:

| access_technology   |   share |
|:--------------------|--------:|
| missing             |       1 |

North Sea:

| access_technology   |   share |
|:--------------------|--------:|
| missing             |       1 |

### Dwell Tier Distribution

Baltic:

| dwell_tier   |   share |
|:-------------|--------:|
| Tier D       |   0.804 |
| Tier A       |   0.156 |
| Tier C       |   0.021 |
| Tier B       |   0.019 |

North Sea:

| dwell_tier   |   share |
|:-------------|--------:|
| Tier D       |   0.767 |
| Tier A       |   0.174 |
| Tier B       |   0.03  |
| Tier C       |   0.029 |

## Top Vessels By High-Confidence Steady Tier A Events

| sea_basin   |      mmsi |   high_confidence_steady_event_count |   duplicate_adjusted_high_event_count |   share_of_basin_high_confidence_events |   farm_count | top_farm                  |   median_duration_min |   median_vessel_length_m | vessel_category_enriched   | access_technology   | registry_source   | registry_match_confidence   |
|:------------|----------:|-------------------------------------:|--------------------------------------:|----------------------------------------:|-------------:|:--------------------------|----------------------:|-------------------------:|:---------------------------|:--------------------|:------------------|:----------------------------|
| Baltic      | 219016663 |                                  216 |                                 216   |                                   0.125 |            1 | Lillgrund                 |               464.842 |                      nan |                            |                     |                   |                             |
| Baltic      | 219009338 |                                  163 |                                 163   |                                   0.094 |            1 | Nysted                    |               498.633 |                      nan |                            |                     |                   |                             |
| Baltic      | 219028973 |                                  102 |                                 101.5 |                                   0.059 |            2 | Rodsand_II                |               502.425 |                      nan |                            |                     |                   |                             |
| Baltic      | 219018788 |                                   96 |                                  96   |                                   0.055 |            2 | Rodsand_II                |               540.542 |                      nan |                            |                     |                   |                             |
| Baltic      | 219016747 |                                   90 |                                  90   |                                   0.052 |            2 | Kriegers_Flak             |               439.65  |                      nan |                            |                     |                   |                             |
| Baltic      | 219023467 |                                   85 |                                  85   |                                   0.049 |            1 | Samsa                     |               238.667 |                      nan |                            |                     |                   |                             |
| Baltic      | 219001771 |                                   78 |                                  78   |                                   0.045 |            1 | Anholt                    |               495.983 |                      nan |                            |                     |                   |                             |
| Baltic      | 219017204 |                                   65 |                                  65   |                                   0.038 |            2 | Kriegers_Flak             |               393.733 |                      nan |                            |                     |                   |                             |
| Baltic      | 211810060 |                                   54 |                                  54   |                                   0.031 |            3 | EnBW_Windpark_Baltic_2    |               504.017 |                      nan |                            |                     |                   |                             |
| Baltic      | 219016873 |                                   51 |                                  51   |                                   0.029 |            1 | Anholt                    |               526.483 |                      nan |                            |                     |                   |                             |
| North Sea   | 218319000 |                                   83 |                                  83   |                                   0.048 |            4 | Trianel_Windpark_Borkum_2 |               464.983 |                      nan |                            |                     |                   |                             |
| North Sea   | 219014012 |                                   73 |                                  73   |                                   0.042 |            1 | Horns_Rev_II              |               495.017 |                      nan |                            |                     |                   |                             |
| North Sea   | 219032199 |                                   64 |                                  64   |                                   0.037 |            4 | Gode_Wind_1_and_2         |               472.508 |                      nan |                            |                     |                   |                             |
| North Sea   | 244834000 |                                   57 |                                  57   |                                   0.033 |            1 | Horns_Rev_I               |               454.683 |                      nan |                            |                     |                   |                             |
| North Sea   | 219019936 |                                   43 |                                  43   |                                   0.025 |            1 | Horns_Rev_II              |               472.4   |                      nan |                            |                     |                   |                             |
| North Sea   | 219020687 |                                   37 |                                  36   |                                   0.022 |            3 | Borkum_Riffgrund_1        |               258.117 |                      nan |                            |                     |                   |                             |
| North Sea   | 219032609 |                                   35 |                                  35   |                                   0.02  |            1 | Butendiek                 |               420.7   |                      nan |                            |                     |                   |                             |
| North Sea   | 232040897 |                                   35 |                                  35   |                                   0.02  |            2 | Dan_Tysk                  |               395.917 |                      nan |                            |                     |                   |                             |
| North Sea   | 235108046 |                                   35 |                                  35   |                                   0.02  |            1 | Horns_Rev_I               |               477.55  |                      nan |                            |                     |                   |                             |
| North Sea   | 244103000 |                                   32 |                                  32   |                                   0.019 |            1 | Horns_Rev_I               |               482.033 |                      nan |                            |                     |                   |                             |

## Characteristic Context

Sea-basin differences are entangled with turbine age, capacity, OEM, and country/farm composition. Existing turbine-characteristics outputs show these exploratory group contrasts for turbines with at least three steady observed years:

Capacity bands:

| characteristic_value   |   turbine_count |   observed_steady_turbine_years |   primary_duplicate_adjusted_intervention_intensity_per_turbine_year |   zero_event_turbine_share | interpretation_flag                      |
|:-----------------------|----------------:|--------------------------------:|---------------------------------------------------------------------:|---------------------------:|:-----------------------------------------|
| lt_3_mw                |             599 |                         8660.25 |                                                                0.151 |                      0.419 | robust_enough_for_exploratory_comparison |
| 5_to_7_9_mw            |            1602 |                        11668.7  |                                                                0.065 |                      0.787 | robust_enough_for_exploratory_comparison |
| 3_to_4_9_mw            |            2533 |                        26276.5  |                                                                0.038 |                      0.799 | robust_enough_for_exploratory_comparison |
| 8_to_9_9_mw            |             520 |                         2448.17 |                                                                0.031 |                      0.898 | robust_enough_for_exploratory_comparison |

Operational age bands:

| characteristic_value   |   turbine_count |   observed_steady_turbine_years |   primary_duplicate_adjusted_intervention_intensity_per_turbine_year |   zero_event_turbine_share | interpretation_flag                      |
|:-----------------------|----------------:|--------------------------------:|---------------------------------------------------------------------:|---------------------------:|:-----------------------------------------|
| 15_plus_years          |            1125 |                         16386.1 |                                                                0.083 |                      0.628 | robust_enough_for_exploratory_comparison |
| 3_to_7_years           |            1435 |                          7222.5 |                                                                0.063 |                      0.856 | robust_enough_for_exploratory_comparison |
| 8_to_14_years          |            2694 |                         25445   |                                                                0.052 |                      0.767 | robust_enough_for_exploratory_comparison |

## Bias Assessment

- Baltic vs North Sea is not thesis-safe as a causal basin comparison yet. The numerator gap is real in the current assigned-event table, but it is not yet separated from fleet composition, country/farm effects, vessel observability, and assignment geometry.
- North Sea undercounting is plausible: many well-covered turbines have zero high-confidence assigned signal and Tier A assignment is lower than the Baltic.
- CTV/SOV observability bias is plausible but not proven. The audit can report registry-backed vessel category/access fields where present, but it should not relabel unknown vessels as CTV/SOV.
- The high+medium sensitivity materially changes the apparent intensity in both basins. This reinforces keeping high-confidence <=200 m Tier A as the primary metric and <=500 m as sensitivity only.

## Recommendation

Use sea basin as a stratification and bias-audit variable for RQ9, not yet as a headline explanatory variable. The next defensible increment is a confounding-aware basin comparison within country/farm/OEM/capacity/age strata, with explicit vessel-observability controls and SCADA/work-order validation where available.
