# RQ9 Controlled Baltic vs North Sea Comparison

This audit asks whether the Baltic/North Sea maintenance intervention-intensity gap remains after controlling for OEM, capacity, operational age, country/farm dominance, and vessel observability. The comparison uses existing RQ9 turbine outputs only. It does not treat AIS evidence as confirmed faults.

## Answer In Brief

The Baltic/North Sea gap generally survives simple one-dimensional controls, but the matched-strata evidence thins quickly under combined controls. The fully combined controls do not yet provide enough non-farm-dominated matched strata for a robust all-controls answer.

The safest interpretation is that `sea_basin` is still a useful geographic stratification and bias-audit label, but not yet a standalone explanatory variable for simulator demand.

## Matched Controls: All Shared Strata

These rows pool only strata where both Baltic and North Sea turbines exist.

| control                  |   matched_strata |   usable_strata |   farm_dominated_strata |   baltic_years |   north_sea_years |   baltic_primary_intensity |   north_sea_primary_intensity |   primary_baltic_to_north_sea_ratio |   sensitivity_baltic_to_north_sea_ratio |
|:-------------------------|-----------------:|----------------:|------------------------:|---------------:|------------------:|---------------------------:|------------------------------:|------------------------------------:|----------------------------------------:|
| OEM                      |                4 |               1 |                       3 |        5442.67 |           39967   |                      0.267 |                         0.035 |                               7.612 |                                   7.093 |
| capacity_band            |                4 |               0 |                       4 |        6987.67 |           43152.3 |                      0.248 |                         0.039 |                               6.302 |                                   5.71  |
| age_band                 |                4 |               3 |                       1 |        6987.67 |           43293.3 |                      0.248 |                         0.039 |                               6.315 |                                   5.725 |
| country                  |                2 |               1 |                       1 |        6267.67 |           13062.9 |                      0.233 |                         0.116 |                               2.005 |                                   1.923 |
| OEM_capacity             |                8 |               0 |                       8 |        5442.67 |           36385.2 |                      0.267 |                         0.038 |                               7.061 |                                   6.596 |
| OEM_age                  |                6 |               1 |                       5 |        5034.33 |           32169.6 |                      0.257 |                         0.037 |                               6.936 |                                   6.079 |
| capacity_age             |                7 |               0 |                       7 |        6987.67 |           34924   |                      0.248 |                         0.038 |                               6.569 |                                   5.749 |
| OEM_capacity_age         |                7 |               0 |                       7 |        5016.33 |           25601.7 |                      0.253 |                         0.039 |                               6.495 |                                   5.438 |
| country_OEM_capacity_age |                3 |               0 |                       3 |        2102.5  |            7098   |                      0.212 |                         0.127 |                               1.67  |                                   1.627 |

## Matched Controls: Excluding Farm-Dominated Or Low-Exposure Strata

A stratum is excluded here if either side is dominated by one farm, has too few farms, lacks 100 turbine-years on either side, or has too little event signal.

| control                  |   matched_strata | usable_strata   | farm_dominated_strata   |   baltic_years |   north_sea_years | baltic_primary_intensity   | north_sea_primary_intensity   | primary_baltic_to_north_sea_ratio   | sensitivity_baltic_to_north_sea_ratio   |
|:-------------------------|-----------------:|:----------------|:------------------------|---------------:|------------------:|:---------------------------|:------------------------------|:------------------------------------|:----------------------------------------|
| OEM                      |                1 | 1.000           | 0.000                   |        4524.25 |           21538.2 | 0.228                      | 0.040                         | 5.670                               | 4.952                                   |
| capacity_band            |                0 |                 |                         |           0    |               0   |                            |                               |                                     |                                         |
| age_band                 |                3 | 3.000           | 0.000                   |        6969.67 |           43081.4 | 0.245                      | 0.038                         | 6.447                               | 5.843                                   |
| country                  |                1 | 1.000           | 0.000                   |        1737.83 |           10135.7 | 0.287                      | 0.103                         | 2.800                               | 3.735                                   |
| OEM_capacity             |                0 |                 |                         |           0    |               0   |                            |                               |                                     |                                         |
| OEM_age                  |                1 | 1.000           | 0.000                   |        2216.75 |           16986.9 | 0.194                      | 0.039                         | 4.923                               | 5.377                                   |
| capacity_age             |                0 |                 |                         |           0    |               0   |                            |                               |                                     |                                         |
| OEM_capacity_age         |                0 |                 |                         |           0    |               0   |                            |                               |                                     |                                         |
| country_OEM_capacity_age |                0 |                 |                         |           0    |               0   |                            |                               |                                     |                                         |

## Strongest Usable Controlled Contrasts

| control_name   | control_value           |   baltic_turbines |   north_sea_turbines |   baltic_years |   north_sea_years |   baltic_primary_intensity |   north_sea_primary_intensity |   primary_baltic_to_north_sea_ratio |   baltic_dominant_farm_event_share |   north_sea_dominant_farm_event_share |
|:---------------|:------------------------|------------------:|---------------------:|---------------:|------------------:|---------------------------:|------------------------------:|------------------------------------:|-----------------------------------:|--------------------------------------:|
| age_band       | 3_to_7_years            |               202 |                 1668 |        953.333 |           7266.67 |                      0.466 |                         0.029 |                              15.815 |                              0.448 |                                 0.301 |
| OEM            | Siemens                 |               414 |                 2194 |       4524.25  |          21538.2  |                      0.228 |                         0.04  |                               5.67  |                              0.26  |                                 0.172 |
| age_band       | 15_plus_years           |               261 |                  865 |       3799.58  |          12586.5  |                      0.22  |                         0.042 |                               5.242 |                              0.322 |                                 0.459 |
| age_band       | 8_to_14_years           |               215 |                 2483 |       2216.75  |          23228.2  |                      0.194 |                         0.039 |                               5.018 |                              0.464 |                                 0.131 |
| OEM_age        | Siemens | 8_to_14_years |               215 |                 1798 |       2216.75  |          16986.9  |                      0.194 |                         0.039 |                               4.923 |                              0.464 |                                 0.175 |
| country        | Germany                 |               308 |                 1329 |       1737.83  |          10135.7  |                      0.287 |                         0.103 |                               2.8   |                              0.331 |                                 0.113 |

## Weakest Or Reversed Usable Controlled Contrasts

| control_name   | control_value           |   baltic_turbines |   north_sea_turbines |   baltic_years |   north_sea_years |   baltic_primary_intensity |   north_sea_primary_intensity |   primary_baltic_to_north_sea_ratio |   baltic_dominant_farm_event_share |   north_sea_dominant_farm_event_share |
|:---------------|:------------------------|------------------:|---------------------:|---------------:|------------------:|---------------------------:|------------------------------:|------------------------------------:|-----------------------------------:|--------------------------------------:|
| country        | Germany                 |               308 |                 1329 |       1737.83  |          10135.7  |                      0.287 |                         0.103 |                               2.8   |                              0.331 |                                 0.113 |
| OEM_age        | Siemens | 8_to_14_years |               215 |                 1798 |       2216.75  |          16986.9  |                      0.194 |                         0.039 |                               4.923 |                              0.464 |                                 0.175 |
| age_band       | 8_to_14_years           |               215 |                 2483 |       2216.75  |          23228.2  |                      0.194 |                         0.039 |                               5.018 |                              0.464 |                                 0.131 |
| age_band       | 15_plus_years           |               261 |                  865 |       3799.58  |          12586.5  |                      0.22  |                         0.042 |                               5.242 |                              0.322 |                                 0.459 |
| OEM            | Siemens                 |               414 |                 2194 |       4524.25  |          21538.2  |                      0.228 |                         0.04  |                               5.67  |                              0.26  |                                 0.172 |
| age_band       | 3_to_7_years            |               202 |                 1668 |        953.333 |           7266.67 |                      0.466 |                         0.029 |                              15.815 |                              0.448 |                                 0.301 |

## Farm-Dominance Trim

This sensitivity removes farms contributing at least 10% of high-confidence adjusted events within either basin.

| comparison_scope                                        | sea_basin   | excluded_farms                                                    |   remaining_turbines |   remaining_farms |   observed_steady_turbine_years |   high_confidence_adjusted_events |   high_medium_adjusted_events |   primary_intensity |   sensitivity_intensity |   zero_event_turbine_share |
|:--------------------------------------------------------|:------------|:------------------------------------------------------------------|---------------------:|------------------:|--------------------------------:|----------------------------------:|------------------------------:|--------------------:|------------------------:|---------------------------:|
| overall_excluding_farms_with_ge_10pct_basin_high_events | Baltic      | Anholt; Horns_Rev_I; Kriegers_Flak; Lillgrund; Nysted; Rodsand_II |                  524 |                14 |                         3985.17 |                           1078.5  |                       3119.5  |               0.271 |                   0.783 |                      0.361 |
| overall_excluding_farms_with_ge_10pct_basin_high_events | North Sea   | Anholt; Horns_Rev_I; Kriegers_Flak; Lillgrund; Nysted; Rodsand_II |                 5555 |                99 |                        43293.3  |                           1699.67 |                       4745.33 |               0.039 |                   0.11  |                      0.848 |

## Vessel Observability Controls

Registry enrichment is not informative in the current dwell table because vessel category, access technology, registry source, and vessel length are effectively missing. The observability check therefore uses MMSI concentration, dwell duration, and Tier A assignment rates.

| sea_basin   |   all_dwell_events |   all_dwell_unique_mmsi |   all_dwell_events_per_mmsi_median |   all_dwell_top5_mmsi_share |   high_confidence_events |   high_confidence_unique_mmsi |   high_confidence_events_per_mmsi_median |   high_confidence_top5_mmsi_share |   all_dwell_duration_median_min |   all_dwell_duration_p25_min |   all_dwell_duration_p75_min |   all_dwell_share_le_60min |   all_dwell_share_le_120min |   high_confidence_duration_median_min |   tier_a_assignment_high_share |   tier_a_assignment_high_medium_share |   vessel_category_enriched_non_null_share |   access_technology_non_null_share |   registry_source_non_null_share |   registry_match_confidence_non_null_share |   vessel_length_m_non_null_share |
|:------------|-------------------:|------------------------:|-----------------------------------:|----------------------------:|-------------------------:|------------------------------:|-----------------------------------------:|----------------------------------:|--------------------------------:|-----------------------------:|-----------------------------:|---------------------------:|----------------------------:|--------------------------------------:|-------------------------------:|--------------------------------------:|------------------------------------------:|-----------------------------------:|---------------------------------:|-------------------------------------------:|---------------------------------:|
| Baltic      |              45673 |                    4489 |                                  2 |                       0.1   |                     1733 |                           129 |                                        2 |                             0.385 |                          425.9  |                      120.483 |                      1236.83 |                      0.083 |                       0.249 |                               459.033 |                          0.331 |                                 0.839 |                                         0 |                                  0 |                                0 |                                          0 |                                0 |
| North Sea   |              46973 |                    3126 |                                  3 |                       0.084 |                     1719 |                           286 |                                        2 |                             0.186 |                          501.45 |                      183.25  |                      1437.13 |                      0.066 |                       0.174 |                               410.517 |                          0.276 |                                 0.775 |                                         0 |                                  0 |                                0 |                                          0 |                                0 |

## Interpretation

- The basin gap survives simple controls by OEM, capacity band, and age band in several matched strata.
- The evidence becomes much less robust when controls are combined, especially with country and farm-dominance constraints.
- Baltic high-confidence signal has higher MMSI concentration and a higher Tier A assignment share than North Sea, so AIS/vessel-observability bias remains plausible.
- North Sea has a much higher zero-event share and lower high-confidence assignment rate, which could reflect real operational differences, under-capture of short transfer activity, or both.
- The comparison is not ready to claim a basin-driven maintenance demand difference. It is ready to motivate a controlled, bias-aware RQ9 subsection.

## Recommendation

Treat the current result as: the Baltic signal remains visible under simple controls, but a thesis-safe basin conclusion requires either richer vessel enrichment or validation against SCADA, fault logs, work orders, or equivalent maintenance records. The next useful increment is a sparse matched-strata table for OEM-capacity-age-country combinations with explicit sample-size thresholds.
