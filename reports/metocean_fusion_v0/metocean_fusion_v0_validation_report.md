# Metocean Fusion v0 Validation Report

Status: completed Fusion v0 research increment. Existing accepted source archives were read, not mutated. No current download, FINO import, NORA3 rerun, source archive interpolation change, or final production dwell-metocean rebuild was run.

## A. Research Question And Experiment Design

**Hypothesis:** replacing the current NORA3-only active weather fields with source-resolved regional waves plus static site depth will improve Hs/Tp event coverage and may alter the observed Tier A Hs/Tp operating envelope.

**Source priority rules:** Baltic wave if the event farm/year and timestamp are covered; else NWS wave if covered; else existing NORA3 active fields; else mark wave missing with a reason. Source-specific comparator columns are preserved where overlaps exist.

**Metrics:** row preservation, duplicate dwell IDs, Hs/Tp coverage gain, source distribution, farm/year/tier coverage, source-overlap bias/RMSE, bathymetry completeness, Tier A percentile-boundary change, and static Hs threshold counts.

**Acceptance gates:** row count equals input; no rows dropped for missing weather; required columns present; no duplicate dwell IDs beyond input state; physical checks pass; bathymetry provenance is populated where depth exists.

**Caveats:** Fusion v0 is still an observed successful-dwell envelope, not calibrated `P(operation | weather)`. Currents and FINO validation are not included. Regional source assignment uses the nearest accepted common sample point to each dwell centroid and preserves the assignment method.

## B. Implementation Outputs

- input_rows: `92660`
- output_rows: `92660`
- row_count_preserved: `True`
- duplicate_dwell_id_count: `0`
- output_table: `/Volumes/Extreme SSD/01_ACTIVE_PROJECTS/O&M_Data_Acquisition_Accessability&Validation/Data/Processed/metocean/fusion_v0/dwell_metocean_fusion_v0.parquet`

## C. Coverage Comparison

- nora3_hs_tp_rows: `44377`
- nora3_hs_tp_rate: `0.4789`
- fusion_hs_tp_rows: `83901`
- fusion_hs_tp_rate: `0.9055`
- absolute_gain_rows: `39524`
- percentage_gain_vs_nora3: `89.06%`
- tier_a_nora3_hs_tp_rows: `13574`
- tier_a_fusion_hs_tp_rows: `14606`

## Source Distribution

| fusion_wave_source   |   rows |      rate |
|:---------------------|-------:|----------:|
| nws                  |  36557 | 0.394528  |
| baltic               |  32896 | 0.355018  |
| nora3                |  14448 | 0.155925  |
| missing              |   8759 | 0.0945284 |

## Bathymetry Coverage

- non_null_depth_rows: `92660`
- missing_depth_rows: `0`
- shallow_depth_le_1m_rows: `24802`
- zero_depth_rows: `11017`
- assignment_method_counts: `{'nearest_common_sample_to_dwell_centroid': 92660}`

### Shallow / Zero-Depth Event Rows

These rows preserve valid EMODnet point-sample provenance but should be reviewed before depth is used as a hard modelling covariate.

| farm_id                | bathymetry_sample_point_id   |   rows |   min_depth_m |   median_depth_m |   max_depth_m |
|:-----------------------|:-----------------------------|-------:|--------------:|-----------------:|--------------:|
| Renland                | turbine_0006                 |  11107 |   0.000965147 |      0.000965147 |   0.000965147 |
| Frederikshavn Offshore | turbine_0001                 |   4838 |   0           |      0           |   0           |
| Avedøre Holme          | turbine_0000                 |   2552 |   0.740437    |      0.740437    |   0.740437    |
| Frederikshavn Offshore | turbine_0002                 |   1840 |   0           |      0           |   0           |
| Frederikshavn Offshore | turbine_0003                 |   1720 |   0           |      0           |   0           |
| Frederikshavn Offshore | turbine_0000                 |   1548 |   0           |      0           |   0           |
| Frederikshavn Offshore | farm_centroid                |   1071 |   0           |      0           |   0           |
| Renland                | turbine_0000                 |     88 |   0.549303    |      0.549303    |   0.549303    |
| Renland                | turbine_0001                 |     13 |   0.0275949   |      0.0275949   |   0.0275949   |
| Renland                | turbine_0004                 |      5 |   0.160484    |      0.160484    |   0.160484    |
| Renland                | turbine_0005                 |      5 |   0.531234    |      0.531234    |   0.531234    |
| Renland                | farm_centroid                |      4 |   0.0827943   |      0.0827943   |   0.0827943   |
| Renland                | turbine_0003                 |      4 |   0.205823    |      0.205823    |   0.205823    |
| London Array           | turbine_0027                 |      3 |   0.89        |      0.89        |   0.89        |
| Renland                | turbine_0002                 |      2 |   0.00058537  |      0.00058537  |   0.00058537  |
| Gunfleet Sands         | turbine_0026                 |      1 |   0.49        |      0.49        |   0.49        |
| London Array           | turbine_0118                 |      1 |   0.5         |      0.5         |   0.5         |

## Physical Checks

- fusion_hs_non_negative: `True`
- fusion_tp_positive_where_non_null: `True`
- fusion_direction_pair_count: `83901`
- fusion_direction_sincos_magnitude_le_1: `True`
- water_depth_positive_down_non_negative: `True`

## Source-Overlap Comparison

| comparison         |   overlap_rows |    hs_bias |   hs_median_abs_diff |   hs_rmse |   tp_bias |   tp_median_abs_diff |   tp_rmse |
|:-------------------|---------------:|-----------:|---------------------:|----------:|----------:|---------------------:|----------:|
| nora3_minus_nws    |          27924 |  0.0122852 |            0.0484783 | 0.103881  | 0.0661213 |             0.270517 |  1.28518  |
| nora3_minus_baltic |          15493 | -0.0373611 |            0.0558444 | 0.100023  | 0.365696  |             0.351715 |  0.751549 |
| nws_minus_baltic   |          30735 | -0.0434272 |            0.0598563 | 0.0873922 | 0.465874  |             0.300441 |  1.12369  |

## Tier A Hs/Tp Envelope Change

- nora3_boundary_bins: `26`
- fusion_boundary_bins: `30`
- common_boundary_bins: `26`
- mean_abs_p95_change_m: `0.1349012773872363`
- max_abs_p95_change_m: `0.5582204068441304`

## Static Hs Threshold Comparison

| subset   |   hs_threshold_m |   nora3_valid_rows |   nora3_rows_below_threshold |   fusion_valid_rows |   fusion_rows_below_threshold |
|:---------|-----------------:|-------------------:|-----------------------------:|--------------------:|------------------------------:|
| all_rows |              1   |              44377 |                        37954 |               83901 |                         75377 |
| tier_a   |              1   |              13574 |                        11646 |               14606 |                         12099 |
| all_rows |              1.5 |              44377 |                        42912 |               83901 |                         82099 |
| tier_a   |              1.5 |              13574 |                        13259 |               14606 |                         14215 |
| all_rows |              2   |              44377 |                        44003 |               83901 |                         83444 |
| tier_a   |              2   |              13574 |                        13520 |               14606 |                         14539 |
| all_rows |              2.5 |              44377 |                        44290 |               83901 |                         83821 |
| tier_a   |              2.5 |              13574 |                        13565 |               14606 |                         14596 |

## Detailed Validation Tables

- `coverage_by_farm`: `/Volumes/Extreme SSD/01_ACTIVE_PROJECTS/O&M_Data_Acquisition_Accessability&Validation/reports/metocean_fusion_v0/coverage_by_farm.csv`
- `coverage_by_year`: `/Volumes/Extreme SSD/01_ACTIVE_PROJECTS/O&M_Data_Acquisition_Accessability&Validation/reports/metocean_fusion_v0/coverage_by_year.csv`
- `coverage_by_tier`: `/Volumes/Extreme SSD/01_ACTIVE_PROJECTS/O&M_Data_Acquisition_Accessability&Validation/reports/metocean_fusion_v0/coverage_by_tier.csv`
- `bathymetry_shallow_by_farm`: `/Volumes/Extreme SSD/01_ACTIVE_PROJECTS/O&M_Data_Acquisition_Accessability&Validation/reports/metocean_fusion_v0/bathymetry_shallow_by_farm.csv`
- `source_distribution`: `/Volumes/Extreme SSD/01_ACTIVE_PROJECTS/O&M_Data_Acquisition_Accessability&Validation/reports/metocean_fusion_v0/source_distribution.csv`
- `source_overlap_comparison`: `/Volumes/Extreme SSD/01_ACTIVE_PROJECTS/O&M_Data_Acquisition_Accessability&Validation/reports/metocean_fusion_v0/source_overlap_comparison.csv`
- `tier_a_tp_boundary_comparison`: `/Volumes/Extreme SSD/01_ACTIVE_PROJECTS/O&M_Data_Acquisition_Accessability&Validation/reports/metocean_fusion_v0/tier_a_tp_boundary_comparison.csv`
- `static_threshold_comparison`: `/Volumes/Extreme SSD/01_ACTIVE_PROJECTS/O&M_Data_Acquisition_Accessability&Validation/reports/metocean_fusion_v0/static_threshold_comparison.csv`

## Conclusion

- coverage_improved: `True`
- material_tier_a_boundary_change_flag: `True`
- ready_for_current_v1_integration: `True` if the caveats above are accepted; currents should be added as a separate v1 pilot after true `uo/vo` products are validated.
- next_increment: `Current pilot planner and one-farm/year current pilots, not broad current downloads.`
