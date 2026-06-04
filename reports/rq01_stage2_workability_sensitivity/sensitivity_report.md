# RQ01 Stage 2 Fusion v2 Workability Sensitivity

## Executive Summary

- Analysis recommendation: `restricted_descriptive_sensitivity_ready_for_review`
- Run timestamp UTC: `2026-06-04T15:01:24Z`
- Scope: restricted descriptive sensitivity of observed Tier A envelopes.
- This report does not train models, estimate calibrated `P(operation | weather)`, rebuild Fusion v2, download data, import FINO, repair wind/current evidence, or treat missing current as zero.
- Wave-only and wave+wind-speed are primary lanes. Current-aware lanes are NWS-domain / coverage-limited sensitivity only.
- Current appears to change the observed envelope in the screened subsets, but current-aware lanes are NWS-domain / coverage-limited sensitivity results, not general cross-European causal evidence.
- Wind direction is excluded from primary predictors. Depth-warning exclusion and depth-warning-only sensitivity subsets are emitted.

## Materiality Screen

The screen is descriptive and threshold-based. It flags sensitivity lanes for review when subset retention, Hs/Tp envelope deltas, or farm concentration shift materially relative to wave-only Tier A evidence.

- Retained share below wave-only threshold: `0.8`
- Absolute Hs p95 delta threshold: `0.25 m`
- Absolute Tp p95 delta threshold: `0.5 s`
- Absolute top-farm share delta threshold: `0.1`

## Lane Comparisons Versus Wave-Only

| comparison_lane_id           | comparison_lane_role                   | depth_scope            |   baseline_tier_a_event_count |   comparison_tier_a_event_count |   retained_share_vs_wave_only |   hs_p95_delta |   tp_p95_delta |   top_farm_share_delta | materiality_screen_result       | materiality_triggers                                         |
|:-----------------------------|:---------------------------------------|:-----------------------|------------------------------:|--------------------------------:|------------------------------:|---------------:|---------------:|-----------------------:|:--------------------------------|:-------------------------------------------------------------|
| wave_wind_speed              | primary                                | all_tier_a             |                         14606 |                           13668 |                      0.93578  |     -0.022     |        0       |             0.00451064 | no_material_difference_screened |                                                              |
| wave_wind_speed              | primary                                | depth_warning_excluded |                         12132 |                           11257 |                      0.927877 |     -0.0207483 |        0.03775 |             0.0061507  | no_material_difference_screened |                                                              |
| wave_current                 | restricted_current_sensitivity         | all_tier_a             |                         14606 |                            5308 |                      0.363412 |      0.162325  |        1.475   |             0.0292246  | material_difference_screened    | retained_share_below_threshold\|tp_p95_delta_above_threshold |
| wave_current                 | restricted_current_sensitivity         | depth_warning_excluded |                         12132 |                            5299 |                      0.436779 |      0.1187    |        1.43875 |             0.0142843  | material_difference_screened    | retained_share_below_threshold\|tp_p95_delta_above_threshold |
| wave_wind_current            | restricted_current_sensitivity         | all_tier_a             |                         14606 |                            4552 |                      0.311653 |      0.13245   |        1.74747 |             0.0423579  | material_difference_screened    | retained_share_below_threshold\|tp_p95_delta_above_threshold |
| wave_wind_current            | restricted_current_sensitivity         | depth_warning_excluded |                         12132 |                            4543 |                      0.374464 |      0.08845   |        1.71022 |             0.0271878  | material_difference_screened    | retained_share_below_threshold\|tp_p95_delta_above_threshold |
| high_confidence_multivariate | restricted_high_confidence_sensitivity | all_tier_a             |                         14606 |                            3402 |                      0.232918 |      0.124375  |       -0.29125 |             0.0403876  | material_difference_screened    | retained_share_below_threshold                               |
| high_confidence_multivariate | restricted_high_confidence_sensitivity | depth_warning_excluded |                         12132 |                            3402 |                      0.280415 |      0.081025  |       -0.3285  |             0.0269845  | material_difference_screened    | retained_share_below_threshold                               |

## Lane Counts

| lane_id                      | lane_role                              | depth_scope            |   tier_a_event_count |   complete_feature_count |   excluded_null_feature_count |   retained_share_vs_wave_only | top_wind_farm   |   top_wind_farm_share |   top_audit_year |   top_audit_year_share |
|:-----------------------------|:---------------------------------------|:-----------------------|---------------------:|-------------------------:|------------------------------:|------------------------------:|:----------------|----------------------:|-----------------:|-----------------------:|
| wave_only                    | primary                                | all_tier_a             |                14606 |                    14606 |                             0 |                    1          | Baltic_Eagle    |             0.0657264 |             2024 |               0.64316  |
| wave_only                    | primary                                | depth_warning_excluded |                12132 |                    12132 |                             0 |                    1          | Baltic_Eagle    |             0.0791296 |             2024 |               0.662051 |
| wave_only                    | primary                                | depth_warning_only     |                 2474 |                     2474 |                             0 |                    1          | Rodsand_II      |             0.270412  |             2024 |               0.550525 |
| wave_wind_speed              | primary                                | all_tier_a             |                13668 |                    13668 |                             0 |                    0.93578    | Baltic_Eagle    |             0.0702371 |             2024 |               0.635206 |
| wave_wind_speed              | primary                                | depth_warning_excluded |                11257 |                    11257 |                             0 |                    0.927877   | Baltic_Eagle    |             0.0852803 |             2024 |               0.652305 |
| wave_wind_speed              | primary                                | depth_warning_only     |                 2411 |                     2411 |                             0 |                    0.974535   | Rodsand_II      |             0.273745  |             2024 |               0.555371 |
| wave_current                 | restricted_current_sensitivity         | all_tier_a             |                 5308 |                     5308 |                             0 |                    0.363412   | Horns_Rev_II    |             0.094951  |             2024 |               0.798606 |
| wave_current                 | restricted_current_sensitivity         | depth_warning_excluded |                 5299 |                     5299 |                             0 |                    0.436779   | Horns_Rev_II    |             0.0934139 |             2024 |               0.798641 |
| wave_current                 | restricted_current_sensitivity         | depth_warning_only     |                    9 |                        9 |                             0 |                    0.00363783 | Horns_Rev_II    |             1         |             2024 |               0.777778 |
| wave_wind_current            | restricted_current_sensitivity         | all_tier_a             |                 4552 |                     4552 |                             0 |                    0.311653   | Horns_Rev_II    |             0.108084  |             2024 |               0.792838 |
| wave_wind_current            | restricted_current_sensitivity         | depth_warning_excluded |                 4543 |                     4543 |                             0 |                    0.374464   | Horns_Rev_II    |             0.106317  |             2024 |               0.792868 |
| wave_wind_current            | restricted_current_sensitivity         | depth_warning_only     |                    9 |                        9 |                             0 |                    0.00363783 | Horns_Rev_II    |             1         |             2024 |               0.777778 |
| high_confidence_multivariate | restricted_high_confidence_sensitivity | all_tier_a             |                 3402 |                     3402 |                             0 |                    0.232918   | Horns_Rev_II    |             0.106114  |             2024 |               0.783951 |
| high_confidence_multivariate | restricted_high_confidence_sensitivity | depth_warning_excluded |                 3402 |                     3402 |                             0 |                    0.280415   | Horns_Rev_II    |             0.106114  |             2024 |               0.783951 |
| high_confidence_multivariate | restricted_high_confidence_sensitivity | depth_warning_only     |                    0 |                        0 |                             0 |                    0          |                 |             0         |                  |               0        |

## Feature Envelope Summary

| lane_id                      | depth_scope            | feature       | unit   |   complete_count |          min |         p25 |     median |        p75 |        p95 |        max |
|:-----------------------------|:-----------------------|:--------------|:-------|-----------------:|-------------:|------------:|-----------:|-----------:|-----------:|-----------:|
| wave_only                    | all_tier_a             | hs            | m      |            14606 |   0          |   0.369008  |   0.598756 |   0.87185  |   1.314    |   4.84629  |
| wave_only                    | all_tier_a             | tp            | s      |            14606 |   1.36788    |   3.35579   |   4.29     |   5.6      |   8.405    |  18.8051   |
| wave_only                    | depth_warning_excluded | hs            | m      |            12132 |   0.001      |   0.408782  |   0.642667 |   0.914    |   1.35735  |   4.84629  |
| wave_only                    | depth_warning_excluded | tp            | s      |            12132 |   1.36788    |   3.55841   |   4.43     |   5.725    |   8.44225  |  18.8051   |
| wave_only                    | depth_warning_only     | hs            | m      |             2474 |   0          |   0.226708  |   0.42     |   0.636    |   0.997025 |   1.72505  |
| wave_only                    | depth_warning_only     | tp            | s      |             2474 |   1.42638    |   2.62489   |   3.3248   |   4.30506  |   8.357    |  17.7      |
| wave_wind_speed              | all_tier_a             | hs            | m      |            13668 |   0          |   0.361673  |   0.590013 |   0.858587 |   1.292    |   4.84629  |
| wave_wind_speed              | all_tier_a             | tp            | s      |            13668 |   1.36788    |   3.31576   |   4.20782  |   5.52542  |   8.405    |  18.8051   |
| wave_wind_speed              | all_tier_a             | wind_speed    | m/s    |            13668 |   0.23       |   4.22843   |   5.84268  |   7.59133  |   9.88     |  18.2235   |
| wave_wind_speed              | depth_warning_excluded | hs            | m      |            11257 |   0.001      |   0.401     |   0.630874 |   0.897    |   1.3366   |   4.84629  |
| wave_wind_speed              | depth_warning_excluded | tp            | s      |            11257 |   1.36788    |   3.53472   |   4.35     |   5.665    |   8.48     |  18.8051   |
| wave_wind_speed              | depth_warning_excluded | wind_speed    | m/s    |            11257 |   0.23       |   4.25      |   5.84286  |   7.58333  |   9.87122  |  18.2235   |
| wave_wind_speed              | depth_warning_only     | hs            | m      |             2411 |   0          |   0.22625   |   0.418843 |   0.636333 |   0.997333 |   1.72505  |
| wave_wind_speed              | depth_warning_only     | tp            | s      |             2411 |   1.42638    |   2.62      |   3.32667  |   4.3      |   8.11167  |  17.7      |
| wave_wind_speed              | depth_warning_only     | wind_speed    | m/s    |             2411 |   0.822727   |   4.12493   |   5.83913  |   7.62429  |   9.93     |  13.79     |
| wave_current                 | all_tier_a             | hs            | m      |             5308 |   0.088      |   0.514     |   0.76     |   1.037    |   1.47633  |   4.84629  |
| wave_current                 | all_tier_a             | tp            | s      |             5308 |   1.82       |   4.355     |   5.29     |   6.41719  |   9.88     |  17.185    |
| wave_current                 | all_tier_a             | current_speed | m/s    |             5308 |   0.00103757 |   0.0822208 |   0.151443 |   0.27215  |   0.506899 |   0.970795 |
| wave_current                 | depth_warning_excluded | hs            | m      |             5299 |   0.088      |   0.514     |   0.76     |   1.03683  |   1.47605  |   4.84629  |
| wave_current                 | depth_warning_excluded | tp            | s      |             5299 |   1.82       |   4.355     |   5.29     |   6.41646  |   9.881    |  17.185    |
| wave_current                 | depth_warning_excluded | current_speed | m/s    |             5299 |   0.00103757 |   0.0822266 |   0.151512 |   0.272109 |   0.506884 |   0.970795 |
| wave_current                 | depth_warning_only     | hs            | m      |                9 |   0.42       |   0.718     |   1.124    |   1.332    |   1.61081  |   1.72505  |
| wave_current                 | depth_warning_only     | tp            | s      |                9 |   3.72       |   4.745     |   5.84     |   6.76621  |   8.043    |   8.565    |
| wave_current                 | depth_warning_only     | current_speed | m/s    |                9 |   0.0343002  |   0.0477707 |   0.126481 |   0.301953 |   0.500524 |   0.610442 |
| wave_wind_current            | all_tier_a             | hs            | m      |             4552 |   0.088      |   0.51      |   0.749512 |   1.026    |   1.44645  |   4.84629  |
| wave_wind_current            | all_tier_a             | tp            | s      |             4552 |   1.82       |   4.29667   |   5.28     |   6.42     |  10.1525   |  17.185    |
| wave_wind_current            | all_tier_a             | wind_speed    | m/s    |             4552 |   0.23       |   4.38083   |   5.77073  |   7.61719  |   9.87606  |  18.2235   |
| wave_wind_current            | all_tier_a             | current_speed | m/s    |             4552 |   0.00341768 |   0.086922  |   0.161384 |   0.28107  |   0.511267 |   0.970795 |
| wave_wind_current            | depth_warning_excluded | hs            | m      |             4543 |   0.088      |   0.51      |   0.749    |   1.02479  |   1.4458   |   4.84629  |
| wave_wind_current            | depth_warning_excluded | tp            | s      |             4543 |   1.82       |   4.29667   |   5.28     |   6.42     |  10.1525   |  17.185    |
| wave_wind_current            | depth_warning_excluded | wind_speed    | m/s    |             4543 |   0.23       |   4.38056   |   5.77     |   7.61     |   9.87811  |  18.2235   |
| wave_wind_current            | depth_warning_excluded | current_speed | m/s    |             4543 |   0.00341768 |   0.0869464 |   0.161435 |   0.280762 |   0.511232 |   0.970795 |
| wave_wind_current            | depth_warning_only     | hs            | m      |                9 |   0.42       |   0.718     |   1.124    |   1.332    |   1.61081  |   1.72505  |
| wave_wind_current            | depth_warning_only     | tp            | s      |                9 |   3.72       |   4.745     |   5.84     |   6.76621  |   8.043    |   8.565    |
| wave_wind_current            | depth_warning_only     | wind_speed    | m/s    |                9 |   2.9125     |   5.66478   |   7.77     |   8.58     |   9.2306   |   9.37546  |
| wave_wind_current            | depth_warning_only     | current_speed | m/s    |                9 |   0.0343002  |   0.0477707 |   0.126481 |   0.301953 |   0.500524 |   0.610442 |
| high_confidence_multivariate | all_tier_a             | hs            | m      |             3402 |   0.098      |   0.522     |   0.764    |   1.02443  |   1.43838  |   4.84629  |
| high_confidence_multivariate | all_tier_a             | tp            | s      |             3402 |   1.82       |   4.24      |   5.15     |   6.17     |   8.11375  |  13.6667   |
| high_confidence_multivariate | all_tier_a             | wind_speed    | m/s    |             3402 |   0.23       |   4.38274   |   5.85174  |   7.68529  |   9.80275  |  18.2235   |
| high_confidence_multivariate | all_tier_a             | current_speed | m/s    |             3402 |   0.00488011 |   0.0872891 |   0.162003 |   0.28396  |   0.51135  |   0.970795 |
| high_confidence_multivariate | depth_warning_excluded | hs            | m      |             3402 |   0.098      |   0.522     |   0.764    |   1.02443  |   1.43838  |   4.84629  |
| high_confidence_multivariate | depth_warning_excluded | tp            | s      |             3402 |   1.82       |   4.24      |   5.15     |   6.17     |   8.11375  |  13.6667   |
| high_confidence_multivariate | depth_warning_excluded | wind_speed    | m/s    |             3402 |   0.23       |   4.38274   |   5.85174  |   7.68529  |   9.80275  |  18.2235   |
| high_confidence_multivariate | depth_warning_excluded | current_speed | m/s    |             3402 |   0.00488011 |   0.0872891 |   0.162003 |   0.28396  |   0.51135  |   0.970795 |
| high_confidence_multivariate | depth_warning_only     | hs            | m      |                0 | nan          | nan         | nan        | nan        | nan        | nan        |
| high_confidence_multivariate | depth_warning_only     | tp            | s      |                0 | nan          | nan         | nan        | nan        | nan        | nan        |
| high_confidence_multivariate | depth_warning_only     | wind_speed    | m/s    |                0 | nan          | nan         | nan        | nan        | nan        | nan        |
| high_confidence_multivariate | depth_warning_only     | current_speed | m/s    |                0 | nan          | nan         | nan        | nan        | nan        | nan        |

## Claim Boundary Checks

| check                                           | status   | severity       |   issue_count | evidence                                                                                     |
|:------------------------------------------------|:---------|:---------------|--------------:|:---------------------------------------------------------------------------------------------|
| required_columns_present                        | pass     | integrity      |             0 | Fusion v2 contains every RQ01 required column.                                               |
| duplicate_dwell_identity                        | pass     | integrity      |             0 | RQ01 requires unique dwell_id rows.                                                          |
| missing_current_null_not_zero                   | pass     | integrity      |             0 | Rows without event-scale current must retain null current values, not zero-filled values.    |
| wave_only_tier_a_nonzero                        | pass     | integrity      |             0 | Primary wave-only Tier A lane must be non-empty.                                             |
| wave_wind_speed_tier_a_nonzero                  | pass     | integrity      |             0 | Primary wave + wind-speed Tier A lane must be non-empty.                                     |
| current_lanes_nonzero_but_restricted            | pass     | integrity      |             0 | Current-aware lanes are available but remain NWS-domain / coverage-limited sensitivity only. |
| wind_direction_excluded_from_primary_predictors | pass     | claim_boundary |             0 | Lane feature columns do not include wind-direction fields.                                   |
| current_lanes_labelled_restricted               | pass     | claim_boundary |             0 | Current-aware lanes are explicitly labelled restricted sensitivity.                          |
| readiness_restrictions_inherited                | pass     | claim_boundary |             0 | RQ01 inherits the Fusion v2 evidence-readiness restricted recommendation and caveats.        |
| depth_warning_sensitivity_present               | pass     | claim_boundary |             0 | RQ01 emits depth-warning exclusion and sensitivity subsets.                                  |
| no_calibrated_probability_claim                 | pass     | claim_boundary |             0 | Outputs are observed-envelope descriptive sensitivity only, not P(operation \| weather).     |

## Output Tables

- `lane_counts`: counts, retention, and concentration by lane and depth scope.
- `lane_feature_summary`: min/quantile/max summaries for observed lane features.
- `binned_occupancy`: occupied bins only; unobserved bins are not denominators.
- `lane_comparisons`: threshold-based descriptive comparison against wave-only Tier A evidence.
- `claim_boundary_checks`: integrity and claim-boundary checks for this restricted analysis.

## Claim Boundary

- Observed envelopes are descriptive summaries of observed Tier A dwell conditions.
- No calibrated operation-success probability or causal weather effect is claimed.
- Current-aware comparisons are restricted to NWS-domain / coverage-limited sensitivity.
- Wind direction remains excluded from primary predictors.
- Missing current remains null/missing and is never coerced to zero.
