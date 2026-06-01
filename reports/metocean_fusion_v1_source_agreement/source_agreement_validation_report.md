# Metocean Fusion v1 Source Agreement Validation Report

Status: completed Fusion v1 source-agreement increment. Existing accepted local sources were read only. No current downloads, FINO import, source archive mutation, NORA3 rerun, source fusion beyond wave-source agreement/confidence, final production dwell-metocean rebuild, or CTV/SOV inference was performed.

## A. Research Design

Hypotheses: multi-source agreement improves confidence in event-level wave assignment; high-disagreement events can materially affect the Tier A observed Hs/Tp envelope; shallow/coastal sites show larger disagreement; a confidence-filtered envelope is more defensible than the Fusion v0 priority envelope; and NORA3, NWS, and Baltic strengths are complementary rather than hierarchical.

Metrics: source coverage/overlap, Hs/Tp R2, RMSE, MAE, bias, median and p95 absolute difference, circular directional difference where present, event-window temporal alignment, spatial/sample distance, confidence-class distribution, and Tier A Hs/Tp p50/p95 envelope impact. Agreement thresholds are transparent: strong if Hs <= 0.15 m and Tp <= 0.75 s; moderate if Hs <= 0.35 m and Tp <= 1.50 s; otherwise weak.

Weighted Hs/Tp columns are sensitivity diagnostics, not truth. The formula is a quality-weighted average using variable completeness, temporal gap/bracketing, sample distance, domain match, depth warning, and agreement support; selected source remains a traceable source candidate.

## 1. Executive Conclusion

- Fusion v1 accepted: `True`
- Candidate rows: `277980`; pairwise rows: `74152`; event confidence rows: `92660`
- Confidence row identity preserved versus dwell input: `True`; duplicate dwell IDs: `0`
- Fusion v0 selected source differs from v1 for `27063` valid-overlap events.
- Tier A valid counts: v0 `14606`, v1 selected `14606`, v1 high-confidence `6440`.
- Mean absolute Tier A p95 Hs boundary change versus v0 across reported variants: `0.044807075027179795` m; max absolute change: `0.417282263729684` m.
- Next increment: Proceed to scoped current pilots next, reusing this candidate/agreement/confidence frame for u/v current sources.

## 2. Why Fusion v0 Was Insufficient

Fusion v0 was a coverage resolver: Baltic if available, else NWS if available, else NORA3. That raised Hs/Tp coverage, but it encoded a hierarchy before testing whether overlapping sources agreed. Fusion v1 keeps the source candidates separate, evaluates overlap and disagreement, and assigns confidence before any envelope interpretation.

## 3. Input Inventory

- Dwell rows: `92660`
- Fusion v0 rows: `92660`
- NWS archive: `{'exists': True, 'partition_count': 1169, 'farm_count': 112, 'row_count': 173270992, 'first_year': 1995, 'last_year': 2024}`
- Baltic archive: `{'exists': True, 'partition_count': 238, 'farm_count': 16, 'row_count': 73866720, 'first_year': 1995, 'last_year': 2024}`
- NORA3 active Hs/Tp valid rows: `44377`
- Bathymetry rows: `6642`

## 4. Candidate Coverage

### Valid Candidate Rows By Source

| source   |   candidate_rows |   valid_candidate_rows |   valid_rate |
|:---------|-----------------:|-----------------------:|-------------:|
| nora3    |            92660 |                  44377 |     0.478923 |
| nws      |            92660 |                  67292 |     0.726225 |
| baltic   |            92660 |                  32896 |     0.355018 |

### Source Pair Overlap

| source_pair     |   overlap_count |
|:----------------|----------------:|
| nora3_vs_nws    |           27924 |
| nora3_vs_baltic |           15493 |
| nws_vs_baltic   |           30735 |

Coverage by source/tier and source/farm/year is written to the detailed validation tables listed below.

## 5. Pairwise Agreement

| source_pair     |   count |    hs_r2 |   hs_rmse |    hs_mae |    hs_bias |   hs_median_abs_diff |   hs_p95_abs_diff |    tp_r2 |   tp_rmse |   tp_mae |   tp_bias |   tp_median_abs_diff |   tp_p95_abs_diff |   direction_count |   direction_mae_deg |   direction_p95_abs_diff_deg |
|:----------------|--------:|---------:|----------:|----------:|-----------:|---------------------:|------------------:|---------:|----------:|---------:|----------:|---------------------:|------------------:|------------------:|--------------------:|-----------------------------:|
| nora3_vs_baltic |   15493 | 0.905213 | 0.100023  | 0.0742454 | -0.0373611 |            0.0558444 |          0.210035 | 0.684647 |  0.751549 | 0.503507 |  0.365696 |             0.351715 |           1.70473 |             15493 |            162.044  |                     179.238  |
| nora3_vs_nws    |   27924 | 0.943619 | 0.10424   | 0.0701857 |  0.0116355 |            0.0480185 |          0.198028 | 0.657419 |  1.28465  | 0.601996 |  0.066923 |             0.269079 |           2.26621 |             27924 |            165.986  |                     179.487  |
| nws_vs_baltic   |   30735 | 0.928544 | 0.0864644 | 0.0684999 | -0.0418538 |            0.0574659 |          0.16362  | 0.276272 |  1.12588  | 0.620972 |  0.470994 |             0.295575 |           2.18845 |             30735 |             20.0679 |                      90.7743 |

## 6. Spatial And Temporal Quality

| source   |   rows |   sample_distance_p50_km |   sample_distance_p95_km |   source_grid_distance_p50_km |   source_grid_distance_p95_km |   nearest_time_gap_p50_min |   nearest_time_gap_p95_min |   event_window_sample_count_p50 |   event_window_sample_count_p95 |   not_bracketed_rows |   shallow_water_rows |
|:---------|-------:|-------------------------:|-------------------------:|------------------------------:|------------------------------:|---------------------------:|---------------------------:|--------------------------------:|--------------------------------:|---------------------:|---------------------:|
| nora3    |  92660 |                nan       |                nan       |                    nan        |                     nan       |                   nan      |                   nan      |                               5 |                              34 |                    0 |                57521 |
| nws      |  92660 |                  2.75144 |                  4.77862 |                      0.739468 |                       1.71996 |                    34.0083 |                    84.8833 |                               1 |                              15 |                25542 |                57521 |
| baltic   |  92660 |                  2.10309 |                  4.67045 |                      0.662944 |                       1.07262 |                    11.7042 |                    28.6    |                               0 |                              23 |                59803 |                57521 |

The shallow/depth warning uses `water_depth_m <= 10`; explicit `<=1`, `<=5`, and `<=10` flags are preserved in the candidate table.

## 7. Confidence Scoring

### Confidence Class Distribution

| wave_confidence_class   |   rows |
|:------------------------|-------:|
| C_low                   |  52335 |
| B_medium                |  16115 |
| A_high                  |  15451 |
| D_unsuitable            |   8759 |

Confidence by farm, selected source, and dwell tier, plus high-disagreement examples, are written to the detailed validation tables.

## 8. Comparison With Fusion v0

- Selected sources differ from v0: `27063`
- v0 priority supported by v1 A/B confidence and same source: `28856`
- v0 priority questionable through changed source, low confidence, or high disagreement: `63870`
- Tier A v0 valid rows: `14606`
- Tier A v1 selected valid rows: `14606`
- Tier A v1 high-confidence rows: `6440`

### Source Comparison Counts

| fusion_wave_source   | selected_wave_source   |   rows |
|:---------------------|:-----------------------|-------:|
| nws                  | nws                    |  36557 |
| baltic               | nws                    |  21748 |
| nora3                | nora3                  |  14448 |
| missing              | missing                |   8759 |
| baltic               | baltic                 |   5833 |
| baltic               | nora3                  |   5315 |

## 9. Workability Envelope Sensitivity

| variant                   |   tp_bin_left |   tp_bin_right |   rows_v0 |   rows_variant |   hs_p50_v0 |   hs_p50_variant |   hs_p95_v0 |   hs_p95_variant |   hs_p95_delta_vs_v0 |
|:--------------------------|--------------:|---------------:|----------:|---------------:|------------:|-----------------:|------------:|-----------------:|---------------------:|
| fusion_v1_selected_all    |           1.5 |            2   |       304 |            272 |    0.123654 |        0.0517482 |    0.20807  |         0.196388 |         -0.011682    |
| fusion_v1_selected_all    |           2   |            2.5 |       720 |            726 |    0.20899  |        0.190488  |    0.327486 |         0.306987 |         -0.0204988   |
| fusion_v1_selected_all    |           2.5 |            3   |      1375 |           1370 |    0.319977 |        0.300846  |    0.481681 |         0.459723 |         -0.0219582   |
| fusion_v1_selected_all    |           3   |            3.5 |      1726 |           1808 |    0.476659 |        0.463424  |    0.668452 |         0.65     |         -0.018452    |
| fusion_v1_selected_all    |           3.5 |            4   |      2041 |           2088 |    0.647593 |        0.636189  |    0.896    |         0.883097 |         -0.0129027   |
| fusion_v1_selected_all    |           4   |            4.5 |      1917 |           1936 |    0.803071 |        0.797417  |    1.0849   |         1.074    |         -0.0108994   |
| fusion_v1_selected_all    |           4.5 |            5   |      1466 |           1457 |    0.865971 |        0.850472  |    1.28913  |         1.26215  |         -0.0269791   |
| fusion_v1_selected_all    |           5   |            5.5 |      1133 |           1125 |    0.804353 |        0.808     |    1.46593  |         1.462    |         -0.00393301  |
| fusion_v1_selected_all    |           5.5 |            6   |      1010 |           1002 |    0.7965   |        0.795167  |    1.532    |         1.53584  |          0.00384396  |
| fusion_v1_selected_all    |           6   |            6.5 |       750 |            732 |    0.826    |        0.835     |    1.7363   |         1.71134  |         -0.0249571   |
| fusion_v1_selected_all    |           6.5 |            7   |       564 |            575 |    0.806454 |        0.808     |    1.70793  |         1.71382  |          0.00589     |
| fusion_v1_selected_all    |           7   |            7.5 |       322 |            324 |    1.00067  |        0.98438   |    2.054    |         2.04045  |         -0.01355     |
| fusion_v1_selected_all    |           7.5 |            8   |       341 |            327 |    0.925585 |        0.944     |    1.99533  |         2.02099  |          0.0256553   |
| fusion_v1_selected_all    |           8   |            8.5 |       145 |            147 |    0.689    |        0.69      |    1.7502   |         1.81605  |          0.06585     |
| fusion_v1_selected_all    |           8.5 |            9   |        78 |             78 |    0.816476 |        0.779333  |    2.22931  |         2.19701  |         -0.0323      |
| fusion_v1_selected_all    |           9   |            9.5 |        88 |             89 |    0.581107 |        0.582214  |    1.81328  |         1.79875  |         -0.0145294   |
| fusion_v1_selected_all    |           9.5 |           10   |        90 |             85 |    0.662583 |        0.65      |    1.30103  |         1.33108  |          0.03005     |
| fusion_v1_selected_all    |          10   |           10.5 |       166 |            167 |    0.625488 |        0.622802  |    1.33     |         1.3311   |          0.0011      |
| fusion_v1_selected_all    |          10.5 |           11   |        73 |             76 |    0.851    |        0.825     |    1.4471   |         1.43072  |         -0.0163769   |
| fusion_v1_selected_all    |          11   |           11.5 |        56 |             57 |    0.743    |        0.752     |    1.28384  |         1.30536  |          0.0215165   |
| fusion_v1_selected_all    |          11.5 |           12   |        50 |             50 |    0.947667 |        0.949     |    1.4586   |         1.4572   |         -0.0014      |
| fusion_v1_selected_all    |          12   |           12.5 |        24 |             22 |    0.879181 |        0.879181  |    1.32873  |         1.21704  |         -0.111692    |
| fusion_v1_selected_all    |          12.5 |           13   |        15 |             16 |    0.606    |        0.585833  |    0.998091 |         0.978691 |         -0.0194      |
| fusion_v1_selected_all    |          13   |           13.5 |         5 |              5 |    0.557    |        0.552     |    0.67502  |         0.674361 |         -0.000658824 |
| fusion_v1_selected_all    |          13.5 |           14   |        22 |             21 |    0.580333 |        0.570667  |    1.07354  |         1.06086  |         -0.012681    |
| fusion_v1_selected_all    |          14   |           14.5 |         5 |              6 |    1.03733  |        1.023     |    1.249    |         1.246    |         -0.003       |
| fusion_v1_selected_all    |          14.5 |           15   |        18 |             19 |    0.717    |        0.708     |    1.17262  |         1.19297  |          0.0203502   |
| fusion_v1_selected_all    |          15   |           15.5 |         8 |              7 |    0.5845   |        0.452286  |    0.6293   |         0.6275   |         -0.0018      |
| fusion_v1_selected_all    |          15.5 |           16   |        12 |             11 |    0.606167 |        0.613     |    0.69925  |         0.7065   |          0.00725     |
| fusion_v1_high_confidence |           1.5 |            2   |       304 |             59 |    0.123654 |        0.0586667 |    0.20807  |         0.2002   |         -0.00786983  |

The high-confidence and shallow-excluded variants are deliberately narrower evidence sets. They are defensible sensitivity boundaries, not calibrated access probabilities.

## 10. Research Interpretation

- Source agreement increases confidence because it exposes which event assignments are supported by independent products rather than only source priority.
- Disagreement should be interpreted by source pair, region, and depth warning before changing workability claims; the detailed tables identify the concentrated examples.
- Fusion v1 can change the provisional Tier A envelope by changing both the selected source and the event subset admitted to high-confidence boundary calculations.
- Before currents are added, the current branch should emit the same candidate, pairwise-agreement, confidence, and missing-reason fields for u/v-derived current speed and direction.

## 11. Clear Next Increment

Proceed to scoped current pilots next, reusing this candidate/agreement/confidence frame for u/v current sources.

Current confidence should reuse the v1 framework: keep source-specific candidates, quantify overlap, score temporal/spatial quality, preserve missing reasons, and only then derive simulator-ready current inputs with provenance and uncertainty.

## Detailed Validation Tables

- `candidate_coverage_by_source`: `reports/metocean_fusion_v1_source_agreement/candidate_coverage_by_source.csv`
- `candidate_coverage_by_source_tier`: `reports/metocean_fusion_v1_source_agreement/candidate_coverage_by_source_tier.csv`
- `candidate_coverage_by_source_farm_year`: `reports/metocean_fusion_v1_source_agreement/candidate_coverage_by_source_farm_year.csv`
- `source_overlap_counts`: `reports/metocean_fusion_v1_source_agreement/source_overlap_counts.csv`
- `pairwise_agreement_metrics`: `reports/metocean_fusion_v1_source_agreement/pairwise_agreement_metrics.csv`
- `spatial_temporal_quality_by_source`: `reports/metocean_fusion_v1_source_agreement/spatial_temporal_quality_by_source.csv`
- `confidence_class_distribution`: `reports/metocean_fusion_v1_source_agreement/confidence_class_distribution.csv`
- `confidence_by_farm`: `reports/metocean_fusion_v1_source_agreement/confidence_by_farm.csv`
- `confidence_by_source`: `reports/metocean_fusion_v1_source_agreement/confidence_by_source.csv`
- `confidence_by_tier`: `reports/metocean_fusion_v1_source_agreement/confidence_by_tier.csv`
- `high_disagreement_examples`: `reports/metocean_fusion_v1_source_agreement/high_disagreement_examples.csv`
- `fusion_v0_vs_v1_source_comparison`: `reports/metocean_fusion_v1_source_agreement/fusion_v0_vs_v1_source_comparison.csv`
- `tier_a_hs_tp_boundary_variants`: `reports/metocean_fusion_v1_source_agreement/tier_a_hs_tp_boundary_variants.csv`
- `tier_a_hs_tp_boundary_delta_vs_v0`: `reports/metocean_fusion_v1_source_agreement/tier_a_hs_tp_boundary_delta_vs_v0.csv`

## Output Paths

- candidate_table: `Data/Processed/metocean/fusion_v1_source_agreement/wave_source_candidates.parquet`
- pairwise_agreement_table: `Data/Processed/metocean/fusion_v1_source_agreement/wave_source_pairwise_agreement.parquet`
- event_confidence_table: `Data/Processed/metocean/fusion_v1_source_agreement/wave_event_confidence.parquet`
- validation_report: `reports/metocean_fusion_v1_source_agreement/source_agreement_validation_report.md`
