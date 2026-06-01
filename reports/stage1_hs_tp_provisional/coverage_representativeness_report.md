# Provisional Stage 1 Hs-Tp Readiness Report

**Label:** Provisional NORA3-derived Tier A wave-only observed operational envelope

This report describes an observed operational envelope from weather-joined dwell rows. It is not a probability model and must not be labelled `P(operation | weather)`.

## Input

- Input file: `Data/Processed/ais_dwell_backfill/cross_farm_dwell_weather_features.parquet`
- Input rows: 92660
- Input columns: 96
- Duplicate dwell IDs: 0

## Primary Subset

- Filter: `active_hs_mean.notnull() AND active_tp_mean.notnull() AND dwell_tier == "Tier A"`
- Rows: 13574
- Farms: 105
- MMSIs: 976
- Year range: 2010 to 2025
- Month range: 1 to 12

## Sensitivity Subset

- Filter: `active_hs_mean.notnull() AND active_tp_mean.notnull()`
- Rows: 44377
- Farms: 113
- MMSIs: 3835
- Year range: 2010 to 2025
- Month range: 1 to 12
- Tier distribution: `{'Tier D': 26948, 'Tier A': 13574, 'Tier C': 2017, 'Tier B': 1838}`

## Physical Range Checks

- `primary_tier_a`: `{'hs_non_negative': True, 'hs_below_or_equal_20m': True, 'tp_positive': True, 'tp_below_or_equal_30s': True}`
- `sensitivity_all_tiers`: `{'hs_non_negative': True, 'hs_below_or_equal_20m': True, 'tp_positive': True, 'tp_below_or_equal_30s': True}`

## Output Inventory

- `coverage_report_md`: `reports/stage1_hs_tp_provisional/coverage_representativeness_report.md`
- `null_rates_csv`: `reports/stage1_hs_tp_provisional/core_null_rates.csv`
- `occupancy_primary_csv`: `reports/stage1_hs_tp_provisional/hs_tp_occupancy_matrix_primary.csv`
- `occupancy_sensitivity_csv`: `reports/stage1_hs_tp_provisional/hs_tp_occupancy_matrix_sensitivity.csv`
- `primary_clean_csv`: `Data/Processed/analysis/stage1_hs_tp_provisional/primary_tier_a_hs_tp.csv`
- `primary_clean_parquet`: `Data/Processed/analysis/stage1_hs_tp_provisional/primary_tier_a_hs_tp.parquet`
- `primary_summary_csv`: `reports/stage1_hs_tp_provisional/primary_subset_summary.csv`
- `scatter_primary_png`: `reports/stage1_hs_tp_provisional/tier_a_hs_tp_scatter.png`
- `scatter_sensitivity_png`: `reports/stage1_hs_tp_provisional/all_tiers_hs_tp_density.png`
- `sensitivity_summary_csv`: `reports/stage1_hs_tp_provisional/sensitivity_subset_summary.csv`
- `static_thresholds_csv`: `reports/stage1_hs_tp_provisional/static_hs_threshold_comparison.csv`
- `tp_boundary_primary_csv`: `reports/stage1_hs_tp_provisional/tp_bin_percentile_boundary_primary.csv`
- `tp_boundary_sensitivity_csv`: `reports/stage1_hs_tp_provisional/tp_bin_percentile_boundary_sensitivity.csv`
- `validation_json`: `reports/stage1_hs_tp_provisional/validation_summary.json`

## Guardrails

- Provisional NORA3-derived wave-only analysis.
- Tier A is an asset-proximal observed-operation proxy.
- No current fields are used.
- No CTV/SOV role inference is performed.
- No final source-agnostic metocean assignment table is rebuilt.
