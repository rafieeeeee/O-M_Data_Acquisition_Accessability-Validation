# RQ9 Farm-Level Maintenance Intervention Intensity Sanity Audit

This audit reviews the farm-level maintenance intervention intensity outputs for simulator readiness. It is not a confirmed fault-driven demand estimate. A vessel visit is not automatically a failure, and Tier A/B dwell evidence remains candidate intervention evidence until SCADA, fault log, work-order, or equivalent validation is linked.

## Scope

- Farm-level only; turbine-level intervention intensity is not implemented here.
- Inputs audited: `/Volumes/Extreme SSD/01_ACTIVE_PROJECTS/O&M_Data_Acquisition_Accessability&Validation/Data/Processed/analysis/rq9_intervention_intensity/farm_intervention_intensity.csv`, `/Volumes/Extreme SSD/01_ACTIVE_PROJECTS/O&M_Data_Acquisition_Accessability&Validation/reports/rq9_intervention_intensity/validation_summary.csv`, `/Volumes/Extreme SSD/01_ACTIVE_PROJECTS/O&M_Data_Acquisition_Accessability&Validation/reports/rq9_intervention_intensity/methodology_report.md`.
- Farm months and candidate dwells are split into pre-operational, commissioning/ramp-up, steady-operational, and unknown phases.
- Default ramp-up buffer: 6 months after the latest parsed turbine commissioning month.
- Ramp-up sensitivity scenarios: 0, 6, 12 months.
- Stricter long-dwell sensitivity reads the existing dwell feature table in memory through the RQ9 builder. No AIS extraction or metocean extraction was rerun.

## Key Totals

| Metric | Value |
| --- | --- |
| Farm rows | 113 |
| Observed farm-years, all non-pre-operational phases | 986.500 |
| Commissioning/ramp-up observed farm-years | 44.583 |
| Steady operational observed farm-years | 941.917 |
| Observed farm-years min / median / max | 0.000 / 9.250 / 15.000 |
| Raw candidate interventions, Tier A + Tier B | 13545 |
| Pre-operational candidate rows preserved separately | 4020 |
| Commissioning/ramp-up candidate interventions | 537 |
| Steady operational candidate interventions | 13008 |
| Tier A candidate visits | 11972 |
| Tier B candidate visits | 1573 |
| Tier B share of raw candidates | 11.6% |
| Current steady long-dwell count | 11162 |
| Stricter steady long-dwell count | 9742 |
| Duplicate-adjusted candidate total | 13021.333 |
| Duplicate adjustment delta | 523.667 (3.9% of raw candidates) |

## Top 20 Steady Operational Intensities

| farm_id | farm_commissioning_end_month | steady_operational_start_month | steady_observed_years | steady_candidate_count | steady_intervention_intensity_per_farm_year | commissioning_candidate_count | pre_operational_candidate_count | coverage_share | confidence_class | recommended_simulator_use |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Kriegers Flak | 2021-09 | 2022-03 | 2.917 | 1042 | 357.257 | 4 | 140 | 0.788 | low_coverage | validation_required |
| Arcadis Ost 1 | 2023-12 | 2024-06 | 0.667 | 182 | 273 | 75 | 50 | 0.56 | low_coverage | insufficient_steady_coverage |
| Kaskasi | 2023-03 | 2023-09 | 1.417 | 201 | 141.882 | 4 | 49 | 0.676 | low_coverage | validation_required |
| Vesterhav Syd | 2024-03 | 2024-09 | 0.417 | 58 | 139.2 | 149 | 32 | 0.5 | low_coverage | insufficient_steady_coverage |
| Wikinger | 2018-10 | 2019-04 | 5.833 | 752 | 128.914 | 27 | 197 | 0.874 | high_observed_signal | steady_operational_only |
| Vesterhav Nord | 2024-03 | 2024-09 | 0.417 | 51 | 122.4 | 141 | 30 | 0.5 | low_coverage | insufficient_steady_coverage |
| Merkur Offshore | 2019-06 | 2019-12 | 5.167 | 459 | 88.839 | 7 | 71 | 0.861 | high_observed_signal | steady_operational_only |
| Horns Rev III | 2019-08 | 2020-02 | 5 | 440 | 88 | 14 | 93 | 0.857 | high_observed_signal | steady_operational_only |
| Anholt | 2013-09 | 2014-03 | 10.833 | 792 | 73.108 | 0 | 88 | 0.919 | high_observed_signal | steady_operational_only |
| EnBW Windpark Baltic 2 | 2015-10 | 2016-04 | 8.75 | 637 | 72.8 | 0 | 65 | 0.902 | high_observed_signal | steady_operational_only |
| Arkona-Becken Südost | 2019-01 | 2019-07 | 5.583 | 398 | 71.284 | 9 | 144 | 0.869 | high_observed_signal | steady_operational_only |
| Butendiek | 2015-08 | 2016-02 | 8.917 | 480 | 53.832 | 0 | 6 | 0.904 | high_observed_signal | steady_operational_only |
| Dan Tysk | 2015-04 | 2015-10 | 9.25 | 481 | 52 | 0 | 8 | 0.907 | high_observed_signal | steady_operational_only |
| Rodsand II | 2010-10 | 2011-04 | 13.75 | 649 | 47.2 | 8 | 79 | 0.934 | high_observed_signal | steady_operational_only |
| Amrumbank West | 2015-10 | 2016-04 | 8.75 | 362 | 41.371 | 0 | 19 | 0.902 | high_observed_signal | steady_operational_only |
| Gode Wind 1 and 2 | 2016-09 | 2017-03 | 7.917 | 324 | 40.926 | 0 | 2 | 0.902 | high_observed_signal | steady_operational_only |
| Horns Rev I | 2002-12 | 2003-06 | 15 | 587 | 39.133 | 0 | 0 | 0.938 | high_observed_signal | steady_operational_only |
| Sandbank | 2017-01 | 2017-07 | 7.583 | 289 | 38.11 | 14 | 2 | 0.898 | high_observed_signal | steady_operational_only |
| Meerwind Sued/Ost | 2014-12 | 2015-06 | 9.583 | 361 | 37.67 | 0 | 7 | 0.91 | high_observed_signal | steady_operational_only |
| Horns Rev II | 2010-01 | 2010-07 | 14.5 | 532 | 36.69 | 7 | 0 | 0.938 | high_observed_signal | steady_operational_only |

## Bottom 20 Steady Operational Intensities

| farm_id | farm_commissioning_end_month | steady_operational_start_month | steady_observed_years | steady_candidate_count | steady_intervention_intensity_per_farm_year | commissioning_candidate_count | pre_operational_candidate_count | coverage_share | confidence_class | recommended_simulator_use |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Blyth Demo Phase 1 | 2018-06 | 2018-12 | 6.167 | 0 | 0 | 0 | 3 | 0.879 | high_observed_zero | validation_required |
| Burbo Bank Extension | 2017-04 | 2017-10 | 7.333 | 0 | 0 | 0 | 10 | 0.895 | high_observed_zero | validation_required |
| Fécamp | 2024-05 | 2024-11 | 0.25 | 0 | 0 | 0 | 0 | 0.45 | low_coverage | insufficient_steady_coverage |
| Kincardine | 2021-10 | 2022-04 | 2.833 | 0 | 0 | 0 | 0 | 0.784 | low_signal_ambiguous | validation_required |
| Norther | 2019-06 | 2019-12 | 5.167 | 0 | 0 | 0 | 4 | 0.861 | high_observed_zero | validation_required |
| Northwester 2 | 2020-05 | 2020-11 | 4.25 | 0 | 0 | 0 | 0 | 0.838 | high_observed_zero | validation_required |
| Northwind | 2014-06 | 2014-12 | 10.083 | 0 | 0 | 0 | 0 | 0.914 | high_observed_zero | validation_required |
| Rampion | 2018-11 | 2019-05 | 5.75 | 0 | 0 | 0 | 13 | 0.872 | high_observed_zero | validation_required |
| Saint-Brieuc | 2024-05 | 2024-11 | 0.25 | 0 | 0 | 0 | 2 | 0.45 | low_coverage | insufficient_steady_coverage |
| Saint-Nazaire | 2022-11 | 2023-05 | 1.75 | 0 | 0 | 0 | 0 | 0.711 | low_signal_ambiguous | validation_required |
| Teesside | 2014-04 | 2014-10 | 10.25 | 0 | 0 | 0 | 28 | 0.915 | high_observed_zero | validation_required |
| Walney Extension 3 | 2018-09 | 2019-03 | 5.917 | 0 | 0 | 0 | 6 | 0.875 | high_observed_zero | validation_required |
| Walney Extension 4 | 2018-09 | 2019-03 | 5.917 | 0 | 0 | 0 | 18 | 0.875 | high_observed_zero | validation_required |
| London Array | 2013-04 | 2013-10 | 11.25 | 1 | 0.089 | 0 | 310 | 0.922 | high_observed_signal | steady_operational_only |
| Lincs | 2013-09 | 2014-03 | 10.833 | 1 | 0.092 | 0 | 137 | 0.919 | high_observed_signal | steady_operational_only |
| Nobelwind | 2017-05 | 2017-11 | 7.25 | 1 | 0.138 | 1 | 4 | 0.894 | high_observed_signal | steady_operational_only |
| Galloper | 2018-04 | 2018-10 | 6.333 | 1 | 0.158 | 0 | 29 | 0.882 | high_observed_signal | steady_operational_only |
| Avedøre Holme | 2011-12 | 2012-06 | 12.583 | 2 | 0.159 | 0 | 2 | 0.929 | high_observed_signal | steady_operational_only |
| Walney 2 | 2012-06 | 2012-12 | 12.083 | 2 | 0.166 | 5 | 28 | 0.926 | high_observed_signal | commissioning_separate_module |
| Aberdeen Offshore Wind Farm | 2018-09 | 2019-03 | 5.917 | 1 | 0.169 | 0 | 2 | 0.875 | high_observed_signal | steady_operational_only |

## Top 20 Commissioning/Ramp-Up Intensities

| farm_id | farm_commissioning_start_month | farm_commissioning_end_month | commissioning_observed_years | commissioning_candidate_count | commissioning_intervention_intensity_per_farm_year | steady_candidate_count | steady_intervention_intensity_per_farm_year | coverage_share | recommended_simulator_use |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Vesterhav Syd | 2024-03 | 2024-03 | 0.5 | 149 | 298 | 58 | 139.2 | 0.5 | insufficient_steady_coverage |
| Vesterhav Nord | 2024-03 | 2024-03 | 0.5 | 141 | 282 | 51 | 122.4 | 0.5 | insufficient_steady_coverage |
| Arcadis Ost 1 | 2023-12 | 2023-12 | 0.5 | 75 | 150 | 182 | 273 | 0.56 | insufficient_steady_coverage |
| Wikinger | 2018-10 | 2018-10 | 0.5 | 27 | 54 | 752 | 128.914 | 0.874 | steady_operational_only |
| Ormonde | 2012-02 | 2012-02 | 0.5 | 22 | 44 | 3 | 0.242 | 0.928 | commissioning_separate_module |
| Gode Wind 3 | 2025-02 | 2025-02 | 0.083 | 3 | 36 | 0 |  | 0.091 | insufficient_steady_coverage |
| EnBW Windpark Baltic 1 | 2011-05 | 2011-05 | 0.5 | 15 | 30 | 247 | 18.759 | 0.932 | steady_operational_only |
| Horns Rev III | 2019-08 | 2019-08 | 0.5 | 14 | 28 | 440 | 88 | 0.857 | steady_operational_only |
| Sandbank | 2017-01 | 2017-01 | 0.5 | 14 | 28 | 289 | 38.11 | 0.898 | steady_operational_only |
| Arkona-Becken Südost | 2019-01 | 2019-01 | 0.5 | 9 | 18 | 398 | 71.284 | 0.869 | steady_operational_only |
| Rodsand II | 2010-10 | 2010-10 | 0.5 | 8 | 16 | 649 | 47.2 | 0.934 | steady_operational_only |
| Horns Rev II | 2010-01 | 2010-01 | 0.5 | 7 | 14 | 532 | 36.69 | 0.938 | steady_operational_only |
| Merkur Offshore | 2019-06 | 2019-06 | 0.5 | 7 | 14 | 459 | 88.839 | 0.861 | steady_operational_only |
| Alpha Ventus | 2010-04 | 2010-04 | 0.5 | 6 | 12 | 296 | 20.772 | 0.937 | steady_operational_only |
| Veja Mate | 2017-05 | 2017-05 | 0.5 | 5 | 10 | 58 | 8 | 0.894 | steady_operational_only |
| Walney 2 | 2012-06 | 2012-06 | 0.5 | 5 | 10 | 2 | 0.166 | 0.926 | commissioning_separate_module |
| Hornsea Project 1 | 2019-12 | 2019-12 | 0.5 | 4 | 8 | 29 | 6.214 | 0.849 | steady_operational_only |
| Kaskasi | 2023-03 | 2023-03 | 0.5 | 4 | 8 | 201 | 141.882 | 0.676 | validation_required |
| Kriegers Flak | 2021-09 | 2021-09 | 0.5 | 4 | 8 | 1042 | 357.257 | 0.788 | validation_required |
| Albatros | 2020-01 | 2020-01 | 0.5 | 3 | 6 | 54 | 11.782 | 0.847 | steady_operational_only |

## Coverage And Denominator Checks

Observed farm-years now vary by commissioning-derived lifecycle phase. Commissioning/ramp-up months are not part of the steady operational denominator used for provisional simulator demand support.

All non-pre-operational observed farm-years:

| metric | value |
| --- | --- |
| count | 113 |
| min | 0 |
| p05 | 0.75 |
| p25 | 5.25 |
| median | 9.25 |
| p75 | 13.667 |
| p90 | 15 |
| p95 | 15 |
| max | 15 |
| total | 986.5 |

Steady operational observed farm-years:

| metric | value |
| --- | --- |
| count | 113 |
| min | 0 |
| p05 | 0.25 |
| p25 | 4.75 |
| median | 8.75 |
| p75 | 13.167 |
| p90 | 15 |
| p95 | 15 |
| max | 15 |
| total | 941.917 |

Coverage share distribution after phase-aware operational-window filtering:

| metric | value |
| --- | --- |
| count | 113 |
| min | 0 |
| p05 | 0.45 |
| p25 | 0.851 |
| median | 0.902 |
| p75 | 0.932 |
| p90 | 0.938 |
| p95 | 0.938 |
| max | 0.938 |
| total | 93.979 |

Operational-window unknown farms:

_None._

## Event Count Distributions

Steady operational candidate counts:

| metric | value |
| --- | --- |
| count | 113 |
| min | 0 |
| p05 | 0 |
| p25 | 2 |
| median | 12 |
| p75 | 135 |
| p90 | 422.8 |
| p95 | 554 |
| max | 1042 |
| total | 13008 |

Commissioning/ramp-up candidate counts:

| metric | value |
| --- | --- |
| count | 113 |
| min | 0 |
| p05 | 0 |
| p25 | 0 |
| median | 0 |
| p75 | 0 |
| p90 | 6.8 |
| p95 | 14.4 |
| max | 149 |
| total | 537 |

Duplicate-adjusted candidate counts:

| metric | value |
| --- | --- |
| count | 113 |
| min | 0 |
| p05 | 0 |
| p25 | 2 |
| median | 15 |
| p75 | 128.5 |
| p90 | 397.1 |
| p95 | 556.7 |
| max | 1046 |
| total | 13021.333 |

Duplicate adjustment deltas:

| metric | value |
| --- | --- |
| count | 113 |
| min | 0 |
| p05 | 0 |
| p25 | 0 |
| median | 0 |
| p75 | 0.5 |
| p90 | 10.2 |
| p95 | 30.433 |
| max | 100.5 |
| total | 523.667 |

## Duplicate Adjustment Impact

The duplicate adjustment is non-destructive: raw counts are preserved and duplicate-group adjusted counts are reported separately. Fractional adjusted counts occur on 18 farms because duplicate groups can be split across multiple farms. No negative adjustment deltas were found.

Largest duplicate adjustment deltas:

| farm_id | candidate_intervention_count | duplicate_group_adjusted_candidate_count | duplicate_adjustment_delta |
| --- | --- | --- | --- |
| Merkur Offshore | 466 | 365.5 | 100.5 |
| Alpha Ventus | 302 | 202 | 100 |
| Borkum Riffgrund 1 | 171 | 117.5 | 53.5 |
| Borkum Riffgrund 2 | 182 | 128.5 | 53.5 |
| Kaskasi | 205 | 160.167 | 44.833 |
| Amrumbank West | 362 | 321.167 | 40.833 |
| Nordsee Ost | 315 | 291.5 | 23.5 |
| Trianel Windpark Borkum 1 | 123 | 105.5 | 17.5 |
| Trianel Windpark Borkum 2 | 140 | 123 | 17 |
| Lynn | 62 | 47 | 15 |

## Confidence Classes

| confidence_class | farm_count |
| --- | --- |
| high_observed_signal | 85 |
| low_coverage | 16 |
| high_observed_zero | 9 |
| low_signal_ambiguous | 3 |

## Recommended Simulator Use

| recommended_simulator_use | farm_count |
| --- | --- |
| steady_operational_only | 83 |
| validation_required | 16 |
| insufficient_steady_coverage | 12 |
| commissioning_separate_module | 2 |

## Ramp-Up Sensitivity Checks

| scenario | ramp_up_months | commissioning_observed_years_total | steady_observed_years_total | commissioning_candidate_count_total | steady_candidate_count_total | steady_mean_rate_per_farm_year | steady_median_rate_per_farm_year | steady_p95_rate_per_farm_year | steady_max_rate_per_farm_year | top_farm |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| ramp_up_0_months | 0 | 0 | 986.5 | 0 | 13545 | 21.799 | 1.621 | 95.972 | 306.146 | Kriegers Flak |
| ramp_up_6_months | 6 | 44.583 | 941.917 | 537 | 13008 | 22.189 | 1.55 | 110.654 | 357.257 | Kriegers Flak |
| ramp_up_12_months | 12 | 88.333 | 898.167 | 943 | 12602 | 21.23 | 1.526 | 93.097 | 424.552 | Kriegers Flak |

Tightening steady long dwell from 120 to 240 minutes removes 1420 long-dwell candidates (12.7% of the current steady long-dwell numerator). Ramp-up sensitivity should be reviewed before any RQ12 demand multiplier uses the steady operational field.

## Red Flags

### Implausibly High Steady Operational Rates

Farms above 50 steady candidate interventions per observed steady farm-year need manual review before use as absolute simulator demand. They may represent short-denominator effects, residual early-life activity, duplicate-proximal activity, or repeated vessel behavior rather than mature maintenance demand.

| farm_id | farm_commissioning_end_month | steady_operational_start_month | steady_observed_years | steady_candidate_count | steady_intervention_intensity_per_farm_year | steady_long_dwell_count | coverage_share | confidence_class | recommended_simulator_use |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Anholt | 2013-09 | 2014-03 | 10.833 | 792 | 73.108 | 747 | 0.919 | high_observed_signal | steady_operational_only |
| Arcadis Ost 1 | 2023-12 | 2024-06 | 0.667 | 182 | 273 | 163 | 0.56 | low_coverage | insufficient_steady_coverage |
| Arkona-Becken Südost | 2019-01 | 2019-07 | 5.583 | 398 | 71.284 | 383 | 0.869 | high_observed_signal | steady_operational_only |
| Butendiek | 2015-08 | 2016-02 | 8.917 | 480 | 53.832 | 443 | 0.904 | high_observed_signal | steady_operational_only |
| Dan Tysk | 2015-04 | 2015-10 | 9.25 | 481 | 52 | 405 | 0.907 | high_observed_signal | steady_operational_only |
| EnBW Windpark Baltic 2 | 2015-10 | 2016-04 | 8.75 | 637 | 72.8 | 582 | 0.902 | high_observed_signal | steady_operational_only |
| Horns Rev III | 2019-08 | 2020-02 | 5 | 440 | 88 | 401 | 0.857 | high_observed_signal | steady_operational_only |
| Kaskasi | 2023-03 | 2023-09 | 1.417 | 201 | 141.882 | 170 | 0.676 | low_coverage | validation_required |
| Kriegers Flak | 2021-09 | 2022-03 | 2.917 | 1042 | 357.257 | 616 | 0.788 | low_coverage | validation_required |
| Merkur Offshore | 2019-06 | 2019-12 | 5.167 | 459 | 88.839 | 384 | 0.861 | high_observed_signal | steady_operational_only |
| Vesterhav Nord | 2024-03 | 2024-09 | 0.417 | 51 | 122.4 | 49 | 0.5 | low_coverage | insufficient_steady_coverage |
| Vesterhav Syd | 2024-03 | 2024-09 | 0.417 | 58 | 139.2 | 56 | 0.5 | low_coverage | insufficient_steady_coverage |
| Wikinger | 2018-10 | 2019-04 | 5.833 | 752 | 128.914 | 691 | 0.874 | high_observed_signal | steady_operational_only |

### High Steady Event Counts With Low Coverage

Using steady candidate count >= the 90th percentile (422.8) and coverage < 80%, these farms have high event evidence but weak observed-source denominator support.

| farm_id | steady_observed_years | steady_candidate_count | steady_intervention_intensity_per_farm_year | coverage_share | confidence_class | recommended_simulator_use |
| --- | --- | --- | --- | --- | --- | --- |
| Kriegers Flak | 2.917 | 1042 | 357.257 | 0.788 | low_coverage | validation_required |

### Insufficient Steady Operational Coverage

These farms have less than one observed steady operational farm-year. Their commissioning and pre-operational evidence is preserved separately and should not be used as generic mature-operational maintenance signal.

| farm_id | steady_operational_start_month | steady_manifest_months | steady_observed_months | steady_candidate_count | commissioning_candidate_count | pre_operational_candidate_count | recommended_simulator_use |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Arcadis Ost 1 | 2024-06 | 19 | 8 | 182 | 75 | 50 | insufficient_steady_coverage |
| Baltic Eagle | 2026-01 | 0 | 0 | 0 | 0 | 1235 | insufficient_steady_coverage |
| Fécamp | 2024-11 | 14 | 3 | 0 | 0 | 0 | insufficient_steady_coverage |
| Gode Wind 3 | 2025-08 | 5 | 0 | 0 | 3 | 324 | insufficient_steady_coverage |
| Hollandse Kust Noord | 2024-06 | 19 | 8 | 7 | 0 | 7 | insufficient_steady_coverage |
| Hollandse Kust Zuid | 2025-06 | 7 | 0 | 0 | 0 | 43 | insufficient_steady_coverage |
| Moray West | 2025-10 | 3 | 0 | 0 | 0 | 10 | insufficient_steady_coverage |
| Neart na Gaoithe | 2026-01 | 0 | 0 | 0 | 0 | 13 | insufficient_steady_coverage |
| Saint-Brieuc | 2024-11 | 14 | 3 | 0 | 0 | 2 | insufficient_steady_coverage |
| Seagreen | 2024-04 | 21 | 10 | 8 | 0 | 2 | insufficient_steady_coverage |
| Vesterhav Nord | 2024-09 | 16 | 5 | 51 | 141 | 30 | insufficient_steady_coverage |
| Vesterhav Syd | 2024-09 | 16 | 5 | 58 | 149 | 32 | insufficient_steady_coverage |

### Commissioning-Driven Activity

These farms have more commissioning/ramp-up candidates than steady candidates. That pattern should feed a separate commissioning-demand module or remain excluded from generic mature-operational demand multipliers.

| farm_id | commissioning_observed_years | commissioning_candidate_count | commissioning_intervention_intensity_per_farm_year | steady_observed_years | steady_candidate_count | steady_intervention_intensity_per_farm_year | recommended_simulator_use |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Ormonde | 0.5 | 22 | 44 | 12.417 | 3 | 0.242 | commissioning_separate_module |
| Vesterhav Nord | 0.5 | 141 | 282 | 0.417 | 51 | 122.4 | insufficient_steady_coverage |
| Vesterhav Syd | 0.5 | 149 | 298 | 0.417 | 58 | 139.2 | insufficient_steady_coverage |

### High Coverage With Zero Or Near-Zero Steady Signal

These farms have coverage >= 90% and <= 0.2 steady candidate interventions per observed steady farm-year. They should not be interpreted as having no maintenance demand without external validation.

| farm_id | steady_observed_years | steady_candidate_count | steady_intervention_intensity_per_farm_year | coverage_share | confidence_class | recommended_simulator_use |
| --- | --- | --- | --- | --- | --- | --- |
| Avedøre Holme | 12.583 | 2 | 0.159 | 0.929 | high_observed_signal | steady_operational_only |
| Lincs | 10.833 | 1 | 0.092 | 0.919 | high_observed_signal | steady_operational_only |
| London Array | 11.25 | 1 | 0.089 | 0.922 | high_observed_signal | steady_operational_only |
| Northwind | 10.083 | 0 | 0 | 0.914 | high_observed_zero | validation_required |
| Teesside | 10.25 | 0 | 0 | 0.915 | high_observed_zero | validation_required |
| Thornton Bank - phase II and III | 10.833 | 2 | 0.185 | 0.919 | high_observed_signal | steady_operational_only |
| Walney 2 | 12.083 | 2 | 0.166 | 0.926 | high_observed_signal | commissioning_separate_module |

## Simulator-Use Assessment

The phase-separated output is more suitable as a farm-level maintenance intervention intensity screen and as a relative evidence layer for RQ12 simulator inputs. Only `steady_intervention_intensity_per_farm_year` should be considered for a generic mature-operational demand multiplier, and even then it remains provisional until external SCADA/fault/work-order validation. Commissioning/ramp-up activity should be kept separate.
