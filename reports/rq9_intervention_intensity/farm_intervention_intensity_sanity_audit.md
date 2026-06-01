# RQ9 Farm-Level Maintenance Intervention Intensity Sanity Audit

This audit reviews the farm-level maintenance intervention intensity outputs for simulator readiness. It is not a confirmed fault-driven demand estimate. A vessel visit is not automatically a failure, and Tier A/B dwell evidence remains candidate intervention evidence until SCADA, fault log, work-order, or equivalent validation is linked.

## Scope

- Farm-level only; turbine-level intervention intensity is not implemented here.
- Inputs audited: `/Volumes/Extreme SSD/01_ACTIVE_PROJECTS/O&M_Data_Acquisition_Accessability&Validation/Data/Processed/analysis/rq9_intervention_intensity/farm_intervention_intensity.csv`, `/Volumes/Extreme SSD/01_ACTIVE_PROJECTS/O&M_Data_Acquisition_Accessability&Validation/reports/rq9_intervention_intensity/validation_summary.csv`, `/Volumes/Extreme SSD/01_ACTIVE_PROJECTS/O&M_Data_Acquisition_Accessability&Validation/reports/rq9_intervention_intensity/methodology_report.md`.
- Stricter long-dwell sensitivity reads the existing dwell feature table in memory through the RQ9 builder. No AIS extraction or metocean extraction was rerun.
- Current long-dwell threshold: 120 minutes.
- Stricter long-dwell threshold used for this audit: 240 minutes.

## Key Totals

| Metric | Value |
| --- | --- |
| Farm rows | 113 |
| Observed farm-years | 986.500 |
| Observed farm-years min / median / max | 0.000 / 9.250 / 15.000 |
| Raw candidate interventions, Tier A + Tier B | 13545 |
| Pre-operational candidate rows excluded | 4020 |
| Tier A candidate visits | 11972 |
| Tier B candidate visits | 1573 |
| Tier B share of raw candidates | 11.6% |
| Current long-dwell count | 11656 |
| Stricter long-dwell count | 10167 |
| Duplicate-adjusted candidate total | 13021.333 |
| Duplicate adjustment delta | 523.667 (3.9% of raw candidates) |

## Top 20 Candidate Intensities

| farm_id | operational_start_month | observed_years | coverage_share | candidate_intervention_count | candidate_interventions_per_observed_farm_year | pre_operational_candidate_count | duplicate_group_adjusted_candidate_count | duplicate_adjustment_delta | confidence_class |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Kriegers Flak | 2021-09 | 3.417 | 0.788 | 1046 | 306.146 | 140 | 1046 | 0 | low_coverage |
| Vesterhav Syd | 2024-03 | 0.917 | 0.5 | 207 | 225.818 | 32 | 207 | 0 | low_coverage |
| Arcadis Ost 1 | 2023-12 | 1.167 | 0.56 | 257 | 220.286 | 50 | 257 | 0 | low_coverage |
| Vesterhav Nord | 2024-03 | 0.917 | 0.5 | 192 | 209.455 | 30 | 192 | 0 | low_coverage |
| Wikinger | 2018-10 | 6.333 | 0.874 | 779 | 123 | 197 | 777 | 2 | high_observed_signal |
| Kaskasi | 2023-03 | 1.917 | 0.676 | 205 | 106.957 | 49 | 160.167 | 44.833 | low_coverage |
| Horns Rev III | 2019-08 | 5.5 | 0.857 | 454 | 82.545 | 93 | 451.5 | 2.5 | high_observed_signal |
| Merkur Offshore | 2019-06 | 5.667 | 0.861 | 466 | 82.235 | 71 | 365.5 | 100.5 | high_observed_signal |
| Anholt | 2013-09 | 11.333 | 0.919 | 792 | 69.882 | 88 | 792 | 0 | high_observed_signal |
| EnBW Windpark Baltic 2 | 2015-10 | 9.25 | 0.902 | 637 | 68.865 | 65 | 637 | 0 | high_observed_signal |
| Arkona-Becken Südost | 2019-01 | 6.083 | 0.869 | 407 | 66.904 | 144 | 405 | 2 | high_observed_signal |
| Butendiek | 2015-08 | 9.417 | 0.904 | 480 | 50.973 | 6 | 480 | 0 | high_observed_signal |
| Dan Tysk | 2015-04 | 9.75 | 0.907 | 481 | 49.333 | 8 | 481 | 0 | high_observed_signal |
| Rodsand II | 2010-10 | 14.25 | 0.934 | 657 | 46.105 | 79 | 656 | 1 | high_observed_signal |
| Amrumbank West | 2015-10 | 9.25 | 0.902 | 362 | 39.135 | 19 | 321.167 | 40.833 | high_observed_signal |
| Horns Rev I | 2002-12 | 15 | 0.938 | 587 | 39.133 | 0 | 587 | 0 | high_observed_signal |
| Gode Wind 1 and 2 | 2016-09 | 8.417 | 0.902 | 324 | 38.495 | 2 | 324 | 0 | high_observed_signal |
| Sandbank | 2017-01 | 8.083 | 0.898 | 303 | 37.485 | 2 | 303 | 0 | high_observed_signal |
| Gode Wind 3 | 2025-02 | 0.083 | 0.091 | 3 | 36 | 324 | 3 | 0 | low_coverage |
| Horns Rev II | 2010-01 | 15 | 0.938 | 539 | 35.933 | 0 | 536.5 | 2.5 | high_observed_signal |

## Bottom 20 Candidate Intensities

| farm_id | operational_start_month | observed_years | coverage_share | candidate_intervention_count | candidate_interventions_per_observed_farm_year | pre_operational_candidate_count | duplicate_group_adjusted_candidate_count | duplicate_adjustment_delta | confidence_class |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Blyth Demo Phase 1 | 2018-06 | 6.667 | 0.879 | 0 | 0 | 3 | 0 | 0 | high_observed_zero |
| Burbo Bank Extension | 2017-04 | 7.833 | 0.895 | 0 | 0 | 10 | 0 | 0 | high_observed_zero |
| Fécamp | 2024-05 | 0.75 | 0.45 | 0 | 0 | 0 | 0 | 0 | low_coverage |
| Hollandse Kust Zuid | 2024-12 | 0.25 | 0.231 | 0 | 0 | 43 | 0 | 0 | low_coverage |
| Kincardine | 2021-10 | 3.333 | 0.784 | 0 | 0 | 0 | 0 | 0 | low_signal_ambiguous |
| Norther | 2019-06 | 5.667 | 0.861 | 0 | 0 | 4 | 0 | 0 | high_observed_zero |
| Northwester 2 | 2020-05 | 4.75 | 0.838 | 0 | 0 | 0 | 0 | 0 | high_observed_zero |
| Northwind | 2014-06 | 10.583 | 0.914 | 0 | 0 | 0 | 0 | 0 | high_observed_zero |
| Rampion | 2018-11 | 6.25 | 0.872 | 0 | 0 | 13 | 0 | 0 | high_observed_zero |
| Saint-Brieuc | 2024-05 | 0.75 | 0.45 | 0 | 0 | 2 | 0 | 0 | low_coverage |
| Saint-Nazaire | 2022-11 | 2.25 | 0.711 | 0 | 0 | 0 | 0 | 0 | low_signal_ambiguous |
| Teesside | 2014-04 | 10.75 | 0.915 | 0 | 0 | 28 | 0 | 0 | high_observed_zero |
| Walney Extension 3 | 2018-09 | 6.417 | 0.875 | 0 | 0 | 6 | 0 | 0 | high_observed_zero |
| Walney Extension 4 | 2018-09 | 6.417 | 0.875 | 0 | 0 | 18 | 0 | 0 | high_observed_zero |
| London Array | 2013-04 | 11.75 | 0.922 | 1 | 0.085 | 310 | 1 | 0 | high_observed_signal |
| Lincs | 2013-09 | 11.333 | 0.919 | 1 | 0.088 | 137 | 1 | 0 | high_observed_signal |
| Galloper | 2018-04 | 6.833 | 0.882 | 1 | 0.146 | 29 | 0.5 | 0.5 | high_observed_signal |
| Avedøre Holme | 2011-12 | 13.083 | 0.929 | 2 | 0.153 | 2 | 2 | 0 | high_observed_signal |
| Aberdeen Offshore Wind Farm | 2018-09 | 6.417 | 0.875 | 1 | 0.156 | 2 | 1 | 0 | high_observed_signal |
| Rentel | 2018-12 | 6.167 | 0.871 | 1 | 0.162 | 7 | 1 | 0 | high_observed_signal |

## Coverage And Denominator Checks

Observed farm-years now vary by farm operational start month. This corrects the v1 global-window issue where every farm had the same 15.0 observed farm-years.

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

Coverage share distribution after operational-window filtering:

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

Raw Tier A/B candidate counts:

| metric | value |
| --- | --- |
| count | 113 |
| min | 0 |
| p05 | 0 |
| p25 | 2 |
| median | 15 |
| p75 | 140 |
| p90 | 424.6 |
| p95 | 558.2 |
| max | 1046 |
| total | 13545 |

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

## Sensitivity Checks

| scenario | total_events | mean_rate_per_farm_year | median_rate_per_farm_year | p95_rate_per_farm_year | max_rate_per_farm_year | top_farm |
| --- | --- | --- | --- | --- | --- | --- |
| tier_a_only | 11972 | 18.934 | 1.441 | 93.637 | 225.818 | Vesterhav Syd |
| tier_a_plus_tier_b | 13545 | 21.799 | 1.621 | 95.972 | 306.146 | Kriegers Flak |
| long_dwell_120_min | 11656 | 18.563 | 1.473 | 83.885 | 216 | Vesterhav Syd |
| long_dwell_240_min | 10167 | 16.039 | 1.212 | 75.198 | 195.273 | Vesterhav Syd |

Switching from Tier A + Tier B to Tier A only removes 1573 candidate visits (11.6% of the raw numerator). Tightening long dwell from 120 to 240 minutes removes 1489 long-dwell candidates (12.8% of the current long-dwell numerator).

## Red Flags

### Implausibly High Raw Rates

Farms above 50 raw candidate interventions per observed farm-year need manual review before use as absolute simulator demand. They may represent intense operational activity, commissioning-period activity, duplicate-proximal activity, or repeated vessel behavior rather than maintenance demand.

| farm_id | operational_start_month | observed_years | candidate_intervention_count | candidate_interventions_per_observed_farm_year | long_dwell_count | long_dwell_interventions_per_observed_farm_year | coverage_share | confidence_class |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Anholt | 2013-09 | 11.333 | 792 | 69.882 | 747 | 65.912 | 0.919 | high_observed_signal |
| Arcadis Ost 1 | 2023-12 | 1.167 | 257 | 220.286 | 237 | 203.143 | 0.56 | low_coverage |
| Arkona-Becken Südost | 2019-01 | 6.083 | 407 | 66.904 | 392 | 64.438 | 0.869 | high_observed_signal |
| Butendiek | 2015-08 | 9.417 | 480 | 50.973 | 443 | 47.044 | 0.904 | high_observed_signal |
| EnBW Windpark Baltic 2 | 2015-10 | 9.25 | 637 | 68.865 | 582 | 62.919 | 0.902 | high_observed_signal |
| Horns Rev III | 2019-08 | 5.5 | 454 | 82.545 | 415 | 75.455 | 0.857 | high_observed_signal |
| Kaskasi | 2023-03 | 1.917 | 205 | 106.957 | 174 | 90.783 | 0.676 | low_coverage |
| Kriegers Flak | 2021-09 | 3.417 | 1046 | 306.146 | 620 | 181.463 | 0.788 | low_coverage |
| Merkur Offshore | 2019-06 | 5.667 | 466 | 82.235 | 390 | 68.824 | 0.861 | high_observed_signal |
| Vesterhav Nord | 2024-03 | 0.917 | 192 | 209.455 | 175 | 190.909 | 0.5 | low_coverage |
| Vesterhav Syd | 2024-03 | 0.917 | 207 | 225.818 | 198 | 216 | 0.5 | low_coverage |
| Wikinger | 2018-10 | 6.333 | 779 | 123 | 716 | 113.053 | 0.874 | high_observed_signal |

### High Event Counts With Low Coverage

Using candidate count >= the 90th percentile (424.6) and coverage < 80%, these farms have high event evidence but weak observed-source denominator support.

| farm_id | operational_start_month | observed_years | candidate_intervention_count | candidate_interventions_per_observed_farm_year | coverage_share | confidence_class |
| --- | --- | --- | --- | --- | --- | --- |
| Kriegers Flak | 2021-09 | 3.417 | 1046 | 306.146 | 0.788 | low_coverage |

### No Observed Coverage After Operational Start

These farms have commissioning-derived operational months in the manifest, but none of those months have observed AIS source coverage. Their pre-operational candidates are preserved separately and should not be treated as operational maintenance signal.

| farm_id | operational_start_month | manifest_months | observed_months | candidate_intervention_count | pre_operational_candidate_count | confidence_class |
| --- | --- | --- | --- | --- | --- | --- |
| Baltic Eagle | 2025-07 | 6 | 0 | 0 | 1235 | low_coverage |
| Moray West | 2025-04 | 9 | 0 | 0 | 10 | low_coverage |
| Neart na Gaoithe | 2025-07 | 6 | 0 | 0 | 13 | low_coverage |

### High Coverage With Zero Or Near-Zero Signal

These farms have coverage >= 90% and <= 0.2 raw candidate interventions per observed farm-year. They should not be interpreted as having no maintenance demand without external validation.

| farm_id | operational_start_month | observed_years | candidate_intervention_count | candidate_interventions_per_observed_farm_year | coverage_share | confidence_class |
| --- | --- | --- | --- | --- | --- | --- |
| Avedøre Holme | 2011-12 | 13.083 | 2 | 0.153 | 0.929 | high_observed_signal |
| Lincs | 2013-09 | 11.333 | 1 | 0.088 | 0.919 | high_observed_signal |
| London Array | 2013-04 | 11.75 | 1 | 0.085 | 0.922 | high_observed_signal |
| Northwind | 2014-06 | 10.583 | 0 | 0 | 0.914 | high_observed_zero |
| Teesside | 2014-04 | 10.75 | 0 | 0 | 0.915 | high_observed_zero |
| Thornton Bank - phase II and III | 2013-09 | 11.333 | 2 | 0.176 | 0.919 | high_observed_signal |
| West of Duddon Sands | 2014-10 | 10.25 | 2 | 0.195 | 0.911 | high_observed_signal |

## Simulator-Use Assessment

The corrected output is more plausible as a farm-level maintenance intervention intensity screen and as a relative evidence layer for RQ12 simulator inputs. It still should not be used as a confirmed fault-driven process. Remaining guardrails are operational-window quality for newly commissioned farms, outlier review for very high rates, and external SCADA/fault/work-order validation before calibrating true fault demand.
