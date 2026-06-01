# RQ9 Farm-Level Maintenance Intervention Intensity Sanity Audit

This audit reviews the committed farm-level maintenance intervention intensity outputs for simulator readiness. It is not failure rate analysis. A vessel visit is not automatically a failure, and Tier A/B dwell evidence remains candidate intervention evidence until SCADA, fault log, work-order, or equivalent validation is linked.

## Scope

- Farm-level only; turbine-level intervention intensity is not implemented here.
- Inputs audited: `Data/Processed/analysis/rq9_intervention_intensity/farm_intervention_intensity.csv`, `reports/rq9_intervention_intensity/validation_summary.csv`, `reports/rq9_intervention_intensity/methodology_report.md`.
- Stricter long-dwell sensitivity additionally reads `Data/Processed/ais_dwell_backfill/cross_farm_dwell_weather_features.parquet` read-only because the farm output stores only the current long-dwell threshold result. No AIS extraction or metocean extraction was rerun.
- Current long-dwell threshold: 120 minutes.
- Stricter long-dwell threshold used for this audit: 240 minutes.

## Key Totals

| Metric | Value |
| --- | --- |
| Farm rows | 113 |
| Observed farm-years | 1695.000 |
| Raw candidate interventions, Tier A + Tier B | 17565 |
| Tier A candidate visits | 15264 |
| Tier B candidate visits | 2301 |
| Tier B share of raw candidates | 13.1% |
| Current long-dwell count | 15227 |
| Stricter long-dwell count | 13337 |
| Duplicate-adjusted candidate total | 16895.833 |
| Duplicate adjustment delta | 669.167 (3.8% of raw candidates) |

## Top 20 Candidate Intensities

| farm_id | observed_years | coverage_share | candidate_intervention_count | candidate_interventions_per_observed_farm_year | duplicate_group_adjusted_candidate_count | duplicate_adjustment_delta | confidence_class |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Baltic Eagle | 15 | 0.938 | 1235 | 82.333 | 1235 | 0 | high_observed_signal |
| Kriegers Flak | 15 | 0.938 | 1186 | 79.067 | 1186 | 0 | high_observed_signal |
| Wikinger | 15 | 0.938 | 976 | 65.067 | 973 | 3 | high_observed_signal |
| Anholt | 15 | 0.938 | 880 | 58.667 | 880 | 0 | high_observed_signal |
| Rodsand II | 15 | 0.938 | 736 | 49.067 | 735 | 1 | high_observed_signal |
| EnBW Windpark Baltic 2 | 15 | 0.938 | 702 | 46.8 | 702 | 0 | high_observed_signal |
| Horns Rev I | 15 | 0.938 | 587 | 39.133 | 587 | 0 | high_observed_signal |
| Arkona-Becken Südost | 15 | 0.938 | 551 | 36.733 | 548 | 3 | high_observed_signal |
| Horns Rev III | 15 | 0.938 | 547 | 36.467 | 544.5 | 2.5 | high_observed_signal |
| Horns Rev II | 15 | 0.938 | 539 | 35.933 | 536.5 | 2.5 | high_observed_signal |
| Merkur Offshore | 15 | 0.938 | 537 | 35.8 | 411.5 | 125.5 | high_observed_signal |
| Dan Tysk | 15 | 0.938 | 489 | 32.6 | 489 | 0 | high_observed_signal |
| Butendiek | 15 | 0.938 | 486 | 32.4 | 486 | 0 | high_observed_signal |
| Nysted | 15 | 0.938 | 429 | 28.6 | 428 | 1 | high_observed_signal |
| Amrumbank West | 15 | 0.938 | 381 | 25.4 | 338 | 43 | high_observed_signal |
| Meerwind Sued/Ost | 15 | 0.938 | 368 | 24.533 | 368 | 0 | high_observed_signal |
| Gode Wind 3 | 15 | 0.938 | 327 | 21.8 | 322 | 5 | high_observed_signal |
| Gode Wind 1 and 2 | 15 | 0.938 | 326 | 21.733 | 321 | 5 | high_observed_signal |
| Nordsee Ost | 15 | 0.938 | 321 | 21.4 | 296.333 | 24.667 | high_observed_signal |
| EnBW Windpark Baltic 1 | 15 | 0.938 | 320 | 21.333 | 320 | 0 | high_observed_signal |

## Bottom 20 Candidate Intensities

| farm_id | observed_years | coverage_share | candidate_intervention_count | candidate_interventions_per_observed_farm_year | duplicate_group_adjusted_candidate_count | duplicate_adjustment_delta | confidence_class |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Fécamp | 15 | 0.938 | 0 | 0 | 0 | 0 | high_observed_zero |
| Kincardine | 15 | 0.938 | 0 | 0 | 0 | 0 | high_observed_zero |
| Northwester 2 | 15 | 0.938 | 0 | 0 | 0 | 0 | high_observed_zero |
| Northwind | 15 | 0.938 | 0 | 0 | 0 | 0 | high_observed_zero |
| Saint-Nazaire | 15 | 0.938 | 0 | 0 | 0 | 0 | high_observed_zero |
| Mermaid | 15 | 0.938 | 1 | 0.067 | 1 | 0 | high_observed_signal |
| OWF Luchterduinen | 15 | 0.938 | 2 | 0.133 | 1 | 1 | high_observed_signal |
| Saint-Brieuc | 15 | 0.938 | 2 | 0.133 | 2 | 0 | high_observed_signal |
| SeaStar | 15 | 0.938 | 2 | 0.133 | 2 | 0 | high_observed_signal |
| West of Duddon Sands | 15 | 0.938 | 2 | 0.133 | 2 | 0 | high_observed_signal |
| Aberdeen Offshore Wind Farm | 15 | 0.938 | 3 | 0.2 | 3 | 0 | high_observed_signal |
| Blyth Demo Phase 1 | 15 | 0.938 | 3 | 0.2 | 3 | 0 | high_observed_signal |
| Borssele Kavel I and II | 15 | 0.938 | 3 | 0.2 | 2.5 | 0.5 | high_observed_signal |
| Avedøre Holme | 15 | 0.938 | 4 | 0.267 | 4 | 0 | high_observed_signal |
| Beatrice Offshore Wind Farm | 15 | 0.938 | 4 | 0.267 | 4 | 0 | high_observed_signal |
| Borssele Kavel III and IV | 15 | 0.938 | 4 | 0.267 | 3.5 | 0.5 | high_observed_signal |
| East Anglia One | 15 | 0.938 | 4 | 0.267 | 4 | 0 | high_observed_signal |
| Norther | 15 | 0.938 | 4 | 0.267 | 2.333 | 1.667 | high_observed_signal |
| Barrow | 15 | 0.938 | 6 | 0.4 | 6 | 0 | high_observed_signal |
| Belwind phase 1 | 15 | 0.938 | 6 | 0.4 | 4.5 | 1.5 | high_observed_signal |

## Coverage And Denominator Checks

Observed farm-years are uniform across all farms. That is a red flag for simulator use because commissioning and operational windows are not yet applied.

| metric | value |
| --- | --- |
| count | 113 |
| min | 15 |
| p05 | 15 |
| p25 | 15 |
| median | 15 |
| p75 | 15 |
| p90 | 15 |
| p95 | 15 |
| max | 15 |
| total | 1695 |

Coverage share is also uniform at 0.9375. The audit found 5 farms where skipped-missing-source months are higher than the common 12-month gap because some farm-months have both observed and skipped statuses in the manifest. Those months are counted as observed when a success or success_no_ais_in_bbox status is present, which matches the methodology but should be called out for denominator transparency.

| farm_id | observed_months | success_months | success_no_ais_in_bbox_months | skipped_missing_source_months | coverage_share | candidate_intervention_count |
| --- | --- | --- | --- | --- | --- | --- |
| Arkona-Becken Südost | 180 | 126 | 54 | 17 | 0.938 | 551 |
| Baltic Eagle | 180 | 127 | 53 | 17 | 0.938 | 1235 |
| EnBW Windpark Baltic 1 | 180 | 85 | 95 | 17 | 0.938 | 320 |
| EnBW Windpark Baltic 2 | 180 | 123 | 57 | 17 | 0.938 | 702 |
| Wikinger | 180 | 128 | 52 | 17 | 0.938 | 976 |

## Event Count Distributions

Raw Tier A/B candidate counts:

| metric | value |
| --- | --- |
| count | 113 |
| min | 0 |
| p05 | 1.6 |
| p25 | 9 |
| median | 34 |
| p75 | 212 |
| p90 | 488.4 |
| p95 | 633 |
| max | 1235 |
| total | 17565 |

Duplicate-adjusted candidate counts:

| metric | value |
| --- | --- |
| count | 113 |
| min | 0 |
| p05 | 1 |
| p25 | 9 |
| median | 33 |
| p75 | 166 |
| p90 | 474.4 |
| p95 | 633 |
| max | 1235 |
| total | 16895.833 |

Duplicate adjustment deltas:

| metric | value |
| --- | --- |
| count | 113 |
| min | 0 |
| p05 | 0 |
| p25 | 0 |
| median | 0 |
| p75 | 2 |
| p90 | 12.133 |
| p95 | 32 |
| max | 125.5 |
| total | 669.167 |

## Duplicate Adjustment Impact

The duplicate adjustment is non-destructive: raw counts are preserved and duplicate-group adjusted counts are reported separately. Fractional adjusted counts occur on 25 farms because duplicate groups can be split across multiple farms. No negative adjustment deltas were found.

Largest duplicate adjustment deltas:

| farm_id | candidate_intervention_count | duplicate_group_adjusted_candidate_count | duplicate_adjustment_delta |
| --- | --- | --- | --- |
| Merkur Offshore | 537 | 411.5 | 125.5 |
| Alpha Ventus | 302 | 177 | 125 |
| Borkum Riffgrund 1 | 174 | 117.5 | 56.5 |
| Borkum Riffgrund 2 | 212 | 155.5 | 56.5 |
| Kaskasi | 254 | 205.5 | 48.5 |
| Amrumbank West | 381 | 338 | 43 |
| Nordsee Ost | 321 | 296.333 | 24.667 |
| Trianel Windpark Borkum 1 | 125 | 104 | 21 |
| Trianel Windpark Borkum 2 | 162 | 141.5 | 20.5 |
| Lynn | 62 | 43.833 | 18.167 |

## Confidence Classes

| confidence_class | farm_count |
| --- | --- |
| high_observed_signal | 108 |
| high_observed_zero | 5 |

The current confidence classes are too coarse for simulator gating: only two classes appear, and all farms inherit the same denominator coverage. Before treating this as a production simulator input, confidence should distinguish at least operational-window availability, duplicate-adjustment burden, zero-signal farms, and high-rate outliers.

## Sensitivity Checks

| scenario | total_events | mean_rate_per_farm_year | median_rate_per_farm_year | p95_rate_per_farm_year | max_rate_per_farm_year | top_farm |
| --- | --- | --- | --- | --- | --- | --- |
| tier_a_only | 15264 | 9.005 | 1.667 | 40.187 | 74.133 | Baltic Eagle |
| tier_a_plus_tier_b | 17565 | 10.363 | 2.267 | 42.2 | 82.333 | Baltic Eagle |
| long_dwell_120_min | 15227 | 8.983 | 1.867 | 39.16 | 77.467 | Baltic Eagle |
| long_dwell_240_min | 13337 | 7.868 | 1.467 | 36.147 | 73.133 | Baltic Eagle |

Switching from Tier A + Tier B to Tier A only removes 2301 candidate visits (13.1% of the raw numerator). Tightening long dwell from 120 to 240 minutes removes 1890 long-dwell candidates (12.4% of the current long-dwell numerator). The top of the distribution remains high under the stricter threshold, so the highest-intensity farms are not solely an artifact of the 120-minute threshold.

## Red Flags

### Implausibly High Raw Rates

Farms above 50 raw candidate interventions per observed farm-year need manual review before use as absolute simulator demand. They may represent intense operational activity, construction/commissioning activity, duplicate-proximal activity, or repeated vessel behavior rather than maintenance demand.

| farm_id | candidate_intervention_count | candidate_interventions_per_observed_farm_year | long_dwell_count | long_dwell_interventions_per_observed_farm_year | coverage_share | confidence_class |
| --- | --- | --- | --- | --- | --- | --- |
| Anholt | 880 | 58.667 | 828 | 55.2 | 0.938 | high_observed_signal |
| Baltic Eagle | 1235 | 82.333 | 1162 | 77.467 | 0.938 | high_observed_signal |
| Kriegers Flak | 1186 | 79.067 | 748 | 49.867 | 0.938 | high_observed_signal |
| Wikinger | 976 | 65.067 | 895 | 59.667 | 0.938 | high_observed_signal |

### High Event Counts With Low Coverage

Using candidate count >= the 90th percentile (488.4) and coverage < 80%, no farms meet this specific red-flag condition. The absence of hits is driven by the uniform coverage value, not by farm-specific operational windows.

_None._

### High Coverage With Zero Or Near-Zero Signal

These farms have coverage >= 90% and <= 0.2 raw candidate interventions per observed farm-year. The five zero-signal farms are especially sensitive to commissioning-window and coverage assumptions.

| farm_id | candidate_intervention_count | candidate_interventions_per_observed_farm_year | coverage_share | confidence_class |
| --- | --- | --- | --- | --- |
| Aberdeen Offshore Wind Farm | 3 | 0.2 | 0.938 | high_observed_signal |
| Blyth Demo Phase 1 | 3 | 0.2 | 0.938 | high_observed_signal |
| Borssele Kavel I and II | 3 | 0.2 | 0.938 | high_observed_signal |
| Fécamp | 0 | 0 | 0.938 | high_observed_zero |
| Kincardine | 0 | 0 | 0.938 | high_observed_zero |
| Mermaid | 1 | 0.067 | 0.938 | high_observed_signal |
| Northwester 2 | 0 | 0 | 0.938 | high_observed_zero |
| Northwind | 0 | 0 | 0.938 | high_observed_zero |
| OWF Luchterduinen | 2 | 0.133 | 0.938 | high_observed_signal |
| Saint-Brieuc | 2 | 0.133 | 0.938 | high_observed_signal |
| Saint-Nazaire | 0 | 0 | 0.938 | high_observed_zero |
| SeaStar | 2 | 0.133 | 0.938 | high_observed_signal |
| West of Duddon Sands | 2 | 0.133 | 0.938 | high_observed_signal |

## Simulator-Use Assessment

The output is useful as a farm-level maintenance intervention intensity screen and as a relative evidence layer for RQ12 simulator inputs. It is not ready to be interpreted as confirmed fault-driven maintenance demand or as an absolute reliability process. Before turbine-level expansion or simulator calibration, the next correction should add farm operational-window handling where available and refine confidence classes so zero-signal and high-rate farms are not treated with the same broad confidence label.
