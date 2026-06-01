# Current Confidence v1 Validation Report

## Research Design

Hypotheses: NWS hourly `u/v` currents can be assigned at event scale for NWS-covered normal farm-years; current evidence coverage is source/domain dependent; current severity may later help explain short dwell, repeat attempt, and high-weather success signatures; current confidence should remain separate from wave confidence until Fusion v2.

Metrics: dwell and Tier A coverage, event-window sample counts, bracketing percentage, nearest time gap, current speed and direction distributions, current variability, confidence class distribution, missingness reasons, and overlap with Fusion v1 wave confidence classes.

## Executive Conclusion

Current Confidence v1 writes one NWS current candidate and one current confidence row for each dwell event. It uses only the accepted local NWS hourly true `uo/vo` archive and keeps missing/non-covered events explicit rather than silently dropping them.

- Candidate table: `Data/Processed/metocean/current_confidence_v1/current_event_candidates.parquet`
- Confidence table: `Data/Processed/metocean/current_confidence_v1/current_event_confidence.parquet`
- Input dwell rows: 92,660
- Candidate rows: 92,660
- Confidence rows: 92,660
- NWS-eligible events: 16,307
- Event-scale current events: 16,307
- Tier A valid-current events: 5,358 of 15,264

## Input Inventory

- Dwell rows: 92,660
- NWS current partitions: 125
- NWS current source rows: 76,886,304
- Manifest rows accepted: 125
- Fusion v1 wave confidence rows: 92,660
- Bathymetry rows: 6,642

## Coverage

- All dwell events: 92,660
- NWS eligible events: 16,307
- Valid current events: 16,307
- Tier A events: 15,264
- Tier A valid current events: 5,358

Top farms by event-scale current coverage:

| wind_farm | event_count |
| --- | --- |
| Horns_Rev_II | 1286 |
| Nordsee_Ost | 1210 |
| Horns_Rev_III | 1018 |
| Meerwind_Sued_Ost | 888 |
| Merkur_Offshore | 881 |
| Borkum_Riffgrund_2 | 816 |
| Trianel_Windpark_Borkum_1 | 813 |
| Borkum_Riffgrund_1 | 806 |
| Gode_Wind_1_and_2 | 796 |
| Amrumbank_West | 753 |
| Kaskasi | 732 |
| Alpha_Ventus | 673 |
| Dan_Tysk | 672 |
| Trianel_Windpark_Borkum_2 | 574 |
| Butendiek | 542 |
| ... | 28 more rows omitted |

Confidence by year:

| year | current_confidence_class | event_count |
| --- | --- | --- |
| 2010 | A_event_scale | 29 |
| 2010 | D_unsuitable | 1740 |
| 2011 | A_event_scale | 48 |
| 2011 | D_unsuitable | 1863 |
| 2012 | A_event_scale | 378 |
| 2012 | D_unsuitable | 5034 |
| 2016 | D_unsuitable | 290 |
| 2017 | A_event_scale | 230 |
| 2017 | D_unsuitable | 3733 |
| 2018 | A_event_scale | 398 |
| 2018 | D_unsuitable | 3829 |
| 2019 | A_event_scale | 303 |
| 2019 | D_unsuitable | 3499 |
| 2020 | A_event_scale | 149 |
| 2020 | D_unsuitable | 3421 |
| 2021 | A_event_scale | 406 |
| 2021 | D_unsuitable | 3475 |
| 2022 | A_event_scale | 422 |
| 2022 | D_unsuitable | 3730 |
| 2023 | A_event_scale | 207 |
| 2023 | D_unsuitable | 3459 |
| 2024 | A_event_scale | 13681 |
| 2024 | D_unsuitable | 35982 |
| 2025 | A_event_scale | 56 |
| 2025 | D_unsuitable | 6298 |

Confidence by dwell tier:

| dwell_tier | current_confidence_class | event_count |
| --- | --- | --- |
| Tier D | D_unsuitable | 63459 |
| Tier A | D_unsuitable | 9906 |
| Tier D | A_event_scale | 9286 |
| Tier A | A_event_scale | 5358 |
| Tier B | D_unsuitable | 1507 |
| Tier C | D_unsuitable | 1481 |
| Tier C | A_event_scale | 869 |
| Tier B | A_event_scale | 794 |

Farm-year confidence counts (top rows):

| wind_farm | year | current_confidence_class | event_count |
| --- | --- | --- | --- |
| Middelgrunden | 2024 | D_unsuitable | 8766 |
| Frederikshavn_Offshore | 2024 | D_unsuitable | 5013 |
| Nissum_Bredning | 2024 | D_unsuitable | 4184 |
| Renland | 2024 | D_unsuitable | 4164 |
| Avedøre_Holme | 2024 | D_unsuitable | 2791 |
| Kriegers_Flak | 2024 | D_unsuitable | 1555 |
| Arkona-Becken_Südost | 2024 | D_unsuitable | 1319 |
| Baltic_Eagle | 2024 | D_unsuitable | 1292 |
| Wikinger | 2024 | D_unsuitable | 1160 |
| Middelgrunden | 2025 | D_unsuitable | 1009 |
| Rodsand_II | 2024 | D_unsuitable | 1002 |
| Horns_Rev_II | 2024 | A_event_scale | 982 |
| Nordsee_Ost | 2024 | A_event_scale | 981 |
| Middelgrunden | 2022 | D_unsuitable | 938 |
| Horns_Rev_III | 2024 | A_event_scale | 823 |
| Merkur_Offshore | 2024 | A_event_scale | 798 |
| Frederikshavn_Offshore | 2025 | D_unsuitable | 791 |
| Renland | 2017 | D_unsuitable | 791 |
| Nissum_Bredning | 2017 | D_unsuitable | 791 |
| Renland | 2020 | D_unsuitable | 783 |
| Nissum_Bredning | 2020 | D_unsuitable | 782 |
| Middelgrunden | 2023 | D_unsuitable | 780 |
| Renland | 2019 | D_unsuitable | 770 |
| Nissum_Bredning | 2019 | D_unsuitable | 770 |
| Gode_Wind_1_and_2 | 2024 | A_event_scale | 764 |
| ... | 736 more rows omitted | | |

## Assignment Quality

- Bracketed events: 16,307
- Events with in-window samples: 15,855
- Nearest time gap minutes min/p50/p95/max: 0.000 / 14.850 / 28.550 / 30.000
- Event-window sample count min/p50/p95/max: 0.000 / 0.000 / 8.000 / 743.000
- Source sample/grid distance km min/p50/p95/max: 0.074 / 2.925 / 4.375 / 5.105

## Current Physical Checks

- Aggregated `current_speed_mean = sqrt(u_mean^2 + v_mean^2)` max error: 0.000000000000
- Direction in [0, 360): True
- Direction sin/cos unit-vector max error: 0.000000000000
- Current speed mean min/p50/p95/max: 0.001 / 0.211 / 0.577 / 1.088 m/s
- Current speed p95 min/p50/p95/max: 0.002 / 0.485 / 0.725 / 1.502 m/s

## Current Variability

This event layer preserves `current_speed_mean` and `current_speed_p95` so later modelling can test whether current severity adds explanatory power beyond waves and bathymetry. Hourly archive-level variability was accepted in the NWS batch validation; event-level variability is represented here by the event-window p95 and short-window bracketing method.

## Confidence

| current_confidence_class | event_count |
| --- | --- |
| D_unsuitable | 76353 |
| A_event_scale | 16307 |

Missing reason distribution:

| current_missing_reason | event_count |
| --- | --- |
| missing_nws_current_partition | 76353 |
| none | 16307 |

## Relation To Wave Confidence

| current_confidence_class | A_high | B_medium | C_low | D_unsuitable |
| --- | --- | --- | --- | --- |
| A_event_scale | 9337 | 5294 | 1297 | 379 |
| D_unsuitable | 6114 | 10821 | 51038 | 8380 |

- Events with both `A_high` wave confidence and `A_event_scale` current confidence: 9,337
- Tier A events with both high wave and event-scale current confidence: 3,402

## Research Interpretation

NWS current evidence is ready for Fusion v2 where the event confidence class is `A_event_scale`; non-covered farm-years remain explicit `D_unsuitable` rows and must not be treated as zero current. Coverage is strongest in the accepted NWS normal farm-years, so Fusion v2 should report source/domain coverage bias by farm and year rather than implying Europe-wide current availability.

Stress-test farm-years should remain separate because shallow/coastal model warnings were intentionally excluded from the accepted normal NWS current archive. Baltic daily current evidence remains contextual and should not be mixed into this NWS event-scale layer.

## Recommendation

Accept Current Confidence v1 if row identity is preserved, physical checks pass, fallback/synthetic provenance is absent, and the confidence distribution keeps missing/non-covered events visible. The next increment should be Fusion v2: wave confidence plus current confidence plus bathymetry event features, without adding new downloads or calibrated probability claims.
