# Wind Confidence v1 Validation Report

## Research Design

Hypotheses: NORA3 wind speed evidence is usable for many dwell events; wind direction coverage is weak and must be separated from wind speed; wind speed alone may improve later modelling beyond waves/currents; and wind direction should not be trusted unless provenance and coverage are explicit.

Metrics: wind speed coverage, wind direction coverage, farm/year/tier coverage, temporal alignment quality from existing event aggregates, raw and processed cache schema evidence, missingness reasons, physical range checks, and relation to wave/current confidence classes.

## Executive Conclusion

Wind Confidence v1 writes one NORA3 wind candidate and one wind confidence row for every dwell event using existing local evidence only. The main result is a split evidence layer: wind speed is broadly modelling-ready, while wind direction is sparse and should remain pending targeted repair.

- Candidate table: `Data/Processed/metocean/wind_confidence_v1/wind_event_candidates.parquet`
- Confidence table: `Data/Processed/metocean/wind_confidence_v1/wind_event_confidence.parquet`
- Input dwell rows: 92,660
- Candidate rows: 92,660
- Confidence rows: 92,660
- Wind speed events: 75,380
- Wind direction events: 197
- Tier A wind speed events: 13,708 of 15,264
- Tier A wind direction events: 34 of 15,264

## Input Inventory

- Dwell rows: 92,660
- Fusion v1 wave confidence rows: 92,660
- Current confidence rows: 92,660
- NORA3 joined-cache manifest rows: 33,587
- NORA3 joined-cache parquet files: 375
- NORA3 joined-cache parquet rows: 24,579,576
- NORA3 joined-cache year range: 2009 to 2025
- Raw NORA3 wind files inspected: 33,985
- Raw NORA3 wind year range: 2009 to 2025
- Joined-cache wind speed fields: `wind_speed_100m, wind_speed_10m`
- Joined-cache wind direction fields: `wind_direction_100m, wind_direction_10m`

Raw wind cache schema counts:

| schema | file_count |
| --- | --- |
| time \| wind_speed_10m | 33521 |
| time \| wind_speed_10m \| wind_direction_10m \| wind_speed_100m \| wind_direction_100m \| lat \| lon | 464 |

## Coverage

- Events with wind speed: 75,380 / 92,660
- Events with wind direction: 197 / 92,660
- Tier A wind speed coverage: 13,708 / 15,264
- Tier A wind direction coverage: 34 / 15,264

Confidence by dwell tier:

| dwell_tier | wind_confidence_class | event_count |
| --- | --- | --- |
| Tier D | B_speed_only | 57647 |
| Tier D | D_unsuitable | 14957 |
| Tier A | B_speed_only | 13674 |
| Tier C | B_speed_only | 2022 |
| Tier B | B_speed_only | 1840 |
| Tier A | D_unsuitable | 1556 |
| Tier B | D_unsuitable | 447 |
| Tier C | D_unsuitable | 320 |
| Tier D | A_speed_direction | 141 |
| Tier A | A_speed_direction | 34 |
| Tier B | A_speed_direction | 14 |
| Tier C | A_speed_direction | 8 |

Confidence by year:

| year | wind_confidence_class | event_count |
| --- | --- | --- |
| 2010 | B_speed_only | 1533 |
| 2010 | D_unsuitable | 236 |
| 2011 | B_speed_only | 1678 |
| 2011 | D_unsuitable | 233 |
| 2012 | B_speed_only | 4586 |
| 2012 | D_unsuitable | 826 |
| 2016 | B_speed_only | 257 |
| 2016 | D_unsuitable | 33 |
| 2017 | B_speed_only | 3411 |
| 2017 | D_unsuitable | 552 |
| 2018 | B_speed_only | 3516 |
| 2018 | D_unsuitable | 711 |
| 2019 | B_speed_only | 3329 |
| 2019 | D_unsuitable | 473 |
| 2020 | B_speed_only | 3146 |
| 2020 | D_unsuitable | 424 |
| 2021 | A_speed_direction | 4 |
| 2021 | B_speed_only | 3386 |
| 2021 | D_unsuitable | 491 |
| 2022 | A_speed_direction | 11 |
| 2022 | B_speed_only | 3497 |
| 2022 | D_unsuitable | 644 |
| 2023 | A_speed_direction | 1 |
| 2023 | B_speed_only | 3161 |
| 2023 | D_unsuitable | 504 |
| 2024 | A_speed_direction | 180 |
| 2024 | B_speed_only | 38788 |
| 2024 | D_unsuitable | 10695 |
| 2025 | A_speed_direction | 1 |
| 2025 | B_speed_only | 4895 |
| 2025 | D_unsuitable | 1458 |

Farm/year confidence counts (top rows):

| wind_farm | year | wind_confidence_class | event_count |
| --- | --- | --- | --- |
| Middelgrunden | 2024 | B_speed_only | 6202 |
| Frederikshavn_Offshore | 2024 | B_speed_only | 3952 |
| Nissum_Bredning | 2024 | B_speed_only | 2755 |
| Renland | 2024 | B_speed_only | 2749 |
| Middelgrunden | 2024 | D_unsuitable | 2564 |
| Avedøre_Holme | 2024 | B_speed_only | 2127 |
| Kriegers_Flak | 2024 | B_speed_only | 1495 |
| Nissum_Bredning | 2024 | D_unsuitable | 1429 |
| Renland | 2024 | D_unsuitable | 1415 |
| Arkona-Becken_Südost | 2024 | B_speed_only | 1230 |
| Baltic_Eagle | 2024 | B_speed_only | 1140 |
| Wikinger | 2024 | B_speed_only | 1109 |
| Frederikshavn_Offshore | 2024 | D_unsuitable | 1061 |
| Rodsand_II | 2024 | B_speed_only | 888 |
| Nordsee_Ost | 2024 | B_speed_only | 869 |
| Horns_Rev_II | 2024 | B_speed_only | 857 |
| Middelgrunden | 2022 | B_speed_only | 804 |
| Horns_Rev_III | 2024 | B_speed_only | 742 |
| Renland | 2020 | B_speed_only | 693 |
| Nissum_Bredning | 2020 | B_speed_only | 692 |
| Nysted | 2024 | B_speed_only | 689 |
| Nissum_Bredning | 2019 | B_speed_only | 682 |
| Renland | 2019 | B_speed_only | 681 |
| Middelgrunden | 2025 | B_speed_only | 681 |
| Renland | 2017 | B_speed_only | 680 |
| ... | 1282 more rows omitted | | |

Farm confidence counts (top rows):

| wind_farm | wind_confidence_class | event_count |
| --- | --- | --- |
| Middelgrunden | B_speed_only | 11353 |
| Frederikshavn_Offshore | B_speed_only | 9211 |
| Nissum_Bredning | B_speed_only | 8807 |
| Renland | B_speed_only | 8784 |
| Middelgrunden | D_unsuitable | 3670 |
| Avedøre_Holme | B_speed_only | 3485 |
| Nissum_Bredning | D_unsuitable | 2466 |
| Renland | D_unsuitable | 2444 |
| Kriegers_Flak | B_speed_only | 2193 |
| Arkona-Becken_Südost | B_speed_only | 2084 |
| Wikinger | B_speed_only | 1826 |
| Frederikshavn_Offshore | D_unsuitable | 1806 |
| Baltic_Eagle | B_speed_only | 1523 |
| Rodsand_II | B_speed_only | 1365 |
| Horns_Rev_II | B_speed_only | 1231 |
| Nordsee_Ost | B_speed_only | 1199 |
| Horns_Rev_III | B_speed_only | 1134 |
| Nysted | B_speed_only | 1060 |
| Avedøre_Holme | D_unsuitable | 1021 |
| Anholt | B_speed_only | 957 |
| EnBW_Windpark_Baltic_2 | B_speed_only | 943 |
| Kaskasi | B_speed_only | 878 |
| Meerwind_Sued_Ost | B_speed_only | 869 |
| Arcadis_Ost_1 | B_speed_only | 792 |
| Amrumbank_West | B_speed_only | 776 |
| ... | 201 more rows omitted | |

## Physical QA

- Wind speed mean min/p50/p95/max: 0.100 / 5.400 / 10.340 / 20.140 m/s
- Wind upper-window diagnostic min/p50/p95/max: 0.100 / 6.670 / 12.711 / 25.060 m/s
- Impossible wind speed rows outside 0-75 m/s: 0
- Direction degrees in [0, 360): True
- Direction sin/cos unit-vector max error: 0.000000000000
- Direction convention: `meteorological_from_degrees_clockwise_from_true_north`
- Direction sin/cos outputs are unit-circle projections of the accepted event mean direction; missing direction remains null.

Note: The existing dwell-weather table stores active wind mean and max, not per-event wind-speed p95. Wind v1 preserves the active max in the `wind_speed_p95` slot as an upper-window diagnostic until a targeted per-sample NORA3 wind repair/reaggregation is approved.

## Missingness Diagnosis

| wind_missing_reason | event_count |
| --- | --- |
| wind_direction_missing_in_existing_active_fields | 75183 |
| no_active_nora3_wind_records | 17279 |
| none | 197 |
| wind_speed_missing_in_active_fields | 1 |

Most raw NORA3 wind cache files in this local archive are speed-only. Direction-capable raw files exist but are a small minority, and the current dwell-weather table contains only sparse active wind-direction aggregates. Therefore direction missingness is treated as an upstream acquisition/schema coverage issue rather than a zero-direction value.

## Confidence

| wind_confidence_class | event_count |
| --- | --- |
| B_speed_only | 75183 |
| D_unsuitable | 17280 |
| A_speed_direction | 197 |

Wind confidence versus wave confidence:

| wind_confidence_class | A_high | B_medium | C_low | D_unsuitable |
| --- | --- | --- | --- | --- |
| A_speed_direction | 116 | 69 | 12 | 0 |
| B_speed_only | 15147 | 12863 | 42927 | 4246 |
| D_unsuitable | 188 | 3183 | 9396 | 4513 |

Wind confidence versus current confidence:

| wind_confidence_class | A_event_scale | D_unsuitable |
| --- | --- | --- |
| A_speed_direction | 185 | 12 |
| B_speed_only | 13022 | 62161 |
| D_unsuitable | 3100 | 14180 |

- Events with both `A_speed_direction` wind and `A_high` wave confidence: 116
- Events with speed-ready wind and `A_high` wave confidence: 15,263
- Events with speed-ready wind and `A_event_scale` current confidence: 13,207

## Research Interpretation

Wind speed is ready for Fusion v2 as a 10 m NORA3 active-window event feature with explicit `B_speed_only` confidence for most usable rows. Wind direction is not ready as a broad modelling feature: the valid event count is too small, and missing direction must not be treated as calm, zero, or aligned wind.

Fusion v2 should include wind speed and wind confidence, while either leaving wind direction nullable or treating it as a narrow sensitivity-only feature. The real source-resolved Fusion v2 should therefore combine wave confidence, wind confidence, current confidence, and bathymetry, with wind direction flagged as repair-pending.

## Recommendation

Accept Wind Confidence v1 if row identity is preserved, speed physical QA passes, direction missingness is explicit, and no synthetic/fallback wind evidence is introduced. Proceed to Fusion v2 with wind speed included and wind direction marked pending targeted NORA3 repair. Run a targeted wind-direction repair only if later modelling or simulator inputs require directional wind effects beyond speed.
