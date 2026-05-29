# Fusion v2 Validation Report

## A. Research Design

Hypotheses: Fusion v2 creates the first modelling-ready multi-parameter event table; wind speed expands explanatory evidence beyond waves and currents; current-aware modelling is limited to accepted NWS-covered normal farm-years; wind direction is sensitivity-only because it is sparse; and farm/year/tier coverage bias must be explicit.

Metrics: row preservation, wave coverage, wind speed/direction coverage, current coverage, combined feature coverage, Tier A coverage, high-confidence subset size, missingness reasons, farm/year/tier bias, depth-warning impact, and wave/wind/current confidence cross-tabs.

## Executive Conclusion

Fusion v2 combines accepted wave confidence, Wind Confidence v1, Current Confidence v1, and EMODnet bathymetry into one event feature table. It preserves all dwell rows and keeps source-specific confidence separate. It is ready for modelling sensitivity, not calibrated access probability.

- Fusion v2 table: `Data/Processed/metocean/fusion_v2/dwell_metocean_fusion_v2.parquet`
- Input dwell rows: 92,660
- Output rows: 92,660
- Row identity preserved: True
- Model-ready wave+wind+current+bathymetry rows: 13,207
- High-confidence multivariate rows: 9,337

## Input Inventory

- Dwell rows: 92,660
- Wave confidence rows: 92,660
- Wind confidence rows: 92,660
- Current confidence rows: 92,660
- Bathymetry rows: 6,642

## Row Identity

- Input rows: 92,660
- Output rows: 92,660
- Duplicate `dwell_id` rows in output: 0
- One-to-one joins preserved input order: True

## Coverage

- Wave rows: 83,901
- Wind speed rows: 75,380
- Wind direction rows: 197
- Current rows: 16,307
- Bathymetry rows: 92,660
- Wave + wind speed rows: 71,134
- Wave + current rows: 15,928
- Wave + wind speed + current rows: 13,207
- Wave + wind speed + current + bathymetry rows: 13,207

Feature class distribution:

| metocean_feature_class | event_count |
| --- | --- |
| wave_wind_bathymetry_no_current | 57927 |
| wave_bathymetry_only | 10046 |
| wave_wind_current_bathymetry_high_confidence | 9337 |
| insufficient_metocean | 8759 |
| wave_wind_current_bathymetry_mixed_confidence | 3870 |
| wave_current_bathymetry_no_wind | 2721 |

Feature coverage by dwell tier:

| dwell_tier | metocean_feature_class | event_count |
| --- | --- | --- |
| Tier D | wave_wind_bathymetry_no_current | 46229 |
| Tier D | wave_bathymetry_only | 9743 |
| Tier A | wave_wind_bathymetry_no_current | 9116 |
| Tier D | insufficient_metocean | 7800 |
| Tier D | wave_wind_current_bathymetry_high_confidence | 5015 |
| Tier A | wave_wind_current_bathymetry_high_confidence | 3402 |
| Tier D | wave_wind_current_bathymetry_mixed_confidence | 2344 |
| Tier D | wave_current_bathymetry_no_wind | 1614 |
| Tier C | wave_wind_bathymetry_no_current | 1304 |
| Tier B | wave_wind_bathymetry_no_current | 1278 |
| Tier A | wave_wind_current_bathymetry_mixed_confidence | 1150 |
| Tier A | wave_current_bathymetry_no_wind | 756 |
| Tier A | insufficient_metocean | 658 |
| Tier C | wave_wind_current_bathymetry_high_confidence | 515 |
| Tier B | wave_wind_current_bathymetry_high_confidence | 405 |
| Tier B | wave_current_bathymetry_no_wind | 213 |
| Tier C | wave_wind_current_bathymetry_mixed_confidence | 209 |
| Tier A | wave_bathymetry_only | 182 |
| Tier B | insufficient_metocean | 170 |
| Tier B | wave_wind_current_bathymetry_mixed_confidence | 167 |
| ... | 4 more rows omitted | |

Feature coverage by year:

| year | metocean_feature_class | event_count |
| --- | --- | --- |
| 2010 | insufficient_metocean | 421 |
| 2010 | wave_bathymetry_only | 118 |
| 2010 | wave_wind_bathymetry_no_current | 1201 |
| 2010 | wave_wind_current_bathymetry_high_confidence | 16 |
| 2010 | wave_wind_current_bathymetry_mixed_confidence | 13 |
| 2011 | insufficient_metocean | 475 |
| 2011 | wave_bathymetry_only | 102 |
| 2011 | wave_current_bathymetry_no_wind | 13 |
| 2011 | wave_wind_bathymetry_no_current | 1286 |
| 2011 | wave_wind_current_bathymetry_high_confidence | 23 |
| 2011 | wave_wind_current_bathymetry_mixed_confidence | 12 |
| 2012 | insufficient_metocean | 979 |
| 2012 | wave_bathymetry_only | 252 |
| 2012 | wave_current_bathymetry_no_wind | 28 |
| 2012 | wave_wind_bathymetry_no_current | 3811 |
| 2012 | wave_wind_current_bathymetry_high_confidence | 214 |
| 2012 | wave_wind_current_bathymetry_mixed_confidence | 128 |
| 2016 | insufficient_metocean | 83 |
| 2016 | wave_bathymetry_only | 10 |
| 2016 | wave_wind_bathymetry_no_current | 197 |
| 2017 | insufficient_metocean | 979 |
| 2017 | wave_bathymetry_only | 220 |
| 2017 | wave_current_bathymetry_no_wind | 36 |
| 2017 | wave_wind_bathymetry_no_current | 2539 |
| 2017 | wave_wind_current_bathymetry_high_confidence | 154 |
| 2017 | wave_wind_current_bathymetry_mixed_confidence | 35 |
| 2018 | insufficient_metocean | 212 |
| 2018 | wave_bathymetry_only | 422 |
| 2018 | wave_current_bathymetry_no_wind | 86 |
| 2018 | wave_wind_bathymetry_no_current | 3213 |
| ... | 41 more rows omitted | |

Feature coverage by farm (top rows):

| wind_farm | metocean_feature_class | event_count |
| --- | --- | --- |
| Middelgrunden | wave_wind_bathymetry_no_current | 11151 |
| Frederikshavn_Offshore | wave_wind_bathymetry_no_current | 8674 |
| Renland | wave_wind_bathymetry_no_current | 8354 |
| Nissum_Bredning | wave_wind_bathymetry_no_current | 6583 |
| Middelgrunden | wave_bathymetry_only | 3342 |
| Nissum_Bredning | insufficient_metocean | 2717 |
| Avedøre_Holme | wave_wind_bathymetry_no_current | 2666 |
| Renland | wave_bathymetry_only | 2224 |
| Kriegers_Flak | wave_wind_bathymetry_no_current | 2193 |
| Arkona-Becken_Südost | wave_wind_bathymetry_no_current | 2084 |
| Nissum_Bredning | wave_bathymetry_only | 1973 |
| Avedøre_Holme | insufficient_metocean | 1840 |
| Wikinger | wave_wind_bathymetry_no_current | 1826 |
| Frederikshavn_Offshore | wave_bathymetry_only | 1620 |
| Baltic_Eagle | wave_wind_bathymetry_no_current | 1523 |
| Rodsand_II | wave_wind_bathymetry_no_current | 1365 |
| Nysted | wave_wind_bathymetry_no_current | 1060 |
| Anholt | wave_wind_bathymetry_no_current | 957 |
| EnBW_Windpark_Baltic_2 | wave_wind_bathymetry_no_current | 943 |
| Nordsee_Ost | wave_wind_current_bathymetry_high_confidence | 851 |
| Arcadis_Ost_1 | wave_wind_bathymetry_no_current | 792 |
| Horns_Rev_I | wave_wind_bathymetry_no_current | 772 |
| Horns_Rev_II | wave_wind_current_bathymetry_high_confidence | 756 |
| Frederikshavn_Offshore | insufficient_metocean | 723 |
| Horns_Rev_III | wave_wind_current_bathymetry_high_confidence | 692 |
| ... | 353 more rows omitted | |

## Tier A Coverage

- Tier A total: 15,264
- Tier A with wave: 14,606
- Tier A with wind speed: 13,708
- Tier A with wind direction: 34
- Tier A with current: 5,358
- Tier A with wave + wind: 13,668
- Tier A with wave + current: 5,308
- Tier A with wave + wind + current: 4,552
- High-confidence Tier A subset: 3,402

## Confidence Cross-Tabs

Wave x wind:

| wave_confidence_class | A_speed_direction | B_speed_only | D_unsuitable |
| --- | --- | --- | --- |
| A_high | 116 | 15147 | 188 |
| B_medium | 69 | 12863 | 3183 |
| C_low | 12 | 42927 | 9396 |
| D_unsuitable | 0 | 4246 | 4513 |

Wave x current:

| wave_confidence_class | A_event_scale | D_unsuitable |
| --- | --- | --- |
| A_high | 9337 | 6114 |
| B_medium | 5294 | 10821 |
| C_low | 1297 | 51038 |
| D_unsuitable | 379 | 8380 |

Wind x current:

| wind_confidence_class | A_event_scale | D_unsuitable |
| --- | --- | --- |
| A_speed_direction | 185 | 12 |
| B_speed_only | 13022 | 62161 |
| D_unsuitable | 3100 | 14180 |

Wave x wind x current summary (top rows):

| wave_confidence_class | wind_confidence_class | current_confidence_class | event_count |
| --- | --- | --- | --- |
| C_low | B_speed_only | D_unsuitable | 41665 |
| B_medium | B_speed_only | D_unsuitable | 10328 |
| C_low | D_unsuitable | D_unsuitable | 9372 |
| A_high | B_speed_only | A_event_scale | 9225 |
| A_high | B_speed_only | D_unsuitable | 5922 |
| D_unsuitable | B_speed_only | D_unsuitable | 4246 |
| D_unsuitable | D_unsuitable | D_unsuitable | 4134 |
| B_medium | D_unsuitable | A_event_scale | 2697 |
| B_medium | B_speed_only | A_event_scale | 2535 |
| C_low | B_speed_only | A_event_scale | 1262 |
| B_medium | D_unsuitable | D_unsuitable | 486 |
| D_unsuitable | D_unsuitable | A_event_scale | 379 |
| A_high | D_unsuitable | D_unsuitable | 188 |
| A_high | A_speed_direction | A_event_scale | 112 |
| B_medium | A_speed_direction | A_event_scale | 62 |
| C_low | D_unsuitable | A_event_scale | 24 |
| C_low | A_speed_direction | A_event_scale | 11 |
| B_medium | A_speed_direction | D_unsuitable | 7 |
| A_high | A_speed_direction | D_unsuitable | 4 |
| C_low | A_speed_direction | D_unsuitable | 1 |

## Distributions

- `selected_hs_mean` min/p50/p95/max: 0.000 / 0.326 / 1.223 / 4.846
- `selected_tp_mean` min/p50/p95/max: 1.247 / 3.113 / 7.183 / 20.170
- `wind_speed_mean` min/p50/p95/max: 0.100 / 5.400 / 10.340 / 20.140
- `current_speed_mean` min/p50/p95/max: 0.001 / 0.211 / 0.577 / 1.088
- `water_depth_m` min/p50/p95/max: 0.000 / 4.902 / 39.462 / 113.330

## Interaction Diagnostics

Thresholds used for diagnostic interactions: Hs p95 `1.223`, Hs p50 `0.326`, wind speed p95 `10.340`, current speed p95 `0.577`.

| interaction_case | event_count | tier_a_count |
| --- | --- | --- |
| high_hs_high_wind | 1364 | 260 |
| high_hs_high_current | 60 | 7 |
| low_hs_high_current | 83 | 8 |
| high_wind_low_wave | 128 | 1 |

Tier A examples:

| interaction_case | dwell_id | wind_farm | dwell_tier | selected_hs_mean | selected_tp_mean | wind_speed_mean | current_speed_mean |
| --- | --- | --- | --- | --- | --- | --- | --- |
| high_hs_high_wind | dw_1d3351bb | OWF_Prinses_Amalia | Tier A | 1.826 | 6.134 | 11.401 | 0.062 |
| high_hs_high_wind | dw_6d54aed0 | Lincs | Tier A | 1.849 | 5.976 | 12.974 | NA |
| high_hs_high_wind | dw_ed3856c6 | Lincs | Tier A | 1.703 | 6.934 | 10.719 | NA |
| high_hs_high_wind | dw_47a90c82 | Moray_West | Tier A | 1.343 | 4.855 | 11.661 | NA |
| high_hs_high_wind | dw_bec0fc41 | Baltic_Eagle | Tier A | 1.432 | 5.026 | 10.777 | NA |
| high_hs_high_current | dw_3ff685b7 | Gode_Wind_1_and_2 | Tier A | 1.772 | 8.290 | 2.470 | 0.697 |
| high_hs_high_current | dw_0041a96b | Gode_Wind_1_and_2 | Tier A | 1.706 | 6.410 | 10.370 | 0.645 |
| high_hs_high_current | dw_ad2229af | Gode_Wind_1_and_2 | Tier A | 2.234 | 7.700 | 10.330 | 0.595 |
| high_hs_high_current | dw_938a3224 | Gode_Wind_1_and_2 | Tier A | 1.682 | 8.210 | NA | 0.653 |
| high_hs_high_current | dw_7704a4f0 | Merkur_Offshore | Tier A | 2.130 | 7.210 | 13.020 | 0.669 |
| low_hs_high_current | dw_25b2fdaf | Triton_Knoll | Tier A | 0.236 | 2.410 | 3.890 | 0.761 |
| low_hs_high_current | dw_7f2109b7 | Thanet | Tier A | 0.092 | 11.040 | 2.650 | 0.795 |
| low_hs_high_current | dw_1850a647 | Borkum_Riffgrund_2 | Tier A | 0.270 | 5.630 | 2.620 | 0.578 |
| low_hs_high_current | dw_55c003df | Nordsee_One | Tier A | 0.318 | 4.780 | 2.600 | 0.611 |
| low_hs_high_current | dw_d45056b2 | Nordsee_One | Tier A | 0.302 | 4.760 | 3.180 | 0.778 |
| high_wind_low_wave | dw_830a0f72 | Nissum_Bredning | Tier A | 0.273 | 2.094 | 10.577 | NA |

## Bathymetry

- Bathymetry complete rows: 92,660
- Depth warning <=1 m rows: 24,802
- Depth warning <=5 m rows: 51,977
- Depth warning <=10 m rows: 57,521
- Current-available rows with <=10 m depth warning: 134

Zero or near-zero depth warnings are preserved as site-context caveats. They do not automatically invalidate an observed dwell, but high-confidence multivariate flags exclude <=10 m depth-warning rows.

## Missingness Reasons

| metocean_missing_reason | event_count |
| --- | --- |
| missing_nws_current_partition; depth_warning_le_10m | 41495 |
| missing_nws_current_partition | 16432 |
| none | 13097 |
| no_active_nora3_wind_records; missing_nws_current_partition; depth_warning_le_10m | 9356 |
| missing_wave; missing_nws_current_partition; depth_warning_le_10m | 4213 |
| no_active_nora3_wind_records | 2697 |
| missing_wave; no_active_nora3_wind_records; missing_nws_current_partition; depth_warning_le_10m | 2323 |
| missing_wave; no_active_nora3_wind_records; missing_nws_current_partition | 1811 |
| no_active_nora3_wind_records; missing_nws_current_partition | 689 |
| missing_wave; no_active_nora3_wind_records | 379 |
| depth_warning_le_10m | 110 |
| missing_wave; missing_nws_current_partition | 33 |
| no_active_nora3_wind_records; depth_warning_le_10m | 24 |
| wind_speed_missing_in_active_fields; missing_nws_current_partition | 1 |

## Bias And Caveats

- NWS current coverage remains source/domain biased; non-covered events keep missing current values and are not treated as zero current.
- NORA3 wind direction is sparse and remains nullable/sensitivity-only.
- Baltic historical true currents remain daily/contextual and are not promoted to event-scale current evidence.
- Stress-test current farm-years remain excluded from the accepted NWS current archive.
- FINO validation has not been imported yet.
- Fusion v2 is not a calibrated `P(operation | weather)` model and does not infer CTV/SOV roles from vessel length.

## Research Interpretation

Fusion v2 is ready for modelling sensitivity. The first modelling subset should compare wave-only against wave + wind speed, wave + event-scale current, and wave + wind speed + current. Wind direction should be held back except for narrow sensitivity checks on the 197 speed+direction rows. Current-aware modelling should be interpreted as NWS-domain evidence, not Europe-wide current coverage.

## Recommendation

Accept Fusion v2 if row identity is preserved, missing current and wind direction remain null, confidence fields are preserved separately, and the model-ready flags match the documented rules. Proceed to Stage 2 modelling sensitivity before targeted wind-direction repair, stress-test current increments, or FINO imports.
