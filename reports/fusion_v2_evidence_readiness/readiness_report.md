# Fusion v2 Evidence-Readiness Report

## Executive Summary

- Final recommendation: `proceed_with_restrictions`
- Run timestamp UTC: `2026-06-04T10:21:08Z`
- This is a report-only readiness audit. It does not train models, compare envelopes, rebuild Fusion v2, download data, import FINO, repair NORA3, repair wind direction, or expand current stress-test farm-years.
- Fusion v2 remains a provisional source-resolved event feature layer, not a calibrated `P(operation | weather)` model.

## Key Caveats

- `partial_event_scale_current_coverage`
- `wind_direction_sensitivity_only`
- `depth_warning_sensitivity_required`

## Readiness Subset Counts

| subset                        | subset_type              |   event_count |   share_of_all |
|:------------------------------|:-------------------------|--------------:|---------------:|
| all_events                    | guardrail_indicator      |         92660 |     1          |
| has_wave                      | guardrail_indicator      |         83901 |     0.905472   |
| has_wind_speed                | guardrail_indicator      |         75380 |     0.813512   |
| has_wind_direction            | guardrail_indicator      |           197 |     0.00212605 |
| has_current                   | guardrail_indicator      |         16307 |     0.175987   |
| has_bathymetry                | guardrail_indicator      |         92660 |     1          |
| model_ready_wave_only         | canonical_readiness_flag |         83901 |     0.905472   |
| model_ready_wave_wind         | canonical_readiness_flag |         71134 |     0.767688   |
| model_ready_wave_current      | canonical_readiness_flag |         15928 |     0.171897   |
| model_ready_wave_wind_current | canonical_readiness_flag |         13207 |     0.142532   |
| model_ready_high_confidence   | canonical_readiness_flag |          9337 |     0.100766   |

## Tier A Readiness Counts

| subset                        |   tier_a_event_count |   share_of_tier_a |
|:------------------------------|---------------------:|------------------:|
| all_events                    |                15264 |        1          |
| has_wave                      |                14606 |        0.956892   |
| has_wind_speed                |                13708 |        0.898061   |
| has_wind_direction            |                   34 |        0.00222746 |
| has_current                   |                 5358 |        0.351022   |
| has_bathymetry                |                15264 |        1          |
| model_ready_wave_only         |                14606 |        0.956892   |
| model_ready_wave_wind         |                13668 |        0.89544    |
| model_ready_wave_current      |                 5308 |        0.347746   |
| model_ready_wave_wind_current |                 4552 |        0.298218   |
| model_ready_high_confidence   |                 3402 |        0.222877   |

## Concentration Diagnostics

| subset                        |   event_count | top_wind_farm   |   top_wind_farm_share |   top_5_wind_farm_share |   top_audit_year |   top_audit_year_share | top_source_domain                             |   top_source_domain_share |
|:------------------------------|--------------:|:----------------|----------------------:|------------------------:|-----------------:|-----------------------:|:----------------------------------------------|--------------------------:|
| all_events                    |         92660 | Middelgrunden   |             0.16213   |                0.572491 |             2024 |               0.53597  | wave=nws; wind=NORA3; current=missing_current |                  0.362918 |
| model_ready_wave_only         |         83901 | Middelgrunden   |             0.172739  |                0.555262 |             2024 |               0.567454 | wave=nws; wind=NORA3; current=missing_current |                  0.400806 |
| model_ready_wave_wind         |         71134 | Middelgrunden   |             0.15676   |                0.526162 |             2024 |               0.538828 | wave=nws; wind=NORA3; current=missing_current |                  0.472742 |
| model_ready_wave_current      |         15928 | Horns_Rev_II    |             0.0807383 |                0.33168  |             2024 |               0.838586 | wave=nws; wind=NORA3; current=NWS             |                  0.757534 |
| model_ready_wave_wind_current |         13207 | Horns_Rev_II    |             0.0867722 |                0.347392 |             2024 |               0.836147 | wave=nws; wind=NORA3; current=NWS             |                  0.913606 |
| model_ready_high_confidence   |          9337 | Nordsee_Ost     |             0.0911428 |                0.372282 |             2024 |               0.832387 | wave=nws; wind=NORA3; current=NWS             |                  1        |

## Current-Ready Versus Current-Missing Bias

| current_group   |   event_count |   share_of_all |   tier_a_event_count | top_wind_farm   |   top_wind_farm_share |   top_audit_year |   top_audit_year_share | top_source_domain                             |
|:----------------|--------------:|---------------:|---------------------:|:----------------|----------------------:|-----------------:|-----------------------:|:----------------------------------------------|
| current_ready   |         16307 |       0.175987 |                 5358 | Horns_Rev_II    |             0.0788618 |             2024 |               0.838965 | wave=nws; wind=NORA3; current=NWS             |
| current_missing |         76353 |       0.824013 |                 9906 | Middelgrunden   |             0.196757  |             2024 |               0.471258 | wave=nws; wind=NORA3; current=missing_current |

## Confidence-Class Distribution

| variable   | confidence_class   |   event_count |   share_of_all |
|:-----------|:-------------------|--------------:|---------------:|
| wave       | C_low              |         52335 |     0.564807   |
| wave       | B_medium           |         16115 |     0.173915   |
| wave       | A_high             |         15451 |     0.166749   |
| wave       | D_unsuitable       |          8759 |     0.0945284  |
| wind       | B_speed_only       |         75183 |     0.811386   |
| wind       | D_unsuitable       |         17280 |     0.186488   |
| wind       | A_speed_direction  |           197 |     0.00212605 |
| current    | D_unsuitable       |         76353 |     0.824013   |
| current    | A_event_scale      |         16307 |     0.175987   |

## Missingness

| field                   |   missing_count |   missing_share |   zero_count |
|:------------------------|----------------:|----------------:|-------------:|
| selected_hs_mean        |            8759 |       0.0945284 |         1082 |
| selected_tp_mean        |            8759 |       0.0945284 |            0 |
| wind_speed_mean         |           17280 |       0.186488  |            0 |
| wind_direction_deg_mean |           92463 |       0.997874  |            0 |
| current_u_mean          |           76353 |       0.824013  |           13 |
| current_v_mean          |           76353 |       0.824013  |           16 |
| current_speed_mean      |           76353 |       0.824013  |            0 |
| water_depth_m           |               0 |       0         |        11017 |

## Depth-Warning Sensitivity

| subset                        |   event_count |   depth_warning_le_10m_count |   depth_warning_le_10m_share |   after_excluding_depth_warning_count |   tier_a_after_excluding_depth_warning_count |
|:------------------------------|--------------:|-----------------------------:|-----------------------------:|--------------------------------------:|---------------------------------------------:|
| all_events                    |         92660 |                        57521 |                   0.620775   |                                 35139 |                                        12703 |
| model_ready_wave_only         |         83901 |                        50985 |                   0.60768    |                                 32916 |                                        12132 |
| model_ready_wave_wind         |         71134 |                        41605 |                   0.584882   |                                 29529 |                                        11257 |
| model_ready_wave_current      |         15928 |                          134 |                   0.00841286 |                                 15794 |                                         5299 |
| model_ready_wave_wind_current |         13207 |                          110 |                   0.00832892 |                                 13097 |                                         4543 |
| model_ready_high_confidence   |          9337 |                            0 |                   0          |                                  9337 |                                         3402 |

## Guardrail Checks

| guardrail                            | status   |   issue_count | severity       | evidence                                                                         |
|:-------------------------------------|:---------|--------------:|:---------------|:---------------------------------------------------------------------------------|
| required_columns_present             | pass     |             0 | integrity      | Fusion v2 has every column required by the readiness audit.                      |
| dwell_row_identity_preserved         | pass     |             0 | integrity      | Fusion v2 dwell_id order matches the accepted dwell-weather input.               |
| duplicate_dwell_identity             | pass     |             0 | integrity      | Fusion v2 should contain no duplicate dwell_id rows.                             |
| readiness_flag_semantics             | pass     |             0 | integrity      | Canonical readiness flags match column masks from ADR 0029.                      |
| missing_current_null_not_zero        | pass     |             0 | integrity      | Missing-current zero-like rows: 0.                                               |
| wind_direction_quarantined           | pass     |             0 | integrity      | Wind direction is usable only for A_speed_direction rows.                        |
| high_confidence_multivariate_nonzero | pass     |             0 | integrity      | High-confidence multivariate subset must be non-empty.                           |
| tier_a_high_confidence_nonzero       | pass     |             0 | integrity      | Tier A high-confidence multivariate subset must be non-empty.                    |
| partial_event_scale_current_coverage | caveat   |         76353 | restriction    | Missing current remains missing; current-aware claims require restrictions.      |
| wind_direction_sensitivity_only      | caveat   |         75183 | restriction    | Wind direction is much narrower than wind-speed-ready evidence.                  |
| depth_warning_sensitivity_required   | caveat   |         57521 | restriction    | Rows with <=10 m depth warnings require exclusion or sensitivity treatment.      |
| fusion_v2_claim_boundary             | pass     |             0 | claim_boundary | Audit is report-only readiness; it makes no calibrated access-probability claim. |
| fino_not_imported                    | pass     |             0 | claim_boundary | Audit does not read FINO observations.                                           |
| baltic_daily_current_not_promoted    | pass     |             0 | claim_boundary | Audit uses Fusion v2 event-scale current flags only.                             |

## Fusion v2 Report Count Cross-Checks

| report_label                                  |   report_count |   audit_count | matches   |
|:----------------------------------------------|---------------:|--------------:|:----------|
| Output rows                                   |          92660 |         92660 | True      |
| Wave rows                                     |          83901 |         83901 | True      |
| Wind speed rows                               |          75380 |         75380 | True      |
| Wind direction rows                           |            197 |           197 | True      |
| Current rows                                  |          16307 |         16307 | True      |
| Wave + wind speed + current + bathymetry rows |          13207 |         13207 | True      |
| High-confidence multivariate rows             |           9337 |          9337 | True      |
| Tier A total                                  |          15264 |         15264 | True      |
| Tier A with wave + wind + current             |           4552 |          4552 | True      |
| High-confidence Tier A subset                 |           3402 |          3402 | True      |

## Claim Boundary

- Missing current remains missing/null and must not be interpreted as zero current.
- Wind direction remains sensitivity-only and excluded from primary Stage 2 predictors unless a later repair increment is accepted.
- Baltic daily/contextual current is not promoted to event-scale current evidence.
- FINO remains validation/planning only and is not imported here.
- Any later Stage 2 work should review this recommendation before merge.
