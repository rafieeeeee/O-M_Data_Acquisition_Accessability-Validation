# Agent Handoff: Metocean Fusion v2 To Stage 2 Modelling

## Repository

`/Volumes/Extreme SSD/01_ACTIVE_PROJECTS/O&M_Data_Acquisition_Accessability&Validation`

## Current Decision

Fusion v2 is accepted as the first source-resolved multi-parameter event feature table. The Fusion v2 evidence-readiness audit is complete and recommends `proceed_with_restrictions`.

Stage 2 has not started. The next increment may be Stage 2 modelling sensitivity only under restricted claim boundaries:

```text
wave-only primary sensitivity lane
wave + wind-speed primary sensitivity lane
current-aware NWS-domain / coverage-limited sensitivity only
wave + wind speed + current NWS-domain / coverage-limited sensitivity only
high-confidence subset only
depth-warning exclusion/sensitivity required
```

Do not start with a calibrated `P(operation | weather)` model. First test whether wind speed materially changes the observed workability envelope relative to wave-only evidence, and treat event-scale current as a coverage-limited NWS-domain sensitivity rather than a headline cross-domain predictor.

## Accepted Inputs

```text
dwell_weather=Data/Processed/ais_dwell_backfill/cross_farm_dwell_weather_features.parquet
wave_confidence=Data/Processed/metocean/fusion_v1_source_agreement/wave_event_confidence.parquet
wind_confidence=Data/Processed/metocean/wind_confidence_v1/wind_event_confidence.parquet
current_confidence=Data/Processed/metocean/current_confidence_v1/current_event_confidence.parquet
bathymetry=Data/Processed/metocean/bathymetry/site_bathymetry_points.parquet
fusion_v2=Data/Processed/metocean/fusion_v2/dwell_metocean_fusion_v2.parquet
```

## Accepted Outputs

```text
fusion_v2_table=Data/Processed/metocean/fusion_v2/dwell_metocean_fusion_v2.parquet
fusion_v2_report=reports/metocean_fusion_v2/fusion_v2_validation_report.md
fusion_v2_logic=src/om_pipeline/metocean/metocean_fusion_v2.py
fusion_v2_cli=scripts/build_metocean_fusion_v2.py
fusion_v2_tests=tests/test_metocean_fusion_v2.py
decision_record=docs/adr/0029-metocean-fusion-v2-multiparameter-event-features.md
readiness_report=reports/fusion_v2_evidence_readiness/readiness_report.md
readiness_summary=Data/Processed/metocean/fusion_v2_evidence_readiness/readiness_summary.json
```

## Validation Snapshot

```text
input_rows=92660
output_rows=92660
duplicate_dwell_id_rows=0
wave_rows=83901
wind_speed_rows=75380
wind_direction_rows=197
current_rows=16307
bathymetry_rows=92660
wave_wind_current_rows=13207
high_confidence_multivariate_rows=9337
tier_a_rows=15264
tier_a_wave_rows=14606
tier_a_wind_speed_rows=13708
tier_a_wind_direction_rows=34
tier_a_current_rows=5358
tier_a_wave_wind_current_rows=4552
tier_a_high_confidence_rows=3402
```

Feature-class distribution:

```text
wave_wind_bathymetry_no_current=57927
wave_bathymetry_only=10046
wave_wind_current_bathymetry_high_confidence=9337
insufficient_metocean=8759
wave_wind_current_bathymetry_mixed_confidence=3870
wave_current_bathymetry_no_wind=2721
```

Evidence-readiness audit result:

```text
recommendation=proceed_with_restrictions
caveats=partial_event_scale_current_coverage, wind_direction_sensitivity_only, depth_warning_sensitivity_required
stage2_started=False
```

## Modelling Guardrails

- Missing current means no accepted NWS event-scale current evidence. It must not be interpreted as zero current.
- Wind direction is only modelling-ready where `wind_confidence_class == A_speed_direction`; all other wind-direction values are null by design.
- Wind speed is modelling-ready where `wind_confidence_class` is `A_speed_direction` or `B_speed_only`.
- Current values are modelling-ready only where `current_confidence_class == A_event_scale`.
- Shallow bathymetry warnings must be reported as a sensitivity or exclusion choice, especially `depth_warning_le_10m`.
- Stress-test current farm-years remain excluded.
- Baltic historical true-current evidence is daily/contextual and must not be promoted to event-scale current.
- FINO remains validation-planning only; no FINO observations are imported.
- Vessel length can be used as a continuous exploratory feature but must not be converted into CTV/SOV roles.

## Restricted Stage 2 Claim Policy

- Wave-only and wave+wind-speed comparisons can be primary sensitivity lanes.
- Current-aware comparisons must be labelled NWS-domain and coverage-limited sensitivity only.
- Wind direction is excluded from primary predictors.
- Depth-warning exclusion or sensitivity treatment is required.
- No calibrated `P(operation | weather)` claim is supported by Fusion v2 readiness evidence.

## Useful Commands

Rebuild Fusion v2:

```bash
/opt/anaconda3/bin/python scripts/build_metocean_fusion_v2.py \
  --dwell-weather Data/Processed/ais_dwell_backfill/cross_farm_dwell_weather_features.parquet \
  --wave-confidence Data/Processed/metocean/fusion_v1_source_agreement/wave_event_confidence.parquet \
  --wind-confidence Data/Processed/metocean/wind_confidence_v1/wind_event_confidence.parquet \
  --current-confidence Data/Processed/metocean/current_confidence_v1/current_event_confidence.parquet \
  --bathymetry Data/Processed/metocean/bathymetry/site_bathymetry_points.parquet \
  --output-dir Data/Processed/metocean/fusion_v2 \
  --report-dir reports/metocean_fusion_v2 \
  --overwrite
```

Run validation tests:

```bash
/opt/anaconda3/bin/python -m pytest tests/test_metocean_fusion_v2.py
/opt/anaconda3/bin/python -m pytest \
  tests/test_wind_confidence_v1.py \
  tests/test_current_confidence_v1.py \
  tests/test_metocean_fusion_v1_source_agreement.py \
  tests/test_metocean.py \
  tests/test_common_metocean_requirements.py
```

## Next Increment Recommendation

Create a restricted Stage 2 modelling sensitivity increment that writes report-only or clearly labelled exploratory artefacts, for example:

```text
reports/stage2_metocean_sensitivity/
Data/Processed/metocean/stage2_sensitivity/
```

The first question should be:

```text
Do wind speed, and separately NWS-domain event-scale current, change the observed Tier A workability envelope compared with wave-only evidence?
```

Recommended comparisons:

1. Tier A wave-only envelope.
2. Tier A Fusion v2 wave + wind-speed-ready events.
3. Tier A Fusion v2 wave + event-scale-current events, labelled NWS-domain and coverage-limited.
4. Tier A Fusion v2 wave + wind speed + current, labelled NWS-domain and coverage-limited.
5. High-confidence multivariate subset.
6. Depth-warning excluded and depth-warning sensitivity subsets.

Hold back targeted wind-direction repair and stress-test current farm-years until the speed/current sensitivity result shows whether they are worth the extra evidence work.
