# ADR 0029: Metocean Fusion v2 Multiparameter Event Features

## Status
Accepted

## Context

Fusion v1 made wave evidence defensible by preserving NORA3, NWS, and Baltic source agreement and confidence instead of using the earlier Baltic > NWS > NORA3 priority resolver. Current Confidence v1 then attached accepted NWS hourly true `uo`/`vo` current evidence to dwell events, and Wind Confidence v1 formalized existing NORA3 active-window wind evidence.

The next modelling increment needs one event-level feature table that keeps these evidence layers together without collapsing their separate provenance and confidence. The target research surface includes `Hs`, `Tp`, wind, current, vessel, task, and site context, so a waves plus currents table would be incomplete unless wind is included or explicitly marked pending.

## Decision

Build Fusion v2 as the first combined source-resolved metocean event feature table:

- Output table: `Data/Processed/metocean/fusion_v2/dwell_metocean_fusion_v2.parquet`
- Validation report: `reports/metocean_fusion_v2/fusion_v2_validation_report.md`
- Core logic: `src/om_pipeline/metocean/metocean_fusion_v2.py`
- CLI: `scripts/build_metocean_fusion_v2.py`

Fusion v2 reads accepted local evidence layers only:

- Dwell-weather event table.
- Wave Confidence v1.
- Wind Confidence v1.
- Current Confidence v1, with its candidate table used only to preserve current p95 and direction metadata already produced by that layer.
- EMODnet bathymetry site context.

Fusion v2 preserves all dwell rows and validates one-to-one joins by stable dwell identity before writing. It keeps wave, wind, current, and bathymetry confidence/provenance fields separate, then adds modelling-readiness flags such as `model_ready_wave_only`, `model_ready_wave_wind`, `model_ready_wave_current`, `model_ready_wave_wind_current`, and `model_ready_high_confidence`.

The table enforces three masking rules:

- Wind speed may be used where `wind_confidence_class` is `A_speed_direction` or `B_speed_only`.
- Wind direction may be used only where `wind_confidence_class == A_speed_direction`; missing direction remains null and is not converted to zero.
- Current values may be used only where `current_confidence_class == A_event_scale`; missing or non-covered current remains null and is not converted to zero.

Fusion v2 does not download data, repair wind direction, include stress-test current farm-years, promote Baltic daily currents to event-scale evidence, import FINO observations, rerun NORA3, promote legacy CMEMS current CSVs, source-fuse variables beyond accepted confidence layers, or make calibrated `P(operation | weather)` claims.

## Consequences

Fusion v2 is modelling-ready for sensitivity analysis, not a final probability model. The recommended first modelling comparisons are:

- Fusion v1 wave-only.
- Fusion v2 wave plus wind speed.
- Fusion v2 wave plus event-scale NWS current.
- Fusion v2 wave plus wind speed plus event-scale NWS current.
- High-confidence subsets only.

Wind direction remains sensitivity-only because Wind Confidence v1 found only `197` usable direction events. Current-aware modelling remains NWS-domain-biased because Current Confidence v1 provides `16,307` event-scale current events and preserves non-covered events as missing. Shallow bathymetry warnings remain explicit caveats, especially for later stress-test current increments.

Accepted Fusion v2 validation results:

- Input dwell rows: `92,660`
- Output rows: `92,660`
- Duplicate output `dwell_id` rows: `0`
- Wave-ready rows: `83,901`
- Wind-speed-ready rows: `75,380`
- Wind-direction-ready rows: `197`
- Event-scale-current rows: `16,307`
- Wave + wind speed + current rows: `13,207`
- High-confidence multivariate rows: `9,337`
- Tier A wave + wind speed + current rows: `4,552`
- High-confidence Tier A rows: `3,402`

The next approved modelling increment should use Fusion v2 to test whether adding wind speed and event-scale current materially changes the observed workability envelope relative to wave-only Fusion v1. Targeted NORA3 wind-direction repair and stress-test current farm-years remain separate future decisions.
