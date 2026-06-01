# RQ9 End-of-Day Handoff

## Status

The RQ9 turbine-feasibility workstream is ready for local merge after validation. Outputs are AIS-derived maintenance intervention intensity, not confirmed failure rate. A vessel visit is not automatically a failure; confirmed failure inference still requires SCADA, fault logs, work orders, or equivalent validation.

Completed increments:

- Farm-level phase-separated intervention intensity.
- Turbine-level feasibility v0 with nearest-turbine Tier A assignment.
- Turbine denominator/exposure v1 with commissioning-aware steady-operational observed turbine-years.
- Turbine characteristics comparison v1 across OEM, model, capacity, rotor diameter, hub height, age, sea basin, country, and farm.
- Sea-basin observability and controlled-comparison audits.
- AIS observability bias audit.

Stage 2 workability has not started.

## Current Caveats

- The Baltic/North Sea gap is strong in the AIS-derived intervention-intensity proxy, but it is not causal or thesis-safe as an operational/reliability signal.
- `sea_basin` is a geographic grouping label, not a physical exposure metric.
- The gap remains entangled with farm/country structure, OEM/capacity/age composition, vessel concentration, assignment geometry, and AIS observability bias.
- CTV/SOV detectability bias is plausible but not proven because vessel registry/access fields remain effectively unavailable in the current event evidence.
- Direct AIS receiver/source geometry is unavailable in the current RQ9 tables: no receiver station, terrestrial/satellite flag, receiver coordinates, or receiver-distance field was found.

## AIS Observability Audit

The audit uses proxy controls already available locally:

- observed source coverage share,
- `success_no_ais_in_bbox` observed-zero share,
- `skipped_missing_source` share,
- Tier A high-confidence assignment share,
- high+medium assignment sensitivity,
- top-5 MMSI concentration,
- zero-event turbine share,
- dwell duration and Tier A/B evidence.

Baseline Baltic/North Sea primary AIS-derived intervention-intensity ratio remains about 6.3x. Under available proxy controls the gap remains visible, but those controls cannot prove equal detectability across receiver networks or vessel access modes.

## Outputs To Start From

- `Data/Processed/analysis/rq9_intervention_intensity/farm_intervention_intensity.csv`
- `Data/Processed/analysis/rq9_intervention_intensity/turbine_intervention_events_v0.csv`
- `Data/Processed/analysis/rq9_intervention_intensity/turbine_exposure_denominator.csv`
- `Data/Processed/analysis/rq9_intervention_intensity/turbine_intervention_intensity_v1.csv`
- `Data/Processed/analysis/rq9_intervention_intensity/turbine_characteristics_rates.csv`
- `reports/rq9_intervention_intensity/turbine_characteristics_report.md`
- `reports/rq9_intervention_intensity/sea_basin_observability_audit.md`
- `reports/rq9_intervention_intensity/sea_basin_controlled_comparison_report.md`
- `reports/rq9_intervention_intensity/ais_observability_bias_audit.md`

## Recommended Next Research Question

Does intervention intensity vary by turbine age/capacity/OEM after controlling for farm, basin, and observability?

Recommended next increment:

1. Preserve receiver-distance/offshore-distance as a required missing control for basin interpretation.
2. Enrich top contributing MMSIs with registry-backed vessel category, dimensions, and access technology where evidence exists.
3. Re-run age/capacity/OEM comparisons with farm, basin, and observability controls.
4. Keep simulator use confidence-weighted and avoid treating AIS-derived intervention intensity as confirmed reliability evidence.
