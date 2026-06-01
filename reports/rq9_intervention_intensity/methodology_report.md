# RQ9 Farm-Level Maintenance Intervention Intensity Report

**Analysis label:** RQ9 farm-level maintenance intervention intensity

This report describes farm-level maintenance intervention intensity from existing AIS dwell behaviour and AIS backfill coverage metadata. It is not failure rate. A vessel visit is not automatically a failure, and true failure-rate inference requires SCADA, fault logs, work orders, or equivalent validation.

## Inputs

- Existing AIS dwell/weather feature table.
- Existing AIS backfill manifest.
- Existing turbine coordinate table, used for farm-level turbine counts and commissioning-derived lifecycle phases.
- No AIS extraction rerun.
- No metocean extraction rerun.

## Denominator Policy

- `success` and `success_no_ais_in_bbox` count as observed months.
- `success_no_ais_in_bbox` is observed zero activity, not missing data.
- `skipped_missing_source` is excluded from the observed denominator.
- When commissioning dates are available, manifest months are split into `pre_operational`, `commissioning_ramp_up`, and `steady_operational` phases.
- The default ramp-up buffer is 6 months after the latest parsed turbine commissioning month.
- Pre-operational months are excluded from the operational denominator.
- Commissioning/ramp-up months are reported separately from steady-operational months.
- If commissioning metadata is missing, AIS source coverage is used as a `unknown_phase` fallback denominator and confidence is lowered.

## Numerator Policy

- Tier A and Tier B dwells are candidate intervention evidence, not fault labels.
- Candidate dwell rows are split into pre-operational, commissioning/ramp-up, steady operational, and unknown phases.
- Pre-operational candidates are retained as `pre_operational_candidate_count`.
- Commissioning/ramp-up candidates are separated from steady operational candidates because early-life work can reflect testing, handover, snagging, warranty work, and campaign activity rather than mature maintenance demand.
- Long dwells are Tier A/B candidate interventions at or above the configured duration threshold.
- Duplicate groups are adjusted through derived fractional counts without destructive deletion.
- `steady_intervention_intensity_per_farm_year` is the simulator-facing provisional source. Commissioning activity should be modelled separately or excluded from generic mature-operational demand multipliers.

## Output Inventory

- Farm intensity output: `/Volumes/Extreme SSD/01_ACTIVE_PROJECTS/O&M_Data_Acquisition_Accessability&Validation/Data/Processed/analysis/rq9_intervention_intensity/farm_intervention_intensity.csv`
- Validation summary: `/Volumes/Extreme SSD/01_ACTIVE_PROJECTS/O&M_Data_Acquisition_Accessability&Validation/reports/rq9_intervention_intensity/validation_summary.csv`
- Methodology report: `/Volumes/Extreme SSD/01_ACTIVE_PROJECTS/O&M_Data_Acquisition_Accessability&Validation/reports/rq9_intervention_intensity/methodology_report.md`

## Summary Metrics

- Farm rows: 113
- Observed farm-years: 986.500
- Commissioning/ramp-up observed farm-years: 44.583
- Steady operational observed farm-years: 941.917
- Observed farm-years range: 0.000 to 15.000
- Operational window known farms: 113
- Operational window unknown farms: 0
- Candidate intervention count: 13545
- Pre-operational candidate count excluded: 4020
- Commissioning/ramp-up candidate count: 537
- Steady operational candidate count: 13008
- Tier A count: 11972
- Tier B count: 1573
- Long dwell count: 11656
- Duplicate adjustment available: True

## Guardrails

- Do not call this output failure rate.
- Do not treat Tier A/B AIS behaviour as confirmed failures.
- Do not use fallback or synthetic current evidence for RQ9 validation.
- Do not start Stage 2 workability work from these outputs.
