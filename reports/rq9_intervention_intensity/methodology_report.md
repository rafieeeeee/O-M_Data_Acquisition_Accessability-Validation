# RQ9 Farm-Level Maintenance Intervention Intensity Report

**Analysis label:** RQ9 farm-level maintenance intervention intensity

This report describes farm-level maintenance intervention intensity from existing AIS dwell behaviour and AIS backfill coverage metadata. It is not failure rate. A vessel visit is not automatically a failure, and true failure-rate inference requires SCADA, fault logs, work orders, or equivalent validation.

## Inputs

- Existing AIS dwell/weather feature table.
- Existing AIS backfill manifest.
- Existing turbine coordinate table, used only for farm-level turbine counts.
- No AIS extraction rerun.
- No metocean extraction rerun.

## Denominator Policy

- `success` and `success_no_ais_in_bbox` count as observed months.
- `success_no_ais_in_bbox` is observed zero activity, not missing data.
- `skipped_missing_source` is excluded from the observed denominator.

## Numerator Policy

- Tier A and Tier B dwells are candidate intervention evidence, not fault labels.
- Long dwells are Tier A/B candidate interventions at or above the configured duration threshold.
- Duplicate groups are adjusted through derived fractional counts without destructive deletion.

## Output Inventory

- Farm intensity output: `/Volumes/Extreme SSD/01_ACTIVE_PROJECTS/O&M_Data_Acquisition_Accessability&Validation/Data/Processed/analysis/rq9_intervention_intensity/farm_intervention_intensity.csv`
- Validation summary: `/Volumes/Extreme SSD/01_ACTIVE_PROJECTS/O&M_Data_Acquisition_Accessability&Validation/reports/rq9_intervention_intensity/validation_summary.csv`
- Methodology report: `/Volumes/Extreme SSD/01_ACTIVE_PROJECTS/O&M_Data_Acquisition_Accessability&Validation/reports/rq9_intervention_intensity/methodology_report.md`

## Summary Metrics

- Farm rows: 113
- Observed farm-years: 1695.000
- Candidate intervention count: 17565
- Tier A count: 15264
- Tier B count: 2301
- Long dwell count: 15227
- Duplicate adjustment available: True

## Guardrails

- Do not call this output failure rate.
- Do not treat Tier A/B AIS behaviour as confirmed failures.
- Do not use fallback or synthetic current evidence for RQ9 validation.
- Do not start Stage 2 workability work from these outputs.
