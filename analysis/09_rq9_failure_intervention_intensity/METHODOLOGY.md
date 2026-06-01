# RQ9 Intervention Intensity Methodology

## Scope

RQ9 is the primary research question for this workstream. The purpose is to estimate offshore wind farm and turbine maintenance intervention intensity from observed AIS dwell behaviour, with explicit uncertainty and validation status.

RQ12 is secondary: these intensity estimates are intended to provide candidate inputs for immediate O&M simulation modelling, especially farm-level maintenance-demand multipliers and turbine-level intervention-intensity modifiers.

RQ4 and RQ8 are later extensions only. RQ4 may use repeated or incomplete intervention patterns as evidence for access disruption, and RQ8 may use validated intervention intensity as a prioritisation input for value or wake-adjusted modelling. Neither extension is part of this first methodology increment.

This is maintenance intervention intensity, not confirmed failure rate. A vessel visit is not automatically a failure. True failure rate inference requires SCADA, fault log, work-order, or equivalent maintenance validation.

## Evidence Hierarchy

The methodology separates behavioural AIS evidence from validated fault evidence:

| Level | Label | Interpretation |
| --- | --- | --- |
| 0 | Observation coverage | Farm-month or turbine-month has usable source coverage metadata, regardless of whether any AIS dwell is present. |
| 1 | Vessel presence | Vessel activity is observed near or inside the farm, but no maintenance intervention is inferred. |
| 2 | Candidate intervention | AIS dwell matches intervention-like spatial and behavioural criteria, such as Tier A asset-proximal or Tier B farm-internal dwell. |
| 3 | Repeated/long intervention | Candidate intervention is strengthened by long dwell duration or repeated visits to the same farm or turbine context. |
| 4 | SCADA-linked intervention | Candidate intervention overlaps with SCADA status, alarm, event, or maintenance-labelled windows. |
| 5 | Confirmed failure | External validation confirms a fault or failure through SCADA fault logs, work orders, maintenance records, or equivalent evidence. |

Levels 0-3 support intervention intensity. Levels 4-5 are required before any result may be described as a validated maintenance event or confirmed failure.

## Denominator Policy

Denominators must be observation-aware. They define the exposure window for rates and are not simply calendar time.

- Farm observation months count farm-months where the AIS backfill manifest reports `success` or `success_no_ais_in_bbox`.
- `success_no_ais_in_bbox` is an observed-zero month: source coverage was available and no qualifying AIS activity was observed in the bounding box.
- `skipped_missing_source` is excluded from the observed denominator. It is missing source coverage, not zero activity.
- Farm observation years are farm observation months divided by 12, with optional stratification by year when sensitivity checks require it.
- Turbine observation months inherit farm-level source coverage and are constrained by turbine commissioning or operational windows where turbine metadata is available.
- Turbine observation years are turbine observation months divided by 12 after commissioning and operational-window filtering.
- Missing source coverage must be preserved as missingness or coverage adjustment, not silently treated as zero intervention activity.

## Numerator Policy

Numerators must preserve the distinction between vessel presence, candidate intervention, likely intervention, SCADA-linked maintenance, and confirmed failure.

- Tier A count: asset-proximal dwell visits meeting the accepted Tier A definition.
- Tier B count: farm-internal dwell visits meeting the accepted Tier B definition.
- Long dwell count: Tier A or Tier B candidate interventions whose duration exceeds a configured long-dwell threshold.
- Repeated visit count: repeated candidate interventions to the same turbine where a turbine association is available, otherwise repeated candidate interventions to the same farm within a configured time window.
- Unique vessel count: distinct vessels contributing candidate interventions for a farm, turbine, year, or analysis period.
- SCADA-linked count: candidate interventions linked to available SCADA status, alarm, event, or maintenance labels. This count remains validation evidence unless it confirms a fault/failure.
- Duplicate-group handling: cross-farm duplicate groups may be flagged, grouped, weighted, or sensitivity-filtered in derived analysis, but dwell outputs must not be destructively deduplicated.

## Planned Outputs

The first implementation will build derived analysis products only:

- `Data/Processed/analysis/rq9_intervention_intensity/farm_intervention_intensity.csv`
- `Data/Processed/analysis/rq9_intervention_intensity/turbine_intervention_intensity.csv`
- `reports/rq9_intervention_intensity/validation_summary.csv`
- `reports/rq9_intervention_intensity/methodology_report.md`

These outputs are not created in this methodology/spec commit.

## First Implementation Increment

The first implementation increment should add only thin, testable code for deriving intervention-intensity inputs from existing committed datasets:

- `src/om_pipeline/analysis/rq9_intervention_intensity.py`
- `scripts/build_rq9_intervention_intensity.py`
- `tests/test_rq9_intervention_intensity.py`

The module should contain the business logic for denominator construction, numerator aggregation, duplicate-group policy, confidence classes, and validation summaries. The script should remain a thin CLI wrapper around the package module. Tests should cover observed-zero month handling, missing-source exclusion, commissioning-window filtering, duplicate-group treatment, repeated-visit logic, and SCADA-linked validation summaries.

## Guardrails

- Do not call AIS visits failures.
- Do not infer true failure rate without SCADA, fault-log, work-order, or equivalent validation.
- Do not rerun AIS extraction.
- Do not rerun metocean extraction.
- Do not modify raw, interim, or processed data in this step.
- Do not use fallback or synthetic current evidence for RQ9 validation.
- Do not destructively deduplicate dwell outputs.
- Do not start Stage 2 workability implementation as part of this workstream.
