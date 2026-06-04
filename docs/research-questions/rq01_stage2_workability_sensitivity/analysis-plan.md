# RQ01 Analysis Plan: Stage 2 Fusion v2 Workability Sensitivity

## Research Question

Do wind speed and event-scale current materially change the observed Tier A
workability envelope relative to wave-only Fusion v1/Fusion v2 evidence?

## Hypothesis

Wind speed may shift or tighten the observed Tier A workability envelope
relative to wave-only evidence. Event-scale current may add useful sensitivity
information, but current-aware claims are expected to remain NWS-domain and
coverage-limited unless later evidence broadens current coverage.

## Accepted Inputs

- `Data/Processed/metocean/fusion_v2/dwell_metocean_fusion_v2.parquet`
- `reports/fusion_v2_evidence_readiness/readiness_report.md`
- `Data/Processed/metocean/fusion_v2_evidence_readiness/readiness_summary.json`
- `reports/metocean_fusion_v2/fusion_v2_validation_report.md`
- `docs/agent-handoff-metocean-fusion-v2.md`
- `docs/adr/0016-empirical-workability-surface-modeling.md`
- `docs/adr/0029-metocean-fusion-v2-multiparameter-event-features.md`
- `CONTEXT.md`

## Excluded Inputs

- FINO observations, because FINO remains validation-planning only.
- Repaired wind-direction evidence, because no repair increment has been
  approved.
- Expanded current stress-test farm-years, because current remains
  coverage-limited sensitivity evidence.
- SCADA/DPR labels, because this RQ is observed-envelope sensitivity, not
  calibrated operation-success modelling.
- Live downloads or source-product rebuilds.

## Claim Boundary

This RQ may compare observed Tier A workability-envelope sensitivity lanes. It
must not claim calibrated `P(operation | weather)`, confirmed access success,
confirmed failure rates, or causal weather effects.

Wave-only and wave+wind-speed lanes may be primary. Current-aware lanes must be
labelled NWS-domain and coverage-limited. Wind direction is excluded from
primary predictors. Depth-warning exclusion or sensitivity treatment is
required.

## Method

Future implementation should compare observed Tier A envelope summaries across:

- wave-only;
- wave plus wind speed;
- wave plus event-scale current;
- wave plus wind speed plus event-scale current;
- high-confidence multivariate subset;
- depth-warning excluded and depth-warning sensitivity subsets.

The method must use accepted Fusion v2 readiness flags and source/confidence
fields rather than rebuilding Fusion v2 joins.

## Validation Checks

- Verify Fusion v2 row identity and required columns.
- Verify Tier A subset membership.
- Verify canonical readiness flags used for each sensitivity lane.
- Verify missing current remains null and is not coerced to zero.
- Verify wind direction is excluded from primary predictors.
- Verify current-aware outputs are labelled NWS-domain / coverage-limited.
- Verify depth-warning exclusion/sensitivity outputs are present.
- Verify generated tables and reports preserve the claim boundary.
- Run context sweep.

## Outputs

Future implementation should write outputs under:

- `reports/rq01_stage2_workability_sensitivity/`
- `Data/Processed/analysis/rq01_stage2_workability_sensitivity/`

This spec-only branch does not create those generated-output directories.

## Caveats

- Current-aware comparisons are limited to accepted NWS event-scale current
  coverage.
- Wind direction remains sparse and excluded from primary predictors.
- Depth-warning rows require exclusion or sensitivity treatment.
- Observed envelopes are not calibrated access probabilities.

## Decision Criteria

The final report should decide whether future Stage 2 implementation can:

- proceed with wave-only and wave+wind-speed as primary lanes;
- include current-aware lanes only as restricted sensitivity;
- require evidence repair before current-aware or directional claims;
- require additional validation before any calibrated probability work.

## Context Files To Update

Update these only after accepted results change project meaning:

- `CONTEXT.md`
- `docs/roadmap.md`
- `start_here/01_project_state_summary.md`
- `docs/agent-handoff-metocean-fusion-v2.md`
- `docs/research-questions/rq-register.md`
