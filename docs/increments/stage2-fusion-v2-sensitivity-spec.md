# Increment Spec: Stage 2 Fusion v2 Workability Sensitivity

## Purpose

Draft the first Stage 2 sensitivity analysis plan under the research-question
register. This is a spec-only increment. It defines how a later implementation
will test:

> Do wind speed and event-scale current materially change the observed Tier A
> workability envelope relative to wave-only Fusion v1/Fusion v2 evidence?

This increment does not run analysis, train models, compare envelopes, write
generated outputs, rebuild Fusion v2, download data, import FINO, or repair
wind/current evidence.

## Scope

In scope:

- Create the RQ01 analysis-plan scaffold.
- Define the accepted inputs and excluded inputs.
- Define the Stage 2 claim boundary.
- Define future sensitivity lanes and validation expectations.
- Mark RQ01 as `Spec drafted` in the RQ register.

Out of scope:

- No calibrated `P(operation | weather)` claim.
- No model training.
- No generated reports or processed data products.
- No creation of empty `reports/` or `Data/Processed/` output directories.
- No wind direction primary predictor.
- No missing-current-to-zero interpretation.
- No Fusion v2 rebuild.
- No FINO import.
- No wind-direction repair.
- No current stress-test expansion.
- No downloads.

## Accepted Inputs For Future Implementation

Primary future input:

- `Data/Processed/metocean/fusion_v2/dwell_metocean_fusion_v2.parquet`

Accepted read-only context and validation inputs:

- `reports/fusion_v2_evidence_readiness/readiness_report.md`
- `Data/Processed/metocean/fusion_v2_evidence_readiness/readiness_summary.json`
- `reports/metocean_fusion_v2/fusion_v2_validation_report.md`
- `docs/agent-handoff-metocean-fusion-v2.md`
- `docs/adr/0016-empirical-workability-surface-modeling.md`
- `docs/adr/0029-metocean-fusion-v2-multiparameter-event-features.md`
- `CONTEXT.md`

The future implementation may read accepted Fusion v1/Fusion v2 wave evidence
for wave-only comparison semantics, but it must not rebuild source joins or
mutate accepted evidence products.

## Future Sensitivity Lanes

Primary lanes:

- Tier A wave-only observed envelope.
- Tier A wave plus wind-speed-ready observed envelope.

Restricted lanes:

- Tier A wave plus event-scale current, labelled NWS-domain and
  coverage-limited.
- Tier A wave plus wind speed plus event-scale current, labelled NWS-domain and
  coverage-limited.
- High-confidence multivariate subset.
- Depth-warning excluded and depth-warning sensitivity subsets.

Wind direction is excluded from primary predictors. Missing current remains
missing and must never be interpreted as zero current.

## Expected Future Outputs

Documented future output paths:

- `reports/rq01_stage2_workability_sensitivity/`
- `Data/Processed/analysis/rq01_stage2_workability_sensitivity/`

This spec-only branch must not create those directories unless the repository
later adopts a `.gitkeep` convention.

## Future Validation Expectations

The implementation plan must include:

- row identity checks against Fusion v2;
- Tier A subset checks;
- readiness flag checks for wave-only, wave+wind-speed, wave+current,
  wave+wind+current, and high-confidence subsets;
- null-current checks;
- wind-direction exclusion checks;
- depth-warning exclusion/sensitivity checks;
- output schema checks for generated tables;
- report claim-boundary checks;
- context sweep.

## Documentation / Context Plan

This spec-only increment updates the RQ register and RQ01 planning files. It
does not update `CONTEXT.md`, `docs/roadmap.md`, or the Fusion v2 handoff unless
validation exposes a direct discoverability problem.

Future implementation must update context or roadmap files only after results
are accepted and project meaning changes.

## Acceptance

This spec-only increment is accepted when:

- RQ01 has an analysis plan, evidence boundary, decision log, and pending final
  report scaffold;
- no findings, recommendations, pseudo-results, or generated outputs are added;
- future output paths are documented but not created;
- context sweep passes;
- `git diff --check` and `git diff --cached --check` pass;
- the branch is committed and ready for spec review.
