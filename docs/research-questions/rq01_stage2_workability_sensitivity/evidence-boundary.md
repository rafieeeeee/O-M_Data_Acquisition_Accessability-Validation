# RQ01 Evidence Boundary

## Accepted Evidence

RQ01 uses accepted Fusion v2 evidence and its readiness audit as the basis for
future observed-envelope sensitivity analysis.

Accepted primary input:

- `Data/Processed/metocean/fusion_v2/dwell_metocean_fusion_v2.parquet`

Accepted readiness and boundary inputs:

- `reports/fusion_v2_evidence_readiness/readiness_report.md`
- `Data/Processed/metocean/fusion_v2_evidence_readiness/readiness_summary.json`
- `reports/metocean_fusion_v2/fusion_v2_validation_report.md`
- `docs/agent-handoff-metocean-fusion-v2.md`
- `docs/adr/0016-empirical-workability-surface-modeling.md`
- `docs/adr/0029-metocean-fusion-v2-multiparameter-event-features.md`
- `CONTEXT.md`

## Required Boundaries

- Stage 2 has not started in this spec branch.
- Fusion v2 is provisional/readiness evidence, not a final probability model.
- Wave-only and wave+wind-speed lanes may be primary observed-envelope
  sensitivity lanes.
- Current-aware comparisons are NWS-domain and coverage-limited sensitivity
  only.
- Wind direction is excluded from primary predictors.
- Missing current remains missing and must not be treated as zero.
- Depth-warning exclusion or sensitivity treatment is required.
- No calibrated `P(operation | weather)` claim is supported.

## Excluded Evidence

- FINO time-series observations.
- New SCADA/DPR labels.
- Repaired wind direction.
- Expanded current stress-test farm-years.
- Rebuilt Fusion v2 joins.
- New downloads.
- Mutated accepted evidence products.

## Targeted Unblocker Policy

Additional data-source work may proceed later only when tied to a named blocked
claim or validation need. FINO, AIS receiver geometry/proxies, wind-direction
repair, and current stress-test expansion are targeted unblockers, not general
preconditions for RQ01 restricted sensitivity analysis.
