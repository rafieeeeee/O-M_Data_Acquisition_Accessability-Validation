# RQ01 Decision Log

## 2026-06-04: Spec Drafted

- RQ01 registered as the first Stage 2 Fusion v2 workability sensitivity
  question.
- Scope limited to a spec-only branch.
- Future implementation must remain observed-envelope sensitivity, not
  calibrated `P(operation | weather)`.
- Future current-aware comparisons must be labelled NWS-domain and
  coverage-limited.
- Future wind-direction use is excluded from primary predictors.
- No analysis, modelling, generated outputs, downloads, repairs, FINO import,
  or Fusion v2 rebuilds are included in this spec branch.

## 2026-06-04: Restricted Implementation Generated For Review

- RQ01 implementation branch generated restricted descriptive sensitivity
  outputs under `reports/rq01_stage2_workability_sensitivity/` and
  `Data/Processed/analysis/rq01_stage2_workability_sensitivity/`.
- Scope remains observed-envelope sensitivity only; no model training,
  calibrated `P(operation | weather)`, downloads, FINO import, Fusion v2
  rebuild, wind/current repair, or missing-current-to-zero interpretation.
- Generated outputs are pending review. They are not yet accepted final RQ
  findings or durable project-context conclusions.
