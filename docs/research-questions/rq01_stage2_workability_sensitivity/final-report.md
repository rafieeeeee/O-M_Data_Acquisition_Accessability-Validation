# RQ01 Final Report

Status: Result accepted.

The accepted generated report is:

```text
reports/rq01_stage2_workability_sensitivity/sensitivity_report.md
```

The accepted processed outputs are:

```text
Data/Processed/analysis/rq01_stage2_workability_sensitivity/
```

## Scope Completed

RQ01 completed a restricted descriptive Stage 2 sensitivity screen using
accepted Fusion v2 event features. It compared observed Tier A workability
envelope lanes for wave-only, wave + wind speed, current-aware sensitivity,
wave + wind speed + current sensitivity, high-confidence multivariate evidence,
and depth-warning subsets.

## Accepted Finding

Wave + wind speed shows no material screened difference from wave-only, while
current-aware and high-confidence multivariate lanes show material screened
differences.

Current-aware findings are NWS-domain / coverage-limited sensitivity only, not
general European causal evidence.

## Claim Boundary

- This is observed-envelope descriptive sensitivity only.
- No calibrated `P(operation | weather)` model has been built or accepted.
- No model training was performed.
- Wind direction is excluded from primary predictors.
- Missing current remains missing and is not interpreted as zero.
- Fusion v2 was not rebuilt.
- FINO was not imported.

## Caveats

- Current-aware lanes are NWS-domain / coverage-limited sensitivity results.
- High-confidence multivariate results are narrower than wave-only evidence and
  should not be generalized as a full-domain conclusion.
- Depth-warning exclusion/sensitivity remains required.
- The result does not establish causality or confirmed operation success/failure.

## Validation

The implementation branch validation passed:

```text
/opt/anaconda3/bin/python scripts/build_rq01_stage2_workability_sensitivity.py
/opt/anaconda3/bin/python -m pytest tests/test_rq01_stage2_workability_sensitivity.py
/opt/anaconda3/bin/python scripts/context_sweep.py
git diff --check
git diff --cached --check
```

## Next Steps

Use this accepted result as the basis for thesis-facing interpretation/reporting
or a follow-on Stage 2 analysis that explains why current-aware lanes differ.
Do not move to calibrated access-probability modelling without new accepted
operation-success labels and a separate spec.
