# Increment Spec: Fusion v2 Evidence-Readiness Audit

## Purpose

Define a report-only audit that determines whether Fusion v2 is fit to support
later Stage 2 sensitivity comparisons. The audit must answer:

> Are the Fusion v2 wave/wind/current/bathymetry evidence slices sufficiently
> complete, balanced, and interpretable to support later observed
> workability-envelope comparisons, before any calibrated probability model?

This increment is readiness-only. It does not compare observed workability
envelopes, train models, calibrate `P(operation | weather)`, or change any
accepted evidence product.

## Scope

In scope:

- Read accepted local evidence only.
- Use Fusion v2 as the primary audit input.
- Cross-check accepted source layers and validation reports where useful.
- Check row identity, duplicate dwell identity, and readiness subset counts.
- Compute counts and shares for Tier A and all canonical readiness subsets.
- Quantify farm, year, and source-domain concentration diagnostics.
- Compare current-ready rows with current-missing rows for coverage bias.
- Confirm wind direction remains quarantined from primary Stage 2 predictors.
- Confirm missing current remains null/missing and is never treated as zero.
- Report depth-warning sensitivity, especially `depth_warning_le_10m`.
- Summarize wave, wind, and current confidence-class distributions.
- Produce a final recommendation: `proceed_to_stage2`,
  `proceed_with_restrictions`, or `repair_evidence_first`.

Out of scope:

- No Stage 2 model training.
- No observed-envelope comparison.
- No calibrated `P(operation | weather)` model.
- No new data downloads.
- No FINO import.
- No NORA3 repair.
- No wind-direction repair.
- No current stress-test expansion.
- No rebuild or mutation of existing Fusion v2, confidence-layer,
  dwell-weather, bathymetry, or source archive products.
- No broad documentation rewrite.

## Assumptions

- Branch: `codex/fusion-v2-evidence-readiness-audit`.
- Base: updated `main` at `077ae0d`.
- Governance branch has been merged.
- No audit implementation has started before this spec.
- Fusion v2 is the canonical readiness input:
  `Data/Processed/metocean/fusion_v2/dwell_metocean_fusion_v2.parquet`.
- Confidence-layer files and existing validation reports are read-only
  cross-check inputs, not a reason to rebuild Fusion v2 joins or create a new
  source-fused table.
- Hard-coded volatile row-count constants should be avoided in implementation
  unless parsed from canonical validation reports or a single registry.

## Accepted Inputs

Primary input:

- `Data/Processed/metocean/fusion_v2/dwell_metocean_fusion_v2.parquet`

Accepted read-only cross-check inputs:

- `Data/Processed/ais_dwell_backfill/cross_farm_dwell_weather_features.parquet`
- `Data/Processed/metocean/fusion_v1_source_agreement/wave_event_confidence.parquet`
- `Data/Processed/metocean/wind_confidence_v1/wind_event_confidence.parquet`
- `Data/Processed/metocean/current_confidence_v1/current_event_confidence.parquet`
- `Data/Processed/metocean/bathymetry/site_bathymetry_points.parquet`
- Existing Fusion v2 and confidence-layer validation reports, used only for
  canonical count cross-checks where useful.

## Canonical Readiness Flags

The audit must use these exact Fusion v2 columns for subset checks:

- `model_ready_wave_only`
- `model_ready_wave_wind`
- `model_ready_wave_current`
- `model_ready_wave_wind_current`
- `model_ready_high_confidence`

Implementation should consume these flags as canonical and separately verify
that their observed semantics still match ADR 0029 and the Fusion v2 validation
report.

## Source-Domain And Basin Policy

Fusion v2 does not contain a basin column. The audit must avoid vague
"basin if available" language and use one of these explicit approaches:

- Report farm, year, and source-domain concentration only, where source-domain
  is derived from existing source and confidence fields.
- Or derive a clearly caveated `sea_basin_geographic` from accepted coordinate
  or farm metadata, stating that it is a geographic grouping only, not a
  physical exposure, observability, reliability, or causal variable.

If the implementation cannot identify an accepted source for
`sea_basin_geographic`, it must omit basin-style claims and record the omission
as a caveat rather than borrowing RQ9-derived basin outputs.

## Expected Outputs

Future implementation should write:

- `reports/fusion_v2_evidence_readiness/readiness_report.md`
- `Data/Processed/metocean/fusion_v2_evidence_readiness/readiness_summary.json`
- Tabular coverage, missingness, and bias outputs as Parquet and/or CSV.

The `readiness_summary.json` must contain at least:

- final recommendation,
- timestamp or run metadata,
- input paths,
- key caveats,
- readiness subset counts,
- Tier A subset counts,
- top farm concentration,
- top 5 farm concentration,
- year concentration,
- current-ready versus current-missing summary,
- wind-direction quarantine result,
- depth-warning sensitivity result.

## Required Future Checks

1. Row identity and duplicate dwell checks.
2. Subset counts using canonical Fusion v2 readiness flags.
3. Tier A counts for all readiness subsets.
4. Farm, year, and source-domain concentration diagnostics:
   - top farm share,
   - top 5 farm share,
   - top year share,
   - sparse farm/year warnings where relevant.
5. Current-ready versus current-missing bias:
   - farm/year concentration for current-ready rows,
   - farm/year concentration for current-missing rows,
   - whether current-ready evidence is too NWS-domain-biased for headline
     claims.
6. Wind evidence:
   - wind speed readiness,
   - wind direction readiness,
   - explicit recommendation that wind direction remains excluded from primary
     Stage 2 predictors unless a later repair increment is approved.
7. Missing current:
   - missing current remains null/missing,
   - no code path may coerce missing current to zero.
8. Depth-warning sensitivity:
   - rows with `depth_warning_le_10m`,
   - subset counts before and after excluding depth-warning rows,
   - explicit caveat for shallow/coastal interpretation.
9. Confidence-class distributions:
   - wave confidence,
   - wind confidence,
   - current confidence.
10. Claim-boundary audit:
    - Fusion v2 is provisional/readiness input, not a final model,
    - no calibrated access-probability claim,
    - FINO remains validation/planning only,
    - wind direction remains sensitivity-only,
    - Baltic daily/contextual current must not be promoted to event-scale
      current,
    - missing current remains missing, not zero.

## Decision Criteria

Avoid arbitrary hard thresholds. Use concentration diagnostics and evidence
caveats to justify the recommendation. The final report must choose exactly one
recommendation:

- `proceed_to_stage2`: use only if the evidence is broad enough across farms,
  years, tiers, and confidence classes that Stage 2 comparisons are not
  dominated by a narrow subset.
- `proceed_with_restrictions`: use if Stage 2 can proceed only with constrained
  claims, such as Tier A only, wave-only headline with wind/current sensitivity
  only, NWS-domain-only current comparisons, wind-speed-only models, wind
  direction excluded from primary predictors, or required depth-warning
  exclusion/sensitivity.
- `repair_evidence_first`: use if multivariate evidence is too sparse, too
  concentrated, or too biased to support meaningful Stage 2 sensitivity
  comparisons.

## Future Implementation Shape

Specify this shape for the later implementation increment, but do not implement
it in this spec-only commit:

- Core logic: `src/om_pipeline/analysis/fusion_v2_evidence_readiness.py`
- CLI wrapper: `scripts/build_fusion_v2_evidence_readiness.py`
- Tests: `tests/test_fusion_v2_evidence_readiness.py`

Future tests should cover:

- canonical readiness flag subset definitions,
- row identity and duplicate logic,
- missing current remains null and is not converted to zero,
- wind direction usability only under direction-ready confidence,
- high-confidence subset logic,
- depth-warning sensitivity logic,
- deterministic summary JSON creation from small synthetic fixtures.

## Validation Plan

For this spec-only commit:

- `git diff --check`
- `/opt/anaconda3/bin/python scripts/context_sweep.py`

For the later implementation:

- `/opt/anaconda3/bin/python scripts/build_fusion_v2_evidence_readiness.py`
- `/opt/anaconda3/bin/python -m pytest tests/test_fusion_v2_evidence_readiness.py`
- `/opt/anaconda3/bin/python scripts/context_sweep.py`
- `git diff --check`

## Documentation / Context Plan

This spec-only commit updates only the increment index and docs discoverability.
Do not update `CONTEXT.md`, `start_here`, `docs/roadmap.md`, ADRs, tests, or
branch-exit notes unless validation exposes a direct issue.

After the audit implementation runs, update derived context only if the result
changes project state, evidence boundaries, or next-step direction.

## Review Notes

- Implementation should wait until this spec has been reviewed.
- The audit should consume Fusion v2 flags as canonical and verify their
  semantics rather than rebuilding Fusion v2 joins.
- The audit should record when basin-style grouping is omitted or only
  geographic, preventing unsupported exposure, observability, reliability, or
  causal claims.

## Acceptance

This spec-only increment is accepted when:

- purpose, scope, inputs, outputs, validation, and decision criteria are
  explicit,
- the spec blocks audit implementation before review,
- only the spec/index/discoverability files are changed,
- `git diff --check` passes,
- `scripts/context_sweep.py` passes,
- the branch is clean after commit.
