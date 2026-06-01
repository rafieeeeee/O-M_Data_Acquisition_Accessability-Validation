# ADR 0025: Metocean Fusion v1 Source Agreement and Confidence

## Status

Accepted.

## Context

Metocean Fusion v0 improved event-level Hs/Tp coverage by selecting sources
with a simple priority rule: Baltic, then NWS, then NORA3. That was useful as a
coverage resolver but is not a defensible final evidence layer because NORA3,
NWS, and Baltic differ in domain coverage, cadence, grid/sample distance, and
near-coastal validity.

## Decision

Add a Fusion v1 source-agreement increment with core logic in
`src/om_pipeline/metocean/metocean_fusion_v1_source_agreement.py` and a thin CLI
wrapper in `scripts/build_metocean_fusion_v1_source_agreement.py`.

Fusion v1 writes:

- `Data/Processed/metocean/fusion_v1_source_agreement/wave_source_candidates.parquet`
- `Data/Processed/metocean/fusion_v1_source_agreement/wave_source_pairwise_agreement.parquet`
- `Data/Processed/metocean/fusion_v1_source_agreement/wave_event_confidence.parquet`
- `reports/metocean_fusion_v1_source_agreement/source_agreement_validation_report.md`

The candidate table keeps NORA3, NWS, and Baltic rows separate, including
explicit missing-source reasons. Pairwise agreement uses transparent thresholds:
strong when Hs differs by at most 0.15 m and Tp by at most 0.75 s; moderate
when Hs differs by at most 0.35 m and Tp by at most 1.50 s; weak otherwise.

Event confidence is selected from source agreement, temporal quality, sample
distance, domain match, variable completeness, and depth warnings. Weighted
Hs/Tp columns are sensitivity diagnostics only and are not treated as truth.

## Non-Decisions

- No current downloads are approved.
- No FINO import is approved.
- No source archive mutation is approved.
- No NORA3 rerun is approved.
- No final production dwell-metocean rebuild is approved.
- No calibrated `P(operation | weather)` claim is made.
- No CTV/SOV vessel role inference is introduced.

## Consequences

Fusion v1 creates a defensible audit layer for RQ6 source sensitivity and RQ12
provenance/uncertainty without prematurely fusing sources. Future current
pilots should reuse the same candidate, agreement, confidence, and
missing-reason framework for u/v current evidence.
