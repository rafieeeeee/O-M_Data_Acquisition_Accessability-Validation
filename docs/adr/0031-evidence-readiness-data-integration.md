# ADR 0031: Evidence Readiness Data Integration Layer

## Status
Accepted

## Context
RQ9 analysis exposed a strong AIS-derived intervention-intensity contrast across
geographies, but the result is entangled with uneven source coverage, missing
direct AIS receiver/source geometry, sparse vessel enrichment, source-domain
metocean coverage, and local-only SCADA validation. Making stronger research
claims before those limits are visible would overstate what the current dataset
can answer.

The project already has processed AIS dwell, RQ9 turbine/farm, Fusion v2,
wind/current confidence, bathymetry, and CARE validation artifacts. The needed
increment is not new extraction or modelling; it is a foundation audit that
integrates existing evidence and preserves missingness semantics.

## Decision
1. Add an evidence-readiness layer under
   `Data/Processed/analysis/evidence_readiness/` with farm-month,
   turbine-month, and RQ readiness matrices.
2. Add methodology and reporting under `analysis/00_data_foundation/` and
   `reports/evidence_readiness/`.
   The builder is the source of truth for the generated report and ignored
   processed matrices.
3. Treat `success` and `success_no_ais_in_bbox` as observed AIS source coverage.
   Treat `skipped_missing_source` as missing source evidence, not zero-event
   evidence.
4. Keep AIS visits as candidate intervention evidence only. The readiness layer
   must not promote AIS-only evidence into confirmed failure interpretation.
5. Separate source availability from detection quality. Direct AIS receiver or
   source geometry is absent unless receiver station, terrestrial/satellite
   channel, receiver coordinates, or equivalent fields exist in the integrated
   evidence tables.
6. Mark SCADA validation as local to the CARE Wind Farm B/C mappings rather than
   a broad European denominator.

## Consequences
- Later RQ work can start from explicit answerability classes instead of
  rediscovering coverage caveats.
- Geographic comparisons remain available as observability audits, but not as
  reliability claims.
- Missing source months, missing currents, missing wind direction, and missing
  vessel metadata remain visible rather than being silently converted to zeros.
- The first RQ-ready path is source-aware metocean sensitivity/readiness work
  (RQ6) only; RQ9 remains blocked for failure claims until receiver/source
  controls and fault/work-order validation exist.
