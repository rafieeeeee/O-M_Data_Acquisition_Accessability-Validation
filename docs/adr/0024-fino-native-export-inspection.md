# ADR 0024: FINO Native Export Inspection

## Status

Accepted

## Context

FINO1, FINO2, and FINO3 are now planned as validation and baseline evidence,
but their portal export schemas remain unverified until BSH Insitu access is
granted. The project needs a small, safe tool that can inspect the first manual
CSV/ASCII export before any import code writes a processed FINO archive.

## Decision

Implement a dry-run FINO export inspector:

- Core logic lives in `src/om_pipeline/metocean/fino_export_inspector.py`.
- The CLI wrapper is `scripts/inspect_fino_export.py`.
- The first intended report path is
  `analysis/06_rq6_metocean_spatial_resolution/fino1_export_inspection_report.md`.
- The inspector accepts one manually exported CSV/ASCII file and reports
  delimiter/header detection, timestamp parsing, inferred cadence, likely
  `Hs`, `Tp`, and wave-direction columns, unit/QC detection, physical-range
  warnings, and a canonical mapping proposal.
- Inspection mode writes only a Markdown report. It does not create processed
  FINO parquet, download data, scrape the BSH portal, handle credentials,
  interpolate observations, source-fuse variables, download currents, rerun
  NORA3, or rebuild dwell-metocean feature tables.

## Consequences

The project can validate the first FINO1 wave-slice export immediately after
human access is granted. This reduces import risk without blocking current
planning work. A later FINO import pilot must still be separately approved once
the native export report confirms exact columns, units, QC flags, timestamps,
cadence, and licence/source acknowledgement wording.
