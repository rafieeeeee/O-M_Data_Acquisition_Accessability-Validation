# ADR 0023: FINO Validation Access Planning

## Status

Accepted

## Context

The source-agnostic metocean roadmap needs in-situ validation evidence before
the final dwell-metocean resolver is rebuilt. FINO1, FINO2, and FINO3 provide
fixed-platform measurements near German North Sea and Baltic offshore wind
clusters, but local FINO data files are not yet present in a usable archive.

The public FINO and BSH pages show that FINO data access is mediated through
BSH-Login and the Insitu specialist procedure. That means the next safe
technical step is metadata/access planning and station-to-farm proximity
matching, not bulk time-series import.

## Decision

Implement a dry-run-only FINO metadata/access planner:

- Core planning logic lives in `src/om_pipeline/metocean/fino_metadata_planner.py`.
- The CLI wrapper is `scripts/plan_fino_metadata_access.py`.
- The first report is
  `analysis/06_rq6_metocean_spatial_resolution/fino_metadata_access_plan.md`.
- FINO is a validation and baseline evidence layer, not an automatic farm-wide
  primary metocean source.
- Station-to-farm proximity is computed from the accepted common metocean
  sample points in
  `Data/Processed/metocean/bathymetry/site_bathymetry_points.parquet`.
- The planner writes only the Markdown report. It does not download FINO data,
  import time series, create currents, source-fuse variables, interpolate to the
  10-minute backbone, rerun NORA3, or rebuild dwell-metocean feature tables.

## Consequences

The project now has a reproducible path for choosing a small FINO pilot once
BSH Insitu access is available. FINO1 is the strongest first candidate because
it is very close to Alpha Ventus and other German Bight farms already present in
the project sample-point set.

The tradeoff is that variable names, exact measurement heights/depths, file
formats, QC columns, and licence wording remain unverified until portal access
is granted and a small native export is inspected.
