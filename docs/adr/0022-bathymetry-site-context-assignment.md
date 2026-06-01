# ADR 0022: Bathymetry Site-Context Assignment

## Status

Accepted

## Context

The source-agnostic metocean roadmap requires a static site-context layer before
currents, FINO validation, and final dwell-metocean fusion are rebuilt.
Bathymetry should cover the same common metocean sample points used by the
regional NWS and Baltic archives, while preserving source provenance and avoiding
large, ambiguous raster downloads.

The planning dry-run selected EMODnet Bathymetry as the primary source and
GEBCO_2026 as fallback/cross-check for all common metocean farm and turbine
sample points.

## Decision

Implement a source-specific bathymetry assignment wrapper:

- Core logic lives in `src/om_pipeline/metocean/bathymetry_assignment.py`.
- The CLI wrapper is `scripts/assign_bathymetry_to_metocean_points.py`.
- Inputs are the common requirements table and
  `Data/Interim/European_Turbine_Coordinates.csv`.
- The point set is farm centroid plus all turbine points for each common
  requirements farm.
- EMODnet is queried through the official `depth_sample` REST endpoint and raw
  responses are cached as JSONL under `Data/Raw/Metocean/Bathymetry/`.
- Processed output is written to
  `Data/Processed/metocean/bathymetry/site_bathymetry_points.parquet`.
- Processed depths use positive-down metres.
- GEBCO_2026 remains the documented fallback/cross-check source, but is not
  fetched when EMODnet returns valid depth samples for all assignment points.

The assignment remains source-specific. It does not create currents, FINO data,
source fusion, 10-minute interpolation, or final dwell-metocean rows.

## Consequences

This approach gives a small, auditable static point table instead of a large
local raster archive. It also keeps the raw evidence reproducible through cached
EMODnet REST payloads and source metadata.

The tradeoff is that the first implementation records EMODnet grid-cell point
samples rather than local raster bilinear interpolation. If later modelling
requires slope, roughness, or grid-cell neighborhood features, a separate
approved raster/tile acquisition task should extend this layer rather than
silently changing the point assignment contract.
