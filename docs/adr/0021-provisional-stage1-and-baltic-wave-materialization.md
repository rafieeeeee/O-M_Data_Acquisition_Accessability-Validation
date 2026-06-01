# ADR 0021: Provisional Stage 1 Envelope and Baltic Wave Materialization

## Status

Accepted.

## Context

The NORA3-derived dwell-weather table is good enough for a provisional first
observed Hs/Tp operating-envelope analysis, but it is not the final
multi-source metocean evidence layer. Current fields are placeholders, wind
direction coverage is incomplete, and Baltic/NWS/CMEMS/FINO/bathymetry sources
must later be combined through field-level provenance.

Two implementation gaps were identified:

1. The existing generic workability script can produce unlabelled or misleading
   outputs for the provisional Stage 1 result.
2. Baltic Copernicus raw wave NetCDF subsets are present, but no processed
   hourly `baltic_` archive materializer exists.

## Decision

Add two dedicated thin wrappers with core logic in `src/om_pipeline/`:

- `src/om_pipeline/analysis/provisional_stage1_hs_tp.py`
- `scripts/build_provisional_stage1_hs_tp_surface.py`
- `src/om_pipeline/metocean/baltic_wave_materializer.py`
- `scripts/materialize_baltic_wave_timeseries.py`

The Stage 1 builder must:

- read `Data/Processed/ais_dwell_backfill/cross_farm_dwell_weather_features.parquet`;
- build the primary subset
  `active_hs_mean.notnull() AND active_tp_mean.notnull() AND dwell_tier == "Tier A"`;
- build an all-tier Hs/Tp sensitivity subset;
- label every output as
  `Provisional NORA3-derived Tier A wave-only observed operational envelope`;
- avoid any claim of `P(operation | weather)`;
- avoid CTV/SOV role inference.

The Baltic materializer must:

- read reviewed raw NetCDFs from `Data/Raw/Metocean/CMEMS/BalticSea/Waves/`;
- preserve native hourly cadence;
- map `VHM0`, `VTPK`, `VMDR`, `VTM10`, and `VTM02` into `baltic_` wave columns;
- write partitioned Parquet under
  `Data/Processed/metocean/baltic_wave_timeseries/wind_farm=<farm>/year=<year>/part.parquet`
  only when not in dry-run mode;
- support `--dry-run`, `--limit-farms`, and overwrite-safe behavior;
- produce a QA report before broad write execution.

## Non-Decisions

- No current downloads are approved by this ADR.
- No NORA3 rerun is approved.
- No final source-agnostic dwell-metocean table rebuild is approved.
- No legacy CMEMS current CSV cache is promoted as final evidence.
- Baltic Stokes drift variables are not treated as Eulerian currents.
- The Baltic materializer does not interpolate to the 10-minute backbone; that
  belongs in the later source-agnostic assignment/resolver layer.

## Consequences

Stage 1 research artefacts can move forward with clear provisional labelling,
while the engineering lane gains a safe dry-run-first path for Baltic processed
wave materialization.

Future source-agnostic metocean assignment work should consume source-labelled
archives (`active_`, `nws_`, `baltic_`, later current and bathymetry sources)
and attach field-level provenance rather than extending the NORA3-only join.
