# ADR 0026: Current Pilot v1 True u/v Candidate Interface

## Status
Accepted

## Context

Fusion v1 established that metocean evidence should preserve source candidates,
agreement diagnostics, confidence classes, and provenance rather than collapsing
too early into a single preferred value. Currents now need the same discipline.
The existing CMEMS helper and raw CSV cache are not sufficient as final evidence
because they store speed/direction only, omit depth and dataset provenance, and
can contain simulated fallback values.

## Decision

Current Pilot v1 will produce scoped one-farm/year current candidate tables for
true Eulerian `uo` and `vo` only. The pilot interface records source product,
dataset, cadence, depth, grid location, spatial distance, assignment method,
source file, and provenance status for each candidate row.

The accepted pilot defaults are:

- Baltic: `Wikinger`, `2020`, `BALTICSEA_MULTIYEAR_PHY_003_011` /
  `cmems_mod_bal_phy_my_P1D-m`.
- NWS: `Borkum_Riffgrund_2`, `2020`, `NWSHELF_MULTIYEAR_PHY_004_009` /
  `cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i`.

The implementation may use local raw NetCDF files when mounted, or a scoped
Copernicus Marine Toolbox subset. It must not use Baltic wave `VSDX/VSDY`,
legacy CMEMS CSV caches, fallback climatology, synthetic currents, broad
downloads, final source fusion, or a final dwell-metocean rebuild.

Before any NWS broad current extraction, the project will run a metadata-only
scale preflight. The preflight interface writes a farm-year eligibility table
with dwell counts, Tier A counts, Fusion v1 wave-confidence support,
bathymetry depth warnings, NWS current domain match, storage/runtime estimates,
and a recommendation of `yes`, `stress_test_only`, or `no`. It is a gate for a
later extraction write, not an extraction itself.

The Baltic hourly-current decision is also metadata-only unless the study scope
changes. Historical Baltic physics reanalysis remains daily/contextual current
evidence. Recent Baltic analysis/forecast products may provide hourly or
sub-hourly true currents, but they do not replace the historical 2010-2020
reanalysis evidence without a separate approved pilot.

After preflight approval, the first NWS scale write was a controlled top-N
batch, not an immediate all-recommended extraction. That top-10 engineering
batch passed, so the normal recommended NWS scale run was approved for all
`recommended_for_scale == yes` farm-years while still excluding
`stress_test_only` rows. The batch writes source-specific hourly current
partitions under
`Data/Processed/metocean/nws_current_timeseries/wind_farm=<farm>/year=<year>/`
and a manifest at `Data/Processed/metocean/nws_current_timeseries/manifest.csv`.
Raw Copernicus subsets for this batch are cached only under the labelled pilot
path `Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/`, separate from
legacy `cmems_raw_*.csv` files. The batch archive records flow-to direction,
depth-selection rule, EMODnet depth warnings, current model bathymetry warnings,
source provenance, and event-scale validation diagnostics.

The accepted normal NWS current archive now covers `125` normal recommended
farm-years with `76,886,304` rows. The scale run processed `115` remaining
farm-years and skipped/revalidated the existing top-10 partitions without
overwrite. No stress-test, Baltic broad, global fallback, legacy CSV,
synthetic, FINO, source-fusion, or final dwell-metocean rebuild step is part of
this decision.

## Consequences

- A blocked pilot is a valid result when credentials, network, or local source
  files are unavailable, provided the output states the blocker explicitly.
- `current_speed` is derived as `sqrt(u^2 + v^2)`.
- `current_direction` is documented as flow-to degrees clockwise from true
  north: `degrees(atan2(u, v)) % 360`.
- Baltic daily currents may be contextual rather than event-scale evidence.
- NWS hourly currents can become event-scale evidence only where source rows
  bracket dwell windows with acceptable spatial distance and provenance.
- Farm-years with extensive `<=10 m` bathymetry are not normal NWS scale
  targets because the NWS model imposes a 10 m minimum depth; useful shallow
  cases must be marked `stress_test_only`.
- The NWS current archive is source-specific evidence. It must not be treated
  as Fusion v2 or joined into final dwell-metocean features until the current
  confidence layer is explicitly approved.
- The next durable layer should be Current Confidence v1, which attaches this
  source-specific archive to dwell events with event-level provenance,
  suitability diagnostics, and confidence classes before any Fusion v2 rebuild.
