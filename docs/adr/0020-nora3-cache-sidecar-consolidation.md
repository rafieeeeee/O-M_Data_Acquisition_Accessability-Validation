# 0020. NORA3 Cache Sidecar Consolidation

## Status

Accepted

## Context

The full cross-farm weather backfill downloads NORA3 wave and wind CSV files into
`Data/Raw/Metocean/NORA3`. That process can run for many hours and writes files
incrementally. Waiting for the entire job to finish before creating joined
artifacts makes progress hard to inspect and increases restart cost.

The downloader writes raw wave files named `nora3_raw_<lat>_<lon>_<year>_<month>.csv`
and wind files named `nora3_wind_raw_<lat>_<lon>_<year>_<month>.csv`. These files
are independent monthly point caches, so completed wave/wind pairs can be joined
without modifying the downloader.

## Decision

Introduce a sidecar consolidation command:

`python scripts/consolidate_nora3_cache.py`

The sidecar:

- reads only from `Data/Raw/Metocean/NORA3`;
- processes only complete wave/wind file pairs;
- skips files modified within a configurable stability window;
- writes joined parquet checkpoints under `Data/Processed/metocean/nora3_joined_cache/`;
- records completed pair IDs in `manifest.csv` so repeated runs are idempotent.

The default batch size is 100 wave/wind pairs. The sidecar does not create ocean
current fields. Current features remain a separate CMEMS/NWS ocean-physics concern.

## Consequences

NORA3 download and consolidation can proceed concurrently without sharing write
targets. Downstream audits can inspect partial joined wave/wind coverage while
the downloader continues. A final compaction or event-level join can run after
the raw cache has settled.
