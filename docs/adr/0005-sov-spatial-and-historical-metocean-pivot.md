# ADR 0005: Spatial Pivot to SOV Assets and Temporal Focus on Historical Metocean (2010-2020)

## Status
Accepted

## Context
1. **Asset Selection:** Our initial pipeline focused on older wind farms (e.g., Alpha Ventus) in the German Bight. These assets primarily utilize Crew Transfer Vessels (CTVs), resulting in sparse AIS signals and less relevance for modern 15MW+ pre-construction operational simulation modelling.
2. **Temporal Window Stability:** Ingesting 2024 metocean hindcasts from MET Norway's monthly subsets proved to be unstable and highly turbulent for our automated pipelines.
3. **Connectivity Failures:** Monthly pre-aggregated NORA3 THREDDS endpoints for newer years suffer from structural grid shifts and availability, whereas legacy archives provide a contiguous dataset if aggregated properly.

## Decision
We are executing a strategic realignment of our data ingestion pipeline:
1. **Spatial Pivot (SOV Focus):** Shift the target pilot wind farm to **Wikinger** (Baltic Sea). Analysis of existing fleet registries shows that Wikinger possesses heavy Service Operation Vessel (SOV) traffic (vessels > 60m with dynamic positioning capabilities), providing dense, high-quality AIS signals.
2. **Temporal Pivot (2010-2020):** Move our historical AIS backfill and Metocean extraction window to the 2010–2020 decade.
3. **Metocean Rerouting:** Force `fetch_nora3_point` to stop targeting monthly `wave_tser` sub-directories and instead pull from MET Norway's legacy global aggregation endpoint: `windsurfer/mywavewam3km_files/aggregate/nora3_wave_agg.nc`.
4. **Resumable AIS Backfill Pipeline:** Configure `scripts/backfill_ais_slices.py` to default to the 2010–2020 period and prioritize quarterly slices (`Jan/Apr/Jul/Oct`) to capture high-level seasonal behavior first.

## Consequences
- **Timeout Risk:** The legacy NORA3 global aggregation endpoint (`nora3_wave_agg.nc`) is prone to request timeouts. The ingestion process must be monitored closely for DAP-connection timeouts.
- **Strict QA Gate:** A rigid gate must be enforced: the NORA3 wave backbone must be fully generated and pass the validation/QA checks before performing any AIS event joins or wind/current data expansions.
- **Improved Data Integrity:** Shifting to Wikinger ensures high-fidelity vessel-aware training matrices that correctly model SOV workability boundaries.
