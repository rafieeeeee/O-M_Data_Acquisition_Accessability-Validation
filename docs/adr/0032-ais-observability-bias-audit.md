# ADR 0032: AIS Receiver/Source Observability Bias Audit

## Status
Accepted

## Context
Farm-level AIS intervention-intensity proxies appear geographically uneven.
Before using those contrasts in research claims, the project needs a dedicated
observability-bias audit that separates source availability, source intensity,
receiver/source evidence, geographic proxies, and downstream dwell/event
proxies.

The local evidence-readiness layer already shows that per-vessel-message AIS
receiver/source assignment is absent from the integrated RQ9 tables. Raw AIS
schemas do include `Data source type`, but that field is source-channel
evidence rather than receiver station geometry.

Raw Danish AIS files can also contain `Type of mobile = Base Station` message
rows with station MMSI and coordinates. Those rows provide direct AIS
base-station geometry reference, but they still do not identify which station
received each vessel ping.

## Decision
1. Add `src/om_pipeline/analysis/ais_observability_bias.py` as the source of
   truth and keep `scripts/build_ais_observability_bias.py` as a thin CLI
   wrapper.
2. Write ignored derived matrices under
   `Data/Processed/analysis/ais_observability_bias/` and tracked documentation
   under `analysis/00_data_foundation/` and `reports/evidence_readiness/`.
3. Use a four-tier evidence ladder with a Tier 1 split: Tier 1a per-message receiver assignment,
   Tier 1b direct AIS source-geometry reference, Tier 2
   source-channel evidence, Tier 3 geographic proxy evidence, and Tier 4
   downstream observability proxy evidence.
4. Treat `Data source type`, source file status, and manifest `input_rows` /
   `clean_rows` as source-channel/source-intensity evidence only.
5. Extract `Type of mobile = Base Station` rows into an ignored derived AIS
   base-station geometry catalogue and use nearest observed base station as a
   farm source-geometry control only.
6. Require explicit provenance before external receiver/coastline references can
   be used. Do not infer receiver location from vessel positions or nearest
   coast.
7. Add a matched base-station distance diagnostic using observed-source
   farm-months only. Bin nearest observed base-station distance within
   country/sea-basin/year-month strata, compare source `clean_rows` separately
   from downstream dwell/Tier rates, and mark sparse strata as
   `insufficient_matched_strata`.
8. Keep all outputs audit-first: AIS event density is not intervention activity
   and not failure rate.

## Consequences
- The project can now distinguish sparse raw AIS candidate pings from sparse
  dwell/event detections.
- Receiver-distance bias cannot be directly confirmed from local data until
  vessel pings can be linked to receiving station IDs or equivalent
  per-message receiver assignment.
- Observed AIS base-station geometry is a stronger geography/source control
  than nearest coast, but it is not proof of the receiving station for any
  vessel message.
- Strong matched clean-row declines plus observed-zero increases may be reported
  as evidence consistent with geographical AIS observability bias, but not as
  receiver-distance causality or operational/failure-rate evidence.
- Geographic and sea-basin comparisons remain observability diagnostics, not
  reliability or operational-performance claims.
