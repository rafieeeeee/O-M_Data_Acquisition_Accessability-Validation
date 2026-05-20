# Documentation Map

Use this file as the first stop when orienting a new agent or returning to the project after a context break.

## Core Context

- [Project context](../CONTEXT.md): Domain vocabulary, pipeline architecture, and current synchronization contracts.
- [Roadmap](roadmap.md): Phase-level project status and next technical milestones.
- [Thesis methodology](thesis-methodology.md): Methods-facing explanation of sampling, filters, limitations, and validation.
- [Provenance](provenance.md): Source lineage and pilot-run acquisition metadata.

## Operational Design Notes

- [DuckDB catalog design](duckdb-catalog-design.md): Local catalog schema and query patterns.
- [Metocean acquisition guide](metocean-acquisition.md): FINO1, NORA3, wind, and current acquisition paths.
- [NORA3 extraction plan](nora3-extraction-plan.md): Implemented wave-backbone extraction contract and remaining access blockers.
- [Wind Farm C current state](wind-farm-c-current-state.md): Current evidence, caveats, feature-matrix outputs, and next steps for the CARE Wind Farm C / Trianel Borkum working mapping.
- [CARE Wind Farm C confirmation methodology](care-wind-farm-confirmation-methodology.md): Independent-agent protocol for confirming the Wind Farm C to Borkum mapping.
- [Event QA design](event-qa-design.md): QA checks for dwell-event plausibility.
- [Stage 2 label taxonomy](stage2-label-taxonomy.md): Operational labels for future SCADA/DPR-aligned modeling.
- [Vessel enrichment plan](vessel-enrichment-plan.md): Candidate data sources for vessel specifications.

## Architectural Decision Records

- [ADR 0001: AIS ingestion architecture](adr/0001-ais-ingestion-architecture.md)
- [ADR 0002: Data layering and storage policy](adr/0002-data-layering-and-storage-policy.md)
- [ADR 0003: Hybrid ingestion funnel](adr/0003-hybrid-ingestion-funnel.md)
- [ADR 0004: Metocean NORA3 caching and interpolation policy](adr/0004-metocean-nora3-caching.md)

## Current Hygiene Notes

- The AIS backfill runner processes farm-candidate slices first by quarter (`Jan/Apr/Jul/Oct`) and can then backfill the remaining months.
- The Wind Farm C feature matrix uses NORA3 wave and wind fields plus CMEMS current fields when available. In offline or unauthenticated environments, current fields fall back to the documented semi-diurnal tidal climatology.
- The AIS + metocean join is intentionally separate from extraction; future work should keep the backbone QA boundary intact.
