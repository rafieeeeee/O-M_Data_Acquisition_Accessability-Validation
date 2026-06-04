# Documentation Map

Use this file as the first stop when orienting a new agent or returning to the project after a context break.

## Core Context

- [Project context](../CONTEXT.md): Domain vocabulary, pipeline architecture, and current synchronization contracts.
- [Start here](../start_here/00_start_here.md): Lightweight agent entrypoint and current-state highlights.
- [Governance](governance.md): Spec-driven increment workflow, definition of done, and branch policy.
- [Context authority map](context-authority-map.md): Source-of-truth map for status, evidence boundaries, and derived summaries.
- [Branch exit checklist](branch-exit-checklist.md): Merge-readiness and branch-deletion sign-off template.
- [Increment specs](increments/README.md): Spec-first plans for meaningful increments before implementation starts.
- [Roadmap](roadmap.md): Phase-level project status and next technical milestones.
- [Thesis methodology](thesis-methodology.md): Methods-facing explanation of sampling, filters, limitations, and validation.
- [Provenance](provenance.md): Source lineage and pilot-run acquisition metadata.

## Operational Design Notes

- [DuckDB catalog design](duckdb-catalog-design.md): Local catalog schema and query patterns.
- [Metocean acquisition guide](metocean-acquisition.md): FINO1, NORA3, wind, and current acquisition paths.
- [ADR 0016: empirical workability surface modeling](adr/0016-empirical-workability-surface-modeling.md): Stage 1 observed/provisional surface contract and configurable $H_s \times T_p$ preset.
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
- [ADR 0031: Stable mainline spec-driven governance](adr/0031-stable-mainline-spec-driven-governance.md)

## Current Hygiene Notes

- `main` is the stable integration baseline. Meaningful work happens on topic branches, is validated and reviewed before merge, and the completed branch is deleted after merge.
- Meaningful increments require a short plan/spec, implementation against that spec, validation evidence, documentation/context updates where project meaning changes, review notes, and acceptance/sign-off.
- Use the context authority map before changing repeated status claims; update authority files and derived summaries in the same increment where project meaning changes.
- The AIS source backfill helper processes farm-candidate slices first by quarter (`Jan/Apr/Jul/Oct`) and can then backfill the remaining months.
- The dwell runner now supports dynamic `european_master` and `germany` clusters resolved from `Data/Interim/European_Turbine_Coordinates.csv`; use `--resume` for broad harvests.
- Stage 1 exists as an observed/provisional workability surface. Its default view is $H_s \times T_p$, implemented as a configurable preset rather than the complete workability definition.
- Stage 2 has not started. The next approved modelling branch should use Fusion v2 to compare wave-only, wave+wind speed, wave+current, and wave+wind+current subsets before any calibrated access-probability model.
- Fusion v2 is an accepted/provisional event feature layer, not a final model. It keeps missing current as null and does not synthesize unavailable current values. Wind direction remains too sparse for broad modelling and should stay out of primary Stage 2 predictors.
- The AIS + metocean join is intentionally separate from extraction; future work should keep the backbone QA boundary intact.
