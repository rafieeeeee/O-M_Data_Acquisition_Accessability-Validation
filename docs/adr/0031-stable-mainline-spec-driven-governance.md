# ADR 0031: Stable Mainline Spec-Driven Governance

## Status

Accepted

## Context

The project now has several parallel evidence layers, repeated handoff summaries,
and research-claim boundaries across `CONTEXT.md`, `start_here/`, `docs/`,
ADRs, and generated reports. Informal judgement is no longer enough to keep
current status, allowed claims, and next-step guidance synchronized.

The main governance risk is not only broken links. It is context rot: stale
handoffs, unsupported research claims, missing validation evidence, and
branches that remain ambiguous after an increment appears complete.

## Decision

Adopt a stable-mainline, spec-driven increment policy:

1. Treat `main` as the stable integration baseline.
2. Do not implement meaningful work directly on `main`.
3. Use one topic branch per meaningful increment.
4. Begin each meaningful increment with a short plan/spec that records purpose,
   scope, assumptions, expected outputs, affected areas, and validation plan.
5. Require validation evidence before merge. The evidence must match the
   increment risk and may include tests, pipeline slices, context sweep, link
   checks, generated-report rebuilds, or evidence/readiness checks.
6. Update `CONTEXT.md`, ADRs, generated reports, `start_here`, roadmap, or
   handoff docs when project meaning, decisions, status, or user workflow
   changes.
7. Record a short review/sign-off note before merge. It must identify what
   changed, unresolved caveats, known blockers, validation results, and merge
   readiness.
8. Merge to `main` only after validation and review/sign-off.
9. Delete the completed topic branch after merge.
10. Maintain a context authority map and branch exit checklist so future agents
    can identify source-of-truth documents and complete increments without
    relying on memory.

## Consequences

- New research or feature work has a clear path from idea to validated mainline.
- `main` remains a stable integration baseline rather than a working scratch
  branch.
- Handoff summaries must point back to authority documents instead of becoming
  independent sources of truth.
- Governance itself becomes testable through `scripts/context_sweep.py`, which
  checks core governance phrases, orientation links, authority-map presence,
  branch-exit checklist presence, and key status guardrails.
- The policy intentionally does not turn every typo or link fix into a heavy
  process. Small fixes may use a lighter path, but still must not happen
  directly on `main`.
- Exact volatile row counts remain in canonical validation reports or state
  summaries. The governance sweep must not duplicate them as free-floating
  constants unless it parses them from a single canonical source.
