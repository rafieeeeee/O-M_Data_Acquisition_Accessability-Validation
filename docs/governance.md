# Governance: Spec-Driven Increment Workflow

This repository is now managed as a stable-mainline, topic-branch project. The goal is lightweight control: every meaningful increment should be planned, validated, documented, reviewed, and accepted before it reaches `main`.

The durable decision record is [ADR 0031: Stable Mainline Spec-Driven Governance](adr/0031-stable-mainline-spec-driven-governance.md). Use the [context authority map](context-authority-map.md) to find source-of-truth files, and complete the [branch exit checklist](branch-exit-checklist.md) before merge.

## Branch Policy

- `main` is the stable integration baseline.
- Do not implement changes directly on `main`.
- Create one topic branch per meaningful increment, starting from current `main` unless the user explicitly approves a different base.
- Merge back to `main` only after the increment is validated, reviewed, and accepted.
- Delete the completed topic branch after merge.
- Keep unrelated work out of the branch. If scope expands, update the spec before implementing the expansion.

For this governance refresh, the pre-refresh branch audit was:

```text
branch=rq6-metocean-resolution-sensitivity
worktree=clean
ahead_of_main=5
ahead_of_origin_main=5
upstream=none
```

## Meaningful Increment

A meaningful increment is any change that affects at least one of:

- domain meaning or terminology,
- pipeline behavior or interfaces,
- data products, schemas, or generated reports,
- analysis conclusions or claim boundaries,
- validation policy or evidence-readiness rules,
- governance, branch policy, or definition of done,
- user handoff, start-here docs, or roadmap direction.

Tiny typo, formatting, or broken-link fixes may use a lighter path, but they still must not happen directly on `main`.

## Staged Workflow

### A. Plan / Design / Spec

Before implementation, record the purpose, scope, assumptions, expected outputs, affected files or data products, and validation approach. The spec can be short, but it must be concrete enough that another agent can implement without guessing.

### B. Implementation

Make code, model, analysis, or documentation changes against the approved spec. Keep scripts as thin CLI wrappers and put reusable logic in `src/om_pipeline/`.

### C. Validation

Run the smallest validation set that proves the increment is safe. Depending on the change, this may include unit tests, pipeline slices, generated-report rebuilds, `scripts/context_sweep.py`, Markdown link checks, evidence/readiness checks, or manual inspection notes.

### D. Documentation / Context Update

Update `CONTEXT.md` when project meaning changes. Update ADRs for significant decisions, hacks, or policy shifts. Update generated reports and `start_here` docs when user handoff or current status changes.

### E. Review / Next Steps / Roadmap

Record what changed, what remains unresolved, what is blocked, and what should happen next. Keep the note concise enough that a future agent can resume from it.

### F. Acceptance / Sign-off

Confirm that the increment meets the spec, validation has passed or any failures are explicitly accepted, docs are current, and the branch is ready to merge.

Use `docs/branch-exit-checklist.md` as the minimum sign-off record for meaningful increments.

### G. Git Control

Merge only after acceptance. After the merge lands on `main`, remove the completed topic branch locally and remotely where applicable.

## Increment Template

```markdown
# Increment Spec: <short name>

## Purpose
<Why this increment exists.>

## Scope
<In scope and explicitly out of scope.>

## Assumptions
<Known constraints, chosen defaults, and dependencies.>

## Affected Areas
<Code, scripts, docs, data products, reports, or tests expected to change.>

## Expected Outputs
<Files, reports, commands, or user-facing outcomes.>

## Validation Plan
<Tests, pipeline slices, context sweep, link checks, evidence/readiness checks.>

## Documentation / Context Plan
<CONTEXT.md, ADRs, generated reports, start-here, roadmap, handoff docs.>

## Review Notes
<What changed, unresolved items, blocked items, and next steps.>

## Acceptance
<Validation status, sign-off status, and merge readiness.>
```

## Definition Of Done

An increment is done when:

- the implemented changes match the approved spec,
- relevant validation has passed or documented exceptions are accepted,
- `CONTEXT.md`, ADRs, reports, and start-here docs are updated where meaning or handoff changed,
- review notes identify unresolved or blocked follow-up work,
- the branch is accepted as merge-ready,
- the completed branch is deleted after merge.
