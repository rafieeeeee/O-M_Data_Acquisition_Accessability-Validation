# Agent Instructions

This repository is designed for autonomous and semi-autonomous development by AI coding agents. Follow these rules to maintain project integrity and legibility.

## Core Mandates

- **Agent Legibility First:** Write code and documentation as if the next person reading it is an agent with a limited context window. Use descriptive symbols and concise context files.
- **Context Preservation:** Always update `CONTEXT.md` when introducing new domain concepts (e.g., a new sensor type or a change in "dwell" definitions).
- **Decision Tracking:** Every significant architectural change or "hack" must be documented in `docs/adr/`.
- **Modular Logic:** Prefer placing core business logic in `src/om_pipeline/`. Keep `scripts/` as thin CLI wrappers.
- **Spec-Driven Increments:** Every meaningful increment must start from a short plan/spec, run on a topic branch, carry validation evidence, update context docs where project meaning changes, and end with explicit review/sign-off before merge.
- **Stable Mainline:** Do not work directly on `main`. `main` is the stable integration baseline; merge only after validation and review, then delete the completed topic branch.
- **Research-Question Control:** Research analysis must start from `docs/research-questions/rq-register.md` and an RQ analysis plan with evidence boundaries, validation checks, output paths, and exit-report expectations.

## Governance

A **meaningful increment** is any change that affects domain meaning, pipeline behavior, data products, analysis conclusions, validation policy, governance, or user handoff. Tiny typo/link-only fixes may use a lighter path, but they still must not happen directly on `main`.

Meaningful increments follow this staged path:

1. **Plan / Design / Spec:** Define purpose, scope, assumptions, expected outputs, affected files or data products, and validation approach before implementation.
2. **Implementation:** Make code, model, analysis, or documentation changes against the approved spec.
3. **Validation:** Run relevant tests, pipeline slices, context sweep, link checks, and evidence/readiness checks.
4. **Documentation / Context Update:** Update `CONTEXT.md`, ADRs, generated reports, `start_here`, or handoff docs where project meaning, decisions, or user workflow changes.
5. **Review / Next Steps / Roadmap:** Record what changed, what remains unresolved or blocked, and what should be tackled next.
6. **Acceptance / Sign-off:** Confirm the increment meets the spec, validation passed, and the branch is ready to merge.
7. **Git Control:** Merge to `main` only after acceptance, then remove the completed topic branch.

## Workflow

1. **Research:** Use `rg` (ripgrep) to understand the domain before proposing changes.
2. **Strategy:** For any non-trivial change, create or update an ADR in `docs/adr/`.
3. **Execution:**
    - Add tests for new logic in `tests/`.
    - Ensure all scripts import from the `om_pipeline` package.
    - **Environment:** Run `pip install -e .` to install the package in editable mode, or ensure `src` is in your `PYTHONPATH`.
4. **Validation:** Run the relevant pipeline slice (e.g., 1 month of AIS data) to confirm no regressions.

## Documentation Pointers

- `CONTEXT.md`: The source of truth for the O&M domain and data pipeline.
- `docs/governance.md`: The staged increment workflow, definition of done, and branch policy.
- `docs/context-authority-map.md`: Source-of-truth map for status, evidence boundaries, and derived summaries.
- `docs/branch-exit-checklist.md`: Required sign-off checklist before merging meaningful increments.
- `docs/research-questions/README.md`: Research-question register, analysis-plan templates, evidence-boundary rules, and targeted-unblocker policy.
- `docs/adr/0031-stable-mainline-spec-driven-governance.md`: Decision record for stable-mainline, spec-driven governance.
- `docs/adr/`: Records of technical decisions.
- `docs/roadmap.md`: The project's progression and future goals.
