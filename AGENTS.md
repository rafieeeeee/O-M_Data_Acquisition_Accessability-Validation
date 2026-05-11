# Agent Instructions

This repository is designed for autonomous and semi-autonomous development by AI coding agents. Follow these rules to maintain project integrity and legibility.

## Core Mandates

- **Agent Legibility First:** Write code and documentation as if the next person reading it is an agent with a limited context window. Use descriptive symbols and concise context files.
- **Context Preservation:** Always update `CONTEXT.md` when introducing new domain concepts (e.g., a new sensor type or a change in "dwell" definitions).
- **Decision Tracking:** Every significant architectural change or "hack" must be documented in `docs/adr/`.
- **Modular Logic:** Prefer placing core business logic in `src/om_pipeline/`. Keep `scripts/` as thin CLI wrappers.

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
- `docs/adr/`: Records of technical decisions.
- `docs/roadmap.md`: The project's progression and future goals.
