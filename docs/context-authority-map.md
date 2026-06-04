# Context Authority Map

Use this map to decide which file owns a project truth, which summaries are
derived from it, and what must be updated when that truth changes. The purpose
is to prevent context rot without copying volatile counts into another registry.

| Topic | Authority | Derived Summaries | Update When Truth Changes |
| --- | --- | --- | --- |
| Governance policy | `AGENTS.md`, `docs/governance.md`, `docs/adr/0031-stable-mainline-spec-driven-governance.md` | `docs/README.md`, `start_here/00_start_here.md`, branch exit notes | Update the ADR for durable policy decisions, `docs/governance.md` for operational workflow, and orientation docs for discoverability. |
| Current project handoff | `start_here/01_project_state_summary.md` | `start_here/00_start_here.md`, `docs/README.md` hygiene notes | Update the state summary when active objective, run state, safety notes, or immediate next work changes; keep `00_start_here` as a short index. |
| Roadmap status | `docs/roadmap.md` | `CONTEXT.md`, `start_here/01_project_state_summary.md`, handoff docs | Update when phase status, blocked/unblocked work, or approved next research branch changes. |
| Research-question planning | `docs/research-questions/rq-register.md`, `docs/research-questions/README.md`, per-RQ `analysis-plan.md` and `evidence-boundary.md` files | `docs/README.md`, `start_here/00_start_here.md`, roadmap next-step notes | Update when an RQ is added, moves status, changes claim boundary, or names a targeted data-source unblocker. |
| Fusion v2 status | `docs/adr/0029-metocean-fusion-v2-multiparameter-event-features.md`, `reports/metocean_fusion_v2/fusion_v2_validation_report.md` | `CONTEXT.md`, `start_here/01_project_state_summary.md`, `docs/agent-handoff-metocean-fusion-v2.md`, `docs/metocean-acquisition.md` | Update the ADR for decision changes, the validation report for rebuilt evidence, and derived summaries when Fusion v2 readiness or guardrails change. |
| Stage 2 modelling boundary | `CONTEXT.md`, `docs/roadmap.md`, `docs/adr/0016-empirical-workability-surface-modeling.md`, `docs/adr/0029-metocean-fusion-v2-multiparameter-event-features.md` | `start_here/00_start_here.md`, `start_here/01_project_state_summary.md`, `docs/agent-handoff-metocean-fusion-v2.md` | Update before any Stage 2 work starts, or when calibrated probability claims become supported. |
| FINO status | `docs/adr/0023-fino-validation-access-planning.md`, `docs/adr/0024-fino-native-export-inspection.md`, `analysis/06_rq6_metocean_spatial_resolution/fino_metadata_access_plan.md`, `docs/metocean-acquisition.md` | `CONTEXT.md`, `start_here/00_start_here.md`, `start_here/01_project_state_summary.md`, Fusion handoff docs | Update when FINO access, native export inspection, import approval, or validation role changes. |
| Wind evidence guardrails | `docs/adr/0028-wind-confidence-v1-event-assignment.md`, `reports/wind_confidence_v1/wind_confidence_validation_report.md`, `docs/metocean-acquisition.md` | `CONTEXT.md`, `start_here/01_project_state_summary.md`, Fusion handoff docs | Update when wind direction coverage is repaired, quarantined differently, or promoted beyond sensitivity-only use. |
| Current evidence guardrails | `docs/adr/0027-current-confidence-v1-event-assignment.md`, `reports/current_confidence_v1/current_confidence_validation_report.md`, `docs/metocean-acquisition.md` | `CONTEXT.md`, `start_here/01_project_state_summary.md`, Fusion handoff docs | Update when event-scale current coverage changes, stress-test farm-years are approved, or missing current semantics change. |
| RQ9 claim boundaries | `CONTEXT.md`, `docs/roadmap.md`, `reports/rq9_intervention_intensity/` | `start_here/01_project_state_summary.md`, future RQ handoff docs | Update when AIS-derived intervention-intensity evidence becomes validated against SCADA, fault logs, work orders, receiver/source controls, or equivalent evidence. |

## Guardrail Rules

- Handoff docs summarize; they do not overrule ADRs, validation reports, or
  `CONTEXT.md`.
- If a derived summary changes a status claim, update the authority file in the
  same increment or record why the authority does not change.
- Do not duplicate volatile row counts in governance checks unless they are
  parsed from canonical reports or a single status registry.
- Before starting Stage 2 modelling, run a dedicated Fusion v2 evidence-readiness
  audit branch rather than treating current summaries as sufficient modelling
  sign-off.
- Before starting analysis work, register the RQ and write an analysis plan with
  evidence boundaries, validation checks, output paths, and exit-report
  expectations.
