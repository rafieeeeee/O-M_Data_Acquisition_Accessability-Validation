# Research Questions

This directory controls research-question planning. It records what each RQ is
allowed to claim, which evidence it may use, which outputs it should produce,
and when it is ready to move from plan to implementation to accepted result.

The register is a research-management layer. It does not replace `CONTEXT.md`,
ADRs, validation reports, generated analysis reports, or the roadmap.

## RQ Lifecycle

Each meaningful research question follows the repository governance workflow:

1. Draft an analysis plan before implementation.
2. Record the evidence boundary and claim limits.
3. Implement only against the accepted plan.
4. Validate inputs, outputs, and claims.
5. Write an exit report with findings, caveats, and next steps.
6. Update authority/context files only when accepted conclusions change project
   meaning.

## Folder Convention

Each RQ should use one folder:

```text
docs/research-questions/rq##_short_name/
  analysis-plan.md
  evidence-boundary.md
  decision-log.md
  final-report.md
```

Use the templates in `docs/research-questions/templates/` when creating new RQ
folders.

## Generated Outputs

Generated analysis outputs should live outside this docs directory:

```text
reports/rq##_short_name/
Data/Processed/analysis/rq##_short_name/
```

Do not create empty generated-output directories in spec-only increments unless
the repository later adopts a `.gitkeep` convention.

## Independence Rules

Parallel RQs are acceptable only when they do not mutate the same accepted
evidence products, change the same claim boundaries, or update the same final
report. When one RQ changes project meaning, dependent RQs must refresh their
evidence boundary before writing conclusions.

Safe parallel work includes report-only Stage 2 sensitivity on accepted Fusion
v2, RQ9 observability/covariate controls on existing outputs, FINO access/import
planning as validation-source work, and AIS receiver/proxy searches as targeted
blocker-reduction work.

Unsafe parallel work includes multiple branches mutating Fusion v2, wind/current
repairs running while another branch assumes those evidence layers are final,
or two branches writing conclusions from different claim boundaries.

## Targeted Unblockers

Additional data-source work should be targeted to a specific blocked claim or
validation need. It is not a general precondition for restricted Stage 2
sensitivity analysis.

Examples:

- FINO remains validation-source work until a separately approved import pilot.
- AIS receiver geometry or accepted proxies unblock stronger RQ9 observability
  controls.
- Wind-direction repair is deferred unless a Stage 2 result shows direction is
  worth targeted evidence work.
- Current stress-test expansion is deferred while current remains
  coverage-limited sensitivity evidence.
- SCADA/DPR labels are required for calibrated probability or success/failure
  claims, not for observed-envelope sensitivity.

## Claim Boundary Expectations

Each RQ must state what it can and cannot claim before implementation starts.
The final report must preserve that boundary unless a separate accepted
increment changes the authority docs.

For workability sensitivity, observed-envelope claims are distinct from
calibrated `P(operation | weather)` claims. For RQ9, AIS-derived intervention
intensity is distinct from confirmed failure-rate evidence.
