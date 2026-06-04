# RQ Register

This register tracks planned and active research questions. It is the planning
index, not the final evidence authority. Accepted findings belong in the RQ
final report and any affected authority files listed in
`docs/context-authority-map.md`.

| RQ | Status | Question | Primary Boundary | Next Increment |
| --- | --- | --- | --- | --- |
| RQ01 Stage 2 Fusion v2 workability sensitivity | Result accepted | Do wind speed and event-scale current materially change the observed Tier A workability envelope relative to wave-only Fusion v1/Fusion v2 evidence? | Observed-envelope sensitivity only; no calibrated access-probability claim. Current-aware comparisons are NWS-domain / coverage-limited, wind direction is excluded, and depth-warning sensitivity is required. | Merge accepted result; use as basis for next RQ01 interpretation/reporting or follow-on Stage 2 work. |
| RQ09 covariate and observability follow-up | Planned | Does AIS-derived intervention intensity vary by turbine age, capacity, or OEM after controlling for farm, basin, and observability? | AIS dwell evidence supports intervention intensity, not confirmed failure rate. Basin remains geographic until receiver/source controls or accepted proxies support stronger claims. | Draft an RQ9 analysis plan after RQ01 spec setup, or in a separate independent branch. |
| FINO validation planning | Planned targeted unblocker | Which FINO station/export pilot should be used to validate metocean evidence without promoting FINO to farm-wide source evidence? | FINO is validation/baseline planning only until access, native export inspection, and import approval are completed. | Keep as validation-source work; do not block restricted Stage 2 sensitivity. |
| AIS receiver geometry/proxy unblocker | Planned targeted unblocker | What receiver/source geometry fields or accepted offshore-distance proxies can reduce RQ9 observability bias? | Supports observability control, not causal basin or reliability claims by itself. | Search available metadata/proxies before stronger RQ9 basin claims. |

## Status Vocabulary

- `Planned`: RQ is registered but has no accepted analysis plan.
- `Spec drafted`: analysis plan and evidence boundary have been drafted for
  review.
- `Implementation active`: code, analysis, or report work has started against
  the accepted plan.
- `Implementation active / pending review`: generated outputs exist but have
  not yet been accepted as final findings.
- `Result accepted`: final report and required context updates have been
  reviewed and accepted.
- `Blocked`: progress depends on a named evidence, access, or validation
  blocker.

## Update Rule

Update this register when an RQ changes status, when its claim boundary changes,
or when a targeted unblocker becomes part of an approved analysis plan.
