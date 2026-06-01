# ADR 0027: Current Confidence v1 Event Assignment

## Status
Accepted

## Context

ADR 0026 accepted the NWS hourly true `uo`/`vo` current archive as
source-specific evidence for `125` normal recommended farm-years. That archive
is intentionally not Fusion v2: it is a time-series source archive with
provenance, depth warnings, and validation. The next durable interface needed
by the modelling pipeline is an event-level current evidence layer that keeps
current confidence separate from wave confidence until the final multi-parameter
Fusion v2 step.

## Decision

Current Confidence v1 attaches the accepted NWS current archive to every dwell
event and writes:

- `Data/Processed/metocean/current_confidence_v1/current_event_candidates.parquet`
- `Data/Processed/metocean/current_confidence_v1/current_event_confidence.parquet`
- `reports/current_confidence_v1/current_confidence_validation_report.md`

The implementation lives in `src/om_pipeline/metocean/current_confidence_v1.py`
with a thin CLI wrapper at `scripts/build_current_confidence_v1.py`.

The candidate table preserves one row per dwell event. Events inside accepted
NWS farm-years receive event-window current aggregation when source timestamps
fall inside the dwell window; short dwell windows between hourly samples use the
bracketing pair and record `temporal_assignment_method =
bracketing_pair_mean`. Events without an accepted NWS partition remain present
with null current values and `current_missing_reason =
missing_nws_current_partition`.

Current direction remains flow-to degrees clockwise from true north:
`degrees(atan2(u, v)) % 360`. Event-level `current_speed_mean` is derived from
the aggregated vector as `sqrt(current_u_mean^2 + current_v_mean^2)`, while
`current_speed_p95` preserves event-window severity.

The confidence layer assigns:

- `A_event_scale` for accepted NWS true `uo/vo` evidence with provenance,
  bracketing, acceptable time gap, and acceptable spatial distance.
- `B_contextual` for true current evidence that is usable but not event-scale.
- `C_low_confidence` for weak temporal or spatial assignment.
- `D_unsuitable` for missing partitions, invalid values, or banned provenance.

No new current downloads, stress-test farm-years, Baltic broad extraction,
global fallback currents, FINO import, source fusion, final dwell-metocean
rebuild, legacy CMEMS CSV promotion, or synthetic/fallback current values are
part of this decision.

## Consequences

- Current Confidence v1 produced `92,660` candidate rows and `92,660`
  confidence rows, preserving dwell identity.
- `16,307` dwell events are `A_event_scale`; `76,353` are `D_unsuitable`
  because no accepted NWS partition exists for the event farm-year.
- `5,358` Tier A events have valid event-scale NWS current evidence.
- `9,337` events have both `A_high` wave confidence and `A_event_scale`
  current confidence; `3,402` of these are Tier A.
- Fusion v2 can now join wave confidence, current confidence, and bathymetry,
  but must continue to expose source/domain coverage bias and must not treat
  `D_unsuitable` current rows as zero-current observations.
