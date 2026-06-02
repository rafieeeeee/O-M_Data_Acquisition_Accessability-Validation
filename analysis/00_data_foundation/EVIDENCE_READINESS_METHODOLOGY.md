# Evidence Readiness Methodology

## Purpose

The evidence-readiness layer is a pre-analysis audit. It integrates existing AIS,
turbine metadata, metocean, vessel metadata, geography, and SCADA/event
validation outputs so later research questions can state what the data can and
cannot answer.

This layer does not rerun AIS extraction, rerun metocean extraction, modify raw
or interim source data, clean data, or start new modelling.

## Reproducibility

The source of truth for the audit is `scripts/build_evidence_readiness.py` and
`src/om_pipeline/analysis/evidence_readiness.py`. Rebuild from the repository
root with:

```bash
/opt/anaconda3/bin/python scripts/build_evidence_readiness.py
```

The processed matrices under `Data/Processed/analysis/evidence_readiness/` are
derived outputs and may be ignored by git. They should be regenerated from the
existing local AIS, turbine, metocean, RQ9, vessel, and SCADA artifacts rather
than hand-edited or committed as source. The tracked files under
`reports/evidence_readiness/` are generated summaries from the same builder and
should be refreshed after source or input changes.

## Observation Units

- `farm-month`: one wind farm and one calendar month from the AIS backfill
  manifest. This is the primary coverage and source-availability unit.
- `turbine-month`: one turbine and one calendar month inherited from its farm's
  manifest month, with turbine assignment evidence where available.
- `dwell/event`: one AIS dwell or assigned intervention candidate. This is the
  event-level evidence unit for metocean joins, proximity tiers, MMSI
  concentration, and turbine assignment confidence.
- `vessel-month`: one MMSI and one calendar month. This is the intended unit for
  future vessel registry/access-technology readiness; in the current integrated
  dwell layer, MMSI exists but registry/access enrichment is effectively absent.

## Coverage Versus Observability

Coverage means a source month exists and was processed. In the AIS manifest,
`success` and `success_no_ais_in_bbox` both count as observed coverage.

Observability means the system was capable of detecting the behaviour needed for
a claim. A month with `success_no_ais_in_bbox` is observed but has zero detected
events. A month with `skipped_missing_source` is missing source evidence and must
not be converted to a zero-event month.

Observability is weaker than coverage when direct receiver/source metadata is
missing. The current RQ9 and dwell tables do not contain receiver station,
terrestrial/satellite channel, receiver coordinates, or distance-to-receiver
fields. The audit therefore keeps indirect proxies separate: observed-zero
share, high-confidence assignment share, top-MMSI concentration, and event
density.

## Source Availability Versus Detection Quality

Source availability is a file, partition, manifest, or validation artifact being
present. Detection quality is whether the source can identify the behaviour with
enough precision for a research claim.

Examples:

- A wave archive partition can make wave evidence available for a farm-year, but
  the event-level Fusion v2 join still determines whether an AIS event has
  usable wave features.
- NWS current partitions make current evidence source-available only for covered
  farm-years. Missing current remains missing and is not zero current.
- Wind speed is broadly available in Wind Confidence v1; wind direction is sparse
  and remains sensitivity-only.
- SCADA/event validation exists locally for CARE Wind Farm B/C mappings, but that
  does not create a Europe-wide validation denominator.

## Direct Evidence Versus Proxy Evidence

Direct evidence is an observation that measures the construct of interest:

- AIS source coverage for whether a source month exists.
- AIS dwell proximity tiers for vessel presence near assets.
- Fusion v2 event features for event-level weather and bathymetry context.
- SCADA service/downtime labels for local validation of workability labels.
- Fault logs or work orders for confirmed failure evidence.

Proxy evidence is useful but not definitive:

- AIS dwell visits are candidate intervention evidence, not confirmed failures.
- Sea basin is a geographic grouping label, not a physical exposure variable.
- Top-MMSI concentration can indicate fleet/detectability concentration, not
  vessel class.
- High turbine-assignment confidence supports asset-level event assignment, not
  fault causality.

## Confidence Classes

- `A_local_validated`: observed source month with AIS event evidence, usable
  core metocean/bathymetry evidence, and local SCADA/event validation.
- `B_integrated_high_assignment`: observed source month with AIS event evidence,
  wave/bathymetry plus wind or current evidence, and strong turbine-assignment
  support.
- `B_integrated_proxy`: observed source month with AIS event evidence and core
  metocean/bathymetry evidence, but without local validation or strong
  assignment support.
- `C_observed_zero`: observed source month with no detected AIS dwell/event.
  This is coverage, not missingness.
- `C_partial_proxy`: observed source month with partial proxy evidence.
- `D_missing_source`: source month skipped because the AIS source was missing.
- `D_unobserved`: no observed source coverage.

## RQ Data-Layer Dependencies

- `RQ1`: AIS dwell/event evidence, wave evidence, and validation or
  non-operation denominator before calibrated access-probability claims.
- `RQ2`: AIS dwell/event evidence, wave evidence, and vessel registry/access
  metadata.
- `RQ3`: vessel registry/access metadata including build year and provenance.
- `RQ4`: AIS dwell/event evidence, Fusion v2 metocean features, and SCADA/event
  validation for attempted/standby labels.
- `RQ5`: AIS dwell/event evidence, full trajectory/port-gap evidence, and vessel
  registry/access metadata.
- `RQ6`: Fusion v2 metocean event features, wave, wind speed, current, and
  bathymetry evidence. It is ready only for source-aware metocean
  sensitivity/readiness work, not calibrated access-probability claims.
- `RQ7`: SCADA state flags plus external curtailment or market data.
- `RQ8`: SCADA events, turbine metadata, and wake/value inputs.
- `RQ9`: AIS intervention-intensity evidence, turbine metadata, direct AIS
  receiver/source geometry, and fault/work-order validation before failure
  claims.
- `RQ10`: external oil and gas benchmark data.
- `RQ11`: AIS movement evidence, deterministic solar/light features, and vessel
  metadata.
- `RQ12`: stable empirical parameters from AIS, Fusion v2, vessel metadata, and
  validation labels.

## Missingness Rules

- Treat `success_no_ais_in_bbox` as observed coverage with zero detected AIS
  dwell/event evidence.
- Treat `skipped_missing_source` as missing source evidence, not zero.
- Keep missing current, missing wind direction, and missing vessel metadata as
  null feature values and false readiness flags. Do not impute physical zero
  values or unknown-but-present vessel enrichment.
- Keep direct AIS receiver/source geometry absent unless receiver station,
  terrestrial/satellite channel, receiver coordinates, or equivalent metadata is
  present.
- Keep local SCADA validation scoped to the mapped CARE farms and event months.

## Why AIS Visits Are Not Confirmed Failures

An AIS visit shows vessel presence and movement behaviour. It does not identify
the work order, turbine fault, crew task, maintenance outcome, or whether a fault
occurred. A vessel may visit for inspection, scheduled maintenance, corrective
maintenance, commissioning, substation work, standby, transfer attempts, or
logistics. Confirmed failure inference requires SCADA, fault-log, work-order, or
equivalent validation that links the visit to a failure state.

Therefore the RQ9 layer remains maintenance intervention-intensity evidence. It
must not be described as an AIS-derived failure metric.
