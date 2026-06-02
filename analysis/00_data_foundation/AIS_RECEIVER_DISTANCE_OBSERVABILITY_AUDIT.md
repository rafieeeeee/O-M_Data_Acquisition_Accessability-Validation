# AIS Receiver Distance Observability Audit Methodology

## Purpose

This audit tests whether AIS-derived farm-level event rates may be geographically skewed by source coverage or detectability. It is a data-readiness layer, not a new RQ model and not a failure-rate analysis.

## Reproducibility

Source of truth: `scripts/build_ais_observability_bias.py` and `src/om_pipeline/analysis/ais_observability_bias.py`.

Rebuild from the repository root:

```bash
/opt/anaconda3/bin/python scripts/build_ais_observability_bias.py
```

Derived matrices under `Data/Processed/analysis/ais_observability_bias/` may be ignored by git. Rebuild them from existing local artifacts rather than editing them directly.

## Evidence Tiers

Tier 1a is per-message receiver assignment evidence: vessel pings linked to receiving station IDs, receiver channel, or equivalent source-provider assignment. Only per-message receiver assignment can directly test receiver-distance bias.

Tier 1b is direct AIS source-geometry reference: `Type of mobile = Base Station` rows with station MMSI and latitude/longitude, or source-provider geometry. This supports farm distance to nearest observed base station as a source-geometry control, but it does not prove which station received a vessel ping.

Tier 2 is source-channel evidence: `Data source type`, source-provider fields, source file/month status, and manifest `input_rows`/`clean_rows`. Tier 2 supports source availability and source-intensity diagnostics but not receiver-distance causality.

Tier 3 is geographic proxy evidence: farm centroid, country, sea basin, water depth, bathymetry, distance to a provenanced external receiver reference, or offshore-distance proxies. These are proxy controls only.

Tier 4 is downstream observability proxy evidence: observed-zero months under coverage, dwell/event density, Tier A/B/C/D counts, assignment confidence, top-MMSI concentration, unique MMSI count, and vessel metadata completeness.

## Missingness Semantics

- `success` and `success_no_ais_in_bbox` are observed source coverage.
- `success_no_ais_in_bbox` is observed zero AIS activity.
- `skipped_missing_source` is missing source evidence and is excluded from observed-zero and event-density denominators.
- Missing per-message receiver assignment is reported as unavailable and is never imputed from vessel positions.
- Observed AIS base-station geometry is reported separately from per-message receiver assignment.
- Missing external receiver/coastline provenance blocks use of external distance references.

## Matched Base-Station Distance Diagnostic

Observed-source farm-months can be binned by distance to nearest observed AIS base-station geometry within country/sea-basin/year-month strata. Strata with at least eight farm-months use quartiles; strata with four to seven farm-months use a near/far median split; smaller strata are marked `insufficient_matched_strata`.

This diagnostic compares source `clean_rows`, observed-zero rates, dwell counts, and Tier A/B/C/D rates. Strong clean-row declines plus observed-zero increases are evidence consistent with geographical AIS observability bias, but still not receiver-distance causality because the nearest observed base station is not a per-message receiving-station assignment.

## Guardrails

- AIS dwell/events are candidate intervention proxies, not confirmed failures.
- Absence of AIS events is not absence of activity unless source observability is established.
- Lower AIS event density is not lower intervention activity and not lower failure rate.
- Nearest observed base station is a source-geometry control, not proof of the receiving station for a vessel ping.
- Nearest coast is not a receiver proxy unless explicitly justified by an accepted external reference.
- Raw Baltic/North Sea contrasts must be checked using source-aware, within-country, or matched-strata diagnostics before interpretation.
