# ADR 0030: Non-Destructive Vessel Dwell Validation and Deduplication

## Status
Accepted

## Context
ADR 0013 defines the interim event deduplication and validation gate. This ADR
records the complementary preservation rule for vessel dwell validation: duplicate
candidates may be flagged, grouped, validated, and de-duplicated in derived
interim products, but raw and processed AIS/backfill files must not be purged to
force a clean analysis result.

During cross-farm analysis of O&M vessel activity in co-located or overlapping
wind farms, spatial bounding boxes and proximity filters can capture identical
pings. This results in duplicate dwell events when aggregating across multiple
files. A destructive purge from per-farm Parquet files would make later boundary
or distance-rule corrections impossible to audit.

## Decision
1. Core AIS/backfill ingestion remains additive and flag-based. It may identify
   `possible_cross_farm_duplicate` rows and group them via `duplicate_group_id`,
   but it must not delete records from raw or processed Parquet files.
2. Deduplication for analysis readiness belongs in validation or interim layers,
   where the input evidence can be regenerated from preserved sources.
3. Validation schema normalization may add compatibility columns such as
   `vessel_id`, `Timestamp`, and `event_type`, but original event evidence must
   remain available for downstream audit.
4. Cleanup work must preserve a patch, manifest, quarantine copy, or regenerated
   source path before removing untracked or derived local artifacts from the
   working tree.

## Consequences
- Preserves full auditability and historical data integrity in raw and processed
  backfill files.
- Keeps ADR 0013 focused on the validation gate while this ADR captures the
  non-destructive preservation rule.
- Prevents cleanup or deduplication shortcuts from silently erasing evidence that
  may be needed for later boundary, vessel, or site-level reconciliation.
