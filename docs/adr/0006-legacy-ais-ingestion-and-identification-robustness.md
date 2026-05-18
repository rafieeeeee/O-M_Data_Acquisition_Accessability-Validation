# ADR 0006: Robust Ingestion of Legacy AIS Slices and Vessel Identification Aggregation Fixes

## Status
Accepted

## Context
1. **Legacy Ingestion Mismatch:** When backfilling legacy pre-2017 Danish Maritime Authority (DMA) AIS data, some monthly zip archives do not contain headers. Our pipeline uses a `resolve_ais_header` function with a fallback to `LEGACY_AIS_HEADER`. However, some historical files have minor column discrepancies (such as trailing delimiters or empty columns), causing `len(first_row)` to be slightly different from the hardcoded length of `LEGACY_AIS_HEADER` (22 elements). This leads to unhandled `ValueError` crashes during the streaming phase.
2. **Identification Aggregation Crash:** During the `identify` phase, pandas attempts to run a `.agg({'Length': 'max', 'Draught': 'max'})` reduction. In historical slices, vessel length and draught values are often parsed as mixed object/string columns containing empty strings (`''`) or placeholders (`'Unknown'`, `'Undefined'`). Pandas raises `TypeError: agg function failed [how->max,dtype->object]` because comparing numeric values with string placeholders is invalid.

## Decision
1. **Robust Header Resolution:** Modify `resolve_ais_header` in `src/om_pipeline/ingestion/ais.py` to dynamically pad or truncate `LEGACY_AIS_HEADER` when a file is determined to be headerless but has a column count slightly different from 22. This keeps the stream flowing while preserving column mappings.
2. **Numeric Coercion for Vessel Attributes:** Coerce the `Length` and `Draught` columns to numeric floats using `pd.to_numeric(..., errors='coerce')` in `identify_vessels` in `src/om_pipeline/identification/dwell_events.py` before performing group-by aggregations. This ensures that non-numeric values are converted to `NaN`, enabling safe and accurate mathematical reductions (`max`, `first`).

## Consequences
- **Failure Resilience:** The pipeline can ingest any pre-2017 historical DMA slice regardless of minor schema drift or trailing delimiter anomalies.
- **Accurate Vessel Registry:** Vessel physical dimensions (`Length` and `Draught`) will be correctly modeled as numeric types in the compiled fleet registries and event outputs, allowing accurate downstream analysis.
- **Cleaner Pipeline Execution:** Running the full backfill (2010–2020) will no longer crash mid-stream or mid-identification.
