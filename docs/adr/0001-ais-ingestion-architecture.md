# ADR 0001: AIS Ingestion Architecture

## Status
Accepted

## Context
The project requires historical AIS data from the Danish Maritime Authority (DMA). The archive format changed in 2024:
- **Pre-2024:** Monthly ZIP archives containing a single large CSV.
- **2024+:** Daily ZIP archives (one per day) within a monthly S3 prefix.

## Decision
We implemented a unified ingestion logic in `stream_ais_filter.py` that:
1. Enumerates all keys under a monthly prefix (e.g., `2024/`).
2. Identifies relevant archives (monthly or daily) based on naming patterns.
3. Streams ZIP files directly from S3.
4. Unzips and filters the inner CSV row-by-row to minimize memory usage and disk writes.

## Consequences
- **Pros:** Handles both legacy and new DMA formats; low memory footprint; atomic writes (using `.tmp` files).
- **Cons:** Requires active S3 listing permissions; slow for very large temporal ranges due to serial processing.
