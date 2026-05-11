# ADR 0002: Data Layering and Storage Policy

## Status
Accepted

## Context
The project processes multi-terabyte AIS datasets. Local storage is limited (1.5TB), and versioning large binary/CSV files in Git is impractical.

## Decision
We enforce a tiered data lifecycle:
1. **Raw:** Regional-filtered CSVs. Stored in `Data/Raw/`. These are the "source of truth" for the local pipeline.
2. **Interim:** Partially processed data (e.g., Fleet Registries). Stored in `Data/Interim/`.
3. **Processed:** Final training matrices and model artifacts. Stored in `Data/Processed/`.

**Storage Rules:**
- `Data/` is excluded from Git via `.gitignore`.
- Raw archives (ZIPs from S3) are deleted immediately after regional filtering.
- Any failed large downloads (e.g., `temp_check.zip`) should be manually cleared if they exceed 50GB.

## Consequences
- **Pros:** Keeps the repository lightweight; optimizes disk usage.
- **Cons:** Requires re-running the ingestion pipeline if raw regional CSVs are lost.
