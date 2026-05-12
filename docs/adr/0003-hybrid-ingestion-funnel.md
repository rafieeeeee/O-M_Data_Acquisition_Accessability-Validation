# ADR 0003: Hybrid Ingestion Funnel

## Status
Accepted

## Context
At the European Master Box scale, the standard "Regional Slice" ingestion mode (Mode A) produces massive files (~20GB/month) because it retains all slow-moving vessels within the geographic bounds. While valuable for audit and regional maritime studies, over 95% of this data is "dead weight" for a longitudinal study focused specifically on offshore wind O&M activity.

## Decision
We will pivot to a **Hybrid Ingestion Funnel** architecture that supports two distinct operational modes:

1.  **Mode A (Regional Slice):**
    -   **Behavior:** Retains all AIS pings within the regional bounding box (e.g., European Master Box) that meet the speed filter.
    -   **Usage:** Validation and audit months (e.g., years with RAVE SCADA ground truth).
    -   **Pros:** High fidelity; reusable for non-O&M studies.
    -   **Cons:** High storage cost (~240GB/year at scale).

2.  **Mode B (Farm-Candidate Extraction):**
    -   **Behavior:** Filters AIS pings directly against a spatial index of wind farm bounding boxes, buffered by a configurable distance (default 2nm).
    -   **Usage:** Standard longitudinal months (2009–2024 quarterly slices).
    -   **Pros:** Low storage cost (~100-200MB/month); optimized for O&M identification.
    -   **Cons:** May exclude vessels waiting/drifting far (e.g., >5km) outside the buffered farm boundaries.

## Consequences
-   **Pros:** Reduces the total 15-year project storage footprint from >3TB to <100GB.
-   **Cons:** Mode B slices are "lossy" with respect to general maritime traffic; re-evaluating waiting areas wider than the buffer requires a full re-ingestion from the source ZIPs.
-   **Technical Debt:** Adds a dependency on a turbine coordinate source during the ingestion phase for Mode B.
