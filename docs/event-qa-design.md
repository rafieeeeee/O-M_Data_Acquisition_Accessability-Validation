# Event QA Design

**Goal:** Build an audit plan for pilot outputs to ensure the AIS ingestion and filtering logic produces valid, robust O&M dwell events.

## 1. Audit Components

### A. Duration Histograms
- **Metric:** Distribution of event `duration_min`.
- **Sanity Check:** 
  - Are there massive spikes at exactly 15 minutes (our lower bound)?
  - Are there impossibly long events (e.g., > 48 hours at a single foundation), indicating a logic error in event termination?
- **Action:** Plot histograms grouped by `Ship type` and `wind_farm`.

### B. Distance Distributions
- **Metric:** `min_dist` from vessel to foundation for each ping.
- **Sanity Check:** 
  - Ensure the 100m proximity filter is strictly enforced.
  - Check the distribution shape: most pings should be < 50m if genuinely connected to a foundation.
- **Action:** Generate density plots of `min_dist` for all identified events.

### C. Vessel Class Sanity Checks
- **Metric:** Cross-reference `Ship type`, `Length`, and `Name` from the fleet registry.
- **Sanity Check:** 
  - Are massive cargo ships (> 150m length) being flagged as O&M vessels? (Indicates coordinate overlap or AIS spoofing).
  - Are pleasure crafts overwhelming the registry?
- **Action:** Filter the registry for `Length > 100m` or `Ship type == 'Cargo'` and manually inspect those MMSIs.

### D. Farm-Level Counts
- **Metric:** Number of events per wind farm.
- **Sanity Check:**
  - Do massive wind farms (e.g., Hornsea) have proportionally more events?
  - Are there farms with zero events despite known operations? (Indicates coordinate mismatch or bounding box failure).
- **Action:** Map total event counts against the number of foundations per farm.

### E. Suspicious MMSIs
- **Metric:** High frequency or highly dispersed events.
- **Sanity Check:** 
  - Is a single MMSI recording events at Alpha Ventus and a UK farm within the same hour? (Impossible travel times).
- **Action:** Calculate max velocity between consecutive events for a single MMSI. Flag if > 30 knots.

## 2. QA Dashboard / Script
A script `scripts/audit_pilot.py` will read the DuckDB catalog and generate these metrics and plots automatically after the July AIS pilot finishes.
