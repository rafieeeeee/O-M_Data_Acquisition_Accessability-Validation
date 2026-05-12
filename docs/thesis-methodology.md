# Thesis Methodology Documentation

**Goal:** Translate the pipeline choices, thresholds, and technical compromises into a defensible methods note suitable for academic publication.

## 1. Introduction & Context
- Define the research gap: Moving from static theoretical O&M limits to dynamic, empirical "workability surfaces" using data fusion (AIS + Metocean + SCADA).

## 2. Sampling Strategy
- **Temporal Scope:** Quarterly slices (e.g., Q1/Q3) were chosen over a continuous multi-year ingest due to the ~300GB/month size of European AIS, ensuring seasonal representation.
- **Spatial Scope:** The "German Bight" serves as the primary pilot and default region. The "European Master Box" (46.5N–60.0N, -4.5E–15.0E) is available as an optional scaling mechanism.

## 3. Filtering Thresholds & Heuristics
- **Speed Filter (Two-Stage):** 
  - *Ingestion Candidate Prefilter:* `SOG <= 2.0 knots`. Justification: Safely isolates slow/stationary candidates while maintaining a buffer for GPS drift.
  - *Dwell-Event Definition:* `mean_sog < 0.5 knots` or event-level stationary logic. Justification: Ensures only vessels physically engaged with a foundation (or idling) are considered, removing transit traffic.
- **Proximity Filter:** `100m radius`. Justification: Accounts for GPS drift, foundation dimensions, and typical safe operating distances during a transfer.
- **Dwell Duration:** `Min 15 minutes`. Justification: Shorter intervals are likely noise or pass-bys; 15 minutes is the minimum time for a meaningful personnel transfer.
- **Event Separation:** `30 minutes`. Justification: If a vessel drops AIS connection or briefly steps away and returns within 30 minutes, it is considered a single continuous maintenance event.

## 4. Known Biases & Limitations
- **AIS Spoofing/Dropouts:** Vessels may turn off AIS near foundations, leading to truncated event durations.
- **Spatial Overlap:** Wind farms very close to shipping lanes may accumulate false positives (addressed by the SOG/Proximity combo, but still a risk).
- **Turbine Coordinates:** The Open European database is largely accurate but may lack newly commissioned foundations or decommissioned test sites.

## 5. Validation Plan
- Cross-reference the heuristic-derived events at Alpha Ventus (and potentially others) against true operator SCADA/DPR logs.
- This will provide a confusion matrix (Precision/Recall) for the AIS extraction logic, validating the 100m / 15-min assumptions.
