# Metocean Implementation Spec (NORA3 / FINO1 Pilot)

**Status:** IMPLEMENTED FOR WAVE-ONLY BACKBONE (Strict QA gate pending)

## 1. Inputs

To begin the metocean extraction, the following inputs are required from the AIS processing phase:
- `OM_Events_*.csv` (The finalized event catalogue)
- Event constraints: `start`, `end`, `wind_farm`, `found_id`
- Spatial references: Turbine/foundation coordinates
- **Target Sources:** 
  - **NORA3:** Primary source to be queried first.
  - **FINO1:** Secondary source (to be queried later, after access is approved).

## 2. Output Schema

The extraction process generates a foundation-time backbone, not an event-joined table. The expected wave-only output schema is:

- `found_id` (String: Foundation/turbine ID)
- `timestamp_10min` (Datetime: Normalized to 10-minute intervals)
- `lat` / `lon` (Float: NORA3 grid or selected point coordinates)
- `hs` (Float: Significant wave height, in meters)
- `tp` (Float: Peak wave period, in seconds)
- `wave_direction` (Float: Mean wave direction, in degrees)
- `source` (String: `NORA3`)
- `interpolation_method` (String: `cubic_scalar+circular_vector`)

## 3. NORA3 Extraction Design

The extraction implementation (`src/om_pipeline/ingestion/nora3.py`, orchestrated by `scripts/extract_metocean.py`) adheres to the following design constraints:
- **Spatial Scope:** One foundation-month group processed at a time.
- **Network Protocol:** Serialized, cache-aware THREDDS access (no concurrent bulk requests).
- **Historical Endpoint:** Default NORA3 access is pinned to the legacy aggregate endpoint (`windsurfer/mywavewam3km_files/aggregate/nora3_wave_agg.nc`) for the 2010-2020 Wikinger SOV study; monthly endpoints should be passed only as explicit overrides.
- **Data Preservation:** Hourly raw values from NORA3 must be preserved and cached.
- **Interpolation:** 10-minute interpolation generated separately from the raw fetch.
  - **Hs and Tp:** Scalar cubic interpolation.
  - **Wave Direction:** Circular/vector interpolation (convert to u,v -> interpolate linearly -> convert back to degrees $\theta = \operatorname{atan2}(v, u)$).

## 4. Join Boundary

**Do not join metocean to AIS yet.** 
The implemented extraction creates `Data/Interim/Metocean_NORA3_Backbone.csv`. The event-level join remains a separate future module that should merge dwell events to the 10-minute backbone by `found_id` and timestamp.

## 5. Human Blocker

**FINO1 is currently BLOCKED.**
Do not attempt to pull FINO1 data until BSH Insitu access is formally approved by the human operator. Rely entirely on NORA3 for initial implementation and testing.

---

## 6. Restart Condition

**RUN FULL METOCEAN EXTRACTION ONLY AFTER:**
1. The DuckDB catalog has registered the relevant `dwell_events` and `turbines` views.
2. The current AIS backfill slice or milestone cohort has been reviewed for event plausibility.

Once the above is satisfied, run `scripts/extract_metocean.py`, then validate with `scripts/qa_metocean_backbone.py` before any AIS + metocean join work.

Passing wave-only NORA3 backbone QA is a strict implementation gate. Do not start wind/current expansion or the AIS + metocean join until the wave backbone has passed missing-value, timestamp-alignment, row-count, duplicate, and span-continuity checks.
