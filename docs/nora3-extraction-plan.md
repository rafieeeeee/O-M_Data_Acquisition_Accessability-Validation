# Metocean Implementation Spec (NORA3 / FINO1 Pilot)

**Status:** PREPARED (Awaiting AIS July Pilot & Vessel Identification Review)

## 1. Inputs

To begin the metocean extraction, the following inputs are required from the AIS processing phase:
- `OM_Events_*.csv` (The finalized event catalogue)
- Event constraints: `start`, `end`, `wind_farm`, `found_id`
- Spatial references: Turbine/foundation coordinates
- **Target Sources:** 
  - **NORA3:** Primary source to be queried first.
  - **FINO1:** Secondary source (to be queried later, after access is approved).

## 2. Output Schema

The extraction process will generate an expected metocean event table with the following schema:

- `event_id` (String/UUID: Foreign key linking to the AIS dwell event)
- `MMSI` (Integer: Vessel identifier)
- `found_id` (String: Foundation/turbine ID)
- `timestamp_10min` (Datetime: Normalized to 10-minute intervals)
- `lat` (Float: Latitude of the foundation)
- `lon` (Float: Longitude of the foundation)
- `Hs` (Float: Significant wave height, in meters)
- `Tp` (Float: Peak wave period, in seconds)
- `wave_direction` (Float: Mean wave direction, in degrees)
- `source` (String: 'NORA3' or 'FINO1')
- `interpolation_method` (String: e.g., 'cubic_scalar', 'circular_vector', 'none')

## 3. NORA3 Extraction Design

The extraction script (`src/om_pipeline/ingestion/nora3.py` or similar) will adhere to the following design constraints:
- **Spatial Scope:** One coordinate/farm processed at a time.
- **Network Protocol:** Serialized, cache-aware THREDDS access (no concurrent bulk requests).
- **Data Preservation:** Hourly raw values from NORA3 must be preserved and cached.
- **Interpolation:** 10-minute interpolation generated separately from the raw fetch.
  - **Hs and Tp:** Scalar cubic interpolation.
  - **Wave Direction:** Circular/vector interpolation (convert to u,v -> interpolate linearly -> convert back to degrees $\theta = \operatorname{atan2}(v, u)$).

## 4. Join Boundary

**Do not join metocean to AIS yet.** 
This specification strictly prepares the *design* for the extraction and structure of the metocean dataset. The actual database join or backbone construction remains out of scope for this step.

## 5. Human Blocker

**FINO1 is currently BLOCKED.**
Do not attempt to pull FINO1 data until BSH Insitu access is formally approved by the human operator. Rely entirely on NORA3 for initial implementation and testing.

---

## 6. Restart Condition

**RESTART METOCEAN IMPLEMENTATION ONLY AFTER:**
1. The July European farm-candidate slice and vessel identification are completed and reviewed.
2. `OM_Events_*.csv` and associated foundation coordinates are confirmed and available.

Once the above is satisfied, you may implement the NORA3 extractor and interpolation scripts following the design in Section 3.
