# ADR 0007: Metocean Expansion for Wind and Current Ingestion

## Status
Accepted

## Context
1. **Multi-Parameter Operational Workability:** Wave state (significant wave height and peak period) is the primary driver of SOV and CTV limits, but wind speed/direction (at hub height and deck height) and ocean currents (surface current speed and direction) are critical constraints for dynamic positioning (DP) operations, cargo transfers, gangway connections, and vessel maneuvering.
2. **Atmospheric Data Availability:** NORA3 provides hourly 3km atmospheric subsets (`nora3_subset_atmos/wind_hourly_v2`) containing wind speed and direction at multiple vertical levels including 10m and 100m.
3. **Current Data Availability:** Copernicus Marine Service (CMEMS) provides NWSHELF_MULTIYEAR_PHY_004_009, a high-fidelity 7km resolution daily/hourly reanalysis of Atlantic-European North West Shelf physical oceanography including surface currents (`uo`, `vo`).
4. **Environment Portability & Resilience:** Executing automated pipelines and unit tests across environments should not fail due to missing external library installations (e.g. `copernicusmarine`) or missing personal CMEMS login credentials.

## Decision
We will expand the metocean acquisition backbone to a comprehensive 10-minute synchronized matrix including wind and currents:
1. **NORA3 Wind Ingestion:** Implement wind speed/direction extraction at 10m and 100m hub heights using MET Norway's NORA3 hourly atmospheric subsets (`nora3_subset_atmos/wind_hourly_v2/arome3kmwind_1hr_YYYYMM.nc`). We will index the standard vertical height coordinate at `height=10` and `height=100`.
2. **CMEMS Current Ingestion:** Set up an ocean current extraction layer (`src/om_pipeline/ingestion/cmems.py`) leveraging the Copernicus Marine Toolbox API (`copernicusmarine`) to fetch eastward (`uo`) and northward (`vo`) current velocities from dataset `cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i`.
3. **Graceful Fallbacks & Offline Capability:** To ensure robustness in non-interactive CI/CD, testing, or offline environments, the CMEMS client will catch missing package imports (`copernicusmarine`) and connection/auth failures, gracefully falling back to physically realistic simulated currents based on coordinate climatology.
4. **Generalized Resampling and Upscaling:** Refactor `MetoceanIngestor.upscale_to_10min` to dynamically identify scalar fields (e.g. `*_speed`, `hs`, `tp`) for Cubic Spline interpolation, and circular vector columns (e.g. `*_direction`) to automatically decompose into orthogonal components, resample, linearly interpolate, and reconstruct the angular profiles [0, 360).
5. **Unified Metocean Backbone Schema:** Expand the intermediate backbone dataset to a 13-column schema:
   `[found_id, timestamp_10min, lat, lon, hs, tp, wave_direction, wind_speed_10m, wind_direction_10m, wind_speed_100m, wind_direction_100m, current_speed, current_direction, source, interpolation_method]`

## Consequences
- **Network Load & Caching:** We will maintain strict serial execution with localized coordinates and monthly file caching policies (caching wave, wind, and current raw records separately) to prevent IP blocking.
- **Robust Pipeline Execution:** By generating realistic current profiles when credentials/packages are absent, we guarantee that the pipeline can run end-to-end anywhere, while printing clear warnings when real downloads are bypassed.
- **Strict Quality Control:** The QA gate (`scripts/qa_metocean_backbone.py`) will be updated to audit the extended schema, ensuring zero nulls and valid angular dimensions.
