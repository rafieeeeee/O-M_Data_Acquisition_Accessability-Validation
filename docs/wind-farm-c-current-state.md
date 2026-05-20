# Wind Farm C Current State

Updated: 2026-05-20

## Current Conclusion

Wind Farm C is treated as a **high-confidence working mapping** to **Trianel Windpark Borkum I & II** with a **0-year temporal shift**. This is strong enough for the current feature-matrix and O&M-labeling work, but it should not be described as absolute certainty. The remaining open validation items are AIS co-occurrence coverage and real CMEMS current reanalysis.

## Why The Mapping Is Now Stronger

The case is not based on one test. It is based on convergence across independent evidence families:

1. **Temporal alignment:** Continuous SCADA/NORA3 scans support direct use of CARE timestamps. The implemented `SCADAHandshake` now uses Wind Farm C timestamps with no year shift, and the 58-event QA slice matched SCADA status for 120,082 of 120,224 10-minute rows (99.9%). The old shifted lookup would search the wrong calendar window.
2. **Borkum-region meteorology:** NORA3 wind correlation supports a 0-year calendar mapping. Nearby German Bight farms can be close under pure wind correlation, so this evidence supports the time window more strongly than it uniquely proves the exact farm.
3. **Turbine fingerprint:** Wind Farm C contains 22 observed CARE assets with power-curve behavior consistent with a mixed 5 MW / 6.15 MW class fleet. That matches the Borkum I + II registry pattern better than a single homogeneous farm.
4. **Registry cross-check:** The local registry has Trianel Windpark Borkum I as 40 Adwen M5000-116 turbines and Borkum II as 32 Senvion 6.2M152 turbines, centered at roughly 54.05N, 6.46E.
5. **Negative controls and directional checks:** The validation campaign shows Borkum-region candidates remain competitive under windowed and directional checks. These checks reduce the risk that the mapping is only a single-window artifact.
6. **AIS co-occurrence:** This remains inconclusive because the local AIS catalog has coverage gaps for the decisive window. It is not currently contradictory evidence, but it is the main reason the mapping should be called high-confidence rather than absolute.

## Metocean Sources In Use

The current feature matrix uses two environmental data families:

- **NORA3 / MET Norway:** Used for waves and wind. The matrix columns `hs`, `tp`, `wave_direction`, `wind_speed_10m`, `wind_direction_10m`, `wind_speed_100m`, and `wind_direction_100m` are sourced from NORA3 and upscaled from hourly to 10-minute cadence.
- **CMEMS / Copernicus Marine:** Intended source for ocean currents (`current_speed`, `current_direction`). In the current local run, `copernicusmarine` was not installed/configured, so the pipeline used the documented semi-diurnal tidal climatology fallback. This is acceptable for pipeline testing and first baseline models, but final current-sensitive analysis should re-run with real CMEMS data.

NORA3 and CMEMS are complementary, not alternatives. NORA3 covers atmospheric and wave hindcast fields. CMEMS covers ocean currents. The validation plan should cross-check environmental sensitivity where fields overlap or where independent data become available, but the current production matrix is not choosing one model over the other.

## Current Artifacts

- Feature matrix: `Data/Processed/wind_farm_c_feature_matrix.parquet` (local generated artifact, not committed)
- Borkum metocean backbone: `Data/Processed/metocean/wind_farm_c_borkum_metocean_10min.csv` (local generated artifact, not committed)
- Feature matrix QA: `reports/care_wind_farm_c_confirmation/wfc_feature_matrix_qa.md`
- SCADA labeling detail: `reports/care_wind_farm_c_confirmation/wfc_labeling_slice_detail.csv`
- De-anonymization ADR: `docs/adr/003-care-de-anonymization.md`

## Immediate Next Steps

1. **Event-level aggregation:** Build one row per CARE event from the 10-minute feature matrix. Include metocean mean/max/std, directional spread, SCADA label shares, and a clear event-level target.
2. **First baseline model:** Train a simple Random Forest or XGBoost classifier on the event-level table. Treat this as a diagnostic baseline, not the final thesis model.
3. **AIS proximity replacement:** Replace the synthetic `min_dist=50m` placeholder with real AIS dwell geometry wherever local catalog coverage exists.
4. **Real CMEMS rerun:** Install/configure `copernicusmarine`, rerun `scripts/extract_wind_farm_c_metocean.py --force`, then rerun `scripts/build_wind_farm_c_feature_matrix.py --force`.
5. **Evidence language discipline:** Use "high-confidence working mapping" or "production working mapping" unless AIS co-occurrence or another independent external check closes the remaining gap.
