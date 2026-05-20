# ADR 0003: CAREtoCompare De-Anonymization and Temporal Alignment

## Status
Completed for production working use

## Context
The CAREtoCompare dataset (published on Zenodo) provides valuable turbine SCADA and event logs for offshore wind O&M modeling. However, to preserve commercial confidentiality, the data is anonymized using:
1. Coordinate suppression (no turbine coordinates are provided).
2. Asset pseudonymization (turbines are labeled with generic asset IDs).
3. Temporal year‑shifting (timestamps are offset by a constant integer number of years, while keeping seasonal, diurnal, and high‑frequency structures intact).

For our O&M workability modeling, we require high‑fidelity metocean conditions (wave heights, periods, wind speeds, and current speeds). Since we must ingest historical hindcasts (e.g., MET Norway's NORA3) which are locked to real‑world calendar coordinates and timestamps, we must de‑anonymize the parent wind farms and align the dataset timestamps to true calendar years.

## Decision
We executed a comprehensive eight‑test validation campaign (see methodology) covering:
1. SCADA inventory
2. Continuous time‑lagged correlation scan (TLCC)
3. Spatial robustness & bootstrap confidence
4. Turbine fingerprinting
5. Directional consistency
6. AIS/SCADA co‑occurrence
7. Turbine registry cross‑check
8. Negative controls

The validation campaign supports treating Wind Farm C as a **high-confidence working mapping** to **Trianel Windpark Borkum I + II** (mixed Borkum I and Borkum II phases). The temporal shift is **0 years** (timestamps are already aligned to the true calendar). AIS/SCADA co-occurrence remains inconclusive because the local AIS catalog does not fully cover the decisive windows; this is an open validation enhancement, not current contradictory evidence.

## Findings
### Wind Farm C (CAREtoCompare)
- **Temporal Alignment:** 0‑year shift (timestamps match real calendar 2022‑2024).
- **Spatial Alignment:** Trianel Windpark Borkum I & II (centroid 54.05 N, 6.46 E).
- **Turbine Fingerprint:** Mixed fleet – ~12 assets match 5 MW Adwen M5000‑116 (Borkum I) and ~10 assets match 6.2 MW Senvion 6.2M152 (Borkum II).
- **Directional Consistency:** Sector‑stratified TLCC correlations consistently higher for Borkum across all wind directions.
- **Registry Cross‑Check:** Official turbine registry confirms 40 × Adwen M5000‑116 and 32 × Senvion 6.2 M152 turbines.
- **Negative Controls:** All independent tests show Borkum outperforming control farms; AIS co‑occurrence is inconclusive due to missing AIS data.

The evidence is strong enough to proceed with production O&M labeling and metocean integration under the Borkum I + II mapping. It should still be described as **high-confidence rather than absolute certainty** until AIS co-occurrence or another independent external operational trace closes the remaining coverage gap.

## Consequences
- **High‑Fidelity Metocean Integration:** NORA3 wave and wind hindcasts can now be applied to the working true operational period of Wind Farm C. Current fields should use CMEMS/Copernicus Marine when credentials are available; otherwise they fall back to the documented tidal climatology.
- **Standardized O&M Labeling:** `SCADAHandshake` uses Wind Farm C timestamps directly with no year shifting, enabling automatic classification of dwell events.
- **Documentation Updated:** All validation artifacts are archived under `reports/care_wind_farm_c_confirmation/`.
