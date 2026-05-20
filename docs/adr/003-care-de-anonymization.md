# ADR 0003: CAREtoCompare De-Anonymization and Temporal Alignment

## Status
Completed

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

All tests except the AIS/SCADA co‑occurrence (limited by pre‑2024 AIS coverage gaps) confirmed the identity of Wind Farm C as **Trianel Windpark Borkum I + II** (mixed Borkum I and Borkum II phases). The temporal shift is **0 years** (timestamps are already aligned to the true calendar).

## Findings
### Wind Farm C (CAREtoCompare)
- **Temporal Alignment:** 0‑year shift (timestamps match real calendar 2022‑2024).
- **Spatial Alignment:** Trianel Windpark Borkum I & II (centroid 54.05 N, 6.46 E).
- **Turbine Fingerprint:** Mixed fleet – ~12 assets match 5 MW Adwen M5000‑116 (Borkum I) and ~10 assets match 6.2 MW Senvion 6.2M152 (Borkum II).
- **Directional Consistency:** Sector‑stratified TLCC correlations consistently higher for Borkum across all wind directions.
- **Registry Cross‑Check:** Official turbine registry confirms 40 × Adwen M5000‑116 and 32 × Senvion 6.2 M152 turbines.
- **Negative Controls:** All independent tests show Borkum outperforming control farms; AIS co‑occurrence is inconclusive due to missing AIS data.

The evidence meets the final synthesis rule (Borkum wins TLCC, robust across windows, registry & fingerprint consistent). Therefore the de‑anonymization is **complete for production O&M labeling and metocean integration**. The AIS/SCADA co‑occurrence gap remains archived as a coverage-limited validation enhancement, not a blocker to the 0-year mapping.

## Consequences
- **High‑Fidelity Metocean Integration:** Accurate NORA3 hindcasts can now be applied to the true operational period of Wind Farm C.
- **Standardized O&M Labeling:** `SCADAHandshake` uses Wind Farm C timestamps directly with no year shifting, enabling automatic classification of dwell events.
- **Documentation Updated:** All validation artifacts are archived under `reports/care_wind_farm_c_confirmation/`.
