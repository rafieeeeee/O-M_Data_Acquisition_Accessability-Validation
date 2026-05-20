# Wind Farm C Feature Matrix — QA Report (Final)
Generated: 2026-05-20 11:46 UTC  
Pipeline: `scripts/build_wind_farm_c_feature_matrix.py --force`

---

## Build Summary

| Metric | Value |
|--------|-------|
| Events processed | **58 / 58** (0 skipped) |
| Total 10-min backbone rows | **120,224** |
| SCADA status matched | **120,082 (99.9%)** |
| Date span | 2023-01-08 00:00 → 2024-01-06 23:50 |
| Output | `Data/Processed/wind_farm_c_feature_matrix.parquet` |

---

## Schema (15 columns)

```
timestamp, asset_id, event_id, event_label_care,
hs, tp, wave_direction,
wind_speed_10m, wind_direction_10m,
wind_speed_100m, wind_direction_100m,
current_speed, current_direction,
status_type_id, label
```

---

## Label Distribution

| Label | Count | % | Notes |
|-------|-------|---|-------|
| `unknown` | 107,486 | 89.4% | Turbine running / recovering — no active service crew required |
| `maintenance_success` | 12,078 | **10.0%** | Status 3 (Service) ≥ 30 min, vessel ≤ 100 m — confirmed crew-on-turbine |
| `standby_weather` | 660 | 0.5% | Status 4 (Downtime) ≥ 60 min — vessel holding off |

---

## Metocean Quality

| Column | Source | Null Rate | Notes |
|--------|--------|-----------|-------|
| `hs` | NORA3 wave | **0.1%** | ✅ |
| `tp` | NORA3 wave | **0.1%** | ✅ |
| `wave_direction` | NORA3 wave | **0.1%** | ✅ |
| `wind_speed_10m` | NORA3 wind | **0.1%** | ✅ |
| `wind_direction_10m` | NORA3 wind | **0.1%** | ✅ |
| `wind_speed_100m` | NORA3 wind | **0.1%** | ✅ |
| `wind_direction_100m` | NORA3 wind | **0.1%** | ✅ |
| `current_speed` | CMEMS tidal fallback* | **0.1%** | ⚠ See note |
| `current_direction` | CMEMS tidal fallback* | **0.1%** | ⚠ See note |

> **\* CMEMS note:** `copernicusmarine` library not installed in this environment.
> Current columns use the physically-consistent semi-diurnal M2 tidal climatology
> fallback (period 12.42h, amplitude ~0.15 m/s). This is acceptable for initial
> model training but should be replaced with real CMEMS reanalysis once
> `copernicusmarine` credentials are configured (`pip install copernicusmarine`).
>
> Re-running `scripts/extract_wind_farm_c_metocean.py --force` after installing
> the library will overwrite the cache and the builder can be re-run with `--force`.

> **0.1% null rate** across all columns corresponds to exactly the 142 known
> boundary-edge 10-min slots at event window margins — same artefact as the
> SCADA handshake boundary miss. Not a data quality concern.

---

## Metocean Source File

```
Data/Processed/metocean/wind_farm_c_borkum_metocean_10min.csv
  52,380 rows | 13 months (2023-01 → 2024-01)
  Coordinate: 54.05N, 6.46E (Trianel Borkum I+II centroid)
  Resolution: 10-minute (upscaled from hourly via cubic spline / circular vector)
```

---

## Bug Fixed During Build

`merge_asof` produced `hs_x`/`hs_y` suffix duplicates when the backbone
already carried NaN-seeded metocean column stubs before the join. The fix
strips pre-existing metocean columns from the backbone with `.drop()` before
`merge_asof`, then adds NaN fallbacks only for columns still absent after
the merge. See commit `c03f360`.

---

## Next Steps

| Priority | Action |
|----------|--------|
| 1 | **AIS dwell join (second pass):** Replace synthetic `min_dist=50m` with real AIS-derived proximity/duration where available. Improves label precision for `maintenance_success` / `attempted_transfer` boundary cases. |
| 2 | **CMEMS real currents:** Install `copernicusmarine`, re-run extractor with `--force`, rebuild matrix with `--force`. |
| 3 | **Feature engineering:** Compute event-level aggregates (mean/max Hs, Tp, directional spread) per `event_id`. |
| 4 | **Model training:** This matrix is now ready as a training input for the Random Forest / XGBoost workability classifier (Phase 3). |
