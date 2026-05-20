# Wind Farm C SCADA Labeling Slice — QA Report
Generated: 2026-05-20 10:18 UTC  
Pipeline: `scripts/run_wind_farm_c_labeling_slice.py` (all 58 events)

---

## Summary

| Metric | Value |
|--------|-------|
| Events processed | 58 / 58 |
| Events with SCADA file | **58** (0 missing) |
| Total 10-min backbone rows | **120,224** |
| Rows with SCADA status matched | **120,082** (99.9%) |
| Rows unmatched (status = NaN) | **142** (0.1%) — boundary artefacts only |
| Date span | 2023-01-08 → 2024-01-06 |

> **0.1% unmatched rate** is expected: these are 10-min boundary ticks at the edges of event windows where the SCADA file's recorded range ends. No structural data gap.

---

## Label Distribution

| Label | Count | % of Total | Interpretation |
|-------|-------|------------|----------------|
| `unknown` | 107,486 | 89.4% | Turbine running / recovering — no maintenance crew action required |
| `maintenance_success` | 12,078 | **10.0%** | Status 3 (Service) ≥ 30 min, vessel within 100 m — confirmed crew-on-turbine |
| `standby_weather` | 660 | 0.5% | Status 4 (Downtime) ≥ 60 min — vessel holding off, weather standby |
| `attempted_transfer` | 0 | 0.0% | No short-duration service or abort events in this event set |

---

## SCADA Status Distribution (sampled)

The 10-min records across all 58 events show a mixed fleet in active O&M:
- **Status 3 (Service)** drives `maintenance_success` labels — healthy signal for anomaly events.
- **Status 0/1/2** (Normal/Derated/Idling) dominates `unknown` — consistent with turbines recovering between faults or normal operation reference windows.
- **Status 4 (Downtime)** accounts for the `standby_weather` tail.

---

## Zero-Shift Confirmation

> **All 58 event windows resolve within 2023-01-08 → 2024-01-06.**
> This is the direct true operating calendar for Trianel Windpark Borkum I+II.
> Under the old +1-year shift, lookups would have searched 2024–2025 data (non-existent),
> producing **0% SCADA match** and **100% `unknown` labels** — the exact regression we fixed.

---

## Files in This Directory

| File | Description |
|------|-------------|
| `wfc_labeling_slice_detail.csv` | Full 120,224-row backbone with per-slot status + labels |
| `wfc_qa_report.md` | This QA summary |

---

## Next Step

Run the production feature matrix builder to join metocean backbone:

```bash
python scripts/build_wind_farm_c_feature_matrix.py
# Output: Data/Processed/wind_farm_c_feature_matrix.parquet
```

Schema: `timestamp | asset_id | event_id | event_label_care | hs | tp | wave_direction | wind_speed_10m | wind_direction_10m | wind_speed_100m | wind_direction_100m | current_speed | current_direction | status_type_id | label`
