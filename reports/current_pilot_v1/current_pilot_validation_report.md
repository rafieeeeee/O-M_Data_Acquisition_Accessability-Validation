# Current Pilot v1 Validation Report

## Research Design

Current matters for RQ1, RQ4, RQ6, and RQ12 because waves alone cannot describe vessel manoeuvring, DP load, approach aborts, or simulator-ready metocean forcing. This increment pilots true Eulerian `u/v` products before broad download so cadence, depth, domain, and provenance failures are visible while the blast radius is still one farm/year.

Event-scale suitability means that current evidence has real `uo/vo`, documented cadence and depth, close spatial match, product-domain fit, source provenance, no fallback/simulated values, enough samples in the dwell window, small nearest-time gaps, and source timestamps bracketing dwell windows where the cadence permits.

Acceptance gates: true `uo/vo`; no Stokes drift; no legacy/fallback CSV promotion; UTC timestamps; documented depth; populated product/dataset/source provenance; physically consistent speed/direction; overwrite-safe outputs.

## Product Metadata Summary

| Pilot | Product ID | Dataset ID | Native Cadence | Spatial Resolution | Depth | Event-scale Prior |
| --- | --- | --- | ---: | ---: | --- | --- |
| Baltic | `BALTICSEA_MULTIYEAR_PHY_003_011` | `cmems_mod_bal_phy_my_P1D-m` | 1440 min | 2 km | 3D physics grid with 56 depth levels; pilot uses nearest-surface uo/vo only | Contextual before validation: true u/v is available, but the approved multi-year reanalysis current dataset is daily, not event-hourly. |
| NWS | `NWSHELF_MULTIYEAR_PHY_004_009` | `cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i` | 60 min | 7 km | 2D surface current product for uo/vo; no depth dimension expected | Potentially event-scale where the farm lies in product coverage and hourly samples bracket dwell windows. |

## Pilot Results

### BALTIC Wikinger 2020

- Status: ran
- Blocked reason: not blocked
- Candidate path: `Data/Processed/metocean/current_pilots/baltic_current_candidates.parquet`
- Row count: 25986
- Valid `u/v` rows: 25986
- Sample points: 71
- Timestamp range: 2020-01-01 00:00:00+00:00 to 2020-12-31 00:00:00+00:00
- Native cadence: 1440 minutes
- Depth levels used: [0.5016462206840515]
- Missing `u/v` rows: 0
- Speed consistency max error: 0.00000000
- Direction range: 0.018 to 359.991 degrees
- Spatial distance p50/p95: 0.749 / 1.091 km
- Provenance complete: True
- File/storage size: 345398 bytes

Event-scale suitability:
- Dwell events: 49
- Bracketed events: 49
- Events with in-window samples: 0
- Nearest gap p50/p95: 655.133 / 718.428 minutes
- Window sample count p50/p95: 0.000 / 0.000
- Suitable percentage: 0.0%
- Confidence class: `B_contextual`
- Confidence reason: True u/v current evidence is present but cadence is contextual rather than event-hourly.

### NWS Borkum Riffgrund 2 2020

- Status: ran
- Blocked reason: not blocked
- Candidate path: `Data/Processed/metocean/current_pilots/nws_current_candidates.parquet`
- Row count: 500688
- Valid `u/v` rows: 500688
- Sample points: 57
- Timestamp range: 2020-01-01 00:00:00+00:00 to 2020-12-31 23:00:00+00:00
- Native cadence: 60 minutes
- Depth levels used: [0.0]
- Missing `u/v` rows: 0
- Speed consistency max error: 0.00000000
- Direction range: 0.000 to 359.653 degrees
- Spatial distance p50/p95: 2.830 / 3.990 km
- Provenance complete: True
- File/storage size: 4498964 bytes

Event-scale suitability:
- Dwell events: 15
- Bracketed events: 15
- Events with in-window samples: 14
- Nearest gap p50/p95: 11.808 / 29.427 minutes
- Window sample count p50/p95: 6.000 / 20.900
- Suitable percentage: 100.0%
- Confidence class: `A_event_scale`
- Confidence reason: Hourly true u/v current evidence closely matches the farm and brackets most dwell windows.

## Recommendation

- Scale Baltic currents only if daily/contextual evidence is sufficient for the intended model or an hourly Baltic true-current source is approved separately.
- Scale NWS currents if the scoped NWS pilot produces real hourly `u/v` rows with adequate event-window bracketing and spatial match.
- Keep global currents as fallback assessment only until regional gaps are proven.
- Reuse this candidate/provenance/confidence pattern for any Current v1 agreement layer.
