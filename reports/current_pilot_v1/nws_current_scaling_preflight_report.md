# NWS Current Scaling Preflight Report

## Research Design

Current Pilot v1 proved that NWS hourly surface `uo/vo` can be event-scale at Borkum Riffgrund 2, but the pilot was only one farm/year. This preflight ranks observed farm-years before any broad current extraction so NWS domain limits, dwell evidence, Tier A density, wave-confidence support, bathymetry, shallow-water warnings, and storage/runtime are visible first.

Acceptance gates for normal scale: NWS current domain match, non-trivial dwell count, non-trivial Tier A count, accepted bathymetry sample points, no dominance by <=10 m sample depths, and acceptable storage/runtime estimate. Shallow/coastal or low-Tier-A but useful cases are marked `stress_test_only`, not normal scale.

## NWS Product Basis

- Product: [`NWSHELF_MULTIYEAR_PHY_004_009`](https://data.marine.copernicus.eu/product/NWSHELF_MULTIYEAR_PHY_004_009/services) / `cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i`.
- Native evidence: hourly true eastward/northward sea-water velocity at approximately 7 km surface grid.
- Manual caveat: the NWS product documentation imposes a 10 m minimum model depth and warns against direct use in extensive <10 m bathymetry areas. Source: [CMEMS-NWS-PUM-004-009-011.pdf](https://catalogue.marine.copernicus.eu/documents/PUM/CMEMS-NWS-PUM-004-009-011.pdf).

## Eligibility Summary

- Farm-year rows evaluated: 468
- Recommended farm-years: 125
- Stress-test farm-years: 41
- Estimated rows if all recommended farm-years are approved: 76,886,304
- Estimated raw subset size if all recommended farm-years are approved: 4920.7 MB
- Estimated processed parquet size if all recommended farm-years are approved: 690.9 MB
- Size formula: rows = sample points x hourly timestamps; raw estimate = 64 bytes/row; processed estimate is calibrated from the accepted NWS current pilot parquet.

## Top 10 Recommended NWS Scale Farm-Years

| wind_farm | year | dwell_count | tier_a_dwell_count | wave_confidence_a_b_count | sample_point_count | median_water_depth_m | pct_sample_points_depth_le_10m | estimated_current_rows |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Horns Rev II | 2024 | 982 | 349 | 754 | 92 | 12.145 | 0.120 | 808128 |
| Horns Rev III | 2024 | 823 | 349 | 722 | 50 | 15.629 | 0.000 | 439200 |
| Butendiek | 2024 | 373 | 315 | 336 | 81 | 19.220 | 0.000 | 711504 |
| Dan Tysk | 2024 | 499 | 288 | 462 | 81 | 25.180 | 0.000 | 711504 |
| Meerwind Sued/Ost | 2024 | 720 | 282 | 669 | 81 | 23.990 | 0.000 | 711504 |
| Amrumbank West | 2024 | 622 | 276 | 583 | 81 | 21.300 | 0.000 | 711504 |
| Nordsee Ost | 2024 | 981 | 225 | 900 | 49 | 23.820 | 0.000 | 430416 |
| Gode Wind 1 and 2 | 2024 | 764 | 223 | 712 | 98 | 32.685 | 0.000 | 860832 |
| Sandbank | 2024 | 256 | 199 | 233 | 73 | 28.250 | 0.000 | 641232 |
| Vesterhav Syd | 2024 | 319 | 194 | 275 | 21 | 21.896 | 0.000 | 184464 |

## Top 5 Stress-Test Farm-Years

| wind_farm | year | dwell_count | tier_a_dwell_count | pct_sample_points_depth_le_10m | shallow_model_warning | recommendation_reason |
| --- | --- | --- | --- | --- | --- | --- |
| Horns Rev I | 2024 | 495 | 401 | 0.790 | dominated_by_depth_le_10m | inside NWS current domain and west of Baltic mask boundary; dwell_count >= 10; Tier A count >= 3; more than half of sample points are <=10 m; storage/runtime estimate acceptable |
| Lynn | 2012 | 200 | 58 | 0.750 | dominated_by_depth_le_10m | inside NWS current domain and west of Baltic mask boundary; dwell_count >= 10; Tier A count >= 3; more than half of sample points are <=10 m; storage/runtime estimate acceptable |
| Inner Dowsing | 2012 | 225 | 54 | 1.000 | severe_depth_le_10m_dominated | inside NWS current domain and west of Baltic mask boundary; dwell_count >= 10; Tier A count >= 3; more than half of sample points are <=10 m; storage/runtime estimate acceptable |
| Nordergründe | 2024 | 69 | 43 | 0.947 | severe_depth_le_10m_dominated | inside NWS current domain and west of Baltic mask boundary; dwell_count >= 10; Tier A count >= 3; more than half of sample points are <=10 m; storage/runtime estimate acceptable |
| Nissum Bredning | 2024 | 4184 | 34 | 1.000 | severe_depth_le_10m_dominated | inside NWS current domain and west of Baltic mask boundary; dwell_count >= 10; Tier A count >= 3; more than half of sample points are <=10 m; storage/runtime estimate acceptable |

## Recommendation

Proceed with a second NWS scaled extraction batch of the top 5-10 recommended farm-years before approving all recommended farm-years. Borkum Riffgrund 2 demonstrated the method, but the preflight shows enough candidate breadth that a controlled batch is the right next write.

Do not scale Baltic historical currents as event-scale evidence. Keep Baltic current evidence contextual unless a separate recent-period hourly Baltic pilot is intentionally approved.

## Guardrails

- No NWS current download or extraction was run in this preflight.
- No Baltic current download or extraction was run.
- No global fallback extraction was run.
- Legacy CMEMS current CSVs and simulated/fallback currents remain banned as research evidence.
