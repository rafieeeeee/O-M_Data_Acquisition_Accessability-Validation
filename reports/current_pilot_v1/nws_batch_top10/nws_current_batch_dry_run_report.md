# NWS Current Batch v1 Dry-Run Report

## Research Design

This batch tests whether the NWS hourly true `uo/vo` pilot method can scale to multiple high-evidence farm-years before any all-125 extraction is approved. The selected top-10 rows are the accepted preflight's highest-ranked normal-scale candidates.

Batch interpretation: `coverage-driven engineering batch`. The selected top-10 are all 2024, so this batch is not a representative historical sample; it is a controlled archive/extractor validation batch.

## Pre-Run Checks

- Product: `NWSHELF_MULTIYEAR_PHY_004_009` / `cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i`
- Selected farm-years: 10
- Stress-test farm-years selected: 0
- Estimated current rows: 6,210,288
- Estimated processed size: 55.8 MB
- Output root free space: 1705744.6 MB
- Raw cache root: `Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots`
- Raw cache root guard: under `Data/Raw/Metocean` and separate from legacy CMEMS CSV cache
- Copernicus tooling import available: True

## Selected Farm-Years

| selected_rank | wind_farm | year | dwell_count | tier_a_dwell_count | sample_point_count | estimated_current_rows | processed_exists | raw_cache_exists |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | Horns Rev II | 2024 | 982 | 349 | 92 | 808128 | False | False |
| 2 | Horns Rev III | 2024 | 823 | 349 | 50 | 439200 | False | False |
| 3 | Butendiek | 2024 | 373 | 315 | 81 | 711504 | False | False |
| 4 | Dan Tysk | 2024 | 499 | 288 | 81 | 711504 | False | False |
| 5 | Meerwind Sued/Ost | 2024 | 720 | 282 | 81 | 711504 | False | False |
| 6 | Amrumbank West | 2024 | 622 | 276 | 81 | 711504 | False | False |
| 7 | Nordsee Ost | 2024 | 981 | 225 | 49 | 430416 | False | False |
| 8 | Gode Wind 1 and 2 | 2024 | 764 | 223 | 98 | 860832 | False | False |
| 9 | Sandbank | 2024 | 256 | 199 | 73 | 641232 | False | False |
| 10 | Vesterhav Syd | 2024 | 319 | 194 | 21 | 184464 | False | False |

## Guardrails

- Dry-run only; no current download or processed archive write was performed.
- No stress-test farms are selected.
- Baltic and global currents are out of scope.
- Legacy CMEMS current CSVs and fallback/synthetic currents remain banned.
