# Current Pilot v1 Product Assessment

## Executive Assessment

Current products are piloted before broad download because final workability modelling needs true Eulerian `uo`/`vo` with known cadence, depth, spatial match, provenance, and event-window suitability. Baltic wave `VSDX`/`VSDY` Stokes drift and legacy CMEMS fallback CSVs are explicitly excluded.

## Candidate Current Sources

| Source | Product | Product ID | Dataset ID | Variables | Cadence | Coverage | Space/depth | Access | Event-scale suitability |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Baltic | [Copernicus Baltic Sea Physics Reanalysis](https://data.marine.copernicus.eu/product/BALTICSEA_MULTIYEAR_PHY_003_011/services) | `BALTICSEA_MULTIYEAR_PHY_003_011` | `cmems_mod_bal_phy_my_P1D-m` | uo, vo | 1440 min | 1993-2024 daily/monthly/yearly reanalysis datasets | 2 km; 3D physics grid with 56 depth levels; pilot uses nearest-surface uo/vo only | Copernicus Marine Toolbox subset/open_dataset | Contextual before validation: true u/v is available, but the approved multi-year reanalysis current dataset is daily, not event-hourly. |
| NWS | [Copernicus Atlantic-European North West Shelf Ocean Physics Reanalysis](https://data.marine.copernicus.eu/product/NWSHELF_MULTIYEAR_PHY_004_009/services) | `NWSHELF_MULTIYEAR_PHY_004_009` | `cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i` | uo, vo | 60 min | 1993-2026 hourly 2D surface currents in the current dataset | 7 km; 2D surface current product for uo/vo; no depth dimension expected | Local annual NetCDF if mounted, otherwise Copernicus Marine Toolbox scoped subset | Potentially event-scale where the farm lies in product coverage and hourly samples bracket dwell windows. |
| Global fallback | [Copernicus Global Ocean Physics Reanalysis](https://data.marine.copernicus.eu/product/GLOBAL_MULTIYEAR_PHY_001_030/services) | `GLOBAL_MULTIYEAR_PHY_001_030` | `cmems_mod_glo_phy_my_0.083deg_P1D-m` | uo, vo | Daily/monthly, depending on dataset part | 1993-2026 reanalysis family | 1/12 degree global grid; 3D, 50 depth levels in the product family | Copernicus Marine Toolbox | Fallback assessment only. Daily/coarser regional fit means it should not be downloaded before Baltic/NWS regional gaps are proven. |

## Access And Storage Notes

- Baltic tooling: copernicusmarine credentials plus xarray/netCDF support. Estimated storage: One-farm/year nearest-surface subset is small; candidate parquet usually tens of thousands of rows.
- NWS tooling: local raw NetCDF or copernicusmarine credentials plus xarray/netCDF support. Estimated storage: One-farm/year nearest-surface subset is moderate; hourly candidates may be hundreds of thousands of rows.
- Global fallback is assessment-only in this increment and must not be downloaded without later approval.

## Acceptance Gates

- True `uo/current_u` and `vo/current_v` must be present.
- `current_speed` must equal `sqrt(u^2 + v^2)` within numerical tolerance.
- Direction is flow-to degrees clockwise from true north: `degrees(atan2(u, v)) % 360`.
- Timestamps must be UTC-normalized and cadence must be reported.
- Depth level, product ID, dataset ID, source file, and extraction method must be populated.
- No fallback, simulated, or legacy current rows may carry current values.
