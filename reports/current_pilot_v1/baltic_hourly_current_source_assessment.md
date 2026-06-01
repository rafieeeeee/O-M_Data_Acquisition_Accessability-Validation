# Baltic Hourly Current Source Assessment

## Executive Decision

No accepted historical Baltic true Eulerian hourly `uo/vo` source was found for the 2010-2020 study window. Keep the Baltic reanalysis current evidence as `B_contextual` unless a separate historical hourly product is approved.

## Sources Checked

| Source | Product ID | Dataset ID | True `uo/vo` | Historical Coverage | Cadence | Decision |
| --- | --- | --- | --- | --- | --- | --- |
| Baltic physics reanalysis | [`BALTICSEA_MULTIYEAR_PHY_003_011`](https://data.marine.copernicus.eu/product/BALTICSEA_MULTIYEAR_PHY_003_011/services) | `cmems_mod_bal_phy_my_P1D-m` | yes | 1993-2024 | daily/monthly/yearly | Accepted only as contextual evidence for historical dwell events. |
| Baltic physics analysis/forecast | [`BALTICSEA_ANALYSISFORECAST_PHY_003_006`](https://data.marine.copernicus.eu/product/BALTICSEA_ANALYSISFORECAST_PHY_003_006/services) | `cmems_mod_bal_phy_anfc_PT1H-i` / `cmems_mod_bal_phy_anfc_PT15M-i` | yes | late 2022 onward | hourly and 15-minute | Not suitable for 2010-2020 historical evidence; possible later-period pilot only. |
| Global physics reanalysis | [`GLOBAL_MULTIYEAR_PHY_001_030`](https://data.marine.copernicus.eu/product/GLOBAL_MULTIYEAR_PHY_001_030/services) | `cmems_mod_glo_phy_my_0.083deg_P1D-m` | yes | 1993-2026 | daily/monthly | Fallback assessment only; not event-scale and not downloaded. |
| Baltic wave archive | `BALTICSEA_MULTIYEAR_WAV_003_015` | `cmems_mod_bal_wav_my_PT1H-i` | no | historical wave hindcast | hourly | `VSDX/VSDY` are Stokes drift, not Eulerian currents. |

## Conclusion

- Historical Baltic reanalysis currents remain `B_contextual` because the accepted multiyear physics dataset is daily.
- The recent Baltic analysis/forecast product has true current datasets at sub-hourly/hourly cadence, but its coverage starts too late for the historical 2010-2020 thesis window.
- Do not force Baltic daily currents into event-scale models.
- Do not use Baltic wave `VSDX/VSDY` as current evidence.

## Next Action

If 2023-2024 Baltic current modelling becomes relevant, run a new one-farm/year pilot against `BALTICSEA_ANALYSISFORECAST_PHY_003_006` / `cmems_mod_bal_phy_anfc_PT1H-i`.
