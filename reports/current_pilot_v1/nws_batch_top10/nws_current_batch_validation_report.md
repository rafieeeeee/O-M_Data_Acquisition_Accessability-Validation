# NWS Current Batch v1 Validation Report

## Executive Conclusion

This top-10 NWS batch is a `coverage-driven engineering batch`. It validates the archive/extraction machinery over multiple high-evidence farm-years, but because all selected rows are 2024, it should not be described as a representative historical sample.

- Farm-years selected: 10
- Farm-years validated: 10
- Farm-years failed: 0
- Final row count: 6,210,288
- Final partition count: 10
- Output root: `Data/Processed/metocean/nws_current_timeseries`
- Raw cache root: `Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots`

## Manifest Summary

| wind_farm | farm_id | year | status | row_count | sample_point_count | timestamp_start | timestamp_end | source_file | processed_path | qa_status | message |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Horns Rev II | Horns_Rev_II | 2024 | validated | 808128 | 92 | 2024-01-01 00:00:00+00:00 | 2024-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Horns_Rev_II/year=2024/nws_current_Horns_Rev_II_2024_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Horns_Rev_II/year=2024/part.parquet | passed | processed and validated |
| Horns Rev III | Horns_Rev_III | 2024 | validated | 439200 | 50 | 2024-01-01 00:00:00+00:00 | 2024-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Horns_Rev_III/year=2024/nws_current_Horns_Rev_III_2024_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Horns_Rev_III/year=2024/part.parquet | passed | processed and validated |
| Butendiek | Butendiek | 2024 | validated | 711504 | 81 | 2024-01-01 00:00:00+00:00 | 2024-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Butendiek/year=2024/nws_current_Butendiek_2024_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Butendiek/year=2024/part.parquet | passed | processed and validated |
| Dan Tysk | Dan_Tysk | 2024 | validated | 711504 | 81 | 2024-01-01 00:00:00+00:00 | 2024-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Dan_Tysk/year=2024/nws_current_Dan_Tysk_2024_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Dan_Tysk/year=2024/part.parquet | passed | processed and validated |
| Meerwind Sued/Ost | Meerwind_Sued_Ost | 2024 | validated | 711504 | 81 | 2024-01-01 00:00:00+00:00 | 2024-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Meerwind_Sued_Ost/year=2024/nws_current_Meerwind_Sued_Ost_2024_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Meerwind_Sued_Ost/year=2024/part.parquet | passed | processed and validated |
| Amrumbank West | Amrumbank_West | 2024 | validated | 711504 | 81 | 2024-01-01 00:00:00+00:00 | 2024-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Amrumbank_West/year=2024/nws_current_Amrumbank_West_2024_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Amrumbank_West/year=2024/part.parquet | passed | processed and validated |
| Nordsee Ost | Nordsee_Ost | 2024 | validated | 430416 | 49 | 2024-01-01 00:00:00+00:00 | 2024-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Nordsee_Ost/year=2024/nws_current_Nordsee_Ost_2024_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Nordsee_Ost/year=2024/part.parquet | passed | processed and validated |
| Gode Wind 1 and 2 | Gode_Wind_1_and_2 | 2024 | validated | 860832 | 98 | 2024-01-01 00:00:00+00:00 | 2024-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Gode_Wind_1_and_2/year=2024/nws_current_Gode_Wind_1_and_2_2024_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Gode_Wind_1_and_2/year=2024/part.parquet | passed | processed and validated |
| Sandbank | Sandbank | 2024 | validated | 641232 | 73 | 2024-01-01 00:00:00+00:00 | 2024-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Sandbank/year=2024/nws_current_Sandbank_2024_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Sandbank/year=2024/part.parquet | passed | processed and validated |
| Vesterhav Syd | Vesterhav_Syd | 2024 | validated | 184464 | 21 | 2024-01-01 00:00:00+00:00 | 2024-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Vesterhav_Syd/year=2024/nws_current_Vesterhav_Syd_2024_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Vesterhav_Syd/year=2024/part.parquet | passed | processed and validated |

## Per Farm-Year QA

| wind_farm | year | row_count | sample_point_count | timestamp_start | timestamp_end | median_cadence_minutes | valid_uv_count | duplicate_count | speed_consistency_max_error | direction_ok | provenance_complete | qa_status |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Horns Rev II | 2024 | 808128 | 92 | 2024-01-01 00:00:00+00:00 | 2024-12-31 23:00:00+00:00 | 60.000 | 808128 | 0 | 0.000 | True | True | passed |
| Horns Rev III | 2024 | 439200 | 50 | 2024-01-01 00:00:00+00:00 | 2024-12-31 23:00:00+00:00 | 60.000 | 439200 | 0 | 0.000 | True | True | passed |
| Butendiek | 2024 | 711504 | 81 | 2024-01-01 00:00:00+00:00 | 2024-12-31 23:00:00+00:00 | 60.000 | 711504 | 0 | 0.000 | True | True | passed |
| Dan Tysk | 2024 | 711504 | 81 | 2024-01-01 00:00:00+00:00 | 2024-12-31 23:00:00+00:00 | 60.000 | 711504 | 0 | 0.000 | True | True | passed |
| Meerwind Sued/Ost | 2024 | 711504 | 81 | 2024-01-01 00:00:00+00:00 | 2024-12-31 23:00:00+00:00 | 60.000 | 711504 | 0 | 0.000 | True | True | passed |
| Amrumbank West | 2024 | 711504 | 81 | 2024-01-01 00:00:00+00:00 | 2024-12-31 23:00:00+00:00 | 60.000 | 711504 | 0 | 0.000 | True | True | passed |
| Nordsee Ost | 2024 | 430416 | 49 | 2024-01-01 00:00:00+00:00 | 2024-12-31 23:00:00+00:00 | 60.000 | 430416 | 0 | 0.000 | True | True | passed |
| Gode Wind 1 and 2 | 2024 | 860832 | 98 | 2024-01-01 00:00:00+00:00 | 2024-12-31 23:00:00+00:00 | 60.000 | 860832 | 0 | 0.000 | True | True | passed |
| Sandbank | 2024 | 641232 | 73 | 2024-01-01 00:00:00+00:00 | 2024-12-31 23:00:00+00:00 | 60.000 | 641232 | 0 | 0.000 | True | True | passed |
| Vesterhav Syd | 2024 | 184464 | 21 | 2024-01-01 00:00:00+00:00 | 2024-12-31 23:00:00+00:00 | 60.000 | 184464 | 0 | 0.000 | True | True | passed |

## Event-Scale Suitability

| wind_farm | year | dwell_event_count | tier_a_dwell_count | events_with_bracketing_current_samples | events_with_window_samples | event_scale_suitable_pct | nearest_time_gap_minutes_p50 | nearest_time_gap_minutes_p95 | event_window_sample_count_p50 | event_window_sample_count_p95 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Horns Rev II | 2024 | 982 | 349 | 982 | 960 | 1.000 | 13.533 | 28.273 | 5.000 | 17.000 |
| Horns Rev III | 2024 | 823 | 349 | 823 | 795 | 1.000 | 13.033 | 28.157 | 4.000 | 41.900 |
| Butendiek | 2024 | 373 | 315 | 373 | 371 | 1.000 | 14.883 | 28.478 | 7.000 | 105.200 |
| Dan Tysk | 2024 | 499 | 288 | 499 | 487 | 1.000 | 15.367 | 28.334 | 7.000 | 60.300 |
| Meerwind Sued/Ost | 2024 | 720 | 282 | 720 | 704 | 1.000 | 16.737 | 28.517 | 4.000 | 17.150 |
| Amrumbank West | 2024 | 622 | 276 | 622 | 604 | 1.000 | 15.588 | 28.829 | 5.000 | 27.900 |
| Nordsee Ost | 2024 | 981 | 225 | 981 | 954 | 1.000 | 16.092 | 28.700 | 4.000 | 20.000 |
| Gode Wind 1 and 2 | 2024 | 764 | 223 | 764 | 742 | 1.000 | 15.692 | 28.643 | 4.000 | 26.000 |
| Sandbank | 2024 | 256 | 199 | 256 | 247 | 1.000 | 15.346 | 28.723 | 5.000 | 42.250 |
| Vesterhav Syd | 2024 | 319 | 194 | 319 | 314 | 1.000 | 14.742 | 28.634 | 7.000 | 29.300 |

## Current Variability

| wind_farm | year | current_speed_min | current_speed_mean | current_speed_p95 | current_speed_max | median_hourly_speed_delta | p95_hourly_speed_delta | median_hourly_direction_change_deg | p95_hourly_direction_change_deg | variability_flag |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Horns Rev II | 2024 | 0.002 | 0.308 | 0.541 | 0.726 | 0.114 | 0.210 | 13.719 | 115.234 | operationally_plausible_variability |
| Horns Rev III | 2024 | 0.001 | 0.264 | 0.469 | 0.668 | 0.089 | 0.180 | 13.274 | 112.470 | operationally_plausible_variability |
| Butendiek | 2024 | 0.007 | 0.284 | 0.449 | 0.644 | 0.069 | 0.174 | 20.670 | 85.517 | operationally_plausible_variability |
| Dan Tysk | 2024 | 0.001 | 0.253 | 0.432 | 0.607 | 0.086 | 0.177 | 10.436 | 124.660 | operationally_plausible_variability |
| Meerwind Sued/Ost | 2024 | 0.002 | 0.366 | 0.603 | 0.837 | 0.104 | 0.256 | 14.208 | 101.929 | operationally_plausible_variability |
| Amrumbank West | 2024 | 0.004 | 0.360 | 0.583 | 0.810 | 0.102 | 0.248 | 15.961 | 93.352 | operationally_plausible_variability |
| Nordsee Ost | 2024 | 0.002 | 0.362 | 0.595 | 0.837 | 0.105 | 0.256 | 14.204 | 101.677 | operationally_plausible_variability |
| Gode Wind 1 and 2 | 2024 | 0.003 | 0.389 | 0.679 | 1.031 | 0.142 | 0.297 | 7.298 | 135.687 | operationally_plausible_variability |
| Sandbank | 2024 | 0.001 | 0.222 | 0.383 | 0.553 | 0.076 | 0.148 | 9.497 | 116.159 | operationally_plausible_variability |
| Vesterhav Syd | 2024 | 0.000 | 0.200 | 0.422 | 0.789 | 0.068 | 0.147 | 4.346 | 149.036 | operationally_plausible_variability |

## Acceptance

The batch is acceptable only if each processed farm-year has true non-null `uo/vo`, no fallback/synthetic provenance, hourly UTC cadence, populated source/depth/direction provenance, no duplicate farm-year-sample-timestamp keys, and event-scale bracketing suitable for the selected dwell windows.
