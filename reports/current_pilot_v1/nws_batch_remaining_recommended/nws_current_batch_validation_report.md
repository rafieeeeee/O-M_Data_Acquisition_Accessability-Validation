# NWS Current Batch v1 Validation Report

## Executive Conclusion

This NWS current batch is a `mixed-year scale batch` over `all_normal_recommended`. It preserves existing accepted partitions, processes missing normal recommended farm-years, and keeps stress-test farm-years out of the source-specific archive.

- Farm-years selected: 125
- Farm-years processed this run: 115
- Farm-years skipped existing and revalidated: 10
- Farm-years accepted in archive: 125
- Farm-years failed: 0
- Final row count: 76,886,304
- Final partition count: 125
- Output root: `Data/Processed/metocean/nws_current_timeseries`
- Raw cache root: `Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots`

## Manifest Summary

| wind_farm | farm_id | year | status | row_count | sample_point_count | timestamp_start | timestamp_end | source_file | processed_path | qa_status | message |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Horns Rev II | Horns_Rev_II | 2024 | skipped_existing | 808128 | 92 | 2024-01-01 00:00:00+00:00 | 2024-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Horns_Rev_II/year=2024/nws_current_Horns_Rev_II_2024_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Horns_Rev_II/year=2024/part.parquet | passed | existing partition validated; overwrite is false |
| Horns Rev III | Horns_Rev_III | 2024 | skipped_existing | 439200 | 50 | 2024-01-01 00:00:00+00:00 | 2024-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Horns_Rev_III/year=2024/nws_current_Horns_Rev_III_2024_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Horns_Rev_III/year=2024/part.parquet | passed | existing partition validated; overwrite is false |
| Butendiek | Butendiek | 2024 | skipped_existing | 711504 | 81 | 2024-01-01 00:00:00+00:00 | 2024-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Butendiek/year=2024/nws_current_Butendiek_2024_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Butendiek/year=2024/part.parquet | passed | existing partition validated; overwrite is false |
| Dan Tysk | Dan_Tysk | 2024 | skipped_existing | 711504 | 81 | 2024-01-01 00:00:00+00:00 | 2024-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Dan_Tysk/year=2024/nws_current_Dan_Tysk_2024_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Dan_Tysk/year=2024/part.parquet | passed | existing partition validated; overwrite is false |
| Meerwind Sued/Ost | Meerwind_Sued_Ost | 2024 | skipped_existing | 711504 | 81 | 2024-01-01 00:00:00+00:00 | 2024-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Meerwind_Sued_Ost/year=2024/nws_current_Meerwind_Sued_Ost_2024_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Meerwind_Sued_Ost/year=2024/part.parquet | passed | existing partition validated; overwrite is false |
| Amrumbank West | Amrumbank_West | 2024 | skipped_existing | 711504 | 81 | 2024-01-01 00:00:00+00:00 | 2024-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Amrumbank_West/year=2024/nws_current_Amrumbank_West_2024_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Amrumbank_West/year=2024/part.parquet | passed | existing partition validated; overwrite is false |
| Nordsee Ost | Nordsee_Ost | 2024 | skipped_existing | 430416 | 49 | 2024-01-01 00:00:00+00:00 | 2024-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Nordsee_Ost/year=2024/nws_current_Nordsee_Ost_2024_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Nordsee_Ost/year=2024/part.parquet | passed | existing partition validated; overwrite is false |
| Gode Wind 1 and 2 | Gode_Wind_1_and_2 | 2024 | skipped_existing | 860832 | 98 | 2024-01-01 00:00:00+00:00 | 2024-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Gode_Wind_1_and_2/year=2024/nws_current_Gode_Wind_1_and_2_2024_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Gode_Wind_1_and_2/year=2024/part.parquet | passed | existing partition validated; overwrite is false |
| Sandbank | Sandbank | 2024 | skipped_existing | 641232 | 73 | 2024-01-01 00:00:00+00:00 | 2024-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Sandbank/year=2024/nws_current_Sandbank_2024_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Sandbank/year=2024/part.parquet | passed | existing partition validated; overwrite is false |
| Vesterhav Syd | Vesterhav_Syd | 2024 | skipped_existing | 184464 | 21 | 2024-01-01 00:00:00+00:00 | 2024-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Vesterhav_Syd/year=2024/nws_current_Vesterhav_Syd_2024_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Vesterhav_Syd/year=2024/part.parquet | passed | existing partition validated; overwrite is false |
| Merkur Offshore | Merkur_Offshore | 2024 | validated | 588528 | 67 | 2024-01-01 00:00:00+00:00 | 2024-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Merkur_Offshore/year=2024/nws_current_Merkur_Offshore_2024_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Merkur_Offshore/year=2024/part.parquet | passed | processed and validated |
| Vesterhav Nord | Vesterhav_Nord | 2024 | validated | 193248 | 22 | 2024-01-01 00:00:00+00:00 | 2024-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Vesterhav_Nord/year=2024/nws_current_Vesterhav_Nord_2024_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Vesterhav_Nord/year=2024/part.parquet | passed | processed and validated |
| Kaskasi | Kaskasi | 2024 | validated | 342576 | 39 | 2024-01-01 00:00:00+00:00 | 2024-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Kaskasi/year=2024/nws_current_Kaskasi_2024_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Kaskasi/year=2024/part.parquet | passed | processed and validated |
| Borkum Riffgrund 1 | Borkum_Riffgrund_1 | 2024 | validated | 693936 | 79 | 2024-01-01 00:00:00+00:00 | 2024-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Borkum_Riffgrund_1/year=2024/nws_current_Borkum_Riffgrund_1_2024_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Borkum_Riffgrund_1/year=2024/part.parquet | passed | processed and validated |
| Trianel Windpark Borkum 2 | Trianel_Windpark_Borkum_2 | 2024 | validated | 289872 | 33 | 2024-01-01 00:00:00+00:00 | 2024-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Trianel_Windpark_Borkum_2/year=2024/nws_current_Trianel_Windpark_Borkum_2_2024_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Trianel_Windpark_Borkum_2/year=2024/part.parquet | passed | processed and validated |
| Nordsee One | Nordsee_One | 2024 | validated | 483120 | 55 | 2024-01-01 00:00:00+00:00 | 2024-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Nordsee_One/year=2024/nws_current_Nordsee_One_2024_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Nordsee_One/year=2024/part.parquet | passed | processed and validated |
| Global Tech I | Global_Tech_I | 2024 | validated | 711504 | 81 | 2024-01-01 00:00:00+00:00 | 2024-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Global_Tech_I/year=2024/nws_current_Global_Tech_I_2024_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Global_Tech_I/year=2024/part.parquet | passed | processed and validated |
| Borkum Riffgrund 2 | Borkum_Riffgrund_2 | 2024 | validated | 500688 | 57 | 2024-01-01 00:00:00+00:00 | 2024-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Borkum_Riffgrund_2/year=2024/nws_current_Borkum_Riffgrund_2_2024_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Borkum_Riffgrund_2/year=2024/part.parquet | passed | processed and validated |
| Trianel Windpark Borkum 1 | Trianel_Windpark_Borkum_1 | 2024 | validated | 360144 | 41 | 2024-01-01 00:00:00+00:00 | 2024-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Trianel_Windpark_Borkum_1/year=2024/nws_current_Trianel_Windpark_Borkum_1_2024_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Trianel_Windpark_Borkum_1/year=2024/part.parquet | passed | processed and validated |
| EnBW Hohe See | EnBW_Hohe_See | 2024 | validated | 632448 | 72 | 2024-01-01 00:00:00+00:00 | 2024-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=EnBW_Hohe_See/year=2024/nws_current_EnBW_Hohe_See_2024_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=EnBW_Hohe_See/year=2024/part.parquet | passed | processed and validated |
| Bard Offshore 1 | Bard_Offshore_1 | 2024 | validated | 711504 | 81 | 2024-01-01 00:00:00+00:00 | 2024-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Bard_Offshore_1/year=2024/nws_current_Bard_Offshore_1_2024_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Bard_Offshore_1/year=2024/part.parquet | passed | processed and validated |
| Horns Rev III | Horns_Rev_III | 2019 | validated | 438000 | 50 | 2019-01-01 00:00:00+00:00 | 2019-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Horns_Rev_III/year=2019/nws_current_Horns_Rev_III_2019_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Horns_Rev_III/year=2019/part.parquet | passed | processed and validated |
| Riffgat | Riffgat | 2024 | validated | 272304 | 31 | 2024-01-01 00:00:00+00:00 | 2024-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Riffgat/year=2024/nws_current_Riffgat_2024_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Riffgat/year=2024/part.parquet | passed | processed and validated |
| Albatros | Albatros | 2024 | validated | 149328 | 17 | 2024-01-01 00:00:00+00:00 | 2024-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Albatros/year=2024/nws_current_Albatros_2024_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Albatros/year=2024/part.parquet | passed | processed and validated |
| Alpha Ventus | Alpha_Ventus | 2024 | validated | 114192 | 13 | 2024-01-01 00:00:00+00:00 | 2024-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Alpha_Ventus/year=2024/nws_current_Alpha_Ventus_2024_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Alpha_Ventus/year=2024/part.parquet | passed | processed and validated |
| Veja Mate | Veja_Mate | 2024 | validated | 597312 | 68 | 2024-01-01 00:00:00+00:00 | 2024-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Veja_Mate/year=2024/nws_current_Veja_Mate_2024_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Veja_Mate/year=2024/part.parquet | passed | processed and validated |
| Deutsche Bucht | Deutsche_Bucht | 2024 | validated | 281088 | 32 | 2024-01-01 00:00:00+00:00 | 2024-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Deutsche_Bucht/year=2024/nws_current_Deutsche_Bucht_2024_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Deutsche_Bucht/year=2024/part.parquet | passed | processed and validated |
| Butendiek | Butendiek | 2017 | validated | 709560 | 81 | 2017-01-01 00:00:00+00:00 | 2017-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Butendiek/year=2017/nws_current_Butendiek_2017_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Butendiek/year=2017/part.parquet | passed | processed and validated |
| Walney 2 | Walney_2 | 2012 | validated | 456768 | 52 | 2012-01-01 00:00:00+00:00 | 2012-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Walney_2/year=2012/nws_current_Walney_2_2012_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Walney_2/year=2012/part.parquet | passed | processed and validated |
| Gemini | Gemini | 2024 | validated | 1326384 | 151 | 2024-01-01 00:00:00+00:00 | 2024-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Gemini/year=2024/nws_current_Gemini_2024_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Gemini/year=2024/part.parquet | passed | processed and validated |
| Dan Tysk | Dan_Tysk | 2021 | validated | 709560 | 81 | 2021-01-01 00:00:00+00:00 | 2021-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Dan_Tysk/year=2021/nws_current_Dan_Tysk_2021_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Dan_Tysk/year=2021/part.parquet | passed | processed and validated |
| Thanet | Thanet | 2012 | validated | 887184 | 101 | 2012-01-01 00:00:00+00:00 | 2012-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Thanet/year=2012/nws_current_Thanet_2012_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Thanet/year=2012/part.parquet | passed | processed and validated |
| Horns Rev II | Horns_Rev_II | 2011 | validated | 805920 | 92 | 2011-01-01 00:00:00+00:00 | 2011-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Horns_Rev_II/year=2011/nws_current_Horns_Rev_II_2011_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Horns_Rev_II/year=2011/part.parquet | passed | processed and validated |
| Ormonde | Ormonde | 2012 | validated | 272304 | 31 | 2012-01-01 00:00:00+00:00 | 2012-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Ormonde/year=2012/nws_current_Ormonde_2012_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Ormonde/year=2012/part.parquet | passed | processed and validated |
| Sandbank | Sandbank | 2017 | validated | 639480 | 73 | 2017-01-01 00:00:00+00:00 | 2017-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Sandbank/year=2017/nws_current_Sandbank_2017_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Sandbank/year=2017/part.parquet | passed | processed and validated |
| Butendiek | Butendiek | 2018 | validated | 709560 | 81 | 2018-01-01 00:00:00+00:00 | 2018-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Butendiek/year=2018/nws_current_Butendiek_2018_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Butendiek/year=2018/part.parquet | passed | processed and validated |
| Thornton Bank - phase I | Thornton_Bank_phase_I | 2012 | validated | 61488 | 7 | 2012-01-01 00:00:00+00:00 | 2012-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Thornton_Bank_phase_I/year=2012/nws_current_Thornton_Bank_phase_I_2012_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Thornton_Bank_phase_I/year=2012/part.parquet | passed | processed and validated |
| Horns Rev III | Horns_Rev_III | 2022 | validated | 438000 | 50 | 2022-01-01 00:00:00+00:00 | 2022-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Horns_Rev_III/year=2022/nws_current_Horns_Rev_III_2022_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Horns_Rev_III/year=2022/part.parquet | passed | processed and validated |
| Butendiek | Butendiek | 2020 | validated | 711504 | 81 | 2020-01-01 00:00:00+00:00 | 2020-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Butendiek/year=2020/nws_current_Butendiek_2020_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Butendiek/year=2020/part.parquet | passed | processed and validated |
| Walney 1 | Walney_1 | 2012 | validated | 456768 | 52 | 2012-01-01 00:00:00+00:00 | 2012-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Walney_1/year=2012/nws_current_Walney_1_2012_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Walney_1/year=2012/part.parquet | passed | processed and validated |
| Meerwind Sued/Ost | Meerwind_Sued_Ost | 2021 | validated | 709560 | 81 | 2021-01-01 00:00:00+00:00 | 2021-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Meerwind_Sued_Ost/year=2021/nws_current_Meerwind_Sued_Ost_2021_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Meerwind_Sued_Ost/year=2021/part.parquet | passed | processed and validated |
| Butendiek | Butendiek | 2019 | validated | 709560 | 81 | 2019-01-01 00:00:00+00:00 | 2019-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Butendiek/year=2019/nws_current_Butendiek_2019_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Butendiek/year=2019/part.parquet | passed | processed and validated |
| Dan Tysk | Dan_Tysk | 2017 | validated | 709560 | 81 | 2017-01-01 00:00:00+00:00 | 2017-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Dan_Tysk/year=2017/nws_current_Dan_Tysk_2017_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Dan_Tysk/year=2017/part.parquet | passed | processed and validated |
| Horns Rev II | Horns_Rev_II | 2010 | validated | 805920 | 92 | 2010-01-01 00:00:00+00:00 | 2010-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Horns_Rev_II/year=2010/nws_current_Horns_Rev_II_2010_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Horns_Rev_II/year=2010/part.parquet | passed | processed and validated |
| Amrumbank West | Amrumbank_West | 2022 | validated | 709560 | 81 | 2022-01-01 00:00:00+00:00 | 2022-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Amrumbank_West/year=2022/nws_current_Amrumbank_West_2022_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Amrumbank_West/year=2022/part.parquet | passed | processed and validated |
| Horns Rev II | Horns_Rev_II | 2019 | validated | 805920 | 92 | 2019-01-01 00:00:00+00:00 | 2019-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Horns_Rev_II/year=2019/nws_current_Horns_Rev_II_2019_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Horns_Rev_II/year=2019/part.parquet | passed | processed and validated |
| EnBW Hohe See | EnBW_Hohe_See | 2019 | validated | 630720 | 72 | 2019-01-01 00:00:00+00:00 | 2019-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=EnBW_Hohe_See/year=2019/nws_current_EnBW_Hohe_See_2019_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=EnBW_Hohe_See/year=2019/part.parquet | passed | processed and validated |
| Dan Tysk | Dan_Tysk | 2019 | validated | 709560 | 81 | 2019-01-01 00:00:00+00:00 | 2019-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Dan_Tysk/year=2019/nws_current_Dan_Tysk_2019_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Dan_Tysk/year=2019/part.parquet | passed | processed and validated |
| Dan Tysk | Dan_Tysk | 2018 | validated | 709560 | 81 | 2018-01-01 00:00:00+00:00 | 2018-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Dan_Tysk/year=2018/nws_current_Dan_Tysk_2018_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Dan_Tysk/year=2018/part.parquet | passed | processed and validated |
| Horns Rev II | Horns_Rev_II | 2012 | validated | 808128 | 92 | 2012-01-01 00:00:00+00:00 | 2012-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Horns_Rev_II/year=2012/nws_current_Horns_Rev_II_2012_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Horns_Rev_II/year=2012/part.parquet | passed | processed and validated |
| Hornsea Project 1 | Hornsea_Project_1 | 2024 | validated | 1537200 | 175 | 2024-01-01 00:00:00+00:00 | 2024-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Hornsea_Project_1/year=2024/nws_current_Hornsea_Project_1_2024_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Hornsea_Project_1/year=2024/part.parquet | passed | processed and validated |
| Horns Rev II | Horns_Rev_II | 2022 | validated | 805920 | 92 | 2022-01-01 00:00:00+00:00 | 2022-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Horns_Rev_II/year=2022/nws_current_Horns_Rev_II_2022_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Horns_Rev_II/year=2022/part.parquet | passed | processed and validated |
| Horns Rev II | Horns_Rev_II | 2017 | validated | 805920 | 92 | 2017-01-01 00:00:00+00:00 | 2017-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Horns_Rev_II/year=2017/nws_current_Horns_Rev_II_2017_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Horns_Rev_II/year=2017/part.parquet | passed | processed and validated |
| Borkum Riffgrund 2 | Borkum_Riffgrund_2 | 2018 | validated | 499320 | 57 | 2018-01-01 00:00:00+00:00 | 2018-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Borkum_Riffgrund_2/year=2018/nws_current_Borkum_Riffgrund_2_2018_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Borkum_Riffgrund_2/year=2018/part.parquet | passed | processed and validated |
| Dan Tysk | Dan_Tysk | 2020 | validated | 711504 | 81 | 2020-01-01 00:00:00+00:00 | 2020-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Dan_Tysk/year=2020/nws_current_Dan_Tysk_2020_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Dan_Tysk/year=2020/part.parquet | passed | processed and validated |
| Butendiek | Butendiek | 2021 | validated | 709560 | 81 | 2021-01-01 00:00:00+00:00 | 2021-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Butendiek/year=2021/nws_current_Butendiek_2021_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Butendiek/year=2021/part.parquet | passed | processed and validated |
| Butendiek | Butendiek | 2022 | validated | 709560 | 81 | 2022-01-01 00:00:00+00:00 | 2022-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Butendiek/year=2022/nws_current_Butendiek_2022_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Butendiek/year=2022/part.parquet | passed | processed and validated |
| Horns Rev II | Horns_Rev_II | 2018 | validated | 805920 | 92 | 2018-01-01 00:00:00+00:00 | 2018-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Horns_Rev_II/year=2018/nws_current_Horns_Rev_II_2018_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Horns_Rev_II/year=2018/part.parquet | passed | processed and validated |
| Nordsee Ost | Nordsee_Ost | 2021 | validated | 429240 | 49 | 2021-01-01 00:00:00+00:00 | 2021-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Nordsee_Ost/year=2021/nws_current_Nordsee_Ost_2021_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Nordsee_Ost/year=2021/part.parquet | passed | processed and validated |
| Meerwind Sued/Ost | Meerwind_Sued_Ost | 2022 | validated | 709560 | 81 | 2022-01-01 00:00:00+00:00 | 2022-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Meerwind_Sued_Ost/year=2022/nws_current_Meerwind_Sued_Ost_2022_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Meerwind_Sued_Ost/year=2022/part.parquet | passed | processed and validated |
| Dan Tysk | Dan_Tysk | 2022 | validated | 709560 | 81 | 2022-01-01 00:00:00+00:00 | 2022-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Dan_Tysk/year=2022/nws_current_Dan_Tysk_2022_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Dan_Tysk/year=2022/part.parquet | passed | processed and validated |
| Horns Rev III | Horns_Rev_III | 2020 | validated | 439200 | 50 | 2020-01-01 00:00:00+00:00 | 2020-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Horns_Rev_III/year=2020/nws_current_Horns_Rev_III_2020_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Horns_Rev_III/year=2020/part.parquet | passed | processed and validated |
| Butendiek | Butendiek | 2023 | validated | 709560 | 81 | 2023-01-01 00:00:00+00:00 | 2023-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Butendiek/year=2023/nws_current_Butendiek_2023_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Butendiek/year=2023/part.parquet | passed | processed and validated |
| Hornsea Project 2 | Hornsea_Project_2 | 2024 | validated | 1458144 | 166 | 2024-01-01 00:00:00+00:00 | 2024-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Hornsea_Project_2/year=2024/nws_current_Hornsea_Project_2_2024_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Hornsea_Project_2/year=2024/part.parquet | passed | processed and validated |
| Global Tech I | Global_Tech_I | 2019 | validated | 709560 | 81 | 2019-01-01 00:00:00+00:00 | 2019-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Global_Tech_I/year=2019/nws_current_Global_Tech_I_2019_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Global_Tech_I/year=2019/part.parquet | passed | processed and validated |
| Meerwind Sued/Ost | Meerwind_Sued_Ost | 2023 | validated | 709560 | 81 | 2023-01-01 00:00:00+00:00 | 2023-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Meerwind_Sued_Ost/year=2023/nws_current_Meerwind_Sued_Ost_2023_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Meerwind_Sued_Ost/year=2023/part.parquet | passed | processed and validated |
| Horns Rev III | Horns_Rev_III | 2021 | validated | 438000 | 50 | 2021-01-01 00:00:00+00:00 | 2021-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Horns_Rev_III/year=2021/nws_current_Horns_Rev_III_2021_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Horns_Rev_III/year=2021/part.parquet | passed | processed and validated |
| Horns Rev II | Horns_Rev_II | 2021 | validated | 805920 | 92 | 2021-01-01 00:00:00+00:00 | 2021-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Horns_Rev_II/year=2021/nws_current_Horns_Rev_II_2021_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Horns_Rev_II/year=2021/part.parquet | passed | processed and validated |
| Sandbank | Sandbank | 2022 | validated | 639480 | 73 | 2022-01-01 00:00:00+00:00 | 2022-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Sandbank/year=2022/nws_current_Sandbank_2022_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Sandbank/year=2022/part.parquet | passed | processed and validated |
| Nordsee Ost | Nordsee_Ost | 2023 | validated | 429240 | 49 | 2023-01-01 00:00:00+00:00 | 2023-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Nordsee_Ost/year=2023/nws_current_Nordsee_Ost_2023_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Nordsee_Ost/year=2023/part.parquet | passed | processed and validated |
| OWF Prinses Amalia | OWF_Prinses_Amalia | 2012 | validated | 535824 | 61 | 2012-01-01 00:00:00+00:00 | 2012-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=OWF_Prinses_Amalia/year=2012/nws_current_OWF_Prinses_Amalia_2012_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=OWF_Prinses_Amalia/year=2012/part.parquet | passed | processed and validated |
| Horns Rev II | Horns_Rev_II | 2020 | validated | 808128 | 92 | 2020-01-01 00:00:00+00:00 | 2020-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Horns_Rev_II/year=2020/nws_current_Horns_Rev_II_2020_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Horns_Rev_II/year=2020/part.parquet | passed | processed and validated |
| Nordsee Ost | Nordsee_Ost | 2022 | validated | 429240 | 49 | 2022-01-01 00:00:00+00:00 | 2022-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Nordsee_Ost/year=2022/nws_current_Nordsee_Ost_2022_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Nordsee_Ost/year=2022/part.parquet | passed | processed and validated |
| Hollandse Kust Zuid | Hollandse_Kust_Zuid | 2024 | validated | 1238544 | 141 | 2024-01-01 00:00:00+00:00 | 2024-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Hollandse_Kust_Zuid/year=2024/nws_current_Hollandse_Kust_Zuid_2024_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Hollandse_Kust_Zuid/year=2024/part.parquet | passed | processed and validated |
| Global Tech I | Global_Tech_I | 2018 | validated | 709560 | 81 | 2018-01-01 00:00:00+00:00 | 2018-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Global_Tech_I/year=2018/nws_current_Global_Tech_I_2018_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Global_Tech_I/year=2018/part.parquet | passed | processed and validated |
| Amrumbank West | Amrumbank_West | 2021 | validated | 709560 | 81 | 2021-01-01 00:00:00+00:00 | 2021-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Amrumbank_West/year=2021/nws_current_Amrumbank_West_2021_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Amrumbank_West/year=2021/part.parquet | passed | processed and validated |
| Horns Rev III | Horns_Rev_III | 2023 | validated | 438000 | 50 | 2023-01-01 00:00:00+00:00 | 2023-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Horns_Rev_III/year=2023/nws_current_Horns_Rev_III_2023_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Horns_Rev_III/year=2023/part.parquet | passed | processed and validated |
| Sandbank | Sandbank | 2021 | validated | 639480 | 73 | 2021-01-01 00:00:00+00:00 | 2021-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Sandbank/year=2021/nws_current_Sandbank_2021_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Sandbank/year=2021/part.parquet | passed | processed and validated |
| Horns Rev II | Horns_Rev_II | 2023 | validated | 805920 | 92 | 2023-01-01 00:00:00+00:00 | 2023-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Horns_Rev_II/year=2023/nws_current_Horns_Rev_II_2023_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Horns_Rev_II/year=2023/part.parquet | passed | processed and validated |
| Sandbank | Sandbank | 2018 | validated | 639480 | 73 | 2018-01-01 00:00:00+00:00 | 2018-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Sandbank/year=2018/nws_current_Sandbank_2018_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Sandbank/year=2018/part.parquet | passed | processed and validated |
| Global Tech I | Global_Tech_I | 2017 | validated | 709560 | 81 | 2017-01-01 00:00:00+00:00 | 2017-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Global_Tech_I/year=2017/nws_current_Global_Tech_I_2017_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Global_Tech_I/year=2017/part.parquet | passed | processed and validated |
| Sandbank | Sandbank | 2023 | validated | 639480 | 73 | 2023-01-01 00:00:00+00:00 | 2023-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Sandbank/year=2023/nws_current_Sandbank_2023_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Sandbank/year=2023/part.parquet | passed | processed and validated |
| Amrumbank West | Amrumbank_West | 2018 | validated | 709560 | 81 | 2018-01-01 00:00:00+00:00 | 2018-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Amrumbank_West/year=2018/nws_current_Amrumbank_West_2018_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Amrumbank_West/year=2018/part.parquet | passed | processed and validated |
| Borkum Riffgrund 2 | Borkum_Riffgrund_2 | 2021 | validated | 499320 | 57 | 2021-01-01 00:00:00+00:00 | 2021-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Borkum_Riffgrund_2/year=2021/nws_current_Borkum_Riffgrund_2_2021_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Borkum_Riffgrund_2/year=2021/part.parquet | passed | processed and validated |
| Merkur Offshore | Merkur_Offshore | 2023 | validated | 586920 | 67 | 2023-01-01 00:00:00+00:00 | 2023-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Merkur_Offshore/year=2023/nws_current_Merkur_Offshore_2023_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Merkur_Offshore/year=2023/part.parquet | passed | processed and validated |
| Triton Knoll | Triton_Knoll | 2024 | validated | 799344 | 91 | 2024-01-01 00:00:00+00:00 | 2024-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Triton_Knoll/year=2024/nws_current_Triton_Knoll_2024_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Triton_Knoll/year=2024/part.parquet | passed | processed and validated |
| Merkur Offshore | Merkur_Offshore | 2021 | validated | 586920 | 67 | 2021-01-01 00:00:00+00:00 | 2021-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Merkur_Offshore/year=2021/nws_current_Merkur_Offshore_2021_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Merkur_Offshore/year=2021/part.parquet | passed | processed and validated |
| Merkur Offshore | Merkur_Offshore | 2022 | validated | 586920 | 67 | 2022-01-01 00:00:00+00:00 | 2022-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Merkur_Offshore/year=2022/nws_current_Merkur_Offshore_2022_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Merkur_Offshore/year=2022/part.parquet | passed | processed and validated |
| Dan Tysk | Dan_Tysk | 2023 | validated | 709560 | 81 | 2023-01-01 00:00:00+00:00 | 2023-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Dan_Tysk/year=2023/nws_current_Dan_Tysk_2023_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Dan_Tysk/year=2023/part.parquet | passed | processed and validated |
| OWF Egmond aan Zee | OWF_Egmond_aan_Zee | 2012 | validated | 325008 | 37 | 2012-01-01 00:00:00+00:00 | 2012-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=OWF_Egmond_aan_Zee/year=2012/nws_current_OWF_Egmond_aan_Zee_2012_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=OWF_Egmond_aan_Zee/year=2012/part.parquet | passed | processed and validated |
| Nordsee One | Nordsee_One | 2022 | validated | 481800 | 55 | 2022-01-01 00:00:00+00:00 | 2022-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Nordsee_One/year=2022/nws_current_Nordsee_One_2022_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Nordsee_One/year=2022/part.parquet | passed | processed and validated |
| Nordsee One | Nordsee_One | 2017 | validated | 481800 | 55 | 2017-01-01 00:00:00+00:00 | 2017-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Nordsee_One/year=2017/nws_current_Nordsee_One_2017_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Nordsee_One/year=2017/part.parquet | passed | processed and validated |
| Sandbank | Sandbank | 2019 | validated | 639480 | 73 | 2019-01-01 00:00:00+00:00 | 2019-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Sandbank/year=2019/nws_current_Sandbank_2019_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Sandbank/year=2019/part.parquet | passed | processed and validated |
| Greater Gabbard | Greater_Gabbard | 2017 | validated | 1235160 | 141 | 2017-01-01 00:00:00+00:00 | 2017-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Greater_Gabbard/year=2017/nws_current_Greater_Gabbard_2017_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Greater_Gabbard/year=2017/part.parquet | passed | processed and validated |
| Meerwind Sued/Ost | Meerwind_Sued_Ost | 2018 | validated | 709560 | 81 | 2018-01-01 00:00:00+00:00 | 2018-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Meerwind_Sued_Ost/year=2018/nws_current_Meerwind_Sued_Ost_2018_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Meerwind_Sued_Ost/year=2018/part.parquet | passed | processed and validated |
| Amrumbank West | Amrumbank_West | 2023 | validated | 709560 | 81 | 2023-01-01 00:00:00+00:00 | 2023-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Amrumbank_West/year=2023/nws_current_Amrumbank_West_2023_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Amrumbank_West/year=2023/part.parquet | passed | processed and validated |
| Global Tech I | Global_Tech_I | 2021 | validated | 709560 | 81 | 2021-01-01 00:00:00+00:00 | 2021-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Global_Tech_I/year=2021/nws_current_Global_Tech_I_2021_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Global_Tech_I/year=2021/part.parquet | passed | processed and validated |
| Kaskasi | Kaskasi | 2023 | validated | 341640 | 39 | 2023-01-01 00:00:00+00:00 | 2023-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Kaskasi/year=2023/nws_current_Kaskasi_2023_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Kaskasi/year=2023/part.parquet | passed | processed and validated |
| Barrow | Barrow | 2012 | validated | 272304 | 31 | 2012-01-01 00:00:00+00:00 | 2012-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Barrow/year=2012/nws_current_Barrow_2012_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Barrow/year=2012/part.parquet | passed | processed and validated |
| Nordsee One | Nordsee_One | 2018 | validated | 481800 | 55 | 2018-01-01 00:00:00+00:00 | 2018-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Nordsee_One/year=2018/nws_current_Nordsee_One_2018_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Nordsee_One/year=2018/part.parquet | passed | processed and validated |
| Trianel Windpark Borkum 1 | Trianel_Windpark_Borkum_1 | 2018 | validated | 359160 | 41 | 2018-01-01 00:00:00+00:00 | 2018-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Trianel_Windpark_Borkum_1/year=2018/nws_current_Trianel_Windpark_Borkum_1_2018_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Trianel_Windpark_Borkum_1/year=2018/part.parquet | passed | processed and validated |
| Nordsee Ost | Nordsee_Ost | 2018 | validated | 429240 | 49 | 2018-01-01 00:00:00+00:00 | 2018-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Nordsee_Ost/year=2018/nws_current_Nordsee_Ost_2018_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Nordsee_Ost/year=2018/part.parquet | passed | processed and validated |
| Meerwind Sued/Ost | Meerwind_Sued_Ost | 2017 | validated | 709560 | 81 | 2017-01-01 00:00:00+00:00 | 2017-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Meerwind_Sued_Ost/year=2017/nws_current_Meerwind_Sued_Ost_2017_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Meerwind_Sued_Ost/year=2017/part.parquet | passed | processed and validated |
| Bard Offshore 1 | Bard_Offshore_1 | 2021 | validated | 709560 | 81 | 2021-01-01 00:00:00+00:00 | 2021-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Bard_Offshore_1/year=2021/nws_current_Bard_Offshore_1_2021_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Bard_Offshore_1/year=2021/part.parquet | passed | processed and validated |
| Bard Offshore 1 | Bard_Offshore_1 | 2017 | validated | 709560 | 81 | 2017-01-01 00:00:00+00:00 | 2017-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Bard_Offshore_1/year=2017/nws_current_Bard_Offshore_1_2017_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Bard_Offshore_1/year=2017/part.parquet | passed | processed and validated |
| EnBW Hohe See | EnBW_Hohe_See | 2021 | validated | 630720 | 72 | 2021-01-01 00:00:00+00:00 | 2021-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=EnBW_Hohe_See/year=2021/nws_current_EnBW_Hohe_See_2021_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=EnBW_Hohe_See/year=2021/part.parquet | passed | processed and validated |
| Hywind Scotland Pilot Park | Hywind_Scotland_Pilot_Park | 2024 | validated | 52704 | 6 | 2024-01-01 00:00:00+00:00 | 2024-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Hywind_Scotland_Pilot_Park/year=2024/nws_current_Hywind_Scotland_Pilot_Park_2024_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Hywind_Scotland_Pilot_Park/year=2024/part.parquet | passed | processed and validated |
| Bard Offshore 1 | Bard_Offshore_1 | 2018 | validated | 709560 | 81 | 2018-01-01 00:00:00+00:00 | 2018-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Bard_Offshore_1/year=2018/nws_current_Bard_Offshore_1_2018_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Bard_Offshore_1/year=2018/part.parquet | passed | processed and validated |
| Meerwind Sued/Ost | Meerwind_Sued_Ost | 2020 | validated | 711504 | 81 | 2020-01-01 00:00:00+00:00 | 2020-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Meerwind_Sued_Ost/year=2020/nws_current_Meerwind_Sued_Ost_2020_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Meerwind_Sued_Ost/year=2020/part.parquet | passed | processed and validated |
| Global Tech I | Global_Tech_I | 2022 | validated | 709560 | 81 | 2022-01-01 00:00:00+00:00 | 2022-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Global_Tech_I/year=2022/nws_current_Global_Tech_I_2022_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Global_Tech_I/year=2022/part.parquet | passed | processed and validated |
| Gode Wind 1 and 2 | Gode_Wind_1_and_2 | 2023 | validated | 858480 | 98 | 2023-01-01 00:00:00+00:00 | 2023-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Gode_Wind_1_and_2/year=2023/nws_current_Gode_Wind_1_and_2_2023_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Gode_Wind_1_and_2/year=2023/part.parquet | passed | processed and validated |
| Borkum Riffgrund 1 | Borkum_Riffgrund_1 | 2018 | validated | 692040 | 79 | 2018-01-01 00:00:00+00:00 | 2018-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Borkum_Riffgrund_1/year=2018/nws_current_Borkum_Riffgrund_1_2018_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Borkum_Riffgrund_1/year=2018/part.parquet | passed | processed and validated |
| Gode Wind 3 | Gode_Wind_3 | 2025 | validated | 210240 | 24 | 2025-01-01 00:00:00+00:00 | 2025-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Gode_Wind_3/year=2025/nws_current_Gode_Wind_3_2025_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Gode_Wind_3/year=2025/part.parquet | passed | processed and validated |
| Veja Mate | Veja_Mate | 2021 | validated | 595680 | 68 | 2021-01-01 00:00:00+00:00 | 2021-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Veja_Mate/year=2021/nws_current_Veja_Mate_2021_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Veja_Mate/year=2021/part.parquet | passed | processed and validated |
| Veja Mate | Veja_Mate | 2017 | validated | 595680 | 68 | 2017-01-01 00:00:00+00:00 | 2017-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Veja_Mate/year=2017/nws_current_Veja_Mate_2017_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Veja_Mate/year=2017/part.parquet | passed | processed and validated |
| Trianel Windpark Borkum 1 | Trianel_Windpark_Borkum_1 | 2021 | validated | 359160 | 41 | 2021-01-01 00:00:00+00:00 | 2021-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Trianel_Windpark_Borkum_1/year=2021/nws_current_Trianel_Windpark_Borkum_1_2021_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Trianel_Windpark_Borkum_1/year=2021/part.parquet | passed | processed and validated |
| Alpha Ventus | Alpha_Ventus | 2011 | validated | 113880 | 13 | 2011-01-01 00:00:00+00:00 | 2011-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Alpha_Ventus/year=2011/nws_current_Alpha_Ventus_2011_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Alpha_Ventus/year=2011/part.parquet | passed | processed and validated |
| Nordsee Ost | Nordsee_Ost | 2020 | validated | 430416 | 49 | 2020-01-01 00:00:00+00:00 | 2020-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Nordsee_Ost/year=2020/nws_current_Nordsee_Ost_2020_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Nordsee_Ost/year=2020/part.parquet | passed | processed and validated |
| EnBW Hohe See | EnBW_Hohe_See | 2020 | validated | 632448 | 72 | 2020-01-01 00:00:00+00:00 | 2020-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=EnBW_Hohe_See/year=2020/nws_current_EnBW_Hohe_See_2020_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=EnBW_Hohe_See/year=2020/part.parquet | passed | processed and validated |
| Global Tech I | Global_Tech_I | 2020 | validated | 711504 | 81 | 2020-01-01 00:00:00+00:00 | 2020-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Global_Tech_I/year=2020/nws_current_Global_Tech_I_2020_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Global_Tech_I/year=2020/part.parquet | passed | processed and validated |
| Gode Wind 1 and 2 | Gode_Wind_1_and_2 | 2022 | validated | 858480 | 98 | 2022-01-01 00:00:00+00:00 | 2022-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Gode_Wind_1_and_2/year=2022/nws_current_Gode_Wind_1_and_2_2022_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Gode_Wind_1_and_2/year=2022/part.parquet | passed | processed and validated |
| Gode Wind 1 and 2 | Gode_Wind_1_and_2 | 2017 | validated | 858480 | 98 | 2017-01-01 00:00:00+00:00 | 2017-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Gode_Wind_1_and_2/year=2017/nws_current_Gode_Wind_1_and_2_2017_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Gode_Wind_1_and_2/year=2017/part.parquet | passed | processed and validated |
| Deutsche Bucht | Deutsche_Bucht | 2021 | validated | 280320 | 32 | 2021-01-01 00:00:00+00:00 | 2021-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Deutsche_Bucht/year=2021/nws_current_Deutsche_Bucht_2021_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Deutsche_Bucht/year=2021/part.parquet | passed | processed and validated |
| Dudgeon | Dudgeon | 2017 | validated | 595680 | 68 | 2017-01-01 00:00:00+00:00 | 2017-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Dudgeon/year=2017/nws_current_Dudgeon_2017_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Dudgeon/year=2017/part.parquet | passed | processed and validated |
| Trianel Windpark Borkum 1 | Trianel_Windpark_Borkum_1 | 2017 | validated | 359160 | 41 | 2017-01-01 00:00:00+00:00 | 2017-12-31 23:00:00+00:00 | Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots/wind_farm=Trianel_Windpark_Borkum_1/year=2017/nws_current_Trianel_Windpark_Borkum_1_2017_cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i.nc | Data/Processed/metocean/nws_current_timeseries/wind_farm=Trianel_Windpark_Borkum_1/year=2017/part.parquet | passed | processed and validated |

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
| Merkur Offshore | 2024 | 588528 | 67 | 2024-01-01 00:00:00+00:00 | 2024-12-31 23:00:00+00:00 | 60.000 | 588528 | 0 | 0.000 | True | True | passed |
| Vesterhav Nord | 2024 | 193248 | 22 | 2024-01-01 00:00:00+00:00 | 2024-12-31 23:00:00+00:00 | 60.000 | 193248 | 0 | 0.000 | True | True | passed |
| Kaskasi | 2024 | 342576 | 39 | 2024-01-01 00:00:00+00:00 | 2024-12-31 23:00:00+00:00 | 60.000 | 342576 | 0 | 0.000 | True | True | passed |
| Borkum Riffgrund 1 | 2024 | 693936 | 79 | 2024-01-01 00:00:00+00:00 | 2024-12-31 23:00:00+00:00 | 60.000 | 693936 | 0 | 0.000 | True | True | passed |
| Trianel Windpark Borkum 2 | 2024 | 289872 | 33 | 2024-01-01 00:00:00+00:00 | 2024-12-31 23:00:00+00:00 | 60.000 | 289872 | 0 | 0.000 | True | True | passed |
| Nordsee One | 2024 | 483120 | 55 | 2024-01-01 00:00:00+00:00 | 2024-12-31 23:00:00+00:00 | 60.000 | 483120 | 0 | 0.000 | True | True | passed |
| Global Tech I | 2024 | 711504 | 81 | 2024-01-01 00:00:00+00:00 | 2024-12-31 23:00:00+00:00 | 60.000 | 711504 | 0 | 0.000 | True | True | passed |
| Borkum Riffgrund 2 | 2024 | 500688 | 57 | 2024-01-01 00:00:00+00:00 | 2024-12-31 23:00:00+00:00 | 60.000 | 500688 | 0 | 0.000 | True | True | passed |
| Trianel Windpark Borkum 1 | 2024 | 360144 | 41 | 2024-01-01 00:00:00+00:00 | 2024-12-31 23:00:00+00:00 | 60.000 | 360144 | 0 | 0.000 | True | True | passed |
| EnBW Hohe See | 2024 | 632448 | 72 | 2024-01-01 00:00:00+00:00 | 2024-12-31 23:00:00+00:00 | 60.000 | 632448 | 0 | 0.000 | True | True | passed |
| Bard Offshore 1 | 2024 | 711504 | 81 | 2024-01-01 00:00:00+00:00 | 2024-12-31 23:00:00+00:00 | 60.000 | 711504 | 0 | 0.000 | True | True | passed |
| Horns Rev III | 2019 | 438000 | 50 | 2019-01-01 00:00:00+00:00 | 2019-12-31 23:00:00+00:00 | 60.000 | 438000 | 0 | 0.000 | True | True | passed |
| Riffgat | 2024 | 272304 | 31 | 2024-01-01 00:00:00+00:00 | 2024-12-31 23:00:00+00:00 | 60.000 | 272304 | 0 | 0.000 | True | True | passed |
| Albatros | 2024 | 149328 | 17 | 2024-01-01 00:00:00+00:00 | 2024-12-31 23:00:00+00:00 | 60.000 | 149328 | 0 | 0.000 | True | True | passed |
| Alpha Ventus | 2024 | 114192 | 13 | 2024-01-01 00:00:00+00:00 | 2024-12-31 23:00:00+00:00 | 60.000 | 114192 | 0 | 0.000 | True | True | passed |
| Veja Mate | 2024 | 597312 | 68 | 2024-01-01 00:00:00+00:00 | 2024-12-31 23:00:00+00:00 | 60.000 | 597312 | 0 | 0.000 | True | True | passed |
| Deutsche Bucht | 2024 | 281088 | 32 | 2024-01-01 00:00:00+00:00 | 2024-12-31 23:00:00+00:00 | 60.000 | 281088 | 0 | 0.000 | True | True | passed |
| Butendiek | 2017 | 709560 | 81 | 2017-01-01 00:00:00+00:00 | 2017-12-31 23:00:00+00:00 | 60.000 | 709560 | 0 | 0.000 | True | True | passed |
| Walney 2 | 2012 | 456768 | 52 | 2012-01-01 00:00:00+00:00 | 2012-12-31 23:00:00+00:00 | 60.000 | 456768 | 0 | 0.000 | True | True | passed |
| Gemini | 2024 | 1326384 | 151 | 2024-01-01 00:00:00+00:00 | 2024-12-31 23:00:00+00:00 | 60.000 | 1326384 | 0 | 0.000 | True | True | passed |
| Dan Tysk | 2021 | 709560 | 81 | 2021-01-01 00:00:00+00:00 | 2021-12-31 23:00:00+00:00 | 60.000 | 709560 | 0 | 0.000 | True | True | passed |
| Thanet | 2012 | 887184 | 101 | 2012-01-01 00:00:00+00:00 | 2012-12-31 23:00:00+00:00 | 60.000 | 887184 | 0 | 0.000 | True | True | passed |
| Horns Rev II | 2011 | 805920 | 92 | 2011-01-01 00:00:00+00:00 | 2011-12-31 23:00:00+00:00 | 60.000 | 805920 | 0 | 0.000 | True | True | passed |
| Ormonde | 2012 | 272304 | 31 | 2012-01-01 00:00:00+00:00 | 2012-12-31 23:00:00+00:00 | 60.000 | 272304 | 0 | 0.000 | True | True | passed |
| Sandbank | 2017 | 639480 | 73 | 2017-01-01 00:00:00+00:00 | 2017-12-31 23:00:00+00:00 | 60.000 | 639480 | 0 | 0.000 | True | True | passed |
| Butendiek | 2018 | 709560 | 81 | 2018-01-01 00:00:00+00:00 | 2018-12-31 23:00:00+00:00 | 60.000 | 709560 | 0 | 0.000 | True | True | passed |
| Thornton Bank - phase I | 2012 | 61488 | 7 | 2012-01-01 00:00:00+00:00 | 2012-12-31 23:00:00+00:00 | 60.000 | 61488 | 0 | 0.000 | True | True | passed |
| Horns Rev III | 2022 | 438000 | 50 | 2022-01-01 00:00:00+00:00 | 2022-12-31 23:00:00+00:00 | 60.000 | 438000 | 0 | 0.000 | True | True | passed |
| Butendiek | 2020 | 711504 | 81 | 2020-01-01 00:00:00+00:00 | 2020-12-31 23:00:00+00:00 | 60.000 | 711504 | 0 | 0.000 | True | True | passed |
| Walney 1 | 2012 | 456768 | 52 | 2012-01-01 00:00:00+00:00 | 2012-12-31 23:00:00+00:00 | 60.000 | 456768 | 0 | 0.000 | True | True | passed |
| Meerwind Sued/Ost | 2021 | 709560 | 81 | 2021-01-01 00:00:00+00:00 | 2021-12-31 23:00:00+00:00 | 60.000 | 709560 | 0 | 0.000 | True | True | passed |
| Butendiek | 2019 | 709560 | 81 | 2019-01-01 00:00:00+00:00 | 2019-12-31 23:00:00+00:00 | 60.000 | 709560 | 0 | 0.000 | True | True | passed |
| Dan Tysk | 2017 | 709560 | 81 | 2017-01-01 00:00:00+00:00 | 2017-12-31 23:00:00+00:00 | 60.000 | 709560 | 0 | 0.000 | True | True | passed |
| Horns Rev II | 2010 | 805920 | 92 | 2010-01-01 00:00:00+00:00 | 2010-12-31 23:00:00+00:00 | 60.000 | 805920 | 0 | 0.000 | True | True | passed |
| Amrumbank West | 2022 | 709560 | 81 | 2022-01-01 00:00:00+00:00 | 2022-12-31 23:00:00+00:00 | 60.000 | 709560 | 0 | 0.000 | True | True | passed |
| Horns Rev II | 2019 | 805920 | 92 | 2019-01-01 00:00:00+00:00 | 2019-12-31 23:00:00+00:00 | 60.000 | 805920 | 0 | 0.000 | True | True | passed |
| EnBW Hohe See | 2019 | 630720 | 72 | 2019-01-01 00:00:00+00:00 | 2019-12-31 23:00:00+00:00 | 60.000 | 630720 | 0 | 0.000 | True | True | passed |
| Dan Tysk | 2019 | 709560 | 81 | 2019-01-01 00:00:00+00:00 | 2019-12-31 23:00:00+00:00 | 60.000 | 709560 | 0 | 0.000 | True | True | passed |
| Dan Tysk | 2018 | 709560 | 81 | 2018-01-01 00:00:00+00:00 | 2018-12-31 23:00:00+00:00 | 60.000 | 709560 | 0 | 0.000 | True | True | passed |
| Horns Rev II | 2012 | 808128 | 92 | 2012-01-01 00:00:00+00:00 | 2012-12-31 23:00:00+00:00 | 60.000 | 808128 | 0 | 0.000 | True | True | passed |
| Hornsea Project 1 | 2024 | 1537200 | 175 | 2024-01-01 00:00:00+00:00 | 2024-12-31 23:00:00+00:00 | 60.000 | 1537200 | 0 | 0.000 | True | True | passed |
| Horns Rev II | 2022 | 805920 | 92 | 2022-01-01 00:00:00+00:00 | 2022-12-31 23:00:00+00:00 | 60.000 | 805920 | 0 | 0.000 | True | True | passed |
| Horns Rev II | 2017 | 805920 | 92 | 2017-01-01 00:00:00+00:00 | 2017-12-31 23:00:00+00:00 | 60.000 | 805920 | 0 | 0.000 | True | True | passed |
| Borkum Riffgrund 2 | 2018 | 499320 | 57 | 2018-01-01 00:00:00+00:00 | 2018-12-31 23:00:00+00:00 | 60.000 | 499320 | 0 | 0.000 | True | True | passed |
| Dan Tysk | 2020 | 711504 | 81 | 2020-01-01 00:00:00+00:00 | 2020-12-31 23:00:00+00:00 | 60.000 | 711504 | 0 | 0.000 | True | True | passed |
| Butendiek | 2021 | 709560 | 81 | 2021-01-01 00:00:00+00:00 | 2021-12-31 23:00:00+00:00 | 60.000 | 709560 | 0 | 0.000 | True | True | passed |
| Butendiek | 2022 | 709560 | 81 | 2022-01-01 00:00:00+00:00 | 2022-12-31 23:00:00+00:00 | 60.000 | 709560 | 0 | 0.000 | True | True | passed |
| Horns Rev II | 2018 | 805920 | 92 | 2018-01-01 00:00:00+00:00 | 2018-12-31 23:00:00+00:00 | 60.000 | 805920 | 0 | 0.000 | True | True | passed |
| Nordsee Ost | 2021 | 429240 | 49 | 2021-01-01 00:00:00+00:00 | 2021-12-31 23:00:00+00:00 | 60.000 | 429240 | 0 | 0.000 | True | True | passed |
| Meerwind Sued/Ost | 2022 | 709560 | 81 | 2022-01-01 00:00:00+00:00 | 2022-12-31 23:00:00+00:00 | 60.000 | 709560 | 0 | 0.000 | True | True | passed |
| Dan Tysk | 2022 | 709560 | 81 | 2022-01-01 00:00:00+00:00 | 2022-12-31 23:00:00+00:00 | 60.000 | 709560 | 0 | 0.000 | True | True | passed |
| Horns Rev III | 2020 | 439200 | 50 | 2020-01-01 00:00:00+00:00 | 2020-12-31 23:00:00+00:00 | 60.000 | 439200 | 0 | 0.000 | True | True | passed |
| Butendiek | 2023 | 709560 | 81 | 2023-01-01 00:00:00+00:00 | 2023-12-31 23:00:00+00:00 | 60.000 | 709560 | 0 | 0.000 | True | True | passed |
| Hornsea Project 2 | 2024 | 1458144 | 166 | 2024-01-01 00:00:00+00:00 | 2024-12-31 23:00:00+00:00 | 60.000 | 1458144 | 0 | 0.000 | True | True | passed |
| Global Tech I | 2019 | 709560 | 81 | 2019-01-01 00:00:00+00:00 | 2019-12-31 23:00:00+00:00 | 60.000 | 709560 | 0 | 0.000 | True | True | passed |
| Meerwind Sued/Ost | 2023 | 709560 | 81 | 2023-01-01 00:00:00+00:00 | 2023-12-31 23:00:00+00:00 | 60.000 | 709560 | 0 | 0.000 | True | True | passed |
| Horns Rev III | 2021 | 438000 | 50 | 2021-01-01 00:00:00+00:00 | 2021-12-31 23:00:00+00:00 | 60.000 | 438000 | 0 | 0.000 | True | True | passed |
| Horns Rev II | 2021 | 805920 | 92 | 2021-01-01 00:00:00+00:00 | 2021-12-31 23:00:00+00:00 | 60.000 | 805920 | 0 | 0.000 | True | True | passed |
| Sandbank | 2022 | 639480 | 73 | 2022-01-01 00:00:00+00:00 | 2022-12-31 23:00:00+00:00 | 60.000 | 639480 | 0 | 0.000 | True | True | passed |
| Nordsee Ost | 2023 | 429240 | 49 | 2023-01-01 00:00:00+00:00 | 2023-12-31 23:00:00+00:00 | 60.000 | 429240 | 0 | 0.000 | True | True | passed |
| OWF Prinses Amalia | 2012 | 535824 | 61 | 2012-01-01 00:00:00+00:00 | 2012-12-31 23:00:00+00:00 | 60.000 | 535824 | 0 | 0.000 | True | True | passed |
| Horns Rev II | 2020 | 808128 | 92 | 2020-01-01 00:00:00+00:00 | 2020-12-31 23:00:00+00:00 | 60.000 | 808128 | 0 | 0.000 | True | True | passed |
| Nordsee Ost | 2022 | 429240 | 49 | 2022-01-01 00:00:00+00:00 | 2022-12-31 23:00:00+00:00 | 60.000 | 429240 | 0 | 0.000 | True | True | passed |
| Hollandse Kust Zuid | 2024 | 1238544 | 141 | 2024-01-01 00:00:00+00:00 | 2024-12-31 23:00:00+00:00 | 60.000 | 1238544 | 0 | 0.000 | True | True | passed |
| Global Tech I | 2018 | 709560 | 81 | 2018-01-01 00:00:00+00:00 | 2018-12-31 23:00:00+00:00 | 60.000 | 709560 | 0 | 0.000 | True | True | passed |
| Amrumbank West | 2021 | 709560 | 81 | 2021-01-01 00:00:00+00:00 | 2021-12-31 23:00:00+00:00 | 60.000 | 709560 | 0 | 0.000 | True | True | passed |
| Horns Rev III | 2023 | 438000 | 50 | 2023-01-01 00:00:00+00:00 | 2023-12-31 23:00:00+00:00 | 60.000 | 438000 | 0 | 0.000 | True | True | passed |
| Sandbank | 2021 | 639480 | 73 | 2021-01-01 00:00:00+00:00 | 2021-12-31 23:00:00+00:00 | 60.000 | 639480 | 0 | 0.000 | True | True | passed |
| Horns Rev II | 2023 | 805920 | 92 | 2023-01-01 00:00:00+00:00 | 2023-12-31 23:00:00+00:00 | 60.000 | 805920 | 0 | 0.000 | True | True | passed |
| Sandbank | 2018 | 639480 | 73 | 2018-01-01 00:00:00+00:00 | 2018-12-31 23:00:00+00:00 | 60.000 | 639480 | 0 | 0.000 | True | True | passed |
| Global Tech I | 2017 | 709560 | 81 | 2017-01-01 00:00:00+00:00 | 2017-12-31 23:00:00+00:00 | 60.000 | 709560 | 0 | 0.000 | True | True | passed |
| Sandbank | 2023 | 639480 | 73 | 2023-01-01 00:00:00+00:00 | 2023-12-31 23:00:00+00:00 | 60.000 | 639480 | 0 | 0.000 | True | True | passed |
| Amrumbank West | 2018 | 709560 | 81 | 2018-01-01 00:00:00+00:00 | 2018-12-31 23:00:00+00:00 | 60.000 | 709560 | 0 | 0.000 | True | True | passed |
| Borkum Riffgrund 2 | 2021 | 499320 | 57 | 2021-01-01 00:00:00+00:00 | 2021-12-31 23:00:00+00:00 | 60.000 | 499320 | 0 | 0.000 | True | True | passed |
| Merkur Offshore | 2023 | 586920 | 67 | 2023-01-01 00:00:00+00:00 | 2023-12-31 23:00:00+00:00 | 60.000 | 586920 | 0 | 0.000 | True | True | passed |
| Triton Knoll | 2024 | 799344 | 91 | 2024-01-01 00:00:00+00:00 | 2024-12-31 23:00:00+00:00 | 60.000 | 799344 | 0 | 0.000 | True | True | passed |
| Merkur Offshore | 2021 | 586920 | 67 | 2021-01-01 00:00:00+00:00 | 2021-12-31 23:00:00+00:00 | 60.000 | 586920 | 0 | 0.000 | True | True | passed |
| Merkur Offshore | 2022 | 586920 | 67 | 2022-01-01 00:00:00+00:00 | 2022-12-31 23:00:00+00:00 | 60.000 | 586920 | 0 | 0.000 | True | True | passed |
| Dan Tysk | 2023 | 709560 | 81 | 2023-01-01 00:00:00+00:00 | 2023-12-31 23:00:00+00:00 | 60.000 | 709560 | 0 | 0.000 | True | True | passed |
| OWF Egmond aan Zee | 2012 | 325008 | 37 | 2012-01-01 00:00:00+00:00 | 2012-12-31 23:00:00+00:00 | 60.000 | 325008 | 0 | 0.000 | True | True | passed |
| Nordsee One | 2022 | 481800 | 55 | 2022-01-01 00:00:00+00:00 | 2022-12-31 23:00:00+00:00 | 60.000 | 481800 | 0 | 0.000 | True | True | passed |
| Nordsee One | 2017 | 481800 | 55 | 2017-01-01 00:00:00+00:00 | 2017-12-31 23:00:00+00:00 | 60.000 | 481800 | 0 | 0.000 | True | True | passed |
| Sandbank | 2019 | 639480 | 73 | 2019-01-01 00:00:00+00:00 | 2019-12-31 23:00:00+00:00 | 60.000 | 639480 | 0 | 0.000 | True | True | passed |
| Greater Gabbard | 2017 | 1235160 | 141 | 2017-01-01 00:00:00+00:00 | 2017-12-31 23:00:00+00:00 | 60.000 | 1235160 | 0 | 0.000 | True | True | passed |
| Meerwind Sued/Ost | 2018 | 709560 | 81 | 2018-01-01 00:00:00+00:00 | 2018-12-31 23:00:00+00:00 | 60.000 | 709560 | 0 | 0.000 | True | True | passed |
| Amrumbank West | 2023 | 709560 | 81 | 2023-01-01 00:00:00+00:00 | 2023-12-31 23:00:00+00:00 | 60.000 | 709560 | 0 | 0.000 | True | True | passed |
| Global Tech I | 2021 | 709560 | 81 | 2021-01-01 00:00:00+00:00 | 2021-12-31 23:00:00+00:00 | 60.000 | 709560 | 0 | 0.000 | True | True | passed |
| Kaskasi | 2023 | 341640 | 39 | 2023-01-01 00:00:00+00:00 | 2023-12-31 23:00:00+00:00 | 60.000 | 341640 | 0 | 0.000 | True | True | passed |
| Barrow | 2012 | 272304 | 31 | 2012-01-01 00:00:00+00:00 | 2012-12-31 23:00:00+00:00 | 60.000 | 272304 | 0 | 0.000 | True | True | passed |
| Nordsee One | 2018 | 481800 | 55 | 2018-01-01 00:00:00+00:00 | 2018-12-31 23:00:00+00:00 | 60.000 | 481800 | 0 | 0.000 | True | True | passed |
| Trianel Windpark Borkum 1 | 2018 | 359160 | 41 | 2018-01-01 00:00:00+00:00 | 2018-12-31 23:00:00+00:00 | 60.000 | 359160 | 0 | 0.000 | True | True | passed |
| Nordsee Ost | 2018 | 429240 | 49 | 2018-01-01 00:00:00+00:00 | 2018-12-31 23:00:00+00:00 | 60.000 | 429240 | 0 | 0.000 | True | True | passed |
| Meerwind Sued/Ost | 2017 | 709560 | 81 | 2017-01-01 00:00:00+00:00 | 2017-12-31 23:00:00+00:00 | 60.000 | 709560 | 0 | 0.000 | True | True | passed |
| Bard Offshore 1 | 2021 | 709560 | 81 | 2021-01-01 00:00:00+00:00 | 2021-12-31 23:00:00+00:00 | 60.000 | 709560 | 0 | 0.000 | True | True | passed |
| Bard Offshore 1 | 2017 | 709560 | 81 | 2017-01-01 00:00:00+00:00 | 2017-12-31 23:00:00+00:00 | 60.000 | 709560 | 0 | 0.000 | True | True | passed |
| EnBW Hohe See | 2021 | 630720 | 72 | 2021-01-01 00:00:00+00:00 | 2021-12-31 23:00:00+00:00 | 60.000 | 630720 | 0 | 0.000 | True | True | passed |
| Hywind Scotland Pilot Park | 2024 | 52704 | 6 | 2024-01-01 00:00:00+00:00 | 2024-12-31 23:00:00+00:00 | 60.000 | 52704 | 0 | 0.000 | True | True | passed |
| Bard Offshore 1 | 2018 | 709560 | 81 | 2018-01-01 00:00:00+00:00 | 2018-12-31 23:00:00+00:00 | 60.000 | 709560 | 0 | 0.000 | True | True | passed |
| Meerwind Sued/Ost | 2020 | 711504 | 81 | 2020-01-01 00:00:00+00:00 | 2020-12-31 23:00:00+00:00 | 60.000 | 711504 | 0 | 0.000 | True | True | passed |
| Global Tech I | 2022 | 709560 | 81 | 2022-01-01 00:00:00+00:00 | 2022-12-31 23:00:00+00:00 | 60.000 | 709560 | 0 | 0.000 | True | True | passed |
| Gode Wind 1 and 2 | 2023 | 858480 | 98 | 2023-01-01 00:00:00+00:00 | 2023-12-31 23:00:00+00:00 | 60.000 | 858480 | 0 | 0.000 | True | True | passed |
| Borkum Riffgrund 1 | 2018 | 692040 | 79 | 2018-01-01 00:00:00+00:00 | 2018-12-31 23:00:00+00:00 | 60.000 | 692040 | 0 | 0.000 | True | True | passed |
| Gode Wind 3 | 2025 | 210240 | 24 | 2025-01-01 00:00:00+00:00 | 2025-12-31 23:00:00+00:00 | 60.000 | 210240 | 0 | 0.000 | True | True | passed |
| Veja Mate | 2021 | 595680 | 68 | 2021-01-01 00:00:00+00:00 | 2021-12-31 23:00:00+00:00 | 60.000 | 595680 | 0 | 0.000 | True | True | passed |
| Veja Mate | 2017 | 595680 | 68 | 2017-01-01 00:00:00+00:00 | 2017-12-31 23:00:00+00:00 | 60.000 | 595680 | 0 | 0.000 | True | True | passed |
| Trianel Windpark Borkum 1 | 2021 | 359160 | 41 | 2021-01-01 00:00:00+00:00 | 2021-12-31 23:00:00+00:00 | 60.000 | 359160 | 0 | 0.000 | True | True | passed |
| Alpha Ventus | 2011 | 113880 | 13 | 2011-01-01 00:00:00+00:00 | 2011-12-31 23:00:00+00:00 | 60.000 | 113880 | 0 | 0.000 | True | True | passed |
| Nordsee Ost | 2020 | 430416 | 49 | 2020-01-01 00:00:00+00:00 | 2020-12-31 23:00:00+00:00 | 60.000 | 430416 | 0 | 0.000 | True | True | passed |
| EnBW Hohe See | 2020 | 632448 | 72 | 2020-01-01 00:00:00+00:00 | 2020-12-31 23:00:00+00:00 | 60.000 | 632448 | 0 | 0.000 | True | True | passed |
| Global Tech I | 2020 | 711504 | 81 | 2020-01-01 00:00:00+00:00 | 2020-12-31 23:00:00+00:00 | 60.000 | 711504 | 0 | 0.000 | True | True | passed |
| Gode Wind 1 and 2 | 2022 | 858480 | 98 | 2022-01-01 00:00:00+00:00 | 2022-12-31 23:00:00+00:00 | 60.000 | 858480 | 0 | 0.000 | True | True | passed |
| Gode Wind 1 and 2 | 2017 | 858480 | 98 | 2017-01-01 00:00:00+00:00 | 2017-12-31 23:00:00+00:00 | 60.000 | 858480 | 0 | 0.000 | True | True | passed |
| Deutsche Bucht | 2021 | 280320 | 32 | 2021-01-01 00:00:00+00:00 | 2021-12-31 23:00:00+00:00 | 60.000 | 280320 | 0 | 0.000 | True | True | passed |
| Dudgeon | 2017 | 595680 | 68 | 2017-01-01 00:00:00+00:00 | 2017-12-31 23:00:00+00:00 | 60.000 | 595680 | 0 | 0.000 | True | True | passed |
| Trianel Windpark Borkum 1 | 2017 | 359160 | 41 | 2017-01-01 00:00:00+00:00 | 2017-12-31 23:00:00+00:00 | 60.000 | 359160 | 0 | 0.000 | True | True | passed |

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
| Merkur Offshore | 2024 | 798 | 183 | 798 | 768 | 1.000 | 16.021 | 28.801 | 4.000 | 38.000 |
| Vesterhav Nord | 2024 | 274 | 172 | 274 | 269 | 1.000 | 17.938 | 28.482 | 7.000 | 59.400 |
| Kaskasi | 2024 | 718 | 171 | 718 | 707 | 1.000 | 15.000 | 28.599 | 6.000 | 33.150 |
| Borkum Riffgrund 1 | 2024 | 755 | 142 | 755 | 728 | 1.000 | 15.858 | 28.834 | 4.000 | 30.000 |
| Trianel Windpark Borkum 2 | 2024 | 574 | 124 | 574 | 549 | 1.000 | 15.104 | 28.806 | 4.000 | 27.350 |
| Nordsee One | 2024 | 330 | 111 | 330 | 320 | 1.000 | 13.983 | 28.395 | 4.000 | 18.550 |
| Global Tech I | 2024 | 263 | 96 | 263 | 258 | 1.000 | 15.500 | 28.813 | 4.000 | 32.000 |
| Borkum Riffgrund 2 | 2024 | 722 | 82 | 722 | 696 | 1.000 | 15.242 | 28.547 | 4.000 | 30.000 |
| Trianel Windpark Borkum 1 | 2024 | 723 | 76 | 723 | 694 | 1.000 | 15.300 | 28.574 | 3.000 | 21.900 |
| EnBW Hohe See | 2024 | 282 | 73 | 282 | 281 | 1.000 | 14.158 | 28.595 | 5.000 | 35.000 |
| Bard Offshore 1 | 2024 | 167 | 70 | 167 | 163 | 1.000 | 13.967 | 28.262 | 5.000 | 32.900 |
| Horns Rev III | 2019 | 73 | 58 | 73 | 71 | 1.000 | 9.000 | 27.395 | 7.000 | 23.000 |
| Riffgat | 2024 | 72 | 44 | 72 | 64 | 1.000 | 15.354 | 27.797 | 3.000 | 8.450 |
| Albatros | 2024 | 275 | 43 | 275 | 270 | 1.000 | 14.275 | 28.533 | 3.000 | 14.300 |
| Alpha Ventus | 2024 | 656 | 41 | 656 | 638 | 1.000 | 15.688 | 28.600 | 4.000 | 27.000 |
| Veja Mate | 2024 | 303 | 41 | 303 | 295 | 1.000 | 13.450 | 28.263 | 4.000 | 24.000 |
| Deutsche Bucht | 2024 | 144 | 38 | 144 | 141 | 1.000 | 14.829 | 27.538 | 3.500 | 37.250 |
| Butendiek | 2017 | 35 | 32 | 35 | 35 | 1.000 | 8.908 | 24.680 | 8.000 | 23.000 |
| Walney 2 | 2012 | 70 | 31 | 70 | 68 | 1.000 | 10.933 | 25.527 | 7.000 | 23.000 |
| Gemini | 2024 | 88 | 25 | 88 | 85 | 1.000 | 15.450 | 29.097 | 6.000 | 22.650 |
| Dan Tysk | 2021 | 34 | 25 | 34 | 34 | 1.000 | 8.904 | 28.612 | 13.500 | 23.350 |
| Thanet | 2012 | 30 | 25 | 30 | 30 | 1.000 | 7.688 | 27.818 | 7.000 | 23.000 |
| Horns Rev II | 2011 | 31 | 24 | 31 | 31 | 1.000 | 11.308 | 27.538 | 9.000 | 14.000 |
| Ormonde | 2012 | 47 | 23 | 47 | 47 | 1.000 | 13.617 | 27.957 | 3.000 | 10.000 |
| Sandbank | 2017 | 28 | 23 | 28 | 28 | 1.000 | 12.708 | 27.645 | 9.500 | 23.000 |
| Butendiek | 2018 | 28 | 23 | 28 | 27 | 1.000 | 7.008 | 25.145 | 8.000 | 23.000 |
| Thornton Bank - phase I | 2012 | 77 | 21 | 77 | 77 | 1.000 | 14.050 | 25.620 | 7.000 | 23.000 |
| Horns Rev III | 2022 | 49 | 20 | 49 | 49 | 1.000 | 11.725 | 28.502 | 5.000 | 22.600 |
| Butendiek | 2020 | 21 | 20 | 21 | 21 | 1.000 | 10.842 | 27.817 | 9.000 | 23.000 |
| Walney 1 | 2012 | 69 | 19 | 69 | 67 | 1.000 | 17.567 | 28.778 | 3.000 | 10.000 |
| Meerwind Sued/Ost | 2021 | 50 | 19 | 50 | 49 | 1.000 | 14.004 | 27.491 | 8.000 | 23.000 |
| Butendiek | 2019 | 27 | 18 | 27 | 27 | 1.000 | 1.400 | 25.802 | 9.000 | 23.700 |
| Dan Tysk | 2017 | 24 | 18 | 24 | 22 | 1.000 | 14.375 | 28.898 | 11.000 | 21.850 |
| Horns Rev II | 2010 | 29 | 18 | 29 | 29 | 1.000 | 11.133 | 28.762 | 8.000 | 9.600 |
| Amrumbank West | 2022 | 81 | 17 | 81 | 79 | 1.000 | 14.250 | 27.817 | 7.000 | 23.000 |
| Horns Rev II | 2019 | 67 | 17 | 67 | 66 | 1.000 | 13.333 | 27.994 | 2.000 | 9.700 |
| EnBW Hohe See | 2019 | 51 | 17 | 51 | 47 | 1.000 | 16.050 | 27.733 | 4.000 | 20.500 |
| Dan Tysk | 2019 | 23 | 17 | 23 | 23 | 1.000 | 14.583 | 29.072 | 6.000 | 22.500 |
| Dan Tysk | 2018 | 23 | 17 | 23 | 23 | 1.000 | 12.725 | 27.742 | 4.000 | 23.000 |
| Horns Rev II | 2012 | 23 | 17 | 23 | 23 | 1.000 | 11.917 | 26.182 | 8.000 | 10.900 |
| Hornsea Project 1 | 2024 | 43 | 16 | 43 | 42 | 1.000 | 10.567 | 26.905 | 4.000 | 15.900 |
| Horns Rev II | 2022 | 29 | 16 | 29 | 29 | 1.000 | 16.475 | 28.782 | 6.000 | 17.400 |
| Horns Rev II | 2017 | 22 | 16 | 22 | 22 | 1.000 | 14.621 | 28.440 | 8.500 | 15.750 |
| Borkum Riffgrund 2 | 2018 | 61 | 15 | 61 | 60 | 1.000 | 12.575 | 28.225 | 3.000 | 7.000 |
| Dan Tysk | 2020 | 26 | 15 | 26 | 26 | 1.000 | 10.329 | 27.652 | 7.500 | 23.000 |
| Butendiek | 2021 | 21 | 15 | 21 | 20 | 1.000 | 8.217 | 27.208 | 8.000 | 23.000 |
| Butendiek | 2022 | 19 | 15 | 19 | 19 | 1.000 | 4.917 | 25.520 | 8.000 | 23.100 |
| Horns Rev II | 2018 | 47 | 14 | 47 | 47 | 1.000 | 18.667 | 28.995 | 4.000 | 19.000 |
| Nordsee Ost | 2021 | 50 | 13 | 50 | 50 | 1.000 | 13.046 | 26.254 | 6.500 | 23.000 |
| Meerwind Sued/Ost | 2022 | 44 | 13 | 44 | 43 | 1.000 | 15.404 | 27.844 | 2.500 | 9.850 |
| Dan Tysk | 2022 | 25 | 13 | 25 | 23 | 1.000 | 13.458 | 27.897 | 8.000 | 23.000 |
| Horns Rev III | 2020 | 23 | 13 | 23 | 23 | 1.000 | 9.992 | 27.919 | 7.000 | 20.900 |
| Butendiek | 2023 | 18 | 13 | 18 | 16 | 1.000 | 6.258 | 23.613 | 12.000 | 23.000 |
| Hornsea Project 2 | 2024 | 51 | 12 | 51 | 49 | 1.000 | 15.050 | 28.612 | 5.000 | 18.000 |
| Global Tech I | 2019 | 50 | 12 | 50 | 45 | 1.000 | 14.175 | 28.477 | 2.500 | 17.000 |
| Meerwind Sued/Ost | 2023 | 27 | 12 | 27 | 27 | 1.000 | 10.058 | 28.345 | 6.000 | 23.000 |
| Horns Rev III | 2021 | 30 | 12 | 30 | 30 | 1.000 | 14.692 | 26.518 | 6.000 | 12.300 |
| Horns Rev II | 2021 | 17 | 12 | 17 | 17 | 1.000 | 16.642 | 27.478 | 7.000 | 9.200 |
| Sandbank | 2022 | 15 | 12 | 15 | 15 | 1.000 | 15.067 | 25.222 | 8.000 | 23.000 |
| Nordsee Ost | 2023 | 30 | 11 | 30 | 29 | 1.000 | 15.775 | 29.006 | 6.500 | 23.000 |
| OWF Prinses Amalia | 2012 | 33 | 11 | 33 | 33 | 1.000 | 8.708 | 23.000 | 8.000 | 23.000 |
| Horns Rev II | 2020 | 24 | 11 | 24 | 23 | 1.000 | 12.204 | 25.039 | 7.000 | 21.200 |
| Nordsee Ost | 2022 | 100 | 10 | 100 | 97 | 1.000 | 14.188 | 27.801 | 6.000 | 23.000 |
| Hollandse Kust Zuid | 2024 | 66 | 10 | 66 | 64 | 1.000 | 14.567 | 28.521 | 3.000 | 9.000 |
| Global Tech I | 2018 | 24 | 10 | 24 | 23 | 1.000 | 13.688 | 26.537 | 3.000 | 11.000 |
| Amrumbank West | 2021 | 22 | 10 | 22 | 22 | 1.000 | 13.613 | 28.574 | 6.500 | 22.900 |
| Horns Rev III | 2023 | 20 | 10 | 20 | 19 | 1.000 | 13.333 | 27.094 | 7.000 | 8.050 |
| Sandbank | 2021 | 18 | 10 | 18 | 18 | 1.000 | 16.550 | 27.943 | 7.000 | 15.350 |
| Horns Rev II | 2023 | 15 | 10 | 15 | 15 | 1.000 | 21.750 | 29.126 | 8.000 | 10.000 |
| Sandbank | 2018 | 15 | 10 | 15 | 15 | 1.000 | 11.892 | 24.621 | 6.000 | 11.800 |
| Global Tech I | 2017 | 14 | 9 | 14 | 14 | 1.000 | 18.750 | 28.259 | 4.500 | 8.350 |
| Sandbank | 2023 | 11 | 9 | 11 | 11 | 1.000 | 22.742 | 28.158 | 8.000 | 19.000 |
| Amrumbank West | 2018 | 12 | 9 | 12 | 11 | 1.000 | 15.242 | 27.607 | 3.000 | 16.050 |
| Borkum Riffgrund 2 | 2021 | 33 | 8 | 33 | 33 | 1.000 | 11.375 | 28.287 | 4.000 | 23.000 |
| Merkur Offshore | 2023 | 28 | 8 | 28 | 28 | 1.000 | 14.025 | 26.681 | 5.000 | 23.000 |
| Triton Knoll | 2024 | 27 | 8 | 27 | 24 | 1.000 | 14.917 | 27.536 | 4.000 | 13.500 |
| Merkur Offshore | 2021 | 30 | 7 | 30 | 30 | 1.000 | 12.200 | 26.476 | 6.500 | 23.000 |
| Merkur Offshore | 2022 | 25 | 7 | 25 | 24 | 1.000 | 9.133 | 18.528 | 7.000 | 23.000 |
| Dan Tysk | 2023 | 18 | 7 | 18 | 16 | 1.000 | 13.296 | 24.000 | 4.500 | 17.150 |
| OWF Egmond aan Zee | 2012 | 18 | 7 | 18 | 17 | 1.000 | 16.050 | 29.277 | 4.500 | 18.750 |
| Nordsee One | 2022 | 13 | 7 | 13 | 11 | 1.000 | 21.175 | 25.538 | 4.000 | 9.000 |
| Nordsee One | 2017 | 12 | 7 | 12 | 12 | 1.000 | 14.929 | 29.923 | 2.500 | 8.700 |
| Sandbank | 2019 | 12 | 7 | 12 | 12 | 1.000 | 14.546 | 27.734 | 4.500 | 23.000 |
| Greater Gabbard | 2017 | 18 | 6 | 18 | 18 | 1.000 | 18.433 | 27.886 | 2.000 | 3.000 |
| Meerwind Sued/Ost | 2018 | 23 | 6 | 23 | 22 | 1.000 | 15.983 | 27.535 | 2.000 | 11.800 |
| Amrumbank West | 2023 | 16 | 6 | 16 | 15 | 1.000 | 16.238 | 29.248 | 8.000 | 23.000 |
| Global Tech I | 2021 | 18 | 6 | 18 | 17 | 1.000 | 15.104 | 26.736 | 6.000 | 23.150 |
| Kaskasi | 2023 | 14 | 6 | 14 | 13 | 1.000 | 16.587 | 29.565 | 8.500 | 23.000 |
| Barrow | 2012 | 11 | 6 | 11 | 11 | 1.000 | 14.567 | 20.629 | 6.000 | 8.000 |
| Nordsee One | 2018 | 10 | 6 | 10 | 10 | 1.000 | 4.521 | 23.995 | 3.000 | 4.100 |
| Trianel Windpark Borkum 1 | 2018 | 55 | 5 | 55 | 54 | 1.000 | 10.167 | 28.131 | 3.000 | 7.000 |
| Nordsee Ost | 2018 | 34 | 5 | 34 | 33 | 1.000 | 12.379 | 28.820 | 3.500 | 12.000 |
| Meerwind Sued/Ost | 2017 | 12 | 5 | 12 | 12 | 1.000 | 16.367 | 24.200 | 2.000 | 12.650 |
| Bard Offshore 1 | 2021 | 13 | 5 | 13 | 13 | 1.000 | 13.950 | 24.148 | 3.000 | 18.400 |
| Bard Offshore 1 | 2017 | 10 | 5 | 10 | 10 | 1.000 | 16.758 | 24.006 | 5.500 | 10.100 |
| EnBW Hohe See | 2021 | 10 | 5 | 10 | 8 | 1.000 | 9.054 | 26.455 | 10.500 | 23.550 |
| Hywind Scotland Pilot Park | 2024 | 11 | 5 | 11 | 9 | 1.000 | 10.558 | 20.538 | 1.000 | 7.500 |
| Bard Offshore 1 | 2018 | 15 | 4 | 15 | 13 | 1.000 | 19.617 | 28.359 | 3.000 | 10.000 |
| Meerwind Sued/Ost | 2020 | 12 | 4 | 12 | 11 | 1.000 | 4.592 | 25.989 | 3.000 | 23.000 |
| Global Tech I | 2022 | 11 | 4 | 11 | 11 | 1.000 | 13.792 | 24.092 | 2.000 | 17.000 |
| Gode Wind 1 and 2 | 2023 | 10 | 4 | 10 | 10 | 1.000 | 11.433 | 24.775 | 6.500 | 12.750 |
| Borkum Riffgrund 1 | 2018 | 51 | 3 | 51 | 51 | 1.000 | 14.017 | 28.038 | 3.000 | 9.500 |
| Gode Wind 3 | 2025 | 56 | 3 | 56 | 53 | 1.000 | 12.875 | 27.179 | 4.000 | 12.250 |
| Veja Mate | 2021 | 24 | 3 | 24 | 24 | 1.000 | 15.542 | 28.880 | 3.500 | 7.850 |
| Veja Mate | 2017 | 20 | 3 | 20 | 20 | 1.000 | 17.012 | 28.051 | 2.500 | 8.150 |
| Trianel Windpark Borkum 1 | 2021 | 24 | 3 | 24 | 23 | 1.000 | 18.017 | 27.545 | 3.500 | 23.000 |
| Alpha Ventus | 2011 | 17 | 3 | 17 | 16 | 1.000 | 18.583 | 26.207 | 6.000 | 21.400 |
| Nordsee Ost | 2020 | 15 | 3 | 15 | 13 | 1.000 | 5.725 | 24.677 | 1.000 | 23.000 |
| EnBW Hohe See | 2020 | 14 | 3 | 14 | 14 | 1.000 | 13.529 | 28.898 | 5.000 | 19.400 |
| Global Tech I | 2020 | 14 | 3 | 14 | 14 | 1.000 | 14.917 | 27.950 | 2.000 | 11.600 |
| Gode Wind 1 and 2 | 2022 | 11 | 3 | 11 | 10 | 1.000 | 9.975 | 21.837 | 3.000 | 7.000 |
| Gode Wind 1 and 2 | 2017 | 11 | 3 | 11 | 11 | 1.000 | 15.942 | 26.683 | 3.000 | 9.000 |
| Deutsche Bucht | 2021 | 12 | 3 | 12 | 12 | 1.000 | 17.096 | 29.271 | 4.000 | 11.500 |
| Dudgeon | 2017 | 13 | 3 | 13 | 13 | 1.000 | 23.308 | 29.327 | 2.000 | 2.400 |
| Trianel Windpark Borkum 1 | 2017 | 11 | 3 | 11 | 10 | 1.000 | 6.850 | 27.571 | 5.000 | 12.500 |

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
| Merkur Offshore | 2024 | 0.001 | 0.376 | 0.661 | 0.981 | 0.140 | 0.294 | 6.310 | 144.850 | operationally_plausible_variability |
| Vesterhav Nord | 2024 | 0.000 | 0.195 | 0.431 | 0.839 | 0.060 | 0.123 | 6.585 | 108.212 | operationally_plausible_variability |
| Kaskasi | 2024 | 0.003 | 0.363 | 0.594 | 0.818 | 0.105 | 0.256 | 14.530 | 99.932 | operationally_plausible_variability |
| Borkum Riffgrund 1 | 2024 | 0.001 | 0.392 | 0.677 | 0.940 | 0.144 | 0.303 | 7.164 | 138.678 | operationally_plausible_variability |
| Trianel Windpark Borkum 2 | 2024 | 0.001 | 0.372 | 0.655 | 0.974 | 0.138 | 0.292 | 6.156 | 145.594 | operationally_plausible_variability |
| Nordsee One | 2024 | 0.005 | 0.386 | 0.666 | 0.971 | 0.139 | 0.290 | 8.391 | 130.870 | operationally_plausible_variability |
| Global Tech I | 2024 | 0.001 | 0.282 | 0.497 | 0.676 | 0.103 | 0.209 | 6.054 | 137.765 | operationally_plausible_variability |
| Borkum Riffgrund 2 | 2024 | 0.001 | 0.392 | 0.677 | 0.943 | 0.144 | 0.303 | 7.214 | 139.123 | operationally_plausible_variability |
| Trianel Windpark Borkum 1 | 2024 | 0.001 | 0.373 | 0.656 | 0.964 | 0.139 | 0.293 | 6.231 | 145.105 | operationally_plausible_variability |
| EnBW Hohe See | 2024 | 0.002 | 0.293 | 0.517 | 0.702 | 0.108 | 0.219 | 5.720 | 139.855 | operationally_plausible_variability |
| Bard Offshore 1 | 2024 | 0.002 | 0.295 | 0.533 | 0.735 | 0.112 | 0.222 | 5.616 | 142.730 | operationally_plausible_variability |
| Horns Rev III | 2019 | 0.001 | 0.264 | 0.466 | 0.697 | 0.090 | 0.182 | 13.358 | 113.813 | operationally_plausible_variability |
| Riffgat | 2024 | 0.006 | 0.381 | 0.644 | 0.892 | 0.121 | 0.307 | 10.764 | 125.588 | operationally_plausible_variability |
| Albatros | 2024 | 0.002 | 0.282 | 0.500 | 0.673 | 0.105 | 0.210 | 5.839 | 137.491 | operationally_plausible_variability |
| Alpha Ventus | 2024 | 0.001 | 0.385 | 0.669 | 0.943 | 0.142 | 0.298 | 6.953 | 140.406 | operationally_plausible_variability |
| Veja Mate | 2024 | 0.001 | 0.300 | 0.543 | 0.751 | 0.114 | 0.226 | 5.699 | 142.323 | operationally_plausible_variability |
| Deutsche Bucht | 2024 | 0.001 | 0.294 | 0.533 | 0.751 | 0.111 | 0.220 | 5.846 | 140.734 | operationally_plausible_variability |
| Butendiek | 2017 | 0.007 | 0.285 | 0.438 | 0.633 | 0.066 | 0.165 | 22.268 | 79.302 | operationally_plausible_variability |
| Walney 2 | 2012 | 0.006 | 0.343 | 0.553 | 0.802 | 0.046 | 0.138 | 26.886 | 55.028 | operationally_plausible_variability |
| Gemini | 2024 | 0.000 | 0.346 | 0.621 | 0.892 | 0.133 | 0.271 | 5.451 | 147.681 | operationally_plausible_variability |
| Dan Tysk | 2021 | 0.000 | 0.257 | 0.438 | 0.626 | 0.086 | 0.179 | 10.526 | 123.887 | operationally_plausible_variability |
| Thanet | 2012 | 0.002 | 0.520 | 1.066 | 1.527 | 0.181 | 0.448 | 13.206 | 110.599 | operationally_plausible_variability |
| Horns Rev II | 2011 | 0.001 | 0.307 | 0.537 | 0.739 | 0.113 | 0.210 | 13.954 | 114.140 | operationally_plausible_variability |
| Ormonde | 2012 | 0.006 | 0.321 | 0.518 | 0.720 | 0.037 | 0.128 | 26.581 | 50.168 | operationally_plausible_variability |
| Sandbank | 2017 | 0.001 | 0.222 | 0.376 | 0.575 | 0.078 | 0.151 | 9.263 | 124.695 | operationally_plausible_variability |
| Butendiek | 2018 | 0.000 | 0.290 | 0.460 | 0.726 | 0.071 | 0.184 | 20.303 | 90.619 | operationally_plausible_variability |
| Thornton Bank - phase I | 2012 | 0.013 | 0.545 | 0.920 | 1.074 | 0.190 | 0.374 | 13.892 | 103.188 | operationally_plausible_variability |
| Horns Rev III | 2022 | 0.001 | 0.259 | 0.462 | 0.711 | 0.089 | 0.181 | 12.944 | 113.988 | operationally_plausible_variability |
| Butendiek | 2020 | 0.011 | 0.283 | 0.441 | 0.669 | 0.067 | 0.171 | 21.240 | 85.616 | operationally_plausible_variability |
| Walney 1 | 2012 | 0.003 | 0.350 | 0.583 | 0.904 | 0.051 | 0.171 | 26.054 | 58.586 | operationally_plausible_variability |
| Meerwind Sued/Ost | 2021 | 0.004 | 0.371 | 0.602 | 0.891 | 0.104 | 0.261 | 14.350 | 102.396 | operationally_plausible_variability |
| Butendiek | 2019 | 0.002 | 0.287 | 0.445 | 0.586 | 0.068 | 0.169 | 21.429 | 85.111 | operationally_plausible_variability |
| Dan Tysk | 2017 | 0.001 | 0.251 | 0.422 | 0.597 | 0.086 | 0.178 | 10.842 | 125.255 | operationally_plausible_variability |
| Horns Rev II | 2010 | 0.001 | 0.306 | 0.533 | 0.752 | 0.115 | 0.213 | 13.800 | 117.244 | operationally_plausible_variability |
| Amrumbank West | 2022 | 0.011 | 0.361 | 0.580 | 0.766 | 0.099 | 0.246 | 16.497 | 91.867 | operationally_plausible_variability |
| Horns Rev II | 2019 | 0.001 | 0.308 | 0.535 | 0.759 | 0.115 | 0.212 | 13.891 | 118.573 | operationally_plausible_variability |
| EnBW Hohe See | 2019 | 0.001 | 0.304 | 0.530 | 0.746 | 0.111 | 0.222 | 6.101 | 132.364 | operationally_plausible_variability |
| Dan Tysk | 2019 | 0.003 | 0.257 | 0.434 | 0.593 | 0.085 | 0.178 | 10.844 | 121.001 | operationally_plausible_variability |
| Dan Tysk | 2018 | 0.002 | 0.259 | 0.440 | 0.610 | 0.088 | 0.181 | 10.379 | 126.184 | operationally_plausible_variability |
| Horns Rev II | 2012 | 0.001 | 0.303 | 0.532 | 0.734 | 0.113 | 0.211 | 13.722 | 118.335 | operationally_plausible_variability |
| Hornsea Project 1 | 2024 | 0.001 | 0.345 | 0.668 | 1.087 | 0.116 | 0.269 | 10.898 | 118.455 | operationally_plausible_variability |
| Horns Rev II | 2022 | 0.000 | 0.305 | 0.535 | 0.773 | 0.114 | 0.211 | 13.515 | 115.413 | operationally_plausible_variability |
| Horns Rev II | 2017 | 0.001 | 0.302 | 0.528 | 0.717 | 0.116 | 0.212 | 13.586 | 118.544 | operationally_plausible_variability |
| Borkum Riffgrund 2 | 2018 | 0.002 | 0.401 | 0.673 | 0.880 | 0.149 | 0.302 | 7.588 | 138.422 | operationally_plausible_variability |
| Dan Tysk | 2020 | 0.001 | 0.255 | 0.431 | 0.624 | 0.085 | 0.178 | 10.518 | 128.274 | operationally_plausible_variability |
| Butendiek | 2021 | 0.003 | 0.287 | 0.450 | 0.671 | 0.069 | 0.174 | 20.753 | 86.734 | operationally_plausible_variability |
| Butendiek | 2022 | 0.003 | 0.283 | 0.441 | 0.598 | 0.066 | 0.165 | 21.738 | 83.159 | operationally_plausible_variability |
| Horns Rev II | 2018 | 0.001 | 0.311 | 0.543 | 0.784 | 0.115 | 0.214 | 14.374 | 116.036 | operationally_plausible_variability |
| Nordsee Ost | 2021 | 0.004 | 0.367 | 0.597 | 0.904 | 0.105 | 0.258 | 14.231 | 102.072 | operationally_plausible_variability |
| Meerwind Sued/Ost | 2022 | 0.004 | 0.367 | 0.597 | 0.822 | 0.103 | 0.255 | 14.653 | 100.025 | operationally_plausible_variability |
| Dan Tysk | 2022 | 0.002 | 0.254 | 0.430 | 0.629 | 0.084 | 0.179 | 10.549 | 125.538 | operationally_plausible_variability |
| Horns Rev III | 2020 | 0.001 | 0.262 | 0.462 | 0.683 | 0.088 | 0.179 | 13.395 | 109.711 | operationally_plausible_variability |
| Butendiek | 2023 | 0.001 | 0.283 | 0.445 | 0.620 | 0.066 | 0.170 | 21.369 | 84.649 | operationally_plausible_variability |
| Hornsea Project 2 | 2024 | 0.000 | 0.360 | 0.698 | 1.140 | 0.105 | 0.253 | 15.467 | 93.992 | operationally_plausible_variability |
| Global Tech I | 2019 | 0.001 | 0.293 | 0.509 | 0.750 | 0.105 | 0.212 | 6.396 | 130.432 | operationally_plausible_variability |
| Meerwind Sued/Ost | 2023 | 0.002 | 0.366 | 0.596 | 0.844 | 0.102 | 0.253 | 14.669 | 100.200 | operationally_plausible_variability |
| Horns Rev III | 2021 | 0.002 | 0.261 | 0.463 | 0.727 | 0.089 | 0.183 | 13.130 | 114.850 | operationally_plausible_variability |
| Horns Rev II | 2021 | 0.000 | 0.308 | 0.539 | 0.784 | 0.115 | 0.214 | 13.704 | 116.880 | operationally_plausible_variability |
| Sandbank | 2022 | 0.002 | 0.227 | 0.385 | 0.558 | 0.074 | 0.149 | 9.984 | 112.895 | operationally_plausible_variability |
| Nordsee Ost | 2023 | 0.002 | 0.362 | 0.591 | 0.817 | 0.104 | 0.253 | 14.626 | 100.859 | operationally_plausible_variability |
| OWF Prinses Amalia | 2012 | 0.002 | 0.473 | 0.851 | 1.184 | 0.175 | 0.438 | 5.449 | 159.539 | operationally_plausible_variability |
| Horns Rev II | 2020 | 0.001 | 0.305 | 0.533 | 0.743 | 0.113 | 0.208 | 13.746 | 115.163 | operationally_plausible_variability |
| Nordsee Ost | 2022 | 0.006 | 0.362 | 0.590 | 0.802 | 0.104 | 0.253 | 14.689 | 100.350 | operationally_plausible_variability |
| Hollandse Kust Zuid | 2024 | 0.001 | 0.502 | 0.976 | 1.794 | 0.199 | 0.460 | 6.555 | 153.638 | operationally_plausible_variability |
| Global Tech I | 2018 | 0.002 | 0.290 | 0.506 | 0.716 | 0.108 | 0.215 | 6.203 | 136.799 | operationally_plausible_variability |
| Amrumbank West | 2021 | 0.006 | 0.366 | 0.589 | 0.864 | 0.102 | 0.251 | 15.844 | 94.502 | operationally_plausible_variability |
| Horns Rev III | 2023 | 0.002 | 0.262 | 0.466 | 0.753 | 0.087 | 0.180 | 13.293 | 108.980 | operationally_plausible_variability |
| Sandbank | 2021 | 0.001 | 0.226 | 0.388 | 0.569 | 0.075 | 0.150 | 9.629 | 117.514 | operationally_plausible_variability |
| Horns Rev II | 2023 | 0.001 | 0.306 | 0.539 | 0.793 | 0.111 | 0.210 | 13.523 | 114.289 | operationally_plausible_variability |
| Sandbank | 2018 | 0.002 | 0.227 | 0.385 | 0.551 | 0.077 | 0.150 | 9.934 | 112.366 | operationally_plausible_variability |
| Global Tech I | 2017 | 0.001 | 0.285 | 0.497 | 0.773 | 0.106 | 0.214 | 5.725 | 141.337 | operationally_plausible_variability |
| Sandbank | 2023 | 0.001 | 0.228 | 0.393 | 0.553 | 0.074 | 0.149 | 10.038 | 110.494 | operationally_plausible_variability |
| Amrumbank West | 2018 | 0.010 | 0.369 | 0.591 | 0.780 | 0.105 | 0.254 | 15.726 | 95.536 | operationally_plausible_variability |
| Borkum Riffgrund 2 | 2021 | 0.003 | 0.395 | 0.674 | 0.972 | 0.146 | 0.302 | 7.299 | 141.078 | operationally_plausible_variability |
| Merkur Offshore | 2023 | 0.002 | 0.381 | 0.670 | 1.019 | 0.140 | 0.298 | 6.068 | 145.926 | operationally_plausible_variability |
| Triton Knoll | 2024 | 0.001 | 0.593 | 1.115 | 1.773 | 0.188 | 0.513 | 9.546 | 129.071 | operationally_plausible_variability |
| Merkur Offshore | 2021 | 0.003 | 0.378 | 0.655 | 0.972 | 0.141 | 0.292 | 6.415 | 147.725 | operationally_plausible_variability |
| Merkur Offshore | 2022 | 0.001 | 0.375 | 0.655 | 0.898 | 0.139 | 0.292 | 6.383 | 148.708 | operationally_plausible_variability |
| Dan Tysk | 2023 | 0.001 | 0.256 | 0.434 | 0.625 | 0.084 | 0.177 | 10.747 | 119.932 | operationally_plausible_variability |
| OWF Egmond aan Zee | 2012 | 0.004 | 0.434 | 0.778 | 1.107 | 0.161 | 0.426 | 3.816 | 166.602 | operationally_plausible_variability |
| Nordsee One | 2022 | 0.004 | 0.387 | 0.667 | 0.896 | 0.140 | 0.289 | 8.441 | 132.248 | operationally_plausible_variability |
| Nordsee One | 2017 | 0.003 | 0.391 | 0.663 | 0.970 | 0.140 | 0.286 | 8.758 | 127.159 | operationally_plausible_variability |
| Sandbank | 2019 | 0.001 | 0.229 | 0.389 | 0.579 | 0.075 | 0.148 | 9.950 | 111.650 | operationally_plausible_variability |
| Greater Gabbard | 2017 | 0.001 | 0.595 | 1.041 | 1.273 | 0.240 | 0.537 | 3.285 | 170.824 | operationally_plausible_variability |
| Meerwind Sued/Ost | 2018 | 0.005 | 0.376 | 0.609 | 0.840 | 0.109 | 0.261 | 13.939 | 103.296 | operationally_plausible_variability |
| Amrumbank West | 2023 | 0.007 | 0.362 | 0.584 | 0.780 | 0.101 | 0.247 | 16.178 | 94.561 | operationally_plausible_variability |
| Global Tech I | 2021 | 0.000 | 0.287 | 0.503 | 0.724 | 0.104 | 0.214 | 6.302 | 136.646 | operationally_plausible_variability |
| Kaskasi | 2023 | 0.007 | 0.364 | 0.591 | 0.799 | 0.104 | 0.254 | 14.850 | 100.231 | operationally_plausible_variability |
| Barrow | 2012 | 0.000 | 0.416 | 0.742 | 1.262 | 0.105 | 0.329 | 15.384 | 88.453 | operationally_plausible_variability |
| Nordsee One | 2018 | 0.001 | 0.394 | 0.662 | 0.904 | 0.142 | 0.286 | 8.874 | 127.230 | operationally_plausible_variability |
| Trianel Windpark Borkum 1 | 2018 | 0.001 | 0.382 | 0.657 | 0.895 | 0.146 | 0.295 | 6.525 | 146.985 | operationally_plausible_variability |
| Nordsee Ost | 2018 | 0.005 | 0.371 | 0.603 | 0.819 | 0.109 | 0.260 | 13.934 | 103.144 | operationally_plausible_variability |
| Meerwind Sued/Ost | 2017 | 0.005 | 0.368 | 0.587 | 0.849 | 0.105 | 0.250 | 14.786 | 96.626 | operationally_plausible_variability |
| Bard Offshore 1 | 2021 | 0.001 | 0.301 | 0.540 | 0.807 | 0.114 | 0.226 | 5.781 | 140.268 | operationally_plausible_variability |
| Bard Offshore 1 | 2017 | 0.000 | 0.298 | 0.531 | 0.807 | 0.116 | 0.227 | 4.983 | 146.616 | operationally_plausible_variability |
| EnBW Hohe See | 2021 | 0.000 | 0.298 | 0.524 | 0.760 | 0.109 | 0.224 | 6.046 | 138.889 | operationally_plausible_variability |
| Hywind Scotland Pilot Park | 2024 | 0.002 | 0.370 | 0.755 | 1.257 | 0.135 | 0.317 | 5.036 | 144.168 | operationally_plausible_variability |
| Bard Offshore 1 | 2018 | 0.001 | 0.307 | 0.544 | 0.774 | 0.117 | 0.227 | 6.094 | 135.035 | operationally_plausible_variability |
| Meerwind Sued/Ost | 2020 | 0.003 | 0.368 | 0.599 | 0.923 | 0.105 | 0.257 | 14.527 | 100.545 | operationally_plausible_variability |
| Global Tech I | 2022 | 0.001 | 0.284 | 0.503 | 0.763 | 0.103 | 0.212 | 6.180 | 137.030 | operationally_plausible_variability |
| Gode Wind 1 and 2 | 2023 | 0.002 | 0.393 | 0.687 | 1.013 | 0.143 | 0.300 | 7.218 | 138.031 | operationally_plausible_variability |
| Borkum Riffgrund 1 | 2018 | 0.002 | 0.401 | 0.673 | 0.879 | 0.148 | 0.301 | 7.596 | 137.983 | operationally_plausible_variability |
| Gode Wind 3 | 2025 | 0.006 | 0.391 | 0.673 | 0.951 | 0.142 | 0.296 | 7.196 | 136.120 | operationally_plausible_variability |
| Veja Mate | 2021 | 0.001 | 0.306 | 0.550 | 0.826 | 0.116 | 0.231 | 5.802 | 140.755 | operationally_plausible_variability |
| Veja Mate | 2017 | 0.000 | 0.303 | 0.541 | 0.814 | 0.118 | 0.230 | 4.955 | 147.484 | operationally_plausible_variability |
| Trianel Windpark Borkum 1 | 2021 | 0.001 | 0.375 | 0.650 | 0.964 | 0.141 | 0.291 | 6.380 | 146.952 | operationally_plausible_variability |
| Alpha Ventus | 2011 | 0.004 | 0.390 | 0.669 | 0.968 | 0.145 | 0.299 | 7.035 | 141.366 | operationally_plausible_variability |
| Nordsee Ost | 2020 | 0.004 | 0.364 | 0.593 | 0.851 | 0.105 | 0.256 | 14.396 | 100.949 | operationally_plausible_variability |
| EnBW Hohe See | 2020 | 0.002 | 0.296 | 0.519 | 0.960 | 0.108 | 0.219 | 6.007 | 136.858 | operationally_plausible_variability |
| Global Tech I | 2020 | 0.001 | 0.285 | 0.498 | 0.936 | 0.104 | 0.210 | 6.304 | 135.824 | operationally_plausible_variability |
| Gode Wind 1 and 2 | 2022 | 0.003 | 0.391 | 0.681 | 0.979 | 0.143 | 0.300 | 7.266 | 138.897 | operationally_plausible_variability |
| Gode Wind 1 and 2 | 2017 | 0.002 | 0.393 | 0.677 | 1.029 | 0.144 | 0.293 | 7.609 | 133.325 | operationally_plausible_variability |
| Deutsche Bucht | 2021 | 0.003 | 0.300 | 0.541 | 0.782 | 0.113 | 0.226 | 5.962 | 139.225 | operationally_plausible_variability |
| Dudgeon | 2017 | 0.008 | 0.530 | 0.845 | 1.091 | 0.140 | 0.298 | 18.572 | 80.637 | operationally_plausible_variability |
| Trianel Windpark Borkum 1 | 2017 | 0.005 | 0.374 | 0.646 | 0.890 | 0.141 | 0.291 | 6.469 | 144.865 | operationally_plausible_variability |

## Acceptance

The batch is acceptable only if each processed farm-year has true non-null `uo/vo`, no fallback/synthetic provenance, hourly UTC cadence, populated source/depth/direction provenance, no duplicate farm-year-sample-timestamp keys, and event-scale bracketing suitable for the selected dwell windows.
