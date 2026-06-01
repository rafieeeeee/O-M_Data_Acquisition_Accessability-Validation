# NWS Current Batch v1 Dry-Run Report

## Research Design

This batch tests whether the NWS hourly true `uo/vo` method can scale across the accepted normal recommended farm-years while preserving existing validated partitions and excluding stress-test rows.

Selection scope: `all_normal_recommended`.
Batch interpretation: `mixed-year scale batch`. Selected years: 2010, 2011, 2012, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025.

## Pre-Run Checks

- Product: `NWSHELF_MULTIYEAR_PHY_004_009` / `cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i`
- Selected farm-years: 125
- Existing processed farm-years in selected set: 10
- Farm-years remaining for extraction in selected set: 115
- Stress-test farm-years selected: 0
- Estimated current rows: 76,886,304
- Estimated processed size: 690.9 MB
- Output root free space: 1705684.8 MB
- Raw cache root: `Data/Raw/Metocean/CMEMS/NorthWestShelf/Currents/Pilots`
- Raw cache root guard: under `Data/Raw/Metocean` and separate from legacy CMEMS CSV cache
- Copernicus tooling import available: True

## Selected Farm-Years

| selected_rank | wind_farm | year | dwell_count | tier_a_dwell_count | sample_point_count | estimated_current_rows | processed_exists | raw_cache_exists |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | Horns Rev II | 2024 | 982 | 349 | 92 | 808128 | True | True |
| 2 | Horns Rev III | 2024 | 823 | 349 | 50 | 439200 | True | True |
| 3 | Butendiek | 2024 | 373 | 315 | 81 | 711504 | True | True |
| 4 | Dan Tysk | 2024 | 499 | 288 | 81 | 711504 | True | True |
| 5 | Meerwind Sued/Ost | 2024 | 720 | 282 | 81 | 711504 | True | True |
| 6 | Amrumbank West | 2024 | 622 | 276 | 81 | 711504 | True | True |
| 7 | Nordsee Ost | 2024 | 981 | 225 | 49 | 430416 | True | True |
| 8 | Gode Wind 1 and 2 | 2024 | 764 | 223 | 98 | 860832 | True | True |
| 9 | Sandbank | 2024 | 256 | 199 | 73 | 641232 | True | True |
| 10 | Vesterhav Syd | 2024 | 319 | 194 | 21 | 184464 | True | True |
| 11 | Merkur Offshore | 2024 | 798 | 183 | 67 | 588528 | False | False |
| 12 | Vesterhav Nord | 2024 | 274 | 172 | 22 | 193248 | False | False |
| 13 | Kaskasi | 2024 | 718 | 171 | 39 | 342576 | False | False |
| 14 | Borkum Riffgrund 1 | 2024 | 755 | 142 | 79 | 693936 | False | False |
| 15 | Trianel Windpark Borkum 2 | 2024 | 574 | 124 | 33 | 289872 | False | False |
| 16 | Nordsee One | 2024 | 330 | 111 | 55 | 483120 | False | False |
| 17 | Global Tech I | 2024 | 263 | 96 | 81 | 711504 | False | False |
| 18 | Borkum Riffgrund 2 | 2024 | 722 | 82 | 57 | 500688 | False | False |
| 19 | Trianel Windpark Borkum 1 | 2024 | 723 | 76 | 41 | 360144 | False | False |
| 20 | EnBW Hohe See | 2024 | 282 | 73 | 72 | 632448 | False | False |
| 21 | Bard Offshore 1 | 2024 | 167 | 70 | 81 | 711504 | False | False |
| 22 | Horns Rev III | 2019 | 73 | 58 | 50 | 438000 | False | False |
| 23 | Riffgat | 2024 | 72 | 44 | 31 | 272304 | False | False |
| 24 | Albatros | 2024 | 275 | 43 | 17 | 149328 | False | False |
| 25 | Alpha Ventus | 2024 | 656 | 41 | 13 | 114192 | False | False |
| 26 | Veja Mate | 2024 | 303 | 41 | 68 | 597312 | False | False |
| 27 | Deutsche Bucht | 2024 | 144 | 38 | 32 | 281088 | False | False |
| 28 | Butendiek | 2017 | 35 | 32 | 81 | 709560 | False | False |
| 29 | Walney 2 | 2012 | 70 | 31 | 52 | 456768 | False | False |
| 30 | Gemini | 2024 | 88 | 25 | 151 | 1326384 | False | False |
| 31 | Dan Tysk | 2021 | 34 | 25 | 81 | 709560 | False | False |
| 32 | Thanet | 2012 | 30 | 25 | 101 | 887184 | False | False |
| 33 | Horns Rev II | 2011 | 31 | 24 | 92 | 805920 | False | False |
| 34 | Ormonde | 2012 | 47 | 23 | 31 | 272304 | False | False |
| 35 | Sandbank | 2017 | 28 | 23 | 73 | 639480 | False | False |
| 36 | Butendiek | 2018 | 28 | 23 | 81 | 709560 | False | False |
| 37 | Thornton Bank - phase I | 2012 | 77 | 21 | 7 | 61488 | False | False |
| 38 | Horns Rev III | 2022 | 49 | 20 | 50 | 438000 | False | False |
| 39 | Butendiek | 2020 | 21 | 20 | 81 | 711504 | False | False |
| 40 | Walney 1 | 2012 | 69 | 19 | 52 | 456768 | False | False |
| 41 | Meerwind Sued/Ost | 2021 | 50 | 19 | 81 | 709560 | False | False |
| 42 | Butendiek | 2019 | 27 | 18 | 81 | 709560 | False | False |
| 43 | Dan Tysk | 2017 | 24 | 18 | 81 | 709560 | False | False |
| 44 | Horns Rev II | 2010 | 29 | 18 | 92 | 805920 | False | False |
| 45 | Amrumbank West | 2022 | 81 | 17 | 81 | 709560 | False | False |
| 46 | Horns Rev II | 2019 | 67 | 17 | 92 | 805920 | False | False |
| 47 | EnBW Hohe See | 2019 | 51 | 17 | 72 | 630720 | False | False |
| 48 | Dan Tysk | 2019 | 23 | 17 | 81 | 709560 | False | False |
| 49 | Dan Tysk | 2018 | 23 | 17 | 81 | 709560 | False | False |
| 50 | Horns Rev II | 2012 | 23 | 17 | 92 | 808128 | False | False |
| 51 | Hornsea Project 1 | 2024 | 43 | 16 | 175 | 1537200 | False | False |
| 52 | Horns Rev II | 2022 | 29 | 16 | 92 | 805920 | False | False |
| 53 | Horns Rev II | 2017 | 22 | 16 | 92 | 805920 | False | False |
| 54 | Borkum Riffgrund 2 | 2018 | 61 | 15 | 57 | 499320 | False | False |
| 55 | Dan Tysk | 2020 | 26 | 15 | 81 | 711504 | False | False |
| 56 | Butendiek | 2021 | 21 | 15 | 81 | 709560 | False | False |
| 57 | Butendiek | 2022 | 19 | 15 | 81 | 709560 | False | False |
| 58 | Horns Rev II | 2018 | 47 | 14 | 92 | 805920 | False | False |
| 59 | Nordsee Ost | 2021 | 50 | 13 | 49 | 429240 | False | False |
| 60 | Meerwind Sued/Ost | 2022 | 44 | 13 | 81 | 709560 | False | False |
| 61 | Dan Tysk | 2022 | 25 | 13 | 81 | 709560 | False | False |
| 62 | Horns Rev III | 2020 | 23 | 13 | 50 | 439200 | False | False |
| 63 | Butendiek | 2023 | 18 | 13 | 81 | 709560 | False | False |
| 64 | Hornsea Project 2 | 2024 | 51 | 12 | 166 | 1458144 | False | False |
| 65 | Global Tech I | 2019 | 50 | 12 | 81 | 709560 | False | False |
| 66 | Meerwind Sued/Ost | 2023 | 27 | 12 | 81 | 709560 | False | False |
| 67 | Horns Rev III | 2021 | 30 | 12 | 50 | 438000 | False | False |
| 68 | Horns Rev II | 2021 | 17 | 12 | 92 | 805920 | False | False |
| 69 | Sandbank | 2022 | 15 | 12 | 73 | 639480 | False | False |
| 70 | Nordsee Ost | 2023 | 30 | 11 | 49 | 429240 | False | False |
| 71 | OWF Prinses Amalia | 2012 | 33 | 11 | 61 | 535824 | False | False |
| 72 | Horns Rev II | 2020 | 24 | 11 | 92 | 808128 | False | False |
| 73 | Nordsee Ost | 2022 | 100 | 10 | 49 | 429240 | False | False |
| 74 | Hollandse Kust Zuid | 2024 | 66 | 10 | 141 | 1238544 | False | False |
| 75 | Global Tech I | 2018 | 24 | 10 | 81 | 709560 | False | False |
| 76 | Amrumbank West | 2021 | 22 | 10 | 81 | 709560 | False | False |
| 77 | Horns Rev III | 2023 | 20 | 10 | 50 | 438000 | False | False |
| 78 | Sandbank | 2021 | 18 | 10 | 73 | 639480 | False | False |
| 79 | Horns Rev II | 2023 | 15 | 10 | 92 | 805920 | False | False |
| 80 | Sandbank | 2018 | 15 | 10 | 73 | 639480 | False | False |
| 81 | Global Tech I | 2017 | 14 | 9 | 81 | 709560 | False | False |
| 82 | Sandbank | 2023 | 11 | 9 | 73 | 639480 | False | False |
| 83 | Amrumbank West | 2018 | 12 | 9 | 81 | 709560 | False | False |
| 84 | Borkum Riffgrund 2 | 2021 | 33 | 8 | 57 | 499320 | False | False |
| 85 | Merkur Offshore | 2023 | 28 | 8 | 67 | 586920 | False | False |
| 86 | Triton Knoll | 2024 | 27 | 8 | 91 | 799344 | False | False |
| 87 | Merkur Offshore | 2021 | 30 | 7 | 67 | 586920 | False | False |
| 88 | Merkur Offshore | 2022 | 25 | 7 | 67 | 586920 | False | False |
| 89 | Dan Tysk | 2023 | 18 | 7 | 81 | 709560 | False | False |
| 90 | OWF Egmond aan Zee | 2012 | 18 | 7 | 37 | 325008 | False | False |
| 91 | Nordsee One | 2022 | 13 | 7 | 55 | 481800 | False | False |
| 92 | Nordsee One | 2017 | 12 | 7 | 55 | 481800 | False | False |
| 93 | Sandbank | 2019 | 12 | 7 | 73 | 639480 | False | False |
| 94 | Greater Gabbard | 2017 | 18 | 6 | 141 | 1235160 | False | False |
| 95 | Meerwind Sued/Ost | 2018 | 23 | 6 | 81 | 709560 | False | False |
| 96 | Amrumbank West | 2023 | 16 | 6 | 81 | 709560 | False | False |
| 97 | Global Tech I | 2021 | 18 | 6 | 81 | 709560 | False | False |
| 98 | Kaskasi | 2023 | 14 | 6 | 39 | 341640 | False | False |
| 99 | Barrow | 2012 | 11 | 6 | 31 | 272304 | False | False |
| 100 | Nordsee One | 2018 | 10 | 6 | 55 | 481800 | False | False |
| 101 | Trianel Windpark Borkum 1 | 2018 | 55 | 5 | 41 | 359160 | False | False |
| 102 | Nordsee Ost | 2018 | 34 | 5 | 49 | 429240 | False | False |
| 103 | Meerwind Sued/Ost | 2017 | 12 | 5 | 81 | 709560 | False | False |
| 104 | Bard Offshore 1 | 2021 | 13 | 5 | 81 | 709560 | False | False |
| 105 | Bard Offshore 1 | 2017 | 10 | 5 | 81 | 709560 | False | False |
| 106 | EnBW Hohe See | 2021 | 10 | 5 | 72 | 630720 | False | False |
| 107 | Hywind Scotland Pilot Park | 2024 | 11 | 5 | 6 | 52704 | False | False |
| 108 | Bard Offshore 1 | 2018 | 15 | 4 | 81 | 709560 | False | False |
| 109 | Meerwind Sued/Ost | 2020 | 12 | 4 | 81 | 711504 | False | False |
| 110 | Global Tech I | 2022 | 11 | 4 | 81 | 709560 | False | False |
| 111 | Gode Wind 1 and 2 | 2023 | 10 | 4 | 98 | 858480 | False | False |
| 112 | Borkum Riffgrund 1 | 2018 | 51 | 3 | 79 | 692040 | False | False |
| 113 | Gode Wind 3 | 2025 | 56 | 3 | 24 | 210240 | False | False |
| 114 | Veja Mate | 2021 | 24 | 3 | 68 | 595680 | False | False |
| 115 | Veja Mate | 2017 | 20 | 3 | 68 | 595680 | False | False |
| 116 | Trianel Windpark Borkum 1 | 2021 | 24 | 3 | 41 | 359160 | False | False |
| 117 | Alpha Ventus | 2011 | 17 | 3 | 13 | 113880 | False | False |
| 118 | Nordsee Ost | 2020 | 15 | 3 | 49 | 430416 | False | False |
| 119 | EnBW Hohe See | 2020 | 14 | 3 | 72 | 632448 | False | False |
| 120 | Global Tech I | 2020 | 14 | 3 | 81 | 711504 | False | False |
| 121 | Gode Wind 1 and 2 | 2022 | 11 | 3 | 98 | 858480 | False | False |
| 122 | Gode Wind 1 and 2 | 2017 | 11 | 3 | 98 | 858480 | False | False |
| 123 | Deutsche Bucht | 2021 | 12 | 3 | 32 | 280320 | False | False |
| 124 | Dudgeon | 2017 | 13 | 3 | 68 | 595680 | False | False |
| 125 | Trianel Windpark Borkum 1 | 2017 | 11 | 3 | 41 | 359160 | False | False |

## Guardrails

- Dry-run only; no current download or processed archive write was performed.
- No stress-test farms are selected.
- Baltic and global currents are out of scope.
- Legacy CMEMS current CSVs and fallback/synthetic currents remain banned.
