# Bathymetry Assignment Full Report

Status: completed bathymetry-only source acquisition and static point assignment. No currents, FINO bulk data, source fusion, 10-minute interpolation, NORA3 reruns, or final dwell-metocean rebuilds were run.

## Executive Conclusion

- accepted_candidate: `True`
- row_count: `6642`
- farm_count: `119`
- missing_depth_count: `0`
- duplicate_wind_farm_sample_point_count: `0`
- fallback_row_count: `0`

## Command

`scripts/assign_bathymetry_to_metocean_points.py --requirements analysis/06_rq6_metocean_spatial_resolution/common_metocean_farm_requirements.csv --source-root Data/Raw/Metocean/Bathymetry --output-dir Data/Processed/metocean/bathymetry --qa-report analysis/06_rq6_metocean_spatial_resolution/bathymetry_assignment_full_report.md --limit-scope common-requirements --primary-source emodnet --fallback-source gebco_2026 --no-overwrite`

## Source Access And Cache

- primary_source: `emodnet`
- primary_version: `EMODnet DTM 2024`
- primary_endpoint: `https://rest.emodnet-bathymetry.eu/depth_sample`
- fallback_source: `gebco_2026`
- fallback_status: `not_fetched_no_emodnet_gaps`
- source_cache_path: `Data/Raw/Metocean/Bathymetry/emodnet_depth_samples/common_requirements_depth_samples.jsonl`
- source_metadata_path: `Data/Raw/Metocean/Bathymetry/emodnet_depth_samples/emodnet_depth_samples_metadata.json`
- raw_source_artifact_type: `EMODnet REST depth_sample JSONL cache`
- raster_tile_note: no raster tiles were downloaded; the official EMODnet point-sample service was used for this fixed-point assignment.

## Output Paths

- processed_output_path: `Data/Processed/metocean/bathymetry/site_bathymetry_points.parquet`
- processed_metadata_path: `Data/Processed/metocean/bathymetry/bathymetry_source_metadata.json`
- qa_report_path: `analysis/06_rq6_metocean_spatial_resolution/bathymetry_assignment_full_report.md`

## Preflight

- requirements_path: `analysis/06_rq6_metocean_spatial_resolution/common_metocean_farm_requirements.csv`
- turbine_coordinates_path: `/Volumes/Extreme SSD/01_ACTIVE_PROJECTS/O&M_Data_Acquisition_Accessability&Validation/Data/Interim/European_Turbine_Coordinates.csv`
- output_existed_before_run: `False`
- no_overwrite: `True`
- disk_free_before: `1.6 TB`
- disk_free_after: `1.6 TB`
- source_access_preflight_status: `ok`

## Schema

- `wind_farm`
- `sample_point_id`
- `sample_point_type`
- `lat`
- `lon`
- `water_depth_m`
- `bathymetry_source`
- `bathymetry_version`
- `bathymetry_grid_lat`
- `bathymetry_grid_lon`
- `bathymetry_distance_m`
- `bathymetry_assignment_method`
- `depth_sign_convention`
- `bathymetry_vertical_datum`
- `bathymetry_spatial_match_status`

## Validation

- missing_depth_rate: `0.0`
- provenance_populated_where_depth_exists: `True`
- positive_down_sign_convention: `True`
- vertical_datum_recorded: `True`
- crs_recorded: `True`
- distance_populated: `True`
- plausible_depths: `True`
- status_counts: `{'ok': 6642}`
- source_counts: `{'emodnet': 6642}`

## Depth Summary

- min_depth_m: `0.0`
- median_depth_m: `24.252499999999998`
- mean_depth_m: `25.30524622970274`
- p95_depth_m: `45.089328`
- max_depth_m: `206.95432`
- zero_depth_count: `5`
- shallow_depth_le_1m_count: `25`

## Shallow-Depth Review

- depth_le_1m_count: `25`
- zero_depth_count: `5`
- zero-depth rows are all at `Frederikshavn Offshore` sample points in the EMODnet REST response cache.
- additional <=1m rows occur at `Renland`, `London Array`, `Gunfleet Sands`, `Avedøre Holme`, and `Scroby Sands`.
- interpretation: keep these rows because EMODnet returned valid cells and provenance is populated, but review shallow/coastal farm coordinates before using bathymetry as a hard modelling covariate.

## Assignment Distance Summary

- min_distance_m: `1.0641859855990417`
- median_distance_m: `35.985191902747324`
- max_distance_m: `66.80726182360314`

## Region Summary

| region                  |   sample_points |   missing_depths |   min_depth_m |   median_depth_m |   mean_depth_m |   p95_depth_m |   max_depth_m |
|:------------------------|----------------:|-----------------:|--------------:|-----------------:|---------------:|--------------:|--------------:|
| Baltic / Belt Seas      |             772 |                0 |         0     |           17.214 |         21.389 |        43.568 |        45.461 |
| Channel / Atlantic edge |             333 |                0 |        12.952 |           27.629 |         27.407 |        37.941 |        40.17  |
| European shelf          |            1659 |                0 |         0.41  |           20.3   |         21.511 |        40.663 |        46.56  |
| North Sea / Skagerrak   |            1637 |                0 |         0.001 |           27.18  |         26.989 |        39.702 |       206.954 |
| Southern North Sea      |            1090 |                0 |        11.578 |           24.801 |         26.085 |        34.53  |        37.578 |
| UK shelf / Irish Sea    |            1151 |                0 |         1.26  |           24.03  |         29.659 |        54.035 |       113.33  |

## Farm Summary

| wind_farm                            | region                  |   sample_points |   missing_depths |   min_depth_m |   median_depth_m |   max_depth_m |
|:-------------------------------------|:------------------------|----------------:|-----------------:|--------------:|-----------------:|--------------:|
| Anholt                               | Baltic / Belt Seas      |             112 |                0 |        14.975 |           16.316 |        18.119 |
| Arcadis Ost 1                        | Baltic / Belt Seas      |              28 |                0 |        42.382 |           44.08  |        45.461 |
| Arkona-Becken Südost                 | Baltic / Belt Seas      |              61 |                0 |        22.543 |           26.718 |        37.003 |
| Avedøre Holme                        | Baltic / Belt Seas      |               4 |                0 |         0.74  |            1.483 |         1.498 |
| Baltic Eagle                         | Baltic / Belt Seas      |              51 |                0 |        40.033 |           43.237 |        44.673 |
| EnBW Windpark Baltic 1               | Baltic / Belt Seas      |              22 |                0 |        16.205 |           17.537 |        18.294 |
| EnBW Windpark Baltic 2               | Baltic / Belt Seas      |              81 |                0 |        22.364 |           35.797 |        43.577 |
| Frederikshavn Offshore               | Baltic / Belt Seas      |               5 |                0 |         0     |            0     |         0     |
| Kriegers Flak                        | Baltic / Belt Seas      |              73 |                0 |        16.538 |           22.916 |        30.559 |
| Lillgrund                            | Baltic / Belt Seas      |              49 |                0 |         3.24  |            6.53  |        10.54  |
| Middelgrunden                        | Baltic / Belt Seas      |              21 |                0 |         4.679 |            4.844 |         5.402 |
| Nysted                               | Baltic / Belt Seas      |              73 |                0 |         5.829 |            7.01  |         9.031 |
| Rodsand II                           | Baltic / Belt Seas      |              91 |                0 |         5.104 |            7.388 |        12.136 |
| Samsa                                | Baltic / Belt Seas      |              11 |                0 |        12.152 |           13.162 |        17.671 |
| Sprogo                               | Baltic / Belt Seas      |               8 |                0 |         6.636 |           11.013 |        16.283 |
| Tunm Knob                            | Baltic / Belt Seas      |              11 |                0 |         2.504 |            3.477 |         3.95  |
| Wikinger                             | Baltic / Belt Seas      |              71 |                0 |        35.682 |           38.743 |        41.155 |
| Fécamp                               | Channel / Atlantic edge |              72 |                0 |        26.168 |           28.876 |        31.908 |
| Rampion                              | Channel / Atlantic edge |             117 |                0 |        19.93  |           27.06  |        40.17  |
| Saint-Brieuc                         | Channel / Atlantic edge |              63 |                0 |        32.869 |           35.645 |        38.994 |
| Saint-Nazaire                        | Channel / Atlantic edge |              81 |                0 |        12.952 |           18.601 |        24.372 |
| Dudgeon                              | European shelf          |              68 |                0 |        16.92  |           20.67  |        25.87  |
| East Anglia One                      | European shelf          |             103 |                0 |        38.79  |           41.82  |        46.56  |
| Galloper                             | European shelf          |              57 |                0 |        25.1   |           31.56  |        36.28  |
| Greater Gabbard                      | European shelf          |             141 |                0 |        21.95  |           27.52  |        31.12  |
| Gunfleet Sands                       | European shelf          |              49 |                0 |         0.49  |            5.15  |        13.68  |
| Gunfleet Sands Demo                  | European shelf          |               3 |                0 |        11.37  |           12.03  |        12.7   |
| Hornsea Project 1                    | European shelf          |             175 |                0 |        23.7   |           30.21  |        36     |
| Hornsea Project 2                    | European shelf          |             166 |                0 |        27.61  |           35     |        41.55  |
| Humber Gateway                       | European shelf          |              74 |                0 |        12.1   |           14.87  |        16.49  |
| Inner Dowsing                        | European shelf          |              28 |                0 |         5.141 |            6.686 |         8.891 |
| Kentish Flats                        | European shelf          |              31 |                0 |         3.38  |            4.16  |         4.63  |
| Kentish Flats Extension              | European shelf          |              16 |                0 |         3.58  |            4.125 |         4.56  |
| Lincs                                | European shelf          |              76 |                0 |         6.853 |           13.755 |        21.195 |
| London Array                         | European shelf          |             176 |                0 |         0.41  |           11.74  |        24.57  |
| Lynn                                 | European shelf          |              28 |                0 |         7.58  |            9.52  |        11.37  |
| Race Bank                            | European shelf          |              92 |                0 |         8.91  |           16.255 |        21.13  |
| Scroby Sands                         | European shelf          |              31 |                0 |         0.88  |            4.2   |        13.89  |
| Sheringham Shoal                     | European shelf          |              89 |                0 |        14.62  |           18.44  |        21.68  |
| Teesside                             | European shelf          |              28 |                0 |         7.7   |           12.125 |        15.05  |
| Thanet                               | European shelf          |             101 |                0 |        15.34  |           21.36  |        27.49  |
| Triton Knoll                         | European shelf          |              91 |                0 |        14.81  |           17.54  |        20.8   |
| Westermost Rough                     | European shelf          |              36 |                0 |        14.06  |           20.21  |        27.51  |
| Albatros                             | North Sea / Skagerrak   |              17 |                0 |        40.07  |           40.27  |        40.46  |
| Alpha Ventus                         | North Sea / Skagerrak   |              13 |                0 |        27.21  |           28.5   |        29.8   |
| Amrumbank West                       | North Sea / Skagerrak   |              81 |                0 |        19.91  |           21.3   |        24.47  |
| Bard Offshore 1                      | North Sea / Skagerrak   |              81 |                0 |        38.65  |           39.6   |        40.84  |
| Borkum Riffgrund 1                   | North Sea / Skagerrak   |              79 |                0 |        23.14  |           25.4   |        28.4   |
| Borkum Riffgrund 2                   | North Sea / Skagerrak   |              57 |                0 |        25.44  |           27.53  |        29.13  |
| Butendiek                            | North Sea / Skagerrak   |              81 |                0 |        17.15  |           19.22  |        21.43  |
| Dan Tysk                             | North Sea / Skagerrak   |              81 |                0 |        21.05  |           25.18  |        31.27  |
| Deutsche Bucht                       | North Sea / Skagerrak   |              32 |                0 |        38.84  |           39.145 |        39.77  |
| EnBW Hohe See                        | North Sea / Skagerrak   |              72 |                0 |        38.94  |           39.385 |        40.16  |
| Global Tech I                        | North Sea / Skagerrak   |              81 |                0 |        38.75  |           39.42  |        40.07  |
| Gode Wind 1 and 2                    | North Sea / Skagerrak   |              98 |                0 |        28.73  |           32.685 |        34.32  |
| Gode Wind 3                          | North Sea / Skagerrak   |              24 |                0 |        29.36  |           31.815 |        34.56  |
| Horns Rev I                          | North Sea / Skagerrak   |              81 |                0 |         6.104 |            8.486 |        12.908 |
| Horns Rev II                         | North Sea / Skagerrak   |              92 |                0 |         2.622 |           12.145 |        22.325 |
| Horns Rev III                        | North Sea / Skagerrak   |              50 |                0 |        11.198 |           15.629 |        18.278 |
| Kaskasi                              | North Sea / Skagerrak   |              39 |                0 |        19.93  |           23.77  |        25.77  |
| Meerwind Sued/Ost                    | North Sea / Skagerrak   |              81 |                0 |        22.56  |           23.99  |        26.52  |
| Merkur Offshore                      | North Sea / Skagerrak   |              67 |                0 |        27.44  |           30.45  |        33.49  |
| Nissum Bredning                      | North Sea / Skagerrak   |               5 |                0 |         1.113 |            1.513 |         2.934 |
| Nordergründe                         | North Sea / Skagerrak   |              19 |                0 |         2.99  |            9.38  |        10.5   |
| Nordsee One                          | North Sea / Skagerrak   |              55 |                0 |        26.65  |           27.99  |        29.06  |
| Nordsee Ost                          | North Sea / Skagerrak   |              49 |                0 |        21.89  |           23.82  |        25.8   |
| Renland                              | North Sea / Skagerrak   |               9 |                0 |         0.001 |            0.16  |         0.549 |
| Riffgat                              | North Sea / Skagerrak   |              31 |                0 |        18.62  |           20.88  |        23.41  |
| Sandbank                             | North Sea / Skagerrak   |              73 |                0 |        24.55  |           28.25  |        33.38  |
| TetraSpar Demonstrator - Metcentre   | North Sea / Skagerrak   |               2 |                0 |       205.151 |          205.151 |       205.151 |
| Trianel Windpark Borkum 1            | North Sea / Skagerrak   |              41 |                0 |        27.09  |           30.23  |        32.89  |
| Trianel Windpark Borkum 2            | North Sea / Skagerrak   |              33 |                0 |        27.57  |           30.49  |        33.03  |
| UNITECH Zefyros by Hywind Technology | North Sea / Skagerrak   |               2 |                0 |       206.954 |          206.954 |       206.954 |
| Veja Mate                            | North Sea / Skagerrak   |              68 |                0 |        38.26  |           39.26  |        39.91  |
| Vesterhav Nord                       | North Sea / Skagerrak   |              22 |                0 |        20.012 |           22.276 |        24.613 |
| Vesterhav Syd                        | North Sea / Skagerrak   |              21 |                0 |        18.94  |           21.896 |        24.655 |
| Belwind phase 1                      | Southern North Sea      |              56 |                0 |        13.892 |           22.202 |        28.727 |
| Belwind phase 2                      | Southern North Sea      |               2 |                0 |        32.407 |           32.407 |        32.407 |
| Borssele Kavel I and II              | Southern North Sea      |              95 |                0 |        17.708 |           29.967 |        36.94  |
| Borssele Kavel III and IV            | Southern North Sea      |              78 |                0 |        20.152 |           30.18  |        34.484 |
| Borssele Kavel V                     | Southern North Sea      |               3 |                0 |        29.961 |           33.62  |        33.723 |
| Gemini                               | Southern North Sea      |             151 |                0 |        28.602 |           33.1   |        36.057 |
| Hollandse Kust Noord                 | Southern North Sea      |              70 |                0 |        19.058 |           23.748 |        25.899 |
| Hollandse Kust Zuid                  | Southern North Sea      |             141 |                0 |        18.528 |           22.155 |        25.605 |
| Mermaid                              | Southern North Sea      |              29 |                0 |        29.046 |           33.324 |        35.812 |
| Nobelwind                            | Southern North Sea      |              51 |                0 |        19.818 |           32.144 |        35.532 |
| Norther                              | Southern North Sea      |              45 |                0 |        17.143 |           24.641 |        31.074 |
| Northwester 2                        | Southern North Sea      |              24 |                0 |        31.33  |           33.484 |        37.578 |
| Northwind                            | Southern North Sea      |              73 |                0 |        17.086 |           20.635 |        27.788 |
| OWF Egmond aan Zee                   | Southern North Sea      |              37 |                0 |        15.303 |           17.581 |        19.518 |
| OWF Luchterduinen                    | Southern North Sea      |              44 |                0 |        18.845 |           20.816 |        22.313 |
| OWF Prinses Amalia                   | Southern North Sea      |              61 |                0 |        19.561 |           21.955 |        24.6   |
| Rentel                               | Southern North Sea      |              43 |                0 |        24.653 |           30.206 |        35.037 |
| SeaStar                              | Southern North Sea      |              31 |                0 |        26.753 |           30.392 |        34.149 |
| Thornton Bank - phase I              | Southern North Sea      |               7 |                0 |        16.789 |           19.553 |        20.662 |
| Thornton Bank - phase II and III     | Southern North Sea      |              49 |                0 |        11.578 |           19.53  |        25.68  |
| Aberdeen Offshore Wind Farm          | UK shelf / Irish Sea    |              12 |                0 |        21.352 |           25.75  |        29.941 |
| Barrow                               | UK shelf / Irish Sea    |              31 |                0 |        12.98  |           15.82  |        17.98  |
| Beatrice Offshore Wind Farm          | UK shelf / Irish Sea    |              85 |                0 |        37.18  |           44.1   |        54.32  |
| Blyth Demo Phase 1                   | UK shelf / Irish Sea    |               6 |                0 |        36.36  |           38.925 |        39.26  |
| Burbo Bank                           | UK shelf / Irish Sea    |              26 |                0 |         3.84  |            5.105 |         6.293 |
| Burbo Bank Extension                 | UK shelf / Irish Sea    |              33 |                0 |         4.54  |            9.254 |        13.989 |
| Gwynt y Mor                          | UK shelf / Irish Sea    |             161 |                0 |        13.35  |           18.75  |        28.11  |
| Hywind Scotland Pilot Park           | UK shelf / Irish Sea    |               6 |                0 |       102.58  |          106.87  |       113.33  |
| Kincardine                           | UK shelf / Irish Sea    |               6 |                0 |        64.49  |           69.63  |        74.37  |
| Methil Demo                          | UK shelf / Irish Sea    |               2 |                0 |         1.825 |            1.825 |         1.825 |
| Moray East                           | UK shelf / Irish Sea    |             101 |                0 |        39.04  |           47.78  |        53.32  |
| Moray West                           | UK shelf / Irish Sea    |              61 |                0 |        36.15  |           45.07  |        48.59  |
| Neart na Gaoithe                     | UK shelf / Irish Sea    |              55 |                0 |        44     |           50.93  |        54.52  |
| North Hoyle                          | UK shelf / Irish Sea    |              31 |                0 |         7.49  |            9.62  |        11.45  |
| Ormonde                              | UK shelf / Irish Sea    |              31 |                0 |        17.35  |           19.49  |        21.14  |
| Rhyl Flats                           | UK shelf / Irish Sea    |              26 |                0 |         6.28  |            9.155 |        12.52  |
| Robin Rigg West                      | UK shelf / Irish Sea    |              61 |                0 |         1.26  |            4.953 |         9.062 |
| Seagreen                             | UK shelf / Irish Sea    |             115 |                0 |        42.53  |           52.72  |        58.86  |
| Walney 1                             | UK shelf / Irish Sea    |              52 |                0 |        19.12  |           21.42  |        23.82  |
| Walney 2                             | UK shelf / Irish Sea    |              52 |                0 |        22.75  |           25.515 |        29.73  |
| Walney Extension 3                   | UK shelf / Irish Sea    |              41 |                0 |        21.08  |           24.79  |        30.88  |
| Walney Extension 4                   | UK shelf / Irish Sea    |              48 |                0 |        23     |           28.66  |        37.01  |
| West of Duddon Sands                 | UK shelf / Irish Sea    |             109 |                0 |        17.54  |           19.67  |        23     |

## Spot Checks Against Raw Cache

| wind_farm                        | sample_point_id   |   raw_avg |   processed_water_depth_m |   absolute_difference_m | status   |
|:---------------------------------|:------------------|----------:|--------------------------:|------------------------:|:---------|
| Aberdeen Offshore Wind Farm      | farm_centroid     | -25.7617  |                  25.7617  |                       0 | ok       |
| Baltic Eagle                     | turbine_0020      | -44.0305  |                  44.0305  |                       0 | ok       |
| Borkum Riffgrund 1               | turbine_0058      | -23.74    |                  23.74    |                       0 | ok       |
| Butendiek                        | turbine_0036      | -19.95    |                  19.95    |                       0 | ok       |
| EnBW Hohe See                    | turbine_0021      | -39.04    |                  39.04    |                       0 | ok       |
| Gemini                           | turbine_0061      | -28.942   |                  28.942   |                       0 | ok       |
| Greater Gabbard                  | turbine_0057      | -26.96    |                  26.96    |                       0 | ok       |
| Hollandse Kust Noord             | turbine_0052      | -22.844   |                  22.844   |                       0 | ok       |
| Horns Rev III                    | turbine_0018      | -17.3107  |                  17.3107  |                       0 | ok       |
| Hornsea Project 2                | turbine_0142      | -32       |                  32       |                       0 | ok       |
| Lincs                            | turbine_0004      | -15.332   |                  15.332   |                       0 | ok       |
| Meerwind Sued/Ost                | turbine_0073      | -26.14    |                  26.14    |                       0 | ok       |
| Nobelwind                        | turbine_0001      | -34.573   |                  34.573   |                       0 | ok       |
| Nysted                           | turbine_0003      |  -7.11221 |                   7.11221 |                       0 | ok       |
| Rampion                          | turbine_0015      | -31.7     |                  31.7     |                       0 | ok       |
| Rodsand II                       | turbine_0077      |  -8.51749 |                   8.51749 |                       0 | ok       |
| Seagreen                         | turbine_0046      | -52.93    |                  52.93    |                       0 | ok       |
| Thornton Bank - phase II and III | turbine_0045      | -15.7174  |                  15.7174  |                       0 | ok       |
| Walney 2                         | turbine_0005      | -22.78    |                  22.78    |                       0 | ok       |
| Wikinger                         | turbine_0069      | -38.3602  |                  38.3602  |                       0 | ok       |

## Source Reference Identifiers

- top_source_references: `{'DDM2024_50m': 582, 'db1': 565, 'db3': 467, 'GERMANY-BALTIC-SEA-DTM': 311, 'db2': 271, '113410': 242, '110357': 213, 'GEBCO2024': 197, '110368': 143, 'RVO_Hollandse_Kust_Zuid_Windfarm_2016': 141, '120008': 127, 'HY07416-2-Mean-Totaal': 115, '113464': 110, '2243_220506_007_WMP_MB_400': 109, '110079': 104, '110297': 101, '110284': 95, '122568': 91, 'RVO_Borssele_Windfarm_2015': 84, '110076': 84}`

## Wave Archive Immutability Check

- before: `{'Data/Processed/metocean/baltic_wave_timeseries': {'exists': True, 'file_count': 238, 'latest_mtime': '2026-05-29T11:36:55.219690561+00:00'}, 'Data/Processed/metocean/nws_wave_timeseries': {'exists': True, 'file_count': 1170, 'latest_mtime': '2026-05-27T16:27:11.418087006+00:00'}, 'Data/Processed/metocean/nora3_joined_cache': {'exists': True, 'file_count': 376, 'latest_mtime': '2026-05-29T10:01:57.960723162+00:00'}}`
- after: `{'Data/Processed/metocean/baltic_wave_timeseries': {'exists': True, 'file_count': 238, 'latest_mtime': '2026-05-29T11:36:55.219690561+00:00'}, 'Data/Processed/metocean/nws_wave_timeseries': {'exists': True, 'file_count': 1170, 'latest_mtime': '2026-05-27T16:27:11.418087006+00:00'}, 'Data/Processed/metocean/nora3_joined_cache': {'exists': True, 'file_count': 376, 'latest_mtime': '2026-05-29T10:01:57.960723162+00:00'}}`
- unchanged: `True`

## Guardrails

- No current downloads were run.
- No FINO bulk data ingestion was run.
- No source fusion or preferred-source variables were created.
- No final dwell-metocean feature table was rebuilt.
- No 10-minute interpolation was performed.
- No NORA3 extraction or consolidation was run.
- Existing Baltic, NWS, and NORA3 wave archives were not modified by this script.

## Notes And Caveats

- EMODnet is the only source used where depths are present. GEBCO_2026 remains the documented fallback/cross-check source, but it was not fetched because EMODnet returned depths for the assignment points.
- Processed depths are positive-down metres. EMODnet REST raw `avg` values observed in the cache are negative below datum and are sign-converted in the processed table.
- The assignment uses EMODnet REST grid-cell depth samples rather than local raster bilinear interpolation; this avoids acquiring large rasters for a fixed-point static site-context table.
