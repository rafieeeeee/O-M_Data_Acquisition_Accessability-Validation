# FINO Metadata and Access Planning Dry-Run

Status: dry-run planning only. No FINO bulk download, FINO time-series import, current download, source fusion, 10-minute interpolation, NORA3 rerun, or final dwell-metocean rebuild was run.

## Executive Conclusion

FINO should be treated as in-situ validation and baseline evidence, not as an automatic farm-wide metocean source. The next safe step is human/credential preparation for BSH Insitu access, followed by a small FINO1 metadata and wave-slice pilot.

- station_count: `3`
- station_ids: `FINO1, FINO2, FINO3`
- project_sample_point_source: `bathymetry_site_bathymetry_points`
- project_sample_point_count: `6642`
- farm_count: `119`
- processed_fino_archive_exists: `False`
- project_role: `validation_baseline_not_farm_wide_primary_source`

## Local FINO Inventory

| asset                  | path                                                                                                               | exists   |   file_count | usability                         |
|:-----------------------|:-------------------------------------------------------------------------------------------------------------------|:---------|-------------:|:----------------------------------|
| FINO1 raw placeholder  | /Volumes/Extreme SSD/01_ACTIVE_PROJECTS/O&M_Data_Acquisition_Accessability&Validation/Data/Raw/Metocean/FINO1      | True     |            0 | empty_or_unverified_placeholder   |
| FINO2 raw placeholder  | /Volumes/Extreme SSD/01_ACTIVE_PROJECTS/O&M_Data_Acquisition_Accessability&Validation/Data/Raw/Metocean/FINO2      | False    |            0 | empty_or_unverified_placeholder   |
| FINO3 raw placeholder  | /Volumes/Extreme SSD/01_ACTIVE_PROJECTS/O&M_Data_Acquisition_Accessability&Validation/Data/Raw/Metocean/FINO3      | False    |            0 | empty_or_unverified_placeholder   |
| processed FINO archive | /Volumes/Extreme SSD/01_ACTIVE_PROJECTS/O&M_Data_Acquisition_Accessability&Validation/Data/Processed/metocean/fino | False    |            0 | missing_no_processed_fino_archive |

## Public Station Metadata Plan

| station_id   |     lat |      lon | available_start   | available_end                                                           | cadence                                                                                  | access_method                                                                                       |
|:-------------|--------:|---------:|:------------------|:------------------------------------------------------------------------|:-----------------------------------------------------------------------------------------|:----------------------------------------------------------------------------------------------------|
| FINO1        | 54.0149 |  6.58764 | 2004-01           | ongoing or latest portal availability; verify per variable after access | 10-minute expected for core wind/wave observations where available; confirm per variable | BSH-Login registration, request Insitu specialist procedure, export selected station-variable files |
| FINO2        | 55.0069 | 13.1542  | 2007-08           | ongoing or latest portal availability; verify per variable after access | 10-minute likely for core meteorology; confirm per variable                              | BSH-Login registration, request Insitu specialist procedure, export selected station-variable files |
| FINO3        | 55.195  |  7.15833 | 2009-09           | ongoing or latest portal availability; verify per variable after access | 10-minute likely for core meteorology/wave observations; confirm per variable            | BSH-Login registration, request Insitu specialist procedure, export selected station-variable files |

## Likely Variables and Measurement Metadata

| station_id   | wave_variables                                                                            | wind_variables                                                                     | current_variables                                                                                  | measurement_heights_depths                                                                |
|:-------------|:------------------------------------------------------------------------------------------|:-----------------------------------------------------------------------------------|:---------------------------------------------------------------------------------------------------|:------------------------------------------------------------------------------------------|
| FINO1        | significant wave height, wave period, wave direction                                      | wind speed and wind direction at mast heights; exact heights require portal export | current speed and direction at several depths reported for hydrography; verify portal availability | wind mast heights and hydrographic depths to be preserved from portal metadata            |
| FINO2        | seastate variables likely available through hydrographic monitoring; verify portal fields | wind speed and direction plus air temperature, pressure, humidity, radiation       | current data described by public oceanography page; verify u/v or speed/direction fields in portal | mast table includes wind speeds at 32.4-102.5 m MSL and wind direction at 31.8-91.8 m MSL |
| FINO3        | wave height and likely wave period/direction in portal; verify variable names             | wind speed and wind direction at mast heights; exact heights require portal export | oceanographic variables likely available; verify current fields and depths in portal               | station-specific mast and hydrographic depths to be preserved from portal metadata        |

## Access Requirements

- Register for BSH-Login.
- Request the `Insitu` specialist procedure.
- After approval, export only selected station-variable windows for the pilot.
- Preserve native portal files and QC/source metadata before normalization.
- Confirm exact licence and source acknowledgement terms after portal access.

## Station-to-Farm Matching

Distances are computed from FINO station coordinates to accepted common metocean sample points. Match roles are planning labels only and do not authorize farm-wide extrapolation.

- nearby_candidate_counts_within_25km: `{'FINO1': 8, 'FINO2': 2, 'FINO3': 2}`

### Closest Candidate Matches

| station_id   | wind_farm                 |   distance_km | nearest_sample_point_id   |   farm_centroid_distance_km | match_role                   |
|:-------------|:--------------------------|--------------:|:--------------------------|----------------------------:|:-----------------------------|
| FINO1        | Alpha Ventus              |         0.407 | turbine_0008              |                       1.308 | direct_validation_candidate  |
| FINO1        | Merkur Offshore           |         1.312 | turbine_0059              |                       3.878 | direct_validation_candidate  |
| FINO1        | Borkum Riffgrund 1        |         2.905 | turbine_0030              |                       5.718 | direct_validation_candidate  |
| FINO1        | Borkum Riffgrund 2        |         3.05  | turbine_0020              |                       9.269 | direct_validation_candidate  |
| FINO1        | Trianel Windpark Borkum 1 |         6.465 | turbine_0012              |                       9.148 | nearby_validation_candidate  |
| FINO1        | Trianel Windpark Borkum 2 |         6.767 | turbine_0026              |                       9.019 | nearby_validation_candidate  |
| FINO1        | Nordsee One               |        10.71  | turbine_0017              |                      15.433 | nearby_validation_candidate  |
| FINO1        | Gode Wind 1 and 2         |        23.203 | turbine_0085              |                      26.654 | nearby_validation_candidate  |
| FINO1        | Gode Wind 3               |        31.103 | turbine_0015              |                      33.913 | regional_benchmark_candidate |
| FINO1        | Gemini                    |        32.284 | turbine_0060              |                      40.856 | regional_benchmark_candidate |
| FINO1        | Riffgat                   |        35.071 | turbine_0000              |                      36.603 | regional_benchmark_candidate |
| FINO1        | EnBW Hohe See             |        44.043 | turbine_0000              |                      50.013 | regional_benchmark_candidate |
| FINO1        | Bard Offshore 1           |        49.314 | turbine_0062              |                      54.75  | regional_benchmark_candidate |
| FINO1        | Global Tech I             |        51.307 | turbine_0035              |                      56.761 | regional_benchmark_candidate |
| FINO1        | Veja Mate                 |        52.764 | turbine_0046              |                      57.392 | regional_benchmark_candidate |
| FINO1        | Albatros                  |        54.817 | turbine_0008              |                      57.074 | regional_benchmark_candidate |
| FINO1        | Deutsche Bucht            |        58.55  | turbine_0007              |                      61.109 | regional_benchmark_candidate |
| FINO2        | EnBW Windpark Baltic 2    |         0.237 | turbine_0020              |                       2.768 | direct_validation_candidate  |
| FINO2        | Kriegers Flak             |         2.846 | turbine_0017              |                      14.071 | direct_validation_candidate  |
| FINO2        | Arcadis Ost 1             |        32.849 | turbine_0018              |                      38.39  | regional_benchmark_candidate |
| FINO2        | Baltic Eagle              |        44.89  | turbine_0046              |                      49.136 | regional_benchmark_candidate |
| FINO2        | EnBW Windpark Baltic 1    |        51.257 | turbine_0015              |                      54.729 | regional_benchmark_candidate |
| FINO2        | Wikinger                  |        58.952 | turbine_0029              |                      61.391 | regional_benchmark_candidate |
| FINO2        | Lillgrund                 |        59.769 | turbine_0004              |                      60.987 | regional_benchmark_candidate |
| FINO2        | Arkona-Becken Südost      |        62.607 | turbine_0000              |                      66.743 | regional_benchmark_candidate |
| FINO3        | Dan Tysk                  |         1.115 | turbine_0032              |                       6.591 | direct_validation_candidate  |
| FINO3        | Sandbank                  |        17.262 | turbine_0053              |                      19.453 | nearby_validation_candidate  |
| FINO3        | Butendiek                 |        40.738 | turbine_0052              |                      43.908 | regional_benchmark_candidate |
| FINO3        | Horns Rev II              |        47.097 | turbine_0036              |                      52.449 | regional_benchmark_candidate |
| FINO3        | Horns Rev I               |        50.956 | turbine_0015              |                      53.936 | regional_benchmark_candidate |

### Nearest Ten Farms Per Station

#### FINO1

| station_id   | wind_farm                 |   distance_km | nearest_sample_point_id   |   farm_centroid_distance_km | match_role                   |
|:-------------|:--------------------------|--------------:|:--------------------------|----------------------------:|:-----------------------------|
| FINO1        | Alpha Ventus              |         0.407 | turbine_0008              |                       1.308 | direct_validation_candidate  |
| FINO1        | Merkur Offshore           |         1.312 | turbine_0059              |                       3.878 | direct_validation_candidate  |
| FINO1        | Borkum Riffgrund 1        |         2.905 | turbine_0030              |                       5.718 | direct_validation_candidate  |
| FINO1        | Borkum Riffgrund 2        |         3.05  | turbine_0020              |                       9.269 | direct_validation_candidate  |
| FINO1        | Trianel Windpark Borkum 1 |         6.465 | turbine_0012              |                       9.148 | nearby_validation_candidate  |
| FINO1        | Trianel Windpark Borkum 2 |         6.767 | turbine_0026              |                       9.019 | nearby_validation_candidate  |
| FINO1        | Nordsee One               |        10.71  | turbine_0017              |                      15.433 | nearby_validation_candidate  |
| FINO1        | Gode Wind 1 and 2         |        23.203 | turbine_0085              |                      26.654 | nearby_validation_candidate  |
| FINO1        | Gode Wind 3               |        31.103 | turbine_0015              |                      33.913 | regional_benchmark_candidate |
| FINO1        | Gemini                    |        32.284 | turbine_0060              |                      40.856 | regional_benchmark_candidate |

#### FINO2

| station_id   | wind_farm              |   distance_km | nearest_sample_point_id   |   farm_centroid_distance_km | match_role                   |
|:-------------|:-----------------------|--------------:|:--------------------------|----------------------------:|:-----------------------------|
| FINO2        | EnBW Windpark Baltic 2 |         0.237 | turbine_0020              |                       2.768 | direct_validation_candidate  |
| FINO2        | Kriegers Flak          |         2.846 | turbine_0017              |                      14.071 | direct_validation_candidate  |
| FINO2        | Arcadis Ost 1          |        32.849 | turbine_0018              |                      38.39  | regional_benchmark_candidate |
| FINO2        | Baltic Eagle           |        44.89  | turbine_0046              |                      49.136 | regional_benchmark_candidate |
| FINO2        | EnBW Windpark Baltic 1 |        51.257 | turbine_0015              |                      54.729 | regional_benchmark_candidate |
| FINO2        | Wikinger               |        58.952 | turbine_0029              |                      61.391 | regional_benchmark_candidate |
| FINO2        | Lillgrund              |        59.769 | turbine_0004              |                      60.987 | regional_benchmark_candidate |
| FINO2        | Arkona-Becken Südost   |        62.607 | turbine_0000              |                      66.743 | regional_benchmark_candidate |
| FINO2        | Avedøre Holme          |        79.12  | turbine_0000              |                      79.412 | context_only                 |
| FINO2        | Middelgrunden          |        80.479 | turbine_0003              |                      82.04  | context_only                 |

#### FINO3

| station_id   | wind_farm      |   distance_km | nearest_sample_point_id   |   farm_centroid_distance_km | match_role                   |
|:-------------|:---------------|--------------:|:--------------------------|----------------------------:|:-----------------------------|
| FINO3        | Dan Tysk       |         1.115 | turbine_0032              |                       6.591 | direct_validation_candidate  |
| FINO3        | Sandbank       |        17.262 | turbine_0053              |                      19.453 | nearby_validation_candidate  |
| FINO3        | Butendiek      |        40.738 | turbine_0052              |                      43.908 | regional_benchmark_candidate |
| FINO3        | Horns Rev II   |        47.097 | turbine_0036              |                      52.449 | regional_benchmark_candidate |
| FINO3        | Horns Rev I    |        50.956 | turbine_0015              |                      53.936 | regional_benchmark_candidate |
| FINO3        | Horns Rev III  |        59.258 | turbine_0041              |                      63.795 | regional_benchmark_candidate |
| FINO3        | Amrumbank West |        79.233 | turbine_0055              |                      82.542 | context_only                 |
| FINO3        | Kaskasi        |        83.289 | turbine_0037              |                      85.633 | context_only                 |
| FINO3        | Nordsee Ost    |        86.247 | turbine_0031              |                      89.829 | context_only                 |
| FINO3        | Global Tech I  |        86.885 | turbine_0033              |                      91.697 | context_only                 |

## Proposed Station Metadata Schema

- `station_id`
- `station_name`
- `lat`
- `lon`
- `operator`
- `source`
- `access_method`
- `available_start`
- `available_end`
- `variables_available`
- `cadence`
- `licence_note`
- `metadata_source`
- `metadata_review_status`

## Proposed Time-Series Schema For Later Pilot

- `station_id`
- `timestamp_utc`
- `variable`
- `value`
- `unit`
- `measurement_height_or_depth`
- `qc_flag`
- `source_file`
- `access_method`

## Proposed Station-Farm Match Schema

- `station_id`
- `station_name`
- `wind_farm`
- `farm_id`
- `distance_km`
- `nearest_sample_point_id`
- `nearest_sample_distance_km`
- `farm_centroid_distance_km`
- `match_role`
- `representativeness_note`

## Validation Gates For Later FINO Import

- Station coordinates are verified against public FINO pages and portal metadata.
- Timestamps are UTC-normalized and cadence is reported per station-variable.
- Units and measurement heights or depths are preserved.
- QC flags and source-file names are preserved.
- Variables are mapped to canonical names only after inspecting portal exports.
- Licence and source acknowledgement requirements are documented before import.
- FINO is used as validation or baseline evidence, not automatic farm-wide assignment.
- Station-to-farm representativeness notes accompany every comparison.
- Comparison metrics include MAE, RMSE, bias, correlation, and coverage.
- No source-fused metocean or final dwell table is written by FINO ingestion.

## Recommended First FINO Pilot

FINO1 station metadata plus one small wave time-series slice, then compare Hs/Tp/wave direction against nearby Alpha Ventus or German Bight NORA3/NWS/Baltic-unavailable source records where temporally overlapping.

Proposed future command, after access is granted and an importer exists:

```bash
/opt/anaconda3/bin/python scripts/import_fino_timeseries.py \
  --station FINO1 \
  --variables hs tp wave_direction \
  --start 2022-01-01 \
  --end 2022-01-31 \
  --raw-root Data/Raw/Metocean/FINO1 \
  --output-dir Data/Processed/metocean/fino \
  --dry-run
```

## Do-Not-Do List

- Do not bulk-download FINO time series before access and variable names are confirmed.
- Do not treat FINO as a farm-wide primary source without a distance rule.
- Do not overwrite accepted NORA3, NWS, Baltic, or bathymetry archives.
- Do not download currents or promote legacy CMEMS current CSVs from this task.
- Do not rebuild the final dwell-metocean feature table.
- Do not interpolate or source-fuse FINO observations in the metadata planner.

## Public Metadata Sources

- `fino_database`: https://www.fino2.de/en/fino2/fino-database.html
- `bsh_login`: https://login.bsh.de/fachverfahren/?lang=en
- `fino1_location`: https://www.fino1.de/en/location.html
- `fino1_hydrography`: https://www.fino1.de/en/research/project/hydrography.html
- `fino2_location`: https://www.fino2.de/en/fino2/location.html
- `fino2_meteorology`: https://www.fino2.de/en/research/meteorology.html
- `fino2_oceanography`: https://www.fino2.de/en/research/oceanography.html
- `fino3_position`: https://www.fino3.de/en/about/position.html

## Files Created Or Modified By This Dry-Run

- This report only.
