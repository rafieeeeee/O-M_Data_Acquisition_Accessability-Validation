# Baltic Processed Wave Archive Acceptance Report

## Executive Conclusion

- Accepted: `True`
- Scope: native-hourly Baltic Copernicus wave archive only; no current, source-fusion, 10-minute interpolation, NORA3, or final dwell-metocean rebuild work was performed.

## Command

```bash
/opt/anaconda3/bin/python scripts/materialize_baltic_wave_timeseries.py --raw-root Data/Raw/Metocean/CMEMS/BalticSea/Waves --output-dir Data/Processed/metocean/baltic_wave_timeseries --qa-report analysis/06_rq6_metocean_spatial_resolution/baltic_wave_materialization_qa_report.md
```

## Archive Summary

- output_root: `Data/Processed/metocean/baltic_wave_timeseries`
- farm_count: `16`
- partition_count: `238`
- row_count: `73866720`
- rows_written_this_run_from_qa_report: `73599936`
- Existing Arcadis pilot partitions: preserved/skipped by overwrite-safe default.
- Expected final scale: `16` farms, `238` partitions, `73,866,720` rows.

## Schema

- Contract column set match: `True`
- Exact prompt order match: `False`
- Actual order: `timestamp_utc, wind_farm, sample_point_id, sample_point_type, lat, lon, baltic_grid_lat, baltic_grid_lon, baltic_spatial_distance_km, baltic_source_file, baltic_extraction_method, baltic_wave_hs, baltic_wave_tp, baltic_wave_dir, baltic_wave_tm10, baltic_wave_tm02, baltic_spatial_match_status`

## Farm Summary

| Farm | Partitions | Rows | Sample points | Years | Time range |
| --- | ---: | ---: | ---: | --- | --- |
| Anholt | 12 | 11128320 | 112 | 2013-2024 | 2013-09-01T00:00:00+00:00 to 2024-12-31T23:00:00+00:00 |
| Arcadis Ost 1 | 2 | 266784 | 28 | 2023-2024 | 2023-12-01T00:00:00+00:00 to 2024-12-31T23:00:00+00:00 |
| Arkona-Becken Südost | 6 | 3209088 | 61 | 2019-2024 | 2019-01-01T00:00:00+00:00 to 2024-12-31T23:00:00+00:00 |
| Avedøre Holme | 14 | 458880 | 4 | 2011-2024 | 2011-12-01T00:00:00+00:00 to 2024-12-31T23:00:00+00:00 |
| EnBW Windpark Baltic 1 | 14 | 2636832 | 22 | 2011-2024 | 2011-05-01T00:00:00+00:00 to 2024-12-31T23:00:00+00:00 |
| EnBW Windpark Baltic 2 | 10 | 6570720 | 81 | 2015-2024 | 2015-10-01T00:00:00+00:00 to 2024-12-31T23:00:00+00:00 |
| Frederikshavn Offshore | 22 | 946200 | 5 | 2003-2024 | 2003-06-01T00:00:00+00:00 to 2024-12-31T23:00:00+00:00 |
| Kriegers Flak | 4 | 2133936 | 73 | 2021-2024 | 2021-09-01T00:00:00+00:00 to 2024-12-31T23:00:00+00:00 |
| Lillgrund | 18 | 7339416 | 49 | 2007-2024 | 2007-12-01T00:00:00+00:00 to 2024-12-31T23:00:00+00:00 |
| Middelgrunden | 24 | 4388328 | 21 | 2001-2024 | 2001-03-01T00:00:00+00:00 to 2024-12-31T23:00:00+00:00 |
| Nysted | 22 | 13493904 | 73 | 2003-2024 | 2003-12-01T00:00:00+00:00 to 2024-12-31T23:00:00+00:00 |
| Rodsand II | 15 | 11369904 | 91 | 2010-2024 | 2010-10-01T00:00:00+00:00 to 2024-12-31T23:00:00+00:00 |
| Samsa | 22 | 2113320 | 11 | 2003-2024 | 2003-02-01T00:00:00+00:00 to 2024-12-31T23:00:00+00:00 |
| Sprogo | 16 | 1057920 | 8 | 2009-2024 | 2009-12-01T00:00:00+00:00 to 2024-12-31T23:00:00+00:00 |
| Tunm Knob | 30 | 2861232 | 11 | 1995-2024 | 1995-05-01T00:00:00+00:00 to 2024-12-31T23:00:00+00:00 |
| Wikinger | 7 | 3891936 | 71 | 2018-2024 | 2018-10-01T00:00:00+00:00 to 2024-12-31T23:00:00+00:00 |

## Validation Checks

- hs_non_negative: `True`
- tp_positive_where_non_null: `True`
- tm10_positive_where_non_null: `True`
- tm02_positive_where_non_null: `True`
- direction_0_360: `True`
- timestamps_hourly_per_sample: `True`
- timestamp_utc_timezone_or_utc_dtype: `True`
- source_file_populated: `True`
- extraction_method_populated: `True`
- spatial_status_populated: `True`
- spatial_distance_populated: `True`
- duplicate wind_farm + sample_point_id + timestamp_utc rows: `0`
- raw NetCDF metadata unchanged: `True`
- raw-vs-processed comparison rows: `32`
- raw-vs-processed all match: `True`

## Physical Ranges

- baltic_wave_hs_min: `0.009946534410119057`
- baltic_wave_hs_max: `5.878973960876465`
- baltic_wave_tp_min: `0.9370400309562683`
- baltic_wave_tp_max: `16.350797653198242`
- baltic_wave_tm10_min: `0.8329220414161682`
- baltic_wave_tm10_max: `8.39478588104248`
- baltic_wave_tm02_min: `0.709149181842804`
- baltic_wave_tm02_max: `6.689032554626465`
- baltic_wave_dir_min: `0.00018310546875`
- baltic_wave_dir_max: `359.99993896484375`
- baltic_spatial_distance_km_min: `0.038095297808072436`
- baltic_spatial_distance_km_max: `1.3063670141895687`

## Missingness

- baltic_wave_hs: nulls=`145632`, non_nulls=`73721088`, null_rate=`0.0019715509230679258`
- baltic_wave_tp: nulls=`145632`, non_nulls=`73721088`, null_rate=`0.0019715509230679258`
- baltic_wave_dir: nulls=`145632`, non_nulls=`73721088`, null_rate=`0.0019715509230679258`
- baltic_wave_tm10: nulls=`145632`, non_nulls=`73721088`, null_rate=`0.0019715509230679258`
- baltic_wave_tm02: nulls=`145632`, non_nulls=`73721088`, null_rate=`0.0019715509230679258`

## Provenance

- spatial status counts: `{'ok': 73866720}`
- extraction method counts: `{'nearest_valid_grid_hourly_wave_only': 73866720}`
- source files represented: `16`

## Raw Comparison Samples

- `{'farm': 'Anholt', 'timestamp': '2013-09-01T00:00:00+00:00', 'sample_point_id': 'farm_centroid', 'source_file': 'cmems_mod_bal_wav_my_PT1H-i_multi-vars_10.90E-11.57E_56.26N-56.94N_2013-09-01-2024-12-31.nc', 'status': 'ok', 'max_abs_diff': 0.0}`
- `{'farm': 'Anholt', 'timestamp': '2024-01-01T00:00:00+00:00', 'sample_point_id': 'farm_centroid', 'source_file': 'cmems_mod_bal_wav_my_PT1H-i_multi-vars_10.90E-11.57E_56.26N-56.94N_2013-09-01-2024-12-31.nc', 'status': 'ok', 'max_abs_diff': 0.0}`
- `{'farm': 'Arcadis Ost 1', 'timestamp': '2023-12-01T00:00:00+00:00', 'sample_point_id': 'farm_centroid', 'source_file': 'cmems_mod_bal_wav_my_PT1H-i_multi-vars_13.35E-13.99E_54.54N-55.09N_2023-12-01-2024-12-31.nc', 'status': 'ok', 'max_abs_diff': 0.0}`
- `{'farm': 'Arcadis Ost 1', 'timestamp': '2024-01-01T00:00:00+00:00', 'sample_point_id': 'farm_centroid', 'source_file': 'cmems_mod_bal_wav_my_PT1H-i_multi-vars_13.35E-13.99E_54.54N-55.09N_2023-12-01-2024-12-31.nc', 'status': 'ok', 'max_abs_diff': 0.0}`
- `{'farm': 'Arkona-Becken Südost', 'timestamp': '2019-01-01T00:00:00+00:00', 'sample_point_id': 'farm_centroid', 'source_file': 'cmems_mod_bal_wav_my_PT1H-i_multi-vars_13.82E-14.40E_54.51N-55.06N_2019-01-01-2024-12-31.nc', 'status': 'ok', 'max_abs_diff': 0.0}`
- `{'farm': 'Arkona-Becken Südost', 'timestamp': '2024-01-01T00:00:00+00:00', 'sample_point_id': 'farm_centroid', 'source_file': 'cmems_mod_bal_wav_my_PT1H-i_multi-vars_13.82E-14.40E_54.51N-55.06N_2019-01-01-2024-12-31.nc', 'status': 'ok', 'max_abs_diff': 0.0}`
- `{'farm': 'Avedøre Holme', 'timestamp': '2011-12-01T00:00:00+00:00', 'sample_point_id': 'farm_centroid', 'source_file': 'cmems_mod_bal_wav_my_PT1H-i_multi-vars_12.21E-12.71E_55.36N-55.84N_2011-12-01-2024-12-31.nc', 'status': 'ok', 'max_abs_diff': 0.0}`
- `{'farm': 'Avedøre Holme', 'timestamp': '2024-01-01T00:00:00+00:00', 'sample_point_id': 'farm_centroid', 'source_file': 'cmems_mod_bal_wav_my_PT1H-i_multi-vars_12.21E-12.71E_55.36N-55.84N_2011-12-01-2024-12-31.nc', 'status': 'ok', 'max_abs_diff': 0.0}`
- `{'farm': 'EnBW Windpark Baltic 1', 'timestamp': '2011-05-01T00:00:00+00:00', 'sample_point_id': 'farm_centroid', 'source_file': 'cmems_mod_bal_wav_my_PT1H-i_multi-vars_12.40E-12.93E_54.34N-54.87N_2011-05-01-2024-12-31.nc', 'status': 'ok', 'max_abs_diff': 0.0}`
- `{'farm': 'EnBW Windpark Baltic 1', 'timestamp': '2024-01-01T00:00:00+00:00', 'sample_point_id': 'farm_centroid', 'source_file': 'cmems_mod_bal_wav_my_PT1H-i_multi-vars_12.40E-12.93E_54.34N-54.87N_2011-05-01-2024-12-31.nc', 'status': 'ok', 'max_abs_diff': 0.0}`
- `{'farm': 'EnBW Windpark Baltic 2', 'timestamp': '2015-10-01T00:00:00+00:00', 'sample_point_id': 'farm_centroid', 'source_file': 'cmems_mod_bal_wav_my_PT1H-i_multi-vars_12.85E-13.46E_54.71N-55.26N_2015-10-01-2024-12-31.nc', 'status': 'ok', 'max_abs_diff': 0.0}`
- `{'farm': 'EnBW Windpark Baltic 2', 'timestamp': '2024-01-01T00:00:00+00:00', 'sample_point_id': 'farm_centroid', 'source_file': 'cmems_mod_bal_wav_my_PT1H-i_multi-vars_12.85E-13.46E_54.71N-55.26N_2015-10-01-2024-12-31.nc', 'status': 'ok', 'max_abs_diff': 0.0}`
- `{'farm': 'Frederikshavn Offshore', 'timestamp': '2003-06-01T00:00:00+00:00', 'sample_point_id': 'farm_centroid', 'source_file': 'cmems_mod_bal_wav_my_PT1H-i_multi-vars_10.32E-10.79E_57.21N-57.69N_2003-06-01-2024-12-31.nc', 'status': 'ok', 'max_abs_diff': 0.0}`
- `{'farm': 'Frederikshavn Offshore', 'timestamp': '2024-01-01T00:00:00+00:00', 'sample_point_id': 'farm_centroid', 'source_file': 'cmems_mod_bal_wav_my_PT1H-i_multi-vars_10.32E-10.79E_57.21N-57.69N_2003-06-01-2024-12-31.nc', 'status': 'ok', 'max_abs_diff': 0.0}`
- `{'farm': 'Kriegers Flak', 'timestamp': '2021-09-01T00:00:00+00:00', 'sample_point_id': 'farm_centroid', 'source_file': 'cmems_mod_bal_wav_my_PT1H-i_multi-vars_12.51E-13.35E_54.72N-55.34N_2021-09-01-2024-12-31.nc', 'status': 'ok', 'max_abs_diff': 0.0}`
- `{'farm': 'Kriegers Flak', 'timestamp': '2024-01-01T00:00:00+00:00', 'sample_point_id': 'farm_centroid', 'source_file': 'cmems_mod_bal_wav_my_PT1H-i_multi-vars_12.51E-13.35E_54.72N-55.34N_2021-09-01-2024-12-31.nc', 'status': 'ok', 'max_abs_diff': 0.0}`
- `{'farm': 'Lillgrund', 'timestamp': '2007-12-01T00:00:00+00:00', 'sample_point_id': 'farm_centroid', 'source_file': 'cmems_mod_bal_wav_my_PT1H-i_multi-vars_12.51E-13.04E_55.26N-55.76N_2007-12-01-2024-12-31.nc', 'status': 'ok', 'max_abs_diff': 0.0}`
- `{'farm': 'Lillgrund', 'timestamp': '2024-01-01T00:00:00+00:00', 'sample_point_id': 'farm_centroid', 'source_file': 'cmems_mod_bal_wav_my_PT1H-i_multi-vars_12.51E-13.04E_55.26N-55.76N_2007-12-01-2024-12-31.nc', 'status': 'ok', 'max_abs_diff': 0.0}`
- `{'farm': 'Middelgrunden', 'timestamp': '2001-03-01T00:00:00+00:00', 'sample_point_id': 'farm_centroid', 'source_file': 'cmems_mod_bal_wav_my_PT1H-i_multi-vars_12.43E-12.90E_55.44N-55.94N_2001-03-01-2024-12-31.nc', 'status': 'ok', 'max_abs_diff': 0.0}`
- `{'farm': 'Middelgrunden', 'timestamp': '2024-01-01T00:00:00+00:00', 'sample_point_id': 'farm_centroid', 'source_file': 'cmems_mod_bal_wav_my_PT1H-i_multi-vars_12.43E-12.90E_55.44N-55.94N_2001-03-01-2024-12-31.nc', 'status': 'ok', 'max_abs_diff': 0.0}`
- `{'farm': 'Nysted', 'timestamp': '2003-12-01T00:00:00+00:00', 'sample_point_id': 'farm_centroid', 'source_file': 'cmems_mod_bal_wav_my_PT1H-i_multi-vars_11.43E-11.99E_54.29N-54.81N_2003-12-01-2024-12-31.nc', 'status': 'ok', 'max_abs_diff': 0.0}`
- `{'farm': 'Nysted', 'timestamp': '2024-01-01T00:00:00+00:00', 'sample_point_id': 'farm_centroid', 'source_file': 'cmems_mod_bal_wav_my_PT1H-i_multi-vars_11.43E-11.99E_54.29N-54.81N_2003-12-01-2024-12-31.nc', 'status': 'ok', 'max_abs_diff': 0.0}`
- `{'farm': 'Rodsand II', 'timestamp': '2010-10-01T00:00:00+00:00', 'sample_point_id': 'farm_centroid', 'source_file': 'cmems_mod_bal_wav_my_PT1H-i_multi-vars_11.21E-11.85E_54.29N-54.82N_2010-10-01-2024-12-31.nc', 'status': 'ok', 'max_abs_diff': 0.0}`
- `{'farm': 'Rodsand II', 'timestamp': '2024-01-01T00:00:00+00:00', 'sample_point_id': 'farm_centroid', 'source_file': 'cmems_mod_bal_wav_my_PT1H-i_multi-vars_11.21E-11.85E_54.29N-54.82N_2010-10-01-2024-12-31.nc', 'status': 'ok', 'max_abs_diff': 0.0}`
- `{'farm': 'Samsa', 'timestamp': '2003-02-01T00:00:00+00:00', 'sample_point_id': 'farm_centroid', 'source_file': 'cmems_mod_bal_wav_my_PT1H-i_multi-vars_10.35E-10.82E_55.47N-55.97N_2003-02-01-2024-12-31.nc', 'status': 'ok', 'max_abs_diff': 0.0}`
- `{'farm': 'Samsa', 'timestamp': '2024-01-01T00:00:00+00:00', 'sample_point_id': 'farm_centroid', 'source_file': 'cmems_mod_bal_wav_my_PT1H-i_multi-vars_10.35E-10.82E_55.47N-55.97N_2003-02-01-2024-12-31.nc', 'status': 'ok', 'max_abs_diff': 0.0}`
- `{'farm': 'Sprogo', 'timestamp': '2009-12-01T00:00:00+00:00', 'sample_point_id': 'farm_centroid', 'source_file': 'cmems_mod_bal_wav_my_PT1H-i_multi-vars_10.71E-11.21E_55.09N-55.59N_2009-12-01-2024-12-31.nc', 'status': 'ok', 'max_abs_diff': 0.0}`
- `{'farm': 'Sprogo', 'timestamp': '2024-01-01T00:00:00+00:00', 'sample_point_id': 'farm_centroid', 'source_file': 'cmems_mod_bal_wav_my_PT1H-i_multi-vars_10.71E-11.21E_55.09N-55.59N_2009-12-01-2024-12-31.nc', 'status': 'ok', 'max_abs_diff': 0.0}`
- `{'farm': 'Tunm Knob', 'timestamp': '1995-05-01T00:00:00+00:00', 'sample_point_id': 'farm_centroid', 'source_file': 'cmems_mod_bal_wav_my_PT1H-i_multi-vars_10.12E-10.60E_55.72N-56.21N_1995-05-01-2024-12-31.nc', 'status': 'ok', 'max_abs_diff': 0.0}`
- `{'farm': 'Tunm Knob', 'timestamp': '2024-01-01T00:00:00+00:00', 'sample_point_id': 'farm_centroid', 'source_file': 'cmems_mod_bal_wav_my_PT1H-i_multi-vars_10.12E-10.60E_55.72N-56.21N_1995-05-01-2024-12-31.nc', 'status': 'ok', 'max_abs_diff': 0.0}`
- `{'farm': 'Wikinger', 'timestamp': '2018-10-01T00:00:00+00:00', 'sample_point_id': 'farm_centroid', 'source_file': 'cmems_mod_bal_wav_my_PT1H-i_multi-vars_13.79E-14.35E_54.56N-55.11N_2018-10-01-2024-12-31.nc', 'status': 'ok', 'max_abs_diff': 0.0}`
- `{'farm': 'Wikinger', 'timestamp': '2024-01-01T00:00:00+00:00', 'sample_point_id': 'farm_centroid', 'source_file': 'cmems_mod_bal_wav_my_PT1H-i_multi-vars_13.79E-14.35E_54.56N-55.11N_2018-10-01-2024-12-31.nc', 'status': 'ok', 'max_abs_diff': 0.0}`

## Guardrails

- No current downloads were run.
- No NORA3 extraction or consolidation was run.
- No final dwell-metocean feature table was rebuilt.
- No source fusion or preferred-source variables were created.
- No legacy CMEMS current CSV cache was promoted.
- Baltic `VSDX`/`VSDY` were not treated as Eulerian currents.
- No 10-minute interpolation was performed in this materializer.
