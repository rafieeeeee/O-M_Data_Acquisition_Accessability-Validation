# Current, Bathymetry, and FINO Acquisition Plan

Date: 2026-05-29

Status: planning only. No current, bathymetry, FINO, NORA3, source-fusion, or final dwell-metocean write step was run for this report.

## Executive conclusion

The project should now move from source-specific wave archives toward a source-agnostic `metocean_assignment` layer, but the next write step should still be small and explicit. The accepted Baltic wave archive completes the third credible wave evidence source alongside NORA3 and NWS. The remaining gaps are true Eulerian currents, bathymetry/site context, FINO validation data, and resolver logic with field-level provenance.

Recommended next approval: implement and run a bathymetry assignment pilot first, because bathymetry is static, useful for every farm, lower-risk than current downloads, and immediately improves site-context features. Current downloads should wait until product, variable, depth, domain, and storage choices are confirmed by one-farm/year pilots.

## Current local inventory

| Asset | Local state | Evidence | Usability |
| --- | --- | --- | --- |
| Baltic processed waves | Present and accepted | `Data/Processed/metocean/baltic_wave_timeseries`, 238 parquet files, 73,866,720 rows, 16 farms, years 1995-2024, 1,331,935,775 bytes | Model-ready source-specific hourly wave archive; not fused and not interpolated to 10 minutes |
| NWS processed waves | Present | `Data/Processed/metocean/nws_wave_timeseries`, 1,170 parquet files, 173,507,512 rows, 112 farms, years 1995-2024, 1,154,865,322 bytes | Model-ready source-specific hourly wave archive |
| NORA3 joined cache | Present | `Data/Processed/metocean/nora3_joined_cache`, 375 parquet batches, 24,579,576 rows, 351,245,305 bytes | Usable for provisional Stage 1 and fallback wave/wind; wind direction coverage remains suspect |
| Baltic current archive | Missing | `Data/Processed/metocean/baltic_current_timeseries` does not exist | Needs acquisition/materialization from a true physics current product |
| NWS current archive | Missing as processed archive | `Data/Processed/metocean/nws_current_timeseries` does not exist | Local raw NWS current NetCDFs are documented externally, but not yet processed into a project archive |
| Legacy CMEMS current CSV cache | Present as raw CSV cache | `Data/Raw/Metocean/CMEMS/cmems_raw_*.csv` sample headers contain `time,current_speed,current_direction,lat,lon` | Do not promote: lacks `uo/vo`, depth, dataset ID, and reliable provenance; existing ingestion code can produce fallback/simulated values |
| Bathymetry | Missing | No local GEBCO/EMODnet/depth processed archive found | Needs static acquisition/assignment |
| FINO | Empty local placeholder | `Data/Raw/Metocean/FINO1` exists with 0 files; no processed FINO archive exists | Needs station metadata and access/import plan before ingestion |

The common metocean requirements table contains 119 farm rows across the United Kingdom, Germany, Denmark, Belgium, Netherlands, France, Norway, and Sweden. The study time span is 1995-05-01 to 2024-12-31, with farm bounding boxes spanning lon -4.1673 to 14.4298 and lat 46.8706 to 59.4006.

Approximate product-domain overlap from the common requirements:

| Domain | Rough local farm overlap | Notes |
| --- | ---: | --- |
| Baltic physics product bbox | 17 farms | Includes the 16 materialized Baltic wave farms plus Baltic Eagle |
| Official NWS physics bbox | 115 farms | Broad official product domain is useful, but exact local current-file coverage is narrower |
| Documented local NWS current file bbox | 22 farms | Local files appear limited to lon -11.0001 to 4.5553 and lat 53.9340 to 63.4012 |
| Global CMEMS gap candidate | 119 farms | Evaluate only after regional gaps are proven; do not use to fill missing current values silently |

## Documentation consistency check

Live documentation no longer contains positive Stage 1 overclaims from the requested search terms. Remaining `P(operation | weather)` hits are negative guardrails that explicitly say the provisional Stage 1 output is not a calibrated probability model.

Small documentation updates were applied to lock the accepted Baltic archive state in:

- `CONTEXT.md`
- `start_here/00_start_here.md`
- `start_here/01_project_state_summary.md`
- `docs/metocean-acquisition.md`

Those updates record that the Baltic wave archive is materialized and accepted, while true Baltic Eulerian currents remain a separate future product.

## Candidate products

| Component | Recommended role | Product / source | Product ID | Dataset ID / access hint | Variables | Time | Space/depth | Decision |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Baltic true currents | Primary Baltic current baseline | Copernicus Baltic Sea Physics Reanalysis | `BALTICSEA_MULTIYEAR_PHY_003_011` | Daily dataset: `cmems_mod_bal_phy_my_P1D-m`; static dataset for bathymetry/grid metadata | `uo`, `vo`, depth/grid metadata | 1993 to 2024, daily/monthly/yearly means | Baltic 53.01-65.89N, 9.04-30.21E; about 2 km; 56 depth levels | Use for a one-farm/year surface-current pilot; note daily cadence is not hourly |
| NWS currents | Western/North Sea current backbone where coverage exists | Copernicus Atlantic/North West Shelf Ocean Physics Reanalysis | `NWSHELF_MULTIYEAR_PHY_004_009` | Hourly surface currents: `cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i`; daily/monthly 3D alternatives exist | `uo`, `vo`; static bathymetry/grid metadata | 1993 to 2026, hourly/daily/monthly | Official bbox 40.07-65N, -19.89-13E; about 7 km; 24 depth levels | Prefer for farms inside verified NWS current coverage; process local raw current files before downloading more |
| Global gap candidate currents | Secondary true-current candidate only | Copernicus Global Ocean Physics Reanalysis | `GLOBAL_MULTIYEAR_PHY_001_030` | Daily: `cmems_mod_glo_phy_my_0.083deg_P1D-m`; monthly; static datasets | `uo`, `vo`; temperature/salinity/SSH available but out of scope | 1993 to 2026, daily/monthly | Global 1/12 degree, 50 depth levels | Evaluate only for proven regional gaps; preserve missing current as null rather than filling from this source silently |
| Bathymetry primary | Static depth/site context | EMODnet Bathymetry DTM | EMODnet Bathymetry 2024/active DTM | Download DTM tiles via EMODnet viewer/catalogue | Mean/min/max/std depth, source reference where available | Static | European seas, 1/16 arc-minute grid, about 115 m | Primary for European offshore wind points; preserve source/reference metadata |
| Bathymetry fallback/cross-check | Static global fallback and consistency check | GEBCO_2026 Grid | `GEBCO_2026` | User-defined area or global NetCDF/GeoTIFF | Elevation in metres plus optional TID source grid | Static, published April 2026 | Global 15 arc-second grid; negative depths, positive land elevations | Use as global fallback/cross-check; not for navigation |
| FINO validation | In-situ validation benchmark | FINO1/FINO2/FINO3 via BSH/FINO database / Insitu Portal | FINO database | Registration/login likely required | Wind, wave, meteorological and oceanographic station variables, heights/depths, QC | FINO1 from Jan 2004; FINO2 from Aug 2007; FINO3 from Sep 2009 | Fixed stations near German North Sea/Baltic offshore sites | Use for validation/benchmarking, not automatic farm-wide assignment |
| NORA3 wind direction | Targeted repair only | Existing NORA3 raw/cache pipeline | Existing local NORA3 files | Inspect wind raw headers/cache schema first | `wind_direction_10m` if present and valid | Existing NORA3 cache span | Existing NORA3 sampled points | Lower priority; repair only if final resolver still needs NORA3 wind direction |

## Farm and domain scoping

### Baltic currents

Candidate farms: Anholt, Arcadis Ost 1, Arkona-Becken Sudost, Avedore Holme, Baltic Eagle, EnBW Windpark Baltic 1, EnBW Windpark Baltic 2, Frederikshavn Offshore, Kriegers Flak, Lillgrund, Middelgrunden, Nysted, Rodsand II, Samsa, Sprogo, Tunm Knob, Wikinger.

Recommended pilot: Wikinger or Arcadis Ost 1 for one complete year near the accepted Baltic wave archive, surface/nearest-surface `uo` and `vo` only. The first pilot must verify whether daily currents are sufficient for workability modelling or only useful as a low-frequency current baseline.

### NWS currents

The official NWS product covers most of the study domain, but documented local NWS current files cover a narrower longitude range ending near 4.555E. Therefore, the first NWS current pilot should use a farm clearly inside local current-file coverage, such as a UK/Irish Sea/North Sea western farm from the 22-farm local overlap set. Do not use a German or Baltic farm for the first NWS-current pilot.

### Global gap candidate currents

Only evaluate global currents after the Baltic and NWS pilots identify
farm/year gaps. The global product is daily, coarser than regional products for
shelf/Baltic sites, and may not be ideal for near-coastal or shallow-water
operational workability. Missing event-scale current evidence must remain null
unless a separate, source-labelled global-current assignment is explicitly
approved.

### Bathymetry

Apply to all 119 common metocean farm rows and, where available, turbine/sample points used by NWS/Baltic materialization. The first pilot should assign EMODnet depth and source reference to all points without joining to dwell events.

### FINO

Treat FINO as validation metadata plus time series. First step is station inventory: coordinates, variables, measurement heights/depths, QC flags, access method, and station-to-farm distances. FINO data should not be treated as a farm-wide primary source unless a distance and representativeness rule is explicitly accepted.

## Start and end date rules

| Data family | Start rule | End rule |
| --- | --- | --- |
| Currents | Farm operation/commissioning start when available; otherwise common requirements fallback | 2024-12-31 for parity with accepted NWS/Baltic wave archives unless the selected product/pilot deliberately uses newer coverage |
| Bathymetry | Static; no time span | Static; record product version/date |
| FINO | Station-specific availability start plus project overlap | Latest available after access/import, but validation windows should be reported per station-variable |

Depth rule for current pilots: start with surface or nearest-surface `uo/vo`. Do not download all 3D depth levels for the first pilot. If later modelling needs depth structure, add a separate scoped depth-sensitivity task.

## Storage and runtime estimates

| Acquisition | Expected files | Raw size class | Processed size class | Runtime class | Main risk |
| --- | ---: | --- | --- | --- | --- |
| Bathymetry clipped EMODnet/GEBCO tiles | A few regional tiles | Small/medium, likely hundreds of MB if clipped | Very small point table, usually under tens of MB | Small | Download/access mechanics and sign convention |
| FINO metadata + small import | Station-variable files | Small to medium | Small | Small | Registration/access, variable naming, QC interpretation |
| Baltic current one-farm/year pilot | 1-3 product subsets | Small | Very small | Small | Daily cadence may be too coarse for event timing |
| Baltic current all-surface archive | Product subsets by farm/year or regional batches | Medium to large | If daily surface only, roughly millions of rows and likely hundreds of MB; hourly/full-depth would be much larger | Medium/large | Accidentally pulling all 56 depth levels or too large a domain |
| NWS current pilot from local raw files | Existing yearly file(s) | No new download if local files are usable | Very small | Small | Local grid overlap is narrower than official product |
| NWS current archive | Existing local raw or targeted downloads | Existing raw documented around 11 GB; targeted downloads depend on coverage | Medium | Medium | Processing the wrong farms outside local coverage |
| Global gap-current pilot | 1 product subset | Small | Very small | Small | Coarser/daily product may underfit shelf/baltic events |

## Proposed output schemas

### Source-specific current archive

Recommended partitioning: `Data/Processed/metocean/<source>_current_timeseries/wind_farm=<farm>/year=<year>/part.parquet`.

Required columns:

- `timestamp_utc`
- `wind_farm`
- `sample_point_id`
- `sample_point_type`
- `lat`
- `lon`
- `current_grid_lat`
- `current_grid_lon`
- `current_spatial_distance_km`
- `current_u`
- `current_v`
- `current_speed`
- `current_direction`
- `current_depth_m`
- `current_source_file`
- `current_product_id`
- `current_dataset_id`
- `current_extraction_method`
- `current_spatial_match_status`

### Bathymetry archive

Recommended output: `Data/Processed/metocean/bathymetry/site_bathymetry_points.parquet`.

Required columns:

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
- `seabed_slope_optional`
- `depth_sign_convention`
- `bathymetry_source_reference`

### FINO archive

Recommended output: station metadata plus normalized long-form observations under `Data/Processed/metocean/fino/`.

Required metadata columns:

- `station_id`
- `station_name`
- `lat`
- `lon`
- `water_depth_m`
- `operator_or_source`
- `access_method`

Required observation columns:

- `station_id`
- `variable`
- `measurement_height_or_depth`
- `timestamp_utc`
- `value`
- `unit`
- `qc_flag`
- `source_file`
- `access_method`
- `distance_to_farm_km`

## Validation gates

### Currents

- `uo/current_u` and `vo/current_v` are present.
- `current_speed == sqrt(current_u^2 + current_v^2)` within tolerance.
- Direction convention is documented, including whether it is direction-to or direction-from.
- Timestamps are UTC-normalized and regular for the selected product cadence.
- Depth level is documented.
- No simulated fallback values.
- Product ID, dataset ID, source file, and extraction method are populated.
- Nearest-grid distance is plausible and reported.
- Physical ranges are plausible.
- No duplicate `wind_farm + sample_point_id + timestamp_utc + current_depth_m` rows.

### Bathymetry

- Depth sign convention is documented.
- Offshore wind depths are plausible.
- No nulls for farm/turbine/sample points unless explained.
- Source and version are recorded.
- Assignment distance is recorded.
- Optional slope method is documented if generated.
- Spot checks compare against EMODnet/GEBCO viewer or raw tile values.

### FINO

- FINO station coordinates are verified.
- Variables, units, and measurement heights/depths are documented.
- QC flags are preserved.
- Cadence is reported by station-variable; 10-minute cadence should be preserved where available.
- Station-to-farm distances are reported.
- Validation metrics are planned: MAE, RMSE, bias, correlation, and coverage.
- Representativeness caveat is included for each station/farm comparison.

### Source-agnostic resolver

- Row count preservation versus the dwell table.
- No duplicate dwell IDs.
- `wave_source`, `wind_source`, `current_source`, `bathymetry_source`, and `validation_source` are populated where corresponding variables are present.
- Missingness reasons are populated where variables are absent.
- Units and physical ranges are valid.
- Direction sin/cos pairs have plausible magnitude.
- Current speed is consistent with `u/v`.
- Bathymetry sign convention is preserved.
- Temporal interpolation method is recorded.
- Nearest-grid distance threshold is enforced.
- FINO comparison metrics are generated where station/farm overlap is meaningful.
- Regression comparison is run against the current NORA3-only dwell-weather output.

## Recommended implementation order

1. Bathymetry acquisition/assignment pilot for all farm/turbine/sample points.
2. FINO station metadata and access/import plan.
3. Baltic current one-farm/year pilot using true `uo/vo`, surface or nearest-surface only.
4. NWS current overlap verification and one western farm/year pilot using local raw files where possible.
5. Broader/global CMEMS current gap pilot only for proven gaps.
6. NORA3 wind-direction targeted repair plan.
7. Source-agnostic `metocean_assignment` dry-run.

## First pilot command proposals

These commands are proposals for future approval. They were not run.

The first practical step is a thin bathymetry wrapper, because no bathymetry assignment script currently exists:

```bash
/opt/anaconda3/bin/python scripts/plan_bathymetry_assignment.py \
  --requirements analysis/06_rq6_metocean_spatial_resolution/common_metocean_farm_requirements.csv \
  --primary-source emodnet \
  --fallback-source gebco_2026 \
  --output-dir Data/Processed/metocean/bathymetry \
  --qa-report analysis/06_rq6_metocean_spatial_resolution/bathymetry_assignment_pilot_report.md \
  --dry-run
```

After the dry-run report is reviewed, a small write pilot could assign bathymetry to the common metocean sample points only:

```bash
/opt/anaconda3/bin/python scripts/assign_bathymetry_to_metocean_points.py \
  --requirements analysis/06_rq6_metocean_spatial_resolution/common_metocean_farm_requirements.csv \
  --source-root Data/Raw/Metocean/Bathymetry \
  --output-dir Data/Processed/metocean/bathymetry \
  --qa-report analysis/06_rq6_metocean_spatial_resolution/bathymetry_assignment_pilot_report.md \
  --limit-scope common-requirements \
  --no-overwrite
```

Proposed Baltic current pilot after bathymetry/FINO metadata:

```bash
/opt/anaconda3/bin/python scripts/plan_cmems_current_subset.py \
  --product-id BALTICSEA_MULTIYEAR_PHY_003_011 \
  --dataset-id cmems_mod_bal_phy_my_P1D-m \
  --farm Wikinger \
  --year 2024 \
  --variables uo vo \
  --depth surface \
  --dry-run
```

Proposed NWS current pilot after local-file overlap verification:

```bash
/opt/anaconda3/bin/python scripts/materialize_nws_current_timeseries.py \
  --raw-root "/Volumes/4TB HDD/Atlantic- European North West Shelf- Ocean Physics Reanalysis" \
  --farm Westermost_Rough \
  --year 2024 \
  --variables uo vo \
  --output-dir Data/Processed/metocean/nws_current_timeseries \
  --qa-report analysis/06_rq6_metocean_spatial_resolution/nws_current_pilot_qa_report.md \
  --no-overwrite
```

## Explicit do-not-do list

- Do not download broad current products before one-farm/year pilots.
- Do not download all 3D depth levels for current pilots.
- Do not treat Baltic `VSDX/VSDY` Stokes drift as Eulerian current.
- Do not promote legacy CMEMS current CSVs as final evidence.
- Do not rebuild the final dwell-metocean feature table yet.
- Do not create source-fused preferred variables yet.
- Do not interpolate Baltic hourly waves to 10-minute cadence in source-specific archives.
- Do not rerun NORA3 extraction/consolidation.
- Do not label provisional Stage 1 output as calibrated `P(operation | weather)`.
- Do not infer CTV/SOV roles from vessel-length bands without an external registry.

## Files inspected

- `start_here/00_start_here.md`
- `start_here/01_project_state_summary.md`
- `start_here/02_incremental_dev_guide.md`
- `start_here/03_domain_map_cheat_sheet.md`
- `CONTEXT.md`
- `AGENTS.md`
- `docs/README.md`
- `docs/metocean-acquisition.md`
- `docs/adr/0007-metocean-wind-and-current-expansion.md`
- `docs/adr/0017-copernicus-nws-metocean-extraction-backbone.md`
- `docs/adr/0018-baltic-copernicus-wave-download-planning.md`
- `docs/adr/0021-provisional-stage1-and-baltic-wave-materialization.md`
- `analysis/06_rq6_metocean_spatial_resolution/baltic_wave_materialization_qa_report.md`
- `analysis/06_rq6_metocean_spatial_resolution/baltic_wave_materialization_acceptance_report.md`
- `analysis/06_rq6_metocean_spatial_resolution/common_metocean_farm_requirements.csv`
- `analysis/06_rq6_metocean_spatial_resolution/nws_file_inventory.md`
- `analysis/06_rq6_metocean_spatial_resolution/nws_spatial_coverage_report.md`
- `analysis/06_rq6_metocean_spatial_resolution/nws_extraction_qa_report.md`
- `analysis/06_rq6_metocean_spatial_resolution/nws_wave_full_extraction_qa_report.md`
- `analysis/06_rq6_metocean_spatial_resolution/baltic_download_plan_summary.md`
- `analysis/06_rq6_metocean_spatial_resolution/METOCEAN_PRODUCT_COVERAGE_DECISION.md`
- `reports/stage1_hs_tp_provisional/coverage_representativeness_report.md`
- `reports/stage1_hs_tp_provisional/validation_summary.json`
- `src/om_pipeline/ingestion/cmems.py`
- `src/om_pipeline/metocean/baltic_wave_materializer.py`
- `src/om_pipeline/metocean/common_requirements.py`
- `scripts/materialize_baltic_wave_timeseries.py`
- `scripts/plan_common_metocean_requirements.py`

## Commands run

Read-only/local checks:

```bash
head -n 1 analysis/06_rq6_metocean_spatial_resolution/common_metocean_farm_requirements.csv
```

```bash
rg -n "not materialized yet|processed_archive_exists=False|still needs processed archive materialization|P\(operation \| weather\)|P\(Access \| Hs, Tp\)|safe access limit|safe operating boundary|CTV archetype|SOV archetype|length <40|length >=60|length ≥60" CONTEXT.md start_here docs analysis reports -g '*.md'
```

```bash
/opt/anaconda3/bin/python - <<'PY'
# Inline read-only pyarrow/pandas inventory of processed wave archives,
# missing current/bathymetry/FINO archives, and common requirements scope.
PY
```

Official web sources checked:

- Copernicus Baltic Sea Physics Reanalysis, `BALTICSEA_MULTIYEAR_PHY_003_011`
- Copernicus NWS Physics Reanalysis, `NWSHELF_MULTIYEAR_PHY_004_009`
- Copernicus NWS Product User Manual, dataset `cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i`
- Copernicus Global Ocean Physics Reanalysis, `GLOBAL_MULTIYEAR_PHY_001_030`
- EMODnet Bathymetry
- GEBCO_2026 Grid
- FINO database / BSH Insitu Portal route

## Files created or modified

Created:

- `analysis/06_rq6_metocean_spatial_resolution/current_bathymetry_fino_acquisition_plan.md`

Modified for state-locking only:

- `CONTEXT.md`
- `start_here/00_start_here.md`
- `start_here/01_project_state_summary.md`
- `docs/metocean-acquisition.md`

## References

- Copernicus Marine, Baltic Sea Physics Reanalysis: https://data.marine.copernicus.eu/product/BALTICSEA_MULTIYEAR_PHY_003_011/description
- Copernicus Marine, Baltic Sea Physics Reanalysis data access: https://data.marine.copernicus.eu/product/BALTICSEA_MULTIYEAR_PHY_003_011/services
- Copernicus Marine, NWS Physics Reanalysis: https://data.marine.copernicus.eu/product/NWSHELF_MULTIYEAR_PHY_004_009/description
- Copernicus Marine, NWS Product User Manual: https://documentation.marine.copernicus.eu/PUM/CMEMS-NWS-PUM-004-009-011.pdf
- Copernicus Marine, Global Ocean Physics Reanalysis: https://data.marine.copernicus.eu/product/GLOBAL_MULTIYEAR_PHY_001_030/description
- EMODnet Bathymetry: https://emodnet.ec.europa.eu/en/bathymetry
- GEBCO_2026 Grid: https://www.gebco.net/data-products-gridded-bathymetry-data/gebco2026-grid
- FINO database: https://www.fino2.de/en/fino2/fino-database.html
