# Project State Summary: Current Execution & Run Status

This file records the current high-level execution state for agents resuming the empirical O&M dwell-event evidence build.

---

## Active Objective

The active objective is no longer only the Wikinger/Baltic pilot. The current harvest goal is to collect as many plausible AIS dwell events as possible for the EngD O&M modelling evidence base while maintaining three metocean branches in parallel:

1. NORA3 wave + wind cache backfill for broad dwell-weather joins.
2. Completed NWS wave hindcast backbone for North Sea and overlap farms.
3. Completed Baltic Copernicus native-hourly wave archive for the Baltic farms in scope.

Stage 1 now exists as an observed/provisional workability surface. Its default view is $H_s \times T_p$, but the surface engine is configurable for additional metocean and grouping dimensions. The Fusion v2 evidence-readiness audit is complete and recommends `proceed_with_restrictions`, with caveats `partial_event_scale_current_coverage`, `wind_direction_sensitivity_only`, and `depth_warning_sensitivity_required`.

RQ01 restricted Stage 2 sensitivity is accepted. Wave + wind speed shows no material screened difference from wave-only, while current-aware and high-confidence multivariate lanes show material screened differences. Current-aware findings are NWS-domain / coverage-limited sensitivity only, not general European causal evidence. Stage 2 remains descriptive/observed-envelope only; no calibrated `P(operation | weather)` model has been built or accepted.

RQ9 now has a completed local turbine-feasibility workstream for AIS-derived maintenance intervention intensity, not confirmed failure rate. Farm-level phase-separated intervention intensity, turbine-level feasibility, turbine denominator/exposure v1, turbine characteristics comparison v1, sea-basin observability audits, and an AIS observability bias audit are available under:

```text
Data/Processed/analysis/rq9_intervention_intensity/
reports/rq9_intervention_intensity/
```

The strongest open caveat is that the Baltic/North Sea gap is not yet causal or thesis-safe as an operational/reliability signal. It remains entangled with country/farm structure, vessel concentration, missing CTV/SOV registry evidence, assignment geometry, and unavailable direct AIS receiver-distance metadata. Tomorrow's recommended RQ9 continuation is: **Does intervention intensity vary by turbine age/capacity/OEM after controlling for farm, basin, and observability?**

The broad source and dwell jobs use Europe-wide farm-candidate slices:

```text
Data/Raw/AIS/Farm-Candidates_European-Master_YYYY_MM_SogMax2.0_Buffer2.0nm.csv
```

These files are month-level sources, not per-farm sources. One source slice can support every processable farm in `Data/Interim/European_Turbine_Coordinates.csv`.

The turbine coordinate file confirms the expanded scope includes German wind farms: **1,637 German turbine rows across 30 German wind farms**, including Alpha Ventus, Trianel Windpark Borkum 1, Trianel Windpark Borkum 2, Wikinger, Baltic Eagle, and others.

Governance handoff: meaningful increments now use the stable-mainline workflow in `docs/governance.md`. Before changing repeated status claims, use `docs/context-authority-map.md` to identify authority files and derived summaries. Before merge, complete `docs/branch-exit-checklist.md`.

---

## Pipeline Run State

### 1. European AIS Source Backfill

Status: partially complete and resumable.

```text
screen session: european_farm_candidate_backfill_2010_2025
log file:       backfill_european_farm_candidates_2010_2025.log
command:        /opt/anaconda3/bin/python -u scripts/backfill_ais_slices.py --start-year 2010 --end-year 2025 --phase all --mode farm_candidate --region european_master --turbine-file Data/Interim/European_Turbine_Coordinates.csv --retries 5 --retry-sleep-seconds 120 --progress-interval 1
```

The helper is resumable and skips raw/event/registry files that already exist.

Current on-disk state:

```text
raw_farm_candidate_month_files=180
manifest_rows=3270
manifest_status_counts={'skipped_exists': 1148, 'attempt': 709, 'success': 589, 'exists': 424, 'planned': 330, 'skipped_no_keys': 47, 'failed': 23}
```

Historical pre-run dry-run state:

```text
scheduled_slices=192
existing_slices=159
planned_missing_slices=33
```

### 2. European Dwell Harvest

Status: materially complete for many farm-months and resumable for missing-source gaps.

```text
screen session: european_dwell_harvest_2010_2025
log file once runner starts: backfill_european_dwell_harvest_2010_2025.log
command: /opt/anaconda3/bin/python -u src/om_pipeline/runners/run_ais_dwell_backfill.py --farm-cluster european_master --years 2010-2025 --months 1,2,3,4,5,6,7,8,9,10,11,12 --resume --output-root data/processed/ais_dwell_backfill/
```

The runner resolves `european_master` dynamically from the turbine coordinate table and processes farms with at least three coordinate points.

Current on-disk state:

```text
dwell_partition_files=4328
distinct_farms_with_partitions=113
partition_year_span=2010-2025
manifest_rows=22052
manifest_status_counts={'success_no_ais_in_bbox': 14605, 'success': 6041, 'skipped_missing_source': 1406}
```

Historical dry-run scope:

```text
processable_farms=113
planned_farm_month_partitions=21696
```

Existing successful partitions are preserved. Old `skipped_missing_source` rows are retried when their source files appear.

### 3. Wikinger / Baltic Pilot

Status: historical pilot source generation is complete; Baltic continuous wave downloads now exist as reviewed raw subsets.

Verified source triples:

```text
complete_triples=88
partial_triples=0
missing_triples=0
```

The Wikinger/Baltic cluster remains available as the static `wikinger` runner cluster.

Baltic wave raw subset status:

```text
raw_subset_farms=16
raw_subset_root=Data/Raw/Metocean/CMEMS/BalticSea/Waves
processed_archive_root=Data/Processed/metocean/baltic_wave_timeseries
processed_archive_exists=True
processed_archive_farms=16
processed_archive_partitions=238
processed_archive_rows=73866720
processed_archive_acceptance=analysis/06_rq6_metocean_spatial_resolution/baltic_wave_materialization_acceptance_report.md
```

### 4. Common Regional Metocean Requirements Planner

Status: implemented and still the authoritative planning layer. Downstream branches have now diverged in execution maturity.

The shared NWS/Baltic planning authority is:

```text
analysis/06_rq6_metocean_spatial_resolution/COMMON_METOCEAN_EXTRACTION_METHODOLOGY.md
```

The executable common requirement planner is:

```text
src/om_pipeline/metocean/common_requirements.py
scripts/plan_common_metocean_requirements.py
```

It derives one canonical wind-farm requirements table from turbine/farm metadata, with temporal bounds from operation or commissioning start to the configured study end date and spatial bounds from the turbine/farm footprint plus buffer. AIS events are validation samples and supporting diagnostics only; they must not define continuous archive start/end dates.

Current canonical dry-run outputs:

```text
analysis/06_rq6_metocean_spatial_resolution/common_metocean_farm_requirements.csv
analysis/06_rq6_metocean_spatial_resolution/common_metocean_farm_requirements.parquet
```

Agent 2 / NWS and Agent 3 / Baltic must consume that same common table and add only product-specific adapter fields such as product domain, variables, source prefix, request formatting, and API details.

### 5. NWS Wave Hindcast Backbone

Status: complete as a processed farm/sample-point archive.

```text
processed_root=Data/Processed/metocean/nws_wave_timeseries
farm_count=112
farm_year_partitions=1169
full_extraction_qa=analysis/06_rq6_metocean_spatial_resolution/nws_wave_full_extraction_qa_report.md
```

Important storage note:

- The processed archive lives inside this repository.
- The local annual raw NWS NetCDF source files referenced by the inventory remain on the external 4TB drive, not under `Data/Raw/Metocean/NWS/`.

### 6. Static Bathymetry Site Context

Status: complete as an EMODnet point-sample static archive for common metocean sample points.

```text
raw_cache_root=Data/Raw/Metocean/Bathymetry/emodnet_depth_samples
processed_output=Data/Processed/metocean/bathymetry/site_bathymetry_points.parquet
metadata=Data/Processed/metocean/bathymetry/bathymetry_source_metadata.json
qa_report=analysis/06_rq6_metocean_spatial_resolution/bathymetry_assignment_full_report.md
farm_count=119
sample_point_count=6642
missing_depth_count=0
duplicate_farm_sample_keys=0
```

The assignment uses official EMODnet `depth_sample` REST responses cached as JSONL. GEBCO_2026 remains the documented cross-check source but was not fetched because EMODnet returned valid depth samples for all points.

### 7. FINO Validation Planning

Status: dry-run metadata/access planning complete; no FINO time-series import has been run.

```text
planning_script=scripts/plan_fino_metadata_access.py
planning_logic=src/om_pipeline/metocean/fino_metadata_planner.py
planning_report=analysis/06_rq6_metocean_spatial_resolution/fino_metadata_access_plan.md
decision_record=docs/adr/0023-fino-validation-access-planning.md
export_inspector_script=scripts/inspect_fino_export.py
export_inspector_logic=src/om_pipeline/metocean/fino_export_inspector.py
export_inspector_decision_record=docs/adr/0024-fino-native-export-inspection.md
stations=FINO1, FINO2, FINO3
sample_point_source=Data/Processed/metocean/bathymetry/site_bathymetry_points.parquet
sample_point_count=6642
processed_fino_archive_exists=False
```

FINO is reserved for validation/baseline comparisons. The export inspector is
report-only for one manually exported BSH Insitu CSV/ASCII file and must not be
treated as an import path or farm-wide primary metocean source unless later
representativeness and import rules are explicitly accepted.

### 8. Metocean Fusion v0

Status: complete as a research increment, not a production final rebuild.

```text
fusion_script=scripts/build_metocean_fusion_v0.py
fusion_logic=src/om_pipeline/metocean/metocean_fusion_v0.py
output_table=Data/Processed/metocean/fusion_v0/dwell_metocean_fusion_v0.parquet
validation_report=reports/metocean_fusion_v0/metocean_fusion_v0_validation_report.md
input_rows=92660
output_rows=92660
nora3_hs_tp_rows=44377
fusion_hs_tp_rows=83901
```

Fusion v0 prioritizes Baltic wave coverage, then NWS wave coverage, then
existing NORA3 active fields, and joins accepted bathymetry by nearest common
sample point to the dwell centroid. It includes no currents or FINO observations
and must not be treated as calibrated `P(operation | weather)`.

### 9. Fusion v1 Wave Source Agreement

Status: accepted as the wave confidence layer.

```text
logic=src/om_pipeline/metocean/metocean_fusion_v1_source_agreement.py
script=scripts/build_metocean_fusion_v1_source_agreement.py
candidate_table=Data/Processed/metocean/fusion_v1_source_agreement/wave_source_candidates.parquet
pairwise_table=Data/Processed/metocean/fusion_v1_source_agreement/wave_source_pairwise_agreement.parquet
confidence_table=Data/Processed/metocean/fusion_v1_source_agreement/wave_event_confidence.parquet
validation_report=reports/metocean_fusion_v1_source_agreement/source_agreement_validation_report.md
```

Fusion v1 replaces the v0 source-priority resolver with source agreement,
pairwise disagreement, and A/B/C/D wave confidence. The main research result is
that `Hs` is relatively robust across products while `Tp` is source-sensitive.

### 10. Current Confidence v1

Status: accepted as the event-scale current confidence layer for normal NWS-covered farm-years.

```text
logic=src/om_pipeline/metocean/current_confidence_v1.py
script=scripts/build_current_confidence_v1.py
candidate_table=Data/Processed/metocean/current_confidence_v1/current_event_candidates.parquet
confidence_table=Data/Processed/metocean/current_confidence_v1/current_event_confidence.parquet
validation_report=reports/current_confidence_v1/current_confidence_validation_report.md
input_rows=92660
event_scale_current_rows=16307
tier_a_current_rows=5358
```

Missing current means no accepted NWS current partition. It is not zero current.
Stress-test farm-years and Baltic daily/contextual currents remain excluded from
event-scale current evidence.

### 11. Wind Confidence v1

Status: accepted as the event-level wind confidence layer from existing NORA3 evidence.

```text
logic=src/om_pipeline/metocean/wind_confidence_v1.py
script=scripts/build_wind_confidence_v1.py
candidate_table=Data/Processed/metocean/wind_confidence_v1/wind_event_candidates.parquet
confidence_table=Data/Processed/metocean/wind_confidence_v1/wind_event_confidence.parquet
validation_report=reports/wind_confidence_v1/wind_confidence_validation_report.md
input_rows=92660
wind_speed_ready_rows=75380
wind_direction_ready_rows=197
tier_a_wind_speed_ready_rows=13708
```

Wind speed is ready for modelling sensitivity. Wind direction is too sparse for
broad modelling and must remain nullable or sensitivity-only until a targeted
NORA3 wind-direction repair is approved.

### 12. Metocean Fusion v2

Status: accepted as the first source-resolved multi-parameter event feature table.

```text
logic=src/om_pipeline/metocean/metocean_fusion_v2.py
script=scripts/build_metocean_fusion_v2.py
output_table=Data/Processed/metocean/fusion_v2/dwell_metocean_fusion_v2.parquet
validation_report=reports/metocean_fusion_v2/fusion_v2_validation_report.md
input_rows=92660
output_rows=92660
duplicate_dwell_id_rows=0
wave_rows=83901
wind_speed_rows=75380
wind_direction_rows=197
current_rows=16307
wave_wind_current_rows=13207
high_confidence_multivariate_rows=9337
tier_a_wave_wind_current_rows=4552
tier_a_high_confidence_rows=3402
```

Fusion v2 joins Wave Confidence v1, Wind Confidence v1, Current Confidence v1,
and EMODnet bathymetry while preserving separate confidence/provenance fields.
It is the accepted/provisional feature layer for the next Stage 2 modelling
sensitivity branch, not calibrated `P(operation | weather)` and not a final
model. The next modelling comparison should test wave-only versus wave+wind,
wave+current, and wave+wind+current subsets. Missing current remains null, not
zero current. Wind direction remains too sparse for primary predictors.

### 13. NORA3 Sidecar Consolidation

Status: active checkpointing path for the long-running weather join backfill.

```text
raw_cache_root=Data/Raw/Metocean/NORA3
joined_cache_root=Data/Processed/metocean/nora3_joined_cache
manifest_success_rows=20571
```

---

## Monitor Commands

List detached jobs:

```bash
screen -ls
```

Follow source generation:

```bash
tail -n 80 backfill_european_farm_candidates_2010_2025.log
tail -n 40 Data/Interim/ais_backfill_manifest.csv
python scripts/audit_ais_backfill_state.py
```

Follow dwell harvest after it starts:

```bash
tail -n 80 backfill_european_dwell_harvest_2010_2025.log
tail -n 40 data/processed/ais_dwell_backfill/logs/backfill_manifest.csv
```

Count current source coverage:

```bash
/opt/anaconda3/bin/python - <<'PY'
from pathlib import Path

raw = Path("Data/Raw/AIS")
missing = []
for year in range(2010, 2026):
    for month in range(1, 13):
        path = raw / f"Farm-Candidates_European-Master_{year}_{month:02d}_SogMax2.0_Buffer2.0nm.csv"
        if not path.exists():
            missing.append((year, month))

print("existing_slices", 192 - len(missing))
print("missing_slices", len(missing))
for year in range(2010, 2026):
    months = [month for yy, month in missing if yy == year]
    if months:
        print(year, ",".join(f"{month:02d}" for month in months))
PY
```

---

## Safety Notes

- Use `/opt/anaconda3/bin/python` for these runners.
- Metocean planning dry-runs are allowed through the common requirements planner and NWS/Baltic adapter planners.
- The NWS processed archive is already present locally; avoid duplicating it under a second folder convention unless you are intentionally migrating storage.
- Baltic raw subset files must not be deleted or renamed casually; one existing farm directory uses the product/download naming variant `Arkona-Becken_Südost`, while the planning CSV uses `Arkona_Becken_Sudost`.
- Do not run live metocean downloads or event-level metocean extraction unless explicitly requested.
- Do not delete existing raw, interim, processed, or report artifacts.
- Treat `Data/` and `data/` carefully:
  - Raw and interim AIS source files live under uppercase `Data/`.
  - Dwell parquet outputs live under lowercase `data/processed/ais_dwell_backfill/`.
