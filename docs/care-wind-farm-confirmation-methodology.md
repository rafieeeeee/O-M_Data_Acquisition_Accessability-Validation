# CAREtoCompare Wind Farm C Confirmation Methodology

This protocol is designed for independent execution by multiple agents. Each agent should run the same tests from local data where possible, record exact commands or scripts used, and separate reproducible evidence from interpretation.

## Objective

Confirm whether CAREtoCompare Wind Farm C corresponds to Trianel Windpark Borkum, and if so whether the SCADA evidence points specifically to Borkum I, Borkum II, or a mixed/subset mapping.

The expected candidate distinction is:

- **Trianel Windpark Borkum I:** 40 Adwen/Areva M5000-116 turbines, 5 MW class, 116 m rotor.
- **Trianel Windpark Borkum II:** 32 Senvion 6.2M152 / Power Upgrade turbines, about 6.15-6.33 MW class, 152 m rotor.

## Required Local Inputs

- CAREtoCompare Wind Farm C SCADA files: `Data/CARE_To_Compare/Wind Farm C/datasets/*.csv`
- Local DuckDB catalog: `Data/catalog.duckdb`
- Turbine registry table or CSV backing the `turbines` view.
- Local NORA3 wind cache if available: `Data/Raw/Metocean/NORA3/nora3_wind_raw_*.csv`
- Existing de-anonymization context: `docs/adr/003-care-de-anonymization.md`

Agents may use official public sources only to verify static turbine metadata. Do not use external sources to patch over missing local analytical evidence.

## Evidence Standard

Use a three-level finding for each test:

- **Confirmed:** the claim reproduces from local data with exact commands/scripts and stable outputs.
- **Supported:** the result is consistent with the claim, but depends on incomplete data, assumptions, or non-local metadata.
- **Not Confirmed:** the result does not reproduce, is unsupported by local fields, or contradicts another stronger signal.

Avoid absolute claims such as "100% certainty." Use confidence language tied to the tests passed.

## Test 1: SCADA Inventory and Temporal Coverage

Purpose: establish what Wind Farm C actually contains before matching it to any real farm.

Procedure:

1. Read all Wind Farm C dataset CSVs with `sep=";"`.
2. Extract per-file row count, `asset_id`, first timestamp, last timestamp, available wind-speed columns, power columns, and yaw/relative wind columns.
3. Report:
   - number of files,
   - number of unique `asset_id` values,
   - global timestamp span,
   - per-asset coverage gaps,
   - whether files appear to be turbine-level assets, event windows, or sliced exports.

Pass criteria:

- The inventory is complete enough to compare asset count and coverage against Borkum I and Borkum II.
- If only a subset exists, downstream conclusions must be phrased as subset evidence.

## Test 2: Continuous Time-Lagged Correlation Scan

Purpose: verify the temporal shift independently from turbine metadata.

Procedure:

1. Build one or more hourly SCADA wind-speed time series from Wind Farm C.
2. For each candidate coordinate, load a continuous multi-year NORA3 100 m wind-speed time series spanning all plausible true calendar years, not only months near the anonymized SCADA calendar.
3. Slide the NORA3 series against the SCADA series at hourly resolution using time-lagged cross-correlation. Do not restrict the search to integer-year shifts.
4. Compute Pearson correlation for every lag with sufficient overlapping samples. Use a fixed minimum overlap threshold and report that threshold.
5. Report the absolute peak lag and the main local maxima, including integer-year-adjacent peaks.
6. Derive true SCADA start/end timestamps from the winning lag and report raw AIS archive availability for that true window before querying processed `dwell_events`.
7. Return the full ranking table, not only the winner.

Candidate coordinates should include:

- Trianel Windpark Borkum I / II centroid.
- Borkum Riffgrund 1 and 2.
- Alpha Ventus.
- Global Tech I.
- Nearby German Bight farms with similar exposure.

Pass criteria:

- Borkum under a continuous lag is the top candidate.
- The runner-up gap is large enough to be meaningful.
- Incorrect lags and nearby local maxima do not produce comparable correlations.
- The winning lag is supported by complete-enough NORA3 cache coverage across the overlapping window.

Required output:

- `candidate`, `lat`, `lon`, `lag_hours`, `lag_days`, `lag_description`, `overlap_hours`, `pearson_r`, `rank`.
- Derived `true_start` and `true_end` for the SCADA evidence.
- NORA3 cache coverage table for the tested coordinate/time span.
- Raw AIS archive availability table for the derived true window.
- A short note on missing NORA3 or AIS months and how they affected overlap.

Important cautions:

- CAREtoCompare timestamps may include leap-year and fractional timedelta artifacts. Do not assume that an intended anonymization shift lands exactly on an integer number of calendar years.
- A high correlation over a short overlap should not outrank a lower but more stable correlation over the full SCADA window without being reported as a coverage-sensitive local maximum.
- If multiple event files or assets show different peak lags, report the temporal mapping as mixed or unresolved rather than forcing a single farm-level shift.

## Test 3: Spatial Robustness and Bootstrap Confidence

Purpose: make sure the spatial match is not a single-window artifact.

Procedure:

1. Split the aligned SCADA/NORA3 comparison into monthly or rolling 30-day windows.
2. Re-rank candidates per window.
3. Bootstrap windows with replacement and estimate how often each candidate wins.
4. Report sensitivity to wind-speed column choice.

Pass criteria:

- Borkum wins across most independent windows.
- Borkum remains top across plausible SCADA wind-speed columns.
- No single month dominates the conclusion.

Required output:

- Per-window ranking table.
- Bootstrap win-rate table.
- Any months removed because of missing NORA3 cache or bad SCADA coverage.

## Test 4: Borkum I vs Borkum II Turbine Fingerprint

Purpose: distinguish the real turbine class rather than treating both Borkum phases as one identity.

Procedure:

1. For each `asset_id`, build empirical power curves using all valid rows.
2. Use normalized active power columns carefully. Record min/max values and whether values exceed `[0, 1]`.
3. Fit separate candidate curves:
   - 5 MW / 116 m class, representing Borkum I M5000-116.
   - 6.15-6.33 MW / 152 m class, representing Borkum II Senvion 6.2M152.
4. Use multiple fit metrics:
   - binned MAE,
   - RMSE,
   - `R^2`,
   - inferred rated wind speed,
   - plateau power behavior.
5. Cluster per-asset results to identify whether all assets share one fingerprint.

Pass criteria:

- Most assets favor the same turbine class.
- The preferred turbine class agrees with the inferred asset count and external turbine registry.
- Any conflicting assets are reported explicitly.

Required output:

- Per-asset fit table.
- Farm-level aggregate fit table.
- Plot of empirical binned curves against both candidate turbine classes.

Important cautions:

- Do not assume `power_6_avg * 6200` is correct without testing the normalization.
- Do not use a theoretical cubic curve as the only reference if an official power curve or project-specific rating is available.
- For Borkum II, include the power-upgrade rating range, not only 6,200 kW.

## Test 5: Directional Consistency

Purpose: test spatial match using wind direction without overclaiming from a wind rose.

Procedure:

1. Compute local NORA3 wind-direction sector distributions for candidate coordinates.
2. Compute SCADA relative wind/yaw-error summary statistics.
3. If absolute wind direction is absent in SCADA, do not claim a direct SCADA wind rose.
4. Instead, compare sector-stratified wind-speed correlations:
   - N, NE, E, SE, S, SW, W, NW, or 16-sector bins.
   - Candidate-specific correlation within each sector.

Pass criteria:

- Borkum remains competitive or dominant across most directional sectors.
- Sector results are physically plausible and not driven by one dominant wind direction.

Required output:

- NORA3 wind rose table by candidate coordinate.
- SCADA relative wind/yaw-error ordinary and circular statistics.
- Sector-stratified correlation table.

Important cautions:

- Relative wind direction near zero only shows yaw control behavior. It does not reveal absolute wind direction by itself.
- A yaw-error standard deviation near 20-30 degrees should not be described as "near-perfect" alignment.

## Test 6: AIS/SCADA Co-Occurrence

Purpose: validate the site identity using independent vessel activity.

Procedure:

1. Query `dwell_events` for Borkum I and Borkum II separately.
2. Apply the current Wind Farm C temporal mapping. The active production mapping is a 0-year shift, so CARE SCADA timestamps are compared directly against true-calendar candidate AIS windows.
3. For each dwell event with a matching CARE event file or asset window, inspect nearby SCADA `status_type_id`, power, wind speed, and downtime/service signals.
4. Compare against non-Borkum candidate farms as controls.

Pass criteria:

- Borkum dwell events align more often with plausible SCADA service/downtime behavior than controls.
- The result holds after excluding small vessels or ambiguous vessel types.

Required output:

- Per-event table with `wind_farm`, `event_id`, `MMSI`, `Name`, `start`, `end`, `duration_min`, `length`, nearest SCADA window, and status summary.
- Control-farm comparison counts.

Important cautions:

- `length > 60m` is a vessel-size proxy, not proof of DP capability.
- Vessel names and MMSIs are research-sensitive. Keep public summaries aggregated unless exposure is explicitly approved.

## Test 7: Turbine Registry and Public Metadata Cross-Check

Purpose: confirm static metadata without letting it substitute for data analysis.

Procedure:

1. Query local `turbines` for Borkum I and Borkum II:
   - turbine count,
   - manufacturer,
   - turbine type,
   - rated power,
   - rotor diameter,
   - commissioning date.
2. Verify these facts against official project pages or stable registry sources.
3. Compare the registry facts to the SCADA fingerprint from Test 4.

Pass criteria:

- Local registry and public metadata agree on Borkum I vs Borkum II turbine classes.
- SCADA fingerprint points to the same phase or clearly documents a mixed/subset result.

Required output:

- Metadata table with source labels.
- Notes on discrepancies, such as 6.15 MW standard rating versus 6.33 MW power-upgrade rating.

## Test 8: Negative Controls

Purpose: prevent confirmation bias.

Procedure:

1. Run the same temporal/spatial correlation methods against nearby non-Borkum candidates.
2. Run the same turbine fingerprint method against incompatible turbine classes.
3. Run AIS/SCADA co-occurrence against at least two control farms with good local AIS coverage.

Pass criteria:

- Borkum evidence is stronger than controls in multiple independent tests.
- Any control that performs similarly is reported as a serious ambiguity.

## Agent Deliverable Template

Each independent agent should return a short report with this structure:

```markdown
# Wind Farm C Confirmation Run

## Scope
- Local data used:
- Missing data:
- External metadata used:

## Findings
| Test | Finding | Confidence | Key Evidence |
| --- | --- | --- | --- |
| SCADA inventory | Confirmed/Supported/Not Confirmed | High/Medium/Low | ... |
| Year-shift scan | ... | ... | ... |
| Spatial robustness | ... | ... | ... |
| Turbine fingerprint | ... | ... | ... |
| Directional consistency | ... | ... | ... |
| AIS/SCADA co-occurrence | ... | ... | ... |
| Registry cross-check | ... | ... | ... |
| Negative controls | ... | ... | ... |

## Candidate Ranking
Include full ranking table, not only winner.

## Borkum I vs Borkum II Assessment
State whether evidence favors Borkum I, Borkum II, mixed Borkum array, or unresolved.

## Reproducibility
List scripts, commands, generated files, and random seeds.

## Caveats
List assumptions and unsupported claims.
```

## Final Synthesis Rule

Do not accept the wind farm identity from a single evidence family. A strong confirmation requires at least:

1. The temporal scan supports the 0-year mapping and Borkum-region candidates remain competitive under spatial correlation.
2. The Borkum win is robust across time windows and negative controls.
3. The SCADA turbine fingerprint matches either Borkum I or Borkum II metadata.
4. AIS/SCADA co-occurrence is directionally consistent with Borkum operations, or explicitly documented as coverage-limited rather than contradictory.

If those conditions are not met, the correct conclusion is "supported but not confirmed" or "phase unresolved," not "fully validated."
