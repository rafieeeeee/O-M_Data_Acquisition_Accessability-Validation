# ADR 0028: Wind Confidence v1 Event Assignment

## Status
Accepted

## Context

Fusion v1 established wave-source agreement and confidence. Current Confidence v1 established event-scale NWS true `uo`/`vo` current confidence. The remaining gap before a real Fusion v2 table was wind: the final workability surface needs wind speed and, where defensible, wind direction alongside `Hs`, `Tp`, current, vessel, task, and site context.

The existing dwell-weather table already contains NORA3 active-window wind fields:

- `active_wind_speed_mean`
- `active_wind_speed_max`
- `active_wind_direction_sin_mean`
- `active_wind_direction_cos_mean`
- `active_n_weather_records`

However, local evidence shows wind speed and wind direction do not have the same coverage. The NORA3 raw wind cache is overwhelmingly speed-only (`33,521` files) with only a small direction-capable subset (`464` files). The event table has `75,380` rows with wind speed but only `197` rows with usable direction.

## Decision

Build Wind Confidence v1 as a separate event-level evidence layer before Fusion v2:

- Candidate table: `Data/Processed/metocean/wind_confidence_v1/wind_event_candidates.parquet`
- Confidence table: `Data/Processed/metocean/wind_confidence_v1/wind_event_confidence.parquet`
- Validation report: `reports/wind_confidence_v1/wind_confidence_validation_report.md`
- Core logic: `src/om_pipeline/metocean/wind_confidence_v1.py`
- CLI: `scripts/build_wind_confidence_v1.py`

The layer reads existing local sources only. It does not download NORA3 data, repair wind direction, mutate raw/joined caches, rebuild dwell weather, import FINO, or create Fusion v2.

Wind confidence classes are:

- `A_speed_direction`: NORA3 active-window wind speed and direction are present with explicit convention.
- `B_speed_only`: NORA3 active-window wind speed is present, but direction is missing or untrusted.
- `C_low_confidence`: wind evidence exists but source/provenance quality is weak.
- `D_unsuitable`: no accepted wind speed evidence or impossible/fallback/synthetic evidence.

The direction convention is recorded as:

```text
meteorological_from_degrees_clockwise_from_true_north
```

Missing wind direction remains null. It must not be converted to zero direction. Direction sin/cos outputs are unit-circle projections of the accepted event mean direction where direction exists.

The existing dwell-weather table stores active wind mean and max, not within-event p95. Wind Confidence v1 preserves `active_wind_speed_max` in the required `wind_speed_p95` output slot as an upper-window diagnostic and documents that it is not a true per-event p95 until targeted per-sample NORA3 reaggregation is approved.

## Consequences

Fusion v2 can proceed with wind speed and wind confidence included. Wind direction should be nullable or sensitivity-only, and any model using direction must state the narrow evidence subset.

The accepted Wind Confidence v1 output has:

- `92,660` candidate rows
- `92,660` confidence rows
- `75,380` speed-ready rows
- `13,708` Tier A speed-ready rows
- `197` speed+direction rows
- confidence distribution: `75,183` `B_speed_only`, `17,280` `D_unsuitable`, `197` `A_speed_direction`

Targeted NORA3 wind-direction repair may be considered later if directional wind effects are needed for simulator inputs or modelling sensitivity. That repair must be a separate approved increment because it changes source completeness, not just event confidence classification.
