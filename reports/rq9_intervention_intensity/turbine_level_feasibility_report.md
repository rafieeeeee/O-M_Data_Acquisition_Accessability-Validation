# RQ9 turbine-level intervention feasibility v0

This report checks whether existing AIS dwell evidence can support a turbine-level maintenance intervention intensity increment. It does not infer confirmed failure rate; SCADA, fault-log, work-order, or equivalent validation remains required.

## Assignment summary

- Tier A event rows inspected: 15264
- Events assigned within 500 m: 11875
- High-confidence events within 200 m: 4388
- Medium-confidence events from 200 to 500 m: 7487
- Unassigned events over 500 m or missing coordinates: 3389
- High-confidence share of assigned events: 0.370
- Duplicate-adjusted Tier A event weight total: 15011.833

## Assignment confidence distribution

| assignment_confidence   |   events |
|:------------------------|---------:|
| medium                  |     7487 |
| high                    |     4388 |
| unassigned              |     3389 |

## Lifecycle phase distribution

| lifecycle_phase       |   events |
|:----------------------|---------:|
| steady_operational    |    11465 |
| pre_operational       |     3292 |
| commissioning_ramp_up |      507 |

## Top farms by assigned Tier A events

| farm_id                |   assigned_tier_a_events |
|:-----------------------|-------------------------:|
| Wikinger               |                      900 |
| Rodsand II             |                      720 |
| Baltic Eagle           |                      657 |
| EnBW Windpark Baltic 2 |                      637 |
| Anholt                 |                      592 |
| Horns Rev I            |                      571 |
| Horns Rev II           |                      519 |
| Butendiek              |                      454 |
| Nysted                 |                      425 |
| Arkona-Becken Südost   |                      410 |
| Kriegers Flak          |                      392 |
| Meerwind Sued/Ost      |                      313 |
| Lillgrund              |                      310 |
| Amrumbank West         |                      305 |
| EnBW Windpark Baltic 1 |                      271 |

## Dwell/event column suitability

| concept            | present_columns                                         | missing_columns   |   non_null_share |
|:-------------------|:--------------------------------------------------------|:------------------|-----------------:|
| event id           | dwell_id, visit_id                                      |                   |         1        |
| farm id/name       | farm_id, wind_farm                                      |                   |         1        |
| tier               | dwell_tier                                              |                   |         1        |
| start/end time     | start_utc, end_utc                                      |                   |         1        |
| duration           | duration_min                                            |                   |         1        |
| lat/lon            | centroid_lat, centroid_lon                              |                   |         1        |
| MMSI               | mmsi                                                    |                   |         1        |
| duplicate group    | duplicate_group_id, possible_cross_farm_duplicate       |                   |         0.481092 |
| source phase label | farm_operational_status_at_event, interpretation_period |                   |         1        |

## Turbine metadata completeness

| metadata_field     | source_column      | present   |   non_null_share |   unique_count | status   |
|:-------------------|:-------------------|:----------|-----------------:|---------------:|:---------|
| turbine_source_id  | Unnamed: 0         | True      |                1 |           6523 | complete |
| farm_name          | wind_farm          | True      |                1 |            119 | complete |
| country            | country            | True      |                1 |              8 | complete |
| latitude           | latitude           | True      |                1 |           5533 | complete |
| longitude          | longitude          | True      |                1 |           5852 | complete |
| commissioning_date | commissioning_date | True      |                1 |             88 | complete |
| oem_manufacturer   | oem_manufacturer   | True      |                1 |             10 | complete |
| turbine_model      | turbine_type       | True      |                1 |             34 | complete |
| rated_capacity     | rated_power        | True      |                1 |             19 | complete |
| rotor_diameter     | rotor_diameter     | True      |                1 |             24 | complete |
| hub_height         | hub_height         | True      |                1 |             55 | complete |

## Answerability matrix

| question                             | status          |   sample_size | confidence_level   | required_missing                                                                     | next_increment                                                                 |
|:-------------------------------------|:----------------|--------------:|:-------------------|:-------------------------------------------------------------------------------------|:-------------------------------------------------------------------------------|
| turbine-level intervention intensity | partially ready |         11875 | low                | turbine-year denominator and validation against work orders/SCADA                    | build turbine observed-year denominators and aggregate high/medium assignments |
| bathtub/age curve                    | partially ready |          9219 | medium             | age-band turbine-year denominators and validation labels                             | derive turbine age bands over observed months                                  |
| farm-to-farm comparison              | ready           |           113 | medium             | minimum-exposure/capping sensitivity for simulator calibration                       | use steady farm-level field with min-observed-year sensitivity                 |
| exposure comparison                  | partially ready |         11875 | low                | static turbine exposure features such as edge distance, depth, and long-run exposure | join turbine layout exposure and depth features                                |
| Baltic vs North Sea comparison       | partially ready |         11875 | low                | explicit sea-basin/region mapping                                                    | add reviewed Baltic/North Sea farm-region mapping                              |
| OEM comparison                       | partially ready |         11875 | medium             | OEM turbine-year denominators and metadata harmonization                             | aggregate assigned events and denominators by OEM                              |
| turbine capacity comparison          | partially ready |         11875 | medium             | capacity-band turbine-year denominators                                              | aggregate assigned events and denominators by rated-capacity band              |

## Recommendation

The next analysis question to answer first is whether exposed turbines have higher steady-operational maintenance intervention intensity. That requires a turbine-year denominator plus static exposure features before using this v0 assignment as simulator demand evidence.
