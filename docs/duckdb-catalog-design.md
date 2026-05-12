# DuckDB Catalog Schema Design

**Goal:** Design the local DuckDB catalog layer for querying AIS slices, dwell events, fleet registries, turbine coordinates, and future metocean joins.

## 1. Tables and Views Proposed

### Core Tables

- `turbines`
  - Source: Open European Offshore Wind Turbine Database
  - Columns: `found_id` (PK), `wind_farm`, `country`, `latitude`, `longitude`, `commission_date`, etc.
  
- `ais_raw` (View over Parquet/CSV in `Data/Raw/`)
  - Columns: `Timestamp`, `MMSI`, `Latitude`, `Longitude`, `SOG`, `COG`, `Heading`, `NavStat`.

- `dwell_events` (Aggregated from `Data/Interim/OM_Events_*.csv`)
  - Columns: `event_uid` (PK: Composite of `MMSI` + `found_id` + local `event_id`), `MMSI`, `Name`, `Ship type`, `wind_farm`, `found_id`, `start`, `end`, `ping_count`, `mean_sog`, `min_dist`, `length`, `draught`, `duration_min`.

- `fleet_registry` (Aggregated from `Data/Interim/Fleet_Registry_*.csv`)
  - Columns: `MMSI`, `wind_farm` (Composite PK: `MMSI` + `wind_farm`), `Name`, `Ship type`, `Total_Dwell_Time`, `Event_Count`, `length`, `draught`.

- `metocean_events` (To be joined later)
  - Columns: `event_uid` (FK), `MMSI`, `found_id`, `timestamp_10min`, `lat`, `lon`, `Hs`, `Tp`, `wave_direction`, `source`.

- `run_metadata`
  - Columns: `run_id`, `timestamp`, `ais_slice`, `rows_scanned`, `rows_kept`, `events_found`, `vessels_identified`.

## 2. File Naming Assumptions

- Database file: `Data/catalog.duckdb`
- Table data will largely rely on DuckDB's ability to seamlessly read and query external CSV/Parquet files (e.g. `read_csv_auto('Data/Interim/OM_Events_*.csv')`). We will create Views pointing to these files rather than duplicating data, except for core dimensional data like `turbines`.

## 3. Recommended Indexes & Clustering

- **Dwell Events:** Create indexes on `MMSI`, `wind_farm`, and `found_id`.
- **Time-Series Joins:** When creating the 10-minute backbone, partition/cluster by `found_id` and `timestamp_10min` to optimize the merge between AIS and Metocean grids.

## 4. Example SQL Queries

**All dwell events for one MMSI:**
```sql
SELECT start, end, duration_min, wind_farm, found_id 
FROM dwell_events 
WHERE MMSI = 123456789 
ORDER BY start;
```

**Event counts by wind farm/month:**
```sql
SELECT wind_farm, date_trunc('month', start) as month, COUNT(*) as event_count 
FROM dwell_events 
GROUP BY wind_farm, month 
ORDER BY event_count DESC;
```

**Duration distribution by vessel:**
```sql
SELECT MMSI, Name, AVG(duration_min) as avg_duration, MAX(duration_min) as max_duration 
FROM dwell_events 
GROUP BY MMSI, Name;
```

**Later metocean join by timestamp/foundation:**
```sql
SELECT e.event_uid, e.start, e.duration_min, m.Hs, m.Tp 
FROM dwell_events e 
JOIN metocean_events m ON e.event_uid = m.event_uid 
WHERE e.wind_farm = 'Alpha Ventus';
```
