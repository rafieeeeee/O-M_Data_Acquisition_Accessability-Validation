# Data Provenance

## AIS (Automatic Identification System)
- **Source:** Danish Maritime Authority (DMA)
- **Archive URL:** http://aisdata.ais.dk.s3.eu-central-1.amazonaws.com
- **History:** 
  - Historical data provided as ZIP archives containing CSV files.
  - Frequency: Generally 1 ZIP per month (pre-2024) or 1 ZIP per day (2024+).
- **Processing:** Regional filtering applied during ingestion to reduce 10GB+ archives to <1GB CSVs covering European waters.

## Offshore Wind Infrastructure
- **Source:** Open European Offshore Wind Turbine Database (EWW)
- **File:** `20251218_eww_opendatabase.csv`
- **License:** ODbL (Open Database License)
- **Attribution:** "Open European Offshore Wind Turbine Database"

## Metocean Data (Planned)
- **NORA3:** Norwegian Reanalysis (3km resolution) for wave hindcasts.
- **FINO1:** In-situ wave and wind measurements from the German Bight.
