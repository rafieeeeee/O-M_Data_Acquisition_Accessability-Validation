# Project Provenance: O&M Data Acquisition & Validation

This document tracks the origin and processing of all primary data sources used in the project.

---

## 1. Primary Data Sources

### AIS (Automatic Identification System)
- **Source**: Danish Maritime Authority (DMA)
- **Access**: Public S3 Archive (`http://aisdata.ais.dk.s3.eu-central-1.amazonaws.com`)
- **Structure**: Monthly ZIP archives containing daily CSV files.
- **Coverage**: Global, with extraction focus on Northern European waters (46.5N-60.0N).

### Metocean (In-Situ)
- **Source**: FINO1 (Forschungsplattformen in Nord- und Ostsee Nr. 1)
- **Parameters**: 10-minute wave spectra ($H_s, T_p, \theta$).
- **Role**: Primary ground truth for German Bight validation.

### Metocean (Hindcast)
- **Source**: NORA3 (Norwegian Reanalysis 3km)
- **Resolution**: 3km spatial, 1-hour temporal (upscaled to 10-minute for synchronization).
- **License**: CC BY 4.0 (MET Norway).

### Operational (SCADA/DPR) [ACCESS PENDING]
- **Sources**: RAVE (Research at Alpha Ventus), EDP (CARE to Compare).
- **Status**: Access applications in progress; datasets not yet integrated into the local pipeline.
- **Role**: Event labeling (Success vs. WoW).

---

## 2. Pilot Run Log: July 2024 (German Bight)

### Execution Configuration
- **Date Range**: 2024-07-01 to 2024-07-31
- **Region**: `german_bight` (Lat: 53.0-56.0, Lon: 6.0-9.0)
- **Speed Filter**: `max_sog = 2.0`
- **Output**: `Data/Raw/AIS/German-Bight_2024_07_SogMax2.0.csv`

### Final Acquisition Metrics
- **Total Rows Scanned**: 677,073,540
- **Total Rows Kept**: 22,828,544 (3.37% retention)
- **Output File Size**: 3.8 GB
- **Daily Coverage**: 31/31 Days (Complete)

### Daily Retained-Row Distribution

| Day | ZIP Archive | Rows Kept | Status |
| :--- | :--- | :--- | :--- |
| 01 | aisdk-2024-07-01.zip | 862,684 | ✅ Success |
| 02 | aisdk-2024-07-02.zip | 785,713 | ✅ Success |
| 03 | aisdk-2024-07-03.zip | 748,349 | ✅ Success |
| 04 | aisdk-2024-07-04.zip | 843,094 | ✅ Success |
| 05 | aisdk-2024-07-05.zip | 771,414 | ✅ Success |
| 06 | aisdk-2024-07-06.zip | 742,101 | ✅ Success |
| 07 | aisdk-2024-07-07.zip | 682,088 | ✅ Success |
| 08 | aisdk-2024-07-08.zip | 623,137 | ✅ Success |
| 09 | aisdk-2024-07-09.zip | 754,592 | ✅ Success |
| 10 | aisdk-2024-07-10.zip | 711,232 | ✅ Success |
| 11 | aisdk-2024-07-11.zip | 669,740 | ✅ Success |
| 12 | aisdk-2024-07-12.zip | 639,172 | ✅ Success |
| 13 | aisdk-2024-07-13.zip | 683,363 | ✅ Success |
| 14 | aisdk-2024-07-14.zip | 694,738 | ✅ Success |
| 15 | aisdk-2024-07-15.zip | 689,764 | ✅ Success |
| 16 | aisdk-2024-07-16.zip | 636,599 | ✅ Success |
| 17 | aisdk-2024-07-17.zip | 696,373 | ✅ Success |
| 18 | aisdk-2024-07-18.zip | 754,961 | ✅ Success |
| 19 | aisdk-2024-07-19.zip | 1,027,426 | ⚠️ High Count |
| 20 | aisdk-2024-07-20.zip | 643,556 | ✅ Success |
| 21 | aisdk-2024-07-21.zip | 671,725 | ✅ Success |
| 22 | aisdk-2024-07-22.zip | 602,757 | ✅ Success |
| 23 | aisdk-2024-07-23.zip | 613,805 | ✅ Success |
| 24 | aisdk-2024-07-24.zip | 632,998 | ✅ Success |
| 25 | aisdk-2024-07-25.zip | 636,706 | ✅ Success |
| 26 | aisdk-2024-07-26.zip | 636,210 | ✅ Success |
| 27 | aisdk-2024-07-27.zip | 770,473 | ✅ Success |
| 28 | aisdk-2024-07-28.zip | 698,337 | ✅ Success |
| 29 | aisdk-2024-07-29.zip | 927,955 | ✅ Success |
| 30 | aisdk-2024-07-30.zip | 1,213,773 | ⚠️ High Count |
| 31 | aisdk-2024-07-31.zip | 763,709 | ✅ Success |

### Observations & Anomalies
- **Consistency**: Most days hover between 600k and 800k rows.
- **Outlier (July 19)**: >1M rows kept. No archive failure; likely reflects a period of significant regional vessel activity or sensor noise.
- **Outlier (July 30)**: >1.2M rows kept. Highest retention in the month.

---

## 3. Ingestion Logic Assumptions
- **Header Synonyms**: `Latitude/lat`, `Longitude/lon/long`, `SOG/speed over ground/speed`.
- **Decimal Handling**: Comma (`,`) decimals replaced with periods (`.`).
- **Malformed Rows**: Counted as `bad_lat_lon` or `bad_sog` and skipped to prevent pipeline crashes.
