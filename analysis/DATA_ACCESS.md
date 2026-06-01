# Data Access Guide

## Canonical Research Table
* **Primary Stage 2 feature layer:** `Data/Processed/metocean/fusion_v2/dwell_metocean_fusion_v2.parquet`
* **Stage 1 observed surface source:** `Data/Processed/ais_dwell_backfill/cross_farm_dwell_weather_features.parquet`
* **Historical pilot table:** `Data/Processed/analysis/om_event_table.parquet`
* **Format:** Apache Parquet
* **Status:** Fusion v2 is accepted/provisional input for the next Stage 2 sensitivity branch. Stage 2 has not started.

## Source Data Inventory

| Source | Path / Table | Status | Notes |
| :--- | :--- | :--- | :--- |
| **Raw Dwell Catalogue** | DuckDB `dwell_events` and AIS dwell backfill partitions | `Available` | Broad European dwell evidence is materially populated and resumable; use `start_here/01_project_state_summary.md` for current counts. |
| **Weather-Joined Features** | `cross_farm_dwell_weather_features.parquet` | `Available` | Source for the Stage 1 observed/provisional $H_s \times T_p$ surface. |
| **Fusion v2 Feature Layer** | `Data/Processed/metocean/fusion_v2/dwell_metocean_fusion_v2.parquet` | `Accepted / provisional` | `92,660` rows with separate wave, wind, current, and bathymetry confidence/provenance fields. |
| **Fleet Registry CSVs** | monthly DMA registry files | `Partial` | 72–73% MMSI enrichment. Continuous size parameters preserved. |
| **NORA3 Wind/Wave** | NORA3 raw cache, joined cache, confidence layers | `Available` | Wind speed is ready for sensitivity (`75,380` rows); wind direction is sparse (`197` direction-ready rows). |
| **NWS True Currents** | `Data/Processed/metocean/current_confidence_v1/current_event_confidence.parquet` | `Accepted / partial coverage` | True `uo`/`vo` event-scale current assignments exist for `16,307` rows. Missing current remains null and must not be treated as zero. |
| **SCADA Operating Codes** | partial/conditional | `Check per RQ` | Fused for specific turbines/farms. Needed for RQ7–RQ9. |
| **Wake/Value Model** | unknown | `Deferred` | Coordinates-based rated yields. Needed for RQ8. |

## Standard Access Snippet

Use this Python snippet to load the canonical table in any RQ folder:

```python
import pandas as pd
from pathlib import Path

PROJECT_ROOT = Path("/Volumes/Extreme SSD/01_ACTIVE_PROJECTS/O&M_Data_Acquisition_Accessability&Validation")
FUSION_V2 = PROJECT_ROOT / "Data" / "Processed" / "metocean" / "fusion_v2" / "dwell_metocean_fusion_v2.parquet"

if FUSION_V2.exists():
    df = pd.read_parquet(FUSION_V2)
    print(f"Loaded Fusion v2 feature layer: {df.shape[0]:,} rows, {df.shape[1]} columns.")
else:
    print(f"Fusion v2 feature layer not found at {FUSION_V2}!")
```
