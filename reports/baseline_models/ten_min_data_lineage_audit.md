# Phase 0: 10-Minute Data Lineage Audit (Wind Farm C)
Generated: 2026-05-20  

This report audits the underlying 10-minute data matrix and freezes modeling assumptions before pipeline training.

## 1. Dataset Dimensions & Temporal Footprint
* **Dataset Path:** `Data/Processed/wind_farm_c_feature_matrix.parquet`
* **Total High-Frequency Rows:** 120,224
* **Total Parent Event Groups:** 58
* **Temporal Span:** `2023-01-08` to `2024-01-06`
* **Interval Spacing:** Spaced at strict, continuous 10-minute intervals (continuous inside parent events).
* **Missing Timestamps:** Checked and verified. There are 0 timestamp gaps within active event bounds.

## 2. Target Label Construction
Labels inside the dataset are derived from two primary sources:
1. **SCADA Status Mapping:**
   * Status Code `3` (Service / Active Maintenance) maps to `maintenance_success` (if duration $\ge$ 30m and proximity $\le$ 50m) or `attempted_transfer`.
   * Status Code `4` (Downtime / Weather Standby) maps to `standby_weather` (if duration $\ge$ 60m).
   * Status Codes `0, 1, 2` map to `unknown`.
2. **Proximity Condition:**
   * Proximity in this dataset utilizes a synthetic `min_dist = 50m` hardcoding to represent close physical coupling during CARE operations.

## 3. Allowed Inputs vs. Forbidden Columns

| Category | Columns | Purpose | Leakage Status |
|---|---|---|---|
| **Metadata** | `['timestamp', 'asset_id', 'event_id']` | Sorting & grouped CV partitioning | Safe (Excluded from $X$) |
| **Metocean Features** | `['hs', 'tp', 'wave_direction_sin', 'wave_direction_cos', 'wind_speed_10m', 'wind_direction_10m_sin', 'wind_direction_10m_cos', 'wind_speed_100m', 'wind_direction_100m_sin', 'wind_direction_100m_cos', 'current_speed', 'current_direction_sin', 'current_direction_cos']` | Input variables representing the physical environment | Safe (Allowed in $X$) |
| **Operational Targets** | `['status_type_id', 'event_label_care', 'label']` | Labels representing operational state and SCADA status | **FORBIDDEN (Leakage Risk - Excluded from $X$)** |

## 4. Key Modeling Constraints
* **No SCADA Bleed:** Under no circumstances will SCADA status (`status_type_id`) or CARE labels (`event_label_care`) bleed into the training matrix.
* **No Pre-Validation Scaling:** Standard scaling and mean imputation will be fitted strictly fold-local within scikit-learn pipeline constructs to prevent cross-validation target leakage.
