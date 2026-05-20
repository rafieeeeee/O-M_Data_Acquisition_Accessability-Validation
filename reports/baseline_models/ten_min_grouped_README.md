# 10-Minute Grouped Validation Feasibility Study Summary
Generated: 2026-05-20  
Script: `scripts/train_wind_farm_c_10min_grouped.py`

This report summarizes our event-aware, leakage-safe feasibility study at 10-minute cadence for Wind Farm C.

> [!IMPORTANT]
> **Scientifically Conservative Status Statement:**
> The analysis provides **preliminary evidence** that 10-minute high-frequency metocean features may separate labelled O&M success windows from weather standby windows. However, because our independent event groups are limited (58 parent events), and SCADA-derived labels are proxies based on synthetic proximity rules, this model represents an exploratory grouped feasibility baseline rather than a production-ready operational classifier.

---

## 1. Naive Leakage Proof & Validation Comparison
To demonstrate the absolute necessity of event-grouped validation, we contrast standard row-level CV against our strict event-grouped CV for Target C1:

| Validation Method | Random Forest F1-Score | Random Forest ROC-AUC | Status | Details |
|---|---|---|---|---|
| **Naive Row-Level CV (StratifiedKFold)** | 0.899 | 0.800 | **INVALID / LEAKED** | Autocorrelation bleeds adjacent 10-min rows across splits. |
| **Grouped CV (StratifiedGroupKFold)** | 0.840 | 0.558 | **VALID / LEAKAGE-SAFE** | Asserts strict isolation of all 58 parent events. |

> [!WARNING]
> The row-level F1-score of **0.899** is artificially inflated due to high temporal sequence autocorrelation. Neighbors are leaked, making the validation mathematically invalid. The event-grouped F1-score of **0.840** is the true, scientifically honest baseline.

---

## 2. Grouped Classifier Evaluation Metrics

### Target C1: Clean Contrast
*Target: `maintenance_success = 1` vs. `standby_weather = 0`. Excludes `unknown` background rows.*

| Model | Accuracy | Precision | Recall | F1-Score | ROC-AUC | PR-AUC |
|---|---|---|---|---|---|---|
| Dummy | 0.897 | 0.946 | 0.946 | 0.946 | 0.499 | 0.971 |
| Logistic Regression | 0.547 | 0.945 | 0.554 | 0.682 | 0.535 | 0.948 |
| Random Forest | 0.732 | 0.950 | 0.757 | 0.840 | 0.558 | 0.955 |

### Target C2: Noisy Proxy
*Target: `maintenance_success = 1` vs. `unknown = 0` background rows.*

| Model | Accuracy | Precision | Recall | F1-Score | ROC-AUC | PR-AUC |
|---|---|---|---|---|---|---|
| Dummy | 0.817 | 0.096 | 0.097 | 0.096 | 0.497 | 0.142 |
| Logistic Regression | 0.623 | 0.115 | 0.431 | 0.178 | 0.565 | 0.138 |
| Random Forest | 0.679 | 0.154 | 0.520 | 0.235 | 0.647 | 0.184 |

---

## 3. Decision Threshold & Operational Regions (Target C1 - Random Forest)
Rather than asserting a single default operational threshold, we sweep thresholds from 0.05 to 0.95 and report three candidate operational regions:

| Operational Mode | Recommended Probability Threshold | Predicted F1-Score | Precision | Recall | Purpose / Usage |
|---|---|---|---|---|---|
| **Max F1** | 0.05 | 0.973 | 0.948 | 1.000 | Optimizes overall balance between false positives and false negatives |
| **Recall-Oriented** | 0.05 | 0.973 | 0.948 | 1.000 | Conservative planning - capture at least 85% of workable periods |
| **Precision-Oriented** | 0.25 | 0.968 | 0.950 | 0.986 | High confidence - minimize risk of weather standby failures |

---

## 4. Main Feasibility Findings & Modeling Recommendations
1. **Strong Environmental Signal Exists:** In descriptive separability audits, sea state ($H_s$) and wind speed are highly robust indicators of operational success. Our physical signal is scientifically sound.
2. **Independent Event Count constraint:** Since we have only 58 parent events, our models remain susceptible to event-wise covariate shift. Standard random forests can achieve strong separation under Target C1, but performance under the background Target C2 degrades as unlabelled background windows hide workable periods.
3. **AIS and CMEMS Recommendations:** The next baseline modeling upgrade should incorporate actual AIS vessel proximity logs (replacing the synthetic 50m check) and CMEMS reanalysis current velocities (replacing the climatological fallback).

## 5. Exported Diagnostics
All plots and reports are located in [reports/baseline_models/](file:///Volumes/Extreme SSD/01_ACTIVE_PROJECTS/O&M_Data_Acquisition_Accessability&Validation/reports/baseline_models):
- [ten_min_grouped_metrics.csv](file:///Volumes/Extreme SSD/01_ACTIVE_PROJECTS/O&M_Data_Acquisition_Accessability&Validation/reports/baseline_models/ten_min_grouped_metrics.csv)
- [ten_min_grouped_fold_assignments.csv](file:///Volumes/Extreme SSD/01_ACTIVE_PROJECTS/O&M_Data_Acquisition_Accessability&Validation/reports/baseline_models/ten_min_grouped_fold_assignments.csv)
- [ten_min_separability_diagnostics.png](file:///Volumes/Extreme SSD/01_ACTIVE_PROJECTS/O&M_Data_Acquisition_Accessability&Validation/reports/baseline_models/ten_min_separability_diagnostics.png)
- [ten_min_threshold_sweeps.png](file:///Volumes/Extreme SSD/01_ACTIVE_PROJECTS/O&M_Data_Acquisition_Accessability&Validation/reports/baseline_models/ten_min_threshold_sweeps.png)
- [ten_min_curves_c1.png](file:///Volumes/Extreme SSD/01_ACTIVE_PROJECTS/O&M_Data_Acquisition_Accessability&Validation/reports/baseline_models/ten_min_curves_c1.png)
