# Baseline Classifier Models — Report Summary
Generated: 2026-05-20  
Pipeline: `scripts/train_wind_farm_c_baseline.py`

> [!NOTE]
> **Scientific Summary & Status Statement:**
> We implemented a baseline diagnostic modeling pipeline. Task A shows promising but leakage-prone diagnostic separability. Task B shows high ROC-AUC ranking under extreme class imbalance, but default-threshold classification fails for Random Forest and the target remains a proxy because AIS proximity is synthetic. Results are exploratory and should guide the next grouped 10-minute modeling experiment, not be treated as thesis-grade evidence yet.

## Diagnostic Classifier Metrics Table

| Task | Model | Accuracy | Precision | Recall | F1-Score | ROC-AUC |
|------|-------|----------|-----------|--------|----------|---------|
| Task A (CARE Anomaly) | Random Forest | 0.829 | 0.813 | 0.820 | 0.814 | 0.920 |
| Task A (CARE Anomaly) | Logistic Regression | 0.761 | 0.793 | 0.607 | 0.670 | 0.728 |
| Task A (CARE Anomaly) | SVM | 0.847 | 0.950 | 0.713 | 0.808 | 0.846 |
| Task B (Workability - Dominant) | Random Forest | 0.914 | 0.000 | 0.000 | 0.000 | 0.936 |
| Task B (Workability - Dominant) | Logistic Regression | 0.897 | 0.400 | 0.400 | 0.400 | 0.867 |
| Task B (Workability - Dominant) | SVM | 0.932 | 0.400 | 0.400 | 0.400 | 0.505 |
| Task B_alt (Workability - Presence) | Random Forest | 0.656 | 0.699 | 0.903 | 0.786 | 0.494 |
| Task B_alt (Workability - Presence) | Logistic Regression | 0.535 | 0.750 | 0.531 | 0.601 | 0.529 |
| Task B_alt (Workability - Presence) | SVM | 0.573 | 0.721 | 0.633 | 0.672 | 0.497 |

## Key Findings & Critical Research Caveats

### 1. Task A: CARE Anomaly Classifier (Diagnostic)
* **Status:** Exploratory and highly prone to event-definition leakage.
* **Caveat:** Uses SCADA operational state summaries (e.g. status duration and label shares) from the *same* event windows. The high classification scores reflect turbine-state mapping consistency and calendar validation, rather than a purely weather-driven predictive anomaly result.

### 2. Task B: O&M Workability Boundary (Dominant vs. Presence Targets)
* **Dominant Target (`event_label_model == "maintenance_success"`):**
  * Extreme class imbalance (53 unknown / 5 maintenance_success). In a 5-Fold Cross-Validation, each fold contains only a single positive test event, making the ROC-AUC extremely fragile and prone to sharp swings based on individual ranks.
  * While the Random Forest achieves a high ranking ROC-AUC of 0.936, at the default decision threshold (0.50) it predicts zero positive workability events, yielding **F1, Precision, and Recall scores of 0.000**.
  * The target remains a proxy. Proximity checks depend on a synthetic `min_dist = 50m` assumption rather than real AIS proximity.
* **Presence Target (`share_label_maintenance_success > 0`):**
  * Alleviates the class split to `[17, 41]`, but collapses prediction metrics to random-guessing levels (ROC-AUC ~0.50). 
  * **Dilution effect:** Averaging weather features across multi-day event windows can strongly dilute weather features and introduce rough-weather noise, hiding the fine-grained calm workability windows where active O&M successfully took place.

### 3. Autocorrelation & Modeling Roadmap
* While transitioning modeling directly to the 120,224-row 10-minute backbone solves class sparsity, **grouped splits by `event_id` or time blocks are absolutely mandatory**.
* Failing to group splits will result in high-frequency temporal autocorrelation leakage, where neighboring 10-minute rows are split across train and test sets, invalidating cross-validation scores.

## Exported Artifacts
All plots and metric reports are located in: `reports/baseline_models/`
- [baseline_metrics.csv](file:///Volumes/Extreme SSD/01_ACTIVE_PROJECTS/O&M_Data_Acquisition_Accessability&Validation/reports/baseline_models/baseline_metrics.csv)
- [task_a_roc_curves.png](file:///Volumes/Extreme SSD/01_ACTIVE_PROJECTS/O&M_Data_Acquisition_Accessability&Validation/reports/baseline_models/task_a_roc_curves.png)
- [task_b_roc_curves.png](file:///Volumes/Extreme SSD/01_ACTIVE_PROJECTS/O&M_Data_Acquisition_Accessability&Validation/reports/baseline_models/task_b_roc_curves.png)
- [task_a_feature_importance.png](file:///Volumes/Extreme SSD/01_ACTIVE_PROJECTS/O&M_Data_Acquisition_Accessability&Validation/reports/baseline_models/task_a_feature_importance.png)
- [task_b_feature_importance.png](file:///Volumes/Extreme SSD/01_ACTIVE_PROJECTS/O&M_Data_Acquisition_Accessability&Validation/reports/baseline_models/task_b_feature_importance.png)
- [empirical_workability_boundary.png](file:///Volumes/Extreme SSD/01_ACTIVE_PROJECTS/O&M_Data_Acquisition_Accessability&Validation/reports/baseline_models/empirical_workability_boundary.png)
