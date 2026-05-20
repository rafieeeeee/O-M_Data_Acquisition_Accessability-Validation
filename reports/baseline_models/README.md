# Baseline Classifier Models — Report Summary
Generated: 2026-05-20  
Pipeline: `scripts/train_wind_farm_c_baseline.py`

## Task Metrics

| Task | Model | Accuracy | Precision | Recall | F1-Score | ROC-AUC |
|------|-------|----------|-----------|--------|----------|---------|
| Task A (CARE Anomaly) | Random Forest | 0.829 | 0.813 | 0.820 | 0.814 | 0.920 |
| Task A (CARE Anomaly) | Logistic Regression | 0.761 | 0.793 | 0.607 | 0.670 | 0.728 |
| Task A (CARE Anomaly) | SVM | 0.847 | 0.950 | 0.713 | 0.808 | 0.846 |
| Task B (Workability) | Random Forest | 0.914 | 0.000 | 0.000 | 0.000 | 0.936 |
| Task B (Workability) | Logistic Regression | 0.897 | 0.400 | 0.400 | 0.400 | 0.867 |
| Task B (Workability) | SVM | 0.932 | 0.400 | 0.400 | 0.400 | 0.505 |

## Key Findings & Interpretation

### Task A: CARE Anomaly Classifier (Diagnostic)
* Explores if weather conditions and SCADA behavior co-vary with CARE-reported fault anomalies.
* Highly predictive due to operational shares (e.g. Service or Downtime durations) indicating the turbine state.
* Useful as a diagnostic check of our de-anonymization and calendar mapping consistency.

### Task B: O&M Workability Boundary (Operational)
* Maps out the vessel workability surface directly from the physical environment to successful O&M handshakes.
* Uses **metocean features only** to prevent operational leakage, representing the true predictive capacity of weather-only workability models.
* Bounded by significant wave height ($H_s$) and wind speed. Refer to `empirical_workability_boundary.png` for the scatter plot layout.

## Exported Artifacts
All plots and metric reports are located in: `reports/baseline_models/`
- [baseline_metrics.csv](file:///Volumes/Extreme SSD/01_ACTIVE_PROJECTS/O&M_Data_Acquisition_Accessability&Validation/reports/baseline_models/baseline_metrics.csv)
- [task_a_roc_curves.png](file:///Volumes/Extreme SSD/01_ACTIVE_PROJECTS/O&M_Data_Acquisition_Accessability&Validation/reports/baseline_models/task_a_roc_curves.png)
- [task_b_roc_curves.png](file:///Volumes/Extreme SSD/01_ACTIVE_PROJECTS/O&M_Data_Acquisition_Accessability&Validation/reports/baseline_models/task_b_roc_curves.png)
- [task_a_feature_importance.png](file:///Volumes/Extreme SSD/01_ACTIVE_PROJECTS/O&M_Data_Acquisition_Accessability&Validation/reports/baseline_models/task_a_feature_importance.png)
- [task_b_feature_importance.png](file:///Volumes/Extreme SSD/01_ACTIVE_PROJECTS/O&M_Data_Acquisition_Accessability&Validation/reports/baseline_models/task_b_feature_importance.png)
- [empirical_workability_boundary.png](file:///Volumes/Extreme SSD/01_ACTIVE_PROJECTS/O&M_Data_Acquisition_Accessability&Validation/reports/baseline_models/empirical_workability_boundary.png)
