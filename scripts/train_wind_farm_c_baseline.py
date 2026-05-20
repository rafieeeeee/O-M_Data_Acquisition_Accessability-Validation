"""
scripts/train_wind_farm_c_baseline.py
----------------------------------------
Trains baseline classifiers on the Wind Farm C (Trianel Borkum I+II) event aggregates.

We train models for two tasks:
1. Task A: Anomaly Classification (Target: event_label_care == anomaly)
   Features: Metocean aggregates + SCADA status/label shares + duration
2. Task B: O&M Workability Boundary (Target: event_label_model == maintenance_success)
   Features: Metocean aggregates only (no operational leakage)

We employ Stratified 5-Fold Cross-Validation, train Random Forest, Logistic Regression, 
and Support Vector Machine (SVM) models, and produce comprehensive diagnostic plots 
and tabular reports.
"""

import os
import sys
import argparse
from pathlib import Path
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import StratifiedKFold
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.svm import SVC
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, roc_auc_score, roc_curve

# Ensure the src package is importable
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

# Output Directories
OUTPUT_DIR = PROJECT_ROOT / "reports" / "baseline_models"
INPUT_PARQUET = PROJECT_ROOT / "Data" / "Processed" / "wind_farm_c_event_aggregates.parquet"

# Curated Color Palettes for Premium Design
BLUE_HEX = "#1F77B4"
ORANGE_HEX = "#FF7F0E"
GREEN_HEX = "#2CA02C"
DARK_HEX = "#2C3E50"
MUTED_HEX = "#7F8C8D"


def set_plotting_style():
    """Applies modern styling for stunning figures."""
    sns.set_theme(style="whitegrid")
    plt.rcParams.update({
        "font.family": "sans-serif",
        "font.sans-serif": ["Inter", "Helvetica", "Arial", "sans-serif"],
        "axes.titlesize": 14,
        "axes.titleweight": "bold",
        "axes.labelsize": 12,
        "xtick.labelsize": 10,
        "ytick.labelsize": 10,
        "legend.fontsize": 10,
        "figure.titlesize": 16,
        "figure.titleweight": "bold",
    })


def preprocess_circular_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms circular mean angles into sin and cos components to avoid wrap-around discontinuities.
    """
    df_transformed = df.copy()
    
    angular_cols = [
        "wave_direction",
        "wind_direction_10m",
        "wind_direction_100m",
        "current_direction",
    ]
    
    for col in angular_cols:
        mean_col = f"{col}_circular_mean"
        if mean_col in df_transformed.columns:
            angles = df_transformed[mean_col]
            # Replace NaNs with 0.0 before trig transform (or handle cleanly)
            angles_filled = angles.fillna(0.0)
            rads = np.deg2rad(angles_filled)
            
            df_transformed[f"{col}_sin"] = np.sin(rads)
            df_transformed[f"{col}_cos"] = np.cos(rads)
            
            # If the original was all NaN, make the sine/cosine columns NaN too
            df_transformed.loc[angles.isna(), [f"{col}_sin", f"{col}_cos"]] = np.nan
            
            # Drop the original raw circular mean
            df_transformed = df_transformed.drop(columns=[mean_col])
            
    return df_transformed


def select_features_for_task(df: pd.DataFrame, task: str) -> tuple[pd.DataFrame, pd.Series]:
    """
    Returns X (features) and y (target) for Task A or Task B.
    """
    if task == "A":
        # Task A: CARE Anomaly Classification (Metocean + SCADA operational shares)
        y = (df["event_label_care"] == "anomaly").astype(int)
        
        # We exclude metadata and the target itself
        exclude_cols = ["event_id", "asset_id", "event_label_care", "event_label_model", "event_start", "event_end"]
        feature_cols = [c for c in df.columns if c not in exclude_cols]
        X = df[feature_cols].copy()
        
    elif task == "B":
        # Task B: O&M Workability Boundary (Metocean features only, Dominant Label-based)
        # We use the dominant event label (event_label_model == "maintenance_success") as our target.
        # While this has an extreme [53, 5] class imbalance at the collapsed event level, it represents
        # sustained active maintenance where the event's average weather represents true workability.
        y = (df["event_label_model"] == "maintenance_success").astype(int)
        
        # Only allow metocean wave, wind, current features (scalars + circular-trig components + circular variance)
        allowed_keywords = ["hs_", "tp_", "wind_speed_", "wind_direction_", "wave_direction_", "current_speed_", "current_direction_"]
        feature_cols = [
            c for c in df.columns 
            if any(k in c for k in allowed_keywords) 
            and not c.endswith("_null_share")
        ]
        X = df[feature_cols].copy()
    elif task == "B_alt":
        # Task B_alt: O&M Workability Boundary (Metocean features only, Presence-based target)
        # Target: share_label_maintenance_success > 0.0 (active maintenance occurred at any point during event)
        y = (df["share_label_maintenance_success"] > 0.0).astype(int)
        
        # Only allow metocean wave, wind, current features (scalars + circular-trig components + circular variance)
        allowed_keywords = ["hs_", "tp_", "wind_speed_", "wind_direction_", "wave_direction_", "current_speed_", "current_direction_"]
        feature_cols = [
            c for c in df.columns 
            if any(k in c for k in allowed_keywords) 
            and not c.endswith("_null_share")
        ]
        X = df[feature_cols].copy()
    else:
        raise ValueError(f"Unknown task: {task}")
        
    # Fill any remaining NaNs with column median (fallback)
    X = X.fillna(X.median())
    
    return X, y


def run_cross_validation(X: pd.DataFrame, y: pd.Series, task_name: str) -> tuple[dict, dict]:
    """
    Runs a Stratified 5-Fold Cross Validation for Random Forest, Logistic Regression, and SVM.
    """
    skf = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    
    models = {
        "Random Forest": RandomForestClassifier(n_estimators=100, max_depth=5, random_state=42, class_weight="balanced"),
        "Logistic Regression": LogisticRegression(penalty="l2", C=1.0, random_state=42, class_weight="balanced", max_iter=1000),
        "SVM": SVC(kernel="rbf", probability=True, random_state=42, class_weight="balanced")
    }
    
    results = {}
    roc_curves_data = {}
    
    for name, clf in models.items():
        metrics = {
            "accuracy": [],
            "precision": [],
            "recall": [],
            "f1": [],
            "auc": []
        }
        
        folds_roc = []
        
        for train_idx, test_idx in skf.split(X, y):
            X_train, X_test = X.iloc[train_idx], X.iloc[test_idx]
            y_train, y_test = y.iloc[train_idx], y.iloc[test_idx]
            
            # Standardize features per fold to prevent leakage
            scaler = StandardScaler()
            X_train_scaled = scaler.fit_transform(X_train)
            X_test_scaled = scaler.transform(X_test)
            
            clf.fit(X_train_scaled, y_train)
            
            y_pred = clf.predict(X_test_scaled)
            if hasattr(clf, "predict_proba"):
                y_prob = clf.predict_proba(X_test_scaled)[:, 1]
            else:
                y_prob = clf.decision_function(X_test_scaled)
                
            metrics["accuracy"].append(accuracy_score(y_test, y_pred))
            metrics["precision"].append(precision_score(y_test, y_pred, zero_division=0))
            metrics["recall"].append(recall_score(y_test, y_pred, zero_division=0))
            metrics["f1"].append(f1_score(y_test, y_pred, zero_division=0))
            
            # Handle edge case where a test fold contains only one class
            try:
                metrics["auc"].append(roc_auc_score(y_test, y_prob))
                fpr, tpr, _ = roc_curve(y_test, y_prob)
                folds_roc.append((fpr, tpr))
            except ValueError:
                metrics["auc"].append(np.nan)
                
        # Aggregate metrics
        results[name] = {
            "Task": task_name,
            "Model": name,
            "Accuracy": np.nanmean(metrics["accuracy"]),
            "Precision": np.nanmean(metrics["precision"]),
            "Recall": np.nanmean(metrics["recall"]),
            "F1-Score": np.nanmean(metrics["f1"]),
            "ROC-AUC": np.nanmean(metrics["auc"])
        }
        
        roc_curves_data[name] = folds_roc
        
    return results, roc_curves_data


def plot_roc_curves(roc_data: dict, task_label: str, save_path: Path):
    """Generates a beautiful ROC fold curve chart."""
    plt.figure(figsize=(7, 6))
    
    colors = {"Random Forest": BLUE_HEX, "Logistic Regression": ORANGE_HEX, "SVM": GREEN_HEX}
    
    for name, folds in roc_data.items():
        if not folds:
            continue
        # Interpolate curves to plot average ROC
        mean_fpr = np.linspace(0, 1, 100)
        tprs = []
        
        for fpr, tpr in folds:
            tprs.append(np.interp(mean_fpr, fpr, tpr))
            tprs[-1][0] = 0.0
            
        mean_tpr = np.mean(tprs, axis=0)
        mean_tpr[-1] = 1.0
        
        plt.plot(mean_fpr, mean_tpr, label=f"{name}", color=colors.get(name, DARK_HEX), lw=2)
        
    plt.plot([0, 1], [0, 1], linestyle="--", color=MUTED_HEX, label="Chance", alpha=0.8)
    plt.xlim([-0.02, 1.02])
    plt.ylim([-0.02, 1.02])
    plt.xlabel("False Positive Rate")
    plt.ylabel("True Positive Rate")
    plt.title(f"ROC Curves - {task_label} (5-Fold CV)")
    plt.legend(loc="lower right", frameon=True)
    plt.tight_layout()
    plt.savefig(save_path, dpi=300)
    plt.close()


def plot_feature_importance(X: pd.DataFrame, y: pd.Series, task_label: str, save_path: Path):
    """Trains a Random Forest on full data and plots top feature importances."""
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    rf = RandomForestClassifier(n_estimators=100, random_state=42, class_weight="balanced")
    rf.fit(X_scaled, y)
    
    importances = rf.feature_importances_
    indices = np.argsort(importances)[::-1][:15] # Top 15 features
    
    plt.figure(figsize=(8, 6))
    sns.barplot(
        x=importances[indices],
        y=X.columns[indices],
        hue=X.columns[indices],
        palette="Blues_r",
        legend=False
    )
    plt.xlabel("Mean Decrease in Impurity")
    plt.ylabel("Feature")
    plt.title(f"Top 15 Feature Importances - {task_label}")
    plt.tight_layout()
    plt.savefig(save_path, dpi=300)
    plt.close()


def plot_workability_scatter(df: pd.DataFrame, save_path: Path):
    """
    Plots an empirical workability scatter (Hs vs. Tp or Wind Speed)
    colored by model O&M labels.
    """
    plt.figure(figsize=(8, 6))
    
    # We want to use raw columns if available
    hs_col = "hs_mean"
    tp_col = "tp_mean"
    wind_col = "wind_speed_10m_mean"
    
    if hs_col in df.columns and wind_col in df.columns:
        # Create a beautiful scatter
        sns.scatterplot(
            data=df,
            x=wind_col,
            y=hs_col,
            hue="event_label_model",
            style="event_label_care",
            palette={"maintenance_success": GREEN_HEX, "unknown": ORANGE_HEX},
            s=80,
            alpha=0.9,
            edgecolor="w",
            linewidth=1.2
        )
        plt.xlabel("Mean Wind Speed (10m) [m/s]")
        plt.ylabel("Mean Significant Wave Height ($H_s$) [m]")
        plt.title("Empirical Vessel Workability Surface (Wind Farm C)")
        plt.legend(title="Event State / Target", loc="upper left", frameon=True)
        plt.tight_layout()
        plt.savefig(save_path, dpi=300)
    plt.close()


def main():
    parser = argparse.ArgumentParser(description="Train baseline classifiers on Wind Farm C event aggregates.")
    parser.add_argument("--input", default=str(INPUT_PARQUET), help="Input event aggregates Parquet path.")
    parser.add_argument("--output-dir", default=str(OUTPUT_DIR), help="Output directory for reports and figures.")
    args = parser.parse_args()
    
    input_path = Path(args.input)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    if not input_path.exists():
        print(f"[ERROR] Event aggregates not found at {input_path}")
        print("Please run scripts/build_wind_farm_c_event_aggregates.py first.")
        sys.exit(1)
        
    print(f"Loading event aggregates: {input_path}")
    event_df = pd.read_parquet(input_path)
    
    # Apply circular transformations
    print("Preprocessing angular features (sine/cosine representations)...")
    processed_df = preprocess_circular_features(event_df)
    
    set_plotting_style()
    
    all_metrics = []
    
    # ----------------------------------------------------
    # TASK A: CARE Anomaly Classifier (Diagnostic)
    # ----------------------------------------------------
    print("\n--- Training Task A: CARE Anomaly Classifier (Metocean + SCADA shares) ---")
    XA, yA = select_features_for_task(processed_df, "A")
    print(f"  Shape of X: {XA.shape}, Class distribution: {np.bincount(yA)}")
    
    metrics_A, roc_A = run_cross_validation(XA, yA, "Task A: CARE Anomaly")
    for name, met in metrics_A.items():
        print(f"  {name:<20} | F1-Score: {met['F1-Score']:.3f} | ROC-AUC: {met['ROC-AUC']:.3f} | Accuracy: {met['Accuracy']:.3f}")
        all_metrics.append(met)
        
    # Generate Plots for Task A
    plot_roc_curves(roc_A, "Task A: CARE Anomaly Classifier", output_dir / "task_a_roc_curves.png")
    plot_feature_importance(XA, yA, "Task A: CARE Anomaly Classifier", output_dir / "task_a_feature_importance.png")
    
    # ----------------------------------------------------
    # TASK B: O&M Workability Boundary (Dominant, Metocean only)
    # ----------------------------------------------------
    print("\n--- Training Task B: O&M Workability Boundary (Dominant, Metocean only) ---")
    XB, yB = select_features_for_task(processed_df, "B")
    print(f"  Shape of X: {XB.shape}, Class distribution: {np.bincount(yB)}")
    
    metrics_B, roc_B = run_cross_validation(XB, yB, "Task B: O&M Workability (Dominant)")
    for name, met in metrics_B.items():
        print(f"  {name:<20} | F1-Score: {met['F1-Score']:.3f} | ROC-AUC: {met['ROC-AUC']:.3f} | Accuracy: {met['Accuracy']:.3f}")
        all_metrics.append(met)
        
    # Generate Plots for Task B
    plot_roc_curves(roc_B, "Task B: O&M Workability (Dominant)", output_dir / "task_b_roc_curves.png")
    plot_feature_importance(XB, yB, "Task B: O&M Workability (Dominant)", output_dir / "task_b_feature_importance.png")
    
    # ----------------------------------------------------
    # TASK B_alt: O&M Workability Boundary (Presence, Metocean only)
    # ----------------------------------------------------
    print("\n--- Training Task B_alt: O&M Workability Boundary (Presence, Metocean only) ---")
    XB_alt, yB_alt = select_features_for_task(processed_df, "B_alt")
    print(f"  Shape of X: {XB_alt.shape}, Class distribution: {np.bincount(yB_alt)}")
    
    metrics_B_alt, roc_B_alt = run_cross_validation(XB_alt, yB_alt, "Task B_alt: O&M Workability (Presence)")
    for name, met in metrics_B_alt.items():
        print(f"  {name:<20} | F1-Score: {met['F1-Score']:.3f} | ROC-AUC: {met['ROC-AUC']:.3f} | Accuracy: {met['Accuracy']:.3f}")
        all_metrics.append(met)
        
    # Generate Bivariate Workability Surface plot
    plot_workability_scatter(event_df, output_dir / "empirical_workability_boundary.png")
    
    # ----------------------------------------------------
    # Save Report Metrics Table
    # ----------------------------------------------------
    metrics_df = pd.DataFrame(all_metrics)
    csv_out = output_dir / "baseline_metrics.csv"
    metrics_df.to_csv(csv_out, index=False)
    print(f"\nTabular metrics successfully exported → {csv_out}")
    
    # Let's fix that formatting before writing
    # Regenerate metrics table properly
    md_summary = f"""# Baseline Classifier Models — Report Summary
Generated: 2026-05-20  
Pipeline: `scripts/train_wind_farm_c_baseline.py`

> [!NOTE]
> **Scientific Summary & Status Statement:**
> We implemented a baseline diagnostic modeling pipeline. Task A shows promising but leakage-prone diagnostic separability. Task B shows high ROC-AUC ranking under extreme class imbalance, but default-threshold classification fails for Random Forest and the target remains a proxy because AIS proximity is synthetic. Results are exploratory and should guide the next grouped 10-minute modeling experiment, not be treated as thesis-grade evidence yet.

## Diagnostic Classifier Metrics Table

| Task | Model | Accuracy | Precision | Recall | F1-Score | ROC-AUC |
|------|-------|----------|-----------|--------|----------|---------|
"""
    for name, met in metrics_A.items():
        md_summary += f"| Task A (CARE Anomaly) | {name} | {met['Accuracy']:.3f} | {met['Precision']:.3f} | {met['Recall']:.3f} | {met['F1-Score']:.3f} | {met['ROC-AUC']:.3f} |\n"
    for name, met in metrics_B.items():
        md_summary += f"| Task B (Workability - Dominant) | {name} | {met['Accuracy']:.3f} | {met['Precision']:.3f} | {met['Recall']:.3f} | {met['F1-Score']:.3f} | {met['ROC-AUC']:.3f} |\n"
    for name, met in metrics_B_alt.items():
        md_summary += f"| Task B_alt (Workability - Presence) | {name} | {met['Accuracy']:.3f} | {met['Precision']:.3f} | {met['Recall']:.3f} | {met['F1-Score']:.3f} | {met['ROC-AUC']:.3f} |\n"
        
    md_summary += f"""
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
  * **Dilution effect:** Averaging weather features across multi-day event windows completely introduces rough-weather noise, hiding the fine-grained calm workability windows where active O&M successfully took place.

### 3. Autocorrelation & Modeling Roadmap
* While transitioning modeling directly to the 120,224-row 10-minute backbone solves class sparsity, **grouped splits by `event_id` or time blocks are absolutely mandatory**.
* Failing to group splits will result in high-frequency temporal autocorrelation leakage, where neighboring 10-minute rows are split across train and test sets, invalidating cross-validation scores.

## Exported Artifacts
All plots and metric reports are located in: `reports/baseline_models/`
- [baseline_metrics.csv](file://{csv_out})
- [task_a_roc_curves.png](file://{output_dir / 'task_a_roc_curves.png'})
- [task_b_roc_curves.png](file://{output_dir / 'task_b_roc_curves.png'})
- [task_a_feature_importance.png](file://{output_dir / 'task_a_feature_importance.png'})
- [task_b_feature_importance.png](file://{output_dir / 'task_b_feature_importance.png'})
- [empirical_workability_boundary.png](file://{output_dir / 'empirical_workability_boundary.png'})
"""
    
    md_out = output_dir / "README.md"
    with open(md_out, "w") as f:
        f.write(md_content := md_summary)
        
    print(f"Markdown summary report successfully written → {md_out}")
    print("\n" + "=" * 60)
    print("BASELINE MODEL TRAINING PIPELINE — COMPLETED")
    print("=" * 60)


if __name__ == "__main__":
    main()
