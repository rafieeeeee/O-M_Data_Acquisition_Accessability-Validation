# ADR 0008: High-Frequency 10-Minute Grouped Validation Modeling Strategy

## Status
Accepted

## Context
1. **Physical Signal Dilution:** Historically, modeling for offshore Wind Farm C was conducted on collapsed event-level aggregates (~58 events). However, active O&M handshakes often represent only a small fraction (e.g. median 4.3%) of multi-day event windows. Event-level averaging heavily dilutes the physical metocean signals and introduces rough-weather noise, hiding true calm-weather workability.
2. **Temporal Autocorrelation & Data Leakage:** Moving to a 10-minute high-frequency matrix (120,224 rows) increases the nominal dataset size and temporal resolution, but introduces severe validation risks. Successive 10-minute rows within the same parent event are highly correlated. Naive, random row-level cross-validation splits (e.g., standard `KFold` or `StratifiedKFold`) leak neighboring rows across train and validation sets, resulting in artificially inflated performance scores (e.g., F1 > 0.89) that fail to generalize to independent events.
3. **Exploratory Small Effective Sample Size:** While the dataset contains over 120k rows, the *effective independent sample size* is strictly bounded by the 58 parent event groups. Supervised learning campaigns must be framed as a *leakage-safe feasibility and separability study* rather than a mature, final "production classifier."
4. **Circular Discontinuities:** Metocean directions (wind, wave, current) are circular angles in degrees $[0, 360)$. Standard linear modeling or distance-based metrics suffer from wrap-around discontinuities (e.g., $359^\circ$ and $1^\circ$ are physically close but numerically far).

## Decision
We will execute our 10-minute backbone modeling campaign using a strict, leakage-safe, and physically interpretable grouped validation architecture:
1. **Mandatory Event-Grouped Partitioning:** Under no circumstances will row-level splits be used for model validation. We enforce strict event isolation by partitioning data using `StratifiedGroupKFold` (both 3-fold and 5-fold) and Leave-One-Event-Out (LOEO) sensitivity sweeps, grouping strictly by `event_id`.
2. **Pointwise Deterministic Circular Projections:** We pre-process all direction columns (`wave_direction`, `wind_direction_10m`, `wind_direction_100m`, `current_direction`) by projecting them onto the unit circle as orthogonal sine and cosine components:
   $$\sin(\theta) = \sin(\text{deg2rad}(\theta)), \quad \cos(\theta) = \cos(\text{deg2rad}(\theta))$$
   This is mathematically leakage-safe because the projection is pointwise and deterministic.
3. **Strict Fold-Local Preprocessing:** Imputation (`SimpleImputer` using median values) and feature standardization (`StandardScaler`) will be fitted **solely on training folds** inside a scikit-learn `Pipeline` to prevent pre-validation feature leakage.
4. **Dual Target Contrasts (C1 and C2):**
   * **Target C1 (Clean Contrast):** Contrast active O&M successes (`maintenance_success = 1`) directly against downtime weather standbys (`standby_weather = 0`), excluding `unknown` periods.
   * **Target C2 (Noisy Proxy):** Contrast O&M successes (`maintenance_success = 1`) against the general background corpus (`unknown = 0`). This is documented as a noisy negative proxy, acknowledging that the background contains unobserved workable windows.
5. **Simple, Interpretability-First Classifiers:** We restrict models to a Baseline Dummy, class-weighted Logistic Regression, and a shallow class-weighted Random Forest (max_depth=5). Heavy ensemble architectures like XGBoost are completely excluded at this stage to prevent overfitting on the small number of independent events.
6. **Bivariate Threshold Operational Sweeps:** Instead of predicting a single default decision threshold (0.50), we sweep thresholds from 0.05 to 0.95, and formally identify three distinct operational modes (Max F1, Recall-oriented, and Precision-oriented) with associated fold-wise standard deviations.

## Consequences
- **Validation Honesty:** By introducing event-grouped CV, we successfully isolated the autocorrelation leakage. The Random Forest F1-score drops from the naive leaked **0.899** to the true, scientifically honest baseline of **0.840** on Target C1, exposing the inflated nature of naive row-level splits.
- **Simpson's Paradox Discovery:** In descriptive physical separability audits, we discovered that raw pooled wave heights ($H_s$) are higher for O&M successes ($1.65$m) than standby weather ($1.31$m). However, our event-normalized analysis revealed that *within* events, successes occur in calmer sea states in **29 out of 40** events (72.5%). This exposes a major Simpson's Paradox caused by seasonal/event confounding, which mathematically justifies why event-level aggregation diluted the signal.
- **Comprehensive Reporting Artifacts:** The training pipeline automatically generates lineage audits, event audits, physical separability reports, and grouped CV README summary reports under `/reports/baseline_models/`.
- **Automated Validation:** The `tests/test_grouped_modeling.py` suite explicitly asserts group isolation, circular transformation coordinates, target constructions, and zero SCADA operational feature leakage.
