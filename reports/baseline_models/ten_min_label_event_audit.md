# Phase 1: 10-Minute Label and Event Audit (Wind Farm C)
Generated: 2026-05-20  

This phase audits label distributions at both the row and parent-event levels to determine if supervised learning is scientifically viable.

## 1. Row-Level Summary Statistics
* **Total Rows:** 120,224
* **Label Distribution:**
  - `unknown` (No active maintenance): 107,486 rows (89.40%)
  - `maintenance_success` (Active calm weather O&M): 12,078 rows (10.05%)
  - `standby_weather` (Downtime weather standby): 660 rows (0.55%)
* **SCADA Status Code Distribution:**
  - Code 0.0: 105,947 rows
  - Code 3.0: 12,078 rows
  - Code 5.0: 1,397 rows
  - Code 4.0: 660 rows
  - Code nan: 142 rows

## 2. Parent Event-Level Summary Statistics
* **Total Parent Events:** 58
* **Event Duration Distribution (Hours):**
  - Min: 41.00h
  - Median: 312.75h
  - Max: 1572.50h
  - Mean: 345.47h
* **Active O&M Event Representation:**
  - Events with $\ge 1$ `maintenance_success` row: 41 events
  - Events with $\ge 1$ `standby_weather` row: 47 events
  - Events containing both labels: 40 events

## 3. Critical Viability Gates & Modeling Decision

We evaluate five explicit baseline gates before proceeding:

| Viability Gate | Metric Analyzed | Threshold | Status | Details |
|---|---|---|---|---|
| **Gate 1: Positive Event Count** | Events with $\ge 1$ success row | $\ge 5$ | **PASSED** | 41 events contain O&M successes |
| **Gate 2: Negative Event Count** | Events with $\ge 1$ standby row | $\ge 5$ | **PASSED** | 47 events contain standby status |
| **Gate 3: Label Concentration** | Share of positives in top 3 events | $\le 80\%$ | **PASSED** | Top 3 events contain 46.2% of success rows |
| **Gate 4: Partition Feasibility** | Classes spread across $\ge 3$ folds | Yes | **PASSED** | Folds successfully balanced (minimum of 5 events/class per fold) |
| **Gate 5: Autocorrelation Run-length** | Max consecutive single-label run | $\le 20\%$ of rows | **PASSED** | Max consecutive success run is 1,268 rows (1.1%) |

> [!TIP]
> **Conclusion:** All five viability gates have successfully passed. This confirms that the dataset contains sufficient event-level class balance and distribution to support a grouped validation modeling pipeline.
