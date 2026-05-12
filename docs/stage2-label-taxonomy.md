# Stage 2 Label Taxonomy

**Goal:** Define event labels and rules for categorizing offshore O&M vessel activities (the "10-Minute Backbone" target states).

## 1. Context
Once AIS dwell events are identified and metocean data is merged, the ultimate goal is to classify each event window into an operational state. This taxonomy provides the ground-truth structure for the vessel workability models.

## 2. Event Labels

### `maintenance_success` (Active Maintenance)
- **Definition:** The vessel successfully transferred personnel/equipment to the foundation and executed the planned maintenance task.
- **Criteria:** 
  - AIS: Dwell event duration > 30 minutes, minimal SOG, proximity < 100m.
  - SCADA (If available): Turbine status indicates 'Maintenance' or manual stop.
  - Metocean: Conditions typically within operational limits.

### `attempted_transfer` (Failed/Aborted)
- **Definition:** The vessel approached the foundation but failed to maintain the connection or aborted the transfer due to conditions.
- **Criteria:**
  - AIS: Short dwell event (e.g., 5-15 mins) or multiple repeated approaches in a short window.
  - Metocean: Conditions likely near or slightly above expected operational limits.

### `standby_weather` (Wait-on-Weather / WoW)
- **Definition:** The vessel is near the wind farm but idling, waiting for a weather window to open.
- **Criteria:**
  - AIS: Vessel is within the wind farm buffer but not within 100m of a foundation. Loitering behavior (low SOG) for extended periods (e.g., > 1 hour).
  - SCADA: Turbine may be faulty but no maintenance is active.

### `cancelled_weather`
- **Definition:** A planned operation was cancelled entirely before the vessel reached the site, or the vessel returned to port immediately.
- **Criteria:** (Mostly derived from Daily Progress Reports - DPRs - rather than strictly AIS, but can manifest as truncated journeys).

### `unknown`
- **Definition:** Default state for events that do not cleanly fit the above, or lack SCADA validation.
- **Criteria:** Dwell events that meet distance/speed thresholds but have highly irregular durations or lack corresponding SCADA drops.

## 3. Implementation Rules
During Stage 2, the labeling pipeline will read the `dwell_events` table and apply these heuristic rules, outputting a new `label` column. Future model training will aim to predict these labels based purely on metocean and vessel characteristic inputs.
