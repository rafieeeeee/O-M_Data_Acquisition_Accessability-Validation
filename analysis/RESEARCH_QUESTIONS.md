# Research Questions Index

This index lists the current status and analytical readiness of all 11 core Research Questions (RQs) and the Simulator Test-Bed.

Stage 1 exists as an observed/provisional $H_s \times T_p$ workability surface. Stage 2 has not started; the next modelling branch should use Fusion v2 to compare wave-only, wave+wind speed, wave+current, and wave+wind+current slices before calibrated probability modelling.

| RQ | Folder | Focus | Status | Readiness Level |
| :---: | :--- | :--- | :--- | :--- |
| **RQ1** | [01_rq1_workability_envelope](file:///Volumes/Extreme%20SSD/01_ACTIVE_PROJECTS/O&M_Data_Acquisition_Accessability&Validation/analysis/01_rq1_workability_envelope) | Wave height & period boundaries | `Stage 1 Exists` | Observed/provisional $H_s \times T_p$ surface only; not calibrated `P(operation | weather)`. |
| **RQ2** | [02_rq2_vessel_specific_behaviour](file:///Volumes/Extreme%20SSD/01_ACTIVE_PROJECTS/O&M_Data_Acquisition_Accessability%Validation/analysis/02_rq2_vessel_specific_behaviour) | Continuous dimensions vs. weather | `Not Started` | Requires size/draft metadata checks. |
| **RQ3** | [03_rq3_vessel_technology_trends](file:///Volumes/Extreme%20SSD/01_ACTIVE_PROJECTS/O&M_Data_Acquisition_Accessability%20Validation/analysis/03_rq3_vessel_technology_trends) | Vessel age / build year learning curves | `Not Started` | Low readiness (needs IMO build years). |
| **RQ4** | [04_rq4_abandonment_retry](file:///Volumes/Extreme%20SSD/01_ACTIVE_PROJECTS/O&M_Data_Acquisition_Accessability%20Validation/analysis/04_rq4_abandonment_retry) | Failed transits & short dwells | `Not Started` | Exploratory (needs SCADA/DPR validation). |
| **RQ5** | [05_rq5_campaign_resupply](file:///Volumes/Extreme%20SSD/01_ACTIVE_PROJECTS/O&M_Data_Acquisition_Accessability%20Validation/analysis/05_rq5_campaign_resupply) | SOV stays and port gaps | `Not Started` | Low readiness (needs trajectory tracks). |
| **RQ6** | [06_rq6_metocean_spatial_resolution](file:///Volumes/Extreme%20SSD/01_ACTIVE_PROJECTS/O&M_Data_Acquisition_Accessability%20Validation/analysis/06_rq6_metocean_spatial_resolution) | Multi-node vs centroid weather | `Evidence Layer Ready` | Fusion v2 carries wave, wind speed, current, and bathymetry confidence; missing current stays null and wind direction stays sensitivity-only. |
| **RQ7** | [07_rq7_opportunistic_om](file:///Volumes/Extreme%20SSD/01_ACTIVE_PROJECTS/O&M_Data_Acquisition_Accessability%20Validation/analysis/07_rq7_opportunistic_om) | Grid curtailment & spot market | `Conditional`| Needs SCADA state flags & EPEX Spot. |
| **RQ8** | [08_rq8_return_to_service_priority](file:///Volumes/Extreme%20SSD/01_ACTIVE_PROJECTS/O&M_Data_Acquisition_Accessability%20Validation/analysis/08_rq8_return_to_service_priority) | Wake-adjusted value prioritization | `Conditional`| Needs SCADA events + wake coordinates. |
| **RQ9** | [09_rq9_failure_intervention_intensity](file:///Volumes/Extreme%20SSD/01_ACTIVE_PROJECTS/O&M_Data_Acquisition_Accessability%20Validation/analysis/09_rq9_failure_intervention_intensity) | Physical visits vs theoretical failure | `Conditional`| Needs raw fault / work-order logs. |
| **RQ10** | [10_rq10_oil_gas_benchmark](file:///Volumes/Extreme%20SSD/01_ACTIVE_PROJECTS/O&M_Data_Acquisition_Accessability%20Validation/analysis/10_rq10_oil_gas_benchmark) | Renewables vs O&G logistics | `Exploratory` | Conceptual benchmarking (needs external O&G). |
| **RQ11** | [11_rq11_safety_working_practices](file:///Volumes/Extreme%20SSD/01_ACTIVE_PROJECTS/O&M_Data_Acquisition_Accessability%20Validation/analysis/11_rq11_safety_working_practices) | Transit SOG & light constraints | `Exploratory` | SOG/solar angles matching. |
| **RQ12** | [12_simulator_feature_tests](file:///Volumes/Extreme%20SSD/01_ACTIVE_PROJECTS/O&M_Data_Acquisition_Accessability%20Validation/analysis/12_simulator_feature_tests) | Materiality ablation testbed | `Not Started` | Awaits empirical parameter parameters. |
