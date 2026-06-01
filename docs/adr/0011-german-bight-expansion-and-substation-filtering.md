# 0011. German Bight Expansion and Substation Filtering

## Status
Accepted

## Context
As the O&M dataset matures, we need to expand beyond the initial 20 km Baltic cluster to accurately capture varying operational patterns across different sea states. The German Bight (specifically Alpha Ventus and Trianel Windpark Borkum I & II) presents a prime candidate for expansion. To perform this efficiently, we must reuse our Mode B (Farm-Candidate) extraction methodology on local bulk DMA AIS datasets to generate targeted candidate tracks within a 2-nautical-mile buffer.

Simultaneously, we observed noise in our workability curves caused by vessels undertaking long-duration stays at Offshore Substations (OSS) rather than turbine foundations. Since OSS operations differ fundamentally from turbine maintenance (e.g., larger vessels, different weather limits), they skew the aggregate thresholds if not isolated.

Furthermore, an audit of the initial pipeline results showed 31.6% (172 records) of vessels lacked correct classification (missing dimensional data). To correct this, a deterministic length-based heuristic needed to be formalized and populated from backup local fleet registries.

## Decision
1. **Mode B Local Extraction**: We built a local variant of our spatial extraction pipeline (`filter_local_csv_to_writer`) to parse offline bulk DMA files, applying bounding boxes strictly tailored to the German Bight Mega-Arrays. Projections enforce local UTM tracking (Zone 32N) to avert distortion.
2. **Substation Flagging (`is_substation`)**: We extended the KDTree spatial logic inside `ais_dwell_detector.py` to propagate a boolean `is_substation` flag. If the nearest asset to a dwell is an OSS, the dwell event preserves this flag (`near_substation = True`). This allows downstream analytical workflows to cleanly separate out OSS visits from generic foundation visits.
3. **Formalized Vessel Enrichment**: We implemented `vessel_enrichment.py` to perform explicit length-based binning (`<40m` = CTV, `>=60m` = SOV, else `Medium-sized Vessel`). An offline recovery script joined missing length data from a backup DuckDB `fleet_registry` catalog, successfully clearing the vessel specification void.

## Consequences
- **Positive:** We unlocked multi-regional analysis (Baltic vs. North Sea) without needing massive local storage duplication. The vessel vocabulary is structurally sound, and workability curves can now cleanly exclude OSS-related dwell skew.
- **Negative/Risk:** If the master `turbines` database does not have accurate or timely `is_substation` indicators populated, the downstream logic will default to `False`, rendering the noise filter inactive until the upstream data is explicitly tagged.
