# Vessel Enrichment Plan

**Goal:** Identify sources and fields to map MMSIs to detailed vessel specifications, enriching the basic AIS metadata for better workability modeling.

## 1. Target Variables

To accurately model a vessel's wave height limits, we need more than just length. We aim to enrich the `fleet_registry` with:
- **Vessel Class:** CTV (Crew Transfer Vessel), SOV (Service Operation Vessel), Jack-up, Cable Layer.
- **Dimensions:** Length Overall (LOA), Beam, Maximum Draught.
- **Transfer Tech:** Bow fender type, Gangway availability, Motion Compensated Gangway (e.g., Ampelmann).
- **Positioning:** Dynamic Positioning (DP) class (e.g., DP2).
- **Operator:** Owner or managing company.

## 2. Proposed Data Sources

### Primary Free/Academic Sources
- **Marinetraffic / VesselFinder (Manual Verification):** Excellent for manual ad-hoc checks of top offenders/vessels. *Caution: Automated scraping likely violates their Terms of Service and should not be implemented without a paid API/licensed access.*
- **ITU MARS Database:** Official radio station registry. Often contains length, tonnage, and vessel type classifications.
- **Global Fishing Watch / Public MMSI Registries:** Useful for filtering out non-O&M vessels.

### Specialized Offshore Registries
- **4C Offshore / Westwood:** Industry databases that track specific SOVs and CTVs. (Requires checking institutional access/licensing).
- **Vessel Operator Fleets:** Manually scraping specifications from major operators (e.g., Windcat Workboats, CWind, Edda Wind, Esvagt).

## 3. Enrichment Workflow

1. **Extraction:** Once the `fleet_registry` is finalized for the July pilot, extract the unique list of valid O&M MMSIs.
2. **Automated Fetch:** Query the ITU database (or a generic vessel API if available) to fill in missing `Length`, `Beam`, and `Gross Tonnage`.
3. **Classification Heuristic:** 
   - Length < 40m = Likely CTV
   - Length > 60m & DP2 = Likely SOV
4. **Manual Review:** For the top 50 most active vessels in the pilot, manually verify their specifications via Google/MarineTraffic and construct a highly accurate subset.
5. **Join:** Update the `fleet_registry` table in DuckDB with these enriched fields.
