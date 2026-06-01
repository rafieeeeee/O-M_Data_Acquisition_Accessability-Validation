# Bathymetry Assignment Planning Dry-Run

Status: dry-run planning only. No bathymetry rasters, final point tables, currents, FINO data, source fusion, NORA3 reruns, or dwell-metocean rebuilds were run.

## Executive Conclusion

Bathymetry assignment is ready for a scoped pilot after source-tile acquisition is explicitly approved. EMODnet is the planned primary source for all common metocean farm/sample points, with GEBCO_2026 reserved for fallback and cross-checks.

## Input Inventory

- requirements_path: `analysis/06_rq6_metocean_spatial_resolution/common_metocean_farm_requirements.csv`
- input_requirements_row_count: `119`
- farm_count: `119`
- sample_point_count: `6642`
- review_required_farm_count: `4`
- coordinate_bounds: lon `-4.1673` to `14.4298`, lat `46.8706` to `59.4006`

## Source Strategy

- primary_source: `emodnet` (EMODnet Bathymetry DTM)
- primary_source_version: `active EMODnet DTM release, confirm exact vintage at download`
- fallback_source: `gebco_2026` (GEBCO_2026 Grid)
- fallback_source_version: `GEBCO_2026`
- EMODnet coverage expectation: expected across European offshore wind points in the current common requirements table.
- GEBCO_2026 fallback use cases: missing EMODnet cells, failed tile access, ambiguous source metadata, or independent cross-check.

## Region Plan

| region                  |   farm_count |   sample_point_count |   min_lon |   max_lon |   min_lat |   max_lat | planned_primary_source   | planned_fallback_source   |
|:------------------------|-------------:|---------------------:|----------:|----------:|----------:|----------:|:-------------------------|:--------------------------|
| Baltic / Belt Seas      |           17 |                  772 |   10.102  |   14.4298 |   54.2752 |   57.6976 | emodnet                  | gebco_2026                |
| Channel / Atlantic edge |            4 |                  333 |   -2.9487 |    0.5437 |   46.8706 |   50.9546 | emodnet                  | gebco_2026                |
| European shelf          |           22 |                 1659 |   -1.3694 |    2.8183 |   51.1538 |   54.9045 | emodnet                  | gebco_2026                |
| North Sea / Skagerrak   |           33 |                 1637 |    4.7639 |    8.5061 |   53.432  |   59.4006 | emodnet                  | gebco_2026                |
| Southern North Sea      |           20 |                 1090 |    2.4563 |    6.3439 |   51.2403 |   54.3243 | emodnet                  | gebco_2026                |
| UK shelf / Irish Sea    |           23 |                 1151 |   -4.1673 |   -1.0823 |   53.1187 |   58.5725 | emodnet                  | gebco_2026                |

## Farm Source Plan

| wind_farm                            | country        | region                  |   sample_point_count | planned_primary_source   | planned_fallback_source   | coverage_expectation                                      |
|:-------------------------------------|:---------------|:------------------------|---------------------:|:-------------------------|:--------------------------|:----------------------------------------------------------|
| Aberdeen Offshore Wind Farm          | United Kingdom | UK shelf / Irish Sea    |                   12 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Albatros                             | Germany        | North Sea / Skagerrak   |                   17 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Alpha Ventus                         | Germany        | North Sea / Skagerrak   |                   13 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Amrumbank West                       | Germany        | North Sea / Skagerrak   |                   81 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Anholt                               | Denmark        | Baltic / Belt Seas      |                  112 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Arcadis Ost 1                        | Germany        | Baltic / Belt Seas      |                   28 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Arkona-Becken Südost                 | Germany        | Baltic / Belt Seas      |                   61 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Avedøre Holme                        | Denmark        | Baltic / Belt Seas      |                    4 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Baltic Eagle                         | Germany        | Baltic / Belt Seas      |                   51 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Bard Offshore 1                      | Germany        | North Sea / Skagerrak   |                   81 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Barrow                               | United Kingdom | UK shelf / Irish Sea    |                   31 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Beatrice Offshore Wind Farm          | United Kingdom | UK shelf / Irish Sea    |                   85 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Belwind phase 1                      | Belgium        | Southern North Sea      |                   56 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Belwind phase 2                      | Belgium        | Southern North Sea      |                    2 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Blyth Demo Phase 1                   | United Kingdom | UK shelf / Irish Sea    |                    6 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Borkum Riffgrund 1                   | Germany        | North Sea / Skagerrak   |                   79 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Borkum Riffgrund 2                   | Germany        | North Sea / Skagerrak   |                   57 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Borssele Kavel I and II              | Netherlands    | Southern North Sea      |                   95 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Borssele Kavel III and IV            | Netherlands    | Southern North Sea      |                   78 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Borssele Kavel V                     | Netherlands    | Southern North Sea      |                    3 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Burbo Bank                           | United Kingdom | UK shelf / Irish Sea    |                   26 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Burbo Bank Extension                 | United Kingdom | UK shelf / Irish Sea    |                   33 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Butendiek                            | Germany        | North Sea / Skagerrak   |                   81 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Dan Tysk                             | Germany        | North Sea / Skagerrak   |                   81 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Deutsche Bucht                       | Germany        | North Sea / Skagerrak   |                   32 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Dudgeon                              | United Kingdom | European shelf          |                   68 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| East Anglia One                      | United Kingdom | European shelf          |                  103 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| EnBW Hohe See                        | Germany        | North Sea / Skagerrak   |                   72 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| EnBW Windpark Baltic 1               | Germany        | Baltic / Belt Seas      |                   22 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| EnBW Windpark Baltic 2               | Germany        | Baltic / Belt Seas      |                   81 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Frederikshavn Offshore               | Denmark        | Baltic / Belt Seas      |                    5 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Fécamp                               | France         | Channel / Atlantic edge |                   72 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Galloper                             | United Kingdom | European shelf          |                   57 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Gemini                               | Netherlands    | Southern North Sea      |                  151 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Global Tech I                        | Germany        | North Sea / Skagerrak   |                   81 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Gode Wind 1 and 2                    | Germany        | North Sea / Skagerrak   |                   98 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Gode Wind 3                          | Germany        | North Sea / Skagerrak   |                   24 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Greater Gabbard                      | United Kingdom | European shelf          |                  141 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Gunfleet Sands                       | United Kingdom | European shelf          |                   49 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Gunfleet Sands Demo                  | United Kingdom | European shelf          |                    3 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Gwynt y Mor                          | United Kingdom | UK shelf / Irish Sea    |                  161 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Hollandse Kust Noord                 | Netherlands    | Southern North Sea      |                   70 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Hollandse Kust Zuid                  | Netherlands    | Southern North Sea      |                  141 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Horns Rev I                          | Denmark        | North Sea / Skagerrak   |                   81 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Horns Rev II                         | Denmark        | North Sea / Skagerrak   |                   92 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Horns Rev III                        | Denmark        | North Sea / Skagerrak   |                   50 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Hornsea Project 1                    | United Kingdom | European shelf          |                  175 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Hornsea Project 2                    | United Kingdom | European shelf          |                  166 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Humber Gateway                       | United Kingdom | European shelf          |                   74 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Hywind Scotland Pilot Park           | United Kingdom | UK shelf / Irish Sea    |                    6 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Inner Dowsing                        | United Kingdom | European shelf          |                   28 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Kaskasi                              | Germany        | North Sea / Skagerrak   |                   39 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Kentish Flats                        | United Kingdom | European shelf          |                   31 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Kentish Flats Extension              | United Kingdom | European shelf          |                   16 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Kincardine                           | United Kingdom | UK shelf / Irish Sea    |                    6 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Kriegers Flak                        | Denmark        | Baltic / Belt Seas      |                   73 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Lillgrund                            | Sweden         | Baltic / Belt Seas      |                   49 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Lincs                                | United Kingdom | European shelf          |                   76 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| London Array                         | United Kingdom | European shelf          |                  176 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Lynn                                 | United Kingdom | European shelf          |                   28 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Meerwind Sued/Ost                    | Germany        | North Sea / Skagerrak   |                   81 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Merkur Offshore                      | Germany        | North Sea / Skagerrak   |                   67 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Mermaid                              | Belgium        | Southern North Sea      |                   29 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Methil Demo                          | United Kingdom | UK shelf / Irish Sea    |                    2 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Middelgrunden                        | Denmark        | Baltic / Belt Seas      |                   21 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Moray East                           | United Kingdom | UK shelf / Irish Sea    |                  101 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Moray West                           | United Kingdom | UK shelf / Irish Sea    |                   61 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Neart na Gaoithe                     | United Kingdom | UK shelf / Irish Sea    |                   55 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Nissum Bredning                      | Denmark        | North Sea / Skagerrak   |                    5 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Nobelwind                            | Belgium        | Southern North Sea      |                   51 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Nordergründe                         | Germany        | North Sea / Skagerrak   |                   19 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Nordsee One                          | Germany        | North Sea / Skagerrak   |                   55 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Nordsee Ost                          | Germany        | North Sea / Skagerrak   |                   49 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| North Hoyle                          | United Kingdom | UK shelf / Irish Sea    |                   31 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Norther                              | Belgium        | Southern North Sea      |                   45 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Northwester 2                        | Belgium        | Southern North Sea      |                   24 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Northwind                            | Belgium        | Southern North Sea      |                   73 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Nysted                               | Denmark        | Baltic / Belt Seas      |                   73 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| OWF Egmond aan Zee                   | Netherlands    | Southern North Sea      |                   37 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| OWF Luchterduinen                    | Netherlands    | Southern North Sea      |                   44 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| OWF Prinses Amalia                   | Netherlands    | Southern North Sea      |                   61 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Ormonde                              | United Kingdom | UK shelf / Irish Sea    |                   31 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Race Bank                            | United Kingdom | European shelf          |                   92 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Rampion                              | United Kingdom | Channel / Atlantic edge |                  117 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Renland                              | Denmark        | North Sea / Skagerrak   |                    9 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Rentel                               | Belgium        | Southern North Sea      |                   43 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Rhyl Flats                           | United Kingdom | UK shelf / Irish Sea    |                   26 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Riffgat                              | Germany        | North Sea / Skagerrak   |                   31 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Robin Rigg West                      | United Kingdom | UK shelf / Irish Sea    |                   61 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Rodsand II                           | Denmark        | Baltic / Belt Seas      |                   91 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Saint-Brieuc                         | France         | Channel / Atlantic edge |                   63 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Saint-Nazaire                        | France         | Channel / Atlantic edge |                   81 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Samsa                                | Denmark        | Baltic / Belt Seas      |                   11 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Sandbank                             | Germany        | North Sea / Skagerrak   |                   73 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Scroby Sands                         | United Kingdom | European shelf          |                   31 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| SeaStar                              | Belgium        | Southern North Sea      |                   31 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Seagreen                             | United Kingdom | UK shelf / Irish Sea    |                  115 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Sheringham Shoal                     | United Kingdom | European shelf          |                   89 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Sprogo                               | Denmark        | Baltic / Belt Seas      |                    8 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Teesside                             | United Kingdom | European shelf          |                   28 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| TetraSpar Demonstrator - Metcentre   | Norway         | North Sea / Skagerrak   |                    2 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Thanet                               | United Kingdom | European shelf          |                  101 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Thornton Bank - phase I              | Belgium        | Southern North Sea      |                    7 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Thornton Bank - phase II and III     | Belgium        | Southern North Sea      |                   49 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Trianel Windpark Borkum 1            | Germany        | North Sea / Skagerrak   |                   41 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Trianel Windpark Borkum 2            | Germany        | North Sea / Skagerrak   |                   33 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Triton Knoll                         | United Kingdom | European shelf          |                   91 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Tunm Knob                            | Denmark        | Baltic / Belt Seas      |                   11 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| UNITECH Zefyros by Hywind Technology | Norway         | North Sea / Skagerrak   |                    2 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Veja Mate                            | Germany        | North Sea / Skagerrak   |                   68 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Vesterhav Nord                       | Denmark        | North Sea / Skagerrak   |                   22 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Vesterhav Syd                        | Denmark        | North Sea / Skagerrak   |                   21 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Walney 1                             | United Kingdom | UK shelf / Irish Sea    |                   52 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Walney 2                             | United Kingdom | UK shelf / Irish Sea    |                   52 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Walney Extension 3                   | United Kingdom | UK shelf / Irish Sea    |                   41 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Walney Extension 4                   | United Kingdom | UK shelf / Irish Sea    |                   48 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| West of Duddon Sands                 | United Kingdom | UK shelf / Irish Sea    |                  109 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Westermost Rough                     | United Kingdom | European shelf          |                   36 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |
| Wikinger                             | Germany        | Baltic / Belt Seas      |                   71 | emodnet                  | gebco_2026                | Expected coverage for European offshore wind farm points. |

## Expected Outputs

- output_root: `Data/Processed/metocean/bathymetry`
- expected_output_path: `Data/Processed/metocean/bathymetry/site_bathymetry_points.parquet`
- expected_metadata_path: `Data/Processed/metocean/bathymetry/bathymetry_source_metadata.json`
- output_exists: `False`
- overwrite_policy: `preserve_existing`
- dry_run_writes: QA report only; no raster, tile, parquet, or output directory writes

## Proposed Output Schema

- `wind_farm`
- `sample_point_id`
- `sample_point_type`
- `lat`
- `lon`
- `water_depth_m`
- `bathymetry_source`
- `bathymetry_version`
- `bathymetry_grid_lat`
- `bathymetry_grid_lon`
- `bathymetry_distance_m`
- `bathymetry_assignment_method`
- `depth_sign_convention`
- `bathymetry_vertical_datum`
- `bathymetry_spatial_match_status`

## Storage And Source-Tile Estimate

- estimated_final_point_table_mb: `9.963`
- estimated_source_tile_storage: hundreds of MB if clipped EMODnet/GEBCO source tiles are downloaded
- estimated_degree_tile_count: `74`
- estimated_regional_bbox_count: `6`

## Assignment Contract

- assignment_method: `bilinear_interpolation_preferred_with_nearest_grid_fallback`
- rationale: Bilinear interpolation is smoother for continuous depth grids; nearest grid is the deterministic fallback for edge cells, masked cells, or QA spot checks.
- depth_sign_convention: `positive_down_meters_in_processed_table`
- coordinate_reference_system: `EPSG:4326 / WGS84 latitude-longitude`
- primary_vertical_datum: `source-specific EMODnet DTM vertical reference; confirm from tile metadata`
- fallback_vertical_datum: `mean sea level approximation in GEBCO elevation grid; confirm metadata`

## Validation Gates For Later Pilot

- No missing sample points unless explained.
- Water depth values are plausible for offshore wind locations.
- Depth sign convention is documented and preserved.
- Bathymetry source and version are populated.
- Vertical datum is documented from source metadata.
- Assignment distance is populated.
- No duplicate wind_farm + sample_point_id rows.
- Spot checks against source tiles or trusted reference depths are recorded.
- Fallback source usage is explicitly flagged.
- Coordinate CRS remains EPSG:4326/WGS84 at the point interface.
- Existing metocean wave archives are not mutated.

## Risks And Assumptions

- Source-tile download mechanics and exact EMODnet vintage remain unapproved and unrun.
- EMODnet vertical datum and source references must be copied from tile metadata during the pilot.
- GEBCO raw elevations use a different convention from the processed positive-depth contract and must be converted if used.
- Farm requirement rows provide sample-point counts and spatial bounds, not the final expanded point coordinate table.
- Bathymetry is static site context and must not mutate wave archives or imply current/wind/source fusion.

## Next Pilot Recommendation

After source tiles are explicitly approved/acquired, assign bathymetry to the common metocean sample points and write site_bathymetry_points.parquet with no source-fusion or dwell-table rebuild.
