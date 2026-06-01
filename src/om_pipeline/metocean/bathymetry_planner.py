"""Dry-run planning for static bathymetry assignment.

This module deliberately plans only. It does not download bathymetry rasters,
open remote services, or write final bathymetry point archives. The first
accepted artifact is a QA/planning report that makes the next pilot explicit.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd


DEFAULT_REQUIREMENTS_PATH = Path(
    "analysis/06_rq6_metocean_spatial_resolution/common_metocean_farm_requirements.csv"
)
DEFAULT_OUTPUT_ROOT = Path("Data/Processed/metocean/bathymetry")
DEFAULT_QA_REPORT = Path(
    "analysis/06_rq6_metocean_spatial_resolution/bathymetry_assignment_pilot_report.md"
)

REQUIRED_REQUIREMENT_COLUMNS = [
    "wind_farm",
    "farm_id",
    "country",
    "min_lon",
    "max_lon",
    "min_lat",
    "max_lat",
    "sample_point_strategy",
    "sample_point_count",
    "review_required",
]

BATHYMETRY_OUTPUT_SCHEMA = [
    "wind_farm",
    "sample_point_id",
    "sample_point_type",
    "lat",
    "lon",
    "water_depth_m",
    "bathymetry_source",
    "bathymetry_version",
    "bathymetry_grid_lat",
    "bathymetry_grid_lon",
    "bathymetry_distance_m",
    "bathymetry_assignment_method",
    "depth_sign_convention",
    "bathymetry_vertical_datum",
    "bathymetry_spatial_match_status",
]

VALIDATION_GATES = [
    "No missing sample points unless explained.",
    "Water depth values are plausible for offshore wind locations.",
    "Depth sign convention is documented and preserved.",
    "Bathymetry source and version are populated.",
    "Vertical datum is documented from source metadata.",
    "Assignment distance is populated.",
    "No duplicate wind_farm + sample_point_id rows.",
    "Spot checks against source tiles or trusted reference depths are recorded.",
    "Fallback source usage is explicitly flagged.",
    "Coordinate CRS remains EPSG:4326/WGS84 at the point interface.",
    "Existing metocean wave archives are not mutated.",
]

PRIMARY_SOURCE_DETAILS = {
    "emodnet": {
        "display_name": "EMODnet Bathymetry DTM",
        "version": "active EMODnet DTM release, confirm exact vintage at download",
        "coverage_expectation": "Expected coverage for European offshore wind farm points.",
        "vertical_datum": "source-specific EMODnet DTM vertical reference; confirm from tile metadata",
    }
}

FALLBACK_SOURCE_DETAILS = {
    "gebco_2026": {
        "display_name": "GEBCO_2026 Grid",
        "version": "GEBCO_2026",
        "coverage_expectation": "Global fallback and cross-check where EMODnet is unavailable.",
        "vertical_datum": "mean sea level approximation in GEBCO elevation grid; confirm metadata",
    }
}


@dataclass(frozen=True)
class BathymetryPlanningResult:
    requirements: pd.DataFrame
    farm_plan: pd.DataFrame
    region_plan: pd.DataFrame
    summary: dict[str, Any]
    output_schema: list[str]
    qa_report: Path | None


def _load_requirements(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Common metocean requirements table not found: {path}")
    if path.suffix.lower() == ".parquet":
        requirements = pd.read_parquet(path)
    else:
        requirements = pd.read_csv(path)

    missing = sorted(set(REQUIRED_REQUIREMENT_COLUMNS) - set(requirements.columns))
    if missing:
        raise ValueError(f"Common requirements table is missing required columns: {missing}")

    requirements = requirements.copy()
    for column in ["min_lon", "max_lon", "min_lat", "max_lat", "sample_point_count"]:
        requirements[column] = pd.to_numeric(requirements[column], errors="coerce")

    invalid = requirements[
        requirements[["min_lon", "max_lon", "min_lat", "max_lat"]].isna().any(axis=1)
        | (requirements["min_lon"] > requirements["max_lon"])
        | (requirements["min_lat"] > requirements["max_lat"])
        | (requirements["min_lon"] < -180)
        | (requirements["max_lon"] > 180)
        | (requirements["min_lat"] < -90)
        | (requirements["max_lat"] > 90)
    ]
    if not invalid.empty:
        farms = ", ".join(str(value) for value in invalid["wind_farm"].head(5))
        raise ValueError(f"Common requirements contain invalid coordinate bounds: {farms}")

    requirements["sample_point_count"] = requirements["sample_point_count"].fillna(0).astype(int)
    return requirements


def _normalise_source(value: str) -> str:
    return value.strip().lower().replace("-", "_")


def _source_detail(source: str, *, primary: bool) -> dict[str, str]:
    source_key = _normalise_source(source)
    details = PRIMARY_SOURCE_DETAILS if primary else FALLBACK_SOURCE_DETAILS
    if source_key not in details:
        role = "primary" if primary else "fallback"
        raise ValueError(f"Unsupported {role} bathymetry source: {source}")
    return details[source_key]


def _classify_region(row: pd.Series) -> str:
    country = str(row.get("country") or "").strip()
    min_lon = float(row["min_lon"])
    max_lon = float(row["max_lon"])
    min_lat = float(row["min_lat"])
    max_lat = float(row["max_lat"])

    if max_lon >= 9.0 and max_lat >= 53.0:
        return "Baltic / Belt Seas"
    if country == "United Kingdom" and max_lon < -1.0:
        return "UK shelf / Irish Sea"
    if country in {"Belgium", "Netherlands"}:
        return "Southern North Sea"
    if country in {"Germany", "Denmark", "Norway", "Sweden"}:
        return "North Sea / Skagerrak"
    if country == "France" or min_lat < 51.0:
        return "Channel / Atlantic edge"
    return "European shelf"


def _degree_tiles_for_bounds(
    min_lon: float,
    max_lon: float,
    min_lat: float,
    max_lat: float,
) -> set[tuple[int, int]]:
    lon_start = math.floor(min_lon)
    lon_stop = math.ceil(max_lon)
    lat_start = math.floor(min_lat)
    lat_stop = math.ceil(max_lat)
    return {
        (lon, lat)
        for lon in range(lon_start, lon_stop)
        for lat in range(lat_start, lat_stop)
    }


def _estimate_degree_tiles(requirements: pd.DataFrame) -> int:
    tiles: set[tuple[int, int]] = set()
    for row in requirements.itertuples(index=False):
        tiles.update(
            _degree_tiles_for_bounds(
                float(row.min_lon),
                float(row.max_lon),
                float(row.min_lat),
                float(row.max_lat),
            )
        )
    return len(tiles)


def _build_farm_plan(
    requirements: pd.DataFrame,
    primary_source: str,
    fallback_source: str,
    output_root: Path,
    overwrite: bool,
) -> pd.DataFrame:
    primary_key = _normalise_source(primary_source)
    fallback_key = _normalise_source(fallback_source)
    primary_detail = _source_detail(primary_key, primary=True)
    fallback_detail = _source_detail(fallback_key, primary=False)

    rows: list[dict[str, Any]] = []
    for _, row in requirements.sort_values("wind_farm").iterrows():
        output_path = output_root / "site_bathymetry_points.parquet"
        region = _classify_region(row)
        rows.append(
            {
                "wind_farm": row["wind_farm"],
                "farm_id": row["farm_id"],
                "country": row.get("country"),
                "region": region,
                "sample_point_strategy": row["sample_point_strategy"],
                "sample_point_count": int(row["sample_point_count"]),
                "min_lon": float(row["min_lon"]),
                "max_lon": float(row["max_lon"]),
                "min_lat": float(row["min_lat"]),
                "max_lat": float(row["max_lat"]),
                "planned_primary_source": primary_key,
                "planned_primary_source_name": primary_detail["display_name"],
                "planned_fallback_source": fallback_key,
                "planned_fallback_source_name": fallback_detail["display_name"],
                "coverage_expectation": primary_detail["coverage_expectation"],
                "fallback_use_case": (
                    "Use fallback only for missing EMODnet cells, failed tile access, "
                    "or cross-checks where source metadata is ambiguous."
                ),
                "expected_output_path": str(output_path),
                "review_required": bool(row.get("review_required", False)),
                "overwrite_policy": "overwrite_requested" if overwrite else "preserve_existing",
            }
        )
    return pd.DataFrame(rows)


def _build_region_plan(farm_plan: pd.DataFrame) -> pd.DataFrame:
    grouped = (
        farm_plan.groupby("region", dropna=False)
        .agg(
            farm_count=("wind_farm", "nunique"),
            sample_point_count=("sample_point_count", "sum"),
            min_lon=("min_lon", "min"),
            max_lon=("max_lon", "max"),
            min_lat=("min_lat", "min"),
            max_lat=("max_lat", "max"),
        )
        .reset_index()
        .sort_values("region")
    )
    for column in ["min_lon", "max_lon", "min_lat", "max_lat"]:
        grouped[column] = grouped[column].round(6)
    grouped["planned_primary_source"] = "emodnet"
    grouped["planned_fallback_source"] = "gebco_2026"
    return grouped


def build_bathymetry_assignment_plan(
    requirements_path: Path = DEFAULT_REQUIREMENTS_PATH,
    primary_source: str = "emodnet",
    fallback_source: str = "gebco_2026",
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    overwrite: bool = False,
) -> BathymetryPlanningResult:
    """Build an in-memory dry-run plan for bathymetry point assignment."""
    requirements = _load_requirements(requirements_path)
    primary_detail = _source_detail(primary_source, primary=True)
    fallback_detail = _source_detail(fallback_source, primary=False)

    farm_plan = _build_farm_plan(
        requirements=requirements,
        primary_source=primary_source,
        fallback_source=fallback_source,
        output_root=output_root,
        overwrite=overwrite,
    )
    region_plan = _build_region_plan(farm_plan)

    output_path = output_root / "site_bathymetry_points.parquet"
    metadata_path = output_root / "bathymetry_source_metadata.json"
    estimated_degree_tiles = _estimate_degree_tiles(requirements)
    sample_points = int(requirements["sample_point_count"].sum())
    output_exists = output_path.exists()

    summary: dict[str, Any] = {
        "requirements_path": str(requirements_path),
        "input_requirements_row_count": int(len(requirements)),
        "farm_count": int(requirements["wind_farm"].nunique()),
        "sample_point_count": sample_points,
        "review_required_farm_count": int(requirements["review_required"].fillna(False).astype(bool).sum()),
        "min_lon": float(requirements["min_lon"].min()),
        "max_lon": float(requirements["max_lon"].max()),
        "min_lat": float(requirements["min_lat"].min()),
        "max_lat": float(requirements["max_lat"].max()),
        "primary_source": _normalise_source(primary_source),
        "primary_source_name": primary_detail["display_name"],
        "primary_source_version": primary_detail["version"],
        "fallback_source": _normalise_source(fallback_source),
        "fallback_source_name": fallback_detail["display_name"],
        "fallback_source_version": fallback_detail["version"],
        "output_root": str(output_root),
        "expected_output_path": str(output_path),
        "expected_metadata_path": str(metadata_path),
        "output_exists": bool(output_exists),
        "overwrite": bool(overwrite),
        "overwrite_policy": "overwrite_requested" if overwrite else "preserve_existing",
        "estimated_degree_tile_count": estimated_degree_tiles,
        "estimated_regional_bbox_count": int(len(region_plan)),
        "estimated_final_point_table_mb": round(max(sample_points, 1) * 0.0015, 3),
        "estimated_source_tile_storage": "hundreds of MB if clipped EMODnet/GEBCO source tiles are downloaded",
        "assignment_method": "bilinear_interpolation_preferred_with_nearest_grid_fallback",
        "assignment_method_rationale": (
            "Bilinear interpolation is smoother for continuous depth grids; nearest "
            "grid is the deterministic fallback for edge cells, masked cells, or QA spot checks."
        ),
        "depth_sign_convention": "positive_down_meters_in_processed_table",
        "bathymetry_vertical_datum": primary_detail["vertical_datum"],
        "fallback_vertical_datum": fallback_detail["vertical_datum"],
        "coordinate_reference_system": "EPSG:4326 / WGS84 latitude-longitude",
        "dry_run_writes": "QA report only; no raster, tile, parquet, or output directory writes",
        "next_pilot_recommendation": (
            "After source tiles are explicitly approved/acquired, assign bathymetry to "
            "the common metocean sample points and write site_bathymetry_points.parquet "
            "with no source-fusion or dwell-table rebuild."
        ),
    }
    return BathymetryPlanningResult(
        requirements=requirements,
        farm_plan=farm_plan,
        region_plan=region_plan,
        summary=summary,
        output_schema=list(BATHYMETRY_OUTPUT_SCHEMA),
        qa_report=None,
    )


def _markdown_table(frame: pd.DataFrame, columns: list[str]) -> str:
    if frame.empty:
        return "_No rows._"
    return frame[columns].to_markdown(index=False)


def render_bathymetry_planning_report(result: BathymetryPlanningResult) -> str:
    """Render the dry-run plan as a compact Markdown QA report."""
    summary = result.summary
    farm_columns = [
        "wind_farm",
        "country",
        "region",
        "sample_point_count",
        "planned_primary_source",
        "planned_fallback_source",
        "coverage_expectation",
    ]
    region_columns = [
        "region",
        "farm_count",
        "sample_point_count",
        "min_lon",
        "max_lon",
        "min_lat",
        "max_lat",
        "planned_primary_source",
        "planned_fallback_source",
    ]

    lines = [
        "# Bathymetry Assignment Planning Dry-Run",
        "",
        "Status: dry-run planning only. No bathymetry rasters, final point tables, currents, FINO data, source fusion, NORA3 reruns, or dwell-metocean rebuilds were run.",
        "",
        "## Executive Conclusion",
        "",
        "Bathymetry assignment is ready for a scoped pilot after source-tile acquisition is explicitly approved. EMODnet is the planned primary source for all common metocean farm/sample points, with GEBCO_2026 reserved for fallback and cross-checks.",
        "",
        "## Input Inventory",
        "",
        f"- requirements_path: `{summary['requirements_path']}`",
        f"- input_requirements_row_count: `{summary['input_requirements_row_count']}`",
        f"- farm_count: `{summary['farm_count']}`",
        f"- sample_point_count: `{summary['sample_point_count']}`",
        f"- review_required_farm_count: `{summary['review_required_farm_count']}`",
        f"- coordinate_bounds: lon `{summary['min_lon']}` to `{summary['max_lon']}`, lat `{summary['min_lat']}` to `{summary['max_lat']}`",
        "",
        "## Source Strategy",
        "",
        f"- primary_source: `{summary['primary_source']}` ({summary['primary_source_name']})",
        f"- primary_source_version: `{summary['primary_source_version']}`",
        f"- fallback_source: `{summary['fallback_source']}` ({summary['fallback_source_name']})",
        f"- fallback_source_version: `{summary['fallback_source_version']}`",
        "- EMODnet coverage expectation: expected across European offshore wind points in the current common requirements table.",
        "- GEBCO_2026 fallback use cases: missing EMODnet cells, failed tile access, ambiguous source metadata, or independent cross-check.",
        "",
        "## Region Plan",
        "",
        _markdown_table(result.region_plan, region_columns),
        "",
        "## Farm Source Plan",
        "",
        _markdown_table(result.farm_plan, farm_columns),
        "",
        "## Expected Outputs",
        "",
        f"- output_root: `{summary['output_root']}`",
        f"- expected_output_path: `{summary['expected_output_path']}`",
        f"- expected_metadata_path: `{summary['expected_metadata_path']}`",
        f"- output_exists: `{summary['output_exists']}`",
        f"- overwrite_policy: `{summary['overwrite_policy']}`",
        f"- dry_run_writes: {summary['dry_run_writes']}",
        "",
        "## Proposed Output Schema",
        "",
        "\n".join(f"- `{column}`" for column in result.output_schema),
        "",
        "## Storage And Source-Tile Estimate",
        "",
        f"- estimated_final_point_table_mb: `{summary['estimated_final_point_table_mb']}`",
        f"- estimated_source_tile_storage: {summary['estimated_source_tile_storage']}",
        f"- estimated_degree_tile_count: `{summary['estimated_degree_tile_count']}`",
        f"- estimated_regional_bbox_count: `{summary['estimated_regional_bbox_count']}`",
        "",
        "## Assignment Contract",
        "",
        f"- assignment_method: `{summary['assignment_method']}`",
        f"- rationale: {summary['assignment_method_rationale']}",
        f"- depth_sign_convention: `{summary['depth_sign_convention']}`",
        f"- coordinate_reference_system: `{summary['coordinate_reference_system']}`",
        f"- primary_vertical_datum: `{summary['bathymetry_vertical_datum']}`",
        f"- fallback_vertical_datum: `{summary['fallback_vertical_datum']}`",
        "",
        "## Validation Gates For Later Pilot",
        "",
        "\n".join(f"- {gate}" for gate in VALIDATION_GATES),
        "",
        "## Risks And Assumptions",
        "",
        "- Source-tile download mechanics and exact EMODnet vintage remain unapproved and unrun.",
        "- EMODnet vertical datum and source references must be copied from tile metadata during the pilot.",
        "- GEBCO raw elevations use a different convention from the processed positive-depth contract and must be converted if used.",
        "- Farm requirement rows provide sample-point counts and spatial bounds, not the final expanded point coordinate table.",
        "- Bathymetry is static site context and must not mutate wave archives or imply current/wind/source fusion.",
        "",
        "## Next Pilot Recommendation",
        "",
        summary["next_pilot_recommendation"],
        "",
    ]
    return "\n".join(lines)


def plan_bathymetry_assignment(
    requirements_path: Path = DEFAULT_REQUIREMENTS_PATH,
    primary_source: str = "emodnet",
    fallback_source: str = "gebco_2026",
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    qa_report: Path | None = DEFAULT_QA_REPORT,
    dry_run: bool = True,
    overwrite: bool = False,
) -> BathymetryPlanningResult:
    """Create a dry-run bathymetry plan and optionally write its QA report."""
    if not dry_run:
        raise ValueError(
            "Bathymetry assignment planning currently supports dry-run mode only. "
            "Do not use it to download or write final bathymetry archives."
        )

    result = build_bathymetry_assignment_plan(
        requirements_path=requirements_path,
        primary_source=primary_source,
        fallback_source=fallback_source,
        output_root=output_root,
        overwrite=overwrite,
    )
    if qa_report is not None:
        qa_report.parent.mkdir(parents=True, exist_ok=True)
        qa_report.write_text(render_bathymetry_planning_report(result), encoding="utf-8")
        result = BathymetryPlanningResult(
            requirements=result.requirements,
            farm_plan=result.farm_plan,
            region_plan=result.region_plan,
            summary=result.summary,
            output_schema=result.output_schema,
            qa_report=qa_report,
        )
    return result
