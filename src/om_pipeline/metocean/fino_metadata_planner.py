"""Dry-run planning for FINO metadata access and validation use.

This module deliberately does not log in to the BSH Insitu Portal, download
FINO files, import time series, or write processed FINO archives. It records the
publicly known station/access metadata and computes station-to-farm proximity
against the accepted common metocean sample points so a later import pilot can
be scoped without pretending FINO is a farm-wide gridded source.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from .extract_nws import haversine_distance


DEFAULT_OUTPUT_REPORT = Path(
    "analysis/06_rq6_metocean_spatial_resolution/fino_metadata_access_plan.md"
)
DEFAULT_REQUIREMENTS_PATH = Path(
    "analysis/06_rq6_metocean_spatial_resolution/common_metocean_farm_requirements.csv"
)
DEFAULT_BATHYMETRY_POINTS = Path(
    "Data/Processed/metocean/bathymetry/site_bathymetry_points.parquet"
)
DEFAULT_RAW_METOCEAN_ROOT = Path("Data/Raw/Metocean")
DEFAULT_PROCESSED_FINO_ROOT = Path("Data/Processed/metocean/fino")

STATION_METADATA_SCHEMA = [
    "station_id",
    "station_name",
    "lat",
    "lon",
    "operator",
    "source",
    "access_method",
    "available_start",
    "available_end",
    "variables_available",
    "cadence",
    "licence_note",
    "metadata_source",
    "metadata_review_status",
]

TIMESERIES_SCHEMA = [
    "station_id",
    "timestamp_utc",
    "variable",
    "value",
    "unit",
    "measurement_height_or_depth",
    "qc_flag",
    "source_file",
    "access_method",
]

STATION_FARM_MATCH_SCHEMA = [
    "station_id",
    "station_name",
    "wind_farm",
    "farm_id",
    "distance_km",
    "nearest_sample_point_id",
    "nearest_sample_distance_km",
    "farm_centroid_distance_km",
    "match_role",
    "representativeness_note",
]

VALIDATION_GATES = [
    "Station coordinates are verified against public FINO pages and portal metadata.",
    "Timestamps are UTC-normalized and cadence is reported per station-variable.",
    "Units and measurement heights or depths are preserved.",
    "QC flags and source-file names are preserved.",
    "Variables are mapped to canonical names only after inspecting portal exports.",
    "Licence and source acknowledgement requirements are documented before import.",
    "FINO is used as validation or baseline evidence, not automatic farm-wide assignment.",
    "Station-to-farm representativeness notes accompany every comparison.",
    "Comparison metrics include MAE, RMSE, bias, correlation, and coverage.",
    "No source-fused metocean or final dwell table is written by FINO ingestion.",
]

DO_NOT_DO = [
    "Do not bulk-download FINO time series before access and variable names are confirmed.",
    "Do not treat FINO as a farm-wide primary source without a distance rule.",
    "Do not overwrite accepted NORA3, NWS, Baltic, or bathymetry archives.",
    "Do not download currents or promote legacy CMEMS current CSVs from this task.",
    "Do not rebuild the final dwell-metocean feature table.",
    "Do not interpolate or source-fuse FINO observations in the metadata planner.",
]

SOURCE_LINKS = {
    "fino_database": "https://www.fino2.de/en/fino2/fino-database.html",
    "bsh_login": "https://login.bsh.de/fachverfahren/?lang=en",
    "fino1_location": "https://www.fino1.de/en/location.html",
    "fino1_hydrography": "https://www.fino1.de/en/research/project/hydrography.html",
    "fino2_location": "https://www.fino2.de/en/fino2/location.html",
    "fino2_meteorology": "https://www.fino2.de/en/research/meteorology.html",
    "fino2_oceanography": "https://www.fino2.de/en/research/oceanography.html",
    "fino3_position": "https://www.fino3.de/en/about/position.html",
}


def _dms_to_decimal(degrees: float, minutes: float = 0.0, seconds: float = 0.0) -> float:
    return degrees + (minutes / 60.0) + (seconds / 3600.0)


STATION_CATALOG = [
    {
        "station_id": "FINO1",
        "station_name": "FINO1",
        "lat": _dms_to_decimal(54, 0, 53.5),
        "lon": _dms_to_decimal(6, 35, 15.5),
        "operator": "Forschungs- und Entwicklungszentrum Fachhochschule Kiel GmbH; verify portal metadata",
        "source": "FINO database / BSH Insitu Portal",
        "access_method": "BSH-Login registration, request Insitu specialist procedure, export selected station-variable files",
        "available_start": "2004-01",
        "available_end": "ongoing or latest portal availability; verify per variable after access",
        "variables_available": (
            "meteorological and oceanographic data; likely wind speed, wind direction, "
            "air temperature, air pressure, humidity, water level, wave height, wave "
            "period, wave direction, current speed/direction by depth, water temperature, "
            "salinity, oxygen"
        ),
        "wave_variables": "significant wave height, wave period, wave direction",
        "wind_variables": "wind speed and wind direction at mast heights; exact heights require portal export",
        "current_variables": "current speed and direction at several depths reported for hydrography; verify portal availability",
        "measurement_heights_depths": "wind mast heights and hydrographic depths to be preserved from portal metadata",
        "cadence": "10-minute expected for core wind/wave observations where available; confirm per variable",
        "file_formats": "Insitu Portal export, likely CSV or text; confirm after access",
        "qc_flags": "quality-checked BSH/FINO fields expected; preserve native QC columns",
        "licence_note": "Free/research use and acknowledgement expected; confirm exact terms after access approval",
        "metadata_source": (
            SOURCE_LINKS["fino_database"]
            + "; "
            + SOURCE_LINKS["bsh_login"]
            + "; "
            + SOURCE_LINKS["fino1_location"]
            + "; "
            + SOURCE_LINKS["fino1_hydrography"]
        ),
        "metadata_review_status": "public_metadata_planned_access_not_imported",
    },
    {
        "station_id": "FINO2",
        "station_name": "FINO2",
        "lat": _dms_to_decimal(55, 0, 24.94),
        "lon": _dms_to_decimal(13, 9, 15.08),
        "operator": "DNV; verify portal metadata",
        "source": "FINO database / BSH Insitu Portal",
        "access_method": "BSH-Login registration, request Insitu specialist procedure, export selected station-variable files",
        "available_start": "2007-08",
        "available_end": "ongoing or latest portal availability; verify per variable after access",
        "variables_available": (
            "official database summary lists meteorological data; public FINO2 pages "
            "also describe long-term oceanographic monitoring including seastate and "
            "current data, so portal variable availability must be verified"
        ),
        "wave_variables": "seastate variables likely available through hydrographic monitoring; verify portal fields",
        "wind_variables": "wind speed and direction plus air temperature, pressure, humidity, radiation",
        "current_variables": "current data described by public oceanography page; verify u/v or speed/direction fields in portal",
        "measurement_heights_depths": "mast table includes wind speeds at 32.4-102.5 m MSL and wind direction at 31.8-91.8 m MSL",
        "cadence": "10-minute likely for core meteorology; confirm per variable",
        "file_formats": "Insitu Portal export, likely CSV or text; confirm after access",
        "qc_flags": "quality-checked BSH/FINO fields expected; preserve native QC columns",
        "licence_note": "Free/research use and acknowledgement expected; confirm exact terms after access approval",
        "metadata_source": (
            SOURCE_LINKS["fino_database"]
            + "; "
            + SOURCE_LINKS["bsh_login"]
            + "; "
            + SOURCE_LINKS["fino2_location"]
            + "; "
            + SOURCE_LINKS["fino2_meteorology"]
            + "; "
            + SOURCE_LINKS["fino2_oceanography"]
        ),
        "metadata_review_status": "public_metadata_planned_access_not_imported",
    },
    {
        "station_id": "FINO3",
        "station_name": "FINO3",
        "lat": _dms_to_decimal(55, 11.7, 0),
        "lon": _dms_to_decimal(7, 9.5, 0),
        "operator": "Forschungs- und Entwicklungszentrum Fachhochschule Kiel GmbH; verify portal metadata",
        "source": "FINO database / BSH Insitu Portal",
        "access_method": "BSH-Login registration, request Insitu specialist procedure, export selected station-variable files",
        "available_start": "2009-09",
        "available_end": "ongoing or latest portal availability; verify per variable after access",
        "variables_available": "meteorological and oceanographic data; public live pages expose wind, temperature, and wave height",
        "wave_variables": "wave height and likely wave period/direction in portal; verify variable names",
        "wind_variables": "wind speed and wind direction at mast heights; exact heights require portal export",
        "current_variables": "oceanographic variables likely available; verify current fields and depths in portal",
        "measurement_heights_depths": "station-specific mast and hydrographic depths to be preserved from portal metadata",
        "cadence": "10-minute likely for core meteorology/wave observations; confirm per variable",
        "file_formats": "Insitu Portal export, likely CSV or text; confirm after access",
        "qc_flags": "quality-checked BSH/FINO fields expected; preserve native QC columns",
        "licence_note": "Free/research use and acknowledgement expected; confirm exact terms after access approval",
        "metadata_source": (
            SOURCE_LINKS["fino_database"]
            + "; "
            + SOURCE_LINKS["bsh_login"]
            + "; "
            + SOURCE_LINKS["fino3_position"]
        ),
        "metadata_review_status": "public_metadata_planned_access_not_imported",
    },
]


@dataclass(frozen=True)
class FinoPlanningResult:
    station_plan: pd.DataFrame
    station_farm_matches: pd.DataFrame
    local_inventory: pd.DataFrame
    summary: dict[str, Any]
    station_metadata_schema: list[str]
    timeseries_schema: list[str]
    station_farm_match_schema: list[str]
    output_report: Path | None


def _load_requirements(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Common metocean requirements table not found: {path}")
    requirements = pd.read_csv(path)
    required = {"wind_farm", "farm_id"}
    missing = sorted(required - set(requirements.columns))
    if missing:
        raise ValueError(f"Common requirements table is missing required columns: {missing}")
    return requirements.copy()


def _load_project_sample_points(
    bathymetry_points_path: Path,
    requirements_path: Path,
) -> pd.DataFrame:
    requirements = _load_requirements(requirements_path)[["wind_farm", "farm_id"]]

    if bathymetry_points_path.exists():
        points = pd.read_parquet(
            bathymetry_points_path,
            columns=["wind_farm", "sample_point_id", "sample_point_type", "lat", "lon"],
        )
        source = "bathymetry_site_bathymetry_points"
    else:
        fallback_requirements = pd.read_csv(requirements_path)
        required = {"wind_farm", "farm_id", "min_lon", "max_lon", "min_lat", "max_lat"}
        missing = sorted(required - set(fallback_requirements.columns))
        if missing:
            raise ValueError(
                "Cannot build fallback farm centroids; requirements table is missing "
                f"required columns: {missing}"
            )
        points = fallback_requirements.assign(
            sample_point_id="bbox_centroid",
            sample_point_type="farm_centroid",
            lat=(fallback_requirements["min_lat"] + fallback_requirements["max_lat"]) / 2.0,
            lon=(fallback_requirements["min_lon"] + fallback_requirements["max_lon"]) / 2.0,
        )[["wind_farm", "sample_point_id", "sample_point_type", "lat", "lon"]]
        source = "requirements_bbox_centroids"

    points = points.merge(requirements, on="wind_farm", how="left")
    points["farm_id"] = points["farm_id"].fillna(points["wind_farm"])
    points["lat"] = pd.to_numeric(points["lat"], errors="coerce")
    points["lon"] = pd.to_numeric(points["lon"], errors="coerce")
    points = points.dropna(subset=["lat", "lon"])
    if points.empty:
        raise ValueError("No project sample points available for FINO station matching.")
    points.attrs["source"] = source
    return points


def _match_role(distance_km: float) -> tuple[str, str]:
    if distance_km <= 5.0:
        return (
            "direct_validation_candidate",
            "Very close to a project farm/sample point; useful for direct validation with wake/sector caveats.",
        )
    if distance_km <= 25.0:
        return (
            "nearby_validation_candidate",
            "Near enough for source comparison, but not a farm-wide assignment without representativeness checks.",
        )
    if distance_km <= 75.0:
        return (
            "regional_benchmark_candidate",
            "Useful as a regional benchmark; direct farm validation requires careful caveats.",
        )
    if distance_km <= 150.0:
        return (
            "context_only",
            "Too far for direct validation; useful only as wider regional context.",
        )
    return (
        "too_far_for_direct_validation",
        "Distance is too large for direct validation of farm-level metocean assignment.",
    )


def _build_station_farm_matches(
    stations: pd.DataFrame,
    sample_points: pd.DataFrame,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    centroid_points = sample_points[sample_points["sample_point_type"] == "farm_centroid"]
    if centroid_points.empty:
        centroid_points = sample_points.sort_values("sample_point_id").drop_duplicates("wind_farm")

    for station in stations.itertuples(index=False):
        all_distances = haversine_distance(
            float(station.lat),
            float(station.lon),
            sample_points["lat"].to_numpy(dtype=float),
            sample_points["lon"].to_numpy(dtype=float),
        )
        point_distances = sample_points.assign(_distance_km=all_distances)
        nearest_by_farm = (
            point_distances.sort_values("_distance_km")
            .groupby("wind_farm", as_index=False)
            .first()
            .rename(
                columns={
                    "sample_point_id": "nearest_sample_point_id",
                    "_distance_km": "nearest_sample_distance_km",
                }
            )
        )

        centroid_distances = haversine_distance(
            float(station.lat),
            float(station.lon),
            centroid_points["lat"].to_numpy(dtype=float),
            centroid_points["lon"].to_numpy(dtype=float),
        )
        centroid_by_farm = centroid_points.assign(
            farm_centroid_distance_km=centroid_distances
        )[["wind_farm", "farm_centroid_distance_km"]]

        merged = nearest_by_farm.merge(centroid_by_farm, on="wind_farm", how="left")
        for _, farm in merged.iterrows():
            role, note = _match_role(float(farm["nearest_sample_distance_km"]))
            rows.append(
                {
                    "station_id": station.station_id,
                    "station_name": station.station_name,
                    "wind_farm": farm["wind_farm"],
                    "farm_id": farm["farm_id"],
                    "distance_km": round(float(farm["nearest_sample_distance_km"]), 3),
                    "nearest_sample_point_id": farm["nearest_sample_point_id"],
                    "nearest_sample_distance_km": round(
                        float(farm["nearest_sample_distance_km"]), 3
                    ),
                    "farm_centroid_distance_km": round(
                        float(farm["farm_centroid_distance_km"]), 3
                    ),
                    "match_role": role,
                    "representativeness_note": note,
                }
            )

    return (
        pd.DataFrame(rows)
        .sort_values(["station_id", "nearest_sample_distance_km", "wind_farm"])
        .reset_index(drop=True)
    )


def _path_file_count(path: Path) -> int | None:
    if not path.exists():
        return None
    if path.is_file():
        return 1
    return sum(1 for candidate in path.rglob("*") if candidate.is_file())


def _build_local_inventory(raw_metocean_root: Path, processed_fino_root: Path) -> pd.DataFrame:
    rows = []
    for station in ["FINO1", "FINO2", "FINO3"]:
        raw_path = raw_metocean_root / station
        file_count = _path_file_count(raw_path)
        rows.append(
            {
                "asset": f"{station} raw placeholder",
                "path": str(raw_path),
                "exists": raw_path.exists(),
                "file_count": file_count if file_count is not None else 0,
                "usability": (
                    "empty_or_unverified_placeholder"
                    if not file_count
                    else "local_files_present_but_not_validated"
                ),
            }
        )
    processed_count = _path_file_count(processed_fino_root)
    rows.append(
        {
            "asset": "processed FINO archive",
            "path": str(processed_fino_root),
            "exists": processed_fino_root.exists(),
            "file_count": processed_count if processed_count is not None else 0,
            "usability": "missing_no_processed_fino_archive"
            if not processed_fino_root.exists()
            else "present_but_requires_schema_validation",
        }
    )
    return pd.DataFrame(rows)


def build_fino_metadata_access_plan(
    requirements_path: Path = DEFAULT_REQUIREMENTS_PATH,
    bathymetry_points_path: Path = DEFAULT_BATHYMETRY_POINTS,
    raw_metocean_root: Path = DEFAULT_RAW_METOCEAN_ROOT,
    processed_fino_root: Path = DEFAULT_PROCESSED_FINO_ROOT,
) -> FinoPlanningResult:
    """Build an in-memory FINO metadata/access plan."""
    stations = pd.DataFrame(STATION_CATALOG)
    sample_points = _load_project_sample_points(
        bathymetry_points_path=bathymetry_points_path,
        requirements_path=requirements_path,
    )
    matches = _build_station_farm_matches(stations, sample_points)
    local_inventory = _build_local_inventory(raw_metocean_root, processed_fino_root)

    nearest_counts = (
        matches[matches["match_role"].isin(["direct_validation_candidate", "nearby_validation_candidate"])]
        .groupby("station_id")["wind_farm"]
        .nunique()
        .to_dict()
    )
    summary = {
        "station_count": int(len(stations)),
        "station_ids": ", ".join(stations["station_id"].tolist()),
        "sample_point_source": sample_points.attrs.get("source", "unknown"),
        "sample_point_count": int(len(sample_points)),
        "farm_count": int(sample_points["wind_farm"].nunique()),
        "raw_fino_file_count": int(local_inventory["file_count"].sum()),
        "processed_fino_archive_exists": bool(processed_fino_root.exists()),
        "access_route": "BSH-Login plus Insitu specialist procedure approval",
        "project_role": "validation_baseline_not_farm_wide_primary_source",
        "nearest_candidate_counts_within_25km": {
            station_id: int(nearest_counts.get(station_id, 0))
            for station_id in stations["station_id"].tolist()
        },
        "recommended_first_pilot": (
            "FINO1 station metadata plus one small wave time-series slice, then "
            "compare Hs/Tp/wave direction against nearby Alpha Ventus or German "
            "Bight NORA3/NWS/Baltic-unavailable source records where temporally overlapping."
        ),
        "future_processed_root": str(processed_fino_root),
    }

    return FinoPlanningResult(
        station_plan=stations,
        station_farm_matches=matches,
        local_inventory=local_inventory,
        summary=summary,
        station_metadata_schema=list(STATION_METADATA_SCHEMA),
        timeseries_schema=list(TIMESERIES_SCHEMA),
        station_farm_match_schema=list(STATION_FARM_MATCH_SCHEMA),
        output_report=None,
    )


def _markdown_table(frame: pd.DataFrame, columns: list[str], *, max_rows: int | None = None) -> str:
    if frame.empty:
        return "_No rows._"
    display = frame[columns]
    if max_rows is not None:
        display = display.head(max_rows)
    return display.to_markdown(index=False)


def render_fino_metadata_access_report(result: FinoPlanningResult) -> str:
    """Render the FINO metadata/access dry-run as Markdown."""
    summary = result.summary
    station_columns = [
        "station_id",
        "lat",
        "lon",
        "available_start",
        "available_end",
        "cadence",
        "access_method",
    ]
    station_variable_columns = [
        "station_id",
        "wave_variables",
        "wind_variables",
        "current_variables",
        "measurement_heights_depths",
    ]
    match_columns = [
        "station_id",
        "wind_farm",
        "distance_km",
        "nearest_sample_point_id",
        "farm_centroid_distance_km",
        "match_role",
    ]
    local_columns = ["asset", "path", "exists", "file_count", "usability"]

    candidate_matches = result.station_farm_matches[
        result.station_farm_matches["match_role"].isin(
            ["direct_validation_candidate", "nearby_validation_candidate", "regional_benchmark_candidate"]
        )
    ]

    lines = [
        "# FINO Metadata and Access Planning Dry-Run",
        "",
        "Status: dry-run planning only. No FINO bulk download, FINO time-series import, current download, source fusion, 10-minute interpolation, NORA3 rerun, or final dwell-metocean rebuild was run.",
        "",
        "## Executive Conclusion",
        "",
        "FINO should be treated as in-situ validation and baseline evidence, not as an automatic farm-wide metocean source. The next safe step is human/credential preparation for BSH Insitu access, followed by a small FINO1 metadata and wave-slice pilot.",
        "",
        f"- station_count: `{summary['station_count']}`",
        f"- station_ids: `{summary['station_ids']}`",
        f"- project_sample_point_source: `{summary['sample_point_source']}`",
        f"- project_sample_point_count: `{summary['sample_point_count']}`",
        f"- farm_count: `{summary['farm_count']}`",
        f"- processed_fino_archive_exists: `{summary['processed_fino_archive_exists']}`",
        f"- project_role: `{summary['project_role']}`",
        "",
        "## Local FINO Inventory",
        "",
        _markdown_table(result.local_inventory, local_columns),
        "",
        "## Public Station Metadata Plan",
        "",
        _markdown_table(result.station_plan, station_columns),
        "",
        "## Likely Variables and Measurement Metadata",
        "",
        _markdown_table(result.station_plan, station_variable_columns),
        "",
        "## Access Requirements",
        "",
        "- Register for BSH-Login.",
        "- Request the `Insitu` specialist procedure.",
        "- After approval, export only selected station-variable windows for the pilot.",
        "- Preserve native portal files and QC/source metadata before normalization.",
        "- Confirm exact licence and source acknowledgement terms after portal access.",
        "",
        "## Station-to-Farm Matching",
        "",
        "Distances are computed from FINO station coordinates to accepted common metocean sample points. Match roles are planning labels only and do not authorize farm-wide extrapolation.",
        "",
        f"- nearby_candidate_counts_within_25km: `{summary['nearest_candidate_counts_within_25km']}`",
        "",
        "### Closest Candidate Matches",
        "",
        _markdown_table(candidate_matches, match_columns, max_rows=30),
        "",
        "### Nearest Ten Farms Per Station",
        "",
    ]

    for station_id, station_matches in result.station_farm_matches.groupby("station_id"):
        lines.extend(
            [
                f"#### {station_id}",
                "",
                _markdown_table(station_matches, match_columns, max_rows=10),
                "",
            ]
        )

    lines.extend(
        [
            "## Proposed Station Metadata Schema",
            "",
            "\n".join(f"- `{column}`" for column in result.station_metadata_schema),
            "",
            "## Proposed Time-Series Schema For Later Pilot",
            "",
            "\n".join(f"- `{column}`" for column in result.timeseries_schema),
            "",
            "## Proposed Station-Farm Match Schema",
            "",
            "\n".join(f"- `{column}`" for column in result.station_farm_match_schema),
            "",
            "## Validation Gates For Later FINO Import",
            "",
            "\n".join(f"- {gate}" for gate in VALIDATION_GATES),
            "",
            "## Recommended First FINO Pilot",
            "",
            summary["recommended_first_pilot"],
            "",
            "Proposed future command, after access is granted and an importer exists:",
            "",
            "```bash",
            "/opt/anaconda3/bin/python scripts/import_fino_timeseries.py \\",
            "  --station FINO1 \\",
            "  --variables hs tp wave_direction \\",
            "  --start 2022-01-01 \\",
            "  --end 2022-01-31 \\",
            "  --raw-root Data/Raw/Metocean/FINO1 \\",
            "  --output-dir Data/Processed/metocean/fino \\",
            "  --dry-run",
            "```",
            "",
            "## Do-Not-Do List",
            "",
            "\n".join(f"- {item}" for item in DO_NOT_DO),
            "",
            "## Public Metadata Sources",
            "",
            "\n".join(f"- `{name}`: {url}" for name, url in SOURCE_LINKS.items()),
            "",
            "## Files Created Or Modified By This Dry-Run",
            "",
            "- This report only.",
            "",
        ]
    )
    return "\n".join(lines)


def plan_fino_metadata_access(
    output_report: Path = DEFAULT_OUTPUT_REPORT,
    requirements_path: Path = DEFAULT_REQUIREMENTS_PATH,
    bathymetry_points_path: Path = DEFAULT_BATHYMETRY_POINTS,
    raw_metocean_root: Path = DEFAULT_RAW_METOCEAN_ROOT,
    processed_fino_root: Path = DEFAULT_PROCESSED_FINO_ROOT,
    dry_run: bool = True,
) -> FinoPlanningResult:
    """Create the FINO metadata/access dry-run report."""
    if not dry_run:
        raise ValueError(
            "FINO metadata planning supports dry-run mode only. "
            "Do not use it to download or import FINO time series."
        )

    result = build_fino_metadata_access_plan(
        requirements_path=requirements_path,
        bathymetry_points_path=bathymetry_points_path,
        raw_metocean_root=raw_metocean_root,
        processed_fino_root=processed_fino_root,
    )
    output_report.parent.mkdir(parents=True, exist_ok=True)
    output_report.write_text(render_fino_metadata_access_report(result), encoding="utf-8")
    return FinoPlanningResult(
        station_plan=result.station_plan,
        station_farm_matches=result.station_farm_matches,
        local_inventory=result.local_inventory,
        summary=result.summary,
        station_metadata_schema=result.station_metadata_schema,
        timeseries_schema=result.timeseries_schema,
        station_farm_match_schema=result.station_farm_match_schema,
        output_report=output_report,
    )
