"""NWS current scale preflight and Baltic hourly-current decision support.

This increment is deliberately non-extractive: it ranks farm-years for a later
NWS hourly `uo`/`vo` scale run and records whether Baltic current evidence can
be event-scale for the historical study window. It does not download currents,
build a final archive, source-fuse values, or promote legacy/simulated current
CSV outputs.
"""

from __future__ import annotations

import math
import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


DEFAULT_REQUIREMENTS = Path(
    "analysis/06_rq6_metocean_spatial_resolution/common_metocean_farm_requirements.csv"
)
DEFAULT_DWELL_WEATHER = Path(
    "Data/Processed/ais_dwell_backfill/cross_farm_dwell_weather_features.parquet"
)
DEFAULT_FUSION_V1 = Path(
    "Data/Processed/metocean/fusion_v1_source_agreement/wave_event_confidence.parquet"
)
DEFAULT_BATHYMETRY = Path("Data/Processed/metocean/bathymetry/site_bathymetry_points.parquet")
DEFAULT_OUTPUT_DIR = Path("Data/Processed/metocean/current_pilots")
DEFAULT_REPORT_DIR = Path("reports/current_pilot_v1")

NWS_ELIGIBILITY_FILENAME = "nws_current_scale_eligibility.parquet"
BALTIC_ASSESSMENT_FILENAME = "baltic_hourly_current_source_assessment.md"
NWS_PREFLIGHT_REPORT_FILENAME = "nws_current_scaling_preflight_report.md"

NWS_CURRENT_PRODUCT_ID = "NWSHELF_MULTIYEAR_PHY_004_009"
NWS_CURRENT_DATASET_ID = "cmems_mod_nws_phy-uv_my_7km-2D_PT1H-i"
NWS_CURRENT_SERVICE_URL = (
    "https://data.marine.copernicus.eu/product/NWSHELF_MULTIYEAR_PHY_004_009/services"
)
NWS_CURRENT_PUM_URL = (
    "https://catalogue.marine.copernicus.eu/documents/PUM/CMEMS-NWS-PUM-004-009-011.pdf"
)

BALTIC_REANALYSIS_PRODUCT_ID = "BALTICSEA_MULTIYEAR_PHY_003_011"
BALTIC_REANALYSIS_DATASET_ID = "cmems_mod_bal_phy_my_P1D-m"
BALTIC_REANALYSIS_URL = (
    "https://data.marine.copernicus.eu/product/BALTICSEA_MULTIYEAR_PHY_003_011/services"
)
BALTIC_FORECAST_PRODUCT_ID = "BALTICSEA_ANALYSISFORECAST_PHY_003_006"
BALTIC_FORECAST_HOURLY_DATASET_ID = "cmems_mod_bal_phy_anfc_PT1H-i"
BALTIC_FORECAST_15MIN_DATASET_ID = "cmems_mod_bal_phy_anfc_PT15M-i"
BALTIC_FORECAST_URL = (
    "https://data.marine.copernicus.eu/product/BALTICSEA_ANALYSISFORECAST_PHY_003_006/services"
)
GLOBAL_FALLBACK_PRODUCT_ID = "GLOBAL_MULTIYEAR_PHY_001_030"
GLOBAL_FALLBACK_DATASET_ID = "cmems_mod_glo_phy_my_0.083deg_P1D-m"
GLOBAL_FALLBACK_URL = (
    "https://data.marine.copernicus.eu/product/GLOBAL_MULTIYEAR_PHY_001_030/services"
)

NWS_CURRENT_DOMAIN = {
    "min_lon": -19.89,
    "max_lon": 13.0,
    "min_lat": 40.07,
    "max_lat": 65.0,
}
NWS_BALTIC_MASK_EAST_LON = 10.0

MIN_DWELL_COUNT_FOR_SCALE = 10
MIN_TIER_A_DWELL_COUNT_FOR_SCALE = 3
MAX_NORMAL_PCT_DEPTH_LE_10M = 0.50
MAX_PROCESSED_MB_PER_FARM_YEAR = 250.0
RAW_BYTES_PER_ESTIMATED_ROW = 64.0
NWS_PILOT_PROCESSED_BYTES_PER_ROW = 4_498_964 / 500_688

ELIGIBILITY_COLUMNS = [
    "wind_farm",
    "farm_id",
    "year",
    "country",
    "region",
    "dwell_count",
    "tier_a_dwell_count",
    "fusion_v1_valid_wave_count",
    "wave_confidence_a_b_count",
    "sample_point_count",
    "median_water_depth_m",
    "p10_water_depth_m",
    "pct_sample_points_depth_le_1m",
    "pct_sample_points_depth_le_5m",
    "pct_sample_points_depth_le_10m",
    "nws_product_domain_match",
    "estimated_current_rows",
    "estimated_raw_size_mb",
    "estimated_processed_size_mb",
    "estimated_runtime_class",
    "shallow_model_warning",
    "recommended_for_scale",
    "recommendation_reason",
]


@dataclass(frozen=True)
class CurrentScalingPreflightResult:
    eligibility_path: Path
    baltic_assessment_path: Path
    preflight_report_path: Path
    eligibility: pd.DataFrame


def normalize_farm_name(value: Any) -> str:
    """Normalize farm labels across display names, slugs, and data partitions."""
    asciiish = unicodedata.normalize("NFKD", str(value)).encode("ascii", "ignore")
    text = asciiish.decode("ascii").casefold()
    return re.sub(r"[^a-z0-9]+", "", text)


def farm_slug(value: Any) -> str:
    asciiish = unicodedata.normalize("NFKD", str(value)).encode("ascii", "ignore")
    text = asciiish.decode("ascii")
    text = re.sub(r"[^A-Za-z0-9]+", "_", text).strip("_")
    return text or "unknown_farm"


def hours_in_year(year: int) -> int:
    start = pd.Timestamp(year=int(year), month=1, day=1, tz="UTC")
    end = pd.Timestamp(year=int(year) + 1, month=1, day=1, tz="UTC")
    return int((end - start).total_seconds() // 3600)


def estimate_current_rows(sample_point_count: int, year: int) -> int:
    return int(max(sample_point_count, 0) * hours_in_year(int(year)))


def estimate_raw_size_mb(rows: int) -> float:
    return round(float(rows) * RAW_BYTES_PER_ESTIMATED_ROW / 1_000_000.0, 3)


def estimate_processed_size_mb(rows: int) -> float:
    return round(float(rows) * NWS_PILOT_PROCESSED_BYTES_PER_ROW / 1_000_000.0, 3)


def classify_runtime(rows: int) -> str:
    if rows < 1_000_000:
        return "small"
    if rows < 5_000_000:
        return "medium"
    return "large"


def classify_region(row: pd.Series) -> str:
    country = str(row.get("country") or "").casefold()
    centroid_lon = (float(row["min_lon"]) + float(row["max_lon"])) / 2
    if float(row["max_lon"]) > NWS_BALTIC_MASK_EAST_LON:
        return "baltic_or_transition"
    if "united kingdom" in country or "ireland" in country:
        return "uk_irish_shelf"
    if country in {"germany", "netherlands", "belgium", "denmark"}:
        return "southern_north_sea" if centroid_lon < 9.0 else "north_sea_transition"
    if country == "france":
        return "channel_or_biscay"
    return "north_west_shelf"


def classify_nws_domain_match(row: pd.Series) -> str:
    min_lon = float(row["min_lon"])
    max_lon = float(row["max_lon"])
    min_lat = float(row["min_lat"])
    max_lat = float(row["max_lat"])
    inside_bbox = (
        min_lon >= NWS_CURRENT_DOMAIN["min_lon"]
        and max_lon <= NWS_CURRENT_DOMAIN["max_lon"]
        and min_lat >= NWS_CURRENT_DOMAIN["min_lat"]
        and max_lat <= NWS_CURRENT_DOMAIN["max_lat"]
    )
    if not inside_bbox:
        return "outside_nws_current_domain"
    if max_lon > NWS_BALTIC_MASK_EAST_LON:
        return "boundary_or_baltic_mask_review"
    return "inside_nws_current_domain"


def classify_shallow_warning(pct_depth_le_10m: float) -> str:
    pct = float(pct_depth_le_10m or 0.0)
    if pct >= 0.80:
        return "severe_depth_le_10m_dominated"
    if pct > 0.50:
        return "dominated_by_depth_le_10m"
    if pct >= 0.20:
        return "moderate_depth_le_10m"
    if pct > 0.0:
        return "some_depth_le_10m"
    return "none"


def recommend_farm_year(row: pd.Series) -> tuple[str, str]:
    reasons: list[str] = []
    domain_ok = row["nws_product_domain_match"] == "inside_nws_current_domain"
    dwell_ok = int(row["dwell_count"]) >= MIN_DWELL_COUNT_FOR_SCALE
    tier_a_ok = int(row["tier_a_dwell_count"]) >= MIN_TIER_A_DWELL_COUNT_FOR_SCALE
    contextual_useful = int(row["dwell_count"]) >= 50
    sample_ok = int(row["sample_point_count"]) > 0
    storage_ok = float(row["estimated_processed_size_mb"]) <= MAX_PROCESSED_MB_PER_FARM_YEAR
    shallow_dominated = float(row["pct_sample_points_depth_le_10m"]) > MAX_NORMAL_PCT_DEPTH_LE_10M

    if domain_ok:
        reasons.append("inside NWS current domain and west of Baltic mask boundary")
    else:
        reasons.append(f"domain issue: {row['nws_product_domain_match']}")
    if dwell_ok:
        reasons.append(f"dwell_count >= {MIN_DWELL_COUNT_FOR_SCALE}")
    else:
        reasons.append(f"dwell_count < {MIN_DWELL_COUNT_FOR_SCALE}")
    if tier_a_ok:
        reasons.append(f"Tier A count >= {MIN_TIER_A_DWELL_COUNT_FOR_SCALE}")
    elif contextual_useful:
        reasons.append("Tier A count is low, but non-Tier-A sensitivity sample is non-trivial")
    else:
        reasons.append(f"Tier A count < {MIN_TIER_A_DWELL_COUNT_FOR_SCALE}")
    if shallow_dominated:
        reasons.append("more than half of sample points are <=10 m")
    elif row["shallow_model_warning"] != "none":
        reasons.append(f"shallow warning: {row['shallow_model_warning']}")
    else:
        reasons.append("bathymetry not shallow-dominated")
    if storage_ok:
        reasons.append("storage/runtime estimate acceptable")
    else:
        reasons.append("storage/runtime estimate too large for normal scale")
    if not sample_ok:
        reasons.append("no bathymetry/sample-point rows")

    if domain_ok and dwell_ok and tier_a_ok and sample_ok and storage_ok and not shallow_dominated:
        return "yes", "; ".join(reasons)
    if domain_ok and dwell_ok and sample_ok and storage_ok and (shallow_dominated or contextual_useful):
        return "stress_test_only", "; ".join(reasons)
    return "no", "; ".join(reasons)


def _read_parquet_columns(path: Path, columns: list[str]) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=columns)
    return pd.read_parquet(path, columns=columns)


def _load_requirements(path: Path) -> pd.DataFrame:
    requirements = pd.read_csv(path)
    requirements["normalized_farm"] = requirements["wind_farm"].map(normalize_farm_name)
    requirements["temporal_start"] = pd.to_datetime(requirements["temporal_start"], errors="coerce")
    requirements["temporal_end"] = pd.to_datetime(requirements["temporal_end"], errors="coerce")
    return requirements


def _load_dwell_counts(path: Path) -> pd.DataFrame:
    columns = ["wind_farm", "farm_id", "dwell_tier", "start_utc"]
    dwell = _read_parquet_columns(path, columns)
    if dwell.empty:
        return pd.DataFrame(
            columns=["normalized_farm", "year", "dwell_count", "tier_a_dwell_count"]
        )
    dwell["start_utc"] = pd.to_datetime(dwell["start_utc"], utc=True, errors="coerce")
    dwell = dwell.dropna(subset=["start_utc"]).copy()
    dwell["year"] = dwell["start_utc"].dt.year.astype(int)
    farm_label = dwell["wind_farm"].fillna(dwell.get("farm_id"))
    dwell["normalized_farm"] = farm_label.map(normalize_farm_name)
    grouped = (
        dwell.groupby(["normalized_farm", "year"], dropna=False)
        .agg(
            dwell_count=("normalized_farm", "size"),
            tier_a_dwell_count=("dwell_tier", lambda value: int((value == "Tier A").sum())),
        )
        .reset_index()
    )
    return grouped


def _load_wave_counts(path: Path) -> pd.DataFrame:
    columns = [
        "wind_farm",
        "farm_id",
        "start_utc",
        "selected_hs_mean",
        "selected_tp_mean",
        "wave_confidence_class",
    ]
    wave = _read_parquet_columns(path, columns)
    if wave.empty:
        return pd.DataFrame(
            columns=["normalized_farm", "year", "fusion_v1_valid_wave_count", "wave_confidence_a_b_count"]
        )
    wave["start_utc"] = pd.to_datetime(wave["start_utc"], utc=True, errors="coerce")
    wave = wave.dropna(subset=["start_utc"]).copy()
    wave["year"] = wave["start_utc"].dt.year.astype(int)
    farm_label = wave["wind_farm"].fillna(wave.get("farm_id"))
    wave["normalized_farm"] = farm_label.map(normalize_farm_name)
    wave["valid_wave"] = wave["selected_hs_mean"].notna() & wave["selected_tp_mean"].notna()
    wave["wave_a_b"] = wave["wave_confidence_class"].isin(["A_high", "B_medium"])
    return (
        wave.groupby(["normalized_farm", "year"], dropna=False)
        .agg(
            fusion_v1_valid_wave_count=("valid_wave", "sum"),
            wave_confidence_a_b_count=("wave_a_b", "sum"),
        )
        .reset_index()
    )


def _load_bathymetry_summary(path: Path) -> pd.DataFrame:
    bathy = _read_parquet_columns(
        path,
        ["wind_farm", "sample_point_id", "water_depth_m"],
    )
    if bathy.empty:
        return pd.DataFrame(
            columns=[
                "normalized_farm",
                "sample_point_count",
                "median_water_depth_m",
                "p10_water_depth_m",
                "pct_sample_points_depth_le_1m",
                "pct_sample_points_depth_le_5m",
                "pct_sample_points_depth_le_10m",
            ]
        )
    bathy["normalized_farm"] = bathy["wind_farm"].map(normalize_farm_name)
    bathy["water_depth_m"] = pd.to_numeric(bathy["water_depth_m"], errors="coerce")
    return (
        bathy.groupby("normalized_farm", dropna=False)
        .agg(
            sample_point_count=("sample_point_id", "nunique"),
            median_water_depth_m=("water_depth_m", "median"),
            p10_water_depth_m=("water_depth_m", lambda value: float(np.nanpercentile(value, 10))),
            pct_sample_points_depth_le_1m=("water_depth_m", lambda value: float((value <= 1.0).mean())),
            pct_sample_points_depth_le_5m=("water_depth_m", lambda value: float((value <= 5.0).mean())),
            pct_sample_points_depth_le_10m=("water_depth_m", lambda value: float((value <= 10.0).mean())),
        )
        .reset_index()
    )


def build_nws_current_scale_eligibility(
    requirements_path: Path = DEFAULT_REQUIREMENTS,
    dwell_weather_path: Path = DEFAULT_DWELL_WEATHER,
    fusion_v1_path: Path = DEFAULT_FUSION_V1,
    bathymetry_path: Path = DEFAULT_BATHYMETRY,
) -> pd.DataFrame:
    """Build farm-year eligibility rows for a later NWS current scale run."""
    requirements = _load_requirements(requirements_path)
    dwell_counts = _load_dwell_counts(dwell_weather_path)
    wave_counts = _load_wave_counts(fusion_v1_path)
    bathymetry = _load_bathymetry_summary(bathymetry_path)

    rows: list[dict[str, Any]] = []
    for _, req in requirements.iterrows():
        normalized = req["normalized_farm"]
        req_start = req["temporal_start"]
        req_end = req["temporal_end"]
        req_dwell = dwell_counts[dwell_counts["normalized_farm"].eq(normalized)]
        if req_dwell.empty:
            continue
        if not pd.isna(req_start):
            req_dwell = req_dwell[req_dwell["year"] >= int(req_start.year)]
        if not pd.isna(req_end):
            req_dwell = req_dwell[req_dwell["year"] <= int(req_end.year)]
        if req_dwell.empty:
            continue

        bathy_rows = bathymetry[bathymetry["normalized_farm"].eq(normalized)]
        bathy = bathy_rows.iloc[0].to_dict() if not bathy_rows.empty else {}
        sample_point_count = int(
            bathy.get("sample_point_count")
            if not pd.isna(bathy.get("sample_point_count", np.nan))
            else req.get("sample_point_count", 0)
        )
        base = {
            "wind_farm": req["wind_farm"],
            "farm_id": req.get("farm_id") or farm_slug(req["wind_farm"]),
            "country": req.get("country"),
            "region": classify_region(req),
            "sample_point_count": sample_point_count,
            "median_water_depth_m": float(bathy.get("median_water_depth_m", math.nan)),
            "p10_water_depth_m": float(bathy.get("p10_water_depth_m", math.nan)),
            "pct_sample_points_depth_le_1m": float(bathy.get("pct_sample_points_depth_le_1m", 0.0) or 0.0),
            "pct_sample_points_depth_le_5m": float(bathy.get("pct_sample_points_depth_le_5m", 0.0) or 0.0),
            "pct_sample_points_depth_le_10m": float(bathy.get("pct_sample_points_depth_le_10m", 0.0) or 0.0),
            "nws_product_domain_match": classify_nws_domain_match(req),
        }

        for _, dwell_row in req_dwell.iterrows():
            year = int(dwell_row["year"])
            wave_row = wave_counts[
                wave_counts["normalized_farm"].eq(normalized) & wave_counts["year"].eq(year)
            ]
            wave = wave_row.iloc[0].to_dict() if not wave_row.empty else {}
            estimated_rows = estimate_current_rows(sample_point_count, year)
            row = {
                **base,
                "year": year,
                "dwell_count": int(dwell_row["dwell_count"]),
                "tier_a_dwell_count": int(dwell_row["tier_a_dwell_count"]),
                "fusion_v1_valid_wave_count": int(wave.get("fusion_v1_valid_wave_count", 0) or 0),
                "wave_confidence_a_b_count": int(wave.get("wave_confidence_a_b_count", 0) or 0),
                "estimated_current_rows": estimated_rows,
                "estimated_raw_size_mb": estimate_raw_size_mb(estimated_rows),
                "estimated_processed_size_mb": estimate_processed_size_mb(estimated_rows),
                "estimated_runtime_class": classify_runtime(estimated_rows),
            }
            row["shallow_model_warning"] = classify_shallow_warning(
                row["pct_sample_points_depth_le_10m"]
            )
            recommendation, reason = recommend_farm_year(pd.Series(row))
            row["recommended_for_scale"] = recommendation
            row["recommendation_reason"] = reason
            rows.append(row)

    eligibility = pd.DataFrame(rows)
    if eligibility.empty:
        return pd.DataFrame(columns=ELIGIBILITY_COLUMNS)
    eligibility = eligibility[ELIGIBILITY_COLUMNS].sort_values(
        [
            "recommended_for_scale",
            "tier_a_dwell_count",
            "wave_confidence_a_b_count",
            "dwell_count",
            "pct_sample_points_depth_le_10m",
            "wind_farm",
            "year",
        ],
        ascending=[True, False, False, False, True, True, True],
    )
    category_order = {"yes": 0, "stress_test_only": 1, "no": 2}
    eligibility["_category_rank"] = eligibility["recommended_for_scale"].map(category_order).fillna(9)
    eligibility = eligibility.sort_values(
        [
            "_category_rank",
            "tier_a_dwell_count",
            "wave_confidence_a_b_count",
            "dwell_count",
            "pct_sample_points_depth_le_10m",
            "wind_farm",
            "year",
        ],
        ascending=[True, False, False, False, True, True, True],
    ).drop(columns=["_category_rank"])
    return eligibility.reset_index(drop=True)


def write_eligibility_table(eligibility: pd.DataFrame, output_dir: Path, overwrite: bool = False) -> Path:
    path = output_dir / NWS_ELIGIBILITY_FILENAME
    if path.exists() and not overwrite:
        raise FileExistsError(f"NWS current scale eligibility output already exists: {path}")
    output_dir.mkdir(parents=True, exist_ok=True)
    eligibility[ELIGIBILITY_COLUMNS].to_parquet(path, index=False)
    return path


def classify_baltic_hourly_source() -> dict[str, Any]:
    """Return a metadata-only decision for historical Baltic hourly true currents."""
    return {
        "historical_hourly_source_exists": False,
        "classification": "keep_baltic_contextual",
        "decision": (
            "No accepted historical Baltic true Eulerian hourly `uo/vo` source was found "
            "for the 2010-2020 study window. Keep the Baltic reanalysis current evidence "
            "as `B_contextual` unless a separate historical hourly product is approved."
        ),
        "candidate_recent_hourly_product_id": BALTIC_FORECAST_PRODUCT_ID,
        "candidate_recent_hourly_dataset_id": BALTIC_FORECAST_HOURLY_DATASET_ID,
        "candidate_recent_15min_dataset_id": BALTIC_FORECAST_15MIN_DATASET_ID,
        "candidate_recent_coverage": "analysis/forecast hourly and 15-minute datasets begin in late 2022",
        "recommended_pilot_if_scope_changes": (
            "If 2023-2024 Baltic current modelling becomes relevant, run a new one-farm/year "
            f"pilot against `{BALTIC_FORECAST_PRODUCT_ID}` / `{BALTIC_FORECAST_HOURLY_DATASET_ID}`."
        ),
    }


def _format_float(value: Any, digits: int = 3) -> str:
    try:
        if pd.isna(value):
            return "NA"
        return f"{float(value):.{digits}f}"
    except (TypeError, ValueError):
        return str(value)


def _markdown_table(df: pd.DataFrame, columns: list[str], limit: int | None = None) -> list[str]:
    if limit is not None:
        df = df.head(limit)
    if df.empty:
        return ["No rows."]
    lines = [
        "| " + " | ".join(columns) + " |",
        "| " + " | ".join(["---"] * len(columns)) + " |",
    ]
    for _, row in df.iterrows():
        values = []
        for col in columns:
            value = row[col]
            if isinstance(value, float):
                value = _format_float(value)
            values.append(str(value))
        lines.append("| " + " | ".join(values) + " |")
    return lines


def build_baltic_hourly_assessment_markdown() -> str:
    decision = classify_baltic_hourly_source()
    lines = [
        "# Baltic Hourly Current Source Assessment",
        "",
        "## Executive Decision",
        "",
        decision["decision"],
        "",
        "## Sources Checked",
        "",
        "| Source | Product ID | Dataset ID | True `uo/vo` | Historical Coverage | Cadence | Decision |",
        "| --- | --- | --- | --- | --- | --- | --- |",
        (
            f"| Baltic physics reanalysis | [`{BALTIC_REANALYSIS_PRODUCT_ID}`]({BALTIC_REANALYSIS_URL}) | "
            f"`{BALTIC_REANALYSIS_DATASET_ID}` | yes | 1993-2024 | daily/monthly/yearly | "
            "Accepted only as contextual evidence for historical dwell events. |"
        ),
        (
            f"| Baltic physics analysis/forecast | [`{BALTIC_FORECAST_PRODUCT_ID}`]({BALTIC_FORECAST_URL}) | "
            f"`{BALTIC_FORECAST_HOURLY_DATASET_ID}` / `{BALTIC_FORECAST_15MIN_DATASET_ID}` | yes | "
            "late 2022 onward | hourly and 15-minute | Not suitable for 2010-2020 historical evidence; possible later-period pilot only. |"
        ),
        (
            f"| Global physics reanalysis | [`{GLOBAL_FALLBACK_PRODUCT_ID}`]({GLOBAL_FALLBACK_URL}) | "
            f"`{GLOBAL_FALLBACK_DATASET_ID}` | yes | 1993-2026 | daily/monthly | Fallback assessment only; not event-scale and not downloaded. |"
        ),
        "| Baltic wave archive | `BALTICSEA_MULTIYEAR_WAV_003_015` | `cmems_mod_bal_wav_my_PT1H-i` | no | historical wave hindcast | hourly | `VSDX/VSDY` are Stokes drift, not Eulerian currents. |",
        "",
        "## Conclusion",
        "",
        "- Historical Baltic reanalysis currents remain `B_contextual` because the accepted multiyear physics dataset is daily.",
        "- The recent Baltic analysis/forecast product has true current datasets at sub-hourly/hourly cadence, but its coverage starts too late for the historical 2010-2020 thesis window.",
        "- Do not force Baltic daily currents into event-scale models.",
        "- Do not use Baltic wave `VSDX/VSDY` as current evidence.",
        "",
        "## Next Action",
        "",
        decision["recommended_pilot_if_scope_changes"],
    ]
    return "\n".join(lines) + "\n"


def write_baltic_hourly_assessment(report_dir: Path, overwrite: bool = False) -> Path:
    path = report_dir / BALTIC_ASSESSMENT_FILENAME
    if path.exists() and not overwrite:
        raise FileExistsError(f"Baltic hourly-current assessment already exists: {path}")
    report_dir.mkdir(parents=True, exist_ok=True)
    path.write_text(build_baltic_hourly_assessment_markdown(), encoding="utf-8")
    return path


def build_nws_preflight_report_markdown(eligibility: pd.DataFrame) -> str:
    recommended = eligibility[eligibility["recommended_for_scale"].eq("yes")]
    stress = eligibility[eligibility["recommended_for_scale"].eq("stress_test_only")]
    total_rows = int(recommended["estimated_current_rows"].sum()) if not recommended.empty else 0
    total_raw = float(recommended["estimated_raw_size_mb"].sum()) if not recommended.empty else 0.0
    total_processed = (
        float(recommended["estimated_processed_size_mb"].sum()) if not recommended.empty else 0.0
    )

    lines = [
        "# NWS Current Scaling Preflight Report",
        "",
        "## Research Design",
        "",
        "Current Pilot v1 proved that NWS hourly surface `uo/vo` can be event-scale at Borkum Riffgrund 2, but the pilot was only one farm/year. This preflight ranks observed farm-years before any broad current extraction so NWS domain limits, dwell evidence, Tier A density, wave-confidence support, bathymetry, shallow-water warnings, and storage/runtime are visible first.",
        "",
        "Acceptance gates for normal scale: NWS current domain match, non-trivial dwell count, non-trivial Tier A count, accepted bathymetry sample points, no dominance by <=10 m sample depths, and acceptable storage/runtime estimate. Shallow/coastal or low-Tier-A but useful cases are marked `stress_test_only`, not normal scale.",
        "",
        "## NWS Product Basis",
        "",
        f"- Product: [`{NWS_CURRENT_PRODUCT_ID}`]({NWS_CURRENT_SERVICE_URL}) / `{NWS_CURRENT_DATASET_ID}`.",
        "- Native evidence: hourly true eastward/northward sea-water velocity at approximately 7 km surface grid.",
        f"- Manual caveat: the NWS product documentation imposes a 10 m minimum model depth and warns against direct use in extensive <10 m bathymetry areas. Source: [{Path(NWS_CURRENT_PUM_URL).name}]({NWS_CURRENT_PUM_URL}).",
        "",
        "## Eligibility Summary",
        "",
        f"- Farm-year rows evaluated: {len(eligibility)}",
        f"- Recommended farm-years: {len(recommended)}",
        f"- Stress-test farm-years: {len(stress)}",
        f"- Estimated rows if all recommended farm-years are approved: {total_rows:,}",
        f"- Estimated raw subset size if all recommended farm-years are approved: {_format_float(total_raw, 1)} MB",
        f"- Estimated processed parquet size if all recommended farm-years are approved: {_format_float(total_processed, 1)} MB",
        "- Size formula: rows = sample points x hourly timestamps; raw estimate = 64 bytes/row; processed estimate is calibrated from the accepted NWS current pilot parquet.",
        "",
        "## Top 10 Recommended NWS Scale Farm-Years",
        "",
    ]
    lines.extend(
        _markdown_table(
            recommended,
            [
                "wind_farm",
                "year",
                "dwell_count",
                "tier_a_dwell_count",
                "wave_confidence_a_b_count",
                "sample_point_count",
                "median_water_depth_m",
                "pct_sample_points_depth_le_10m",
                "estimated_current_rows",
            ],
            limit=10,
        )
    )
    lines.extend(["", "## Top 5 Stress-Test Farm-Years", ""])
    lines.extend(
        _markdown_table(
            stress,
            [
                "wind_farm",
                "year",
                "dwell_count",
                "tier_a_dwell_count",
                "pct_sample_points_depth_le_10m",
                "shallow_model_warning",
                "recommendation_reason",
            ],
            limit=5,
        )
    )
    lines.extend(
        [
            "",
            "## Recommendation",
            "",
            "Proceed with a second NWS scaled extraction batch of the top 5-10 recommended farm-years before approving all recommended farm-years. Borkum Riffgrund 2 demonstrated the method, but the preflight shows enough candidate breadth that a controlled batch is the right next write.",
            "",
            "Do not scale Baltic historical currents as event-scale evidence. Keep Baltic current evidence contextual unless a separate recent-period hourly Baltic pilot is intentionally approved.",
            "",
            "## Guardrails",
            "",
            "- No NWS current download or extraction was run in this preflight.",
            "- No Baltic current download or extraction was run.",
            "- No global fallback extraction was run.",
            "- Legacy CMEMS current CSVs and simulated/fallback currents remain banned as research evidence.",
        ]
    )
    return "\n".join(lines) + "\n"


def write_nws_preflight_report(
    eligibility: pd.DataFrame,
    report_dir: Path,
    overwrite: bool = False,
) -> Path:
    path = report_dir / NWS_PREFLIGHT_REPORT_FILENAME
    if path.exists() and not overwrite:
        raise FileExistsError(f"NWS current scaling preflight report already exists: {path}")
    report_dir.mkdir(parents=True, exist_ok=True)
    path.write_text(build_nws_preflight_report_markdown(eligibility), encoding="utf-8")
    return path


def run_current_scaling_preflight(
    requirements_path: Path = DEFAULT_REQUIREMENTS,
    dwell_weather_path: Path = DEFAULT_DWELL_WEATHER,
    fusion_v1_path: Path = DEFAULT_FUSION_V1,
    bathymetry_path: Path = DEFAULT_BATHYMETRY,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    report_dir: Path = DEFAULT_REPORT_DIR,
    overwrite: bool = False,
) -> CurrentScalingPreflightResult:
    eligibility = build_nws_current_scale_eligibility(
        requirements_path=requirements_path,
        dwell_weather_path=dwell_weather_path,
        fusion_v1_path=fusion_v1_path,
        bathymetry_path=bathymetry_path,
    )
    eligibility_path = write_eligibility_table(eligibility, output_dir, overwrite=overwrite)
    baltic_path = write_baltic_hourly_assessment(report_dir, overwrite=overwrite)
    preflight_path = write_nws_preflight_report(eligibility, report_dir, overwrite=overwrite)
    return CurrentScalingPreflightResult(
        eligibility_path=eligibility_path,
        baltic_assessment_path=baltic_path,
        preflight_report_path=preflight_path,
        eligibility=eligibility,
    )

