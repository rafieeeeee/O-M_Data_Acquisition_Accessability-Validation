"""Provisional Stage 1 Hs/Tp observed-envelope builder.

This module deliberately builds an observed operational envelope from rows
where operations were seen. It does not estimate ``P(operation | weather)``
because the current table does not include a complete denominator of
non-operation weather windows.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


PROVISIONAL_STAGE1_LABEL = (
    "Provisional NORA3-derived Tier A wave-only observed operational envelope"
)

CORE_COLUMNS = [
    "dwell_id",
    "visit_id",
    "mmsi",
    "dwell_tier",
    "wind_farm",
    "start_utc",
    "end_utc",
    "active_hs_mean",
    "active_tp_mean",
]

CORE_NULL_RATE_COLUMNS = [
    "active_hs_mean",
    "active_tp_mean",
    "dwell_tier",
    "wind_farm",
    "mmsi",
    "start_utc",
]

TP_BIN_EDGES = [0, 3, 4, 5, 6, 7, 8, 10, 15]
TP_BIN_LABELS = [
    "(0, 3]",
    "(3, 4]",
    "(4, 5]",
    "(5, 6]",
    "(6, 7]",
    "(7, 8]",
    "(8, 10]",
    "(10, 15]",
]


@dataclass(frozen=True)
class ProvisionalStage1Outputs:
    """Paths and validation metrics written by the Stage 1 builder."""

    processed_output_dir: Path
    report_output_dir: Path
    files: dict[str, Path]
    validation: dict[str, Any]


def _jsonable(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        return float(value)
    if isinstance(value, np.bool_):
        return bool(value)
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return value


def _ensure_output_path(path: Path, allowed_roots: list[Path]) -> None:
    resolved = path.resolve()
    for root in allowed_roots:
        try:
            resolved.relative_to(root.resolve())
            return
        except ValueError:
            continue
    roots = ", ".join(str(root) for root in allowed_roots)
    raise ValueError(f"Refusing to write outside approved output roots: {path} not in {roots}")


def _read_dwell_weather_table(input_path: Path) -> pd.DataFrame:
    if not input_path.exists():
        raise FileNotFoundError(f"Stage 1 input file not found: {input_path}")
    df = pd.read_parquet(input_path)
    missing = sorted({"active_hs_mean", "active_tp_mean", "dwell_tier"} - set(df.columns))
    if missing:
        raise ValueError(f"Stage 1 input file is missing required columns: {missing}")
    return df


def _with_time_columns(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    if "start_utc" in result.columns:
        result["start_utc"] = pd.to_datetime(result["start_utc"], utc=True, errors="coerce")
        result["year"] = result["start_utc"].dt.year
        result["month"] = result["start_utc"].dt.month
    return result


def _select_clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    keep = [column for column in CORE_COLUMNS if column in df.columns]
    optional = [
        column
        for column in [
            "vessel_length_m",
            "length_enriched",
            "vessel_type",
            "vessel_type_enriched",
            "active_wind_speed_mean",
            "active_source_available",
            "active_weather_missing_fraction",
        ]
        if column in df.columns
    ]
    clean = df[keep + optional].copy()
    clean["analysis_label"] = PROVISIONAL_STAGE1_LABEL
    return clean


def build_stage1_subsets(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return primary Tier A Hs/Tp rows and all-tier Hs/Tp sensitivity rows."""
    working = _with_time_columns(df)
    wave_complete = working["active_hs_mean"].notna() & working["active_tp_mean"].notna()
    sensitivity = working.loc[wave_complete].copy()
    primary = sensitivity.loc[sensitivity["dwell_tier"] == "Tier A"].copy()
    return primary, sensitivity


def duplicate_dwell_id_count(df: pd.DataFrame) -> int | None:
    if "dwell_id" not in df.columns:
        return None
    return int(df["dwell_id"].duplicated().sum())


def _value_counts_dict(series: pd.Series) -> dict[str, int]:
    return {str(key): int(value) for key, value in series.value_counts(dropna=False).items()}


def summarize_subset(df: pd.DataFrame, subset_name: str) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "analysis_label": PROVISIONAL_STAGE1_LABEL,
        "subset": subset_name,
        "rows": int(len(df)),
        "duplicate_dwell_id_count": duplicate_dwell_id_count(df),
        "farm_count": int(df["wind_farm"].nunique()) if "wind_farm" in df.columns else None,
        "mmsi_count": int(df["mmsi"].nunique()) if "mmsi" in df.columns else None,
        "dwell_tier_distribution": (
            _value_counts_dict(df["dwell_tier"]) if "dwell_tier" in df.columns else {}
        ),
    }
    if "year" in df.columns:
        years = df["year"].dropna()
        summary["year_min"] = int(years.min()) if not years.empty else None
        summary["year_max"] = int(years.max()) if not years.empty else None
        summary["year_distribution"] = _value_counts_dict(df["year"].dropna().astype(int))
    if "month" in df.columns:
        months = df["month"].dropna()
        summary["month_min"] = int(months.min()) if not months.empty else None
        summary["month_max"] = int(months.max()) if not months.empty else None
        summary["month_distribution"] = _value_counts_dict(df["month"].dropna().astype(int))
    for column in ["active_hs_mean", "active_tp_mean"]:
        if column in df.columns:
            values = pd.to_numeric(df[column], errors="coerce").dropna()
            summary[f"{column}_min"] = float(values.min()) if not values.empty else None
            summary[f"{column}_median"] = float(values.median()) if not values.empty else None
            summary[f"{column}_max"] = float(values.max()) if not values.empty else None
    return summary


def null_rate_table(df: pd.DataFrame, columns: list[str] | None = None) -> pd.DataFrame:
    columns = columns or CORE_NULL_RATE_COLUMNS
    rows = []
    for column in columns:
        if column not in df.columns:
            rows.append(
                {
                    "analysis_label": PROVISIONAL_STAGE1_LABEL,
                    "column": column,
                    "present": False,
                    "non_null_count": 0,
                    "null_count": None,
                    "null_rate": None,
                }
            )
            continue
        null_count = int(df[column].isna().sum())
        rows.append(
            {
                "analysis_label": PROVISIONAL_STAGE1_LABEL,
                "column": column,
                "present": True,
                "non_null_count": int(df[column].notna().sum()),
                "null_count": null_count,
                "null_rate": float(null_count / len(df)) if len(df) else None,
            }
        )
    return pd.DataFrame(rows)


def compute_tp_boundary_table(df: pd.DataFrame, subset_name: str) -> pd.DataFrame:
    working = df.dropna(subset=["active_hs_mean", "active_tp_mean"]).copy()
    working["tp_bin"] = pd.cut(
        working["active_tp_mean"],
        bins=TP_BIN_EDGES,
        labels=TP_BIN_LABELS,
        right=True,
        include_lowest=False,
    )
    if working.empty:
        return pd.DataFrame(
            columns=[
                "analysis_label",
                "subset",
                "tp_bin",
                "count",
                "hs_p50",
                "hs_p75",
                "hs_p90",
                "hs_p95",
                "hs_min",
                "hs_max",
            ]
        )
    grouped = working.groupby("tp_bin", observed=False)["active_hs_mean"]
    stats = grouped.agg(
        count="count",
        hs_p50="median",
        hs_p75=lambda x: np.percentile(x, 75) if len(x) else np.nan,
        hs_p90=lambda x: np.percentile(x, 90) if len(x) else np.nan,
        hs_p95=lambda x: np.percentile(x, 95) if len(x) else np.nan,
        hs_min="min",
        hs_max="max",
    ).reset_index()
    stats.insert(0, "subset", subset_name)
    stats.insert(0, "analysis_label", PROVISIONAL_STAGE1_LABEL)
    return stats


def compute_occupancy_matrix(
    df: pd.DataFrame,
    hs_step: float = 0.25,
    tp_step: float = 0.5,
    hs_max: float = 6.0,
    tp_min: float = 0.0,
    tp_max: float = 16.0,
) -> pd.DataFrame:
    """Build an observed Hs/Tp occupancy count matrix, not an access probability."""
    working = df.dropna(subset=["active_hs_mean", "active_tp_mean"]).copy()
    hs_edges = np.round(np.arange(0.0, hs_max + hs_step, hs_step), 3)
    tp_edges = np.round(np.arange(tp_min, tp_max + tp_step, tp_step), 3)
    working["hs_bin"] = pd.cut(working["active_hs_mean"], bins=hs_edges, right=False)
    working["tp_bin"] = pd.cut(working["active_tp_mean"], bins=tp_edges, right=False)
    matrix = pd.crosstab(working["hs_bin"], working["tp_bin"], dropna=False)
    matrix.index = [f"{interval.left:.2f}-{interval.right:.2f}" for interval in matrix.index]
    matrix.columns = [f"{interval.left:.1f}-{interval.right:.1f}" for interval in matrix.columns]
    matrix.index.name = "hs_bin_m"
    return matrix


def static_threshold_comparison(
    primary: pd.DataFrame,
    sensitivity: pd.DataFrame,
    thresholds: tuple[float, ...] = (1.0, 1.5, 2.0, 2.5, 3.0),
) -> pd.DataFrame:
    rows = []
    for subset_name, subset in [("primary_tier_a", primary), ("sensitivity_all_tiers", sensitivity)]:
        hs = pd.to_numeric(subset["active_hs_mean"], errors="coerce")
        total = int(hs.notna().sum())
        for threshold in thresholds:
            within = int((hs <= threshold).sum())
            rows.append(
                {
                    "analysis_label": PROVISIONAL_STAGE1_LABEL,
                    "subset": subset_name,
                    "hs_threshold_m": threshold,
                    "rows_with_hs": total,
                    "rows_at_or_below_threshold": within,
                    "share_at_or_below_threshold": float(within / total) if total else None,
                }
            )
    return pd.DataFrame(rows)


def physical_range_checks(primary: pd.DataFrame, sensitivity: pd.DataFrame) -> dict[str, Any]:
    checks: dict[str, Any] = {}
    for subset_name, subset in [("primary_tier_a", primary), ("sensitivity_all_tiers", sensitivity)]:
        hs = pd.to_numeric(subset["active_hs_mean"], errors="coerce").dropna()
        tp = pd.to_numeric(subset["active_tp_mean"], errors="coerce").dropna()
        checks[subset_name] = {
            "hs_non_negative": bool((hs >= 0).all()) if not hs.empty else None,
            "hs_below_or_equal_20m": bool((hs <= 20).all()) if not hs.empty else None,
            "tp_positive": bool((tp > 0).all()) if not tp.empty else None,
            "tp_below_or_equal_30s": bool((tp <= 30).all()) if not tp.empty else None,
        }
    return checks


def _write_scatter_plot(
    df: pd.DataFrame,
    output_path: Path,
    title: str,
    density: bool = False,
) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    plot_df = df.dropna(subset=["active_hs_mean", "active_tp_mean"]).copy()
    fig, ax = plt.subplots(figsize=(10, 7))
    if density and len(plot_df) > 0:
        image = ax.hexbin(
            plot_df["active_tp_mean"],
            plot_df["active_hs_mean"],
            gridsize=45,
            mincnt=1,
            cmap="viridis",
        )
        fig.colorbar(image, ax=ax, label="Observed dwell count")
    else:
        ax.scatter(
            plot_df["active_tp_mean"],
            plot_df["active_hs_mean"],
            s=12,
            alpha=0.35,
            edgecolors="none",
            color="#2563eb",
        )
    ax.set_title(title)
    ax.set_xlabel("Peak wave period Tp (s)")
    ax.set_ylabel("Significant wave height Hs (m)")
    ax.grid(True, alpha=0.25)
    ax.text(
        0.01,
        0.99,
        PROVISIONAL_STAGE1_LABEL,
        transform=ax.transAxes,
        ha="left",
        va="top",
        fontsize=9,
        bbox={"boxstyle": "round,pad=0.35", "facecolor": "white", "alpha": 0.85},
    )
    fig.tight_layout()
    fig.savefig(output_path, dpi=220)
    plt.close(fig)


def _write_report(
    report_path: Path,
    input_path: Path,
    primary_summary: dict[str, Any],
    sensitivity_summary: dict[str, Any],
    validation: dict[str, Any],
    files: dict[str, Path],
) -> None:
    lines = [
        "# Provisional Stage 1 Hs-Tp Readiness Report",
        "",
        f"**Label:** {PROVISIONAL_STAGE1_LABEL}",
        "",
        "This report describes an observed operational envelope from weather-joined dwell rows. "
        "It is not a probability model and must not be labelled `P(operation | weather)`.",
        "",
        "## Input",
        "",
        f"- Input file: `{input_path}`",
        f"- Input rows: {validation['input_rows']}",
        f"- Input columns: {validation['input_columns']}",
        f"- Duplicate dwell IDs: {validation['duplicate_dwell_id_count']}",
        "",
        "## Primary Subset",
        "",
        "- Filter: `active_hs_mean.notnull() AND active_tp_mean.notnull() AND dwell_tier == \"Tier A\"`",
        f"- Rows: {primary_summary['rows']}",
        f"- Farms: {primary_summary['farm_count']}",
        f"- MMSIs: {primary_summary['mmsi_count']}",
        f"- Year range: {primary_summary.get('year_min')} to {primary_summary.get('year_max')}",
        f"- Month range: {primary_summary.get('month_min')} to {primary_summary.get('month_max')}",
        "",
        "## Sensitivity Subset",
        "",
        "- Filter: `active_hs_mean.notnull() AND active_tp_mean.notnull()`",
        f"- Rows: {sensitivity_summary['rows']}",
        f"- Farms: {sensitivity_summary['farm_count']}",
        f"- MMSIs: {sensitivity_summary['mmsi_count']}",
        f"- Year range: {sensitivity_summary.get('year_min')} to {sensitivity_summary.get('year_max')}",
        f"- Month range: {sensitivity_summary.get('month_min')} to {sensitivity_summary.get('month_max')}",
        f"- Tier distribution: `{sensitivity_summary['dwell_tier_distribution']}`",
        "",
        "## Physical Range Checks",
        "",
    ]
    for subset_name, checks in validation["physical_range_checks"].items():
        lines.append(f"- `{subset_name}`: `{checks}`")

    lines.extend(
        [
            "",
            "## Output Inventory",
            "",
        ]
    )
    for label, path in sorted(files.items()):
        lines.append(f"- `{label}`: `{path}`")

    lines.extend(
        [
            "",
            "## Guardrails",
            "",
            "- Provisional NORA3-derived wave-only analysis.",
            "- Tier A is an asset-proximal observed-operation proxy.",
            "- No current fields are used.",
            "- No CTV/SOV role inference is performed.",
            "- No final source-agnostic metocean assignment table is rebuilt.",
        ]
    )
    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def build_provisional_stage1_outputs(
    input_path: Path,
    report_output_dir: Path,
    processed_output_dir: Path,
) -> ProvisionalStage1Outputs:
    """Build dedicated provisional Stage 1 outputs from the dwell-weather table."""
    report_output_dir.mkdir(parents=True, exist_ok=True)
    processed_output_dir.mkdir(parents=True, exist_ok=True)
    allowed_roots = [report_output_dir, processed_output_dir]

    df = _read_dwell_weather_table(input_path)
    primary, sensitivity = build_stage1_subsets(df)
    primary_clean = _select_clean_columns(primary)

    primary_summary = summarize_subset(primary, "primary_tier_a")
    sensitivity_summary = summarize_subset(sensitivity, "sensitivity_all_tiers")
    validation = {
        "analysis_label": PROVISIONAL_STAGE1_LABEL,
        "input_path": input_path,
        "input_exists": True,
        "input_rows": int(len(df)),
        "input_columns": int(len(df.columns)),
        "duplicate_dwell_id_count": duplicate_dwell_id_count(df),
        "primary_rows_expected_around_13574": abs(primary_summary["rows"] - 13574) <= 250,
        "sensitivity_rows_expected_around_44377": abs(sensitivity_summary["rows"] - 44377) <= 250,
        "primary_summary": primary_summary,
        "sensitivity_summary": sensitivity_summary,
        "physical_range_checks": physical_range_checks(primary, sensitivity),
    }

    files = {
        "primary_clean_parquet": processed_output_dir / "primary_tier_a_hs_tp.parquet",
        "primary_clean_csv": processed_output_dir / "primary_tier_a_hs_tp.csv",
        "primary_summary_csv": report_output_dir / "primary_subset_summary.csv",
        "sensitivity_summary_csv": report_output_dir / "sensitivity_subset_summary.csv",
        "null_rates_csv": report_output_dir / "core_null_rates.csv",
        "tp_boundary_primary_csv": report_output_dir / "tp_bin_percentile_boundary_primary.csv",
        "tp_boundary_sensitivity_csv": report_output_dir / "tp_bin_percentile_boundary_sensitivity.csv",
        "occupancy_primary_csv": report_output_dir / "hs_tp_occupancy_matrix_primary.csv",
        "occupancy_sensitivity_csv": report_output_dir / "hs_tp_occupancy_matrix_sensitivity.csv",
        "static_thresholds_csv": report_output_dir / "static_hs_threshold_comparison.csv",
        "scatter_primary_png": report_output_dir / "tier_a_hs_tp_scatter.png",
        "scatter_sensitivity_png": report_output_dir / "all_tiers_hs_tp_density.png",
        "validation_json": report_output_dir / "validation_summary.json",
        "coverage_report_md": report_output_dir / "coverage_representativeness_report.md",
    }
    for path in files.values():
        _ensure_output_path(path, allowed_roots)

    primary_clean.to_parquet(files["primary_clean_parquet"], index=False)
    primary_clean.to_csv(files["primary_clean_csv"], index=False)
    pd.DataFrame([primary_summary]).to_csv(files["primary_summary_csv"], index=False)
    pd.DataFrame([sensitivity_summary]).to_csv(files["sensitivity_summary_csv"], index=False)
    null_rate_table(df).to_csv(files["null_rates_csv"], index=False)
    compute_tp_boundary_table(primary, "primary_tier_a").to_csv(
        files["tp_boundary_primary_csv"], index=False
    )
    compute_tp_boundary_table(sensitivity, "sensitivity_all_tiers").to_csv(
        files["tp_boundary_sensitivity_csv"], index=False
    )
    compute_occupancy_matrix(primary).to_csv(files["occupancy_primary_csv"])
    compute_occupancy_matrix(sensitivity).to_csv(files["occupancy_sensitivity_csv"])
    static_threshold_comparison(primary, sensitivity).to_csv(
        files["static_thresholds_csv"], index=False
    )
    _write_scatter_plot(
        primary,
        files["scatter_primary_png"],
        "Tier A observed Hs/Tp envelope",
        density=False,
    )
    _write_scatter_plot(
        sensitivity,
        files["scatter_sensitivity_png"],
        "All-tier observed Hs/Tp sensitivity density",
        density=True,
    )
    files["validation_json"].write_text(
        json.dumps(_jsonable(validation), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    _write_report(
        report_path=files["coverage_report_md"],
        input_path=input_path,
        primary_summary=primary_summary,
        sensitivity_summary=sensitivity_summary,
        validation=validation,
        files=files,
    )

    return ProvisionalStage1Outputs(
        processed_output_dir=processed_output_dir,
        report_output_dir=report_output_dir,
        files=files,
        validation=_jsonable(validation),
    )
