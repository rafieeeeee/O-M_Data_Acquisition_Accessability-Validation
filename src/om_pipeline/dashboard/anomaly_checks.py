"""
anomaly_checks.py

Modular helper library to detect pipeline anomalies and data missingness
from the O&M pipeline index parquet dataset. Strictly read-only.
"""

from pathlib import Path
import pandas as pd
import numpy as np

# Central default thresholds for noise-control
DEFAULT_THRESHOLDS = {
    "duplicate_threshold": 0.3,          # flag duplicate shares > 30%
    "tier_d_threshold": 0.4,             # flag Tier D transit shares > 40%
    "dwell_visit_ratio_threshold": 2.0,   # flag dwell-to-visit ratio > 2.0x
    "weather_missing_threshold": 0.25,   # flag weather missingness fraction > 25%
    "weather_expected_max_year": 2024    # NORA3 is expected only up to 2024
}

# Wind Farm Commissioning Dates used for scope-aware missing raw source checks
# Formatted as (Year, Month). No raw source files are expected before these dates.
FARM_COMMISSIONING_DATES = {
    "Alpha_Ventus": (2010, 4),
    "Amrumbank_West": (2015, 10),
    "Arkona-Becken_Südost": (2019, 1),
    "Baltic_Eagle": (2025, 7),
    "Borkum_Riffgrund_1": (2015, 10),
    "EnBW_Windpark_Baltic_1": (2011, 5),
    "EnBW_Windpark_Baltic_2": (2015, 10),
    "Nordsee_Ost": (2015, 5),
    "Wikinger": (2018, 10)
}

def run_anomaly_checks(df_index: pd.DataFrame, thresholds: dict = None, root_path: Path = None) -> pd.DataFrame:
    """
    Evaluates 10 separate pipeline diagnostic rules against the data observatory index.

    Args:
        df_index: DataFrame parsed from data_index.parquet
        thresholds: Dictionary of threshold overrides (merges with DEFAULT_THRESHOLDS)
        root_path: Alternative project root path to resolve processed manifest logs

    Returns:
        DataFrame containing columns:
        ['Severity', 'Wind Farm', 'Year', 'Month', 'Anomaly Type', 'Evidence', 'Suggested Action']
        Sorted by severity (Critical, Warning, Info), then Wind Farm, Year, Month.
    """
    if df_index.empty:
        return pd.DataFrame(columns=[
            "Severity", "Wind Farm", "Year", "Month",
            "Anomaly Type", "Evidence", "Suggested Action"
        ])

    # Resolve thresholds
    t = DEFAULT_THRESHOLDS.copy()
    if thresholds:
        t.update(thresholds)

    # Load processed manifest to safely distinguish backlog from true crashes
    df_proc = pd.DataFrame()
    manifest_rel_path = "Data/Processed/ais_dwell_backfill/logs/backfill_manifest.csv"
    manifest_path = Path(manifest_rel_path)
    if root_path:
        manifest_path = root_path / manifest_rel_path

    if manifest_path.exists():
        try:
            df_proc = pd.read_csv(manifest_path)
        except Exception:
            pass

    anomalies = []

    for _, row in df_index.iterrows():
        severity = "Info"  # Default fallback for this iteration
        farm_id = row.get("farm_id", "")
        farm_name = row.get("farm_name", "")
        year = int(row.get("year", 0))
        month = int(row.get("month", 0))
        is_target = bool(row.get("is_target_cluster", False))

        # Determine operational status
        is_operational = False
        if farm_id in FARM_COMMISSIONING_DATES:
            comm_year, comm_month = FARM_COMMISSIONING_DATES[farm_id]
            is_operational = (year > comm_year) or (year == comm_year and month >= comm_month)

        # Look up manifest status if available
        manifest_status = None
        if not df_proc.empty:
            match_m = df_proc[
                (df_proc["farm_id"] == farm_name) &
                (df_proc["year"] == year) &
                (df_proc["month"] == month)
            ]
            if not match_m.empty:
                manifest_status = match_m.iloc[0].get("status")

        raw_status = str(row.get("raw_source_status")).lower()
        raw_rows = int(row.get("raw_source_rows", 0))
        raw_size = float(row.get("raw_source_file_size_mb", 0.0))

        filtered_status = str(row.get("filtered_ais_status")).lower()
        filtered_rows = int(row.get("filtered_ais_rows", 0))

        visits_status = str(row.get("visits_status")).lower()
        visits_count = int(row.get("visits_count", 0))

        dwells_status = str(row.get("dwells_status")).lower()
        dwells_count = int(row.get("dwells_count", 0))
        tier_a = int(row.get("tier_a_count", 0))
        tier_b = int(row.get("tier_b_count", 0))
        tier_c = int(row.get("tier_c_count", 0))
        tier_d = int(row.get("tier_d_count", 0))

        dup_flags = int(row.get("duplicate_flag_count", 0))

        weather_status = str(row.get("weather_join_status")).lower()
        weather_count = int(row.get("weather_join_count", 0))
        weather_missing = float(row.get("weather_missing_fraction", np.nan))

        # 1. Missing Raw Source (Scope-aware)
        if raw_status == "missing" and is_operational:
            severity = "Warning" if is_target else "Info"
            anomalies.append({
                "Severity": severity,
                "Wind Farm": farm_name,
                "Year": year,
                "Month": month,
                "Anomaly Type": "Missing Raw Source",
                "Evidence": f"Raw S3 file is missing in Data/Raw/AIS after operational date ({comm_year}-{comm_month:02d}).",
                "Raw Status": raw_status,
                "Raw Size (MB)": raw_size,
                "Visits": visits_count,
                "Dwells": dwells_count,
                "Manifest Status": manifest_status,
                "Suggested Action": "Verify S3 bucket sync or check if backfill partition is planned."
            })

        # 2. Interrupted Filtered AIS
        if raw_status == "exists" and raw_size > 0:
            if filtered_status in ("interrupted", "empty"):
                # Differentiate severity based on manifest outcome
                if manifest_status == "success":
                    severity = "Critical"
                elif is_target and is_operational:
                    severity = "Warning"
                else:
                    severity = "Info"

                anomalies.append({
                    "Severity": severity,
                    "Wind Farm": farm_name,
                    "Year": year,
                    "Month": month,
                    "Anomaly Type": "Interrupted Filtered AIS",
                    "Evidence": f"Raw source exists ({raw_size} MB) but filtered AIS status is '{filtered_status}' (Manifest status: {manifest_status}).",
                    "Raw Status": raw_status,
                    "Raw Size (MB)": raw_size,
                    "Visits": visits_count,
                    "Dwells": dwells_count,
                    "Manifest Status": manifest_status,
                    "Suggested Action": "Re-run AIS cleaning pipeline slice; check log files for OOM or timeout."
                })

        # 3. Raw Exists but Visits Missing (Scope-aware & Manifest-aware noise control)
        if raw_status == "exists" and raw_size > 0.1 and visits_status == "missing":
            # If the partition is in the manifest but marked as empty-box or skipped, it is empty by design!
            if manifest_status in ("success_no_ais_in_bbox", "skipped_missing_source"):
                pass
            elif manifest_status == "success":
                # Real pipeline failure - manifest claims success but visits file is missing!
                severity = "Critical"
                anomalies.append({
                    "Severity": severity,
                    "Wind Farm": farm_name,
                    "Year": year,
                    "Month": month,
                    "Anomaly Type": "Missing Visits Output",
                    "Evidence": f"Backfill manifest reports success, but visits parquet file is missing locally.",
                    "Raw Status": raw_status,
                    "Raw Size (MB)": raw_size,
                    "Visits": visits_count,
                    "Dwells": dwells_count,
                    "Manifest Status": manifest_status,
                    "Suggested Action": "Execute the farm visit extraction runner (run_ais_dwell_backfill.py)."
                })
            elif is_operational:
                # Unprocessed backlog (Warning for target cluster, Info for validation)
                severity = "Warning" if is_target else "Info"
                anomalies.append({
                    "Severity": severity,
                    "Wind Farm": farm_name,
                    "Year": year,
                    "Month": month,
                    "Anomaly Type": "Missing Visits Output",
                    "Evidence": f"Raw S3 file exists ({raw_size} MB) but partition has not been backfilled yet (unprocessed backlog).",
                    "Raw Status": raw_status,
                    "Raw Size (MB)": raw_size,
                    "Visits": visits_count,
                    "Dwells": dwells_count,
                    "Manifest Status": manifest_status,
                    "Suggested Action": "Execute backfill runner for this partition when scheduled."
                })

        # 4. Visits Exist but Dwells Missing (Scope-aware & Manifest-aware)
        if visits_status == "exists" and visits_count > 0 and dwells_status == "missing":
            if manifest_status == "success":
                severity = "Critical"
            elif is_target and is_operational:
                severity = "Warning"
            else:
                severity = "Info"

            anomalies.append({
                "Severity": severity,
                "Wind Farm": farm_name,
                "Year": year,
                "Month": month,
                "Anomaly Type": "Missing Dwells Output",
                "Evidence": f"Visits partition contains {visits_count} entries, but dwells parquet is missing (Manifest status: {manifest_status}).",
                "Raw Status": raw_status,
                "Raw Size (MB)": raw_size,
                "Visits": visits_count,
                "Dwells": dwells_count,
                "Manifest Status": manifest_status,
                "Suggested Action": "Execute the dwell event detection runner."
            })

        # 5. Dwells Exist but Weather Join Missing
        if dwells_status == "exists" and dwells_count > 0 and weather_status == "missing":
            # Metocean weather features availability check (Optional metocean status)
            is_nora3_expected = (year <= t["weather_expected_max_year"])
            severity = "Warning" if is_nora3_expected else "Info"
            anomalies.append({
                "Severity": severity,
                "Wind Farm": farm_name,
                "Year": year,
                "Month": month,
                "Anomaly Type": "Missing Weather Join",
                "Evidence": f"Dwell events ({dwells_count}) exist in output, but metocean weather joined features are missing (Expected NORA3: {is_nora3_expected}).",
                "Raw Status": raw_status,
                "Raw Size (MB)": raw_size,
                "Visits": visits_count,
                "Dwells": dwells_count,
                "Manifest Status": manifest_status,
                "Suggested Action": "Run metocean feature extraction and join pipeline slice."
            })

        # 6. High Duplicate Flag Share
        if dwells_status == "exists" and dwells_count > 0:
            dup_share = dup_flags / dwells_count
            if dup_share > t["duplicate_threshold"]:
                severity = "Warning"
                anomalies.append({
                    "Severity": severity,
                    "Wind Farm": farm_name,
                    "Year": year,
                    "Month": month,
                    "Anomaly Type": "High Duplicate Share",
                    "Evidence": f"Duplicate overlap flag rate is {dup_share:.1%} ({dup_flags}/{dwells_count} events).",
                    "Raw Status": raw_status,
                    "Raw Size (MB)": raw_size,
                    "Visits": visits_count,
                    "Dwells": dwells_count,
                    "Manifest Status": manifest_status,
                    "Suggested Action": "Audit spatial coordinates overlap or check for multi-farm duplicate telemetry."
                })

        # 7. High Tier D Share
        if dwells_status == "exists" and dwells_count > 0:
            tier_d_share = tier_d / dwells_count
            if tier_d_share > t["tier_d_threshold"]:
                severity = "Info"
                anomalies.append({
                    "Severity": severity,
                    "Wind Farm": farm_name,
                    "Year": year,
                    "Month": month,
                    "Anomaly Type": "High Tier D Share",
                    "Evidence": f"Tier D (outer transit/drifting) share is {tier_d_share:.1%} ({tier_d}/{dwells_count} events).",
                    "Raw Status": raw_status,
                    "Raw Size (MB)": raw_size,
                    "Visits": visits_count,
                    "Dwells": dwells_count,
                    "Manifest Status": manifest_status,
                    "Suggested Action": "Inspect spatial extent; verify if vessel was transiting without array interaction."
                })

        # 8. High Dwell-to-Visit Ratio
        if visits_status == "exists" and visits_count > 0 and dwells_status == "exists":
            ratio = dwells_count / visits_count
            if ratio > t["dwell_visit_ratio_threshold"]:
                severity = "Warning"
                anomalies.append({
                    "Severity": severity,
                    "Wind Farm": farm_name,
                    "Year": year,
                    "Month": month,
                    "Anomaly Type": "High Dwell-to-Visit Ratio",
                    "Evidence": f"Dwells-to-visits ratio is unusually high at {ratio:.1f}x ({dwells_count} dwells across {visits_count} visits).",
                    "Raw Status": raw_status,
                    "Raw Size (MB)": raw_size,
                    "Visits": visits_count,
                    "Dwells": dwells_count,
                    "Manifest Status": manifest_status,
                    "Suggested Action": "Audit telemetry sampling density; check for excessive segmentation of dwell events."
                })

        # 9. Zero-Row Months where Source Files Exist
        if raw_status == "exists" and raw_size > 0.1 and is_operational:
            if (filtered_status == "exists" and filtered_rows == 0) or \
               (visits_status == "exists" and visits_count == 0) or \
               (dwells_status == "exists" and dwells_count == 0):
                # Backlogs showing 0s on processed partitions
                if manifest_status in ("success_no_ais_in_bbox", "skipped_missing_source"):
                    # Success empty is expected, not an anomaly!
                    pass
                else:
                    severity = "Warning" if is_target else "Info"
                    anomalies.append({
                        "Severity": severity,
                        "Wind Farm": farm_name,
                        "Year": year,
                        "Month": month,
                        "Anomaly Type": "Zero-Row Pipeline Output",
                        "Evidence": f"Raw file exists ({raw_size} MB) but telemetry row filters returned 0 active counts (Filtered: {filtered_rows}, Visits: {visits_count}, Dwells: {dwells_count}, Manifest status: {manifest_status}).",
                        "Raw Status": raw_status,
                        "Raw Size (MB)": raw_size,
                        "Visits": visits_count,
                        "Dwells": dwells_count,
                        "Manifest Status": manifest_status,
                        "Suggested Action": "Verify if spatial bounds filtered out all pings or if vessel list is empty."
                    })

        # 10. Optional Weather/Metocean Missingness
        if weather_status == "joined" and weather_count > 0:
            if not pd.isna(weather_missing) and weather_missing > t["weather_missing_threshold"]:
                is_nora3_expected = (year <= t["weather_expected_max_year"])
                severity = "Warning" if is_nora3_expected else "Info"
                anomalies.append({
                    "Severity": severity,
                    "Wind Farm": farm_name,
                    "Year": year,
                    "Month": month,
                    "Anomaly Type": "High Weather Missingness",
                    "Evidence": f"Metocean weather missingness fraction is {weather_missing:.1%} within joined records.",
                    "Raw Status": raw_status,
                    "Raw Size (MB)": raw_size,
                    "Visits": visits_count,
                    "Dwells": dwells_count,
                    "Manifest Status": manifest_status,
                    "Suggested Action": "Check NORA3 grid coverage bounds or fill missing grid interpolation holes."
                })

    df_anomalies = pd.DataFrame(anomalies)
    if df_anomalies.empty:
        return pd.DataFrame(columns=[
            "Severity", "Wind Farm", "Year", "Month",
            "Anomaly Type", "Evidence", "Suggested Action"
        ])

    # Sort by severity priority: Critical -> Warning -> Info
    severity_order = {"Critical": 0, "Warning": 1, "Info": 2}
    df_anomalies["severity_priority"] = df_anomalies["Severity"].map(severity_order)

    df_anomalies = df_anomalies.sort_values(
        by=["severity_priority", "Wind Farm", "Year", "Month"]
    ).drop(columns=["severity_priority"])

    return df_anomalies.reset_index(drop=True)
