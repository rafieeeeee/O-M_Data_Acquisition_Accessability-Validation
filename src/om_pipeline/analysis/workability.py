import os
import glob
from dataclasses import dataclass

import pandas as pd
import numpy as np


@dataclass(frozen=True)
class WorkabilitySurfaceBin:
    """Binning rule for one observed metocean or grouping feature."""

    column: str
    edges: tuple[float, ...]
    name: str | None = None
    unit: str | None = None
    right: bool = False
    include_lowest: bool = True

    @property
    def alias(self) -> str:
        if self.name:
            return self.name
        cleaned = self.column
        for prefix in ("active_",):
            if cleaned.startswith(prefix):
                cleaned = cleaned[len(prefix):]
        for suffix in ("_mean",):
            if cleaned.endswith(suffix):
                cleaned = cleaned[: -len(suffix)]
        return cleaned


@dataclass(frozen=True)
class WorkabilitySurfaceSpec:
    """Configurable observed-envelope surface definition.

    The output is an observed occupancy/summary table. It is not calibrated
    P(operation | weather), because the input rows only represent observed
    operations unless an explicit denominator is supplied later.
    """

    name: str
    bins: tuple[WorkabilitySurfaceBin, ...]
    grouping_columns: tuple[str, ...] = ()
    subset_column: str | None = "dwell_tier"
    subset_values: tuple[str, ...] = ("Tier A",)
    output_format: str = "long_form_observed_occupancy"
    label: str = "observed/provisional workability envelope"

    @property
    def feature_columns(self) -> tuple[str, ...]:
        return tuple(bin_spec.column for bin_spec in self.bins)


def hs_tp_surface_spec(
    grouping_columns: tuple[str, ...] = (),
) -> WorkabilitySurfaceSpec:
    """Return the default Hs/Tp observed-envelope preset."""

    return WorkabilitySurfaceSpec(
        name="hs_tp",
        bins=(
            WorkabilitySurfaceBin(
                column="active_hs_mean",
                edges=tuple(np.round(np.arange(0.0, 5.25, 0.25), 3)),
                name="hs",
                unit="m",
            ),
            WorkabilitySurfaceBin(
                column="active_tp_mean",
                edges=tuple(np.round(np.arange(0.0, 16.5, 0.5), 3)),
                name="tp",
                unit="s",
            ),
        ),
        grouping_columns=grouping_columns,
    )


def build_workability_surface_table(
    df: pd.DataFrame,
    spec: WorkabilitySurfaceSpec | None = None,
) -> pd.DataFrame:
    """Build a simulator-ready long-form observed occupancy surface."""

    spec = spec or hs_tp_surface_spec()
    required = list(spec.feature_columns) + list(spec.grouping_columns)
    if spec.subset_column:
        required.append(spec.subset_column)
    missing = sorted(set(required) - set(df.columns))
    if missing:
        raise ValueError(f"Workability surface input is missing required columns: {missing}")

    working = df.copy()
    input_rows = int(len(working))
    if spec.subset_column and spec.subset_values:
        working = working[working[spec.subset_column].isin(spec.subset_values)].copy()
    subset_rows = int(len(working))

    for column in spec.feature_columns:
        working[column] = pd.to_numeric(working[column], errors="coerce")

    null_feature_mask = working[list(spec.feature_columns)].isna().any(axis=1)
    excluded_null_feature_rows = int(null_feature_mask.sum())
    complete = working.loc[~null_feature_mask].copy()
    complete_feature_rows = int(len(complete))

    bin_columns: list[str] = []
    for bin_spec in spec.bins:
        bin_column = f"{bin_spec.alias}_bin"
        bin_columns.append(bin_column)
        complete[bin_column] = pd.cut(
            complete[bin_spec.column],
            bins=list(bin_spec.edges),
            right=bin_spec.right,
            include_lowest=bin_spec.include_lowest,
        )

    groupby_columns = list(spec.grouping_columns) + bin_columns
    if complete.empty:
        table = pd.DataFrame(columns=groupby_columns + ["observed_count"])
    else:
        table = (
            complete.groupby(groupby_columns, observed=False, dropna=False)
            .size()
            .reset_index(name="observed_count")
        )

    for bin_spec, bin_column in zip(spec.bins, bin_columns):
        table[f"{bin_spec.alias}_feature_column"] = bin_spec.column
        table[f"{bin_spec.alias}_unit"] = bin_spec.unit
        table[f"{bin_spec.alias}_bin_left"] = table[bin_column].map(
            lambda value: float(value.left) if pd.notna(value) else np.nan
        )
        table[f"{bin_spec.alias}_bin_right"] = table[bin_column].map(
            lambda value: float(value.right) if pd.notna(value) else np.nan
        )
        table[bin_column] = table[bin_column].map(
            lambda value: f"{value.left:g}-{value.right:g}" if pd.notna(value) else None
        )

    table.insert(0, "surface_name", spec.name)
    table.insert(1, "surface_label", spec.label)
    table.insert(2, "output_format", spec.output_format)
    table.insert(3, "feature_columns", "|".join(spec.feature_columns))
    table.insert(4, "grouping_columns", "|".join(spec.grouping_columns))
    table.insert(5, "input_rows", input_rows)
    table.insert(6, "subset_rows", subset_rows)
    table.insert(7, "complete_feature_rows", complete_feature_rows)
    table.insert(8, "excluded_null_feature_rows", excluded_null_feature_rows)
    return table

def build_vessel_registry(project_root: str) -> dict:
    """
    Scans all Interim Fleet Registry CSV files to build a robust mapping of MMSI to vessel length.
    Takes the maximum length found for each MMSI across all files to ensure completeness.
    """
    mmsi_metadata = {}
    glob_pattern = os.path.join(project_root, "Data", "Interim", "Fleet_Registry_*.csv")
    files = glob.glob(glob_pattern)
    print(f"Scanning {len(files)} fleet registry files for vessel metadata...")
    
    for f in files:
        try:
            df = pd.read_csv(f)
            df.columns = [c.strip() for c in df.columns]
            
            # Identify key columns using flexible synonym resolution
            m_col = next((c for c in df.columns if c.lower() == 'mmsi'), None)
            l_col = next((c for c in df.columns if c.lower() in ['length', 'vessel_length']), None)
            n_col = next((c for c in df.columns if c.lower() in ['name', 'shipname']), None)
            t_col = next((c for c in df.columns if c.lower() in ['empirical_type', 'ship type', 'ship_type']), None)
            
            if m_col is None:
                continue
                
            df[m_col] = pd.to_numeric(df[m_col], errors='coerce')
            for _, row in df.iterrows():
                m = row[m_col]
                if pd.isna(m):
                    continue
                m = int(m)
                if m <= 0:
                    continue
                    
                if m not in mmsi_metadata:
                    mmsi_metadata[m] = {'length': np.nan, 'name': None, 'vessel_type': None}
                
                if l_col and pd.notna(row[l_col]):
                    val = float(row[l_col])
                    mmsi_metadata[m]['length'] = max(mmsi_metadata[m]['length'], val) if pd.notna(mmsi_metadata[m]['length']) else val
                if n_col and pd.notna(row[n_col]) and str(row[n_col]).strip() != "":
                    mmsi_metadata[m]['name'] = str(row[n_col]).strip()
                if t_col and pd.notna(row[t_col]) and str(row[t_col]).strip() != "":
                    mmsi_metadata[m]['vessel_type'] = str(row[t_col]).strip()
        except Exception as e:
            # Silence specific file errors, report skipped file
            print(f"  Warning: Skipping or errored on file {os.path.basename(f)}: {e}")
            
    print(f"Loaded specifications for {len(mmsi_metadata)} unique MMSIs from fleet registry.")
    return mmsi_metadata

def classify_vessel(length: float) -> str:
    """Classifies vessel based on length thresholds."""
    if pd.isna(length) or length <= 0:
        return 'Unclassified (Missing Length)'
    elif length < 40.0:
        return 'CTV (<40m)'
    elif length >= 60.0:
        return 'SOV (>=60m)'
    else:
        return 'Medium-sized Vessel'

def load_and_enrich_dwells(project_root: str) -> pd.DataFrame:
    """
    Loads cross-farm weather features and enriches them with registry-derived vessel characteristics.
    """
    backfill_path = os.path.join(project_root, "Data", "Processed", "ais_dwell_backfill", "cross_farm_dwell_weather_features.parquet")
    pilot_path = os.path.join(project_root, "Data", "Processed", "cross_farm_dwell_weather_features.parquet")
    
    if not os.path.exists(backfill_path):
        raise FileNotFoundError(f"Required backfill weather features file not found at: {backfill_path}")
        
    print(f"Loading backfill parquets from: {backfill_path}")
    df_backfill = pd.read_parquet(backfill_path)
    
    if os.path.exists(pilot_path):
        print(f"Loading pilot parquets from: {pilot_path}")
        df_pilot = pd.read_parquet(pilot_path)
        if not df_pilot.empty:
            # Align columns if they differ slightly
            if 'wind_farm' not in df_pilot.columns and 'farm_id' in df_pilot.columns:
                df_pilot['wind_farm'] = df_pilot['farm_id']
            if 'wind_farm' not in df_backfill.columns and 'farm_id' in df_backfill.columns:
                df_backfill['wind_farm'] = df_backfill['farm_id']
            
            # Combine backfill and pilot
            df_combined = pd.concat([df_backfill, df_pilot], ignore_index=True)
        else:
            df_combined = df_backfill.copy()
    else:
        df_combined = df_backfill.copy()
        
    print(f"Loaded {len(df_combined)} total raw dwells.")
    
    # Enrich with vessel registry specifications
    vessel_reg = build_vessel_registry(project_root)
    df_combined['mmsi'] = pd.to_numeric(df_combined['mmsi'], errors='coerce').fillna(0).astype(int)
    
    df_combined['length_enriched'] = df_combined['mmsi'].apply(lambda m: vessel_reg.get(m, {}).get('length', np.nan))
    df_combined['name_enriched'] = df_combined['mmsi'].apply(lambda m: vessel_reg.get(m, {}).get('name', None))
    df_combined['vessel_type_enriched'] = df_combined['mmsi'].apply(lambda m: vessel_reg.get(m, {}).get('vessel_type', None))
    
    # Apply classification
    df_combined['vessel_class'] = df_combined['length_enriched'].apply(classify_vessel)
    
    return df_combined

def compute_tp_bin_statistics(df: pd.DataFrame, vessel_class: str = None) -> pd.DataFrame:
    """
    Groups successful O&M dwells into wave period (Tp) bins and calculates:
      - count: number of observed operations
      - median_hs: median active significant wave height
      - boundary_hs: 95th percentile active significant wave height (the operational limit)
      - max_hs: maximum wave height observed during operation
    """
    # Filter to Tier A dwells (successful asset-proximal O&M proxy)
    active_df = df[df['dwell_tier'] == 'Tier A'].copy()
    
    # Filter for specific vessel class if requested
    if vessel_class:
        active_df = active_df[active_df['vessel_class'] == vessel_class]
        
    # Drop rows missing weather data
    active_df = active_df.dropna(subset=['active_hs_mean', 'active_tp_mean'])
    
    if active_df.empty:
        return pd.DataFrame()
        
    # Define wave period bins reflecting standard offshore operations
    bins = [0, 3, 4, 5, 6, 7, 8, 10, 15]
    labels = ['(0, 3]', '(3, 4]', '(4, 5]', '(5, 6]', '(6, 7]', '(7, 8]', '(8, 10]', '(10, 15]']
    
    active_df['tp_bin'] = pd.cut(active_df['active_tp_mean'], bins=bins, labels=labels, right=True)
    
    # Aggregate statistics
    stats = active_df.groupby('tp_bin', observed=False)['active_hs_mean'].agg(
        count='count',
        median_hs='median',
        boundary_hs=lambda x: np.percentile(x, 95) if len(x) > 0 else np.nan,
        max_hs='max'
    ).reset_index()
    
    return stats

def generate_workability_lookup_matrix(df: pd.DataFrame, vessel_class: str = None) -> pd.DataFrame:
    """
    Generates a 2D lookup matrix mapping significant wave height (Hs) rows to wave period (Tp) columns.
    For each cell (hs, tp), it calculates the access probability:
      - 1.0 (fully workable) if wave height is below the 50th percentile of successful operations for that Tp.
      - 0.0 (infeasible) if wave height is above the 95th percentile (workability boundary).
      - Linear interpolation between 1.0 and 0.0 for wave heights in between.
    
    Grid resolution:
      - Hs: 0.0m to 5.0m in 0.1m increments.
      - Tp: 2.0s to 15.0s in 0.5s increments.
    """
    active_df = df[df['dwell_tier'] == 'Tier A'].copy()
    if vessel_class:
        active_df = active_df[active_df['vessel_class'] == vessel_class]
        
    active_df = active_df.dropna(subset=['active_hs_mean', 'active_tp_mean'])
    
    # Establish grid coordinates
    hs_grid = np.round(np.arange(0.0, 5.1, 0.1), 1)
    tp_grid = np.round(np.arange(2.0, 15.5, 0.5), 1)
    
    # Initialize output matrix with 0.0
    matrix = pd.DataFrame(0.0, index=hs_grid, columns=tp_grid)
    matrix.index.name = 'hs'
    
    # If not enough data, return empty stubs
    if len(active_df) < 5:
        return matrix
        
    # For each Tp column in our simulation-ready grid, we estimate local stats using a Gaussian sliding kernel
    # to ensure a smooth, continuous workability surface instead of jagged step bins.
    for tp in tp_grid:
        # Distance weight from active_tp_mean to the current column's Tp
        # Using a bandwidth of 1.0s to smooth statistical estimations
        delta_tp = active_df['active_tp_mean'] - tp
        weights = np.exp(-0.5 * (delta_tp / 1.0) ** 2)
        
        # Skip if sum of weights is too low (no observed operations in this wave period regime)
        if weights.sum() < 2.0:
            continue
            
        # Calculate weighted percentiles
        sorted_indices = np.argsort(active_df['active_hs_mean'].values)
        sorted_hs = active_df['active_hs_mean'].values[sorted_indices]
        sorted_w = weights.values[sorted_indices]
        
        cum_w = np.cumsum(sorted_w)
        cum_w /= cum_w[-1] # Normalise to [0, 1]
        
        # 50th and 95th percentile significant wave heights for this Tp
        p50_hs = sorted_hs[np.searchsorted(cum_w, 0.5)]
        p95_hs = sorted_hs[np.searchsorted(cum_w, 0.95)]
        
        # Smooth boundaries to reasonable ranges
        p50_hs = max(p50_hs, 0.5)
        p95_hs = max(p95_hs, 1.0)
        if p95_hs <= p50_hs:
            p95_hs = p50_hs + 0.5
            
        # Fill the Hs column based on our access probability logic
        for hs in hs_grid:
            if hs <= p50_hs:
                prob = 1.0
            elif hs >= p95_hs:
                prob = 0.0
            else:
                # Linear fall-off from 1.0 down to 0.0
                prob = (p95_hs - hs) / (p95_hs - p50_hs)
            matrix.at[hs, tp] = np.round(prob, 3)
            
    return matrix
