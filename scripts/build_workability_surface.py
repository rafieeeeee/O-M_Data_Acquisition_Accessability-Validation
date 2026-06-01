"""
scripts/build_workability_surface.py
----------------------------------------
Builds empirical wave period (Hs × Tp) workability surfaces and lookup matrices 
from fused AIS dwell telemetry and high-resolution NORA3 wave hindcasts.
Produces publication-quality figures and simulation-ready lookup tables.
"""

import os
import sys
import argparse
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path

# Ensure the src package is importable
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.om_pipeline.analysis.workability import (
    load_and_enrich_dwells,
    compute_tp_bin_statistics,
    generate_workability_lookup_matrix
)

# ---------------------------------------------------------------------------
# Styling and Colors for Premium Academic Visuals (Design-System Aligned)
# ---------------------------------------------------------------------------
BG_DARK = "#0b0d10"
BG_CARD = "#12161f"
BORDER_DARK = "#1f293d"
TEXT_WHITE = "#f8fafc"
TEXT_MUTED = "#94a3b8"

ACCENT_BLUE = "#3b82f6"   # CTV color
ACCENT_GREEN = "#10b981"  # SOV color
ACCENT_RED = "#ef4444"    # Boundary limit line color
ACCENT_AMBER = "#f59e0b"  # Medium-sized color

def set_premium_plot_style():
    """Sets a gorgeous, premium dark theme for our figures matching the dashboard design system."""
    sns.set_theme(style="white", rc={
        "figure.facecolor": BG_DARK,
        "axes.facecolor": BG_CARD,
        "axes.edgecolor": BORDER_DARK,
        "grid.color": BORDER_DARK,
        "text.color": TEXT_WHITE,
        "axes.labelcolor": TEXT_MUTED,
        "xtick.color": TEXT_MUTED,
        "ytick.color": TEXT_MUTED,
        "font.family": "sans-serif",
        "font.sans-serif": ["Outfit", "Inter", "Helvetica", "Arial", "sans-serif"],
        "grid.linestyle": "--",
        "grid.linewidth": 0.8
    })

def plot_workability_surface(df: pd.DataFrame, overall_stats: pd.DataFrame, 
                             ctv_stats: pd.DataFrame, sov_stats: pd.DataFrame, 
                             output_path: Path):
    """
    Generates a gorgeous, publication-ready scatter plot demonstrating the non-linear
    Hs x Tp workability boundary for CTVs, SOVs, and overall operations.
    """
    set_premium_plot_style()
    
    # Filter for Tier A dwells with valid weather
    active_df = df[(df['dwell_tier'] == 'Tier A') & df['active_hs_mean'].notna() & df['active_tp_mean'].notna()].copy()
    
    # Setup figure
    fig, ax = plt.subplots(figsize=(10, 7))
    ax.grid(True)
    
    # Scatter observed dwells
    scatter_palette = {
        'CTV (<40m)': ACCENT_BLUE,
        'SOV (>=60m)': ACCENT_GREEN,
        'Medium-sized Vessel': ACCENT_AMBER,
        'Unclassified (Missing Length)': TEXT_MUTED
    }
    
    sns.scatterplot(
        data=active_df,
        x='active_tp_mean',
        y='active_hs_mean',
        hue='vessel_class',
        palette=scatter_palette,
        alpha=0.6,
        s=45,
        edgecolor=BG_DARK,
        linewidth=0.5,
        ax=ax
    )
    
    # Helper to interpolate boundary lines smoothly
    def plot_boundary_line(stats_df, color, label, linestyle='-', linewidth=2.5):
        if stats_df.empty or 'boundary_hs' not in stats_df.columns:
            return
        
        # Bin centers (rough Tp values)
        tp_centers = [2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 9.0, 12.5]
        
        # Interpolate the boundary_hs values for plotting
        bounds = stats_df['boundary_hs'].values
        
        # Filter out NaNs for interpolation
        valid = [i for i, b in enumerate(bounds) if pd.notna(b)]
        if not valid:
            return
            
        plot_x = [tp_centers[i] for i in valid]
        plot_y = [bounds[i] for i in valid]
        
        # Smooth with spline or linear interpolation for gorgeous curves
        from scipy.interpolate import make_interp_spline
        if len(plot_x) > 3:
            x_new = np.linspace(min(plot_x), max(plot_x), 100)
            spl = make_interp_spline(plot_x, plot_y, k=2)
            y_smooth = spl(x_new)
            ax.plot(x_new, y_smooth, color=color, linestyle=linestyle, linewidth=linewidth, label=label)
        else:
            ax.plot(plot_x, plot_y, color=color, linestyle=linestyle, linewidth=linewidth, label=label)
            
    # Plot empirical boundaries
    plot_boundary_line(overall_stats, ACCENT_RED, "Overall empirical limit (95th %ile)", linewidth=3)
    plot_boundary_line(ctv_stats, ACCENT_BLUE, "CTV-specific limit (95th %ile)", linestyle='--')
    plot_boundary_line(sov_stats, ACCENT_GREEN, "SOV-specific limit (95th %ile)", linestyle='--')
    
    # Customise axis limits and labels
    ax.set_xlim(1.5, 14.5)
    ax.set_ylim(-0.1, 5.5)
    
    ax.set_xlabel("Wave Peak Period ($T_p$) [seconds]", fontsize=12, fontweight='bold', labelpad=10)
    ax.set_ylabel("Significant Wave Height ($H_s$) [meters]", fontsize=12, fontweight='bold', labelpad=10)
    
    # Beautiful legend matching the theme
    legend = ax.legend(
        title="Vessel Archetype & Boundaries",
        loc="upper left",
        frameon=True,
        facecolor=BG_CARD,
        edgecolor=BORDER_DARK
    )
    plt.setp(legend.get_title(), color=TEXT_WHITE, fontweight='bold')
    
    # Premium Titles & Layout
    plt.title("Empirical Multi-Parameter Vessel Workability Surface", 
              fontsize=15, fontweight='bold', pad=20, color=TEXT_WHITE)
    
    # Subtle subtitle overlay inside chart
    ax.text(0.98, 0.02, 
             "N=581 Successful O&M Access Events\nData Source: Fused AIS Dwells & NORA3 Hindcast", 
             color=TEXT_MUTED, fontsize=9, ha='right', va='bottom', transform=ax.transAxes,
             bbox=dict(facecolor=BG_CARD, edgecolor=BORDER_DARK, boxstyle='round,pad=0.5'))
             
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, facecolor=BG_DARK)
    plt.close()
    print(f"  [SUCCESS] Thesis workability surface chart generated → {output_path}")

def main():
    parser = argparse.ArgumentParser(
        description="Build empirical O&M workability surfaces and simulation parameters."
    )
    parser.add_argument("--project-root", default=str(PROJECT_ROOT), help="Project directory root.")
    parser.add_argument("--force", action="store_true", help="Overwrite existing files.")
    args = parser.parse_args()
    
    root_dir = Path(args.project_root)
    reports_dir = root_dir / "reports"
    tables_dir = reports_dir / "tables"
    figures_dir = reports_dir / "figures"
    processed_dir = root_dir / "Data" / "Processed"
    
    # Ensure folders exist
    tables_dir.mkdir(parents=True, exist_ok=True)
    figures_dir.mkdir(parents=True, exist_ok=True)
    processed_dir.mkdir(parents=True, exist_ok=True)
    
    print("=" * 60)
    print("O&M WORKABILITY SURFACE GENERATION ENGINE")
    print("=" * 60)
    
    # 1. Load and enrich data
    try:
        df = load_and_enrich_dwells(str(root_dir))
    except FileNotFoundError as e:
        print(f"[ERROR] {e}")
        print("Please ensure weather join is active and that parquet files exist in Data/Processed/.")
        sys.exit(1)
        
    # 2. Compute Wave Period (Tp) Statistics
    print("\nComputing Wave Period (Tp) Operational Envelopes...")
    overall_stats = compute_tp_bin_statistics(df)
    ctv_stats = compute_tp_bin_statistics(df, "CTV (<40m)")
    sov_stats = compute_tp_bin_statistics(df, "SOV (>=60m)")
    
    if overall_stats.empty:
        print("[ERROR] Statistics are empty. Verify Tier A dwells and weather column coverage.")
        sys.exit(1)
        
    # Save overall summary table
    summary_path = tables_dir / "workability_surface_summary.csv"
    overall_stats.to_csv(summary_path, index=False)
    print(f"  [SUCCESS] Binned wave statistics written → {summary_path}")
    
    # Print a beautiful console view of the stats
    print("\nEmpirical Operating Limits by Wave Period Bin:")
    print("-" * 70)
    print(f"{'Tp Bin':<12} | {'Dwells':<8} | {'Median Hs (m)':<15} | {'Empirical Limit (m)':<20} | {'Max Hs (m)':<10}")
    print("-" * 70)
    for _, row in overall_stats.iterrows():
        print(f"{row['tp_bin']:<12} | {int(row['count']):<8} | {row['median_hs']:<15.2f} | {row['boundary_hs']:<20.2f} | {row['max_hs']:<10.2f}")
    print("-" * 70)
    
    # 3. Generate Simulation access lookup matrices
    print("\nGenerating simulation access lookup matrices...")
    matrix_overall = generate_workability_lookup_matrix(df)
    matrix_ctv = generate_workability_lookup_matrix(df, "CTV (<40m)")
    matrix_sov = generate_workability_lookup_matrix(df, "SOV (>=60m)")
    
    # Save matrices
    matrix_path = processed_dir / "workability_matrix.csv"
    ctv_matrix_path = processed_dir / "workability_matrix_ctv.csv"
    sov_matrix_path = processed_dir / "workability_matrix_sov.csv"
    
    matrix_overall.to_csv(matrix_path)
    matrix_ctv.to_csv(ctv_matrix_path)
    matrix_sov.to_csv(sov_matrix_path)
    
    print(f"  [SUCCESS] Simulation access lookup table generated → {matrix_path}")
    print(f"  [SUCCESS] CTV specific lookup table generated → {ctv_matrix_path}")
    print(f"  [SUCCESS] SOV specific lookup table generated → {sov_matrix_path}")
    
    # 4. Generate premium thesis figures
    print("\nGenerating publication-quality workability surface figure...")
    plot_path = figures_dir / "empirical_hs_tp_workability_surface.png"
    plot_workability_surface(df, overall_stats, ctv_stats, sov_stats, plot_path)
    
    print("\n" + "=" * 60)
    print("O&M WORKABILITY SURFACE GENERATION — COMPLETED")
    print("=" * 60)

if __name__ == "__main__":
    main()
