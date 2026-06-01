import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import os

def generate_workability_ecdf(df: pd.DataFrame, output_dir: str):
    """
    Generates Empirical Cumulative Distribution Functions (ECDF) for Hs and U_hub
    segmented by Vessel Class and Behavioral Tier.
    """
    os.makedirs(output_dir, exist_ok=True)
    
    # Filter for relevant tiers and valid values
    plot_df = df[df['dwell_tier'].isin(['Tier A', 'Tier C'])].copy()
    plot_df['class_tier'] = plot_df['vessel_class'] + " - " + plot_df['dwell_tier']
    
    # ECDFs for Significant Wave Height (Hs)
    if 'hs' in plot_df.columns:
        plt.figure(figsize=(10, 6))
        sns.ecdfplot(data=plot_df, x='hs', hue='class_tier')
        plt.title('ECDF of Significant Wave Height (Hs) by Vessel Class and Tier')
        plt.xlabel('Significant Wave Height (m)')
        plt.ylabel('Cumulative Probability')
        plt.grid(True, linestyle='--', alpha=0.7)
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, 'hs_ecdf.png'))
        plt.close()
        
    # ECDFs for Wind Speed (U_hub or wind_speed_100m)
    wind_col = 'wind_speed_100m' if 'wind_speed_100m' in plot_df.columns else 'U_hub'
    if wind_col in plot_df.columns:
        plt.figure(figsize=(10, 6))
        sns.ecdfplot(data=plot_df, x=wind_col, hue='class_tier')
        plt.title('ECDF of Hub-Height Wind Speed by Vessel Class and Tier')
        plt.xlabel('Wind Speed (m/s)')
        plt.ylabel('Cumulative Probability')
        plt.grid(True, linestyle='--', alpha=0.7)
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, 'wind_ecdf.png'))
        plt.close()

def generate_workability_boxplots(df: pd.DataFrame, output_dir: str):
    """
    Generates Boxplots for Hs and U_hub segmented by Vessel Class and Behavioral Tier.
    """
    os.makedirs(output_dir, exist_ok=True)
    
    plot_df = df[df['dwell_tier'].isin(['Tier A', 'Tier C'])].copy()
    
    # Boxplot for Significant Wave Height (Hs)
    if 'hs' in plot_df.columns:
        plt.figure(figsize=(10, 6))
        sns.boxplot(data=plot_df, x='vessel_class', y='hs', hue='dwell_tier')
        plt.title('Boxplot of Significant Wave Height (Hs) by Vessel Class and Tier')
        plt.xlabel('Vessel Class')
        plt.ylabel('Significant Wave Height (m)')
        plt.grid(True, linestyle='--', alpha=0.7, axis='y')
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, 'hs_boxplot.png'))
        plt.close()
        
    # Boxplot for Wind Speed
    wind_col = 'wind_speed_100m' if 'wind_speed_100m' in plot_df.columns else 'U_hub'
    if wind_col in plot_df.columns:
        plt.figure(figsize=(10, 6))
        sns.boxplot(data=plot_df, x='vessel_class', y=wind_col, hue='dwell_tier')
        plt.title('Boxplot of Hub-Height Wind Speed by Vessel Class and Tier')
        plt.xlabel('Vessel Class')
        plt.ylabel('Wind Speed (m/s)')
        plt.grid(True, linestyle='--', alpha=0.7, axis='y')
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, 'wind_boxplot.png'))
        plt.close()

def generate_oss_control_matrix(df: pd.DataFrame) -> pd.DataFrame:
    """
    OSS Control Routine: Segments data using near_substation flag.
    Outputs a summary table comparing workability thresholds (Hs 90th percentile)
    with and without the inclusion of OSS data.
    """
    if 'hs' not in df.columns or 'near_substation' not in df.columns:
        return pd.DataFrame()
        
    # Ensure Tier A only for core turbine/substation maintenance comparison
    base_df = df[df['dwell_tier'] == 'Tier A'].copy()
    
    results = []
    for v_class in base_df['vessel_class'].unique():
        class_df = base_df[base_df['vessel_class'] == v_class]
        
        # Including OSS (All data)
        p90_all = class_df['hs'].quantile(0.90) if not class_df.empty else np.nan
        
        # Excluding OSS (Turbines only)
        turb_df = class_df[~class_df['near_substation'].astype(bool)]
        p90_turb = turb_df['hs'].quantile(0.90) if not turb_df.empty else np.nan
        
        results.append({
            'vessel_class': v_class,
            'Hs_90th_Including_OSS': p90_all,
            'Hs_90th_Excluding_OSS': p90_turb,
            'Delta (Excl - Incl)': p90_turb - p90_all if pd.notna(p90_all) and pd.notna(p90_turb) else np.nan,
            'Total_Events': len(class_df),
            'Turbine_Events': len(turb_df)
        })
        
    return pd.DataFrame(results)
