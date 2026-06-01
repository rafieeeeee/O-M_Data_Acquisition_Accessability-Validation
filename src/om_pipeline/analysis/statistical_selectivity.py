import pandas as pd
import numpy as np
from scipy import stats

def calculate_environmental_selectivity(active_df: pd.DataFrame, baseline_df: pd.DataFrame, metric: str = 'hs') -> dict:
    """
    Environmental Selectivity Index (ESI):
    Evaluates the statistical difference between active operational windows and the 
    baseline (e.g., 7-day prior) environmental climatology.
    
    Uses Two-Sample Kolmogorov-Smirnov (K-S) test and Mann-Whitney U test.
    """
    if active_df.empty or baseline_df.empty:
        return {
            'metric': metric,
            'ks_stat': np.nan,
            'ks_pvalue': np.nan,
            'mwu_stat': np.nan,
            'mwu_pvalue': np.nan,
            'active_mean': np.nan,
            'baseline_mean': np.nan,
            'delta_mean': np.nan,
            'significant_selection': False
        }
        
    active_data = active_df[metric].dropna()
    baseline_data = baseline_df[metric].dropna()
    
    if len(active_data) == 0 or len(baseline_data) == 0:
        return {
            'metric': metric,
            'ks_stat': np.nan,
            'ks_pvalue': np.nan,
            'mwu_stat': np.nan,
            'mwu_pvalue': np.nan,
            'active_mean': np.nan,
            'baseline_mean': np.nan,
            'delta_mean': np.nan,
            'significant_selection': False
        }

    # K-S Test for distributional differences
    ks_stat, ks_p = stats.ks_2samp(active_data, baseline_data)
    
    # Mann-Whitney U Test for difference in central tendency
    mwu_stat, mwu_p = stats.mannwhitneyu(active_data, baseline_data, alternative='less')
    
    active_mean = active_data.mean()
    baseline_mean = baseline_data.mean()
    delta = active_mean - baseline_mean
    
    # Is the operational window significantly calmer?
    # Alpha = 0.05
    is_significant = (mwu_p < 0.05) and (delta < 0)
    
    return {
        'metric': metric,
        'ks_stat': ks_stat,
        'ks_pvalue': ks_p,
        'mwu_stat': mwu_stat,
        'mwu_pvalue': mwu_p,
        'active_mean': active_mean,
        'baseline_mean': baseline_mean,
        'delta_mean': delta,
        'significant_selection': is_significant
    }
