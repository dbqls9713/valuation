'''
Summary metrics for backtest results.

Provides functions to compute aggregate statistics from backtest output.
'''

from typing import Any, Dict

import pandas as pd


def compute_summary_stats(results: pd.DataFrame) -> pd.DataFrame:
  '''
  Compute summary statistics from backtest results.

  Args:
    results: Long-form DataFrame from BacktestRunner

  Returns:
    DataFrame with summary stats per scenario
  '''
  mask = results['iv_per_share'].notna() & (results['iv_per_share'] > 0)
  valid = results[mask]

  if valid.empty:
    return pd.DataFrame({'message': ['No valid results']})

  summary_rows = []

  for scenario in valid['scenario'].unique():
    scenario_data = valid[valid['scenario'] == scenario]

    stats = {
        'scenario': scenario,
        'quarters': len(scenario_data),
        'avg_iv': scenario_data['iv_per_share'].mean(),
        'median_iv': scenario_data['iv_per_share'].median(),
    }

    if 'price_to_iv' in scenario_data.columns:
      ptiv = scenario_data['price_to_iv'].dropna()
      if len(ptiv) > 0:
        stats['avg_price_to_iv'] = ptiv.mean()
        stats['median_price_to_iv'] = ptiv.median()
        stats['min_price_to_iv'] = ptiv.min()
        stats['max_price_to_iv'] = ptiv.max()

    if 'margin_of_safety' in scenario_data.columns:
      mos = scenario_data['margin_of_safety'].dropna()
      if len(mos) > 0:
        stats['avg_mos'] = mos.mean()
        stats['pct_undervalued'] = (mos > 0).sum() / len(mos)

    if 'g0' in scenario_data.columns:
      g0 = scenario_data['g0'].dropna()
      if len(g0) > 0:
        stats['avg_g0'] = g0.mean()

    if 'buyback_rate' in scenario_data.columns:
      bb = scenario_data['buyback_rate'].dropna()
      if len(bb) > 0:
        stats['avg_buyback'] = bb.mean()

    summary_rows.append(stats)

  return pd.DataFrame(summary_rows)


def compute_scenario_comparison(results: pd.DataFrame) -> pd.DataFrame:
  '''
  Compare IVs across scenarios for the same dates.

  Args:
    results: Long-form DataFrame from BacktestRunner

  Returns:
    Wide-form DataFrame with scenarios as columns, dates as rows
  '''
  mask = results['iv_per_share'].notna() & (results['iv_per_share'] > 0)
  valid = results[mask]

  if valid.empty:
    return pd.DataFrame()

  pivot = valid.pivot_table(
      index='as_of_date',
      columns='scenario',
      values='iv_per_share',
      aggfunc='first',
  )

  return pivot


def compute_hit_rate(
    results: pd.DataFrame,
    threshold: float = 0.8,
    scenario: str = 'default',
) -> Dict[str, Any]:
  '''
  Compute "hit rate" - how often market price was below IV by threshold.

  Args:
    results: Long-form DataFrame from BacktestRunner
    threshold: Price/IV threshold (e.g., 0.8 means 20% undervalued)
    scenario: Scenario name to analyze

  Returns:
    Dictionary with hit rate statistics
  '''
  valid = results[(results['scenario'] == scenario) &
                  (results['price_to_iv'].notna())]

  if valid.empty:
    return {'error': 'No valid data'}

  below_threshold = valid[valid['price_to_iv'] < threshold]

  return {
      'scenario': scenario,
      'threshold': threshold,
      'total_quarters': len(valid),
      'below_threshold': len(below_threshold),
      'hit_rate': len(below_threshold) / len(valid),
      'dates_below': below_threshold['as_of_date'].tolist(),
  }
