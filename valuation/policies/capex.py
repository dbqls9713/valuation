'''
CAPEX estimation policies.

These policies determine how CAPEX is calculated for Owner Earnings.
Different methods handle volatile CAPEX differently.
'''

from abc import ABC, abstractmethod
from typing import Optional

import numpy as np
import pandas as pd

from valuation.domain.types import FundamentalsSlice, PolicyOutput


class CapexPolicy(ABC):
  '''
  Base class for CAPEX estimation policies.

  Subclasses implement compute() to return a CAPEX value for OE calculation.
  '''

  @abstractmethod
  def compute(self, data: FundamentalsSlice) -> PolicyOutput[float]:
    '''
    Compute CAPEX value for Owner Earnings calculation.

    Args:
      data: Point-in-time fundamental data slice

    Returns:
      PolicyOutput with CAPEX value and diagnostics
    '''


class RawTTMCapex(CapexPolicy):
  '''
  Use raw TTM CAPEX without adjustment.

  This is the simplest method but can be volatile if CAPEX varies
  significantly year-to-year.
  '''

  def compute(self, data: FundamentalsSlice) -> PolicyOutput[float]:
    '''Return latest TTM CAPEX as-is.'''
    return PolicyOutput(value=abs(data.latest_capex_ttm),
                        diag={
                            'capex_method': 'raw_ttm',
                            'capex_raw_ttm': data.latest_capex_ttm,
                        })


class WeightedAverageCapex(CapexPolicy):
  '''
  3-year weighted average CAPEX with linear weights (1:2:3).

  Most recent year gets highest weight, smoothing out CAPEX volatility
  while still reflecting recent trends.
  '''

  def __init__(self, years: int = 3, weights: Optional[list] = None):
    '''
    Initialize weighted average policy.

    Args:
      years: Number of years to average (default: 3)
      weights: Custom weights (default: [1, 2, 3] for 3 years)
    '''
    self.years = years
    self.weights = weights

  def compute(self, data: FundamentalsSlice) -> PolicyOutput[float]:
    '''Compute weighted average of yearly CAPEX values.'''
    capex_series = data.capex_ttm_history.dropna()
    if capex_series.empty:
      return PolicyOutput(value=float('nan'),
                          diag={
                              'capex_method': 'weighted_avg',
                              'error': 'no_data'
                          })

    capex_df = capex_series.reset_index()
    capex_df.columns = pd.Index(['end', 'capex_ttm'])
    capex_df['year'] = pd.to_datetime(capex_df['end']).dt.year

    yearly_capex = capex_df.groupby('year')['capex_ttm'].last()

    if len(yearly_capex) < 2:
      raw_capex = abs(data.latest_capex_ttm)
      return PolicyOutput(value=raw_capex,
                          diag={
                              'capex_method': 'weighted_avg_fallback',
                              'reason': 'insufficient_years',
                              'available_years': len(yearly_capex),
                              'capex_used': raw_capex,
                          })

    capex_years = yearly_capex.tail(self.years)
    n = len(capex_years)

    if self.weights:
      weights = self.weights[-n:]
    else:
      weights = list(range(1, n + 1))

    weighted_capex = sum(
        abs(float(v)) * w
        for v, w in zip(capex_years.values, weights)) / sum(weights)

    return PolicyOutput(value=weighted_capex,
                        diag={
                            'capex_method': 'weighted_avg',
                            'years_used': n,
                            'weights': weights,
                            'yearly_values': {
                                int(y): float(v)
                                for y, v in capex_years.items()
                                if isinstance(y, (int, str))
                            },
                            'capex_raw_ttm': data.latest_capex_ttm,
                            'capex_weighted': weighted_capex,
                        })


class IntensityClippedCapex(CapexPolicy):
  '''
  CAPEX intensity clipping based on historical CAPEX/CFO ratio.

  If current CAPEX/CFO ratio exceeds historical percentile, the excess
  is reduced by a factor (default: halved).
  '''

  def __init__(
      self,
      percentile: float = 90,
      reduction_factor: float = 0.5,
      lookback_quarters: int = 20,
  ):
    '''
    Initialize intensity clipping policy.

    Args:
      percentile: Historical percentile threshold (default: 90th)
      reduction_factor: How much to reduce excess (default: 0.5 = half)
      lookback_quarters: Quarters to consider for percentile (default: 20)
    '''
    self.percentile = percentile
    self.reduction_factor = reduction_factor
    self.lookback_quarters = lookback_quarters

  def compute(self, data: FundamentalsSlice) -> PolicyOutput[float]:
    '''Compute CAPEX with intensity clipping.'''
    capex_series = data.capex_ttm_history.dropna()
    cfo_series = data.cfo_ttm_history.dropna()

    if capex_series.empty or cfo_series.empty:
      return PolicyOutput(value=float('nan'),
                          diag={
                              'capex_method': 'intensity_clipped',
                              'error': 'no_data'
                          })

    common_idx = capex_series.index.intersection(cfo_series.index)
    if len(common_idx) < 5:
      raw_capex = abs(data.latest_capex_ttm)
      return PolicyOutput(value=raw_capex,
                          diag={
                              'capex_method': 'intensity_clipped_fallback',
                              'reason': 'insufficient_history',
                              'common_quarters': len(common_idx),
                              'capex_used': raw_capex,
                          })

    capex_aligned = capex_series.loc[common_idx].abs()
    cfo_aligned = cfo_series.loc[common_idx]

    cfo_aligned = cfo_aligned.replace(0, np.nan)
    intensity = capex_aligned / cfo_aligned
    intensity = intensity.replace([np.inf, -np.inf], np.nan).dropna()

    if len(intensity) < 5:
      raw_capex = abs(data.latest_capex_ttm)
      return PolicyOutput(value=raw_capex,
                          diag={
                              'capex_method': 'intensity_clipped_fallback',
                              'reason': 'insufficient_intensity_data',
                              'capex_used': raw_capex,
                          })

    lookback = intensity.tail(self.lookback_quarters)
    threshold = lookback.quantile(self.percentile / 100)

    current_cfo = data.latest_cfo_ttm
    current_capex = abs(data.latest_capex_ttm)

    if current_cfo <= 0:
      return PolicyOutput(value=current_capex,
                          diag={
                              'capex_method': 'intensity_clipped_skip',
                              'reason': 'negative_cfo',
                              'capex_used': current_capex,
                          })

    current_intensity = current_capex / current_cfo
    clipping_applied = False
    clipped_capex = current_capex

    if current_intensity > threshold:
      excess_intensity = current_intensity - threshold
      excess = excess_intensity * self.reduction_factor
      adjusted_intensity = threshold + excess
      clipped_capex = adjusted_intensity * current_cfo
      clipping_applied = True

    return PolicyOutput(value=clipped_capex,
                        diag={
                            'capex_method': 'intensity_clipped',
                            'percentile_threshold': self.percentile,
                            'intensity_threshold': float(threshold),
                            'current_intensity': current_intensity,
                            'clipping_applied': clipping_applied,
                            'capex_raw': current_capex,
                            'capex_clipped': clipped_capex,
                        })
