'''
Growth rate estimation policies.

These policies estimate the initial growth rate (g0) for the DCF model
based on historical Owner Earnings per share growth.
'''

from abc import ABC, abstractmethod
from typing import Optional, Tuple

import pandas as pd

from valuation.domain.types import FundamentalsSlice, PolicyOutput
from valuation.policies.capex import CapexPolicy, WeightedAverageCapex


class GrowthPolicy(ABC):
  '''
  Base class for growth rate estimation policies.

  Subclasses implement compute() to return an initial growth rate.
  '''

  @abstractmethod
  def compute(
      self,
      data: FundamentalsSlice,
      capex_policy: Optional[CapexPolicy] = None,
  ) -> PolicyOutput[float]:
    '''
    Compute initial growth rate for DCF model.

    Args:
      data: Point-in-time fundamental data slice
      capex_policy: Optional CAPEX policy for OE per share calculation

    Returns:
      PolicyOutput with growth rate and diagnostics
    '''


class CAGRGrowth(GrowthPolicy):
  '''
  CAGR-based growth rate with threshold and clipping.

  Calculates Compound Annual Growth Rate of OE per share over a lookback
  period, applies a minimum threshold, and clips to a maximum.
  '''

  def __init__(
      self,
      min_years: int = 3,
      threshold: float = 0.0,
      clip_min: float = 0.0,
      clip_max: float = 0.18,
  ):
    '''
    Initialize CAGR growth policy.

    Args:
      min_years: Minimum years of data required (default: 3)
      threshold: Minimum growth rate to proceed (default: 4%)
      clip_min: Minimum clipped growth rate (default: 0%)
      clip_max: Maximum clipped growth rate (default: 18%)
    '''
    self.min_years = min_years
    self.threshold = threshold
    self.clip_min = clip_min
    self.clip_max = clip_max

  def compute(
      self,
      data: FundamentalsSlice,
      capex_policy: Optional[CapexPolicy] = None,
  ) -> PolicyOutput[float]:
    '''
    Compute CAGR of OE per share with threshold and clipping.

    Growth is computed using CAPEX calculated by the provided policy
    (or WeightedAverageCapex by default) for consistency with OE0.
    '''
    if capex_policy is None:
      capex_policy = WeightedAverageCapex()

    oeps_history, oeps_diag = self._compute_oeps_history(data, capex_policy)

    if oeps_history.empty or len(oeps_history) < 2:
      return PolicyOutput(value=float('nan'),
                          diag={
                              'growth_method': 'cagr',
                              'error': 'insufficient_data',
                              'available_years': len(oeps_history),
                              **oeps_diag,
                          })

    oeps_df = oeps_history.reset_index()
    oeps_df.columns = pd.Index(['end', 'oeps'])
    oeps_df['year'] = pd.to_datetime(oeps_df['end']).dt.year

    annual_oeps = oeps_df.groupby('year')['oeps'].last()

    if len(annual_oeps) < 2:
      return PolicyOutput(value=float('nan'),
                          diag={
                              'growth_method': 'cagr',
                              'error': 'insufficient_years',
                              'years_available': len(annual_oeps),
                              **oeps_diag,
                          })

    years = sorted(annual_oeps.index)
    first_oeps = annual_oeps[years[0]]
    last_oeps = annual_oeps[years[-1]]
    num_years = len(years) - 1

    if first_oeps <= 0:
      return PolicyOutput(value=float('nan'),
                          diag={
                              'growth_method': 'cagr',
                              'error': 'negative_first_oeps',
                              'first_oeps': first_oeps,
                              **oeps_diag,
                          })

    if last_oeps <= 0:
      return PolicyOutput(value=float('nan'),
                          diag={
                              'growth_method': 'cagr',
                              'error': 'negative_last_oeps',
                              'first_oeps': first_oeps,
                              'last_oeps': last_oeps,
                              **oeps_diag,
                          })

    raw_cagr = (last_oeps / first_oeps)**(1 / num_years) - 1
    clipped_cagr = max(self.clip_min, min(self.clip_max, raw_cagr))
    below_threshold = clipped_cagr <= self.threshold

    return PolicyOutput(value=clipped_cagr,
                        diag={
                            'growth_method': 'cagr',
                            'raw_cagr': raw_cagr,
                            'clipped_cagr': clipped_cagr,
                            'clip_range': (self.clip_min, self.clip_max),
                            'threshold': self.threshold,
                            'below_threshold': below_threshold,
                            'first_year': int(years[0]),
                            'last_year': int(years[-1]),
                            'first_oeps': float(first_oeps),
                            'last_oeps': float(last_oeps),
                            'num_years': num_years,
                            **oeps_diag,
                        })

  def _compute_oeps_history(
      self,
      data: FundamentalsSlice,
      capex_policy: CapexPolicy,  # pylint: disable=unused-argument
  ) -> Tuple[pd.Series, dict]:
    '''
    Compute OE per share history using the CAPEX policy.

    For consistency, uses the same CAPEX calculation method for
    historical OE as will be used for OE0.
    '''
    cfo_series = data.cfo_ttm_history.dropna()
    capex_series = data.capex_ttm_history.dropna()
    shares_series = data.shares_history.dropna()

    common_idx = cfo_series.index.intersection(capex_series.index).intersection(
        shares_series.index)

    if len(common_idx) < 4:
      return pd.Series(dtype=float), {'oeps_error': 'insufficient_common_data'}

    cfo_aligned = cfo_series.loc[common_idx]
    capex_aligned = capex_series.loc[common_idx].abs()
    shares_aligned = shares_series.loc[common_idx]

    as_of = data.as_of_end
    lookback = as_of - pd.DateOffset(years=self.min_years)
    mask = pd.to_datetime(common_idx) >= lookback

    recent_idx = common_idx[mask]
    if len(recent_idx) < 4:
      return pd.Series(dtype=float), {
          'oeps_error': 'insufficient_recent_data',
          'recent_quarters': len(recent_idx),
      }

    oeps_list = []
    oeps_idx = []

    df_temp = pd.DataFrame({
        'cfo': cfo_aligned.loc[recent_idx],
        'capex': capex_aligned.loc[recent_idx],
        'shares': shares_aligned.loc[recent_idx],
    })
    df_temp['year'] = pd.to_datetime(df_temp.index).year

    for idx in recent_idx:
      row_year = pd.to_datetime(idx).year
      years_to_use = [row_year - 2, row_year - 1, row_year]

      yearly_capex = []
      for y in years_to_use:
        year_data = df_temp[df_temp['year'] == y]
        if not year_data.empty:
          yearly_capex.append(year_data['capex'].iloc[-1])

      if len(yearly_capex) >= 2:
        n = len(yearly_capex)
        weights = list(range(1, n + 1))
        weighted_capex = sum(
            c * w for c, w in zip(yearly_capex, weights)) / sum(weights)
      else:
        weighted_capex = df_temp.loc[idx, 'capex']

      oe = df_temp.loc[idx, 'cfo'] - weighted_capex
      shares = df_temp.loc[idx, 'shares']

      if shares > 0:
        oeps_list.append(oe / shares)
        oeps_idx.append(idx)

    return pd.Series(oeps_list, index=oeps_idx), {
        'oeps_quarters': len(oeps_list)
    }
