'''
Share change / buyback rate policies.

These policies estimate the annual share reduction rate (buyback rate)
from historical share count data.
'''

from abc import ABC, abstractmethod

import pandas as pd

from valuation.domain.types import FundamentalsSlice, PolicyOutput


class SharePolicy(ABC):
  '''
  Base class for share change policies.

  Subclasses implement compute() to return an annual share reduction rate.
  '''

  @abstractmethod
  def compute(self, data: FundamentalsSlice) -> PolicyOutput[float]:
    '''
    Compute annual share reduction rate (buyback rate).

    Args:
      data: Point-in-time fundamental data slice

    Returns:
      PolicyOutput with buyback rate and diagnostics
      Positive rate means share reduction (buybacks)
      Negative rate means share dilution (issuance)
    '''


class AvgShareChange(SharePolicy):
  '''
  Average share change rate over lookback period.

  Computes CAGR of share reduction over the specified years.
  '''

  def __init__(self, years: int = 5):
    '''
    Initialize average share change policy.

    Args:
      years: Lookback period for averaging (default: 5)
    '''
    self.years = years

  def compute(self, data: FundamentalsSlice) -> PolicyOutput[float]:
    '''Compute CAGR of share change over lookback period.'''
    shares_series = data.shares_history.dropna()

    if shares_series.empty:
      return PolicyOutput(value=0.0,
                          diag={
                              'shares_method': 'avg_change',
                              'error': 'no_shares_data',
                          })

    lookback = data.as_of_end - pd.DateOffset(years=self.years)
    mask = pd.to_datetime(shares_series.index) >= lookback
    recent_shares = shares_series[mask]

    if len(recent_shares) < 2:
      return PolicyOutput(value=0.0,
                          diag={
                              'shares_method': 'avg_change',
                              'error': 'insufficient_shares_history',
                              'quarters_available': len(recent_shares),
                          })

    shares_df = recent_shares.reset_index()
    shares_df.columns = pd.Index(['end', 'shares'])
    shares_df['year'] = pd.to_datetime(shares_df['end']).dt.year

    yearly_shares = shares_df.groupby('year')['shares'].last()

    if len(yearly_shares) < 2:
      return PolicyOutput(value=0.0,
                          diag={
                              'shares_method': 'avg_change',
                              'error': 'insufficient_yearly_data',
                              'years_available': len(yearly_shares),
                          })

    years = sorted(yearly_shares.index)
    sh_old = yearly_shares[years[0]]
    sh_new = yearly_shares[years[-1]]
    years_diff = years[-1] - years[0]

    if years_diff <= 0 or sh_old <= 0 or sh_new <= 0:
      return PolicyOutput(value=0.0,
                          diag={
                              'shares_method': 'avg_change',
                              'error': 'invalid_share_values',
                              'sh_old': float(sh_old),
                              'sh_new': float(sh_new),
                          })

    buyback_rate = 1.0 - (sh_new / sh_old)**(1.0 / years_diff)

    return PolicyOutput(value=buyback_rate,
                        diag={
                            'shares_method': 'avg_change',
                            'lookback_years': self.years,
                            'actual_years': years_diff,
                            'sh_old': float(sh_old),
                            'sh_new': float(sh_new),
                            'first_year': int(years[0]),
                            'last_year': int(years[-1]),
                            'buyback_rate': buyback_rate,
                        })
