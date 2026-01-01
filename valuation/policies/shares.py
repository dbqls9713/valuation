"""
Share change / buyback rate policies.

These policies estimate the annual share reduction rate (buyback rate)
from historical share count data.
"""

from abc import ABC
from abc import abstractmethod

import pandas as pd

from valuation.domain.types import FundamentalsSlice
from valuation.domain.types import PolicyOutput


class SharePolicy(ABC):
  """
  Base class for share change policies.

  Subclasses implement compute() to return an annual share reduction rate.
  """

  @abstractmethod
  def compute(self, data: FundamentalsSlice) -> PolicyOutput[float]:
    """
    Compute annual share reduction rate (buyback rate).

    Args:
      data: Point-in-time fundamental data slice

    Returns:
      PolicyOutput with buyback rate and diagnostics
      Positive rate means share reduction (buybacks)
      Negative rate means share dilution (issuance)
    """

class AvgShareChange(SharePolicy):
  """
  Average share change rate over lookback period.

  Computes CAGR of share reduction over the specified years.
  """

  def __init__(self, years: int = 5):
    """
    Initialize average share change policy.

    Args:
      years: Lookback period for averaging (default: 5)
    """
    self.years = years

  def compute(self, data: FundamentalsSlice) -> PolicyOutput[float]:
    """Compute CAGR of share change over lookback period."""
    if not data.quarters:
      return PolicyOutput(
          value=0.0, diag={
              'shares_method': 'avg_change',
              'error': 'no_shares_data',
          })

    lookback_date = data.as_of_date - pd.DateOffset(years=self.years)

    yearly_shares: dict[int, float] = {}
    for q in data.quarters:
      if q.shares is None:
        continue
      if q.end < lookback_date:
        continue
      year = q.fiscal_year
      yearly_shares[year] = q.shares

    if len(yearly_shares) < 2:
      return PolicyOutput(
          value=0.0,
          diag={
              'shares_method': 'avg_change',
              'error': 'insufficient_yearly_data',
              'years_available': len(yearly_shares),
          })

    years_list = sorted(yearly_shares.keys())
    sh_old = yearly_shares[years_list[0]]
    sh_new = yearly_shares[years_list[-1]]
    years_diff = years_list[-1] - years_list[0]

    if years_diff <= 0 or sh_old <= 0 or sh_new <= 0:
      return PolicyOutput(
          value=0.0,
          diag={
              'shares_method': 'avg_change',
              'error': 'invalid_share_values',
              'sh_old': sh_old,
              'sh_new': sh_new,
          })

    buyback_rate = 1.0 - (sh_new / sh_old)**(1.0 / years_diff)

    return PolicyOutput(
        value=buyback_rate,
        diag={
            'shares_method': 'avg_change',
            'lookback_years': self.years,
            'actual_years': years_diff,
            'sh_old': sh_old,
            'sh_new': sh_new,
            'first_year': years_list[0],
            'last_year': years_list[-1],
            'buyback_rate': buyback_rate,
        })
