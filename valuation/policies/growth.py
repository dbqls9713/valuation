"""
Growth rate estimation policies.

These policies estimate the initial growth rate (g0) for the DCF model.
"""

from abc import ABC
from abc import abstractmethod

from valuation.domain.types import FundamentalsSlice
from valuation.domain.types import PolicyOutput


class GrowthPolicy(ABC):
  """Base class for growth rate estimation policies."""

  @abstractmethod
  def compute(self, data: FundamentalsSlice) -> PolicyOutput[float]:
    """
    Compute initial growth rate for DCF model.

    Args:
      data: Point-in-time fundamental data slice

    Returns:
      PolicyOutput with growth rate and diagnostics
    """


class FixedGrowth(GrowthPolicy):
  """Fixed growth rate for the DCF model."""

  def __init__(self, growth_rate: float):
    self.growth_rate = growth_rate

  def compute(self, data: FundamentalsSlice) -> PolicyOutput[float]:
    return PolicyOutput(value=self.growth_rate,
                        diag={
                            'growth_method': 'fixed',
                            'growth_rate': self.growth_rate,
                        })


class AvgOEGrowth(GrowthPolicy):
  """
  Average Owner Earnings growth rate over 3 years.

  Buckets quarters by years ago from as_of_date:
    - Year 1: 0 ~ 1.25 years ago
    - Year 3: 2.25 ~ 3.25 years ago

  Calculates average OE for each year bucket, then computes CAGR.
  Result is clipped to min/max bounds.
  """

  def __init__(self, min_growth: float = 0.0, max_growth: float = 0.20):
    self.min_growth = min_growth
    self.max_growth = max_growth

  def compute(self, data: FundamentalsSlice) -> PolicyOutput[float]:
    year_buckets: dict[int, list[float]] = {1: [], 3: []}

    for q in data.quarters:
      if q.cfo_ttm is None or q.capex_ttm is None:
        continue

      oe = q.cfo_ttm - abs(q.capex_ttm)
      years_ago = (data.as_of_date - q.end).days / 365.25

      if years_ago < 1.25:
        year_buckets[1].append(oe)
      elif 2.25 <= years_ago < 3.25:
        year_buckets[3].append(oe)

    if not year_buckets[1] or not year_buckets[3]:
      return PolicyOutput(value=float('nan'),
                          diag={
                              'growth_method': 'avg_oe_3y',
                              'error': 'insufficient_data',
                              'year1_n': len(year_buckets[1]),
                              'year3_n': len(year_buckets[3]),
                          })

    oe_new = sum(year_buckets[1]) / len(year_buckets[1])
    oe_old = sum(year_buckets[3]) / len(year_buckets[3])

    if oe_old <= 0 or oe_new <= 0:
      return PolicyOutput(value=float('nan'),
                          diag={
                              'growth_method': 'avg_oe_3y',
                              'error': 'non_positive_oe',
                              'oe_old': oe_old,
                              'oe_new': oe_new,
                          })

    cagr = (oe_new / oe_old)**(1 / 3) - 1
    clipped = max(self.min_growth, min(self.max_growth, cagr))

    return PolicyOutput(value=clipped,
                        diag={
                            'growth_method': 'avg_oe_3y',
                            'oe_old': oe_old,
                            'oe_new': oe_new,
                            'year1_n': len(year_buckets[1]),
                            'year3_n': len(year_buckets[3]),
                            'raw_cagr': cagr,
                            'clipped_growth': clipped,
                            'min_growth': self.min_growth,
                            'max_growth': self.max_growth,
                        })
