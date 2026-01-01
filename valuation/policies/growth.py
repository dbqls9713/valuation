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
