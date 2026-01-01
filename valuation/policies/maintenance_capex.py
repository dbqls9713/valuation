"""
Maintenance CAPEX estimation policies.

Maintenance CAPEX represents the capital expenditures required to maintain
current operations. Since true maintenance CAPEX is not directly reported,
total CAPEX is used as an approximation.

Final Owner Earnings = Pre-Maintenance OE - Maintenance CAPEX
"""

from abc import ABC
from abc import abstractmethod

from valuation.domain.types import FundamentalsSlice
from valuation.domain.types import PolicyOutput


class MaintenanceCapexPolicy(ABC):
  """
  Base class for Maintenance CAPEX estimation policies.

  Maintenance CAPEX is the capital spending required to maintain existing
  operations. Since this is not reported separately, we approximate it
  using total CAPEX with various adjustment methods.
  """

  @abstractmethod
  def compute(self, data: FundamentalsSlice) -> PolicyOutput[float]:
    """
    Compute Maintenance CAPEX estimate.

    Args:
      data: Point-in-time fundamental data slice

    Returns:
      PolicyOutput with maintenance CAPEX value and diagnostics
    """


class TTMCapex(MaintenanceCapexPolicy):
  """
  Use raw TTM CAPEX without adjustment.

  This is the simplest method but can be volatile if CAPEX varies
  significantly year-to-year.
  """

  def compute(self, data: FundamentalsSlice) -> PolicyOutput[float]:
    """Return latest TTM CAPEX as-is."""
    capex = abs(data.latest_capex_ttm)

    return PolicyOutput(value=capex,
                        diag={
                            'maint_capex_method': 'ttm',
                            'capex_ttm': data.latest_capex_ttm,
                            'maint_capex': capex,
                        })


class AvgCapex(MaintenanceCapexPolicy):
  """
  Weighted average CAPEX over 3 years with 3:2:1 weights.

  Buckets quarters by years ago from as_of_date:
    - Year 1: 0 ~ 1.25 years ago (weight 3)
    - Year 2: 1.25 ~ 2.25 years ago (weight 2)
    - Year 3: 2.25 ~ 3.25 years ago (weight 1)

  Falls back to TTM if insufficient data.
  """

  def compute(self, data: FundamentalsSlice) -> PolicyOutput[float]:
    """Return weighted 3-year average of TTM CAPEX."""
    weighted_avg, diag = data.weighted_yearly_avg('capex_ttm')

    if weighted_avg is None:
      avg_capex = abs(data.latest_capex_ttm)
      return PolicyOutput(value=avg_capex,
                          diag={
                              'maint_capex_method': 'weighted_avg_3y',
                              'fallback': 'ttm',
                              'maint_capex': avg_capex,
                              **diag,
                          })

    avg_capex = abs(weighted_avg)

    return PolicyOutput(value=avg_capex,
                        diag={
                            'maint_capex_method': 'weighted_avg_3y',
                            'maint_capex': avg_capex,
                            **diag,
                        })
