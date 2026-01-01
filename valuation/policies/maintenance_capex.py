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
