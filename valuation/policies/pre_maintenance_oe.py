"""
Pre-Maintenance Owner Earnings estimation policies.

Pre-Maintenance OE represents the cash earnings before deducting maintenance
capital expenditures. CFO (Cash Flow from Operations) is used as an
approximation of this value.

Final Owner Earnings = Pre-Maintenance OE - Maintenance CAPEX
"""

from abc import ABC
from abc import abstractmethod

from valuation.domain.types import FundamentalsSlice
from valuation.domain.types import PolicyOutput


class PreMaintenanceOEPolicy(ABC):
  """
  Base class for Pre-Maintenance Owner Earnings policies.

  Pre-Maintenance OE is the cash earnings before subtracting maintenance
  capital requirements. CFO is the most common approximation.
  """

  @abstractmethod
  def compute(self, data: FundamentalsSlice) -> PolicyOutput[float]:
    """
    Compute Pre-Maintenance Owner Earnings.

    Args:
      data: Point-in-time fundamental data slice

    Returns:
      PolicyOutput with pre-maintenance OE value and diagnostics
    """


class TTMPreMaintenanceOE(PreMaintenanceOEPolicy):
  """
  Standard TTM-based Pre-Maintenance Owner Earnings.

  Uses CFO_TTM (Trailing Twelve Months Cash Flow from Operations).
  """

  def compute(self, data: FundamentalsSlice) -> PolicyOutput[float]:
    """Return TTM CFO as pre-maintenance OE."""
    cfo_ttm = data.latest_cfo_ttm

    return PolicyOutput(value=cfo_ttm,
                        diag={
                            'pre_maint_oe_method': 'ttm',
                            'cfo_ttm': cfo_ttm,
                        })
