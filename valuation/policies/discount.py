"""
Discount rate policies.

These policies determine the required rate of return (discount rate)
used in DCF valuation.
"""

from abc import ABC
from abc import abstractmethod

from valuation.domain.types import PolicyOutput


class DiscountPolicy(ABC):
  """
  Base class for discount rate policies.

  Subclasses implement compute() to return a discount rate.
  """

  @abstractmethod
  def compute(self) -> PolicyOutput[float]:
    """
    Compute discount rate.

    Returns:
      PolicyOutput with discount rate and diagnostics
    """

class FixedRate(DiscountPolicy):
  """
  Fixed discount rate.

  Simple policy that returns a constant required return.
  """

  def __init__(self, rate: float = 0.10):
    """
    Initialize fixed rate policy.

    Args:
      rate: Fixed discount rate (default: 10%)
    """
    self.rate = rate

  def compute(self) -> PolicyOutput[float]:
    """Return fixed discount rate."""
    return PolicyOutput(
      value=self.rate,
      diag={
        'discount_method': 'fixed',
        'discount_rate': self.rate,
      }
    )
