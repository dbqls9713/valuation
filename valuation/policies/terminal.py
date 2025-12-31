"""
Terminal growth policies.

These policies determine the terminal (perpetual) growth rate used
in the Gordon Growth Model for terminal value calculation.
"""

from abc import ABC
from abc import abstractmethod

from valuation.domain.types import PolicyOutput


class TerminalPolicy(ABC):
  """
  Base class for terminal growth policies.

  Subclasses implement compute() to return a terminal growth rate.
  """

  @abstractmethod
  def compute(self) -> PolicyOutput[float]:
    """
    Compute terminal growth rate.

    Returns:
      PolicyOutput with terminal growth rate and diagnostics
    """

class GordonTerminal(TerminalPolicy):
  """
  Fixed terminal growth rate for Gordon Growth Model.

  Typically set to long-term GDP growth rate or inflation rate.
  """

  def __init__(self, g_terminal: float = 0.03):
    """
    Initialize Gordon terminal policy.

    Args:
      g_terminal: Terminal growth rate (default: 3%)
    """
    self.g_terminal = g_terminal

  def compute(self) -> PolicyOutput[float]:
    """Return fixed terminal growth rate."""
    return PolicyOutput(value=self.g_terminal,
                        diag={
                            'terminal_method': 'gordon',
                            'g_terminal': self.g_terminal,
                        })
