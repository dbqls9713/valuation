'''
Growth fade policies.

These policies determine how growth rate transitions from initial (g0)
to terminal (g_terminal) over the explicit forecast period.
'''

from abc import ABC, abstractmethod

from valuation.domain.types import PolicyOutput


class FadePolicy(ABC):
  '''
  Base class for growth fade policies.

  Subclasses implement compute() to return the ending growth rate (g_end)
  for the explicit forecast period.
  '''

  @abstractmethod
  def compute(
      self,
      g0: float,
      g_terminal: float,
      n_years: int,
  ) -> PolicyOutput[float]:
    '''
    Compute ending growth rate for explicit period.

    Args:
      g0: Initial growth rate
      g_terminal: Terminal (perpetual) growth rate
      n_years: Number of explicit forecast years

    Returns:
      PolicyOutput with g_end value and diagnostics
    '''


class LinearFade(FadePolicy):
  '''
  Linear fade from g0 to g_end.

  g_end is set to g_terminal + spread, and growth rates interpolate
  linearly from g0 to g_end over the forecast period.
  '''

  def __init__(self, g_end_spread: float = 0.01):
    '''
    Initialize linear fade policy.

    Args:
      g_end_spread: Spread above terminal rate for g_end (default: 1%)
    '''
    self.g_end_spread = g_end_spread

  def compute(
      self,
      g0: float,
      g_terminal: float,
      n_years: int,
  ) -> PolicyOutput[float]:
    '''Compute g_end as g_terminal + spread.'''
    g_end = g_terminal + self.g_end_spread

    return PolicyOutput(value=g_end,
                        diag={
                            'fade_method': 'linear',
                            'g_end_spread': self.g_end_spread,
                            'g_end': g_end,
                            'g_terminal': g_terminal,
                        })


class StepFade(FadePolicy):
  '''
  Step fade: maintain g0 for initial years, then fade.

  Growth stays at g0 for high_growth_years, then linearly fades
  to g_end in the remaining years.
  '''

  def __init__(self, high_growth_years: int = 5, g_end_spread: float = 0.01):
    '''
    Initialize step fade policy.

    Args:
      high_growth_years: Years to keep initial growth (default: 5)
      g_end_spread: Spread above terminal (default: 1%)
    '''
    self.high_growth_years = high_growth_years
    self.g_end_spread = g_end_spread

  def compute(
      self,
      g0: float,
      g_terminal: float,
      n_years: int,
  ) -> PolicyOutput[float]:
    '''Compute g_end with step fade consideration.'''
    g_end = g_terminal + self.g_end_spread

    return PolicyOutput(
        value=g_end,
        diag={
            'fade_method': 'step',
            'high_growth_years': min(self.high_growth_years, n_years),
            'g_end_spread': self.g_end_spread,
            'g_end': g_end,
            'g_terminal': g_terminal,
        })
