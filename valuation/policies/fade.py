'''
Growth fade policies.

These policies determine how growth rate transitions from initial (g0)
to terminal (g_end) over the explicit forecast period.

The policy returns a sequence of growth rates [g1, g2, ..., gN] for each
year of the explicit forecast period.
'''

from abc import ABC, abstractmethod
from typing import List

from valuation.domain.types import PolicyOutput


class FadePolicy(ABC):
  '''
  Base class for growth fade policies.

  Subclasses implement compute() to return the full sequence of growth rates
  for the explicit forecast period.
  '''

  @abstractmethod
  def compute(
      self,
      g0: float,
      g_terminal: float,
      n_years: int,
  ) -> PolicyOutput[List[float]]:
    '''
    Compute growth rate sequence for explicit forecast period.

    Args:
      g0: Initial growth rate
      g_terminal: Terminal (perpetual) growth rate
      n_years: Number of explicit forecast years

    Returns:
      PolicyOutput with list of growth rates [g_year1, g_year2, ..., g_yearN]
    '''


class LinearFade(FadePolicy):
  '''
  Linear fade from g0 to g_end.

  Growth rates interpolate linearly from g0 (year 1) to g_end (year N).
  g_end is calculated as g_terminal + g_end_spread.
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
  ) -> PolicyOutput[List[float]]:
    '''Compute linearly fading growth rates.'''
    g_end = g_terminal + self.g_end_spread

    if n_years < 1:
      return PolicyOutput(value=[], diag={'fade_method': 'linear'})

    if n_years == 1:
      return PolicyOutput(value=[g0],
                          diag={
                              'fade_method': 'linear',
                              'g_end_spread': self.g_end_spread,
                              'g_end': g_end,
                          })

    growth_rates = []
    for t in range(n_years):
      g = g0 + (g_end - g0) * (t / (n_years - 1))
      growth_rates.append(g)

    return PolicyOutput(value=growth_rates,
                        diag={
                            'fade_method': 'linear',
                            'g_end_spread': self.g_end_spread,
                            'g_end': g_end,
                            'g_terminal': g_terminal,
                        })


class GeometricFade(FadePolicy):
  '''
  Geometric (exponential) fade from g0 to g_end.

  Growth rates decay geometrically, which produces a smoother transition
  that front-loads the decline (faster initial drop, slower later).
  '''

  def __init__(self, g_end_spread: float = 0.01):
    '''
    Initialize geometric fade policy.

    Args:
      g_end_spread: Spread above terminal rate for g_end (default: 1%)
    '''
    self.g_end_spread = g_end_spread

  def compute(
      self,
      g0: float,
      g_terminal: float,
      n_years: int,
  ) -> PolicyOutput[List[float]]:
    '''Compute geometrically fading growth rates.'''
    g_end = g_terminal + self.g_end_spread

    if n_years < 1:
      return PolicyOutput(value=[], diag={'fade_method': 'geometric'})

    if n_years == 1:
      return PolicyOutput(value=[g0],
                          diag={
                              'fade_method': 'geometric',
                              'g_end_spread': self.g_end_spread,
                              'g_end': g_end,
                          })

    if g0 <= 0 or g_end <= 0:
      return LinearFade(self.g_end_spread).compute(g0, g_terminal, n_years)

    ratio = (g_end / g0)**(1.0 / (n_years - 1))
    growth_rates = [g0 * (ratio**t) for t in range(n_years)]

    return PolicyOutput(value=growth_rates,
                        diag={
                            'fade_method': 'geometric',
                            'g_end_spread': self.g_end_spread,
                            'g_end': g_end,
                            'g_terminal': g_terminal,
                            'decay_ratio': ratio,
                        })


class StepThenFade(FadePolicy):
  '''
  Step fade: maintain g0 for initial years, then linearly fade.

  Growth stays at g0 for high_growth_years, then linearly fades
  to g_end in the remaining years.
  '''

  def __init__(self, high_growth_years: int = 5, g_end_spread: float = 0.01):
    '''
    Initialize step-then-fade policy.

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
  ) -> PolicyOutput[List[float]]:
    '''Compute step-then-fade growth rates.'''
    g_end = g_terminal + self.g_end_spread
    hg_years = min(self.high_growth_years, n_years)

    if n_years < 1:
      return PolicyOutput(value=[], diag={'fade_method': 'step_then_fade'})

    growth_rates = [g0] * hg_years

    fade_years = n_years - hg_years
    if fade_years > 0:
      for t in range(1, fade_years + 1):
        g = g0 + (g_end - g0) * (t / fade_years)
        growth_rates.append(g)

    return PolicyOutput(value=growth_rates,
                        diag={
                            'fade_method': 'step_then_fade',
                            'high_growth_years': hg_years,
                            'fade_years': fade_years,
                            'g_end_spread': self.g_end_spread,
                            'g_end': g_end,
                            'g_terminal': g_terminal,
                        })


# Backwards compatibility alias
StepFade = StepThenFade
