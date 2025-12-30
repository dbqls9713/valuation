'''
Pure DCF math engine.

This module contains pure functions for DCF calculations. No pandas, no I/O,
just numeric computations. All inputs must be prepared before calling these.

Key functions:
  compute_intrinsic_value: Main entry point, computes IV per share
  compute_growth_path: Generates yearly growth rates with fade
  compute_pv_explicit: PV of explicit forecast period
  compute_terminal_value: Gordon growth terminal value
'''

from math import isfinite
from typing import List, Tuple


def compute_growth_path(g0: float, g_end: float, n_years: int) -> List[float]:
  '''
  Compute yearly growth rates with linear fade from g0 to g_end.

  Args:
    g0: Initial growth rate
    g_end: Final growth rate at end of explicit period
    n_years: Number of forecast years

  Returns:
    List of n_years growth rates, linearly interpolated from g0 to g_end

  Example:
    >>> compute_growth_path(0.15, 0.04, 5)
    [0.15, 0.1225, 0.095, 0.0675, 0.04]
  '''
  if n_years < 1:
    return []
  if n_years == 1:
    return [g0]

  growth_rates = []
  for t in range(n_years):
    g = g0 + (g_end - g0) * (t / (n_years - 1))
    growth_rates.append(g)
  return growth_rates


def compute_pv_explicit(
    oe0: float,
    sh0: float,
    buyback_rate: float,
    growth_path: List[float],
    discount_rate: float,
) -> Tuple[float, float, float]:
  '''
  Compute present value of explicit forecast period.

  Args:
    oe0: Initial owner earnings (absolute, not per share)
    sh0: Initial shares outstanding
    buyback_rate: Annual share reduction rate (b)
    growth_path: List of yearly growth rates
    discount_rate: Required return (r)

  Returns:
    Tuple of (pv_total, final_oeps, final_shares):
    - pv_total: Total PV of explicit period OE per share
    - final_oeps: OE per share in final year
    - final_shares: Shares outstanding in final year
  '''
  pv = 0.0
  oe = oe0
  shares = sh0

  for t, g in enumerate(growth_path, start=1):
    oe *= (1.0 + g)
    shares *= (1.0 - buyback_rate)

    if shares <= 0:
      return float('nan'), float('nan'), 0.0

    oeps = oe / shares
    pv += oeps / ((1.0 + discount_rate) ** t)

  final_oeps = oe / shares if shares > 0 else 0.0
  return pv, final_oeps, shares


def compute_terminal_value(
    final_oeps: float,
    g_terminal: float,
    discount_rate: float,
    final_year: int,
) -> float:
  '''
  Compute discounted terminal value using Gordon Growth Model.

  Args:
    final_oeps: OE per share in final explicit year
    g_terminal: Terminal (perpetual) growth rate
    discount_rate: Required return (r)
    final_year: Number of years to discount back

  Returns:
    Present value of terminal value

  Raises:
    Returns nan if discount_rate <= g_terminal (model undefined)
  '''
  if discount_rate <= g_terminal:
    return float('nan')

  tv = (final_oeps * (1.0 + g_terminal)) / (discount_rate - g_terminal)
  discounted_tv = tv / ((1.0 + discount_rate) ** final_year)
  return discounted_tv


def compute_intrinsic_value(
    oe0: float,
    sh0: float,
    buyback_rate: float,
    g0: float,
    g_end: float,
    g_terminal: float,
    n_years: int,
    discount_rate: float,
) -> Tuple[float, float, float]:
  '''
  Compute intrinsic value per share using two-stage DCF model.

  Stage 1: Explicit forecast period with fading growth and share buybacks
  Stage 2: Terminal value using Gordon Growth Model

  Args:
    oe0: Initial owner earnings (CFO - CAPEX)
    sh0: Current shares outstanding
    buyback_rate: Annual share reduction rate (b)
    g0: Initial growth rate
    g_end: Growth rate at end of explicit period
    g_terminal: Perpetual terminal growth rate
    n_years: Number of explicit forecast years
    discount_rate: Required return (r)

  Returns:
    Tuple of (iv_per_share, pv_explicit, tv_component):
    - iv_per_share: Total intrinsic value per share
    - pv_explicit: PV contribution from explicit period
    - tv_component: PV contribution from terminal value
  '''
  if not all(isfinite(x) for x in [oe0, sh0, buyback_rate, g0, g_end,
                                    g_terminal, discount_rate]):
    return float('nan'), float('nan'), float('nan')

  if discount_rate <= g_terminal or sh0 <= 0 or n_years < 1:
    return float('nan'), float('nan'), float('nan')

  growth_path = compute_growth_path(g0, g_end, n_years)
  pv_explicit, final_oeps, _ = compute_pv_explicit(
    oe0, sh0, buyback_rate, growth_path, discount_rate)

  if not isfinite(pv_explicit):
    return float('nan'), float('nan'), float('nan')

  tv_component = compute_terminal_value(
    final_oeps, g_terminal, discount_rate, n_years)

  if not isfinite(tv_component):
    return float('nan'), float('nan'), float('nan')

  iv_per_share = pv_explicit + tv_component
  return iv_per_share, pv_explicit, tv_component
