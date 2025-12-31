"""
Policy registry for mapping string names to policy factories.

This enables scenarios to be configured with string names (YAML/JSON friendly)
while still instantiating the correct policy classes.

To add a new policy:
1. Implement the policy class in the appropriate module
   (e.g., policies/capex.py)
2. Add a factory function here that creates the policy instance
3. Register it in the appropriate registry dictionary

Example:
  # In policies/capex.py
  class MyNewCapex(CapexPolicy):
    def compute(self, data: FundamentalsSlice) -> PolicyOutput[float]:
      ...

  # In scenarios/registry.py
  def _my_new_capex():
    return MyNewCapex(param1=value1)

  CAPEX_POLICIES['my_new_capex'] = _my_new_capex
"""

from collections.abc import Callable
from typing import Any, cast

from valuation.policies.capex import CapexPolicy
from valuation.policies.capex import IntensityClippedCapex
from valuation.policies.capex import RawTTMCapex
from valuation.policies.capex import WeightedAverageCapex
from valuation.policies.discount import DiscountPolicy
from valuation.policies.discount import FixedRate
from valuation.policies.fade import FadePolicy
from valuation.policies.fade import GeometricFade
from valuation.policies.fade import LinearFade
from valuation.policies.fade import StepThenFade
from valuation.policies.growth import CAGRGrowth
from valuation.policies.growth import GrowthPolicy
from valuation.policies.shares import AvgShareChange
from valuation.policies.shares import SharePolicy
from valuation.policies.terminal import GordonTerminal
from valuation.policies.terminal import TerminalPolicy
from valuation.scenarios.config import ScenarioConfig

CAPEX_POLICIES: dict[str, Callable[[], CapexPolicy]] = {
    'raw_ttm':
        RawTTMCapex,
    'weighted_3y_123':
        lambda: WeightedAverageCapex(years=3, weights=[1, 2, 3]),
    'weighted_3y':
        lambda: WeightedAverageCapex(years=3),
    'intensity_clipped':
        lambda: IntensityClippedCapex(percentile=90, reduction_factor=0.5),
    'intensity_clipped_p80':
        lambda: IntensityClippedCapex(percentile=80, reduction_factor=0.5),
}

GROWTH_POLICIES: dict[str, Callable[[], GrowthPolicy]] = {
    'cagr_3y_clip':
        lambda: CAGRGrowth(
            min_years=3, threshold=0.0, clip_min=0.0, clip_max=0.18),
    'cagr_3y_clip_25':
        lambda: CAGRGrowth(
            min_years=3, threshold=0.0, clip_min=0.0, clip_max=0.25),
    'cagr_3y_no_clip':
        lambda: CAGRGrowth(
            min_years=3, threshold=0.0, clip_min=-1.0, clip_max=1.0),
    'cagr_5y_clip':
        lambda: CAGRGrowth(
            min_years=5, threshold=0.0, clip_min=0.0, clip_max=0.18),
}

FADE_POLICIES: dict[str, Callable[[], FadePolicy]] = {
    'linear': lambda: LinearFade(g_end_spread=0.01),
    'linear_0p02': lambda: LinearFade(g_end_spread=0.02),
    'geometric': lambda: GeometricFade(g_end_spread=0.01),
    'step_5y': lambda: StepThenFade(high_growth_years=5, g_end_spread=0.01),
    'step_3y': lambda: StepThenFade(high_growth_years=3, g_end_spread=0.01),
}

SHARE_POLICIES: dict[str, Callable[[], SharePolicy]] = {
    'avg_5y': lambda: AvgShareChange(years=5),
    'avg_3y': lambda: AvgShareChange(years=3),
    'avg_10y': lambda: AvgShareChange(years=10),
}

TERMINAL_POLICIES: dict[str, Callable[[], TerminalPolicy]] = {
    'gordon': lambda: GordonTerminal(g_terminal=0.03),
    'gordon_2pct': lambda: GordonTerminal(g_terminal=0.02),
    'gordon_4pct': lambda: GordonTerminal(g_terminal=0.04),
}

DISCOUNT_POLICIES: dict[str, Callable[[], DiscountPolicy]] = {
    'fixed_0p06': lambda: FixedRate(rate=0.06),
    'fixed_0p07': lambda: FixedRate(rate=0.07),
    'fixed_0p08': lambda: FixedRate(rate=0.08),
    'fixed_0p09': lambda: FixedRate(rate=0.09),
    'fixed_0p10': lambda: FixedRate(rate=0.10),
    'fixed_0p11': lambda: FixedRate(rate=0.11),
    'fixed_0p12': lambda: FixedRate(rate=0.12),
}

POLICY_REGISTRY = {
    'capex': CAPEX_POLICIES,
    'growth': GROWTH_POLICIES,
    'fade': FADE_POLICIES,
    'shares': SHARE_POLICIES,
    'terminal': TERMINAL_POLICIES,
    'discount': DISCOUNT_POLICIES,
}

def create_policies(config: ScenarioConfig) -> dict[str, Any]:
  """
  Create policy instances from scenario configuration.

  Args:
    config: ScenarioConfig with policy names

  Returns:
    Dictionary with instantiated policy objects:
    - capex: CapexPolicy
    - growth: GrowthPolicy
    - fade: FadePolicy
    - shares: SharePolicy
    - terminal: TerminalPolicy
    - discount: DiscountPolicy

  Raises:
    KeyError: If a policy name is not found in the registry
  """
  try:
    capex_factory = CAPEX_POLICIES[config.capex]
  except KeyError as e:
    raise KeyError(f"Unknown capex policy: '{config.capex}'. "
                   f'Available: {list(CAPEX_POLICIES.keys())}') from e

  try:
    growth_factory = GROWTH_POLICIES[config.growth]
  except KeyError as e:
    raise KeyError(f"Unknown growth policy: '{config.growth}'. "
                   f'Available: {list(GROWTH_POLICIES.keys())}') from e

  try:
    fade_factory = FADE_POLICIES[config.fade]
  except KeyError as e:
    raise KeyError(f"Unknown fade policy: '{config.fade}'. "
                   f'Available: {list(FADE_POLICIES.keys())}') from e

  try:
    shares_factory = SHARE_POLICIES[config.shares]
  except KeyError as e:
    raise KeyError(f"Unknown shares policy: '{config.shares}'. "
                   f'Available: {list(SHARE_POLICIES.keys())}') from e

  try:
    terminal_factory = TERMINAL_POLICIES[config.terminal]
  except KeyError as e:
    raise KeyError(f"Unknown terminal policy: '{config.terminal}'. "
                   f'Available: {list(TERMINAL_POLICIES.keys())}') from e

  try:
    discount_factory = DISCOUNT_POLICIES[config.discount]
  except KeyError as e:
    raise KeyError(f"Unknown discount policy: '{config.discount}'. "
                   f'Available: {list(DISCOUNT_POLICIES.keys())}') from e

  return {
      'capex': capex_factory(),
      'growth': growth_factory(),
      'fade': fade_factory(),
      'shares': shares_factory(),
      'terminal': terminal_factory(),
      'discount': discount_factory(),
  }

def list_policies() -> dict[str, list[str]]:
  """
  List all available policies by category.

  Returns:
    Dictionary mapping category names to list of policy names
  """
  result: dict[str, list[str]] = {}
  for category, policies_dict in POLICY_REGISTRY.items():
    policy_dict = cast(dict[str, object], policies_dict)
    result[category] = list(policy_dict.keys())
  return result
