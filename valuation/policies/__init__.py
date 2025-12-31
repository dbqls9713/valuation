"""
Valuation policies for estimating DCF inputs.

Each policy estimates one component of the valuation model (CAPEX, growth rate,
etc.) and returns both a value and diagnostic information.

To add a new policy:
1. Create a new class inheriting from the appropriate base (e.g., CapexPolicy)
2. Implement the compute() method returning PolicyOutput
3. Register in scenarios/registry.py

Example:
  class MyCustomCapex(CapexPolicy):
    def compute(self, data: FundamentalsSlice) -> PolicyOutput[float]:
    capex = ...  # your calculation
    return PolicyOutput(value=capex, diag={'method': 'my_custom'})
"""

from valuation.policies.capex import CapexPolicy
from valuation.policies.capex import IntensityClippedCapex
from valuation.policies.capex import RawTTMCapex
from valuation.policies.capex import WeightedAverageCapex
from valuation.policies.discount import DiscountPolicy
from valuation.policies.discount import FixedRate
from valuation.policies.fade import FadePolicy
from valuation.policies.fade import LinearFade
from valuation.policies.growth import CAGRGrowth
from valuation.policies.growth import GrowthPolicy
from valuation.policies.shares import AvgShareChange
from valuation.policies.shares import SharePolicy
from valuation.policies.terminal import GordonTerminal
from valuation.policies.terminal import TerminalPolicy

__all__ = [
  'CapexPolicy', 'RawTTMCapex', 'WeightedAverageCapex', 'IntensityClippedCapex',
  'GrowthPolicy', 'CAGRGrowth',
  'FadePolicy', 'LinearFade',
  'SharePolicy', 'AvgShareChange',
  'TerminalPolicy', 'GordonTerminal',
  'DiscountPolicy', 'FixedRate',
]
