'''
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
'''

from valuation.policies.capex import (
  CapexPolicy,
  RawTTMCapex,
  WeightedAverageCapex,
  IntensityClippedCapex,
)
from valuation.policies.growth import (
  GrowthPolicy,
  CAGRGrowth,
)
from valuation.policies.fade import (
  FadePolicy,
  LinearFade,
)
from valuation.policies.shares import (
  SharePolicy,
  AvgShareChange,
)
from valuation.policies.terminal import (
  TerminalPolicy,
  GordonTerminal,
)
from valuation.policies.discount import (
  DiscountPolicy,
  FixedRate,
)

__all__ = [
  'CapexPolicy', 'RawTTMCapex', 'WeightedAverageCapex', 'IntensityClippedCapex',
  'GrowthPolicy', 'CAGRGrowth',
  'FadePolicy', 'LinearFade',
  'SharePolicy', 'AvgShareChange',
  'TerminalPolicy', 'GordonTerminal',
  'DiscountPolicy', 'FixedRate',
]
