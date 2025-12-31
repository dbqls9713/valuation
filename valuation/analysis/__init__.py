'''
Valuation analysis utilities.

Note: To avoid RuntimeWarning when using -m flag, import directly:
  from valuation.analysis.batch_valuation import batch_valuation
  from valuation.analysis.compare_capex import plot_capex_comparison
  from valuation.analysis.sensitivity import SensitivityTableBuilder
'''

__all__ = [
    'batch_valuation',
    'plot_capex_comparison',
    'SensitivityTableBuilder',
]

# Direct imports for convenience (may cause RuntimeWarning with -m flag)
from valuation.analysis.batch_valuation import batch_valuation
from valuation.analysis.plot_prices import plot_capex_comparison
from valuation.analysis.sensitivity import SensitivityTableBuilder
