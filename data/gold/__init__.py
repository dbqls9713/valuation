'''
Gold layer: Model-ready panels.

Transforms Silver layer tables into analysis-ready wide-format panels.
Each panel has a defined schema for validation.

Available panels:
- valuation_panel: DCF valuation with CFO, CAPEX, shares, prices

Usage:
  from data.gold.panels import ValuationPanelBuilder

  builder = ValuationPanelBuilder(silver_dir, gold_dir)
  panel = builder.build()
  builder.save()
'''

from data.gold.panels import ValuationPanelBuilder
from data.gold.config import VALUATION_PANEL_SCHEMA

__all__ = [
    'ValuationPanelBuilder',
    'VALUATION_PANEL_SCHEMA',
]
