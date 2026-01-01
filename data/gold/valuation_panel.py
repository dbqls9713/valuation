"""
DEPRECATED: Use data.gold.panels.ValuationPanelBuilder instead.

This module is kept for backwards compatibility.

New usage:
  from data.gold.panels import ValuationPanelBuilder

  builder = ValuationPanelBuilder(silver_dir, gold_dir, min_date='2010-01-01')
  panel = builder.build()
  builder.save()
"""

from pathlib import Path
from typing import Optional
import warnings

import pandas as pd

from data.gold.panels import ValuationPanelBuilder


def build_valuation_panel(
    *,
    silver_dir: Path,
    gold_dir: Path,
    min_date: Optional[str] = None,
) -> pd.DataFrame:
  """
  DEPRECATED: Use ValuationPanelBuilder instead.

  Build model-ready valuation panel from Silver tables.
  """
  warnings.warn(
      'build_valuation_panel is deprecated. '
      'Use ValuationPanelBuilder instead.',
      DeprecationWarning,
      stacklevel=2,
  )

  builder = ValuationPanelBuilder(
      silver_dir=silver_dir,
      gold_dir=gold_dir,
      min_date=min_date,
  )
  result: pd.DataFrame = builder.build()
  builder.save()

  return result
