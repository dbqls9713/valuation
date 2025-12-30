"""
Build Gold layer outputs.

Usage:
  python -m data.gold.build
"""

from pathlib import Path

import pandas as pd

from data.gold.valuation_panel import build_valuation_panel


def main() -> None:
  # Set pandas display options to show all columns
  pd.set_option("display.max_columns", None)
  pd.set_option("display.width", None)
  pd.set_option("display.max_colwidth", None)

  silver_dir = Path("data/silver/out")
  gold_dir = Path("data/gold/out")

  panel = build_valuation_panel(
      silver_dir=silver_dir,
      gold_dir=gold_dir,
      min_date="2010-01-01",
  )

  print(f"Valuation panel: {panel.shape}, columns: {list(panel.columns)}")
  print("\nSample (first 10 rows):")
  print(panel.head(10).to_string(index=False))


if __name__ == "__main__":
  main()
