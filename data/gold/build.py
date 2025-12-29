"""
Build Gold layer outputs.

Usage:
  python -m data.gold.build
"""

from pathlib import Path

from data.gold.valuation_panel import build_valuation_panel


def main() -> None:
  silver_dir = Path("data/silver")
  gold_dir = Path("data/gold")

  panel = build_valuation_panel(
      silver_dir=silver_dir,
      gold_dir=gold_dir,
      min_date="2010-01-01",
  )

  print(f"Valuation panel: {panel.shape}, columns: {list(panel.columns)}")
  print("\nSample:")
  print(panel.head(10))


if __name__ == "__main__":
  main()
