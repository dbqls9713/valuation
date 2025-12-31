"""
Caching data loader for valuation data.

Provides efficient loading and caching of Gold panel and price data
to avoid repeated file I/O in batch processing scenarios.

Usage:
  # Single valuation (no caching needed)
  result = run_valuation(ticker='AAPL', as_of_date='2024-12-31')

  # Batch valuation (with caching)
  loader = ValuationDataLoader()
  for ticker in tickers:
    result = run_valuation(ticker, as_of_date, loader=loader)
"""

from pathlib import Path
from typing import Optional

import pandas as pd


class ValuationDataLoader:
  """
  Cached data loader for valuation operations.

  Loads and caches:
  - Gold panel (with split adjustments)
  - Price data

  This avoids repeated file I/O when running multiple valuations.
  """

  def __init__(
      self,
      gold_path: Path = Path('data/gold/out/valuation_panel.parquet'),
      silver_dir: Path = Path('data/silver/out'),
  ):
    """
    Initialize data loader.

    Args:
      gold_path: Path to Gold panel parquet file
      silver_dir: Path to Silver layer output directory
    """
    self.gold_path = gold_path
    self.silver_dir = silver_dir

    self._panel: Optional[pd.DataFrame] = None
    self._prices: Optional[pd.DataFrame] = None

  def load_panel(self) -> pd.DataFrame:
    """
    Load and cache Gold panel with split adjustments.

    Returns:
      DataFrame with split-adjusted shares

    Raises:
      FileNotFoundError: If Gold panel does not exist
    """
    if self._panel is not None:
      return self._panel

    if not self.gold_path.exists():
      raise FileNotFoundError(
          f'Gold panel not found: {self.gold_path}. '
          'Run "python -m data.gold.build" first.')

    panel = pd.read_parquet(self.gold_path)
    panel['end'] = pd.to_datetime(panel['end'])
    panel['filed'] = pd.to_datetime(panel['filed'])

    # Apply split adjustments
    panel = self._adjust_for_splits(panel)

    self._panel = panel
    return panel

  def load_prices(self) -> pd.DataFrame:
    """
    Load and cache price data.

    Returns:
      DataFrame with daily price data

    Raises:
      FileNotFoundError: If price data does not exist
    """
    if self._prices is not None:
      return self._prices

    prices_path = self.silver_dir / 'stooq' / 'prices_daily.parquet'
    if not prices_path.exists():
      raise FileNotFoundError(f'Price data not found: {prices_path}')

    prices = pd.read_parquet(prices_path)
    prices['date'] = pd.to_datetime(prices['date'])

    self._prices = prices
    return prices

  def clear_cache(self) -> None:
    """Clear all cached data."""
    self._panel = None
    self._prices = None

  @staticmethod
  def _adjust_for_splits(panel: pd.DataFrame) -> pd.DataFrame:
    """
    Adjust shares for stock splits across all tickers.

    Must be applied before PIT filtering to ensure historical shares
    are on the same basis as current prices.

    Args:
      panel: Raw Gold panel data

    Returns:
      Panel with split-adjusted shares
    """
    adjusted_parts = []

    for ticker in panel['ticker'].unique():
      ticker_data = panel[panel['ticker'] == ticker].copy()
      ticker_data = ticker_data.sort_values('end')

      shares_missing = ('shares_q' not in ticker_data.columns or
                        ticker_data['shares_q'].isna().all())
      if shares_missing:
        adjusted_parts.append(ticker_data)
        continue

      ticker_data['shares_ratio'] = (ticker_data['shares_q'] /
                                     ticker_data['shares_q'].shift(1))

      splits = ticker_data[(ticker_data['shares_ratio'] > 2) |
                           (ticker_data['shares_ratio'] < 0.5)].copy()

      if not splits.empty:
        for idx in splits.index[::-1]:
          split_date = ticker_data.loc[idx, 'end']
          split_ratio = ticker_data.loc[idx, 'shares_ratio']

          mask = ticker_data['end'] < split_date
          ticker_data.loc[mask, 'shares_q'] *= split_ratio

          ticker_data['shares_ratio'] = (ticker_data['shares_q'] /
                                         ticker_data['shares_q'].shift(1))

      ticker_data = ticker_data.drop(columns=['shares_ratio'])
      adjusted_parts.append(ticker_data)

    result: pd.DataFrame = pd.concat(adjusted_parts, ignore_index=True)
    return result
