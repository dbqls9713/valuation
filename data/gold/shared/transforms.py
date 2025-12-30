'''
Shared transformation functions for Gold layer panels.

These functions handle common operations like pivoting metrics
and joining with price data using point-in-time logic.
'''

from typing import List, Optional

import pandas as pd


def pivot_metrics_wide(
    metrics_q: pd.DataFrame,
    metrics: Optional[List[str]] = None,
) -> pd.DataFrame:
  '''
  Pivot metrics_quarterly from long to wide format.

  Args:
    metrics_q: Long-format metrics (cik10, metric, end, q_val, ttm_val, filed)
    metrics: List of metrics to include (None = all)

  Returns:
    Wide-format DataFrame with columns like cfo_q, cfo_ttm, capex_q, etc.

  Note:
    Filed date is consolidated using max() across all metrics for each
    (cik10, end) combination.
  '''
  if metrics:
    metrics_q = metrics_q[metrics_q['metric'].isin(metrics)].copy()

  metric_list = metrics_q['metric'].unique()
  if len(metric_list) == 0:
    return pd.DataFrame(columns=['cik10', 'end'])

  parts = []
  for metric in metric_list:
    m = metrics_q[metrics_q['metric'] == metric].copy()

    m_wide = m.pivot_table(
        index=['cik10', 'end'],
        values=['q_val', 'ttm_val', 'filed'],
        aggfunc={
            'q_val': 'first',
            'ttm_val': 'first',
            'filed': 'max',
        },
    ).reset_index()

    metric_lower = metric.lower()
    m_wide = m_wide.rename(
        columns={
            'q_val': f'{metric_lower}_q',
            'ttm_val': f'{metric_lower}_ttm',
            'filed': f'{metric_lower}_filed',
        })

    parts.append(m_wide)

  result = parts[0]
  for part in parts[1:]:
    result = result.merge(part, on=['cik10', 'end'], how='outer')

  filed_cols = [c for c in result.columns if c.endswith('_filed')]
  if not filed_cols:
    return result  # type: ignore[no-any-return]

  result['filed'] = result[filed_cols].max(axis=1)
  result = result.drop(columns=filed_cols)
  return result  # type: ignore[no-any-return]


def join_prices_pit(
    metrics_wide: pd.DataFrame,
    prices: pd.DataFrame,
    ticker_col: str = 'ticker',
) -> pd.DataFrame:
  '''
  Join metrics with prices using point-in-time logic.

  For each metric row, finds the first available price after the filed date.
  This ensures no look-ahead bias in the panel.

  Args:
    metrics_wide: Wide-format metrics with ticker and filed columns
    prices: Daily prices with symbol and date columns
    ticker_col: Name of ticker column in metrics_wide

  Returns:
    Panel with price and date columns added
  '''
  metrics_wide = metrics_wide.copy()
  metrics_wide['end'] = pd.to_datetime(metrics_wide['end'])
  metrics_wide['filed'] = pd.to_datetime(metrics_wide['filed'])

  prices = prices.copy()
  prices['date'] = pd.to_datetime(prices['date'])

  prices = prices.rename(columns={'symbol': 'ticker'})
  prices['ticker'] = prices['ticker'].str.replace('.US', '', regex=False)

  panel_parts = []

  for ticker, ticker_metrics in metrics_wide.groupby(ticker_col):
    ticker_prices = prices[prices['ticker'] == ticker].copy()
    if ticker_prices.empty:
      continue

    ticker_prices = ticker_prices.sort_values('date')
    merged = pd.merge_asof(
        ticker_metrics.sort_values('filed'),
        ticker_prices[['date', 'close']],
        left_on='filed',
        right_on='date',
        direction='forward',
    )
    merged = merged.rename(columns={'close': 'price'})
    panel_parts.append(merged)

  if not panel_parts:
    cols = list(metrics_wide.columns) + ['date', 'price']
    return pd.DataFrame(columns=cols)

  return pd.concat(panel_parts, ignore_index=True)


def calculate_market_cap(
    panel: pd.DataFrame,
    shares_col: str = 'shares_q',
    price_col: str = 'price',
) -> pd.DataFrame:
  '''
  Calculate market capitalization.

  Args:
    panel: Panel with shares and price columns
    shares_col: Name of shares column
    price_col: Name of price column

  Returns:
    Panel with market_cap column added
  '''
  panel = panel.copy()

  if shares_col not in panel.columns or price_col not in panel.columns:
    panel['market_cap'] = None
    return panel

  panel['market_cap'] = panel[shares_col] * panel[price_col]
  return panel
