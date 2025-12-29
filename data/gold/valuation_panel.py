"""
Build Gold layer: model-ready valuation panel.

Joins Silver layer tables into wide-format panel:
- Quarterly metrics (CFO, CAPEX, shares)
- TTM metrics
- Daily prices
- Market cap calculation

Output: valuation_panel.parquet
"""

from pathlib import Path
from typing import Optional

import pandas as pd

from data.silver.io import write_parquet_with_meta


def build_valuation_panel(
    *,
    silver_dir: Path,
    gold_dir: Path,
    min_date: Optional[str] = None,
) -> pd.DataFrame:
  """
    Build model-ready valuation panel from Silver tables.

    Args:
        silver_dir: Path to silver layer directory
        gold_dir: Path to gold layer output directory
        min_date: Optional minimum date filter (YYYY-MM-DD)

    Returns:
        Wide-format panel with ticker, date, metrics, price
    """
  companies = pd.read_parquet(silver_dir / "sec" / "companies.parquet")
  metrics_q = pd.read_parquet(silver_dir / "sec" / "metrics_quarterly.parquet")
  prices = pd.read_parquet(silver_dir / "stooq" / "prices_daily.parquet")

  panel = _build_panel(companies, metrics_q, prices, min_date=min_date)

  gold_dir.mkdir(parents=True, exist_ok=True)
  write_parquet_with_meta(
      panel,
      gold_dir / "valuation_panel.parquet",
      inputs=[
          silver_dir / "sec" / "companies.parquet",
          silver_dir / "sec" / "metrics_quarterly.parquet",
          silver_dir / "stooq" / "prices_daily.parquet",
      ],
      meta_extra={
          "layer": "gold",
          "dataset": "valuation_panel",
          "min_date": min_date,
      },
  )

  return panel


def _build_panel(
    companies: pd.DataFrame,
    metrics_q: pd.DataFrame,
    prices: pd.DataFrame,
    min_date: Optional[str] = None,
) -> pd.DataFrame:
  """
    Core logic to build panel from Silver tables.

    Strategy:
    1. Pivot metrics to wide format (one row per cik10/end)
    2. Map cik10 -> ticker
    3. Join with daily prices (ticker/date)
    4. Calculate market cap
    5. Filter and sort
    """
  metrics_wide = _pivot_metrics_wide(metrics_q)

  metrics_wide = metrics_wide.merge(companies[["cik10", "ticker"]],
                                    on="cik10",
                                    how="left")

  metrics_wide = metrics_wide.dropna(subset=["ticker"])

  panel = _join_prices(metrics_wide, prices)

  if "shares_q" in panel.columns and "price" in panel.columns:
    panel["market_cap"] = panel["shares_q"] * panel["price"]
  else:
    panel["market_cap"] = None

  if min_date:
    panel = panel[panel["date"] >= min_date]

  panel = panel.sort_values(["ticker", "date"]).reset_index(drop=True)

  return panel


def _pivot_metrics_wide(metrics_q: pd.DataFrame) -> pd.DataFrame:
  """
    Pivot metrics_quarterly to wide format.

    Input: long format (cik10, metric, end, q_val, ttm_val)
    Output: wide format (cik10, end, cfo_q, cfo_ttm, capex_q, ...)
    """
  metrics_list = metrics_q["metric"].unique()

  parts = []
  for metric in metrics_list:
    m = metrics_q[metrics_q["metric"] == metric].copy()

    m_wide = m.pivot_table(
        index=["cik10", "end", "filed"],
        values=["q_val", "ttm_val"],
        aggfunc="first",
    ).reset_index()

    metric_lower = metric.lower()
    m_wide = m_wide.rename(columns={
        "q_val": f"{metric_lower}_q",
        "ttm_val": f"{metric_lower}_ttm",
    })

    parts.append(m_wide)

  if not parts:
    return pd.DataFrame(columns=["cik10", "end", "filed"])

  result = parts[0]
  for part in parts[1:]:
    result = result.merge(part, on=["cik10", "end", "filed"], how="outer")

  return result


def _join_prices(
    metrics_wide: pd.DataFrame,
    prices: pd.DataFrame,
) -> pd.DataFrame:
  """
    Join metrics with daily prices using point-in-time logic.

    For each metric period end, find the next available price after filed date.
    This ensures we only use information available at that time.
    """
  metrics_wide = metrics_wide.copy()
  metrics_wide["end"] = pd.to_datetime(metrics_wide["end"])
  metrics_wide["filed"] = pd.to_datetime(metrics_wide["filed"])

  prices = prices.copy()
  prices["date"] = pd.to_datetime(prices["date"])

  prices = prices.rename(columns={"symbol": "ticker"})
  prices["ticker"] = prices["ticker"].str.replace(".US", "", regex=False)

  panel_parts = []

  for ticker, ticker_metrics in metrics_wide.groupby("ticker"):
    ticker_prices = prices[prices["ticker"] == ticker].copy()

    if ticker_prices.empty:
      continue

    ticker_prices = ticker_prices.sort_values("date")

    merged = pd.merge_asof(
        ticker_metrics.sort_values("filed"),
        ticker_prices[["date", "close"]],
        left_on="filed",
        right_on="date",
        direction="forward",
    )

    merged = merged.rename(columns={"close": "price"})
    panel_parts.append(merged)

  if not panel_parts:
    return pd.DataFrame(columns=list(metrics_wide.columns) + ["date", "price"])

  panel = pd.concat(panel_parts, ignore_index=True)

  panel = panel.drop(columns=["cik10"], errors="ignore")

  return panel
