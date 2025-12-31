"""
Valuation panel builder.

Builds the model-ready valuation panel for DCF analysis:
- Quarterly and TTM metrics (CFO, CAPEX, Shares)
- Point-in-time prices
- Market capitalization
"""

from pathlib import Path
from typing import Optional

import pandas as pd

from data.gold.config.schemas import PanelSchema
from data.gold.config.schemas import VALUATION_PANEL_SCHEMA
from data.gold.shared.transforms import calculate_market_cap
from data.gold.shared.transforms import join_prices_pit
from data.gold.shared.transforms import pivot_metrics_wide
from data.shared.io import ParquetWriter


class ValuationPanelBuilder:
  """
  Builds the valuation panel from Silver layer tables.

  Usage:
    builder = ValuationPanelBuilder(silver_dir, gold_dir)
    panel = builder.build()
    builder.save()
  """

  REQUIRED_METRICS = ['CFO', 'CAPEX', 'SHARES']

  def __init__(
      self,
      silver_dir: Path,
      gold_dir: Path,
      min_date: Optional[str] = None,
  ):
    """
    Initialize valuation panel builder.

    Args:
      silver_dir: Path to Silver layer output directory
      gold_dir: Path to Gold layer output directory
      min_date: Optional minimum date filter (YYYY-MM-DD)
    """
    self.silver_dir = Path(silver_dir)
    self.gold_dir = Path(gold_dir)
    self.min_date = min_date
    self.schema: PanelSchema = VALUATION_PANEL_SCHEMA
    self.panel: Optional[pd.DataFrame] = None

  def build(self) -> pd.DataFrame:
    """
    Build the valuation panel.

    Returns:
      Wide-format panel DataFrame
    """
    companies = pd.read_parquet(self.silver_dir / 'sec' / 'companies.parquet')
    metrics_q = pd.read_parquet(self.silver_dir / 'sec' /
                                'metrics_quarterly.parquet')
    prices = pd.read_parquet(self.silver_dir / 'stooq' / 'prices_daily.parquet')

    metrics_wide = pivot_metrics_wide(metrics_q, metrics=self.REQUIRED_METRICS)

    metrics_wide = self._filter_complete_rows(metrics_wide)

    metrics_wide = metrics_wide.merge(
        companies[['cik10', 'ticker']],
        on='cik10',
        how='left',
    )
    metrics_wide = metrics_wide.dropna(subset=['ticker'])

    panel = join_prices_pit(metrics_wide, prices, ticker_col='ticker')

    panel = calculate_market_cap(panel)

    panel = panel.drop(columns=['cik10'], errors='ignore')

    if self.min_date:
      panel = panel[panel['end'] >= self.min_date]

    self.panel = panel.sort_values(['ticker', 'end']).reset_index(drop=True)
    return self.panel

  def _filter_complete_rows(self, df: pd.DataFrame) -> pd.DataFrame:
    """Keep only rows with both CFO_TTM and CAPEX_TTM."""
    mask = df['cfo_ttm'].notna() & df['capex_ttm'].notna()
    return df[mask].copy()

  def validate(self) -> list[str]:
    """
    Validate the built panel against schema.

    Returns:
      List of validation errors (empty if valid)
    """
    if self.panel is None:
      return ['Panel not built yet. Call build() first.']

    return self.schema.validate(self.panel)

  def save(self) -> Path:
    """
    Save the panel to Gold output directory.

    Returns:
      Path to saved parquet file
    """
    if self.panel is None:
      raise ValueError('Panel not built. Call build() first.')

    self.gold_dir.mkdir(parents=True, exist_ok=True)

    output_path = self.gold_dir / f'{self.schema.name}.parquet'

    writer = ParquetWriter()
    writer.write(
        self.panel,
        output_path,
        inputs=[
            self.silver_dir / 'sec' / 'companies.parquet',
            self.silver_dir / 'sec' / 'metrics_quarterly.parquet',
            self.silver_dir / 'stooq' / 'prices_daily.parquet',
        ],
        metadata={
            'layer': 'gold',
            'dataset': self.schema.name,
            'min_date': self.min_date,
            'schema_version': '1.0',
        },
    )

    return output_path

  def summary(self) -> str:
    """Return summary of the built panel."""
    if self.panel is None:
      return 'Panel not built yet.'

    n_tickers = self.panel['ticker'].nunique()
    date_min = self.panel['end'].min()
    date_max = self.panel['end'].max()
    lines = [
        f'Panel: {self.schema.name}',
        f'Shape: {self.panel.shape}',
        f'Tickers: {n_tickers}',
        f'Date range: {date_min} to {date_max}',
        f'Columns: {list(self.panel.columns)}',
    ]
    return '\n'.join(lines)
