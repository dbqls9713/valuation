"""
Gold layer panel builders.

Builds model-ready panels for DCF analysis:
- valuation_panel: Latest version for current valuation
- backtest_panel: All filed versions for PIT backtesting
"""

from abc import ABC
from abc import abstractmethod
from pathlib import Path
from typing import Optional

import pandas as pd

from data.gold.config.schemas import BACKTEST_PANEL_SCHEMA
from data.gold.config.schemas import PanelSchema
from data.gold.config.schemas import VALUATION_PANEL_SCHEMA
from data.gold.transforms import calculate_market_cap
from data.gold.transforms import join_metrics_by_cfo_filed
from data.gold.transforms import join_prices_pit
from data.shared.io import ParquetWriter


class _BasePanelBuilder(ABC):
  """Base class for panel builders."""

  REQUIRED_METRICS = ['CFO', 'CAPEX', 'SHARES']

  def __init__(
      self,
      silver_dir: Path,
      gold_dir: Path,
      schema: PanelSchema,
      min_date: Optional[str] = None,
  ):
    self.silver_dir = Path(silver_dir)
    self.gold_dir = Path(gold_dir)
    self.schema = schema
    self.min_date = min_date
    self.panel: Optional[pd.DataFrame] = None

  @abstractmethod
  def build(self) -> pd.DataFrame:
    """Build the panel. Subclasses must implement."""

  def _load_data(self) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Load required data from Silver layer."""
    companies = pd.read_parquet(self.silver_dir / 'sec' / 'companies.parquet')
    metrics_q = pd.read_parquet(self.silver_dir / 'sec' /
                                'metrics_quarterly.parquet')
    prices = pd.read_parquet(self.silver_dir / 'stooq' / 'prices_daily.parquet')
    return companies, metrics_q, prices

  def _build_wide_metrics(self, metrics_q: pd.DataFrame) -> pd.DataFrame:
    """
    Join metrics using CFO's filed date as the reference point.

    For each CFO filing, CAPEX and SHARES are joined using the most recent
    values available at that time (filed <= CFO filed date).
    """
    filtered = metrics_q[metrics_q['metric'].isin(self.REQUIRED_METRICS)]
    return join_metrics_by_cfo_filed(filtered)

  def _filter_complete_rows(self, df: pd.DataFrame) -> pd.DataFrame:
    """Keep only rows with both CFO_TTM and CAPEX_TTM."""
    mask = df['cfo_ttm'].notna() & df['capex_ttm'].notna()
    return df[mask].copy()

  def validate(self) -> list[str]:
    """Validate the built panel against schema."""
    if self.panel is None:
      return ['Panel not built yet. Call build() first.']
    return self.schema.validate(self.panel)

  def save(self) -> Path:
    """Save the panel to Gold output directory."""
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

    panel = self.panel
    n_tickers = panel['ticker'].nunique()
    date_min = panel['end'].min()
    date_max = panel['end'].max()
    return '\n'.join([
        f'Panel: {self.schema.name}',
        f'Shape: {panel.shape}',
        f'Tickers: {n_tickers}',
        f'Date range: {date_min} to {date_max}',
        f'Columns: {list(panel.columns)}',
    ])


class BacktestPanelBuilder(_BasePanelBuilder):
  """
  Builds backtest panel with all filed versions for PIT analysis.

  Primary key: (ticker, end, filed)

  Shares are normalized to the latest filed version for each (ticker, end)
  to ensure consistency with split-adjusted prices.
  """

  def __init__(
      self,
      silver_dir: Path,
      gold_dir: Path,
      min_date: Optional[str] = None,
  ):
    super().__init__(silver_dir, gold_dir, BACKTEST_PANEL_SCHEMA, min_date)

  def build(self) -> pd.DataFrame:
    """Build backtest panel with all PIT versions."""
    companies, metrics_q, prices = self._load_data()

    metrics_wide = self._build_wide_metrics(metrics_q)
    metrics_wide = self._filter_complete_rows(metrics_wide)

    # Note: Shares are NOT normalized here to preserve PIT consistency.
    # Split adjustment is handled by ValuationDataLoader._adjust_for_splits

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

    self.panel = panel.sort_values(['ticker', 'end',
                                    'filed']).reset_index(drop=True)
    return self.panel


class ValuationPanelBuilder(_BasePanelBuilder):
  """
  Builds valuation panel with latest filed version only.

  Primary key: (ticker, end)
  """

  def __init__(
      self,
      silver_dir: Path,
      gold_dir: Path,
      min_date: Optional[str] = None,
  ):
    super().__init__(silver_dir, gold_dir, VALUATION_PANEL_SCHEMA, min_date)

  def build(self) -> pd.DataFrame:
    """Build valuation panel with latest version per period."""
    companies, metrics_q, prices = self._load_data()

    metrics_wide = self._build_wide_metrics(metrics_q)
    metrics_wide = self._filter_complete_rows(metrics_wide)

    # Keep only latest filed version per (cik10, end)
    metrics_wide = metrics_wide.sort_values('filed')
    metrics_wide = metrics_wide.groupby(['cik10', 'end'],
                                        as_index=False).tail(1)

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
