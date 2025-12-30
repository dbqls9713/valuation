'''
Domain types for the valuation framework.

These dataclasses provide typed interfaces between components, ensuring
policies don't directly depend on raw DataFrame columns.
'''

from dataclasses import dataclass, field
from typing import Any, Dict, Generic, List, Optional, TypeVar

import pandas as pd

T = TypeVar('T')


@dataclass
class PolicyOutput(Generic[T]):
  '''
  Standard output from any policy.

  Every policy returns both a computed value and diagnostic information
  explaining how the value was computed.

  Attributes:
    value: The computed value (type depends on policy)
    diag: Dictionary of diagnostic information
  '''
  value: T
  diag: Dict[str, Any] = field(default_factory=dict)


@dataclass
class FundamentalsSlice:
  '''
  Point-in-time slice of fundamental data for a single company.

  This is the primary input to policies. All data should reflect only
  what was available as of the cutoff date (PIT-safe).

  Attributes:
    ticker: Company ticker symbol
    as_of_end: Quarter end date for valuation
    filed_cutoff: PIT cutoff date (only data filed before this is used)
    cfo_ttm_history: Series of TTM CFO values indexed by quarter end date
    capex_ttm_history: Series of TTM CAPEX values indexed by quarter end date
    shares_history: Series of shares count indexed by quarter end date
    latest_cfo_ttm: Most recent TTM CFO value
    latest_capex_ttm: Most recent TTM CAPEX value
    latest_shares: Most recent shares count
    latest_filed: Filing date of the most recent data
  '''
  ticker: str
  as_of_end: pd.Timestamp
  filed_cutoff: pd.Timestamp
  cfo_ttm_history: pd.Series
  capex_ttm_history: pd.Series
  shares_history: pd.Series
  latest_cfo_ttm: float
  latest_capex_ttm: float
  latest_shares: float
  latest_filed: pd.Timestamp

  @classmethod
  def from_panel(cls, panel: pd.DataFrame, ticker: str,
                 as_of_date: pd.Timestamp) -> 'FundamentalsSlice':
    '''
    Construct FundamentalsSlice from Gold panel with PIT filtering.

    Args:
      panel: Gold valuation panel DataFrame
      ticker: Company ticker symbol
      as_of_date: Point-in-time date (only data filed <= this date)

    Returns:
      FundamentalsSlice with PIT-filtered data
    '''
    ticker_data = panel[panel['ticker'] == ticker].copy()
    if ticker_data.empty:
      raise ValueError(f'No data for ticker {ticker}')

    pit_data = ticker_data[ticker_data['filed'] <= as_of_date].copy()
    if pit_data.empty:
      raise ValueError(f'No data for {ticker} as of {as_of_date.date()}')

    pit_data = pit_data.sort_values('end')
    latest = pit_data.iloc[-1]

    return cls(
        ticker=ticker,
        as_of_end=latest['end'],
        filed_cutoff=as_of_date,
        cfo_ttm_history=pit_data.set_index('end')['cfo_ttm'],
        capex_ttm_history=pit_data.set_index('end')['capex_ttm'],
        shares_history=pit_data.set_index('end')['shares_q'],
        latest_cfo_ttm=float(latest['cfo_ttm']),
        latest_capex_ttm=float(latest['capex_ttm']),
        latest_shares=float(latest['shares_q']),
        latest_filed=latest['filed'],
    )


@dataclass
class MarketSlice:
  '''
  Market price data for comparison.

  Attributes:
    price: Market price per share
    price_date: Date of the price observation
  '''
  price: float
  price_date: pd.Timestamp


@dataclass
class PreparedInputs:
  '''
  Fully prepared inputs for the DCF engine.

  All policy outputs are aggregated here before passing to the pure math engine.

  Attributes:
    oe0: Initial owner earnings (CFO - CAPEX)
    sh0: Current shares outstanding
    buyback_rate: Annual share reduction rate
    g0: Initial growth rate (first year in growth_path)
    g_terminal: Terminal growth rate (for Gordon Growth)
    growth_path: Yearly growth rates [g1, g2, ..., gN] from fade policy
    n_years: Number of explicit forecast years
    discount_rate: Required return / discount rate
  '''
  oe0: float
  sh0: float
  buyback_rate: float
  g0: float
  g_terminal: float
  growth_path: List[float]
  n_years: int
  discount_rate: float

  @property
  def g_end(self) -> float:
    '''Growth rate at end of explicit period (last in growth_path).'''
    if not self.growth_path:
      return self.g_terminal
    return self.growth_path[-1]


@dataclass
class ValuationResult:
  '''
  Complete valuation result with diagnostics.

  Attributes:
    iv_per_share: Intrinsic value per share
    pv_explicit: Present value of explicit forecast period
    tv_component: Terminal value component (discounted)
    market_price: Market price (if provided)
    price_to_iv: Market price / IV ratio (if market price provided)
    margin_of_safety: (IV - Price) / IV (if market price provided)
    inputs: The PreparedInputs used for calculation
    diag: Merged diagnostics from all policies
  '''
  iv_per_share: float
  pv_explicit: float
  tv_component: float
  market_price: Optional[float] = None
  price_to_iv: Optional[float] = None
  margin_of_safety: Optional[float] = None
  inputs: Optional[PreparedInputs] = None
  diag: Dict[str, Any] = field(default_factory=dict)

  def to_dict(self) -> Dict[str, Any]:
    '''Convert to dictionary for DataFrame creation.'''
    result = {
        'iv_per_share': self.iv_per_share,
        'pv_explicit': self.pv_explicit,
        'tv_component': self.tv_component,
        'market_price': self.market_price,
        'price_to_iv': self.price_to_iv,
        'margin_of_safety': self.margin_of_safety,
    }
    if self.inputs:
      result.update({
          'oe0': self.inputs.oe0,
          'sh0': self.inputs.sh0,
          'buyback_rate': self.inputs.buyback_rate,
          'g0': self.inputs.g0,
          'g_terminal': self.inputs.g_terminal,
          'discount_rate': self.inputs.discount_rate,
      })
    result.update(self.diag)
    return result


@dataclass
class ExclusionReason:
  '''
  Reason why a valuation was excluded/skipped.

  Attributes:
    reason: Human-readable explanation
    code: Machine-readable code (e.g., 'insufficient_data', 'low_growth')
    details: Additional context
  '''
  reason: str
  code: str
  details: Dict[str, Any] = field(default_factory=dict)
