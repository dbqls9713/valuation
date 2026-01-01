import math

import pandas as pd
import pytest

from valuation.domain.types import FundamentalsSlice
from valuation.domain.types import PreparedInputs
from valuation.domain.types import QuarterData
from valuation.domain.types import ValuationResult


class TestQuarterData:
  """Tests for QuarterData dataclass."""

  def test_period_property(self):
    """period property returns formatted string."""
    qd = QuarterData(
        fiscal_year=2023,
        fiscal_quarter='Q1',
        end=pd.Timestamp('2023-03-31'),
        filed=pd.Timestamp('2023-05-01'),
        cfo_ttm=100.0,
    )
    assert qd.period == '2023Q1'

  def test_optional_fields_default_none(self):
    """Optional fields default to None."""
    qd = QuarterData(
        fiscal_year=2023,
        fiscal_quarter='Q1',
        end=pd.Timestamp('2023-03-31'),
        filed=pd.Timestamp('2023-05-01'),
    )
    assert qd.cfo_ttm is None
    assert qd.capex_ttm is None
    assert qd.shares is None


class TestFundamentalsSlice:
  """Tests for FundamentalsSlice dataclass."""

  def _make_panel(self, num_quarters: int = 4) -> pd.DataFrame:
    """Create test panel with fiscal_year/fiscal_quarter columns."""
    rows = []
    for i in range(num_quarters):
      year = 2022 + i // 4
      q = i % 4 + 1
      month = q * 3
      rows.append({
          'ticker': 'AAPL',
          'end': pd.Timestamp(year=year, month=month, day=28),
          'filed': pd.Timestamp(year=year, month=month, day=28) +
          pd.Timedelta(days=45),
          'fiscal_year': year,
          'fiscal_quarter': f'Q{q}',
          'cfo_ttm': 100 + i * 10,
          'capex_ttm': 20 + i * 2,
          'shares_q': 100 - i * 2,
      })
    return pd.DataFrame(rows)

  def test_from_panel_normal_case(self):
    """Construct from panel with valid data."""
    panel = self._make_panel(4)
    as_of = panel['filed'].max() + pd.Timedelta(days=5)

    result = FundamentalsSlice.from_panel(panel, 'AAPL', as_of)

    assert result.ticker == 'AAPL'
    assert result.latest_cfo_ttm == 130.0
    assert result.latest_capex_ttm == 26.0
    assert result.latest_shares == 94.0
    assert len(result.quarters) == 4

  def test_from_panel_no_ticker(self):
    """Missing ticker raises ValueError."""
    panel = self._make_panel(4)
    as_of = panel['filed'].max()

    with pytest.raises(ValueError, match='No data for ticker MSFT'):
      FundamentalsSlice.from_panel(panel, 'MSFT', as_of)

  def test_from_panel_no_pit_data(self):
    """No data as of date raises ValueError."""
    panel = self._make_panel(4)
    early_date = panel['filed'].min() - pd.Timedelta(days=365)

    with pytest.raises(ValueError, match='No data for AAPL as of'):
      FundamentalsSlice.from_panel(panel, 'AAPL', early_date)

  def test_from_panel_missing_required_fields(self):
    """Missing required fields raises ValueError."""
    panel = self._make_panel(4)
    panel.loc[panel.index[-1], 'cfo_ttm'] = float('nan')
    as_of = panel['filed'].max() + pd.Timedelta(days=5)

    with pytest.raises(ValueError, match='Missing required data'):
      FundamentalsSlice.from_panel(panel, 'AAPL', as_of)

  def test_pit_filtering(self):
    """PIT filtering excludes future data."""
    panel = self._make_panel(4)
    as_of = panel['filed'].iloc[2] + pd.Timedelta(days=5)

    result = FundamentalsSlice.from_panel(panel, 'AAPL', as_of)

    assert result.latest_cfo_ttm == 120.0
    assert len(result.quarters) == 3

  def test_history_properties(self):
    """History properties return correct lists."""
    panel = self._make_panel(4)
    as_of = panel['filed'].max() + pd.Timedelta(days=5)

    result = FundamentalsSlice.from_panel(panel, 'AAPL', as_of)

    assert result.cfo_ttm_history == [100.0, 110.0, 120.0, 130.0]
    assert result.capex_ttm_history == [20.0, 22.0, 24.0, 26.0]
    assert result.shares_history == [100.0, 98.0, 96.0, 94.0]


class TestPreparedInputs:
  """Tests for PreparedInputs dataclass."""

  def test_g_end_property(self):
    """g_end property returns last growth rate."""
    inputs = PreparedInputs(
        oe0=100.0,
        sh0=100.0,
        buyback_rate=0.02,
        g0=0.10,
        g_terminal=0.03,
        growth_path=[0.10, 0.08, 0.06, 0.04],
        n_years=4,
        discount_rate=0.10,
    )

    assert inputs.g_end == 0.04

  def test_g_end_empty_growth_path(self):
    """g_end returns g_terminal if no growth path."""
    inputs = PreparedInputs(
        oe0=100.0,
        sh0=100.0,
        buyback_rate=0.02,
        g0=0.10,
        g_terminal=0.03,
        growth_path=[],
        n_years=0,
        discount_rate=0.10,
    )

    assert inputs.g_end == 0.03


class TestValuationResult:
  """Tests for ValuationResult dataclass."""

  def test_with_nan_iv(self):
    """Handle NaN intrinsic value."""
    result = ValuationResult(
        iv_per_share=float('nan'),
        pv_explicit=float('nan'),
        tv_component=float('nan'),
    )

    assert math.isnan(result.iv_per_share)

  def test_to_dict_basic(self):
    """Convert to dictionary."""
    result = ValuationResult(
        iv_per_share=150.0,
        pv_explicit=80.0,
        tv_component=70.0,
    )

    d = result.to_dict()

    assert d['iv_per_share'] == 150.0
    assert d['pv_explicit'] == 80.0
    assert d['tv_component'] == 70.0

  def test_to_dict_with_inputs(self):
    """to_dict includes input parameters."""
    inputs = PreparedInputs(
        oe0=100.0,
        sh0=100.0,
        buyback_rate=0.02,
        g0=0.08,
        g_terminal=0.03,
        growth_path=[0.08, 0.06, 0.05],
        n_years=3,
        discount_rate=0.10,
    )

    result = ValuationResult(
        iv_per_share=150.0,
        pv_explicit=80.0,
        tv_component=70.0,
        inputs=inputs,
    )

    d = result.to_dict()

    assert d['oe0'] == 100.0
    assert d['g0'] == 0.08
    assert d['discount_rate'] == 0.10

  def test_to_dict_with_diagnostics(self):
    """to_dict includes diagnostics."""
    result = ValuationResult(
        iv_per_share=150.0,
        pv_explicit=80.0,
        tv_component=70.0,
        diag={
            'capex_method': 'weighted_avg',
            'years': 3
        },
    )

    d = result.to_dict()

    assert d['capex_method'] == 'weighted_avg'
    assert d['years'] == 3
