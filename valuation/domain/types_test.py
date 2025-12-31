import math

import pandas as pd
import pytest

from valuation.domain.types import FundamentalsSlice
from valuation.domain.types import PreparedInputs
from valuation.domain.types import ValuationResult


class TestFundamentalsSlice:
  """Tests for FundamentalsSlice dataclass."""

  def test_from_panel_normal_case(self):
    """Construct from panel with valid data."""
    dates = pd.date_range('2022-03-31', periods=4, freq='Q')
    panel = pd.DataFrame({
        'ticker': ['AAPL'] * 4,
        'end': dates,
        'filed': dates + pd.Timedelta(days=45),
        'cfo_ttm': [100, 110, 120, 130],
        'capex_ttm': [20, 22, 24, 26],
        'shares_q': [100, 98, 96, 94],
    })

    as_of = dates[-1] + pd.Timedelta(days=50)
    result = FundamentalsSlice.from_panel(panel, 'AAPL', as_of)

    assert result.ticker == 'AAPL'
    assert result.latest_cfo_ttm == 130.0
    assert result.latest_capex_ttm == 26.0
    assert result.latest_shares == 94.0

  def test_from_panel_no_ticker(self):
    """Missing ticker raises ValueError."""
    dates = pd.date_range('2022-03-31', periods=4, freq='Q')
    panel = pd.DataFrame({
        'ticker': ['AAPL'] * 4,
        'end': dates,
        'filed': dates + pd.Timedelta(days=45),
        'cfo_ttm': [100, 110, 120, 130],
        'capex_ttm': [20, 22, 24, 26],
        'shares_q': [100, 98, 96, 94],
    })

    with pytest.raises(ValueError, match='No data for ticker MSFT'):
      FundamentalsSlice.from_panel(panel, 'MSFT', dates[-1])

  def test_from_panel_no_pit_data(self):
    """No data as of date raises ValueError."""
    dates = pd.date_range('2022-03-31', periods=4, freq='Q')
    panel = pd.DataFrame({
        'ticker': ['AAPL'] * 4,
        'end': dates,
        'filed': dates + pd.Timedelta(days=45),
        'cfo_ttm': [100, 110, 120, 130],
        'capex_ttm': [20, 22, 24, 26],
        'shares_q': [100, 98, 96, 94],
    })

    early_date = dates[0] - pd.Timedelta(days=365)
    with pytest.raises(ValueError, match='No data for AAPL as of'):
      FundamentalsSlice.from_panel(panel, 'AAPL', early_date)

  def test_from_panel_missing_required_fields(self):
    """Missing required fields raises ValueError."""
    dates = pd.date_range('2022-03-31', periods=4, freq='Q')
    panel = pd.DataFrame({
        'ticker': ['AAPL'] * 4,
        'end': dates,
        'filed': dates + pd.Timedelta(days=45),
        'cfo_ttm': [100, 110, 120, float('nan')],
        'capex_ttm': [20, 22, 24, 26],
        'shares_q': [100, 98, 96, 94],
    })

    as_of = dates[-1] + pd.Timedelta(days=50)
    with pytest.raises(ValueError, match='Missing required data'):
      FundamentalsSlice.from_panel(panel, 'AAPL', as_of)

  def test_pit_filtering(self):
    """PIT filtering excludes future data."""
    dates = pd.date_range('2022-03-31', periods=4, freq='Q')
    panel = pd.DataFrame({
        'ticker': ['AAPL'] * 4,
        'end': dates,
        'filed': dates + pd.Timedelta(days=45),
        'cfo_ttm': [100, 110, 120, 130],
        'capex_ttm': [20, 22, 24, 26],
        'shares_q': [100, 98, 96, 94],
    })

    as_of = dates[2] + pd.Timedelta(days=50)
    result = FundamentalsSlice.from_panel(panel, 'AAPL', as_of)

    assert result.latest_cfo_ttm == 120.0
    assert len(result.cfo_ttm_history) == 3


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
