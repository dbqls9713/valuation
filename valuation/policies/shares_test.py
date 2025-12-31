import pandas as pd
import pytest

from valuation.domain.types import FundamentalsSlice
from valuation.domain.types import PolicyOutput
from valuation.policies.shares import AvgShareChange


class TestAvgShareChange:
  """Tests for AvgShareChange policy."""

  def test_5year_buyback_scenario(self):
    """Standard 5-year average with buybacks.

    Shares progression (consistent 2% annual buyback):
    - 5 years ago: 110
    - Latest: 100

    Annual rate: (100/110)^(1/5) - 1 = -0.0186 = -1.86%
    Buyback rate: 1.86%
    """
    dates = pd.date_range('2019-03-31', periods=20, freq='Q')

    # yapf: disable
    # 연도별 마지막 Q: Y1=107, Y2=103, Y3=100, Y4=100, Y5=100
    shares_values = [
        110, 109, 108, 107, 106, 105, 104, 103, 102, 101,
        101, 100, 100, 100, 100, 100, 100, 100, 100, 100,
    ]
    # yapf: enable

    data = FundamentalsSlice(
        ticker='BUYBACK',
        as_of_end=dates[-1],
        filed_cutoff=dates[-1],
        cfo_ttm_history=pd.Series([100] * 20, index=dates),
        capex_ttm_history=pd.Series([20] * 20, index=dates),
        shares_history=pd.Series(shares_values, index=dates),
        latest_cfo_ttm=100.0,
        latest_capex_ttm=20.0,
        latest_shares=100.0,
        latest_filed=dates[-1],
    )

    policy = AvgShareChange(years=5)
    result = policy.compute(data)

    # (100/110)^(1/5) - 1 ≈ -1.86%, so buyback rate ≈ 1.86%
    assert isinstance(result, PolicyOutput)
    assert result.value == pytest.approx(0.0186, abs=0.005)
    assert result.diag['shares_method'] == 'avg_change'
    assert result.diag['lookback_years'] == 5
    assert result.diag['sh_old'] > result.diag['sh_new']

  def test_increasing_shares_dilution(self):
    """Share dilution scenario (negative buyback rate).

    Shares progression (2 per quarter increase):
    - 5 years ago (20 quarters): 100
    - Latest: 100 + 19*2 = 138

    Annual rate: (138/100)^(1/5) - 1 = 0.0661 = 6.61%
    Negative buyback (dilution): -6.61%
    """
    dates = pd.date_range('2019-12-31', periods=20, freq='Q')
    shares_values = [100 + i * 2 for i in range(20)]  # 100, 102, ..., 138

    data = FundamentalsSlice(
        ticker='DILUTE',
        as_of_end=dates[-1],
        filed_cutoff=dates[-1],
        cfo_ttm_history=pd.Series([100] * 20, index=dates),
        capex_ttm_history=pd.Series([20] * 20, index=dates),
        shares_history=pd.Series(shares_values, index=dates),
        latest_cfo_ttm=100.0,
        latest_capex_ttm=20.0,
        latest_shares=138.0,
        latest_filed=dates[-1],
    )

    policy = AvgShareChange(years=5)
    result = policy.compute(data)

    # (138/100)^(1/5) - 1 ≈ 6.61%, so negative buyback
    assert result.value == pytest.approx(-0.0661, abs=0.005)
    assert result.diag['sh_new'] > result.diag['sh_old']

  def test_decreasing_shares_buyback(self):
    """Share buyback scenario (positive buyback rate).

    Shares progression (3% annual buyback):
    - 3 years ago: 106
    - Latest: 100

    Annual rate: (100/106)^(1/3) - 1 = -0.0195 = -1.95%
    Buyback rate: 1.95%
    """
    dates = pd.date_range('2021-03-31', periods=12, freq='Q')
    # 연도별 마지막 Q: Y1=103, Y2=101, Y3=100
    shares_values = [106, 105, 104, 103, 103, 102, 101, 101, 100, 100, 100, 100]

    data = FundamentalsSlice(
        ticker='BUYBACK',
        as_of_end=dates[-1],
        filed_cutoff=dates[-1],
        cfo_ttm_history=pd.Series([100] * 12, index=dates),
        capex_ttm_history=pd.Series([20] * 12, index=dates),
        shares_history=pd.Series(shares_values, index=dates),
        latest_cfo_ttm=100.0,
        latest_capex_ttm=20.0,
        latest_shares=100.0,
        latest_filed=dates[-1],
    )

    policy = AvgShareChange(years=3)
    result = policy.compute(data)

    # (100/106)^(1/3) - 1 ≈ -1.95%
    assert result.value == pytest.approx(0.0195, abs=0.005)
    assert result.diag['sh_new'] <= result.diag['sh_old']

  def test_insufficient_history(self):
    """Less than 5 years requested but has 1 year.

    Calculates with available data.
    """
    dates = pd.date_range('2023-12-31', periods=3, freq='Q')
    data = FundamentalsSlice(
        ticker='SHORT',
        as_of_end=dates[-1],
        filed_cutoff=dates[-1],
        cfo_ttm_history=pd.Series([100, 110, 120], index=dates),
        capex_ttm_history=pd.Series([20, 22, 24], index=dates),
        shares_history=pd.Series([100, 98, 96], index=dates),
        latest_cfo_ttm=120.0,
        latest_capex_ttm=24.0,
        latest_shares=96.0,
        latest_filed=dates[-1],
    )

    policy = AvgShareChange(years=5)
    result = policy.compute(data)

    assert result.value > 0
    assert result.diag['lookback_years'] == 5
    assert result.diag['actual_years'] == 1

  def test_no_shares_data(self):
    """No shares data available."""
    dates = pd.date_range('2023-12-31', periods=3, freq='Q')
    data = FundamentalsSlice(
        ticker='NOSHARES',
        as_of_end=dates[-1],
        filed_cutoff=dates[-1],
        cfo_ttm_history=pd.Series([100, 110, 120], index=dates),
        capex_ttm_history=pd.Series([20, 22, 24], index=dates),
        shares_history=pd.Series([float('nan')] * 3, index=dates),
        latest_cfo_ttm=120.0,
        latest_capex_ttm=24.0,
        latest_shares=float('nan'),
        latest_filed=dates[-1],
    )

    policy = AvgShareChange(years=5)
    result = policy.compute(data)

    assert result.value == 0.0
    assert result.diag['error'] == 'no_shares_data'

  def test_custom_lookback_years(self, sample_fundamentals):
    """Custom lookback period."""
    policy = AvgShareChange(years=3)
    result = policy.compute(sample_fundamentals)

    if result.diag.get('error') is None:
      assert result.diag['lookback_years'] == 3

  def test_stable_shares(self):
    """Stable share count returns zero buyback rate."""
    dates = pd.date_range('2019-12-31', periods=20, freq='Q')
    shares_values = [100] * 20

    data = FundamentalsSlice(
        ticker='STABLE',
        as_of_end=dates[-1],
        filed_cutoff=dates[-1],
        cfo_ttm_history=pd.Series([100] * 20, index=dates),
        capex_ttm_history=pd.Series([20] * 20, index=dates),
        shares_history=pd.Series(shares_values, index=dates),
        latest_cfo_ttm=100.0,
        latest_capex_ttm=20.0,
        latest_shares=100.0,
        latest_filed=dates[-1],
    )

    policy = AvgShareChange(years=5)
    result = policy.compute(data)

    assert result.value == pytest.approx(0.0, abs=1e-6)

  def test_diagnostics_structure(self, sample_fundamentals):
    """Verify diagnostic fields."""
    policy = AvgShareChange(years=5)
    result = policy.compute(sample_fundamentals)

    expected_keys = ['shares_method', 'lookback_years']

    for key in expected_keys:
      assert key in result.diag

    if result.diag.get('error') is None:
      detail_keys = [
          'sh_old', 'sh_new', 'first_year', 'last_year', 'buyback_rate'
      ]
      for key in detail_keys:
        assert key in result.diag

  def test_negative_shares_edge_case(self):
    """Edge case with invalid negative shares."""
    dates = pd.date_range('2019-12-31', periods=20, freq='Q')
    shares_values = [100] * 10 + [-10] * 10

    data = FundamentalsSlice(
        ticker='INVALID',
        as_of_end=dates[-1],
        filed_cutoff=dates[-1],
        cfo_ttm_history=pd.Series([100] * 20, index=dates),
        capex_ttm_history=pd.Series([20] * 20, index=dates),
        shares_history=pd.Series(shares_values, index=dates),
        latest_cfo_ttm=100.0,
        latest_capex_ttm=20.0,
        latest_shares=-10.0,
        latest_filed=dates[-1],
    )

    policy = AvgShareChange(years=5)
    result = policy.compute(data)

    assert result.value == 0.0
    assert result.diag['error'] == 'invalid_share_values'
