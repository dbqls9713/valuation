import pandas as pd
import pytest

from valuation.domain.types import FundamentalsSlice
from valuation.domain.types import PolicyOutput
from valuation.domain.types import QuarterData
from valuation.policies.shares import AvgShareChange


def _make_quarters_with_shares(
    start_year: int,
    num_quarters: int,
    shares_values: list[float | None],
) -> list[QuarterData]:
  """Helper to create QuarterData list with shares."""
  quarters: list[QuarterData] = []
  quarter_map = ['Q1', 'Q2', 'Q3', 'Q4']
  end_months = [3, 6, 9, 12]

  for i in range(num_quarters):
    year = start_year + i // 4
    q_idx = i % 4
    qd = QuarterData(
        fiscal_year=year,
        fiscal_quarter=quarter_map[q_idx],
        end=pd.Timestamp(year=year, month=end_months[q_idx], day=28),
        filed=pd.Timestamp(year=year, month=end_months[q_idx], day=28) +
        pd.DateOffset(days=45),
        cfo_ttm=100.0,
        capex_ttm=20.0,
        shares=shares_values[i] if i < len(shares_values) else None,
    )
    quarters.append(qd)
  return quarters


class TestAvgShareChange:
  """Tests for AvgShareChange policy."""

  def test_5year_buyback_scenario(self):
    """Standard 5-year average with buybacks."""
    shares_values = [
        110, 109, 108, 107, 106, 105, 104, 103, 102, 101, 101, 100, 100, 100,
        100, 100, 100, 100, 100, 100
    ]
    quarters = _make_quarters_with_shares(2019, 20, shares_values)

    data = FundamentalsSlice(
        ticker='BUYBACK',
        as_of_date=pd.Timestamp('2024-10-01'),
        quarters=quarters,
    )

    policy = AvgShareChange(years=5)
    result = policy.compute(data)

    assert isinstance(result, PolicyOutput)
    assert result.value == pytest.approx(0.0186, abs=0.005)
    assert result.diag['shares_method'] == 'avg_change'
    assert result.diag['lookback_years'] == 5
    assert result.diag['sh_old'] > result.diag['sh_new']

  def test_increasing_shares_dilution(self):
    """Share dilution scenario (negative buyback rate)."""
    shares_values = [100 + i * 2 for i in range(20)]

    quarters = _make_quarters_with_shares(2019, 20, shares_values)

    data = FundamentalsSlice(
        ticker='DILUTE',
        as_of_date=pd.Timestamp('2024-10-01'),
        quarters=quarters,
    )

    policy = AvgShareChange(years=5)
    result = policy.compute(data)

    assert result.value == pytest.approx(-0.0661, abs=0.005)
    assert result.diag['sh_new'] > result.diag['sh_old']

  def test_decreasing_shares_buyback(self):
    """Share buyback scenario (positive buyback rate)."""
    shares_values = [106, 105, 104, 103, 103, 102, 101, 101, 100, 100, 100, 100]

    quarters = _make_quarters_with_shares(2021, 12, shares_values)

    data = FundamentalsSlice(
        ticker='BUYBACK',
        as_of_date=pd.Timestamp('2024-07-01'),
        quarters=quarters,
    )

    policy = AvgShareChange(years=3)
    result = policy.compute(data)

    assert result.value == pytest.approx(0.0195, abs=0.005)
    assert result.diag['sh_new'] <= result.diag['sh_old']

  def test_insufficient_history(self):
    """Less than 2 years returns insufficient_yearly_data error."""
    shares_values = [100.0, 98.0, 96.0]

    quarters = _make_quarters_with_shares(2023, 3, shares_values)

    data = FundamentalsSlice(
        ticker='SHORT',
        as_of_date=pd.Timestamp('2024-01-01'),
        quarters=quarters,
    )

    policy = AvgShareChange(years=5)
    result = policy.compute(data)

    assert result.value == 0.0
    assert result.diag['error'] == 'insufficient_yearly_data'
    assert result.diag['years_available'] == 1

  def test_no_shares_data(self):
    """No shares data available."""
    quarters = _make_quarters_with_shares(2023, 3, [None, None, None])

    data = FundamentalsSlice(
        ticker='NOSHARES',
        as_of_date=pd.Timestamp('2024-01-01'),
        quarters=quarters,
    )

    policy = AvgShareChange(years=5)
    result = policy.compute(data)

    assert result.value == 0.0
    assert 'error' in result.diag

  def test_custom_lookback_years(self, sample_fundamentals):
    """Custom lookback period."""
    policy = AvgShareChange(years=3)
    result = policy.compute(sample_fundamentals)

    if result.diag.get('error') is None:
      assert result.diag['lookback_years'] == 3

  def test_stable_shares(self):
    """Stable share count returns zero buyback rate."""
    shares_values = [100.0] * 20

    quarters = _make_quarters_with_shares(2019, 20, shares_values)

    data = FundamentalsSlice(
        ticker='STABLE',
        as_of_date=pd.Timestamp('2024-10-01'),
        quarters=quarters,
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
    shares_values = [100.0] * 10 + [-10.0] * 10

    quarters = _make_quarters_with_shares(2019, 20, shares_values)

    data = FundamentalsSlice(
        ticker='INVALID',
        as_of_date=pd.Timestamp('2024-10-01'),
        quarters=quarters,
    )

    policy = AvgShareChange(years=5)
    result = policy.compute(data)

    assert result.value == 0.0
    assert result.diag['error'] == 'invalid_share_values'
