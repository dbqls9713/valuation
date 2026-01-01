import pandas as pd

from valuation.domain.types import FundamentalsSlice
from valuation.domain.types import PolicyOutput
from valuation.domain.types import QuarterData
from valuation.policies.growth import AvgOEGrowth
from valuation.policies.growth import FixedGrowth


def _make_quarters(
    start_year: int,
    num_quarters: int,
    cfo_ttm_values: list[float],
    capex_ttm_values: list[float],
    shares_values: list[float],
) -> list[QuarterData]:
  """Helper to create QuarterData list."""
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
        cfo_ttm=cfo_ttm_values[i],
        capex_ttm=capex_ttm_values[i],
        shares=shares_values[i],
    )
    quarters.append(qd)
  return quarters


class TestFixedGrowth:
  """Tests for FixedGrowth policy."""

  def test_returns_fixed_rate(self):
    """Returns the configured fixed growth rate."""
    quarters = _make_quarters(
        start_year=2020,
        num_quarters=4,
        cfo_ttm_values=[100.0, 110.0, 120.0, 130.0],
        capex_ttm_values=[20.0, 22.0, 24.0, 26.0],
        shares_values=[100.0, 100.0, 100.0, 100.0],
    )
    data = FundamentalsSlice(
        ticker='TEST',
        as_of_date=pd.Timestamp('2021-01-15'),
        quarters=quarters,
    )

    policy = FixedGrowth(growth_rate=0.10)
    result = policy.compute(data)

    assert isinstance(result, PolicyOutput)
    assert result.value == 0.10
    assert result.diag['growth_method'] == 'fixed'
    assert result.diag['growth_rate'] == 0.10

  def test_different_rates(self):
    """Different fixed rates work correctly."""
    quarters = _make_quarters(
        start_year=2020,
        num_quarters=4,
        cfo_ttm_values=[100.0, 110.0, 120.0, 130.0],
        capex_ttm_values=[20.0, 22.0, 24.0, 26.0],
        shares_values=[100.0, 100.0, 100.0, 100.0],
    )
    data = FundamentalsSlice(
        ticker='TEST',
        as_of_date=pd.Timestamp('2021-01-15'),
        quarters=quarters,
    )

    for rate in [0.05, 0.08, 0.12, 0.15]:
      policy = FixedGrowth(growth_rate=rate)
      result = policy.compute(data)
      assert result.value == rate


class TestAvgOEGrowth:
  """Tests for AvgOEGrowth policy."""

  def test_insufficient_data_returns_nan(self):
    """Returns NaN when no Year 3 data (only 1 year of data)."""
    quarters = _make_quarters(
        start_year=2023,
        num_quarters=4,
        cfo_ttm_values=[100.0, 110.0, 120.0, 130.0],
        capex_ttm_values=[20.0, 22.0, 24.0, 26.0],
        shares_values=[100.0, 100.0, 100.0, 100.0],
    )
    data = FundamentalsSlice(
        ticker='TEST',
        as_of_date=pd.Timestamp('2024-01-15'),
        quarters=quarters,
    )

    policy = AvgOEGrowth()
    result = policy.compute(data)

    assert result.value != result.value
    assert result.diag['error'] == 'insufficient_data'
    assert result.diag['year3_n'] == 0

  def test_calculates_cagr_correctly(self):
    """Calculates 3-year CAGR correctly using Year 1 and Year 3 buckets."""
    quarters = _make_quarters(
        start_year=2021,
        num_quarters=16,
        cfo_ttm_values=[100.0] * 16,
        capex_ttm_values=[20.0] * 16,
        shares_values=[100.0] * 16,
    )

    quarters[0] = QuarterData(
        fiscal_year=2021,
        fiscal_quarter='Q1',
        end=pd.Timestamp('2021-03-28'),
        filed=pd.Timestamp('2021-05-12'),
        cfo_ttm=100.0,
        capex_ttm=20.0,
        shares=100.0,
    )
    quarters[-1] = QuarterData(
        fiscal_year=2024,
        fiscal_quarter='Q4',
        end=pd.Timestamp('2024-12-28'),
        filed=pd.Timestamp('2025-02-11'),
        cfo_ttm=160.0,
        capex_ttm=20.0,
        shares=100.0,
    )

    data = FundamentalsSlice(
        ticker='TEST',
        as_of_date=pd.Timestamp('2025-02-15'),
        quarters=quarters,
    )

    policy = AvgOEGrowth()
    result = policy.compute(data)

    assert result.diag['year1_n'] > 0
    assert result.diag['year3_n'] > 0
    assert result.value >= 0

  def test_clips_to_max_growth(self):
    """Clips high growth to max."""
    quarters = _make_quarters(
        start_year=2021,
        num_quarters=16,
        cfo_ttm_values=[50.0] * 4 + [100.0] * 8 + [300.0] * 4,
        capex_ttm_values=[10.0] * 16,
        shares_values=[100.0] * 16,
    )
    data = FundamentalsSlice(
        ticker='TEST',
        as_of_date=pd.Timestamp('2025-02-15'),
        quarters=quarters,
    )

    policy = AvgOEGrowth(max_growth=0.20)
    result = policy.compute(data)

    assert result.value == 0.20
    assert result.diag['raw_cagr'] > 0.20
    assert result.diag['clipped_growth'] == 0.20

  def test_non_positive_oe_returns_nan(self):
    """Returns NaN when OE is non-positive."""
    quarters = _make_quarters(
        start_year=2021,
        num_quarters=16,
        cfo_ttm_values=[50.0] * 16,
        capex_ttm_values=[100.0] * 16,
        shares_values=[100.0] * 16,
    )
    data = FundamentalsSlice(
        ticker='TEST',
        as_of_date=pd.Timestamp('2025-02-15'),
        quarters=quarters,
    )

    policy = AvgOEGrowth()
    result = policy.compute(data)

    assert result.value != result.value
    assert result.diag['error'] == 'non_positive_oe'
