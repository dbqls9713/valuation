import pandas as pd
import pytest

from valuation.domain.types import FundamentalsSlice
from valuation.domain.types import QuarterData


def _make_quarters(
    start_year: int,
    num_quarters: int,
    cfo_ttm_values: list[float],
    capex_ttm_values: list[float],
    shares_values: list[float],
    cfo_q_values: list[float] | None = None,
    capex_q_values: list[float] | None = None,
) -> list[QuarterData]:
  """Helper to create QuarterData list from value lists."""
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
        cfo_q=cfo_q_values[i] if cfo_q_values else None,
        capex_q=capex_q_values[i] if capex_q_values else None,
    )
    quarters.append(qd)
  return quarters


@pytest.fixture
def sample_fundamentals() -> FundamentalsSlice:
  """Create a realistic FundamentalsSlice for testing."""
  quarters = _make_quarters(
      start_year=2020,
      num_quarters=12,
      cfo_ttm_values=[100, 110, 120, 130, 140, 150, 160, 170, 180, 190, 200,
                      210],
      capex_ttm_values=[20, 22, 24, 26, 28, 30, 32, 34, 36, 38, 40, 42],
      shares_values=[100, 99, 98, 97, 96, 95, 94, 93, 92, 91, 90, 89],
      cfo_q_values=[25, 28, 30, 33, 35, 38, 40, 43, 45, 48, 50, 53],
      capex_q_values=[5, 6, 6, 7, 7, 8, 8, 9, 9, 10, 10, 11],
  )
  return FundamentalsSlice(
      ticker='TEST',
      as_of_date=pd.Timestamp('2023-02-14'),
      quarters=quarters,
  )


@pytest.fixture
def minimal_fundamentals() -> FundamentalsSlice:
  """Minimal FundamentalsSlice with 3 data points."""
  quarters = _make_quarters(
      start_year=2022,
      num_quarters=3,
      cfo_ttm_values=[100, 110, 120],
      capex_ttm_values=[20, 22, 24],
      shares_values=[100, 98, 96],
      cfo_q_values=[25, 28, 30],
      capex_q_values=[5, 6, 6],
  )
  return FundamentalsSlice(
      ticker='MIN',
      as_of_date=pd.Timestamp('2023-06-30'),
      quarters=quarters,
  )


@pytest.fixture
def declining_fundamentals() -> FundamentalsSlice:
  """FundamentalsSlice with declining metrics."""
  quarters = _make_quarters(
      start_year=2021,
      num_quarters=8,
      cfo_ttm_values=[200, 190, 180, 170, 160, 150, 140, 130],
      capex_ttm_values=[40, 38, 36, 34, 32, 30, 28, 26],
      shares_values=[100, 101, 102, 103, 104, 105, 106, 107],
      cfo_q_values=[50, 48, 45, 43, 40, 38, 35, 33],
      capex_q_values=[10, 10, 9, 9, 8, 8, 7, 7],
  )
  return FundamentalsSlice(
      ticker='DECLINE',
      as_of_date=pd.Timestamp('2023-09-30'),
      quarters=quarters,
  )
