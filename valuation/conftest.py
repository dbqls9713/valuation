import pandas as pd
import pytest

from valuation.domain.types import FundamentalsSlice


@pytest.fixture
def sample_fundamentals() -> FundamentalsSlice:
  """Create a realistic FundamentalsSlice for testing."""
  return FundamentalsSlice(
      ticker='TEST',
      as_of_end=pd.Timestamp('2022-12-31'),
      filed_cutoff=pd.Timestamp('2023-02-14'),
      cfo_ttm_history=pd.Series(
          [100, 110, 120, 130, 140, 150, 160, 170, 180, 190, 200, 210],
          index=pd.date_range('2020-03-31', periods=12, freq='Q'),
      ),
      cfo_q_history=pd.Series(
          [25, 28, 30, 33, 35, 38, 40, 43, 45, 48, 50, 53],
          index=pd.date_range('2020-03-31', periods=12, freq='Q'),
      ),
      capex_ttm_history=pd.Series(
          [20, 22, 24, 26, 28, 30, 32, 34, 36, 38, 40, 42],
          index=pd.date_range('2020-03-31', periods=12, freq='Q'),
      ),
      capex_q_history=pd.Series(
          [5, 6, 6, 7, 7, 8, 8, 9, 9, 10, 10, 11],
          index=pd.date_range('2020-03-31', periods=12, freq='Q'),
      ),
      shares_history=pd.Series(
          [100, 99, 98, 97, 96, 95, 94, 93, 92, 91, 90, 89],
          index=pd.date_range('2020-03-31', periods=12, freq='Q'),
      ),
      latest_cfo_ttm=210.0,
      latest_capex_ttm=42.0,
      latest_shares=89.0,
      latest_filed=pd.Timestamp('2023-02-14'),
  )


@pytest.fixture
def minimal_fundamentals() -> FundamentalsSlice:
  """Minimal FundamentalsSlice with 3 data points."""
  return FundamentalsSlice(
      ticker='MIN',
      as_of_end=pd.Timestamp('2023-06-30'),
      filed_cutoff=pd.Timestamp('2023-06-30'),
      cfo_ttm_history=pd.Series(
          [100, 110, 120],
          index=pd.date_range('2022-12-31', periods=3, freq='Q'),
      ),
      cfo_q_history=pd.Series(
          [25, 28, 30],
          index=pd.date_range('2022-12-31', periods=3, freq='Q'),
      ),
      capex_ttm_history=pd.Series(
          [20, 22, 24],
          index=pd.date_range('2022-12-31', periods=3, freq='Q'),
      ),
      capex_q_history=pd.Series(
          [5, 6, 6],
          index=pd.date_range('2022-12-31', periods=3, freq='Q'),
      ),
      shares_history=pd.Series(
          [100, 98, 96],
          index=pd.date_range('2022-12-31', periods=3, freq='Q'),
      ),
      latest_cfo_ttm=120.0,
      latest_capex_ttm=24.0,
      latest_shares=96.0,
      latest_filed=pd.Timestamp('2023-06-30'),
  )


@pytest.fixture
def declining_fundamentals() -> FundamentalsSlice:
  """FundamentalsSlice with declining metrics."""
  return FundamentalsSlice(
      ticker='DECLINE',
      as_of_end=pd.Timestamp('2023-09-30'),
      filed_cutoff=pd.Timestamp('2023-09-30'),
      cfo_ttm_history=pd.Series(
          [200, 190, 180, 170, 160, 150, 140, 130],
          index=pd.date_range('2021-12-31', periods=8, freq='Q'),
      ),
      cfo_q_history=pd.Series(
          [50, 48, 45, 43, 40, 38, 35, 33],
          index=pd.date_range('2021-12-31', periods=8, freq='Q'),
      ),
      capex_ttm_history=pd.Series(
          [40, 38, 36, 34, 32, 30, 28, 26],
          index=pd.date_range('2021-12-31', periods=8, freq='Q'),
      ),
      capex_q_history=pd.Series(
          [10, 10, 9, 9, 8, 8, 7, 7],
          index=pd.date_range('2021-12-31', periods=8, freq='Q'),
      ),
      shares_history=pd.Series(
          [100, 101, 102, 103, 104, 105, 106, 107],
          index=pd.date_range('2021-12-31', periods=8, freq='Q'),
      ),
      latest_cfo_ttm=130.0,
      latest_capex_ttm=26.0,
      latest_shares=107.0,
      latest_filed=pd.Timestamp('2023-09-30'),
  )
