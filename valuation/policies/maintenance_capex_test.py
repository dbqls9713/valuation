import pandas as pd

from valuation.domain.types import FundamentalsSlice
from valuation.domain.types import PolicyOutput
from valuation.policies.maintenance_capex import TTMCapex


class TestTTMCapex:
  """Tests for TTMCapex policy."""

  def test_returns_absolute_value(self):
    """Returns absolute value of latest CAPEX."""
    dates = pd.date_range('2022-12-31', periods=4, freq='Q')
    data = FundamentalsSlice(
        ticker='TEST',
        as_of_end=dates[-1],
        filed_cutoff=dates[-1],
        cfo_ttm_history=pd.Series([100, 110, 120, 130], index=dates),
        capex_ttm_history=pd.Series([20, 22, 24, 26], index=dates),
        shares_history=pd.Series([100, 100, 100, 100], index=dates),
        latest_cfo_ttm=130.0,
        latest_capex_ttm=26.0,
        latest_shares=100.0,
        latest_filed=dates[-1],
    )

    policy = TTMCapex()
    result = policy.compute(data)

    assert isinstance(result, PolicyOutput)
    assert result.value == 26.0
    assert result.diag['maint_capex_method'] == 'ttm'
    assert result.diag['maint_capex'] == 26.0

  def test_negative_capex_converted_to_positive(self):
    """Handles negative CAPEX values."""
    dates = pd.date_range('2022-12-31', periods=4, freq='Q')
    data = FundamentalsSlice(
        ticker='TEST',
        as_of_end=dates[-1],
        filed_cutoff=dates[-1],
        cfo_ttm_history=pd.Series([100, 110, 120, 130], index=dates),
        capex_ttm_history=pd.Series([-20, -22, -24, -26], index=dates),
        shares_history=pd.Series([100, 100, 100, 100], index=dates),
        latest_cfo_ttm=130.0,
        latest_capex_ttm=-26.0,
        latest_shares=100.0,
        latest_filed=dates[-1],
    )

    policy = TTMCapex()
    result = policy.compute(data)

    assert result.value == 26.0
    assert result.diag['capex_ttm'] == -26.0
