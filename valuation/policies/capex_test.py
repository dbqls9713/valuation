import math

import pandas as pd
import pytest

from valuation.domain.types import FundamentalsSlice
from valuation.domain.types import PolicyOutput
from valuation.policies.capex import IntensityClippedCapex
from valuation.policies.capex import RawTTMCapex
from valuation.policies.capex import WeightedAverageCapex


class TestRawTTMCapex:
  """Tests for RawTTMCapex policy."""

  def test_normal_case(self, sample_fundamentals):
    """Returns absolute value of latest CAPEX."""
    policy = RawTTMCapex()
    result = policy.compute(sample_fundamentals)

    assert isinstance(result, PolicyOutput)
    assert result.value == 42.0
    assert result.diag['capex_method'] == 'raw_ttm'
    assert result.diag['capex_raw_ttm'] == 42.0

  def test_negative_capex(self):
    """Handles negative CAPEX (converts to positive)."""
    dates = pd.date_range('2022-12-31', periods=3, freq='Q')
    data = FundamentalsSlice(
        ticker='TEST',
        as_of_end=dates[-1],
        filed_cutoff=dates[-1],
        cfo_ttm_history=pd.Series([100, 110, 120], index=dates),
        capex_ttm_history=pd.Series([-20, -22, -24], index=dates),
        shares_history=pd.Series([100, 98, 96], index=dates),
        latest_cfo_ttm=120.0,
        latest_capex_ttm=-24.0,
        latest_shares=96.0,
        latest_filed=dates[-1],
    )

    policy = RawTTMCapex()
    result = policy.compute(data)

    assert result.value == 24.0
    assert result.diag['capex_raw_ttm'] == -24.0

  def test_diagnostics_content(self, sample_fundamentals):
    """Verify diagnostic output structure."""
    policy = RawTTMCapex()
    result = policy.compute(sample_fundamentals)

    assert 'capex_method' in result.diag
    assert 'capex_raw_ttm' in result.diag
    assert isinstance(result.diag, dict)


class TestWeightedAverageCapex:
  """Tests for WeightedAverageCapex policy."""

  def test_3year_normal(self):
    """Standard 3-year weighted average with default weights [1, 2, 3].

    Using simple round numbers for easy calculation:
    - Year 2020 (last Q): 10
    - Year 2021 (last Q): 20
    - Year 2022 (last Q): 30

    Expected: (10*1 + 20*2 + 30*3) / (1+2+3) = 140 / 6 = 23.333...
    """
    dates = pd.date_range('2020-03-31', periods=12, freq='Q')
    # 연도별 마지막 Q: 2020=10, 2021=20, 2022=30
    capex_values = [7, 8, 9, 10, 14, 16, 18, 20, 24, 26, 28, 30]

    data = FundamentalsSlice(
        ticker='TEST',
        as_of_end=dates[-1],
        filed_cutoff=dates[-1],
        cfo_ttm_history=pd.Series([100] * 12, index=dates),
        capex_ttm_history=pd.Series(capex_values, index=dates),
        shares_history=pd.Series([100] * 12, index=dates),
        latest_cfo_ttm=100.0,
        latest_capex_ttm=30.0,
        latest_shares=100.0,
        latest_filed=dates[-1],
    )

    policy = WeightedAverageCapex(years=3)
    result = policy.compute(data)

    expected = (10 * 1 + 20 * 2 + 30 * 3) / 6  # 23.333...
    assert isinstance(result, PolicyOutput)
    assert result.value == pytest.approx(expected, rel=1e-6)
    assert result.diag['capex_method'] == 'weighted_avg'
    assert result.diag['years_used'] == 3
    assert result.diag['weights'] == [1, 2, 3]

  def test_custom_weights(self):
    """Custom weights [2, 3, 5] for different weighting scheme.

    Using simple round numbers:
    - Year 2020 (last Q): 12
    - Year 2021 (last Q): 18
    - Year 2022 (last Q): 24

    Expected: (12*2 + 18*3 + 24*5) / (2+3+5) = 198 / 10 = 19.8
    """
    dates = pd.date_range('2020-03-31', periods=12, freq='Q')
    # 연도별 마지막 Q: 2020=12, 2021=18, 2022=24
    capex_values = [8, 9, 10, 12, 13, 15, 16, 18, 19, 21, 22, 24]

    data = FundamentalsSlice(
        ticker='TEST',
        as_of_end=dates[-1],
        filed_cutoff=dates[-1],
        cfo_ttm_history=pd.Series([100] * 12, index=dates),
        capex_ttm_history=pd.Series(capex_values, index=dates),
        shares_history=pd.Series([100] * 12, index=dates),
        latest_cfo_ttm=100.0,
        latest_capex_ttm=24.0,
        latest_shares=100.0,
        latest_filed=dates[-1],
    )

    policy = WeightedAverageCapex(years=3, weights=[2, 3, 5])
    result = policy.compute(data)

    expected = (12 * 2 + 18 * 3 + 24 * 5) / 10  # 19.8
    assert result.value == pytest.approx(expected, rel=1e-6)
    assert result.diag['weights'] == [2, 3, 5]

  def test_insufficient_data(self):
    """Less than 3 years available - uses 2 years with weights [1, 2].

    Two years of data (2023, 2024):
    - Year 2023 (last Q): 20
    - Year 2024 (last Q): 22

    Expected: (20*1 + 22*2) / (1+2) = 64 / 3 = 21.333...
    """
    dates = pd.date_range('2023-12-31', periods=2, freq='Q')
    data = FundamentalsSlice(
        ticker='TEST',
        as_of_end=dates[-1],
        filed_cutoff=dates[-1],
        cfo_ttm_history=pd.Series([100, 110], index=dates),
        capex_ttm_history=pd.Series([20, 22], index=dates),
        shares_history=pd.Series([100, 98], index=dates),
        latest_cfo_ttm=110.0,
        latest_capex_ttm=22.0,
        latest_shares=98.0,
        latest_filed=dates[-1],
    )

    policy = WeightedAverageCapex(years=3)
    result = policy.compute(data)

    expected = (20 * 1 + 22 * 2) / 3  # 21.333...
    assert result.value == pytest.approx(expected, rel=1e-6)
    assert result.diag['capex_method'] == 'weighted_avg'
    assert result.diag['years_used'] == 2
    assert result.diag['weights'] == [1, 2]

  def test_with_nan_values(self):
    """Handle NaN values in history."""
    dates = pd.date_range('2020-03-31', periods=12, freq='Q')
    capex_values = [20, 22, float('nan'), 26, 28, 30, 32, 34, 36, 38, 40, 42]

    data = FundamentalsSlice(
        ticker='TEST',
        as_of_end=dates[-1],
        filed_cutoff=dates[-1],
        cfo_ttm_history=pd.Series([100] * 12, index=dates),
        capex_ttm_history=pd.Series(capex_values, index=dates),
        shares_history=pd.Series([100] * 12, index=dates),
        latest_cfo_ttm=100.0,
        latest_capex_ttm=42.0,
        latest_shares=100.0,
        latest_filed=dates[-1],
    )

    policy = WeightedAverageCapex(years=3)
    result = policy.compute(data)

    assert result.value > 0
    assert math.isfinite(result.value)

  def test_empty_capex_history(self):
    """No CAPEX data available."""
    dates = pd.date_range('2023-12-31', periods=3, freq='Q')
    data = FundamentalsSlice(
        ticker='TEST',
        as_of_end=dates[-1],
        filed_cutoff=dates[-1],
        cfo_ttm_history=pd.Series([100, 110, 120], index=dates),
        capex_ttm_history=pd.Series([float('nan')] * 3, index=dates),
        shares_history=pd.Series([100, 98, 96], index=dates),
        latest_cfo_ttm=120.0,
        latest_capex_ttm=float('nan'),
        latest_shares=96.0,
        latest_filed=dates[-1],
    )

    policy = WeightedAverageCapex(years=3)
    result = policy.compute(data)

    assert math.isnan(result.value)
    assert result.diag['error'] == 'no_data'

  def test_diagnostic_yearly_values(self, sample_fundamentals):
    """Verify yearly_values in diagnostics."""
    policy = WeightedAverageCapex(years=3)
    result = policy.compute(sample_fundamentals)

    if result.diag['capex_method'] == 'weighted_avg':
      assert 'yearly_values' in result.diag
      assert isinstance(result.diag['yearly_values'], dict)


class TestIntensityClippedCapex:
  """Tests for IntensityClippedCapex policy."""

  def test_normal_case_no_clipping(self, sample_fundamentals):
    """CAPEX intensity within historical bounds."""
    policy = IntensityClippedCapex(percentile=90)
    result = policy.compute(sample_fundamentals)

    assert isinstance(result, PolicyOutput)
    assert result.value > 0
    assert result.diag['capex_method'] == 'intensity_clipped'
    assert 'clipping_applied' in result.diag

  def test_clipping_applied(self):
    """CAPEX intensity exceeds threshold - should clip."""
    dates = pd.date_range('2020-03-31', periods=12, freq='Q')

    cfo_values = [100] * 11 + [100]
    capex_values = [20] * 11 + [80]

    data = FundamentalsSlice(
        ticker='TEST',
        as_of_end=dates[-1],
        filed_cutoff=dates[-1],
        cfo_ttm_history=pd.Series(cfo_values, index=dates),
        capex_ttm_history=pd.Series(capex_values, index=dates),
        shares_history=pd.Series([100] * 12, index=dates),
        latest_cfo_ttm=100.0,
        latest_capex_ttm=80.0,
        latest_shares=100.0,
        latest_filed=dates[-1],
    )

    policy = IntensityClippedCapex(percentile=90, reduction_factor=0.5)
    result = policy.compute(data)

    assert result.value < 80.0
    assert result.diag['clipping_applied'] is True
    assert result.diag['capex_raw'] == 80.0

  def test_clipping_within_bounds(self):
    """CAPEX intensity below threshold - no clipping."""
    dates = pd.date_range('2020-03-31', periods=12, freq='Q')

    cfo_values = [100] * 12
    capex_values = [20] * 12

    data = FundamentalsSlice(
        ticker='TEST',
        as_of_end=dates[-1],
        filed_cutoff=dates[-1],
        cfo_ttm_history=pd.Series(cfo_values, index=dates),
        capex_ttm_history=pd.Series(capex_values, index=dates),
        shares_history=pd.Series([100] * 12, index=dates),
        latest_cfo_ttm=100.0,
        latest_capex_ttm=20.0,
        latest_shares=100.0,
        latest_filed=dates[-1],
    )

    policy = IntensityClippedCapex(percentile=90)
    result = policy.compute(data)

    assert result.value == 20.0
    assert result.diag['clipping_applied'] is False

  def test_insufficient_history(self):
    """Less than 5 quarters - should fallback."""
    dates = pd.date_range('2023-12-31', periods=3, freq='Q')
    data = FundamentalsSlice(
        ticker='TEST',
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

    policy = IntensityClippedCapex()
    result = policy.compute(data)

    assert result.value == 24.0
    assert 'fallback' in result.diag['capex_method']
    assert 'insufficient' in result.diag['reason']

  def test_negative_cfo(self):
    """Current CFO is negative - skip clipping."""
    dates = pd.date_range('2020-03-31', periods=12, freq='Q')

    cfo_values = [100] * 11 + [-50]
    capex_values = [20] * 12

    data = FundamentalsSlice(
        ticker='TEST',
        as_of_end=dates[-1],
        filed_cutoff=dates[-1],
        cfo_ttm_history=pd.Series(cfo_values, index=dates),
        capex_ttm_history=pd.Series(capex_values, index=dates),
        shares_history=pd.Series([100] * 12, index=dates),
        latest_cfo_ttm=-50.0,
        latest_capex_ttm=20.0,
        latest_shares=100.0,
        latest_filed=dates[-1],
    )

    policy = IntensityClippedCapex()
    result = policy.compute(data)

    assert result.value == 20.0
    assert result.diag['reason'] == 'negative_cfo'

  def test_custom_parameters(self, sample_fundamentals):
    """Custom percentile and reduction factor."""
    policy = IntensityClippedCapex(
        percentile=75,
        reduction_factor=0.3,
        lookback_quarters=10,
    )
    result = policy.compute(sample_fundamentals)

    assert result.value > 0
    assert result.diag['percentile_threshold'] == 75

  def test_diagnostics_structure(self, sample_fundamentals):
    """Verify all expected diagnostic fields."""
    policy = IntensityClippedCapex()
    result = policy.compute(sample_fundamentals)

    expected_keys = [
        'capex_method',
        'current_intensity',
        'clipping_applied',
        'capex_raw',
        'capex_clipped',
    ]

    for key in expected_keys:
      assert key in result.diag
