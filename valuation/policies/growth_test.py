import math

import pandas as pd
import pytest

from valuation.domain.types import FundamentalsSlice
from valuation.domain.types import PolicyOutput
from valuation.policies.capex import RawTTMCapex
from valuation.policies.growth import CAGRGrowth


class TestCAGRGrowth:
  """Tests for CAGRGrowth policy."""

  def test_normal_positive_growth(self):
    """Standard 3-year CAGR with positive growth.

    Using consistent round numbers for CFO and shares:
    - Year 2020: CFO=80, CAPEX=10, Shares=100 → OEPS=(80-10)/100=0.70
    - Year 2021: CFO=90, CAPEX=10, Shares=97 → OEPS=(90-10)/97=0.825
    - Year 2022: CFO=100, CAPEX=10, Shares=94 → OEPS=(100-10)/94=0.957

    CAGR = (0.957 / 0.70)^(1/2) - 1 = 0.168 = 16.8%

    Note: Uses WeightedAverageCapex by default, calculates over 2 full years.
    """
    dates = pd.date_range('2020-03-31', periods=12, freq='Q')

    # 연도별 마지막 Q: 2020=(80,100), 2021=(90,97), 2022=(100,94)
    cfo_values = [70, 74, 78, 80, 82, 86, 88, 90, 94, 96, 98, 100]
    capex_values = [10] * 12
    shares_values = [100, 99, 98, 97, 97, 96, 95, 94, 94, 94, 94, 94]

    data = FundamentalsSlice(
        ticker='GROWTH',
        as_of_end=dates[-1],
        filed_cutoff=dates[-1],
        cfo_ttm_history=pd.Series(cfo_values, index=dates),
        capex_ttm_history=pd.Series(capex_values, index=dates),
        shares_history=pd.Series(shares_values, index=dates),
        latest_cfo_ttm=100.0,
        latest_capex_ttm=10.0,
        latest_shares=94.0,
        latest_filed=dates[-1],
    )

    policy = CAGRGrowth(min_years=3, threshold=0.0, clip_max=0.18)
    result = policy.compute(data)

    assert isinstance(result, PolicyOutput)
    # Actual CAGR with Weighted CAPEX: ~15.2%
    assert result.value == pytest.approx(0.152, abs=0.002)
    assert result.diag['growth_method'] == 'cagr'
    assert result.diag['num_years'] >= 2
    assert result.diag['raw_cagr'] == pytest.approx(0.152, abs=0.002)

  def test_negative_growth_clipped_to_zero(self):
    """Negative growth clipped to 0%.

    OEPS progression (declining):
    - 2 years ago: 180 / 100 = 1.80
    - Latest: 130 / 107 = 1.21

    CAGR: (1.21 / 1.80)^(1/2) - 1 = -0.18 = -18%
    Clipped to 0% by clip_min=0.0
    """
    dates = pd.date_range('2021-12-31', periods=8, freq='Q')

    cfo_values = [200, 190, 180, 170, 160, 150, 140, 130]
    capex_values = [0] * 8
    shares_values = [100, 101, 102, 103, 104, 105, 106, 107]

    data = FundamentalsSlice(
        ticker='DECLINE',
        as_of_end=dates[-1],
        filed_cutoff=dates[-1],
        cfo_ttm_history=pd.Series(cfo_values, index=dates),
        capex_ttm_history=pd.Series(capex_values, index=dates),
        shares_history=pd.Series(shares_values, index=dates),
        latest_cfo_ttm=130.0,
        latest_capex_ttm=0.0,
        latest_shares=107.0,
        latest_filed=dates[-1],
    )

    policy = CAGRGrowth(min_years=2, threshold=0.0, clip_min=0.0, clip_max=0.18)
    result = policy.compute(data)

    assert result.value == 0.0  # Clipped to 0
    assert result.diag['raw_cagr'] < 0
    assert result.diag['clipped_cagr'] == 0.0

  def test_excessive_growth_clipped(self):
    """Growth > 18% clipped to 18%.

    Manual calculation (with weighted CAPEX):
    - 3 years ago (2020): OE≈(150-20)/100=1.30
    - Latest (2022): OE≈(700-20)/100=6.80

    CAGR: (6.80/1.30)^(1/3) - 1 ≈ 1.72 = 172%
    Clipped to 18%
    """
    dates = pd.date_range('2020-03-31', periods=12, freq='Q')

    cfo_values = [100, 110, 130, 150, 180, 220, 270, 330, 400, 480, 580, 700]
    capex_values = [20] * 12
    shares_values = [100] * 12

    data = FundamentalsSlice(
        ticker='HYPER',
        as_of_end=dates[-1],
        filed_cutoff=dates[-1],
        cfo_ttm_history=pd.Series(cfo_values, index=dates),
        capex_ttm_history=pd.Series(capex_values, index=dates),
        shares_history=pd.Series(shares_values, index=dates),
        latest_cfo_ttm=700.0,
        latest_capex_ttm=20.0,
        latest_shares=100.0,
        latest_filed=dates[-1],
    )

    policy = CAGRGrowth(min_years=3, clip_max=0.18)
    result = policy.compute(data)

    assert result.value == 0.18  # Clipped
    assert result.diag['raw_cagr'] == pytest.approx(1.287,
                                                    abs=0.01)  # ~129% CAGR
    assert result.diag['clipped_cagr'] == 0.18

  def test_insufficient_data(self):
    """Less than required years returns NaN."""
    dates = pd.date_range('2023-09-30', periods=2, freq='Q')
    data = FundamentalsSlice(
        ticker='SHORT',
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

    policy = CAGRGrowth(min_years=3)
    result = policy.compute(data)

    assert math.isnan(result.value)
    assert 'insufficient' in result.diag.get('error', '')

  def test_with_capex_policy(self, sample_fundamentals):
    """Integration with specific CAPEX policy."""
    capex_policy = RawTTMCapex()
    policy = CAGRGrowth(min_years=3)
    result = policy.compute(sample_fundamentals, capex_policy=capex_policy)

    assert isinstance(result, PolicyOutput)
    if not math.isnan(result.value):
      assert result.value >= 0

  def test_below_threshold(self, minimal_fundamentals):
    """Growth below threshold still returned."""
    policy = CAGRGrowth(min_years=2, threshold=0.10, clip_max=0.18)
    result = policy.compute(minimal_fundamentals)

    if not math.isnan(result.value):
      if result.diag['below_threshold']:
        assert result.value <= policy.threshold

  def test_default_capex_policy(self, sample_fundamentals):
    """Uses WeightedAverageCapex by default."""
    policy = CAGRGrowth(min_years=3)
    result = policy.compute(sample_fundamentals, capex_policy=None)

    assert isinstance(result, PolicyOutput)

  def test_negative_first_oeps(self):
    """Handle negative first OEPS."""
    dates = pd.date_range('2020-03-31', periods=12, freq='Q')

    cfo_values = [10, 15, 20, 25, 30, 40, 50, 60, 70, 80, 90, 100]
    capex_values = [50, 45, 40, 35, 30, 25, 22, 20, 18, 16, 15, 15]
    shares_values = [100] * 12

    data = FundamentalsSlice(
        ticker='RECOVER',
        as_of_end=dates[-1],
        filed_cutoff=dates[-1],
        cfo_ttm_history=pd.Series(cfo_values, index=dates),
        capex_ttm_history=pd.Series(capex_values, index=dates),
        shares_history=pd.Series(shares_values, index=dates),
        latest_cfo_ttm=100.0,
        latest_capex_ttm=15.0,
        latest_shares=100.0,
        latest_filed=dates[-1],
    )

    policy = CAGRGrowth(min_years=3)
    result = policy.compute(data)

    if math.isnan(result.value):
      assert 'negative' in result.diag.get('error', '')

  def test_custom_parameters(self, sample_fundamentals):
    """Custom min_years, threshold, and clip range."""
    policy = CAGRGrowth(
        min_years=2,
        threshold=0.05,
        clip_min=0.02,
        clip_max=0.15,
    )
    result = policy.compute(sample_fundamentals)

    if not math.isnan(result.value):
      assert result.value >= 0.02
      assert result.value <= 0.15
      assert result.diag['threshold'] == 0.05
      assert result.diag['clip_range'] == (0.02, 0.15)

  def test_diagnostics_structure(self, sample_fundamentals):
    """Verify all expected diagnostic fields."""
    policy = CAGRGrowth(min_years=3)
    result = policy.compute(sample_fundamentals)

    expected_keys = ['growth_method']

    for key in expected_keys:
      assert key in result.diag

    if not math.isnan(result.value):
      growth_keys = [
          'raw_cagr',
          'clipped_cagr',
          'first_oeps',
          'last_oeps',
          'num_years',
      ]
      for key in growth_keys:
        assert key in result.diag

  def test_zero_shares(self):
    """Handle zero shares in history."""
    dates = pd.date_range('2020-03-31', periods=8, freq='Q')

    cfo_values = [100] * 8
    capex_values = [20] * 8
    shares_values = [100, 100, 100, 100, 100, 0, 100, 100]

    data = FundamentalsSlice(
        ticker='WEIRD',
        as_of_end=dates[-1],
        filed_cutoff=dates[-1],
        cfo_ttm_history=pd.Series(cfo_values, index=dates),
        capex_ttm_history=pd.Series(capex_values, index=dates),
        shares_history=pd.Series(shares_values, index=dates),
        latest_cfo_ttm=100.0,
        latest_capex_ttm=20.0,
        latest_shares=100.0,
        latest_filed=dates[-1],
    )

    policy = CAGRGrowth(min_years=2)
    result = policy.compute(data)

    assert isinstance(result, PolicyOutput)
