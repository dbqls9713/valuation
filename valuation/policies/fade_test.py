import pytest

from valuation.domain.types import PolicyOutput
from valuation.policies.fade import GeometricFade
from valuation.policies.fade import LinearFade
from valuation.policies.fade import StepThenFade


class TestLinearFade:
  """Tests for LinearFade policy."""

  def test_10year_forecast(self):
    """Linear fade over 10 years."""
    policy = LinearFade(g_end_spread=0.01)
    result = policy.compute(g0=0.10, g_terminal=0.03, n_years=10)

    assert isinstance(result, PolicyOutput)
    assert len(result.value) == 10
    assert result.value[0] == pytest.approx(0.10, rel=1e-6)
    assert result.value[-1] == pytest.approx(0.04, rel=1e-6)
    assert result.diag['fade_method'] == 'linear'
    assert result.diag['g_end'] == 0.04

  def test_3year_forecast(self):
    """Linear fade over short 3-year period."""
    policy = LinearFade(g_end_spread=0.01)
    result = policy.compute(g0=0.08, g_terminal=0.03, n_years=3)

    assert len(result.value) == 3
    assert result.value[0] == pytest.approx(0.08, rel=1e-6)
    assert result.value[-1] == pytest.approx(0.04, rel=1e-6)
    assert result.value[1] > result.value[2]

  def test_to_zero(self):
    """Linear fade to zero terminal growth."""
    policy = LinearFade(g_end_spread=0.0)
    result = policy.compute(g0=0.08, g_terminal=0.0, n_years=5)

    assert len(result.value) == 5
    assert result.value[0] == pytest.approx(0.08, rel=1e-6)
    assert result.value[-1] == pytest.approx(0.0, rel=1e-6)

  def test_single_year(self):
    """Single year forecast returns g0."""
    policy = LinearFade(g_end_spread=0.01)
    result = policy.compute(g0=0.10, g_terminal=0.03, n_years=1)

    assert len(result.value) == 1
    assert result.value[0] == 0.10

  def test_zero_years(self):
    """Zero year forecast returns empty list."""
    policy = LinearFade(g_end_spread=0.01)
    result = policy.compute(g0=0.10, g_terminal=0.03, n_years=0)

    assert len(result.value) == 0

  def test_custom_g_end_spread(self):
    """Custom g_end_spread."""
    policy = LinearFade(g_end_spread=0.02)
    result = policy.compute(g0=0.10, g_terminal=0.03, n_years=5)

    assert result.value[-1] == pytest.approx(0.05, rel=1e-6)
    assert result.diag['g_end'] == 0.05

  def test_decreasing_sequence(self):
    """Growth rates decrease monotonically."""
    policy = LinearFade(g_end_spread=0.01)
    result = policy.compute(g0=0.12, g_terminal=0.03, n_years=8)

    for i in range(len(result.value) - 1):
      assert result.value[i] >= result.value[i + 1]


class TestGeometricFade:
  """Tests for GeometricFade policy."""

  def test_10year_geometric_fade(self):
    """Geometric fade over 10 years."""
    policy = GeometricFade(g_end_spread=0.01)
    result = policy.compute(g0=0.10, g_terminal=0.03, n_years=10)

    assert isinstance(result, PolicyOutput)
    assert len(result.value) == 10
    assert result.value[0] == pytest.approx(0.10, rel=1e-6)
    assert result.value[-1] == pytest.approx(0.04, rel=1e-6)
    assert result.diag['fade_method'] == 'geometric'
    assert 'decay_ratio' in result.diag

  def test_geometric_faster_initial_drop(self):
    """Geometric fade drops faster initially than linear."""
    linear = LinearFade(g_end_spread=0.01)
    geometric = GeometricFade(g_end_spread=0.01)

    linear_result = linear.compute(g0=0.10, g_terminal=0.03, n_years=10)
    geometric_result = geometric.compute(g0=0.10, g_terminal=0.03, n_years=10)

    assert geometric_result.value[3] < linear_result.value[3]

  def test_single_year_geometric(self):
    """Single year geometric returns g0."""
    policy = GeometricFade(g_end_spread=0.01)
    result = policy.compute(g0=0.10, g_terminal=0.03, n_years=1)

    assert len(result.value) == 1
    assert result.value[0] == 0.10

  def test_zero_years_geometric(self):
    """Zero years returns empty list."""
    policy = GeometricFade(g_end_spread=0.01)
    result = policy.compute(g0=0.10, g_terminal=0.03, n_years=0)

    assert len(result.value) == 0

  def test_negative_growth_fallback(self):
    """Negative growth falls back to linear."""
    policy = GeometricFade(g_end_spread=0.01)
    result = policy.compute(g0=-0.05, g_terminal=0.03, n_years=5)

    assert len(result.value) == 5
    assert result.diag['fade_method'] == 'linear'

  def test_zero_g0_fallback(self):
    """Zero g0 falls back to linear."""
    policy = GeometricFade(g_end_spread=0.01)
    result = policy.compute(g0=0.0, g_terminal=0.03, n_years=5)

    assert len(result.value) == 5
    assert result.diag['fade_method'] == 'linear'

  def test_decay_ratio_calculation(self):
    """Verify decay ratio is calculated correctly."""
    policy = GeometricFade(g_end_spread=0.0)
    result = policy.compute(g0=0.08, g_terminal=0.02, n_years=5)

    expected_ratio = (0.02 / 0.08)**(1.0 / 4)
    assert result.diag['decay_ratio'] == pytest.approx(expected_ratio, rel=1e-6)


class TestStepThenFade:
  """Tests for StepThenFade policy."""

  def test_10year_with_5year_step(self):
    """10-year forecast with 5-year high growth."""
    policy = StepThenFade(high_growth_years=5, g_end_spread=0.01)
    result = policy.compute(g0=0.15, g_terminal=0.03, n_years=10)

    assert isinstance(result, PolicyOutput)
    assert len(result.value) == 10

    for i in range(5):
      assert result.value[i] == 0.15

    assert result.value[-1] == pytest.approx(0.04, rel=1e-6)
    assert result.diag['fade_method'] == 'step_then_fade'
    assert result.diag['high_growth_years'] == 5
    assert result.diag['fade_years'] == 5

  def test_3year_with_2year_step(self):
    """Short forecast with step."""
    policy = StepThenFade(high_growth_years=2, g_end_spread=0.01)
    result = policy.compute(g0=0.12, g_terminal=0.03, n_years=3)

    assert len(result.value) == 3
    assert result.value[0] == 0.12
    assert result.value[1] == 0.12
    assert result.value[2] < 0.12

  def test_step_exceeds_n_years(self):
    """High growth years exceeds forecast period."""
    policy = StepThenFade(high_growth_years=10, g_end_spread=0.01)
    result = policy.compute(g0=0.15, g_terminal=0.03, n_years=5)

    assert len(result.value) == 5
    for g in result.value:
      assert g == 0.15
    assert result.diag['high_growth_years'] == 5
    assert result.diag['fade_years'] == 0

  def test_zero_high_growth_years(self):
    """Zero high growth years is immediate fade."""
    policy = StepThenFade(high_growth_years=0, g_end_spread=0.01)
    result = policy.compute(g0=0.10, g_terminal=0.03, n_years=5)

    assert len(result.value) == 5
    assert result.value[0] < 0.10
    assert result.value[-1] == pytest.approx(0.04, rel=1e-6)

  def test_single_year_step(self):
    """Single year returns g0."""
    policy = StepThenFade(high_growth_years=3, g_end_spread=0.01)
    result = policy.compute(g0=0.15, g_terminal=0.03, n_years=1)

    assert len(result.value) == 1
    assert result.value[0] == 0.15

  def test_zero_years_step(self):
    """Zero years returns empty."""
    policy = StepThenFade(high_growth_years=3, g_end_spread=0.01)
    result = policy.compute(g0=0.15, g_terminal=0.03, n_years=0)

    assert len(result.value) == 0

  def test_diagnostics_structure(self):
    """Verify diagnostic fields."""
    policy = StepThenFade(high_growth_years=3, g_end_spread=0.02)
    result = policy.compute(g0=0.12, g_terminal=0.03, n_years=7)

    assert result.diag['high_growth_years'] == 3
    assert result.diag['fade_years'] == 4
    assert result.diag['g_end'] == 0.05
    assert result.diag['g_end_spread'] == 0.02
