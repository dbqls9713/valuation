from valuation.domain.types import PolicyOutput
from valuation.policies.discount import FixedRate


class TestFixedRate:
  """Tests for FixedRate discount policy."""

  def test_basic_usage(self):
    """Basic discount rate policy usage."""
    policy = FixedRate(rate=0.06)
    result = policy.compute()

    assert isinstance(result, PolicyOutput)
    assert result.value == 0.06
    assert result.diag['discount_method'] == 'fixed'
    assert result.diag['discount_rate'] == 0.06

  def test_default_initialization(self):
    """Default initialization to 10%."""
    policy = FixedRate()
    result = policy.compute()

    assert result.value == 0.10

  def test_diagnostics_content(self):
    """Verify diagnostic output structure."""
    policy = FixedRate(rate=0.09)
    result = policy.compute()

    assert 'discount_method' in result.diag
    assert 'discount_rate' in result.diag
    assert isinstance(result.diag, dict)
