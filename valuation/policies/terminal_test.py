from valuation.domain.types import PolicyOutput
from valuation.policies.terminal import GordonTerminal


class TestGordonTerminal:
  """Tests for GordonTerminal policy."""

  def test_basic_usage(self):
    """Basic terminal growth policy usage."""
    policy = GordonTerminal(g_terminal=0.03)
    result = policy.compute()

    assert isinstance(result, PolicyOutput)
    assert result.value == 0.03
    assert result.diag['terminal_method'] == 'gordon'
    assert result.diag['g_terminal'] == 0.03

  def test_default_initialization(self):
    """Default initialization to 3%."""
    policy = GordonTerminal()
    result = policy.compute()

    assert result.value == 0.03

  def test_diagnostics_content(self):
    """Verify diagnostic output structure."""
    policy = GordonTerminal(g_terminal=0.025)
    result = policy.compute()

    assert 'terminal_method' in result.diag
    assert 'g_terminal' in result.diag
    assert isinstance(result.diag, dict)
