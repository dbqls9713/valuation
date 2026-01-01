"""
CAPEX sign validator.

Validates that CAPEX values follow the absolute value convention (>= 0).
"""
import pandas as pd

from data.shared.validation.base import CheckResult
from data.shared.validation.base import fail_result
from data.shared.validation.base import pass_result


class CapexSignValidator:
  """
  Validate that CAPEX values are non-negative.

  CAPEX is stored as absolute value (positive) by convention.
  """

  def __init__(self, epsilon: float = 1e-9):
    self.epsilon = epsilon

  def validate(self, metrics_q: pd.DataFrame, name: str) -> CheckResult:
    """
    Check that CAPEX q_val values are >= 0.

    Args:
      metrics_q: metrics_quarterly DataFrame
      name: Check name for result

    Returns:
      CheckResult with pass/fail status
    """
    cap = metrics_q[metrics_q['metric'] == 'CAPEX'].copy()

    if cap.empty:
      return pass_result(name, 'No CAPEX rows (skipped)')

    bad_mask = cap['q_val'] < -self.epsilon
    bad_count = int(bad_mask.sum())

    if bad_count > 0:
      sample = cap.loc[bad_mask,
                       ['cik10', 'end', 'q_val']].head(10).to_dict(
                           orient='records')
      return fail_result(
          name, f'{bad_count} CAPEX rows have q_val < 0 '
          f'(eps={self.epsilon}). Sample: {sample}')

    return pass_result(name, f'All CAPEX q_val >= -{self.epsilon}')
