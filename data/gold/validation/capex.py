"""
CAPEX sign validator for Gold panels.

Validates that CAPEX values are positive (after abs() transformation).
"""
import pandas as pd

from data.shared.validation.base import CheckResult
from data.shared.validation.base import fail_result
from data.shared.validation.base import pass_result


class CapexSignValidator:
  """
  Validate that CAPEX values in Gold panels are positive.

  Gold panels store CAPEX as absolute values in capex_ttm and capex_q columns.
  """

  def __init__(self, epsilon: float = 1e-9):
    self.epsilon = epsilon

  def validate(self, df: pd.DataFrame, name: str) -> CheckResult:
    """
    Check that CAPEX column values are >= 0.

    Args:
      df: Gold panel DataFrame
      name: Check name for result

    Returns:
      CheckResult with pass/fail status
    """
    capex_cols = [c for c in ['capex_ttm', 'capex_q'] if c in df.columns]

    if not capex_cols:
      return pass_result(name, 'No CAPEX columns (skipped)')

    total_negative = 0
    total_checked = 0
    details: list[str] = []

    for col in capex_cols:
      data = df[col].dropna()
      if data.empty:
        continue

      negative_count = int((data < -self.epsilon).sum())
      total_negative += negative_count
      total_checked += len(data)

      if negative_count > 0:
        details.append(f'{col}: {negative_count} negative')

    if total_negative > 0:
      joined = ', '.join(details)
      return fail_result(
          name, f'{total_negative}/{total_checked} CAPEX values negative. '
          f'{joined}')

    return pass_result(name, f'All {total_checked} CAPEX values >= 0')
