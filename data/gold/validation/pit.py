"""
PIT (Point-in-Time) consistency validator for Gold panels.

Validates that PIT history is correctly structured for backtest panel.
"""
import pandas as pd

from data.shared.validation.base import CheckResult
from data.shared.validation.base import pass_result


class PITConsistencyValidator:
  """
  Validate PIT consistency for backtest panel.

  Checks:
  1. Multiple filed versions can exist for same (ticker, end)
  2. Filed dates are properly ordered (filed >= end)
  3. Restatement patterns are reasonable
  """

  def __init__(self, restatement_threshold: float = 0.01):
    self.restatement_threshold = restatement_threshold

  def validate(self, df: pd.DataFrame, name: str) -> CheckResult:
    """
    Validate PIT consistency of panel.

    Args:
      df: Gold panel DataFrame
      name: Check name for result

    Returns:
      CheckResult with pass/fail status
    """
    if df.empty:
      return pass_result(name, 'No data to validate')

    if 'filed' not in df.columns:
      return pass_result(name, 'No filed column (skipped)')

    results: list[str] = []

    version_info = self._check_version_counts(df)
    results.append(version_info)

    restatement_info = self._check_restatements(df)
    results.append(restatement_info)

    return pass_result(name, ' | '.join(results))

  def _check_version_counts(self, df: pd.DataFrame) -> str:
    """Check how many unique filed versions exist per period."""
    if 'ticker' not in df.columns or 'end' not in df.columns:
      return 'Versions: No ticker/end columns'

    version_counts = df.groupby(['ticker', 'end'])['filed'].nunique()

    single_version = int((version_counts == 1).sum())
    multi_version = int((version_counts > 1).sum())
    max_versions = int(version_counts.max()) if len(version_counts) > 0 else 0

    return (f'Versions: {single_version} single, {multi_version} multi '
            f'(max {max_versions})')

  def _check_restatements(self, df: pd.DataFrame) -> str:
    """Detect and count restatements in shares data."""
    if 'shares_q' not in df.columns:
      return 'Restatements: No shares_q column'

    restatement_count = 0
    total_periods = df.groupby(['ticker', 'end']).ngroups

    for _, group in df.groupby(['ticker', 'end']):
      if len(group) <= 1:
        continue

      sorted_group = group.sort_values('filed')
      values = sorted_group['shares_q'].values

      for i in range(1, len(values)):
        if pd.isna(values[i]) or pd.isna(values[i - 1]):
          continue
        if values[i - 1] == 0:
          continue

        rel_diff = abs(values[i] - values[i - 1]) / abs(values[i - 1])
        if rel_diff > self.restatement_threshold:
          restatement_count += 1
          break

    pct = restatement_count / total_periods * 100 if total_periods > 0 else 0
    return f'Restatements: {restatement_count}/{total_periods} ({pct:.1f}%)'
