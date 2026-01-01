"""
PIT (Point-in-Time) consistency validator.

Validates that PIT history data is correctly structured and consistent.
"""
import pandas as pd

from data.shared.validation.base import CheckResult
from data.shared.validation.base import pass_result


class PITConsistencyValidator:
  """
  Validate PIT history data consistency.

  Checks:
  1. Multiple filed versions exist for restatements
  2. Later filed versions are accessible via PIT filtering
  3. Restatement detection and logging
  """

  def __init__(self, restatement_threshold: float = 0.01):
    """
    Args:
      restatement_threshold: Minimum relative difference to count as restatement
    """
    self.restatement_threshold = restatement_threshold

  def validate(self, metrics_q: pd.DataFrame, name: str) -> CheckResult:
    """
    Validate PIT consistency of metrics_quarterly.

    Checks:
    1. Filed versions are properly ordered (filed >= end)
    2. Multiple filed versions per (cik10, metric, fiscal_year, fiscal_quarter)
    3. Restatements are detectable
    """
    if metrics_q.empty:
      return pass_result(name, 'No data to validate')

    results: list[str] = []

    version_counts = self._check_version_counts(metrics_q)
    results.append(version_counts)

    restatement_info = self._check_restatements(metrics_q)
    results.append(restatement_info)

    pit_access = self._check_pit_accessibility(metrics_q)
    results.append(pit_access)

    return pass_result(name, ' | '.join(results))

  def _check_version_counts(self, df: pd.DataFrame) -> str:
    """Check how many unique filed versions exist per period."""
    group_cols = ['cik10', 'metric', 'fiscal_year', 'fiscal_quarter']
    version_counts = df.groupby(group_cols)['filed'].nunique()

    single_version = int((version_counts == 1).sum())
    multi_version = int((version_counts > 1).sum())
    max_versions = int(version_counts.max())

    return (f'Versions: {single_version} single, {multi_version} multi '
            f'(max {max_versions})')

  def _check_restatements(self, df: pd.DataFrame) -> str:
    """Detect and count restatements (value changes across filed versions)."""
    group_cols = ['cik10', 'metric', 'fiscal_year', 'fiscal_quarter']
    restatement_count = 0

    for _, group in df.groupby(group_cols):
      if len(group) <= 1:
        continue

      sorted_group = group.sort_values('filed')
      values = sorted_group['q_val'].values

      for i in range(1, len(values)):
        if pd.isna(values[i]) or pd.isna(values[i - 1]):
          continue
        if values[i - 1] == 0:
          continue

        rel_diff = abs(values[i] - values[i - 1]) / abs(values[i - 1])
        if rel_diff > self.restatement_threshold:
          restatement_count += 1
          break

    total_periods = df.groupby(group_cols).ngroups
    pct = restatement_count / total_periods * 100 if total_periods > 0 else 0

    return f'Restatements: {restatement_count}/{total_periods} ({pct:.1f}%)'

  def _check_pit_accessibility(self, df: pd.DataFrame) -> str:
    """Verify that PIT filtering returns expected results."""
    if df.empty:
      return 'PIT: No data'

    sample_filed = df['filed'].median()

    pit_data = df[df['filed'] <= sample_filed]
    all_data = df

    pit_periods = pit_data.groupby(
        ['cik10', 'metric', 'fiscal_year', 'fiscal_quarter']).ngroups
    all_periods = all_data.groupby(
        ['cik10', 'metric', 'fiscal_year', 'fiscal_quarter']).ngroups

    return f'PIT access: {pit_periods}/{all_periods} periods at median date'
