"""
Manual spotcheck validator.

Validates metrics against manually curated fixture data.
"""
from pathlib import Path

import pandas as pd

from data.shared.validation.base import CheckResult
from data.shared.validation.base import fail_result
from data.shared.validation.base import pass_result


class ManualSpotcheckValidator:
  """
  Validate metrics against a manual fixture CSV.

  Fixture CSV schema:
    cik10, metric, end, expected_val
  Optional columns: note, source_url
  """

  def __init__(self, tolerance: float = 1e-6):
    self.tolerance = tolerance

  def validate(self, metrics_q: pd.DataFrame, fixture_path: Path,
               name: str) -> CheckResult:
    """
    Compare metrics with manually curated expected values.

    Args:
      metrics_q: metrics_quarterly DataFrame
      fixture_path: Path to fixture CSV
      name: Check name for result

    Returns:
      CheckResult with pass/fail status
    """
    if not fixture_path.exists():
      return fail_result(name, f'Fixture file not found: {fixture_path}')

    fx = pd.read_csv(fixture_path)

    required_cols = ['cik10', 'metric', 'end', 'expected_val']
    missing_cols = [c for c in required_cols if c not in fx.columns]
    if missing_cols:
      return fail_result(name,
                         f'Fixture missing required columns: {missing_cols}')

    fx['end'] = pd.to_datetime(fx['end'])

    m = metrics_q.copy()
    m['end'] = pd.to_datetime(m['end'])

    merged = fx.merge(m, on=['cik10', 'metric', 'end'], how='left')

    missing_mask = merged['q_val'].isna()
    if missing_mask.any():
      n = int(missing_mask.sum())
      missing_sample = merged.loc[missing_mask,
                                  ['cik10', 'metric', 'end']].head(10).to_dict(
                                      orient='records')
      return fail_result(
          name, f'{n} fixture rows not found in metrics_quarterly. '
          f'Sample: {missing_sample}')

    merged['diff'] = (merged['q_val'] - merged['expected_val']).abs()
    bad_mask = merged['diff'] > self.tolerance

    if bad_mask.any():
      n = int(bad_mask.sum())
      bad_sample = merged.loc[
          bad_mask,
          ['cik10', 'metric', 'end', 'expected_val', 'q_val', 'diff']].head(10)
      return fail_result(
          name, f'{n}/{len(merged)} fixture rows mismatch '
          f'(tol={self.tolerance}). '
          f'Sample:\n{bad_sample.to_string(index=False)}')

    return pass_result(
        name, f'All {len(merged)} fixture rows match (tol={self.tolerance})')
