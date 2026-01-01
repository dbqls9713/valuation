"""
TTM (Trailing Twelve Months) correctness validator.

Validates that TTM values are correctly computed using PIT logic.
"""
import pandas as pd

from data.shared.validation.base import CheckResult
from data.shared.validation.base import fail_result
from data.shared.validation.base import pass_result


class TTMCorrectnessValidator:
  """
  Validate TTM correctness using PIT logic.

  For PIT-based TTM, each row's ttm_val should be computed from
  the 4 most recent quarters available at that row's filed date.
  """

  def __init__(self, tolerance: float = 1e-6, max_errors: int = 100):
    self.tolerance = tolerance
    self.max_errors = max_errors

  def validate(self, metrics_q: pd.DataFrame, name: str) -> CheckResult:
    """
    Validate TTM values against PIT-based recomputation.

    Args:
      metrics_q: metrics_quarterly DataFrame
      name: Check name for result

    Returns:
      CheckResult with pass/fail status
    """
    m = metrics_q.copy()
    errors: list[dict[str, object]] = []

    for cik in m['cik10'].unique():
      if len(errors) >= self.max_errors:
        break

      cik_data = m[m['cik10'] == cik].copy()
      cik_errors = self._check_company(cik_data, cik)
      errors.extend(cik_errors)

    total_with_ttm = int(m['ttm_val'].notna().sum())

    if errors:
      sample_df = pd.DataFrame(errors[:10])
      return fail_result(
          name,
          f'{len(errors)}+ rows fail TTM check (tol={self.tolerance}, '
          f'sampled first {self.max_errors}). '
          f'Sample:\n{sample_df.to_string(index=False)}')

    return pass_result(
        name, f'TTM values pass PIT-based check (tol={self.tolerance}). '
        f'Total rows with TTM={total_with_ttm}')

  def _check_company(self, cik_data: pd.DataFrame,
                     cik: str) -> list[dict[str, object]]:
    """Check TTM for a single company."""
    errors: list[dict[str, object]] = []

    for metric in cik_data['metric'].unique():
      if len(errors) >= self.max_errors:
        break

      metric_data = cik_data[cik_data['metric'] == metric].copy()
      metric_data = metric_data.sort_values(['filed', 'end'])

      metric_errors = self._check_metric(metric_data, cik, metric)
      errors.extend(metric_errors)

    return errors

  def _check_metric(self, metric_data: pd.DataFrame, cik: str,
                    metric: str) -> list[dict[str, object]]:
    """Check TTM for a single metric of a company."""
    errors: list[dict[str, object]] = []

    for _, row in metric_data.iterrows():
      if len(errors) >= self.max_errors:
        break

      if pd.isna(row['ttm_val']):
        continue

      ttm_recomputed = self._compute_pit_ttm(metric_data, row['filed'])
      if ttm_recomputed is None:
        continue

      diff = abs(row['ttm_val'] - ttm_recomputed)
      if diff > self.tolerance:
        errors.append({
            'cik10': cik,
            'metric': metric,
            'end': row['end'],
            'ttm_val': row['ttm_val'],
            'ttm_recomputed': ttm_recomputed,
            'diff': diff
        })

    return errors

  def _compute_pit_ttm(self, metric_data: pd.DataFrame,
                       filed: pd.Timestamp) -> float | None:
    """Compute PIT-based TTM for a given filed date."""
    available = metric_data[metric_data['filed'] <= filed]
    available = available.drop_duplicates(subset=['end'], keep='last')
    available = available.sort_values('end')
    recent_4 = available.tail(4)

    if len(recent_4) < 4:
      return None

    return float(recent_4['q_val'].sum())
