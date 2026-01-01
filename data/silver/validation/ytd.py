"""
YTD-to-Quarterly identity validator.

Validates that YTD values in facts_long match reconstructed YTD
from quarterly q_val in metrics_quarterly, using PIT logic.
"""
import pandas as pd

from data.shared.validation.base import CheckResult
from data.shared.validation.base import fail_result
from data.shared.validation.base import pass_result
from data.silver.config.metric_specs import METRIC_SPECS


class YTDIdentityValidator:
  """
  Validate YTD identity between facts and quarterly metrics using PIT logic.

  For each row in facts_long (with YTD value), reconstructs the YTD by
  summing quarterly values from metrics_quarterly using the same PIT logic
  as the original transformation.
  """

  def __init__(self, tolerance: float = 1e-6):
    self.tolerance = tolerance

  def validate(self, facts: pd.DataFrame, metrics_q: pd.DataFrame,
               name: str) -> CheckResult:
    """
    Compare facts_long YTD with reconstructed YTD using PIT logic.

    For each facts_long row, find the corresponding metrics_quarterly rows
    with filed <= current filed, then sum quarters to reconstruct YTD.
    """
    if facts.empty or metrics_q.empty:
      return pass_result(name, 'No data to validate')

    ytd_metrics = [m for m, s in METRIC_SPECS.items() if s.get('is_ytd', False)]

    facts_ytd = facts[facts['metric'].isin(ytd_metrics)].copy()
    if facts_ytd.empty:
      return pass_result(name, 'No YTD metrics to validate')

    for metric in ytd_metrics:
      if METRIC_SPECS[metric].get('abs', False):
        mask = facts_ytd['metric'] == metric
        facts_ytd.loc[mask, 'val'] = facts_ytd.loc[mask, 'val'].abs()

    errors: list[dict[str, object]] = []

    for cik in facts_ytd['cik10'].unique():
      cik_facts = facts_ytd[facts_ytd['cik10'] == cik]
      cik_metrics = metrics_q[metrics_q['cik10'] == cik]

      for metric in cik_facts['metric'].unique():
        m_facts = cik_facts[cik_facts['metric'] == metric]
        m_metrics = cik_metrics[cik_metrics['metric'] == metric]

        errs = self._check_metric(m_facts, m_metrics, cik, metric)
        errors.extend(errs)

        if len(errors) >= 100:
          break
      if len(errors) >= 100:
        break

    total = len(facts_ytd)
    if errors:
      sample_df = pd.DataFrame(errors[:10])
      return fail_result(
          name, f'{len(errors)}/{total} rows fail YTD identity (PIT-based). '
          f'Sample:\n{sample_df.to_string(index=False)}')

    return pass_result(name, f'All {total} YTD rows pass identity check (PIT)')

  def _check_metric(self, facts: pd.DataFrame, metrics: pd.DataFrame, cik: str,
                    metric: str) -> list[dict[str, object]]:
    """Check YTD identity for a single metric of a company."""
    errors: list[dict[str, object]] = []

    fp_to_quarters = {
        'Q1': ['Q1'],
        'Q2': ['Q1', 'Q2'],
        'Q3': ['Q1', 'Q2', 'Q3'],
        'FY': ['Q1', 'Q2', 'Q3', 'Q4'],
    }

    fp_to_fiscal_quarter = {'Q1': 'Q1', 'Q2': 'Q2', 'Q3': 'Q3', 'FY': 'Q4'}

    for _, row in facts.iterrows():
      fp = str(row['fp'])
      if fp not in fp_to_quarters:
        continue

      # Only check rows where fp matches fiscal_quarter
      # (skip comparative disclosures where fp != fiscal_quarter)
      expected_fq = fp_to_fiscal_quarter[fp]
      if row['fiscal_quarter'] != expected_fq:
        continue

      fiscal_year = row['fiscal_year']
      filed = row['filed']
      ytd_val = float(row['val'])

      quarters_needed = fp_to_quarters[fp]
      q_sum = self._reconstruct_ytd_pit(metrics, fiscal_year, filed,
                                        quarters_needed)

      if q_sum is None:
        continue

      diff = abs(ytd_val - q_sum)
      if diff > self.tolerance:
        errors.append({
            'cik10': cik,
            'metric': metric,
            'fiscal_year': fiscal_year,
            'fp': fp,
            'filed': filed,
            'ytd_val': ytd_val,
            'recon': q_sum,
            'diff': diff,
        })

    return errors

  def _reconstruct_ytd_pit(self, metrics: pd.DataFrame, fiscal_year: int,
                           filed: pd.Timestamp,
                           quarters: list[str]) -> float | None:
    """
    Reconstruct YTD using PIT logic.

    Find quarterly values where filed <= current filed date,
    keeping only the latest filed version for each quarter.
    """
    fy_metrics = metrics[metrics['fiscal_year'] == fiscal_year]
    pit_metrics = fy_metrics[fy_metrics['filed'] <= filed]

    if pit_metrics.empty:
      return None

    q_sum = 0.0
    for q in quarters:
      q_data = pit_metrics[pit_metrics['fiscal_quarter'] == q]
      if q_data.empty:
        return None

      latest = q_data.sort_values('filed').iloc[-1]
      q_sum += float(latest['q_val'])

    return q_sum
