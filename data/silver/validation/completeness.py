"""
Quarterly completeness validator.

Validates that each company has reasonable quarterly data coverage.
"""
import pandas as pd

from data.shared.validation.base import CheckResult
from data.shared.validation.base import fail_result
from data.shared.validation.base import pass_result


class QuarterlyCompletenessValidator:
  """
  Validate quarterly data coverage for each company.

  Checks that companies have data for most quarters between
  their first_filing_date and the target_date.
  """

  def __init__(self, missing_threshold: float = 0.25):
    """
    Args:
      missing_threshold: Maximum allowed fraction of missing quarters
    """
    self.missing_threshold = missing_threshold

  def validate(self, companies: pd.DataFrame, metrics_q: pd.DataFrame,
               target_date: str, name: str) -> CheckResult:
    """
    Check quarterly completeness for all companies.

    Args:
      companies: Companies DataFrame with first_filing_date
      metrics_q: metrics_quarterly DataFrame
      target_date: Target date string (YYYY-MM-DD)
      name: Check name for result

    Returns:
      CheckResult with pass/fail status
    """
    if 'first_filing_date' not in companies.columns:
      return fail_result(name, 'Missing first_filing_date in companies table')

    target_dt = pd.to_datetime(target_date)
    issues: list[str] = []

    for _, company in companies.iterrows():
      issue = self._check_company(company, metrics_q, target_dt)
      if issue:
        issues.append(issue)

    if issues:
      sample = issues[:10]
      return fail_result(
          name, f'{len(issues)} companies missing significant quarters. '
          f'Sample: {sample}')

    return pass_result(name, 'All companies have reasonable quarterly coverage')

  def _check_company(self, company: pd.Series, metrics_q: pd.DataFrame,
                     target_dt: pd.Timestamp) -> str | None:
    """Check a single company's quarterly completeness."""
    cik10 = company['cik10']
    first_filing = pd.to_datetime(company['first_filing_date'])

    if pd.isna(first_filing):
      return None

    company_metrics = metrics_q[metrics_q['cik10'] == cik10]
    if company_metrics.empty:
      return f'{cik10}: No metrics data'

    expected_quarters = self._generate_expected_quarters(first_filing,
                                                         target_dt)
    actual_quarters = set(company_metrics['end'].dt.to_period('Q'))
    expected_periods = set(pd.to_datetime(expected_quarters).to_period('Q'))

    missing = expected_periods - actual_quarters
    threshold = len(expected_periods) * self.missing_threshold

    if len(missing) > threshold:
      return f'{cik10}: Missing {len(missing)}/{len(expected_periods)} quarters'

    return None

  def _generate_expected_quarters(
      self, start: pd.Timestamp, end: pd.Timestamp) -> list[pd.Timestamp]:
    """Generate list of expected quarter end dates."""
    quarters = []
    current = start
    while current <= end:
      quarters.append(current)
      current = current + pd.DateOffset(months=3)
    return quarters
