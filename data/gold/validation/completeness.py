"""
Quarterly completeness validator for Gold panels.

Validates that each company has reasonable quarterly data coverage.
"""
import pandas as pd

from data.shared.validation.base import CheckResult
from data.shared.validation.base import fail_result
from data.shared.validation.base import pass_result


class QuarterlyCompletenessValidator:
  """
  Validate quarterly data coverage for each company in Gold panel.

  Checks that companies have data for most quarters between
  their first_filing_date and the target_date.
  """

  def __init__(self, missing_threshold: float = 0.25):
    self.missing_threshold = missing_threshold

  def validate(self, companies: pd.DataFrame, panel: pd.DataFrame,
               target_date: str, name: str) -> CheckResult:
    """
    Check quarterly completeness for all companies.

    Args:
      companies: Companies DataFrame with first_filing_date
      panel: Gold panel DataFrame
      target_date: Target date string (YYYY-MM-DD)
      name: Check name for result

    Returns:
      CheckResult with pass/fail status
    """
    if 'first_filing_date' not in companies.columns:
      return fail_result(name, 'Missing first_filing_date in companies table')

    if 'ticker' not in panel.columns or 'end' not in panel.columns:
      return pass_result(name, 'No ticker/end columns (skipped)')

    target_dt = pd.to_datetime(target_date)
    issues: list[str] = []

    for _, company in companies.iterrows():
      issue = self._check_company(company, panel, target_dt)
      if issue:
        issues.append(issue)

    if issues:
      sample = issues[:10]
      return fail_result(
          name, f'{len(issues)} companies missing significant quarters. '
          f'Sample: {sample}')

    return pass_result(name, 'All companies have reasonable quarterly coverage')

  def _check_company(self, company: pd.Series, panel: pd.DataFrame,
                     target_dt: pd.Timestamp) -> str | None:
    """Check a single company's quarterly completeness."""
    ticker = company['ticker']
    first_filing = pd.to_datetime(company['first_filing_date'])

    if pd.isna(first_filing):
      return None

    company_data = panel[panel['ticker'] == ticker]
    if company_data.empty:
      return f'{ticker}: No panel data'

    expected_quarters = self._count_expected_quarters(first_filing, target_dt)
    actual_quarters = company_data['end'].dt.to_period('Q').nunique()

    missing = expected_quarters - actual_quarters
    threshold = expected_quarters * self.missing_threshold

    if missing > threshold:
      return f'{ticker}: Missing {missing}/{expected_quarters} quarters'

    return None

  def _count_expected_quarters(self, start: pd.Timestamp,
                               end: pd.Timestamp) -> int:
    """Count expected quarters between two dates."""
    if start > end:
      return 0
    months = (end.year - start.year) * 12 + (end.month - start.month)
    return int(max(1, months // 3))
