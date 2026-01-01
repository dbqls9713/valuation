"""
Owner Earnings (OE) validator for Gold panels.

Validates OE statistics (CFO - CAPEX) as informational metrics.
"""
import pandas as pd

from data.shared.validation.base import CheckResult
from data.shared.validation.base import pass_result


class OEPositiveValidator:
  """
  Check Owner Earnings (CFO - CAPEX) positivity rate.

  This is an informational check (warning only), as negative OE
  can be legitimate for capital-intensive or growth companies.
  """

  def validate(self, df: pd.DataFrame, name: str) -> CheckResult:
    """
    Calculate OE positive ratio.

    Args:
      df: Gold panel DataFrame with cfo_ttm, capex_ttm columns
      name: Check name for result

    Returns:
      CheckResult with statistics (always passes)
    """
    if 'cfo_ttm' not in df.columns or 'capex_ttm' not in df.columns:
      return pass_result(name, 'OE columns not present (skipped)')

    valid_mask = df['cfo_ttm'].notna() & df['capex_ttm'].notna()
    valid_df = df[valid_mask]

    if valid_df.empty:
      return pass_result(name, 'No valid OE data')

    oe = valid_df['cfo_ttm'] - valid_df['capex_ttm']
    positive_count = int((oe > 0).sum())
    total = len(oe)
    pct = positive_count / total * 100 if total > 0 else 0

    return pass_result(name,
                       f'OE positive: {positive_count}/{total} ({pct:.1f}%)')
