"""
TTM (Trailing Twelve Months) correctness validator for Gold panels.

Validates that TTM values equal the sum of last 4 quarterly values.
"""
import pandas as pd

from data.shared.validation.base import CheckResult
from data.shared.validation.base import fail_result
from data.shared.validation.base import pass_result


class TTMCorrectnessValidator:
  """
  Validate TTM = sum of last 4 quarters in Gold panels.

  For each ticker, checks that cfo_ttm equals sum of last 4 cfo_q values.
  Uses PIT logic: for each (ticker, end, filed), finds 4 most recent quarters
  available at that filed date.
  """

  def __init__(self, tolerance: float = 1e-6, sample_tickers: int = 10):
    self.tolerance = tolerance
    self.sample_tickers = sample_tickers

  def validate(self, df: pd.DataFrame, name: str) -> CheckResult:
    """
    Validate TTM correctness by spot-checking sample tickers.

    Args:
      df: Gold panel DataFrame with cfo_ttm, cfo_q columns
      name: Check name for result

    Returns:
      CheckResult with pass/fail status
    """
    if 'cfo_ttm' not in df.columns or 'cfo_q' not in df.columns:
      return pass_result(name, 'No TTM/Q columns (skipped)')

    if 'ticker' not in df.columns:
      return pass_result(name, 'No ticker column (skipped)')

    errors: list[dict[str, object]] = []
    checked = 0

    tickers = df['ticker'].unique()[:self.sample_tickers]

    for ticker in tickers:
      ticker_errors, ticker_checked = self._check_ticker(df, ticker)
      errors.extend(ticker_errors)
      checked += ticker_checked

    if checked == 0:
      return pass_result(name, 'No TTM data to check')

    if errors:
      sample_df = pd.DataFrame(errors[:5])
      return fail_result(
          name, f'{len(errors)}/{checked} TTM != sum(Q). '
          f'Sample:\n{sample_df.to_string(index=False)}')

    return pass_result(name, f'TTM spot check passed ({checked} rows)')

  def _check_ticker(self, df: pd.DataFrame,
                    ticker: str) -> tuple[list[dict[str, object]], int]:
    """Check TTM for a single ticker using PIT logic."""
    ticker_df = df[df['ticker'] == ticker].copy()
    errors: list[dict[str, object]] = []
    checked = 0

    if len(ticker_df) < 4:
      return errors, checked

    has_filed = 'filed' in ticker_df.columns

    if has_filed:
      unique_ends = ticker_df.drop_duplicates(subset=['end'],
                                              keep='last').sort_values('end')
    else:
      unique_ends = ticker_df.sort_values('end')

    if len(unique_ends) < 4:
      return errors, checked

    for i in range(3, len(unique_ends)):
      row = unique_ends.iloc[i]

      if has_filed:
        filed = row['filed']
        available = ticker_df[(ticker_df['filed'] <= filed)]
        available = available.drop_duplicates(subset=['end'], keep='last')
        available = available.sort_values('end')
        recent_4 = available.tail(4)
      else:
        recent_4 = unique_ends.iloc[i - 3:i + 1]

      if len(recent_4) < 4:
        continue

      q_vals = recent_4['cfo_q']
      if q_vals.isna().any():
        continue

      ttm_val = row['cfo_ttm']
      if pd.isna(ttm_val):
        continue

      q_sum = q_vals.sum()
      checked += 1

      if abs(q_sum - ttm_val) > self.tolerance:
        errors.append({
            'ticker': ticker,
            'end': row['end'],
            'q_sum': q_sum,
            'ttm_val': ttm_val,
            'diff': abs(q_sum - ttm_val)
        })

    return errors, checked
