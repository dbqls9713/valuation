"""
TTM (Trailing Twelve Months) correctness validator for Gold panels.

Validates that TTM = sum of 4 quarters by fiscal_year/fiscal_quarter.
"""
import pandas as pd

from data.shared.validation.base import CheckResult
from data.shared.validation.base import fail_result
from data.shared.validation.base import pass_result


class TTMCorrectnessValidator:
  """
  Validate TTM = sum of 4 quarters by fiscal_year/fiscal_quarter.

  For each row, gets the 4 quarters ending at (fiscal_year, fiscal_quarter),
  using only values filed <= current row's filed date (PIT).
  """

  def __init__(self, tolerance: float = 1e-6, sample_tickers: int = 10):
    self.tolerance = tolerance
    self.sample_tickers = sample_tickers

  def validate(self, df: pd.DataFrame, name: str) -> CheckResult:
    """
    Validate TTM correctness by spot-checking sample tickers.

    Args:
      df: Gold panel DataFrame with cfo_ttm, cfo_q, fiscal_year, fiscal_quarter
      name: Check name for result

    Returns:
      CheckResult with pass/fail status
    """
    required = ['cfo_ttm', 'cfo_q', 'fiscal_year', 'fiscal_quarter']
    if not all(col in df.columns for col in required):
      return pass_result(name, 'Missing required columns (skipped)')

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

  def _get_prior_quarters(self, fy: int, fq: str) -> list[tuple[int, str]]:
    """Get the 4 quarters ending at (fy, fq)."""
    quarters = ['Q1', 'Q2', 'Q3', 'Q4']
    q_idx = quarters.index(fq)

    result = []
    for i in range(4):
      offset = q_idx - i
      if offset >= 0:
        result.append((fy, quarters[offset]))
      else:
        result.append((fy - 1, quarters[4 + offset]))

    return result

  def _check_ticker(self, df: pd.DataFrame,
                    ticker: str) -> tuple[list[dict[str, object]], int]:
    """Check TTM for a single ticker using fiscal_year/fiscal_quarter logic."""
    ticker_df = df[df['ticker'] == ticker].copy()
    errors: list[dict[str, object]] = []
    checked = 0

    if len(ticker_df) < 4:
      return errors, checked

    has_filed = 'filed' in ticker_df.columns
    ticker_df = ticker_df.sort_values(
        ['fiscal_year', 'fiscal_quarter', 'filed']
        if has_filed else ['fiscal_year', 'fiscal_quarter'])

    unique_quarters = ticker_df.drop_duplicates(
        subset=['fiscal_year', 'fiscal_quarter'], keep='last')

    for _, row in unique_quarters.iterrows():
      fy = row['fiscal_year']
      fq = row['fiscal_quarter']
      filed = row['filed'] if has_filed else None
      ttm_val = row['cfo_ttm']

      if pd.isna(ttm_val):
        continue

      target_quarters = self._get_prior_quarters(fy, fq)

      q_vals = []
      for t_fy, t_fq in target_quarters:
        if has_filed:
          candidates = ticker_df[(ticker_df['fiscal_year'] == t_fy) &
                                 (ticker_df['fiscal_quarter'] == t_fq) &
                                 (ticker_df['filed'] <= filed)]
        else:
          candidates = ticker_df[(ticker_df['fiscal_year'] == t_fy) &
                                 (ticker_df['fiscal_quarter'] == t_fq)]

        if not candidates.empty:
          latest = candidates.sort_values('filed').iloc[-1] if has_filed \
                   else candidates.iloc[-1]
          q_val = latest['cfo_q']
          if pd.notna(q_val):
            q_vals.append(q_val)

      if len(q_vals) != 4:
        continue

      q_sum = sum(q_vals)
      checked += 1

      if abs(q_sum - ttm_val) > self.tolerance:
        errors.append({
            'ticker': ticker,
            'fy_fq': f'{fy}{fq}',
            'q_sum': q_sum,
            'ttm_val': ttm_val,
            'diff': abs(q_sum - ttm_val)
        })

    return errors, checked
