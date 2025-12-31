"""
Shared transformation utilities.
"""

import pandas as pd


class TTMCalculator:
  """Calculate TTM (Trailing Twelve Months) values."""

  def calculate(self,
                df: pd.DataFrame,
                value_col: str,
                group_cols: list[str],
                sort_col: str = 'end') -> pd.DataFrame:
    """
    Calculate TTM by rolling 4-quarter sum.

    Args:
        df: Input dataframe
        value_col: Column to calculate TTM for
        group_cols: Columns to group by
        sort_col: Column to sort by (usually 'end')

    Returns:
        DataFrame with ttm_<value_col> column added
    """
    df = df.sort_values(group_cols + [sort_col])

    ttm_col = f'ttm_{value_col}'
    rolling_result = df.groupby(group_cols)[value_col].rolling(4).sum()
    df[ttm_col] = rolling_result.reset_index(level=list(range(len(group_cols))),
                                             drop=True)

    return df

class FiscalYearCalculator:
  """Calculate fiscal year from calendar dates."""

  def calculate(self, facts: pd.DataFrame,
                companies: pd.DataFrame) -> pd.DataFrame:
    """
    Add fiscal_year column based on fye_mmdd.

    Fiscal year is defined as the year when the fiscal period ends.
    For example, if FYE is Sep 26:
    - FY 2019 = period ending on 2019-09-26 (spans 2018-09-27 to 2019-09-26)
    - Dates on or before FYE belong to that calendar year's FY
    - Dates after FYE belong to next calendar year's FY

    A tolerance of ±7 days is applied to handle cases where the actual
    period end date is close to but not exactly on the FYE date
    (e.g., due to weekends or holidays).

    Examples with FYE = Sep 26 (0926):
    - 2019-03-30 (before 0926) → FY 2019
    - 2019-09-26 (on FYE)      → FY 2019
    - 2019-09-28 (within +7)   → FY 2019 (tolerance)
    - 2019-12-28 (after 0926)  → FY 2020

    Args:
        facts: Facts DataFrame with end dates
        companies: Companies DataFrame with fye_mmdd

    Returns:
        Facts DataFrame with fiscal_year column
    """
    facts = facts.copy()
    facts = facts.merge(companies[['cik10', 'fye_mmdd']],
                       on='cik10',
                       how='left')

    facts['end'] = pd.to_datetime(facts['end'])
    facts['year'] = facts['end'].dt.year
    facts['mmdd'] = facts['end'].dt.strftime('%m%d')

    def calc_fiscal_year(row):
      if pd.isna(row['fye_mmdd']):
        return row['year']

      # Parse fye_mmdd and current date mmdd to day-of-year for comparison
      fye_month = int(row['fye_mmdd'][:2])
      fye_day = int(row['fye_mmdd'][2:])
      cur_month = int(row['mmdd'][:2])
      cur_day = int(row['mmdd'][2:])

      # Approximate day-of-year (good enough for this purpose)
      fye_doy = fye_month * 31 + fye_day
      cur_doy = cur_month * 31 + cur_day

      # Within ±7 days of FYE, or before FYE → current year
      # After FYE (beyond tolerance) → next year
      tolerance = 7  # 7 days (in our month*31 approximation)
      if cur_doy <= fye_doy + tolerance:
        return row['year']
      return row['year'] + 1

    facts['fiscal_year'] = facts.apply(calc_fiscal_year, axis=1)

    facts = facts.drop(columns=['fye_mmdd', 'year', 'mmdd'])
    return facts
