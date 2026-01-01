"""
SEC-specific transformations.

Silver layer: Normalization only. YTD->Q and TTM calculations moved to Gold.
"""

import pandas as pd

from data.silver.config.metric_specs import METRIC_SPECS
from data.silver.shared.transforms import FiscalYearCalculator


class SECFactsTransformer:
  """Transform SEC facts data."""

  def __init__(self):
    self.fiscal_year_calc = FiscalYearCalculator()

  def add_fiscal_year(self, facts: pd.DataFrame,
                      companies: pd.DataFrame) -> pd.DataFrame:
    """Add fiscal_year column."""
    result = self.fiscal_year_calc.calculate(facts, companies)
    return result  # type: ignore[no-any-return]

  def deduplicate(self, df: pd.DataFrame) -> pd.DataFrame:
    """
    Deduplicate facts by (fiscal_year, fiscal_quarter, filed).

    Keeps all filed versions for PIT (Point-in-Time) analysis.
    When multiple records exist for the same key, keeps the highest
    priority tag (as defined in METRIC_SPECS).

    Requires 'fiscal_quarter' column to be present.
    """
    if df.empty:
      return df

    if 'fiscal_quarter' not in df.columns:
      raise ValueError('fiscal_quarter column required for deduplicate')

    # Filter out invalid data (fy=0 or empty fp)
    valid = df[(df['fy'] > 0) & (df['fp'] != '')].copy()

    # When same (fiscal_year, fiscal_quarter, filed) has multiple records,
    # keep highest priority tag and latest end date
    tag_priority: dict[str, int] = {}
    for spec in METRIC_SPECS.values():
      tags: list[str] = spec['tags']  # type: ignore[assignment]
      for idx, tag in enumerate(tags):
        tag_priority[tag] = idx

    valid['_tag_priority'] = valid['tag'].map(
        lambda t: tag_priority.get(t, 999))
    # Sort by tag priority (asc), then end date (desc)
    valid = valid.sort_values(['_tag_priority', 'end'], ascending=[True, False])

    # Deduplicate by (fiscal_year, fiscal_quarter, filed), keep best tag
    dedup_cols = ['cik10', 'metric', 'fiscal_year', 'fiscal_quarter', 'filed']
    valid = valid.drop_duplicates(subset=dedup_cols, keep='first')

    valid = valid.drop(columns=['_tag_priority'])
    return valid.reset_index(drop=True)

  def normalize_values(self, df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply value normalizations based on metric_specs.

    - abs: Take absolute value (CAPEX)
    - normalize_to_actual_count: Convert millions to actual (SHARES)
    """
    result = df.copy()

    for metric, spec in METRIC_SPECS.items():
      mask = result['metric'] == metric

      if bool(spec.get('abs', False)):
        result.loc[mask, 'val'] = result.loc[mask, 'val'].abs()

      if bool(spec.get('normalize_to_actual_count', False)):
        result.loc[mask, 'val'] = self._normalize_shares(result.loc[mask,
                                                                    'val'])

    return result

  @staticmethod
  def _normalize_shares(series: pd.Series) -> pd.Series:
    """
    Normalize shares to actual count (not millions).

    Heuristic: If value < 1,000,000, assume it's in millions.
    """
    threshold = 1_000_000

    def normalize_value(val: float) -> float:
      if pd.isna(val):
        return val
      if abs(val) < threshold:
        return val * 1_000_000
      return val

    return series.apply(normalize_value)  # type: ignore[no-any-return]
