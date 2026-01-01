"""
SEC-specific transformations.
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


class SECMetricsBuilder:
  """Build quarterly metrics from facts."""

  def build(self, facts_long: pd.DataFrame) -> pd.DataFrame:
    """
    From minimal facts_long -> per-metric quarterly discrete values + TTM.

    Output columns:
      cik10, metric, end, filed, fy, fp, fiscal_year, q_val, ttm_val, tag
    """
    out_parts: list[pd.DataFrame] = []

    for metric, spec in METRIC_SPECS.items():
      df = facts_long[facts_long['metric'] == metric].copy()
      if df.empty:
        continue

      if bool(spec.get('abs', False)):
        df['val'] = df['val'].abs()

      if bool(spec.get('normalize_to_actual_count', False)):
        df['val'] = self._normalize_shares_to_actual_count(df['val'])

      parts = []
      for cik10, g in df.groupby('cik10'):
        if bool(spec.get('is_ytd', False)):
          qg = self._ytd_to_quarter_pit(g, value_col='val', out_col='q_val')
          if bool(spec.get('abs', False)):
            qg['q_val'] = qg['q_val'].abs()
        else:
          qg = g.rename(columns={'val': 'q_val'})[[
              'end', 'filed', 'fy', 'fp', 'fiscal_year', 'q_val', 'tag'
          ]].copy()

        qg['cik10'] = cik10
        qg['metric'] = metric
        parts.append(qg)

      non_empty_parts = [p for p in parts if not p.empty]
      if not non_empty_parts:
        continue
      q_all = pd.concat(non_empty_parts, ignore_index=True)

      # Calculate TTM for flow metrics using PIT logic
      if bool(spec.get('is_ytd', False)):
        q_all = self._calculate_ttm_pit(q_all)
      else:
        q_all['ttm_val'] = pd.Series([pd.NA] * len(q_all), dtype='Float64')

      out_parts.append(q_all[[
          'cik10', 'metric', 'end', 'filed', 'fy', 'fp', 'fiscal_year', 'q_val',
          'ttm_val', 'tag'
      ]])

    if not out_parts:
      return pd.DataFrame(columns=[
          'cik10', 'metric', 'end', 'filed', 'fy', 'fp', 'fiscal_year', 'q_val',
          'ttm_val', 'tag'
      ])

    # Filter out empty DataFrames and ensure consistent dtypes for concat
    non_empty_parts = []
    for p in out_parts:
      if p.empty:
        continue
      p = p.copy()
      if 'ttm_val' in p.columns:
        p['ttm_val'] = p['ttm_val'].astype('Float64')
      if 'q_val' in p.columns:
        p['q_val'] = p['q_val'].astype('Float64')
      non_empty_parts.append(p)

    if not non_empty_parts:
      return pd.DataFrame(columns=[
          'cik10', 'metric', 'end', 'filed', 'fy', 'fp', 'fiscal_year', 'q_val',
          'ttm_val', 'tag'
      ])

    out = pd.concat(non_empty_parts, ignore_index=True)
    out = out.sort_values(['cik10', 'metric', 'end']).reset_index(drop=True)
    return out

  @staticmethod
  def _normalize_shares_to_actual_count(series: pd.Series) -> pd.Series:
    """
    Normalize shares to actual count (not millions).

    Heuristic: If value < 1,000,000, assume it's in millions
    and multiply by 1M. Otherwise, assume it's already actual count.

    Examples:
      717.2 → 717,200,000 (was in millions)
      7,466,000,000 → 7,466,000,000 (already actual count)
    """
    threshold = 1_000_000

    def normalize_value(val):
      if pd.isna(val):
        return val
      if abs(val) < threshold:
        return val * 1_000_000
      return val

    return series.apply(normalize_value)  # type: ignore[no-any-return]

  def _ytd_to_quarter_pit(self,
                          df_ytd: pd.DataFrame,
                          value_col: str = 'val',
                          out_col: str = 'q_val') -> pd.DataFrame:
    """
    Convert YTD values to quarterly values using PIT (Point-in-Time) logic.

    For each row, find the previous quarter's YTD value that was filed before
    this row's filed date, then subtract to get the quarterly value.
    This ensures each filed version uses only data available at that time.

    Only processes rows where fp matches fiscal_quarter to avoid
    incorrectly processing comparative disclosures.
    """
    if df_ytd.empty:
      return pd.DataFrame()

    df = df_ytd.copy()
    df = df.sort_values('filed')

    prev_fp_map = {'Q2': 'Q1', 'Q3': 'Q2', 'FY': 'Q3'}
    fp_to_fq = {'Q1': 'Q1', 'Q2': 'Q2', 'Q3': 'Q3', 'FY': 'Q4'}

    out_rows = []

    for _, row in df.iterrows():
      fp = str(row['fp'])
      fiscal_year = row['fiscal_year']
      filed = row['filed']
      ytd_val = float(row[value_col])

      # Skip comparative disclosures (fp doesn't match fiscal_quarter)
      expected_fq = fp_to_fq.get(fp)
      if expected_fq and row.get('fiscal_quarter') != expected_fq:
        continue

      if fp == 'Q1':
        q_val = ytd_val
      elif fp in prev_fp_map:
        prev_q = prev_fp_map[fp]
        # Match fp AND fiscal_quarter to avoid comparative disclosures
        candidates = df[(df['fiscal_year'] == fiscal_year) &
                        (df['fp'] == prev_q) &
                        (df['fiscal_quarter'] == prev_q) &
                        (df['filed'] < filed)]

        if not candidates.empty:
          prev_row = candidates.sort_values('filed').iloc[-1]
          prev_ytd_val = float(prev_row[value_col])
          q_val = ytd_val - prev_ytd_val
        else:
          q_val = ytd_val
      else:
        continue

      out_rows.append({
          'end': row['end'],
          'filed': row['filed'],
          'fy': row['fy'],
          'fp': 'Q4' if fp == 'FY' else fp,
          'fiscal_year': fiscal_year,
          out_col: q_val,
          'tag': row['tag'],
      })

    return pd.DataFrame(out_rows)

  def _calculate_ttm_pit(self, q_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate TTM using PIT logic.

    For each row, sum the 4 most recent quarterly values that were
    filed before this row's filed date.
    """
    if q_df.empty:
      return q_df

    df = q_df.copy()
    df = df.sort_values(['cik10', 'filed', 'end'])

    ttm_values = []

    for _, row in df.iterrows():
      cik10 = row['cik10']
      filed = row['filed']

      candidates = df[(df['cik10'] == cik10) & (df['filed'] <= filed)]
      candidates = candidates.drop_duplicates(subset=['end'],
                                              keep='last').sort_values('end')

      recent_4 = candidates.tail(4)

      if len(recent_4) == 4:
        ttm = recent_4['q_val'].sum()
      else:
        ttm = pd.NA

      ttm_values.append(ttm)

    df['ttm_val'] = pd.array(ttm_values, dtype='Float64')
    return df
