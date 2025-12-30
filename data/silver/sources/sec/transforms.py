"""
SEC-specific transformations.
"""
from typing import List

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

  def deduplicate(self,
                  df: pd.DataFrame,
                  keep_all_versions: bool = False) -> pd.DataFrame:
    """
    Keep only the latest filed value for each period, or all versions.

    Args:
        df: Input DataFrame
        keep_all_versions: If True, keep all filed versions for PIT analysis.
                          If False, keep only the latest filed version.

    Uses fiscal_year for grouping when available.
    Filters out comparative statements (fy != fiscal_year) to avoid mixing
    different periods in downstream processing.
    """
    if df.empty:
      return df

    if 'fiscal_year' not in df.columns:
      group_cols = ['cik10', 'metric', 'end', 'fy', 'fp']
      out = df.sort_values(group_cols + ['filed'])
      out = out.groupby(group_cols, as_index=False).tail(1)
      return out.reset_index(drop=True)

    # Filter out comparative statements: keep only fy == fiscal_year
    df = df[df['fy'] == df['fiscal_year']].copy()

    if keep_all_versions:
      # For PIT: keep all unique (end, fp, filed) combinations
      # Remove exact duplicates but preserve different filing dates
      return df.drop_duplicates(
          subset=['cik10', 'metric', 'end', 'fp',
                  'filed'], keep='last').reset_index(drop=True)

    # Original logic: keep only latest filed version
    out_parts: List[pd.DataFrame] = []

    for _, g in df.groupby(['cik10', 'metric', 'fiscal_year'], dropna=True):
      # Since we filtered fy == fiscal_year, all records belong to this
      # fiscal_year
      for _, sub in g.groupby(['end', 'fp']):
        quarterly_fps = {'Q1', 'Q2', 'Q3', 'Q4'}
        has_quarterly = any(r['fp'] in quarterly_fps for _, r in sub.iterrows())
        has_fy = any(r['fp'] == 'FY' for _, r in sub.iterrows())

        if has_quarterly and has_fy:
          sub = sub[sub['fp'].isin(quarterly_fps)]

        if len(sub) > 1:
          # Multiple filings for same period: keep latest
          sub = sub.sort_values('filed', ascending=False).head(1)

        out_parts.append(sub)

    if not out_parts:
      return pd.DataFrame(columns=df.columns)

    non_empty = [p for p in out_parts if not p.empty]
    if not non_empty:
      return pd.DataFrame(columns=df.columns)

    return pd.concat(non_empty, ignore_index=True).reset_index(drop=True)


class SECMetricsBuilder:
  """Build quarterly metrics from facts."""

  def build(self, facts_long: pd.DataFrame) -> pd.DataFrame:
    """
    From minimal facts_long -> per-metric quarterly discrete values + TTM.

    Output columns:
      cik10, metric, end, filed, fy, fp, fiscal_year, q_val, ttm_val, tag
    """
    out_parts: List[pd.DataFrame] = []

    for metric, spec in METRIC_SPECS.items():
      df = facts_long[facts_long['metric'] == metric].copy()
      if df.empty:
        continue

      if bool(spec.get('abs', False)):
        df['val'] = df['val'].abs()

      parts = []
      for cik10, g in df.groupby('cik10'):
        if bool(spec.get('is_ytd', False)):
          qg = self._ytd_to_quarter(g,
                                    value_col='val',
                                    out_col='q_val',
                                    group_by_fiscal_year=True)
          if bool(spec.get('abs', False)):
            qg['q_val'] = qg['q_val'].abs()
        else:
          # Non-YTD metrics: preserve the actual tag for each row
          qg = g.rename(columns={'val': 'q_val'})[[
              'end', 'filed', 'fy', 'fp', 'fiscal_year', 'q_val', 'tag'
          ]].copy()

        qg['cik10'] = cik10
        qg['metric'] = metric
        parts.append(qg)

      non_empty_parts = [p for p in parts if not p.empty]
      if not non_empty_parts:
        continue
      q_all = pd.concat(non_empty_parts,
                        ignore_index=True).sort_values(['cik10', 'end'])

      # Calculate TTM only for flow metrics (is_ytd=True)
      if bool(spec.get('is_ytd', False)):
        q_all['ttm_val'] = (q_all.sort_values([
            'cik10', 'metric', 'end'
        ]).groupby(['cik10', 'metric'
                   ])['q_val'].rolling(4).sum().reset_index(level=[0, 1],
                                                            drop=True))
      else:
        # For point-in-time metrics (like SHARES), ttm_val is not meaningful
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

  def _ytd_to_quarter(self,
                      df_ytd: pd.DataFrame,
                      *,
                      value_col: str = 'val',
                      out_col: str = 'q_val',
                      group_by_fiscal_year: bool = True) -> pd.DataFrame:
    """Convert YTD values to discrete quarterly values."""
    if df_ytd.empty:
      return pd.DataFrame()

    group_col = 'fiscal_year' if group_by_fiscal_year else 'fy'
    if group_col not in df_ytd.columns:
      return pd.DataFrame()

    df_ytd = df_ytd.sort_values(['end'])
    out_rows = []

    for group_val, g in df_ytd.groupby(group_col):
      period_data = {}
      for _, row in g.iterrows():
        fp = str(row['fp'])
        tag = row.get('tag')
        if not tag:
          raise ValueError(
              f"Missing tag for {row.get('cik10')} {row.get('metric')} "
              f"{row.get('end')} {fp}")
        period_data[fp] = {
            'end': row['end'],
            'filed': row['filed'],
            'fy': row.get('fy'),
            'tag': tag,
            value_col: float(row[value_col])
        }

      def _emit_row(fp_key, fp_out, q_value, current_period_data,
                    current_group_val):
        tag = current_period_data[fp_key].get('tag')
        if not tag:
          raise ValueError(f"Missing tag for period {fp_key}")
        row_dict = {
            'end': current_period_data[fp_key]['end'],
            'filed': current_period_data[fp_key]['filed'],
            'fp': fp_out,
            'tag': tag,
            out_col: float(q_value),
        }
        if 'fy' in current_period_data[fp_key]:
          row_dict['fy'] = current_period_data[fp_key]['fy']
        if group_by_fiscal_year:
          row_dict['fiscal_year'] = current_group_val
        else:
          row_dict['fy'] = current_group_val
        out_rows.append(row_dict)

      if 'Q1' in period_data:
        _emit_row('Q1', 'Q1', period_data['Q1'][value_col], period_data,
                  group_val)

      if 'Q2' in period_data:
        if 'Q1' in period_data:
          q2_val = period_data['Q2'][value_col] - period_data['Q1'][value_col]
        else:
          q2_val = period_data['Q2'][value_col]
        _emit_row('Q2', 'Q2', q2_val, period_data, group_val)

      if 'Q3' in period_data:
        if 'Q2' in period_data:
          q3_val = period_data['Q3'][value_col] - period_data['Q2'][value_col]
        elif 'Q1' in period_data:
          q3_val = period_data['Q3'][value_col] - period_data['Q1'][value_col]
        else:
          q3_val = period_data['Q3'][value_col]
        _emit_row('Q3', 'Q3', q3_val, period_data, group_val)

      if 'FY' in period_data:
        if 'Q3' in period_data:
          q4_val = period_data['FY'][value_col] - period_data['Q3'][value_col]
        elif 'Q2' in period_data:
          q4_val = period_data['FY'][value_col] - period_data['Q2'][value_col]
        elif 'Q1' in period_data:
          q4_val = period_data['FY'][value_col] - period_data['Q1'][value_col]
        else:
          q4_val = period_data['FY'][value_col]
        _emit_row('FY', 'Q4', q4_val, period_data, group_val)

    return pd.DataFrame(out_rows)
