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

      # Normalize to actual count if specified (for SHARES)
      if bool(spec.get('normalize_to_actual_count', False)):
        df['val'] = self._normalize_shares_to_actual_count(df['val'])

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
