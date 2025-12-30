"""
Validate Silver layer outputs.

Validates:
- SEC facts_long + metrics_quarterly
- Stooq prices_daily

Checks:
1) Schema + types
2) Key uniqueness
3) filed >= end
4) YTD->Quarter identity vs facts_long (core correctness)
5) TTM correctness
6) CAPEX abs convention (>=0)
7) Optional: manual spotcheck fixture file

Run:
  python -m data.silver.validate
  python -m data.silver.validate --silver-dir data/silver/out
  python -m data.silver.validate --with-manual
"""

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import List

import pandas as pd

from data.silver.config.metric_specs import METRIC_SPECS
from data.silver.config.schemas import (FACTS_LONG_SCHEMA,
                                        METRICS_QUARTERLY_SCHEMA,
                                        METRICS_QUARTERLY_HISTORY_SCHEMA,
                                        PRICES_DAILY_SCHEMA)


@dataclass(frozen=True)
class CheckResult:
  name: str
  ok: bool
  details: str


def _require_columns(df: pd.DataFrame, cols: List[str], name: str) -> None:
  missing = [c for c in cols if c not in df.columns]
  if missing:
    raise ValueError(f'[{name}] missing columns: {missing}')


def _check_schema(df: pd.DataFrame, schema, name: str) -> CheckResult:
  """
  Validate DataFrame against schema definition.

  Checks:
  1. All required columns exist
  2. No null values in non-nullable columns
  """
  errors = []

  # Check missing columns
  missing = [c.name for c in schema.columns if c.name not in df.columns]
  if missing:
    return CheckResult(f'{name}_schema', False, f'Missing columns: {missing}')

  # Check nullable constraints
  for col_spec in schema.columns:
    if col_spec.nullable or col_spec.name not in df.columns:
      continue

    null_count = df[col_spec.name].isna().sum()
    empty_count = (df[col_spec.name]
                   == '').sum() if df[col_spec.name].dtype == 'object' else 0

    if null_count > 0:
      errors.append(f'{col_spec.name}: {null_count} null values')

    if empty_count > 0:
      errors.append(f'{col_spec.name}: {empty_count} empty strings')

  if errors:
    return CheckResult(f'{name}_schema', False,
                       f'Non-nullable fields have invalid values: {errors}')

  return CheckResult(f'{name}_schema', True, 'All schema constraints satisfied')


def _assert_unique(df: pd.DataFrame, key_cols: List[str],
                   name: str) -> CheckResult:
  dup = df.duplicated(key_cols, keep=False)
  if dup.any():
    n = int(dup.sum())
    sample = df.loc[dup, key_cols].head(5).to_dict(orient='records')
    msg = f'Found {n} duplicate rows by key {key_cols}. Sample: {sample}'
    return CheckResult(name, False, msg)
  return CheckResult(name, True, f'Unique by {key_cols}')


def _assert_filed_ge_end(df: pd.DataFrame, name: str) -> CheckResult:
  bad = df['filed'] < df['end']
  if bad.any():
    n = int(bad.sum())
    cols = ['cik10', 'metric', 'end', 'filed', 'fp']
    sample = df.loc[bad, cols].head(5).to_dict(orient='records')
    return CheckResult(name, False,
                       f'{n} rows have filed < end. Sample: {sample}')
  return CheckResult(name, True, 'All rows satisfy filed >= end')


def _check_ytd_identity(facts: pd.DataFrame, metrics_q: pd.DataFrame,
                        tol: float) -> CheckResult:
  """
  Compare facts_long YTD with reconstructed YTD from quarterly q_val.

  Uses fiscal_year (not fy) for grouping to avoid comparative period
  mixing.

  We expect facts_long to contain fp in {Q1,Q2,Q3,FY} for cashflow YTD.
  metrics_quarterly contains fp in {Q1,Q2,Q3,Q4}.
  """
  if ('fiscal_year' not in facts.columns or
      'fiscal_year' not in metrics_q.columns):
    return CheckResult(
        'ytd_identity', False,
        'Missing fiscal_year column in facts_long or metrics_quarterly')

  f = facts[['cik10', 'metric', 'end', 'fiscal_year', 'fp', 'val']].copy()
  m = metrics_q[['cik10', 'metric', 'end', 'fiscal_year', 'fp', 'q_val']].copy()

  f['fp'] = f['fp'].astype(str)
  m['fp'] = m['fp'].astype(str)

  ytd_metrics = [m for m, s in METRIC_SPECS.items() if s.get('is_ytd', False)]
  f_ytd = f[f['metric'].isin(ytd_metrics)].copy()

  for metric in ytd_metrics:
    if METRIC_SPECS[metric].get('abs', False):
      f_ytd.loc[f_ytd['metric'] == metric,
                'val'] = f_ytd.loc[f_ytd['metric'] == metric, 'val'].abs()

  mq = m.copy()
  mq = mq[mq['fp'].isin(['Q1', 'Q2', 'Q3', 'Q4'])]

  pivot = (
      mq.pivot_table(
          index=['cik10', 'metric', 'fiscal_year'],
          columns='fp',
          values='q_val',
          aggfunc='last'  # type: ignore[arg-type]
      ).reset_index())

  merged = f_ytd.merge(pivot, on=['cik10', 'metric', 'fiscal_year'], how='left')

  def recon_ytd(row: pd.Series) -> float:
    q1 = row.get('Q1')
    q2 = row.get('Q2')
    q3 = row.get('Q3')
    q4 = row.get('Q4')

    fp = row['fp']
    if fp == 'Q1':
      q1_val = (
          float(q1)  # type: ignore[arg-type]
          if pd.notna(q1) else float('nan'))
      return q1_val
    if fp == 'Q2':
      val = (
          float(q1 + q2)  # type: ignore[operator]
          if pd.notna(q1) and pd.notna(q2) else float('nan'))
      return val
    if fp == 'Q3':
      all_present = pd.notna(q1) and pd.notna(q2) and pd.notna(q3)
      q3_val = (
          float(q1 + q2 + q3)  # type: ignore[operator]
          if all_present else float('nan'))
      return q3_val
    if fp == 'FY':
      all_present = (pd.notna(q1) and pd.notna(q2) and pd.notna(q3) and
                     pd.notna(q4))
      fy_val = (
          float(q1 + q2 + q3 + q4)  # type: ignore[operator]
          if all_present else float('nan'))
      return fy_val
    return float('nan')

  merged['recon'] = merged.apply(recon_ytd, axis=1)

  comp = merged[pd.notna(merged['recon'])].copy()
  if comp.empty:
    return CheckResult('ytd_identity', False,
                       'No comparable rows found (missing quarters/Q4).')

  comp['diff'] = (comp['val'] - comp['recon']).abs()
  bad = comp['diff'] > tol

  if bad.any():
    n = int(bad.sum())
    sample = comp.loc[
        bad,
        ['cik10', 'metric', 'fiscal_year', 'fp', 'end', 'val', 'recon', 'diff'
        ]].head(10)
    return CheckResult(
        'ytd_identity',
        False,
        f'{n}/{len(comp)} rows fail YTD identity (tol={tol}). '
        f'Sample:\n{sample.to_string(index=False)}',
    )

  return CheckResult(
      'ytd_identity', True,
      f'All comparable rows pass YTD identity (tol={tol}). '
      f'Compared rows={len(comp)}')


def _check_ttm(metrics_q: pd.DataFrame, tol: float) -> CheckResult:
  m = metrics_q.sort_values(['cik10', 'metric', 'end']).copy()
  rolling_result = (m.groupby(
      ['cik10', 'metric'])['q_val'].rolling(4).sum().reset_index(level=[0, 1],
                                                                 drop=True))
  m['ttm_recomputed'] = rolling_result

  comp = m[pd.notna(m['ttm_val']) & pd.notna(m['ttm_recomputed'])].copy()
  if comp.empty:
    return CheckResult('ttm_check', False,
                       'No comparable TTM rows found (need >=4 quarters).')

  comp['diff'] = (comp['ttm_val'] - comp['ttm_recomputed']).abs()
  bad = comp['diff'] > tol
  if bad.any():
    n = int(bad.sum())
    sample = comp.loc[
        bad,
        ['cik10', 'metric', 'end', 'ttm_val', 'ttm_recomputed', 'diff']].head(
            10)
    return CheckResult(
        'ttm_check', False, f'{n}/{len(comp)} rows fail TTM check (tol={tol}). '
        f'Sample:\n{sample.to_string(index=False)}')
  return CheckResult(
      'ttm_check', True, f'All comparable rows pass TTM check (tol={tol}). '
      f'Compared rows={len(comp)}')


def _check_capex_abs(metrics_q: pd.DataFrame, eps: float) -> CheckResult:
  cap = metrics_q[metrics_q['metric'] == 'CAPEX'].copy()
  if cap.empty:
    return CheckResult('capex_abs', True, 'No CAPEX rows (skipped).')
  bad = cap['q_val'] < -eps
  if bad.any():
    n = int(bad.sum())
    cols = ['cik10', 'end', 'q_val']
    sample = cap.loc[bad, cols].head(10).to_dict(orient='records')
    msg = f'{n} CAPEX rows have q_val < 0 (eps={eps}). Sample: {sample}'
    return CheckResult('capex_abs', False, msg)
  return CheckResult('capex_abs', True, f'All CAPEX q_val >= -{eps}')


def _check_prices(prices: pd.DataFrame) -> List[CheckResult]:
  results: List[CheckResult] = []
  _require_columns(prices,
                   ['symbol', 'date', 'open', 'high', 'low', 'close', 'volume'],
                   'prices_daily')

  results.append(
      _assert_unique(prices, ['symbol', 'date'], 'prices_unique_symbol_date'))

  bad = prices['close'] <= 0
  if bad.any():
    n = int(bad.sum())
    sample = prices.loc[bad, ['symbol', 'date', 'close']].head(10).to_dict(
        orient='records')
    results.append(
        CheckResult('prices_positive_close', False,
                    f'{n} rows have close <= 0. Sample: {sample}'))
  else:
    results.append(CheckResult('prices_positive_close', True, 'All close > 0'))

  return results


def _check_quarterly_completeness(
    companies: pd.DataFrame,
    metrics_q: pd.DataFrame,
    target_date: str,
) -> CheckResult:
  """
  Check if each company has all expected quarters.

  Verifies quarterly data coverage from first_filing_date to target_date.

  Args:
      companies: Companies with first_filing_date and fye_mmdd
      metrics_q: Metrics quarterly DataFrame
      target_date: Target date string (YYYY-MM-DD)
  """
  if 'first_filing_date' not in companies.columns:
    return CheckResult('quarterly_completeness', False,
                       'Missing first_filing_date in companies table')

  target_dt = pd.to_datetime(target_date)
  issues = []

  for _, company in companies.iterrows():
    cik10 = company['cik10']
    first_filing = pd.to_datetime(company['first_filing_date'])

    if pd.isna(first_filing):
      continue

    company_metrics = metrics_q[metrics_q['cik10'] == cik10]
    if company_metrics.empty:
      issues.append(f'{cik10}: No metrics data')
      continue

    # Generate expected quarters from first_filing_date to target_date
    expected_quarters = []
    current = first_filing
    while current <= target_dt:
      expected_quarters.append(current)
      current = current + pd.DateOffset(months=3)

    # Check actual quarters
    actual_quarters = set(company_metrics['end'].dt.to_period('Q'))
    expected_quarter_periods = set(
        pd.to_datetime(expected_quarters).to_period('Q'))

    missing = expected_quarter_periods - actual_quarters
    threshold = len(expected_quarter_periods) * 0.25
    if len(missing) > threshold:  # Allow 25% missing
      msg = (f'{cik10}: Missing {len(missing)}/'
             f'{len(expected_quarter_periods)} quarters')
      issues.append(msg)

  if issues:
    sample = issues[:10]
    msg = (f'{len(issues)} companies missing significant quarters. '
           f'Sample: {sample}')
    return CheckResult('quarterly_completeness', False, msg)

  return CheckResult('quarterly_completeness', True,
                     'All companies have reasonable quarterly coverage')


def _manual_spotcheck(metrics_q: pd.DataFrame, fixture_path: Path,
                      tol: float) -> CheckResult:
  """
  Fixture CSV schema:
    cik10, metric, end, expected_val
  optional: note, source_url
  """
  fx = pd.read_csv(fixture_path)
  _require_columns(fx, ['cik10', 'metric', 'end', 'expected_val'],
                   'manual_fixture')
  fx['end'] = pd.to_datetime(fx['end'])

  m = metrics_q.copy()
  m['end'] = pd.to_datetime(m['end'])

  merged = fx.merge(m, on=['cik10', 'metric', 'end'], how='left')
  missing = merged['q_val'].isna()
  if missing.any():
    n = int(missing.sum())
    cols = ['cik10', 'metric', 'end']
    sample_missing = (merged.loc[missing,
                                 cols].head(10).to_dict(orient='records'))
    msg = (f'{n} fixture rows not found in metrics_quarterly. '
           f'Sample: {sample_missing}')
    return CheckResult('manual_spotcheck', False, msg)

  merged['diff'] = (merged['q_val'] - merged['expected_val']).abs()
  bad = merged['diff'] > tol
  if bad.any():
    n = int(bad.sum())
    sample_bad: pd.DataFrame = merged.loc[
        bad,
        ['cik10', 'metric', 'end', 'expected_val', 'q_val', 'diff']].head(10)
    return CheckResult(
        'manual_spotcheck',
        False,
        f'{n}/{len(merged)} fixture rows mismatch (tol={tol}). '
        f'Sample:\n{sample_bad.to_string(index=False)}',
    )
  return CheckResult('manual_spotcheck', True,
                     f'All fixture rows match (tol={tol}). Rows={len(merged)}')


def main() -> None:
  ap = argparse.ArgumentParser(description='Validate Silver layer outputs')
  ap.add_argument('--silver-dir',
                  type=Path,
                  default=Path('data/silver/out'),
                  help='Silver directory to validate')
  ap.add_argument('--tol',
                  type=float,
                  default=1e-6,
                  help='absolute tolerance for numeric checks')
  ap.add_argument('--capex-eps',
                  type=float,
                  default=1e-9,
                  help='epsilon for CAPEX >= 0 check')
  ap.add_argument('--with-manual',
                  action='store_true',
                  help='run manual fixture spotcheck if fixture exists')
  args = ap.parse_args()

  silver_dir = args.silver_dir
  sec_dir = silver_dir / 'sec'
  stooq_dir = silver_dir / 'stooq'

  facts_path = sec_dir / 'facts_long.parquet'
  metrics_q_path = sec_dir / 'metrics_quarterly.parquet'
  metrics_h_path = sec_dir / 'metrics_quarterly_history.parquet'
  prices_path = stooq_dir / 'prices_daily.parquet'
  manual_fixture_path = Path('data/validation/sec_manual_spotcheck.csv')

  if not facts_path.exists():
    print(f'Error: {facts_path} not found')
    raise SystemExit(1)

  if not metrics_q_path.exists():
    print(f'Error: {metrics_q_path} not found')
    raise SystemExit(1)

  facts = pd.read_parquet(facts_path)
  metrics_q = pd.read_parquet(metrics_q_path)
  metrics_h = pd.read_parquet(
      metrics_h_path) if metrics_h_path.exists() else pd.DataFrame()
  prices = pd.read_parquet(
      prices_path) if prices_path.exists() else pd.DataFrame()

  # Load companies and metadata
  companies_path = sec_dir / 'companies.parquet'
  if companies_path.exists():
    companies = pd.read_parquet(companies_path)
  else:
    companies = pd.DataFrame()

  # Load target_date from metadata
  meta_path = metrics_q_path.with_suffix(metrics_q_path.suffix + '.meta.json')
  target_date = None
  if meta_path.exists():
    meta = json.loads(meta_path.read_text())
    target_date = meta.get('target_date')

  results: List[CheckResult] = []

  print(f'Validating Silver layer: {silver_dir}')
  print(f'  Facts: {facts.shape}')
  print(f'  Metrics (latest): {metrics_q.shape}')
  print(f'  Metrics (history): {metrics_h.shape}')
  print(f"  Prices: {prices.shape if not prices.empty else 'N/A'}")
  print(f"  Companies: {companies.shape if not companies.empty else 'N/A'}")
  print(f"  Target date: {target_date or 'N/A'}")

  print()

  # --- Schema validation ---
  results.append(_check_schema(facts, FACTS_LONG_SCHEMA, 'facts_long'))
  results.append(
      _check_schema(metrics_q, METRICS_QUARTERLY_SCHEMA, 'metrics_quarterly'))
  if not metrics_h.empty:
    results.append(
        _check_schema(metrics_h, METRICS_QUARTERLY_HISTORY_SCHEMA,
                      'metrics_quarterly_history'))
  if not prices.empty:
    results.append(_check_schema(prices, PRICES_DAILY_SCHEMA, 'prices_daily'))

  # --- SEC tables ---
  _require_columns(facts, [
      'cik10', 'metric', 'namespace', 'tag', 'unit', 'end', 'filed', 'fy', 'fp',
      'form', 'val'
  ], 'facts_long')
  _require_columns(metrics_q, [
      'cik10', 'metric', 'end', 'filed', 'fy', 'fp', 'q_val', 'ttm_val', 'tag'
  ], 'metrics_quarterly')

  results.append(
      _assert_unique(facts, ['cik10', 'metric', 'end', 'fy', 'fp'],
                     'facts_unique_period'))
  results.append(
      _assert_unique(metrics_q, ['cik10', 'metric', 'end', 'fp'],
                     'metrics_unique_period'))
  if not metrics_h.empty:
    results.append(
        _assert_unique(metrics_h, ['cik10', 'metric', 'end', 'fp', 'filed'],
                       'metrics_history_unique'))
  results.append(_assert_filed_ge_end(facts, 'facts_filed_ge_end'))
  results.append(_assert_filed_ge_end(metrics_q, 'metrics_filed_ge_end'))
  if not metrics_h.empty:
    results.append(
        _assert_filed_ge_end(metrics_h, 'metrics_history_filed_ge_end'))

  results.append(_check_ytd_identity(facts, metrics_q, tol=float(args.tol)))
  results.append(_check_ttm(metrics_q, tol=float(args.tol)))
  results.append(_check_capex_abs(metrics_q, eps=float(args.capex_eps)))

  # --- Quarterly completeness ---
  if not companies.empty and target_date:
    results.append(
        _check_quarterly_completeness(companies, metrics_q, target_date))

  # --- Prices ---
  if not prices.empty:
    results.extend(_check_prices(prices))

  # --- Manual fixture (optional) ---
  if args.with_manual and manual_fixture_path.exists():
    results.append(
        _manual_spotcheck(metrics_q, manual_fixture_path, tol=float(args.tol)))

  # Print summary
  ok = all(r.ok for r in results)
  print('=' * 70)
  print('=== Silver Validation Summary ===')
  print('=' * 70)
  for r in results:
    status = '✓ OK  ' if r.ok else '✗ FAIL'
    print(f'{status} {r.name}')
    if not r.ok or '--verbose' in str(args):
      print(f'       {r.details}')

  print('=' * 70)
  passed = sum(1 for r in results if r.ok)
  print(f'Results: {passed}/{len(results)} checks passed')
  print('=' * 70)

  if not ok:
    raise SystemExit(1)
  else:
    print('\n✓ All validation checks passed!')


if __name__ == '__main__':
  main()
