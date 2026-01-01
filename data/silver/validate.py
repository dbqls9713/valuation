"""
Validate Silver layer outputs.

Validates:
- SEC facts_long + metrics_quarterly
- Stooq prices_daily

Checks (must pass - code correctness):
1) Schema + types
2) Primary key uniqueness
3) filed >= end date order
4) TTM = Q1+Q2+Q3+Q4 correctness
5) CAPEX sign convention (<=0)
6) PIT data consistency

Warnings (data quality monitoring):
1) YTD identity (SEC data cross-filing consistency)
2) Quarterly completeness (data coverage)

Run:
  python -m data.silver.validate
  python -m data.silver.validate --silver-dir data/silver/out
  python -m data.silver.validate --with-manual
"""
import argparse
import json
from pathlib import Path

import pandas as pd

from data.shared.validation.base import pass_result
from data.shared.validation.common import DateOrderValidator
from data.shared.validation.common import PositiveValueValidator
from data.shared.validation.common import SchemaValidator
from data.shared.validation.common import UniqueKeyValidator
from data.shared.validation.runner import ValidationRunner
from data.silver.config.schemas import FACTS_LONG_SCHEMA
from data.silver.config.schemas import METRICS_QUARTERLY_SCHEMA
from data.silver.config.schemas import PRICES_DAILY_SCHEMA
from data.silver.validation.capex import CapexSignValidator
from data.silver.validation.completeness import QuarterlyCompletenessValidator
from data.silver.validation.manual import ManualSpotcheckValidator
from data.silver.validation.pit import PITConsistencyValidator
from data.silver.validation.ttm import TTMCorrectnessValidator
from data.silver.validation.ytd import YTDIdentityValidator


def _load_data(
    silver_dir: Path
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, str | None]:
  """Load Silver layer data files."""
  sec_dir = silver_dir / 'sec'
  stooq_dir = silver_dir / 'stooq'

  facts_path = sec_dir / 'facts_long.parquet'
  metrics_q_path = sec_dir / 'metrics_quarterly.parquet'
  prices_path = stooq_dir / 'prices_daily.parquet'
  companies_path = sec_dir / 'companies.parquet'

  if not facts_path.exists():
    raise FileNotFoundError(f'{facts_path} not found')
  if not metrics_q_path.exists():
    raise FileNotFoundError(f'{metrics_q_path} not found')

  facts = pd.read_parquet(facts_path)
  metrics_q = pd.read_parquet(metrics_q_path)
  prices = (pd.read_parquet(prices_path)
            if prices_path.exists() else pd.DataFrame())
  companies = (pd.read_parquet(companies_path)
               if companies_path.exists() else pd.DataFrame())

  meta_path = metrics_q_path.with_suffix(metrics_q_path.suffix + '.meta.json')
  target_date = None
  if meta_path.exists():
    meta = json.loads(meta_path.read_text())
    target_date = meta.get('target_date')

  return facts, metrics_q, prices, companies, target_date


def _add_facts_checks(runner: ValidationRunner, facts: pd.DataFrame) -> None:
  """Add facts_long validation checks."""
  runner.add_check('facts_schema',
                   SchemaValidator(FACTS_LONG_SCHEMA).validate, facts,
                   'facts_long')

  pk = ['cik10', 'metric', 'fiscal_year', 'fiscal_quarter', 'filed']
  runner.add_check('facts_pk',
                   UniqueKeyValidator(pk).validate, facts,
                   'facts_unique_period')

  runner.add_check('facts_dates',
                   DateOrderValidator('filed', 'end').validate, facts,
                   'facts_filed_ge_end')


def _add_metrics_checks(runner: ValidationRunner, metrics_q: pd.DataFrame,
                        facts: pd.DataFrame, tolerance: float,
                        capex_eps: float) -> None:
  """Add metrics_quarterly validation checks."""
  runner.add_check('metrics_schema',
                   SchemaValidator(METRICS_QUARTERLY_SCHEMA).validate,
                   metrics_q, 'metrics_quarterly')

  pk = ['cik10', 'metric', 'fiscal_year', 'fiscal_quarter', 'filed']
  runner.add_check('metrics_pk',
                   UniqueKeyValidator(pk).validate, metrics_q,
                   'metrics_unique_period')

  runner.add_check('metrics_dates',
                   DateOrderValidator('filed', 'end').validate, metrics_q,
                   'metrics_filed_ge_end')

  runner.add_check('ttm_check',
                   TTMCorrectnessValidator(tolerance).validate, metrics_q,
                   'ttm_check')

  runner.add_check('capex_sign',
                   CapexSignValidator(capex_eps).validate, metrics_q,
                   'capex_abs')

  runner.add_check('pit_consistency',
                   PITConsistencyValidator().validate, metrics_q,
                   'pit_consistency')

  # Data quality warnings (SEC data issues, not our code)
  runner.add_warning('ytd_identity',
                     YTDIdentityValidator(tolerance).validate, facts, metrics_q,
                     'ytd_identity')


def _add_prices_checks(runner: ValidationRunner, prices: pd.DataFrame) -> None:
  """Add prices_daily validation checks."""
  if prices.empty:
    runner.add_check(
        'prices_skip',
        lambda: pass_result('prices_daily', 'No prices data (skipped)'))
    return

  runner.add_check('prices_schema',
                   SchemaValidator(PRICES_DAILY_SCHEMA).validate, prices,
                   'prices_daily')

  runner.add_check('prices_pk',
                   UniqueKeyValidator(['symbol', 'date']).validate, prices,
                   'prices_unique_symbol_date')

  runner.add_check('prices_close',
                   PositiveValueValidator('close', allow_zero=False).validate,
                   prices, 'prices_positive_close')


def main() -> None:
  """CLI entrypoint."""
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

  print(f'Validating Silver layer: {args.silver_dir}')

  try:
    facts, metrics_q, prices, companies, target_date = _load_data(
        args.silver_dir)
  except FileNotFoundError as e:
    print(f'Error: {e}')
    raise SystemExit(1) from e

  print(f'  Facts: {facts.shape}')
  print(f'  Metrics: {metrics_q.shape}')
  print(f"  Prices: {prices.shape if not prices.empty else 'N/A'}")
  print(f"  Companies: {companies.shape if not companies.empty else 'N/A'}")
  print(f"  Target date: {target_date or 'N/A'}")
  print()

  runner = ValidationRunner('Silver Layer')

  _add_facts_checks(runner, facts)
  _add_metrics_checks(runner, metrics_q, facts, args.tol, args.capex_eps)
  _add_prices_checks(runner, prices)

  # Data coverage warning
  if not companies.empty and target_date:
    runner.add_warning('completeness',
                       QuarterlyCompletenessValidator().validate, companies,
                       metrics_q, target_date, 'quarterly_completeness')

  manual_fixture_path = Path('data/validation/sec_manual_spotcheck.csv')
  if args.with_manual and manual_fixture_path.exists():
    runner.add_check('manual',
                     ManualSpotcheckValidator(args.tol).validate, metrics_q,
                     manual_fixture_path, 'manual_spotcheck')

  runner.run()
  runner.print_summary()

  if runner.all_passed:
    print('\n✓ All validation checks passed!')
  else:
    raise SystemExit(1)


if __name__ == '__main__':
  main()
