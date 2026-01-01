"""
Validate Silver layer outputs.

Silver layer contains normalized data only:
- facts_long: YTD values as-is from SEC filings
- prices_daily: Daily prices from Stooq
- companies: Company metadata

Checks:
1) Schema + types
2) Primary key uniqueness
3) filed >= end date order

Run:
  python -m data.silver.validate
  python -m data.silver.validate --silver-dir data/silver/out
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
from data.silver.config.schemas import PRICES_DAILY_SCHEMA


def _load_data(
    silver_dir: Path
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, str | None]:
  """Load Silver layer data files."""
  sec_dir = silver_dir / 'sec'
  stooq_dir = silver_dir / 'stooq'

  facts_path = sec_dir / 'facts_long.parquet'
  prices_path = stooq_dir / 'prices_daily.parquet'
  companies_path = sec_dir / 'companies.parquet'

  if not facts_path.exists():
    raise FileNotFoundError(f'{facts_path} not found')

  facts = pd.read_parquet(facts_path)
  prices = (pd.read_parquet(prices_path)
            if prices_path.exists() else pd.DataFrame())
  companies = (pd.read_parquet(companies_path)
               if companies_path.exists() else pd.DataFrame())

  meta_path = facts_path.with_suffix(facts_path.suffix + '.meta.json')
  target_date = None
  if meta_path.exists():
    meta = json.loads(meta_path.read_text())
    target_date = meta.get('target_date')

  return facts, prices, companies, target_date


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

  capex_data = facts[facts['metric'] == 'CAPEX']
  if not capex_data.empty:
    runner.add_check('facts_capex_sign',
                     PositiveValueValidator('val', allow_zero=True).validate,
                     capex_data, 'facts_capex_abs')


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
  args = ap.parse_args()

  print(f'Validating Silver layer: {args.silver_dir}')

  try:
    facts, prices, companies, target_date = _load_data(args.silver_dir)
  except FileNotFoundError as e:
    print(f'Error: {e}')
    raise SystemExit(1) from e

  print(f'  Facts: {facts.shape}')
  print(f"  Prices: {prices.shape if not prices.empty else 'N/A'}")
  print(f"  Companies: {companies.shape if not companies.empty else 'N/A'}")
  print(f"  Target date: {target_date or 'N/A'}")
  print()

  runner = ValidationRunner('Silver Layer')

  _add_facts_checks(runner, facts)
  _add_prices_checks(runner, prices)

  runner.run()
  runner.print_summary()

  if runner.all_passed:
    print('\n✓ All validation checks passed!')
  else:
    raise SystemExit(1)


if __name__ == '__main__':
  main()
