"""
Validate Gold layer outputs.

Validates all panels against their schemas:
- Schema column presence
- Primary key uniqueness
- Nullable constraints
- Domain-specific checks (filed >= end, OE statistics)

Usage:
  python -m data.gold.validate
  python -m data.gold.validate --gold-dir data/gold/out
"""
import argparse
import logging
from pathlib import Path

import pandas as pd

from data.gold.config.schemas import BACKTEST_PANEL_SCHEMA
from data.gold.config.schemas import PanelSchema
from data.gold.config.schemas import VALUATION_PANEL_SCHEMA
from data.shared.validation.base import CheckResult
from data.shared.validation.common import DateOrderValidator
from data.shared.validation.common import SchemaValidator
from data.shared.validation.common import UniqueKeyValidator
from data.shared.validation.runner import ValidationRunner

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logger = logging.getLogger(__name__)


def _check_oe_positive(df: pd.DataFrame, name: str) -> CheckResult:
  """Check Owner Earnings (CFO - CAPEX) positivity rate."""
  if 'cfo_ttm' not in df.columns or 'capex_ttm' not in df.columns:
    return CheckResult(name=name,
                       ok=True,
                       details='OE columns not present (skipped)')

  oe_positive = int(((df['cfo_ttm'] - df['capex_ttm']) > 0).sum())
  total = len(df)
  pct = oe_positive / total * 100 if total > 0 else 0

  return CheckResult(name=name,
                     ok=True,
                     details=f'OE positive: {oe_positive}/{total} ({pct:.1f}%)')


def _validate_panel(gold_dir: Path, schema: PanelSchema,
                    runner: ValidationRunner) -> None:
  """Add validation checks for a panel to the runner."""
  path = gold_dir / f'{schema.name}.parquet'
  panel_name = schema.name

  if not path.exists():
    runner.add_check(f'{panel_name}_exists',
                     lambda n=panel_name, p=path: CheckResult(
                         name=f'{n}_exists',
                         ok=False,
                         details=f'File not found: {p}',
                     ))
    return

  df = pd.read_parquet(path)
  logger.info('Loaded %s: %s', panel_name, df.shape)

  runner.add_check(f'{panel_name}_exists',
                   lambda n=panel_name, s=df.shape: CheckResult(
                       name=f'{n}_exists',
                       ok=True,
                       details=f'Shape: {s}',
                   ))

  runner.add_check(f'{panel_name}_schema',
                   SchemaValidator(schema).validate, df, panel_name)

  if schema.primary_key:
    pk_cols = [c for c in schema.primary_key if c in df.columns]
    if pk_cols:
      runner.add_check(f'{panel_name}_pk',
                       UniqueKeyValidator(pk_cols).validate, df,
                       f'{panel_name}_pk_unique')

  if 'end' in df.columns and 'filed' in df.columns:
    runner.add_check(f'{panel_name}_dates',
                     DateOrderValidator('filed', 'end').validate, df,
                     f'{panel_name}_filed_ge_end')

  runner.add_check(f'{panel_name}_oe', _check_oe_positive, df,
                   f'{panel_name}_oe_positive')


def main() -> None:
  """CLI entrypoint."""
  parser = argparse.ArgumentParser(description='Validate Gold layer outputs')
  parser.add_argument(
      '--gold-dir',
      type=Path,
      default=Path('data/gold/out'),
      help='Gold layer output directory',
  )
  args = parser.parse_args()

  logger.info('Validating Gold layer: %s', args.gold_dir)

  runner = ValidationRunner('Gold Layer')

  _validate_panel(args.gold_dir, VALUATION_PANEL_SCHEMA, runner)
  _validate_panel(args.gold_dir, BACKTEST_PANEL_SCHEMA, runner)

  runner.run()
  runner.log_summary()

  if not runner.all_passed:
    raise SystemExit(1)


if __name__ == '__main__':
  main()
