"""
Validate Gold layer outputs.

Validates all panels against their schemas:
- Schema column presence
- Primary key uniqueness
- Nullable constraints
- Domain-specific checks (filed >= end, TTM, CAPEX sign, PIT consistency)

Checks (must pass):
1) Schema + types
2) Primary key uniqueness
3) filed >= end date order
4) CAPEX sign (positive after abs())
5) TTM = sum of 4 quarters

Warnings (data quality monitoring):
1) OE positive ratio
2) Quarterly completeness
3) PIT consistency info

Usage:
  python -m data.gold.validate
  python -m data.gold.validate --gold-dir data/gold/out
"""
import argparse
import json
import logging
from pathlib import Path

import pandas as pd

from data.gold.config.schemas import BACKTEST_PANEL_SCHEMA
from data.gold.config.schemas import PanelSchema
from data.gold.config.schemas import VALUATION_PANEL_SCHEMA
from data.gold.validation.capex import CapexSignValidator
from data.gold.validation.completeness import QuarterlyCompletenessValidator
from data.gold.validation.oe import OEPositiveValidator
from data.gold.validation.pit import PITConsistencyValidator
from data.gold.validation.ttm import TTMCorrectnessValidator
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


def _validate_panel(gold_dir: Path, schema: PanelSchema,
                    runner: ValidationRunner, companies: pd.DataFrame,
                    target_date: str | None) -> None:
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

  runner.add_warning(f'{panel_name}_capex',
                     CapexSignValidator().validate, df,
                     f'{panel_name}_capex_sign')

  runner.add_warning(f'{panel_name}_ttm',
                     TTMCorrectnessValidator().validate, df,
                     f'{panel_name}_ttm_check')

  runner.add_warning(f'{panel_name}_oe',
                     OEPositiveValidator().validate, df,
                     f'{panel_name}_oe_positive')

  if panel_name == 'backtest_panel':
    runner.add_warning(f'{panel_name}_pit',
                       PITConsistencyValidator().validate, df,
                       f'{panel_name}_pit_consistency')

  if not companies.empty and target_date:
    runner.add_warning(f'{panel_name}_completeness',
                       QuarterlyCompletenessValidator().validate, companies, df,
                       target_date, f'{panel_name}_quarterly_completeness')


def _load_companies(silver_dir: Path) -> pd.DataFrame:
  """Load companies from Silver layer."""
  path = silver_dir / 'sec' / 'companies.parquet'
  if path.exists():
    return pd.read_parquet(path)
  return pd.DataFrame()


def _load_target_date(silver_dir: Path) -> str | None:
  """Load target date from Silver metadata."""
  meta_path = silver_dir / 'sec' / 'facts_long.parquet.meta.json'
  if meta_path.exists():
    meta = json.loads(meta_path.read_text())
    target_date = meta.get('target_date')
    return str(target_date) if target_date else None
  return None


def main() -> None:
  """CLI entrypoint."""
  parser = argparse.ArgumentParser(description='Validate Gold layer outputs')
  parser.add_argument(
      '--gold-dir',
      type=Path,
      default=Path('data/gold/out'),
      help='Gold layer output directory',
  )
  parser.add_argument(
      '--silver-dir',
      type=Path,
      default=Path('data/silver/out'),
      help='Silver layer output directory (for companies data)',
  )
  args = parser.parse_args()

  logger.info('Validating Gold layer: %s', args.gold_dir)

  companies = _load_companies(args.silver_dir)
  target_date = _load_target_date(args.silver_dir)
  logger.info('Loaded %d companies, target_date=%s', len(companies),
              target_date)

  runner = ValidationRunner('Gold Layer')

  _validate_panel(args.gold_dir, VALUATION_PANEL_SCHEMA, runner, companies,
                  target_date)
  _validate_panel(args.gold_dir, BACKTEST_PANEL_SCHEMA, runner, companies,
                  target_date)

  runner.run()
  runner.log_summary()

  if not runner.all_passed:
    raise SystemExit(1)


if __name__ == '__main__':
  main()
