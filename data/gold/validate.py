"""
Validate Gold layer outputs.

Validates all panels against their schemas:
- Schema column presence
- Primary key uniqueness
- Nullable constraints

Usage:
  python -m data.gold.validate
  python -m data.gold.validate --gold-dir data/gold/out
"""

import argparse
from dataclasses import dataclass
import logging
from pathlib import Path

import pandas as pd

from data.gold.config.schemas import BACKTEST_PANEL_SCHEMA
from data.gold.config.schemas import PanelSchema
from data.gold.config.schemas import VALUATION_PANEL_SCHEMA

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CheckResult:
  """Result of a single validation check."""
  name: str
  ok: bool
  details: str


def validate_panel(
    df: pd.DataFrame,
    schema: PanelSchema,
) -> list[CheckResult]:
  """
  Validate a panel DataFrame against its schema.

  Args:
    df: Panel DataFrame to validate
    schema: Schema to validate against

  Returns:
    List of CheckResult objects
  """
  results = []

  expected_cols = set(schema.column_names())
  actual_cols = set(df.columns)
  missing = expected_cols - actual_cols
  extra = actual_cols - expected_cols

  if missing:
    results.append(
        CheckResult(
            name=f'{schema.name}_columns',
            ok=False,
            details=f'Missing columns: {missing}',
        ))
  elif extra:
    results.append(
        CheckResult(
            name=f'{schema.name}_columns',
            ok=True,
            details=f'Extra columns (ok): {extra}',
        ))
  else:
    results.append(
        CheckResult(
            name=f'{schema.name}_columns',
            ok=True,
            details=f'All {len(expected_cols)} columns present',
        ))

  if schema.primary_key:
    pk_cols = [c for c in schema.primary_key if c in df.columns]
    if pk_cols:
      duplicates = df.duplicated(subset=pk_cols, keep=False).sum()
      results.append(
          CheckResult(
              name=f'{schema.name}_pk_unique',
              ok=duplicates == 0,
              details=f'Primary key duplicates: {duplicates}',
          ))

  for col_spec in schema.columns:
    if col_spec.name not in df.columns:
      continue
    if col_spec.nullable:
      continue

    null_count = df[col_spec.name].isna().sum()
    results.append(
        CheckResult(
            name=f'{schema.name}_{col_spec.name}_not_null',
            ok=null_count == 0,
            details=f'Null count: {null_count}',
        ))

  return results


def _validate_panel_file(
    gold_dir: Path,
    schema: PanelSchema,
) -> list[CheckResult]:
  """Validate a panel parquet file against its schema."""
  path = gold_dir / f'{schema.name}.parquet'
  panel_name = schema.name

  if not path.exists():
    return [
        CheckResult(
            name=f'{panel_name}_exists',
            ok=False,
            details=f'File not found: {path}',
        )
    ]

  df = pd.read_parquet(path)
  results = [
      CheckResult(
          name=f'{panel_name}_exists',
          ok=True,
          details=f'Shape: {df.shape}',
      )
  ]

  results.extend(validate_panel(df, schema))

  if 'end' in df.columns and 'filed' in df.columns:
    invalid = (df['filed'] < df['end']).sum()
    results.append(
        CheckResult(
            name=f'{panel_name}_filed_ge_end',
            ok=invalid == 0,
            details=f'Rows with filed < end: {invalid}',
        ))

  if 'cfo_ttm' in df.columns and 'capex_ttm' in df.columns:
    oe_positive = ((df['cfo_ttm'] - df['capex_ttm']) > 0).sum()
    total = len(df)
    pct = oe_positive / total * 100 if total > 0 else 0
    results.append(
        CheckResult(
            name=f'{panel_name}_oe_positive',
            ok=True,
            details=f'OE positive: {oe_positive}/{total} ({pct:.1f}%)',
        ))

  return results


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

  all_results: list[CheckResult] = []
  all_results.extend(_validate_panel_file(args.gold_dir,
                                          VALUATION_PANEL_SCHEMA))
  all_results.extend(_validate_panel_file(args.gold_dir, BACKTEST_PANEL_SCHEMA))

  passed = sum(1 for r in all_results if r.ok)
  failed = sum(1 for r in all_results if not r.ok)

  logger.info('')
  logger.info('=' * 70)
  logger.info('Validation Results: %d passed, %d failed', passed, failed)
  logger.info('=' * 70)

  for result in all_results:
    status = '✓' if result.ok else '✗'
    level = logging.INFO if result.ok else logging.ERROR
    logger.log(level, '%s %s: %s', status, result.name, result.details)

  if failed > 0:
    raise SystemExit(1)


if __name__ == '__main__':
  main()
