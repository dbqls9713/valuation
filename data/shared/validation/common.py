"""
Common validators used across data layers.

These validators handle generic checks like schema validation,
primary key uniqueness, and date ordering constraints.
"""
from typing import Any, Protocol

import pandas as pd

from data.shared.validation.base import CheckResult
from data.shared.validation.base import fail_result
from data.shared.validation.base import pass_result


class ColumnSpec(Protocol):
  """Protocol for column specification."""

  name: str
  nullable: bool


class Schema(Protocol):
  """Protocol for schema objects."""

  columns: list[ColumnSpec]
  primary_key: list[str] | None


class SchemaValidator:
  """Validate DataFrame against a schema definition."""

  def __init__(self, schema: Any):
    self.schema = schema

  def validate(self, df: pd.DataFrame, name: str) -> list[CheckResult]:
    """
    Validate DataFrame against schema.

    Checks:
    1. All required columns exist
    2. No null values in non-nullable columns
    """
    results: list[CheckResult] = []

    col_names = [c.name for c in self.schema.columns]
    expected_cols = set(col_names)
    actual_cols = set(df.columns)
    missing = expected_cols - actual_cols

    if missing:
      results.append(
          fail_result(f'{name}_columns', f'Missing columns: {missing}'))
      return results

    extra = actual_cols - expected_cols
    if extra:
      results.append(
          pass_result(f'{name}_columns',
                      f'All columns present (extra: {extra})'))
    else:
      results.append(
          pass_result(f'{name}_columns',
                      f'All {len(expected_cols)} columns present'))

    nullable_errors = []
    for col_spec in self.schema.columns:
      if col_spec.nullable or col_spec.name not in df.columns:
        continue

      null_count = int(df[col_spec.name].isna().sum())
      if null_count > 0:
        nullable_errors.append(f'{col_spec.name}: {null_count} nulls')

      if df[col_spec.name].dtype == 'object':
        empty_count = int((df[col_spec.name] == '').sum())
        if empty_count > 0:
          nullable_errors.append(f'{col_spec.name}: {empty_count} empty')

    if nullable_errors:
      results.append(
          fail_result(f'{name}_nullable',
                      f'Non-nullable violations: {nullable_errors}'))
    else:
      results.append(
          pass_result(f'{name}_nullable',
                      'All non-nullable constraints satisfied'))

    return results


class UniqueKeyValidator:
  """Validate that specified columns form a unique key."""

  def __init__(self, key_cols: list[str]):
    self.key_cols = key_cols

  def validate(self, df: pd.DataFrame, name: str) -> CheckResult:
    """Check that key_cols are unique across all rows."""
    missing_cols = [c for c in self.key_cols if c not in df.columns]
    if missing_cols:
      return fail_result(name, f'Key columns missing: {missing_cols}')

    dup_mask = df.duplicated(self.key_cols, keep=False)
    dup_count = int(dup_mask.sum())

    if dup_count > 0:
      sample = df.loc[dup_mask, self.key_cols].head(5).to_dict(orient='records')
      return fail_result(
          name, f'{dup_count} duplicates by {self.key_cols}. Sample: {sample}')

    return pass_result(name, f'Unique by {self.key_cols}')


class DateOrderValidator:
  """Validate that one date column is >= another."""

  def __init__(self, later_col: str = 'filed', earlier_col: str = 'end'):
    self.later_col = later_col
    self.earlier_col = earlier_col

  def validate(self, df: pd.DataFrame, name: str) -> CheckResult:
    """Check that later_col >= earlier_col for all rows."""
    if self.later_col not in df.columns:
      return fail_result(name, f'Column {self.later_col} not found')
    if self.earlier_col not in df.columns:
      return fail_result(name, f'Column {self.earlier_col} not found')

    bad_mask = df[self.later_col] < df[self.earlier_col]
    bad_count = int(bad_mask.sum())

    if bad_count > 0:
      sample_cols = [self.earlier_col, self.later_col]
      id_cols = ['cik10', 'ticker', 'metric']
      sample_cols = [c for c in id_cols if c in df.columns] + sample_cols
      sample = df.loc[bad_mask, sample_cols].head(5).to_dict(orient='records')
      return fail_result(
          name, f'{bad_count} rows have {self.later_col} < '
          f'{self.earlier_col}. Sample: {sample}')

    return pass_result(
        name, f'All rows satisfy {self.later_col} >= {self.earlier_col}')


class PositiveValueValidator:
  """Validate that a column has positive (or non-negative) values."""

  def __init__(self, column: str, allow_zero: bool = True):
    self.column = column
    self.allow_zero = allow_zero

  def validate(self, df: pd.DataFrame, name: str) -> CheckResult:
    """Check that column values are positive."""
    if self.column not in df.columns:
      return fail_result(name, f'Column {self.column} not found')

    if self.allow_zero:
      bad_mask = df[self.column] < 0
      constraint = '>= 0'
    else:
      bad_mask = df[self.column] <= 0
      constraint = '> 0'

    bad_mask = bad_mask & df[self.column].notna()
    bad_count = int(bad_mask.sum())

    if bad_count > 0:
      s = df.loc[bad_mask, [self.column]].head(10).to_dict(orient='records')
      return fail_result(
          name, f'{bad_count} rows have {self.column} not {constraint}. '
          f'Sample: {s}')

    return pass_result(name, f'All {self.column} values satisfy {constraint}')
