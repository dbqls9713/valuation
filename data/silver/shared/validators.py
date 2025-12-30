"""
Shared validators.
"""
import pandas as pd

from data.silver.core.validator import Validator, ValidationResult


class BasicValidator(Validator):
  """Basic validation checks."""

  def validate(self, name: str, df: pd.DataFrame) -> ValidationResult:
    """Run basic validation checks."""
    errors = []
    warnings = []

    if df.empty:
      warnings.append(f'{name}: DataFrame is empty')

    if 'end' in df.columns and 'filed' in df.columns:
      bad = df['filed'] < df['end']
      if bad.any():
        n = int(bad.sum())
        errors.append(f'{name}: {n} rows have filed < end')

    return ValidationResult(is_valid=len(errors) == 0,
                            errors=errors,
                            warnings=warnings)
