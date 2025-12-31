"""
Validator abstraction.
"""
from abc import ABC
from abc import abstractmethod
from dataclasses import dataclass

import pandas as pd


@dataclass
class ValidationResult:
  """Result of validation."""
  is_valid: bool
  errors: list[str]
  warnings: list[str]

class Validator(ABC):
  """Base validator."""

  @abstractmethod
  def validate(self, name: str, df: pd.DataFrame) -> ValidationResult:
    """
    Validate a dataset.

    Args:
        name: Dataset name
        df: DataFrame to validate

    Returns:
        ValidationResult with errors and warnings
    """
    pass
