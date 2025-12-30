"""
Validator abstraction.
"""
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List

import pandas as pd


@dataclass
class ValidationResult:
  """Result of validation."""
  is_valid: bool
  errors: List[str]
  warnings: List[str]


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
