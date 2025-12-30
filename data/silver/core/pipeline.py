"""
Pipeline abstraction for Silver layer data processing.
"""
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List

import pandas as pd


@dataclass
class PipelineContext:
  """Context shared across pipeline stages."""
  bronze_dir: Path
  silver_dir: Path
  metadata: Dict = field(default_factory=dict)


@dataclass
class PipelineResult:
  """Result of pipeline execution."""
  success: bool
  datasets: Dict[str, pd.DataFrame]
  metadata: Dict
  errors: List[str]


class Pipeline(ABC):
  """Base pipeline for data processing."""

  def __init__(self, context: PipelineContext):
    self.context = context
    self.datasets: Dict[str, pd.DataFrame] = {}
    self.errors: List[str] = []

  @abstractmethod
  def extract(self) -> None:
    """Extract data from bronze layer."""
    pass

  @abstractmethod
  def transform(self) -> None:
    """Transform extracted data."""
    pass

  @abstractmethod
  def validate(self) -> None:
    """Validate transformed data."""
    pass

  @abstractmethod
  def load(self) -> None:
    """Load data to silver layer."""
    pass

  def run(self) -> PipelineResult:
    """Execute full pipeline."""
    try:
      self.extract()
      self.transform()
      self.validate()
      self.load()
      return PipelineResult(success=len(self.errors) == 0,
                            datasets=self.datasets,
                            metadata=self.context.metadata,
                            errors=self.errors)
    except Exception as e:  # pylint: disable=broad-except
      self.errors.append(str(e))
      return PipelineResult(success=False,
                            datasets=self.datasets,
                            metadata=self.context.metadata,
                            errors=self.errors)
