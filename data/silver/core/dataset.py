"""
Dataset abstraction with schema validation.
"""
from dataclasses import dataclass
from typing import Optional

import pandas as pd


@dataclass
class ColumnSpec:
  """Column specification."""
  name: str
  dtype: str
  nullable: bool = True
  unique: bool = False
  description: str = ''

@dataclass
class DatasetSchema:
  """Schema for a dataset."""
  name: str
  columns: list[ColumnSpec]
  primary_key: Optional[list[str]] = None
  description: str = ''

class Dataset:
  """Base dataset with schema validation."""

  def __init__(self, schema: DatasetSchema):
    self.schema = schema
    self._data: Optional[pd.DataFrame] = None

  @property
  def data(self) -> pd.DataFrame:
    if self._data is None:
      raise ValueError('Dataset not loaded')
    return self._data

  @data.setter
  def data(self, df: pd.DataFrame):
    self.validate_schema(df)
    self._data = df

  def validate_schema(self, df: pd.DataFrame) -> None:
    """Validate DataFrame against schema."""
    missing = [c.name for c in self.schema.columns if c.name not in df.columns]
    if missing:
      raise ValueError(f'Missing columns: {missing}')

    for col_spec in self.schema.columns:
      if col_spec.nullable or col_spec.name not in df.columns:
        continue

      null_count = df[col_spec.name].isna().sum()
      if null_count > 0:
        raise ValueError(f"Column {col_spec.name} is not nullable but has "
                         f"{null_count} null values")

    if not self.schema.primary_key:
      return

    duplicates = df.duplicated(subset=self.schema.primary_key, keep=False)
    if duplicates.any():
      n_dups = int(duplicates.sum())
      raise ValueError(
          f'Duplicate primary key {self.schema.primary_key}: {n_dups} rows')
