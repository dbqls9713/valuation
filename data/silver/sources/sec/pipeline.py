"""
SEC data processing pipeline.
"""
from pathlib import Path
from typing import List

import pandas as pd

from data.silver.core.pipeline import Pipeline, PipelineContext
from data.silver.sources.sec.extractors import SECCompanyFactsExtractor
from data.silver.sources.sec.transforms import SECFactsTransformer, SECMetricsBuilder
from data.silver.shared.validators import BasicValidator
from data.silver.shared.io import ParquetWriter


class SECPipeline(Pipeline):
  """Pipeline for SEC data processing."""

  def __init__(self, context: PipelineContext):
    super().__init__(context)
    self.sec_dir = context.bronze_dir / 'sec'
    self.out_dir = context.silver_dir / 'sec'

    self.extractor = SECCompanyFactsExtractor()
    self.transformer = SECFactsTransformer()
    self.metrics_builder = SECMetricsBuilder()
    self.validator = BasicValidator()
    self.writer = ParquetWriter()

  def extract(self) -> None:
    """Extract from companyfacts JSON files."""
    cf_files = self._get_companyfact_files()

    companies = self.extractor.extract_companies(
        self.sec_dir / 'company_tickers.json', self.sec_dir / 'submissions')
    self.datasets['companies'] = companies

    facts_list = []
    for cf_file in cf_files:
      try:
        facts = self.extractor.extract_facts(cf_file)
        if not facts.empty:
          facts_list.append(facts)
      except Exception as e:  # pylint: disable=broad-except
        self.errors.append(f'Failed to extract {cf_file.name}: {str(e)}')

    if facts_list:
      self.datasets['facts_raw'] = pd.concat(facts_list, ignore_index=True)
    else:
      self.datasets['facts_raw'] = pd.DataFrame()

  def transform(self) -> None:
    """Apply transformations."""
    facts = self.datasets['facts_raw']
    companies = self.datasets['companies']

    if facts.empty:
      self.datasets['facts_long'] = pd.DataFrame()
      self.datasets['metrics_quarterly'] = pd.DataFrame()
      return

    facts = self.transformer.add_fiscal_year(facts, companies)
    facts = self.transformer.deduplicate(facts)

    self.datasets['facts_long'] = facts

    metrics_q = self.metrics_builder.build(facts)
    self.datasets['metrics_quarterly'] = metrics_q

  def validate(self) -> None:
    """Run validation checks."""
    for name, dataset in self.datasets.items():
      if name == 'facts_raw':
        continue

      validation_result = self.validator.validate(name, dataset)
      if not validation_result.is_valid:
        self.errors.extend(validation_result.errors)

  def load(self) -> None:
    """Write to parquet files."""
    self.out_dir.mkdir(parents=True, exist_ok=True)

    datasets_to_write = {
        'companies': self.datasets.get('companies'),
        'facts_long': self.datasets.get('facts_long'),
        'metrics_quarterly': self.datasets.get('metrics_quarterly'),
    }

    cf_files = self._get_companyfact_files()

    # Calculate target_date as max filed date from facts_long
    target_date = None
    facts_long = self.datasets.get('facts_long')
    has_filed = (facts_long is not None and not facts_long.empty and
                 'filed' in facts_long.columns)
    if has_filed:
      target_date = str(facts_long['filed'].max().date())

    for name, dataset in datasets_to_write.items():
      if dataset is None or dataset.empty:
        continue

      output_path = self.out_dir / f'{name}.parquet'

      metadata = {
          'layer': 'silver',
          'source': 'sec',
          'dataset': name,
      }

      inputs = cf_files if name != 'companies' else [
          self.sec_dir / 'company_tickers.json'
      ]

      self.writer.write(dataset,
                        output_path,
                        inputs=inputs,
                        metadata=metadata,
                        target_date=target_date)

  def _get_companyfact_files(self) -> List[Path]:
    """Get companyfacts files."""
    cf_dir = self.sec_dir / 'companyfacts'
    if not cf_dir.exists():
      return []
    return sorted(p for p in cf_dir.glob('CIK*.json')
                 if not p.name.endswith('.meta.json'))
