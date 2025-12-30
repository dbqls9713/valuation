"""
Silver layer build CLI.

Usage:
    python -m data.silver [--sources sec stooq]
                          [--bronze-dir PATH] [--silver-dir PATH]
"""
import argparse
import logging
from pathlib import Path

from data.silver.core.pipeline import PipelineContext
from data.silver.sources.sec.pipeline import SECPipeline
from data.silver.sources.stooq.pipeline import StooqPipeline

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def main():
  parser = argparse.ArgumentParser(description='Build Silver layer')
  parser.add_argument('--sources',
                      nargs='+',
                      choices=['sec', 'stooq'],
                      default=['sec', 'stooq'],
                      help='Sources to build')
  parser.add_argument('--bronze-dir',
                      type=Path,
                      default=Path('data/bronze/out'),
                      help='Bronze directory')
  parser.add_argument('--silver-dir',
                      type=Path,
                      default=Path('data/silver/out'),
                      help='Silver output directory')
  args = parser.parse_args()

  context = PipelineContext(bronze_dir=args.bronze_dir,
                            silver_dir=args.silver_dir)

  pipelines = {}
  if 'sec' in args.sources:
    pipelines['sec'] = SECPipeline(context)
  if 'stooq' in args.sources:
    pipelines['stooq'] = StooqPipeline(context)

  results = {}
  for name, pipeline in pipelines.items():
    logger.info('Running %s pipeline...', name)
    results[name] = pipeline.run()

  success_count = sum(1 for r in results.values() if r.success)
  print()
  print('=' * 70)
  print(f'Build Summary: {success_count}/{len(results)} pipelines succeeded')
  print('=' * 70)

  for name, result in results.items():
    if result.success:
      logger.info('✓ %s pipeline completed', name)
      for dataset_name, df in result.datasets.items():
        logger.info('  %s: %s', dataset_name, df.shape)
    else:
      logger.error('✗ %s pipeline failed', name)
      for error in result.errors:
        logger.error('  %s', error)

  if success_count < len(results):
    raise SystemExit(1)


if __name__ == '__main__':
  main()
