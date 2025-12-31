"""
Run backtest using scenario config files (JSON/YAML).

This script enables large-scale experiments by:
1. Loading multiple scenario configs from JSON files
2. Running backtests across all scenarios
3. Comparing results side-by-side

Usage:
  # Single config file
  python -m valuation.analysis.backtest_from_configs \
    --ticker AAPL \
    --start-date 2020-01-01 \
    --end-date 2024-12-31 \
    --configs scenarios/base/default.json

  # Multiple config files
  python -m valuation.analysis.backtest_from_configs \
    --ticker AAPL \
    --start-date 2020-01-01 \
    --end-date 2024-12-31 \
    --configs scenarios/capex_experiments/*.json

  # All configs in a directory
  python -m valuation.analysis.backtest_from_configs \
    --ticker AAPL \
    --start-date 2020-01-01 \
    --end-date 2024-12-31 \
    --config-dir scenarios/discount_experiments

  # Grid search (all combinations)
  python -m valuation.analysis.backtest_from_configs \
    --ticker AAPL \
    --start-date 2020-01-01 \
    --end-date 2024-12-31 \
    --config-dir scenarios/grid_search \
    --output results/grid_search_aapl.csv
"""

import argparse
import json
import logging
from pathlib import Path

from valuation.analysis.backtest.runner import run_batch_backtest
from valuation.scenarios.config import ScenarioConfig

logger = logging.getLogger(__name__)

def load_configs_from_files(config_paths: list[Path]) -> list[ScenarioConfig]:
  """
  Load scenario configs from JSON files.

  Args:
    config_paths: List of paths to JSON config files

  Returns:
    List of ScenarioConfig objects
  """
  configs = []
  for path in config_paths:
    try:
      with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)
      config = ScenarioConfig.from_dict(data)
      configs.append(config)
      logger.info('Loaded config: %s from %s', config.name, path.name)
    except Exception as e:  # pylint: disable=broad-except
      logger.error('Failed to load %s: %s', path, e)

  return configs

def load_configs_from_dir(config_dir: Path) -> list[ScenarioConfig]:
  """
  Load all JSON configs from a directory.

  Args:
    config_dir: Directory containing JSON config files

  Returns:
    List of ScenarioConfig objects
  """
  json_files = sorted(config_dir.glob('*.json'))
  return load_configs_from_files(json_files)

def main():
  """CLI entrypoint for backtest from configs."""
  parser = argparse.ArgumentParser(
      description='Run backtest using JSON config files')
  parser.add_argument('--ticker',
                      type=str,
                      required=True,
                      help='Company ticker')
  parser.add_argument('--start-date',
                      type=str,
                      required=True,
                      help='Start date (YYYY-MM-DD)')
  parser.add_argument('--end-date',
                      type=str,
                      required=True,
                      help='End date (YYYY-MM-DD)')

  config_group = parser.add_mutually_exclusive_group(required=True)
  config_group.add_argument('--configs',
                            nargs='+',
                            type=Path,
                            help='Config file paths')
  config_group.add_argument('--config-dir',
                            type=Path,
                            help='Directory with config files')

  parser.add_argument('--output',
                      type=str,
                      default='output/analysis/backtest_results.csv',
                      help='Output CSV path')
  parser.add_argument('--gold-path',
                      type=Path,
                      default=Path('data/gold/out/valuation_panel.parquet'))
  parser.add_argument('--silver-dir',
                      type=Path,
                      default=Path('data/silver/out'))
  parser.add_argument('-v',
                      '--verbose',
                      action='store_true',
                      help='Verbose output')

  args = parser.parse_args()

  if args.configs:
    configs = load_configs_from_files(args.configs)
  else:
    configs = load_configs_from_dir(args.config_dir)

  if not configs:
    logger.error('No valid configs loaded')
    return

  logger.info('')
  logger.info('=' * 70)
  logger.info('Backtest Configuration')
  logger.info('=' * 70)
  logger.info('Ticker: %s', args.ticker)
  logger.info('Period: %s to %s', args.start_date, args.end_date)
  logger.info('Scenarios: %s', [c.name for c in configs])
  logger.info('Output: %s', args.output)
  logger.info('=' * 70)
  logger.info('')

  results = run_batch_backtest(
      tickers=[args.ticker],
      start_date=args.start_date,
      end_date=args.end_date,
      scenarios=configs,
      gold_path=args.gold_path,
      silver_dir=args.silver_dir,
      verbose=args.verbose,
  )

  Path(args.output).parent.mkdir(parents=True, exist_ok=True)
  results.to_csv(args.output, index=False)

  logger.info('')
  logger.info('=' * 70)
  logger.info('Results saved to: %s', args.output)
  logger.info('Total rows: %d', len(results))
  logger.info('=' * 70)

if __name__ == '__main__':
  logging.basicConfig(
      level=logging.INFO,
      format='%(message)s',
  )
  main()
