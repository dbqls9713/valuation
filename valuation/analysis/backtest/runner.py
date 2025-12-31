'''
Backtest runner for comparing valuation scenarios over time.

This module provides the BacktestRunner class which:
1. Iterates through quarter-end dates
2. Runs valuations for each date using PIT data
3. Compares multiple scenarios side-by-side
4. Outputs long-form results for analysis

Usage:
  from valuation.backtest.runner import BacktestRunner
  from valuation.scenarios.config import ScenarioConfig

  runner = BacktestRunner(
    ticker='GOOGL',
    start_date='2020-01-01',
    end_date='2024-12-31',
    scenarios=[ScenarioConfig.default(), ScenarioConfig.raw_capex()],
  )
  results = runner.run()
  results.to_csv('backtest_results.csv', index=False)
'''

import argparse
import logging
from pathlib import Path
from typing import List, Optional

import pandas as pd

from valuation.analysis.backtest.metrics import compute_summary_stats
from valuation.run import run_valuation
from valuation.scenarios.config import ScenarioConfig

logger = logging.getLogger(__name__)


class BacktestRunner:
  '''
  Run valuations across multiple dates and scenarios.

  Supports:
  - Multiple scenarios for comparison
  - Multiple tickers (batch mode)
  - Verbose progress output
  - Long-form output for easy analysis
  '''

  def __init__(
      self,
      ticker: str,
      start_date: str,
      end_date: str,
      scenarios: Optional[List[ScenarioConfig]] = None,
      gold_path: Path = Path('data/gold/out/valuation_panel.parquet'),
      silver_dir: Path = Path('data/silver/out'),
  ):
    '''
    Initialize backtest runner.

    Args:
      ticker: Company ticker symbol
      start_date: Backtest start date (YYYY-MM-DD)
      end_date: Backtest end date (YYYY-MM-DD)
      scenarios: List of ScenarioConfig (default: just default)
      gold_path: Path to Gold panel parquet
      silver_dir: Path to Silver layer output directory
    '''
    self.ticker = ticker
    self.start_date = pd.Timestamp(start_date)
    self.end_date = pd.Timestamp(end_date)
    self.scenarios = scenarios or [ScenarioConfig.default()]
    self.gold_path = gold_path
    self.silver_dir = silver_dir

  def _generate_quarter_ends(self) -> List[pd.Timestamp]:
    '''Generate list of quarter-end dates.'''
    quarters = []
    current = self.start_date

    while current <= self.end_date:
      month = ((current.month - 1) // 3 + 1) * 3
      quarter_end = (pd.Timestamp(current.year, month, 1) +
                     pd.offsets.MonthEnd(0))

      if quarter_end >= self.start_date and quarter_end not in quarters:
        quarters.append(quarter_end)

      current = quarter_end + pd.DateOffset(months=3)

    return sorted(quarters)

  def run(self, verbose: bool = True) -> pd.DataFrame:
    '''
    Run backtest across all quarters and scenarios.

    Args:
      verbose: Print progress to stdout

    Returns:
      Long-form DataFrame with columns:
      - as_of_date: Quarter end date
      - scenario: Scenario name
      - ticker: Company ticker
      - iv_per_share: Intrinsic value
      - market_price: Market price
      - price_to_iv: Market / IV ratio
      - ... (all diagnostics)
    '''
    quarters = self._generate_quarter_ends()
    results = []

    if verbose:
      logger.info('Running backtest for %s', self.ticker)
      logger.info('Period: %s to %s', self.start_date.date(),
                  self.end_date.date())
      logger.info('Quarters: %d', len(quarters))
      logger.info('Scenarios: %s', [s.name for s in self.scenarios])
      logger.info('')

    for as_of in quarters:
      for config in self.scenarios:
        try:
          result = run_valuation(
              ticker=self.ticker,
              as_of_date=str(as_of.date()),
              config=config,
              gold_path=self.gold_path,
              silver_dir=self.silver_dir,
          )

          row = result.to_dict()
          row['as_of_date'] = as_of

          if verbose:
            excluded = row.get('excluded', False)
            if excluded:
              reason = row.get('exclusion_reason', 'unknown')
              logger.info('✗ %s [%s]: %s', as_of.date(), config.name, reason)
            else:
              iv = row.get('iv_per_share', float('nan'))
              price = row.get('market_price')
              ratio = row.get('price_to_iv')
              if price and ratio:
                logger.info('✓ %s [%s]: IV=$%.2f, P=$%.2f, P/IV=%.2f%%',
                            as_of.date(), config.name, iv, price, ratio * 100)
              else:
                logger.info('✓ %s [%s]: IV=$%.2f', as_of.date(), config.name,
                            iv)

          results.append(row)

        except Exception as e:  # pylint: disable=broad-except
          if verbose:
            logger.warning('✗ %s [%s]: %s', as_of.date(), config.name, e)
          results.append({
              'as_of_date': as_of,
              'scenario': config.name,
              'ticker': self.ticker,
              'error': str(e),
          })

    if not results:
      raise ValueError('No successful backtest results')

    return pd.DataFrame(results)


def run_batch_backtest(
    tickers: List[str],
    start_date: str,
    end_date: str,
    scenarios: List[ScenarioConfig],
    gold_path: Path = Path('data/gold/out/valuation_panel.parquet'),
    silver_dir: Path = Path('data/silver/out'),
    verbose: bool = True,
) -> pd.DataFrame:
  '''
  Run backtest for multiple tickers.

  Args:
    tickers: List of ticker symbols
    start_date: Start date (YYYY-MM-DD)
    end_date: End date (YYYY-MM-DD)
    scenarios: List of ScenarioConfig
    gold_path: Path to Gold panel
    silver_dir: Path to Silver directory
    verbose: Print progress

  Returns:
    Combined long-form DataFrame for all tickers
  '''
  all_results = []

  for ticker in tickers:
    if verbose:
      separator = '=' * 70
      logger.info('\n%s', separator)
      logger.info('Processing %s', ticker)
      logger.info(separator)

    runner = BacktestRunner(
        ticker=ticker,
        start_date=start_date,
        end_date=end_date,
        scenarios=scenarios,
        gold_path=gold_path,
        silver_dir=silver_dir,
    )

    try:
      results = runner.run(verbose=verbose)
      all_results.append(results)
    except Exception as e:  # pylint: disable=broad-except
      if verbose:
        logger.error('Error processing %s: %s', ticker, e)

  if not all_results:
    raise ValueError('No successful results for any ticker')

  return pd.concat(all_results, ignore_index=True)


def main():
  '''CLI entrypoint for backtest runner.'''
  parser = argparse.ArgumentParser(description='Run valuation backtest')
  parser.add_argument('--ticker',
                      type=str,
                      required=True,
                      help='Company ticker')
  parser.add_argument('--start-date',
                      type=str,
                      required=True,
                      help='Start date')
  parser.add_argument('--end-date', type=str, required=True, help='End date')
  parser.add_argument(
      '--scenarios',
      nargs='+',
      default=['default'],
      choices=['default', 'raw_capex', 'clipped_capex', 'discount_6pct'],
      help='Scenarios to run',
  )
  parser.add_argument('--output',
                      type=str,
                      default='output/analysis/backtest_results.csv',
                      help='Output')
  parser.add_argument(
      '--gold-path',
      type=Path,
      default=Path('data/gold/out/valuation_panel.parquet'),
  )
  parser.add_argument('--silver-dir',
                      type=Path,
                      default=Path('data/silver/out'))
  args = parser.parse_args()

  scenario_map = {
      'default': ScenarioConfig.default,
      'raw_capex': ScenarioConfig.raw_capex,
      'clipped_capex': ScenarioConfig.clipped_capex,
      'discount_6pct': ScenarioConfig.discount_6pct,
  }

  scenarios = [scenario_map[s]() for s in args.scenarios]

  runner = BacktestRunner(
      ticker=args.ticker,
      start_date=args.start_date,
      end_date=args.end_date,
      scenarios=scenarios,
      gold_path=args.gold_path,
      silver_dir=args.silver_dir,
  )

  results = runner.run()
  results.to_csv(args.output, index=False)

  summary = compute_summary_stats(results)

  separator = '=' * 70
  logger.info('\n%s', separator)
  logger.info('Backtest Summary')
  logger.info(separator)
  logger.info('\n%s', summary.to_string())
  logger.info('\nResults saved to: %s', args.output)


if __name__ == '__main__':
  logging.basicConfig(
      level=logging.INFO,
      format='%(message)s',
  )
  main()
