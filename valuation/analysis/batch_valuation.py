'''
Batch valuation for multiple tickers at a single point in time.

This module provides tools to:
1. Run valuations for multiple tickers at once
2. Compare valuations across companies
3. Export results to CSV for further analysis

Usage (CLI):
  # From ticker file
  python -m valuation.analysis.batch_valuation \
    --tickers-file data/bronze/tickers_dow30.txt \
    --as-of-date 2024-12-31 \
    --output results/dow30_valuation.csv

  # Specific tickers
  python -m valuation.analysis.batch_valuation \
    --tickers AAPL GOOGL MSFT \
    --as-of-date 2024-12-31 \
    --scenario conservative \
    --output results/bigtech.csv \
    -v

Usage (Python API):
  from valuation.analysis.batch_valuation import batch_valuation
  from valuation.scenarios.config import ScenarioConfig

  df = batch_valuation(
    tickers=['AAPL', 'GOOGL', 'MSFT'],
    as_of_date='2024-12-31',
    config=ScenarioConfig.default(),
  )
  df.to_csv('results.csv', index=False)
'''

import argparse
import logging
import traceback
from pathlib import Path
from typing import List

import pandas as pd

from valuation.domain.types import ValuationResult
from valuation.run import run_valuation
from valuation.scenarios.config import ScenarioConfig

logger = logging.getLogger(__name__)


def _result_to_dict(
    ticker: str,
    as_of_date: str,
    scenario_name: str,
    result: ValuationResult,
) -> dict:
  '''Convert ValuationResult to flat dictionary for DataFrame row.'''
  row = {
      'ticker': ticker,
      'as_of_date': as_of_date,
      'scenario': scenario_name,
      'iv_per_share': result.iv_per_share,
      'market_price': result.market_price,
      'price_to_iv': result.price_to_iv,
      'margin_of_safety': result.margin_of_safety,
  }

  if result.diag:
    row.update(result.diag)

  return row


def batch_valuation(
    tickers: List[str],
    as_of_date: str,
    config: ScenarioConfig,
    gold_path: Path = Path('data/gold/out/valuation_panel.parquet'),
    silver_dir: Path = Path('data/silver/out'),
    verbose: bool = False,
) -> pd.DataFrame:
  '''
  Run valuation for multiple tickers at a single date.

  Args:
    tickers: List of ticker symbols
    as_of_date: Valuation date (YYYY-MM-DD)
    config: ScenarioConfig with policy settings
    gold_path: Path to Gold panel parquet
    silver_dir: Path to Silver layer output directory
    verbose: Enable verbose logging

  Returns:
    DataFrame with columns:
    - ticker: Ticker symbol
    - as_of_date: Valuation date
    - scenario: Scenario name
    - iv_per_share: Intrinsic value per share
    - market_price: Market price (if available)
    - price_to_iv: Market price / IV ratio
    - margin_of_safety: (IV - Price) / IV
    - ... all policy diagnostics ...

  Raises:
    ValueError: If no successful results
  '''
  results = []

  for i, ticker in enumerate(tickers, 1):
    if verbose:
      logger.info('[%d/%d] Processing %s...', i, len(tickers), ticker)

    try:
      result = run_valuation(
          ticker=ticker,
          as_of_date=as_of_date,
          config=config,
          gold_path=gold_path,
          silver_dir=silver_dir,
          include_market_price=True,
      )

      row = _result_to_dict(ticker, as_of_date, config.name, result)
      results.append(row)

      if verbose:
        iv = result.iv_per_share
        price = result.market_price or 0.0
        mos = (result.margin_of_safety or 0.0) * 100

        logger.info('  IV: $%.2f, Price: $%.2f, MoS: %.1f%%', iv, price, mos)

    except Exception as e:  # pylint: disable=broad-except
      logger.warning('Failed to process %s: %s', ticker, str(e))
      if verbose:
        logger.debug('%s', traceback.format_exc())

  if not results:
    raise ValueError(f'No successful results for any ticker in {tickers}')

  return pd.DataFrame(results)


def _load_tickers_from_file(file_path: Path) -> List[str]:
  '''Load ticker symbols from text file (one per line, # for comments).'''
  with open(file_path, 'r', encoding='utf-8') as f:
    tickers = [
        line.strip() for line in f
        if line.strip() and not line.strip().startswith('#')
    ]
  return tickers


def _print_summary(df: pd.DataFrame) -> None:
  '''Print summary statistics for batch valuation results.'''
  total = len(df)
  has_price = df['market_price'].notna().sum()

  logger.info('')
  logger.info('=' * 70)
  logger.info('Summary Statistics')
  logger.info('=' * 70)
  logger.info('Total companies: %d', total)
  logger.info('With market price: %d', has_price)
  logger.info('')

  if has_price > 0:
    priced_df = df[df['market_price'].notna()]

    logger.info('Intrinsic Value:')
    logger.info('  Mean:   $%.2f', df['iv_per_share'].mean())
    logger.info('  Median: $%.2f', df['iv_per_share'].median())
    logger.info('  Min:    $%.2f (%s)',
                df['iv_per_share'].min(),
                df.loc[df['iv_per_share'].idxmin(), 'ticker'])
    logger.info('  Max:    $%.2f (%s)',
                df['iv_per_share'].max(),
                df.loc[df['iv_per_share'].idxmax(), 'ticker'])
    logger.info('')

    logger.info('Price/IV Ratio:')
    logger.info('  Mean:   %.2f%%', priced_df['price_to_iv'].mean() * 100)
    logger.info('  Median: %.2f%%', priced_df['price_to_iv'].median() * 100)
    logger.info('')

    logger.info('Margin of Safety:')
    logger.info('  Mean:   %.1f%%', priced_df['margin_of_safety'].mean() * 100)
    logger.info('  Median: %.1f%%',
                priced_df['margin_of_safety'].median() * 100)
    logger.info('')

    undervalued = priced_df[priced_df['margin_of_safety'] > 0]
    logger.info('Undervalued (MoS > 0): %d / %d (%.1f%%)',
                len(undervalued),
                has_price,
                len(undervalued) / has_price * 100)

    if len(undervalued) > 0:
      logger.info('Top 5 undervalued:')
      top5 = undervalued.nlargest(5, 'margin_of_safety')
      for _, row in top5.iterrows():
        logger.info('  %s: IV=$%.2f, Price=$%.2f, MoS=%.1f%%',
                    row['ticker'],
                    row['iv_per_share'],
                    row['market_price'],
                    row['margin_of_safety'] * 100)

  logger.info('=' * 70)


def main() -> None:
  '''CLI entrypoint for batch valuation.'''
  parser = argparse.ArgumentParser(
      description='Batch valuation for multiple tickers',
      formatter_class=argparse.RawDescriptionHelpFormatter,
      epilog=__doc__,
  )

  ticker_group = parser.add_mutually_exclusive_group(required=True)
  ticker_group.add_argument('--tickers',
                            nargs='+',
                            help='Space-separated ticker symbols')
  ticker_group.add_argument('--tickers-file',
                            type=Path,
                            help='File with ticker symbols (one per line)')

  parser.add_argument('--as-of-date',
                      type=str,
                      required=True,
                      help='Valuation date (YYYY-MM-DD)')

  parser.add_argument('--scenario',
                      type=str,
                      default='default',
                      help='Scenario name (default: default)')

  parser.add_argument('--output',
                      type=Path,
                      required=True,
                      help='Output CSV file path')

  parser.add_argument('--gold-path',
                      type=Path,
                      default=Path('data/gold/out/valuation_panel.parquet'),
                      help='Path to Gold panel parquet')

  parser.add_argument('--silver-dir',
                      type=Path,
                      default=Path('data/silver/out'),
                      help='Path to Silver layer output directory')

  parser.add_argument('-v',
                      '--verbose',
                      action='store_true',
                      help='Verbose output')

  args = parser.parse_args()

  logging.basicConfig(
      level=logging.DEBUG if args.verbose else logging.INFO,
      format='%(asctime)s [%(levelname)s] %(message)s',
      datefmt='%Y-%m-%d %H:%M:%S',
  )

  if args.tickers:
    tickers = args.tickers
    logger.info('Processing %d tickers as of %s',
                len(tickers),
                args.as_of_date)
  else:
    tickers = _load_tickers_from_file(args.tickers_file)
    logger.info('Loaded %d tickers from %s',
                len(tickers),
                args.tickers_file)
    logger.info('Processing as of %s', args.as_of_date)

  scenario_map = {
      'default': ScenarioConfig.default,
      'raw_capex': ScenarioConfig.raw_capex,
      'clipped_capex': ScenarioConfig.clipped_capex,
      'discount_6pct': ScenarioConfig.discount_6pct,
  }

  if args.scenario not in scenario_map:
    available = ', '.join(scenario_map.keys())
    raise ValueError(
        f'Unknown scenario: {args.scenario}. Available: {available}')

  config = scenario_map[args.scenario]()
  logger.info('Using scenario: %s', config.name)
  logger.info('')

  results = batch_valuation(
      tickers=tickers,
      as_of_date=args.as_of_date,
      config=config,
      gold_path=args.gold_path,
      silver_dir=args.silver_dir,
      verbose=args.verbose,
  )

  args.output.parent.mkdir(parents=True, exist_ok=True)
  results.to_csv(args.output, index=False)

  logger.info('')
  logger.info('Saved %d results to %s', len(results), args.output)

  _print_summary(results)


if __name__ == '__main__':
  main()
