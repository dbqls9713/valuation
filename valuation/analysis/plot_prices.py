'''
Compare intrinsic values calculated with different CAPEX methods.

Creates charts showing IV from different CAPEX approaches vs market price.
Uses the valuation policy system for consistent calculations.

Usage:
  python -m valuation.analysis.compare_capex \\
      --tickers AAPL GOOGL META MSFT \\
      --start-date 2020-01-01 \\
      --end-date 2025-12-31

  python -m valuation.analysis.compare_capex \\
      --tickers-file data/bronze/tickers_dow30.txt \\
      --output-dir charts/capex_comparison
'''

import argparse
import logging
from pathlib import Path
from typing import Dict, Optional

import matplotlib.pyplot as plt
import pandas as pd

from valuation.domain.types import FundamentalsSlice
from valuation.engine.dcf import compute_intrinsic_value
from valuation.run import load_gold_panel, adjust_for_splits, get_price_after_filing
from valuation.scenarios.config import ScenarioConfig
from valuation.scenarios.registry import create_policies

logger = logging.getLogger(__name__)


def calculate_iv_for_date(
    panel: pd.DataFrame,
    ticker: str,
    as_of_date: pd.Timestamp,
    scenario: ScenarioConfig,
    silver_dir: Path = Path('data/silver/out'),
) -> Optional[Dict]:
  '''
  Calculate IV for a single date using a specific scenario.

  Args:
      panel: Full Gold panel (split-adjusted)
      ticker: Company ticker
      as_of_date: As-of date for PIT filtering
      scenario: ScenarioConfig with policy settings
      silver_dir: Path to Silver layer

  Returns:
      Dict with iv, growth, diagnostics or None if calculation fails
  '''
  try:
    fundamentals = FundamentalsSlice.from_panel(
        panel=panel,
        ticker=ticker,
        as_of_date=as_of_date,
    )
  except (ValueError, KeyError):
    return None

  policies = create_policies(scenario)

  capex_result = policies['capex'].compute(fundamentals)
  if pd.isna(capex_result.value):
    return None

  oe0 = fundamentals.latest_cfo_ttm - capex_result.value

  growth_result = policies['growth'].compute(fundamentals, policies['capex'])
  if pd.isna(growth_result.value):
    return None

  if growth_result.diag.get('below_threshold', False):
    return None

  terminal_result = policies['terminal'].compute()
  g_terminal = terminal_result.value

  fade_result = policies['fade'].compute(
      g0=growth_result.value,
      g_terminal=g_terminal,
      n_years=scenario.n_years,
  )

  shares_result = policies['shares'].compute(fundamentals)
  sh0 = fundamentals.latest_shares
  buyback_rate = shares_result.value

  discount_result = policies['discount'].compute()
  discount_rate = discount_result.value

  iv, pv_explicit, tv = compute_intrinsic_value(
      oe0=oe0,
      sh0=sh0,
      buyback_rate=buyback_rate,
      growth_path=fade_result.value,
      g_terminal=g_terminal,
      discount_rate=discount_rate,
  )

  if not pd.isna(iv) and iv > 0:
    try:
      market_slice = get_price_after_filing(ticker, fundamentals.latest_filed,
                                            silver_dir)
      market_price = market_slice.price
    except (FileNotFoundError, ValueError):
      market_price = None

    return {
        'iv': iv,
        'growth': growth_result.value,
        'oe0': oe0,
        'shares': sh0,
        'buyback': buyback_rate,
        'market_price': market_price,
        'pv_explicit': pv_explicit,
        'terminal_value': tv,
    }

  return None


def plot_capex_comparison(
    ticker: str,
    panel: pd.DataFrame,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
    output_dir: Path,
    silver_dir: Path = Path('data/silver/out'),
) -> None:
  '''Plot IV comparison for different CAPEX methods vs market price.'''
  ticker_panel = panel[panel['ticker'] == ticker].copy()

  if ticker_panel.empty:
    logger.warning('No data for %s', ticker)
    return

  ticker_panel = ticker_panel.sort_values('end')
  ticker_panel = ticker_panel[(ticker_panel['end'] >= start_date) &
                              (ticker_panel['end'] <= end_date)]

  if len(ticker_panel) < 4:
    logger.warning('Insufficient data for %s', ticker)
    return

  scenarios = {
      'raw': ScenarioConfig.raw_capex(),
      'weighted': ScenarioConfig.default(),
      'clipped': ScenarioConfig.clipped_capex(),
  }

  quarter_ends = sorted(ticker_panel['end'].unique())
  results: Dict[str, list] = {
      'dates': [],
      'iv_raw': [],
      'iv_weighted': [],
      'iv_clipped': [],
      'market_price': [],
  }

  logger.info('Calculating IVs for %d quarter-ends...', len(quarter_ends))

  for as_of_date in quarter_ends:
    result_raw = calculate_iv_for_date(panel, ticker, as_of_date,
                                       scenarios['raw'], silver_dir)
    result_weighted = calculate_iv_for_date(panel, ticker, as_of_date,
                                            scenarios['weighted'], silver_dir)
    result_clipped = calculate_iv_for_date(panel, ticker, as_of_date,
                                           scenarios['clipped'], silver_dir)

    if result_raw and result_weighted and result_clipped:
      market_price = (result_weighted['market_price'] or
                      result_raw['market_price'] or
                      result_clipped['market_price'])

      if market_price:
        results['dates'].append(as_of_date)
        results['iv_raw'].append(result_raw['iv'])
        results['iv_weighted'].append(result_weighted['iv'])
        results['iv_clipped'].append(result_clipped['iv'])
        results['market_price'].append(market_price)

  if len(results['dates']) == 0:
    logger.warning('No valid results for %s', ticker)
    return

  logger.info('Generated %d data points', len(results['dates']))

  _, ax = plt.subplots(figsize=(14, 8))

  ax.plot(results['dates'],
          results['iv_raw'],
          'o-',
          label='(a) IV - Raw TTM CAPEX',
          linewidth=2,
          markersize=6,
          alpha=0.8)
  ax.plot(results['dates'],
          results['iv_weighted'],
          's-',
          label='(b) IV - 3Y Weighted (1:2:3)',
          linewidth=2,
          markersize=6,
          alpha=0.8)
  ax.plot(results['dates'],
          results['iv_clipped'],
          '^-',
          label='(c) IV - Intensity Clipping',
          linewidth=2,
          markersize=6,
          alpha=0.8)

  ax.plot(results['dates'],
          results['market_price'],
          'D-',
          label='Market Price (filed+1d)',
          linewidth=2.5,
          markersize=7,
          color='red',
          alpha=0.9)

  ax.set_xlabel('Quarter End Date', fontsize=12, fontweight='bold')
  ax.set_ylabel('Price per Share ($)', fontsize=12, fontweight='bold')
  ax.set_title(f'{ticker} - Intrinsic Value by CAPEX Method vs Market Price',
               fontsize=14,
               fontweight='bold',
               pad=20)
  ax.legend(loc='best', fontsize=11, framealpha=0.9)
  ax.grid(True, alpha=0.3, linestyle='--')

  avg_iv_raw = sum(results['iv_raw']) / len(results['iv_raw'])
  avg_iv_weighted = sum(results['iv_weighted']) / len(results['iv_weighted'])
  avg_iv_clipped = sum(results['iv_clipped']) / len(results['iv_clipped'])
  avg_market = sum(results['market_price']) / len(results['market_price'])

  first_year = results['dates'][0].year
  last_year = results['dates'][-1].year

  stats_text = (f'Average ({first_year}-{last_year}):\n'
                f'  Raw IV:      ${avg_iv_raw:.2f}\n'
                f'  Weighted IV: ${avg_iv_weighted:.2f}\n'
                f'  Clipped IV:  ${avg_iv_clipped:.2f}\n'
                f'  Market:      ${avg_market:.2f}\n'
                f'  Price/IV (Weighted): {avg_market/avg_iv_weighted:.1%}')

  ax.text(0.02,
          0.98,
          stats_text,
          transform=ax.transAxes,
          verticalalignment='top',
          fontsize=10,
          bbox=dict(boxstyle='round', facecolor='lightblue', alpha=0.8))

  plt.tight_layout()

  output_path = output_dir / f'{ticker}_capex_comparison.png'
  plt.savefig(output_path, dpi=150, bbox_inches='tight')
  logger.info('Saved: %s', output_path)

  plt.close()


def main() -> None:
  '''CLI entrypoint for CAPEX comparison analysis.'''
  parser = argparse.ArgumentParser(
      description='Compare IVs from different CAPEX methods',
      formatter_class=argparse.RawDescriptionHelpFormatter,
      epilog='''
Examples:
  # Basic usage
  python -m valuation.analysis.compare_capex \\
      --tickers AAPL GOOGL META MSFT

  # From ticker file
  python -m valuation.analysis.compare_capex \\
      --tickers-file data/bronze/tickers_dow30.txt \\
      --start-date 2020-01-01

  # Custom output directory
  python -m valuation.analysis.compare_capex \\
      --tickers AAPL GOOGL \\
      --output-dir charts/capex_analysis
      ''')

  parser.add_argument('--tickers',
                      nargs='+',
                      help='Tickers to analyze (e.g., AAPL GOOGL META)')
  parser.add_argument('--tickers-file',
                      type=Path,
                      help='Path to file with ticker list (one per line)')
  parser.add_argument('--start-date',
                      default='2020-01-01',
                      help='Start date (YYYY-MM-DD)')
  parser.add_argument('--end-date',
                      default='2025-12-31',
                      help='End date (YYYY-MM-DD)')
  parser.add_argument('--output-dir',
                      type=Path,
                      default=Path('output/analysis/price_charts'),
                      help='Output directory for charts')
  parser.add_argument('--gold-path',
                      type=Path,
                      default=Path('data/gold/out/valuation_panel.parquet'),
                      help='Path to Gold panel')
  parser.add_argument('--silver-dir',
                      type=Path,
                      default=Path('data/silver/out'),
                      help='Path to Silver directory')
  parser.add_argument('--verbose',
                      '-v',
                      action='store_true',
                      help='Verbose output')

  args = parser.parse_args()

  logging.basicConfig(
      level=logging.DEBUG if args.verbose else logging.INFO,
      format='%(asctime)s [%(levelname)s] %(message)s',
      datefmt='%Y-%m-%d %H:%M:%S',
  )

  if args.tickers_file:
    if not args.tickers_file.exists():
      raise FileNotFoundError(f'File not found: {args.tickers_file}')

    with open(args.tickers_file, 'r', encoding='utf-8') as f:
      tickers = [
          line.strip()
          for line in f
          if line.strip() and not line.strip().startswith('#')
      ]

    if not tickers:
      raise ValueError('No tickers found in file')

    logger.info('Loaded %d tickers from %s', len(tickers), args.tickers_file)
  elif args.tickers:
    tickers = args.tickers
  else:
    tickers = ['AAPL', 'GOOGL', 'META', 'MSFT']
    logger.warning('No tickers specified, using defaults: %s', tickers)

  args.output_dir.mkdir(parents=True, exist_ok=True)

  logger.info('Loading Gold panel: %s', args.gold_path)
  panel = load_gold_panel(args.gold_path)
  panel = adjust_for_splits(panel)

  start = pd.Timestamp(args.start_date)
  end = pd.Timestamp(args.end_date)

  logger.info('Analyzing %d companies: %s', len(tickers), ', '.join(tickers))
  logger.info('Period: %s to %s', args.start_date, args.end_date)

  for ticker in tickers:
    logger.info('Processing %s...', ticker)
    plot_capex_comparison(
        ticker=ticker,
        panel=panel,
        start_date=start,
        end_date=end,
        output_dir=args.output_dir,
        silver_dir=args.silver_dir,
    )

  logger.info('All charts saved to: %s', args.output_dir)


if __name__ == '__main__':
  main()
