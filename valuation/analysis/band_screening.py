"""
Screen stocks based on valuation band criteria.

This script filters tickers that satisfy:
1. High hit rate: sufficient quarters with valid IV data
2. High in-band ratio: market price stayed within lower/upper IV bounds
3. Current undervaluation: market price below lower bound by threshold
4. Data freshness: latest data within tolerance of end date

Usage:
  python -m valuation.analysis.band_screening \
    --tickers-file data/snp500.txt \
    --lower-config scenarios/base/conservative.json \
    --upper-config scenarios/base/aggressive.json \
    --start-date 2020-01-01 \
    --end-date 2025-12-31 \
    --tolerance-day 90 \
    --min-hit-rate 0.5 \
    --min-inband-ratio 0.6 \
    --dev-threshold 0.1 \
    --concurrency 4 \
    --output output/screened_tickers.txt
"""

import argparse
from concurrent.futures import as_completed
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
import json
import logging
from pathlib import Path
from typing import Optional

import pandas as pd

from valuation.data_loader import ValuationDataLoader
from valuation.domain.types import FundamentalsSlice
from valuation.engine.dcf import compute_intrinsic_value
from valuation.scenarios.config import ScenarioConfig
from valuation.scenarios.registry import create_policies
from valuation.scenarios.registry import PolicyBundle

logger = logging.getLogger(__name__)


@dataclass
class ScreeningResult:
  """Result of screening a single ticker."""
  ticker: str
  passed: bool
  hit_rate: Optional[float] = None
  in_band_ratio: Optional[float] = None
  deviation: Optional[float] = None
  latest_date: Optional[pd.Timestamp] = None
  error: Optional[str] = None


def load_config_from_file(config_path: Path) -> ScenarioConfig:
  """Load ScenarioConfig from JSON file."""
  with open(config_path, 'r', encoding='utf-8') as f:
    data = json.load(f)
  return ScenarioConfig.from_dict(data)


def load_tickers_from_file(file_path: Path) -> list[str]:
  """Load ticker symbols from text file (one per line, # for comments)."""
  with open(file_path, 'r', encoding='utf-8') as f:
    tickers = [
        line.strip()
        for line in f
        if line.strip() and not line.strip().startswith('#')
    ]
  return tickers


def generate_quarter_ends(
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
) -> list[pd.Timestamp]:
  """Generate list of quarter-end dates between start and end."""
  quarters = []
  current = start_date

  while current <= end_date:
    month = ((current.month - 1) // 3 + 1) * 3
    quarter_end = pd.Timestamp(current.year, month, 1) + pd.offsets.MonthEnd(0)

    if quarter_end >= start_date and quarter_end <= end_date:
      if quarter_end not in quarters:
        quarters.append(quarter_end)

    current = quarter_end + pd.DateOffset(months=3)

  return sorted(quarters)


def get_price_at_date(
    ticker_prices: pd.DataFrame,
    target_date: pd.Timestamp,
) -> Optional[float]:
  """Get closing price at or before target_date."""
  valid = ticker_prices[ticker_prices['date'] <= target_date]
  if valid.empty:
    return None
  return float(valid.iloc[-1]['close'])


def calculate_iv_fast(
    ticker_panel: pd.DataFrame,
    as_of_date: pd.Timestamp,
    policies: PolicyBundle,
    n_years: int,
) -> Optional[float]:
  """Calculate IV using pre-filtered panel and pre-created policies."""
  try:
    fundamentals = FundamentalsSlice.from_ticker_panel(
        ticker_panel=ticker_panel,
        as_of_date=as_of_date,
    )
  except (ValueError, KeyError):
    return None

  pre_maint_oe_result = policies['pre_maint_oe'].compute(fundamentals)
  if pd.isna(pre_maint_oe_result.value):
    return None

  maint_capex_result = policies['maint_capex'].compute(fundamentals)
  if pd.isna(maint_capex_result.value):
    return None

  oe0 = pre_maint_oe_result.value - maint_capex_result.value

  growth_result = policies['growth'].compute(fundamentals)
  if pd.isna(growth_result.value) or growth_result.diag.get(
      'below_threshold', False):
    return 0.0

  terminal_result = policies['terminal'].compute()
  g_terminal = terminal_result.value

  fade_result = policies['fade'].compute(
      g0=growth_result.value,
      g_terminal=g_terminal,
      n_years=n_years,
  )

  shares_result = policies['shares'].compute(fundamentals)
  sh0 = fundamentals.latest_shares
  buyback_rate = shares_result.value

  discount_result = policies['discount'].compute()
  discount_rate = discount_result.value

  g0 = growth_result.value
  growth_path = [g0 * fade_result.value[i] for i in range(n_years)]

  iv, _, _ = compute_intrinsic_value(
      oe0=oe0,
      sh0=sh0,
      buyback_rate=buyback_rate,
      growth_path=growth_path,
      g_terminal=g_terminal,
      discount_rate=discount_rate,
  )

  return iv if iv > 0 else None


def screen_ticker(
    ticker: str,
    ticker_panel: pd.DataFrame,
    ticker_prices: pd.DataFrame,
    lower_policies: PolicyBundle,
    upper_policies: PolicyBundle,
    lower_n_years: int,
    upper_n_years: int,
    quarter_ends: list[pd.Timestamp],
    end_date: pd.Timestamp,
    tolerance_day: int,
    min_hit_rate: float,
    min_inband_ratio: float,
    dev_threshold: float,
) -> ScreeningResult:
  """
  Screen a single ticker against band criteria.

  Args:
    ticker: Ticker symbol
    ticker_panel: Panel pre-filtered for this ticker
    ticker_prices: Prices pre-filtered for this ticker
    lower_policies: Pre-created policies for lower bound
    upper_policies: Pre-created policies for upper bound
    lower_n_years: n_years for lower config
    upper_n_years: n_years for upper config

  Returns:
    ScreeningResult with pass/fail status and metrics
  """
  if ticker_prices.empty:
    return ScreeningResult(
        ticker=ticker,
        passed=False,
        error='No price data',
    )

  if ticker_panel.empty:
    return ScreeningResult(
        ticker=ticker,
        passed=False,
        error='No panel data',
    )

  lower_ivs: list[tuple[pd.Timestamp, float]] = []
  upper_ivs: list[tuple[pd.Timestamp, float]] = []
  market_prices: list[tuple[pd.Timestamp, float]] = []

  for as_of_date in quarter_ends:
    market_price = get_price_at_date(ticker_prices, as_of_date)
    if market_price is None:
      continue

    lower_iv = calculate_iv_fast(ticker_panel, as_of_date, lower_policies,
                                 lower_n_years)
    upper_iv = calculate_iv_fast(ticker_panel, as_of_date, upper_policies,
                                 upper_n_years)

    if lower_iv is None or upper_iv is None:
      continue

    if lower_iv <= 0 or upper_iv <= 0:
      continue

    lower_ivs.append((as_of_date, lower_iv))
    upper_ivs.append((as_of_date, upper_iv))
    market_prices.append((as_of_date, market_price))

  if len(lower_ivs) < 2:
    return ScreeningResult(
        ticker=ticker,
        passed=False,
        error='Insufficient IV data points',
    )

  hit_rate = len(lower_ivs) / len(quarter_ends)
  if hit_rate < min_hit_rate:
    return ScreeningResult(
        ticker=ticker,
        passed=False,
        hit_rate=hit_rate,
        error=f'Hit rate too low: {hit_rate:.1%} < {min_hit_rate:.1%}',
    )

  in_band_count = 0
  for i, (_, price) in enumerate(market_prices):
    lower_iv = lower_ivs[i][1]
    upper_iv = upper_ivs[i][1]
    if lower_iv <= price <= upper_iv:
      in_band_count += 1

  in_band_ratio = in_band_count / len(market_prices)

  latest_date, latest_price = market_prices[-1]
  latest_lower = lower_ivs[-1][1]
  latest_upper = upper_ivs[-1][1]

  band_width = latest_upper - latest_lower
  if band_width <= 0:
    return ScreeningResult(
        ticker=ticker,
        passed=False,
        error='Invalid band width (upper <= lower)',
    )

  deviation = (latest_lower - latest_price) / band_width

  days_diff = abs((end_date - latest_date).days)

  passed = (in_band_ratio >= min_inband_ratio and deviation >= dev_threshold and
            days_diff < tolerance_day)

  return ScreeningResult(
      ticker=ticker,
      passed=passed,
      hit_rate=hit_rate,
      in_band_ratio=in_band_ratio,
      deviation=deviation,
      latest_date=latest_date,
  )


def run_screening(
    tickers: list[str],
    lower_config: ScenarioConfig,
    upper_config: ScenarioConfig,
    start_date: str,
    end_date: str,
    tolerance_day: int,
    min_hit_rate: float,
    min_inband_ratio: float,
    dev_threshold: float,
    concurrency: int,
    gold_path: Path,
    silver_dir: Path,
) -> list[ScreeningResult]:
  """
  Run screening on all tickers with parallel processing.

  Returns:
    List of ScreeningResult for all tickers
  """
  loader = ValuationDataLoader(gold_path=gold_path, silver_dir=silver_dir)
  panel = loader.load_panel()
  prices = loader.load_prices()

  start_ts = pd.Timestamp(start_date)
  end_ts = pd.Timestamp(end_date)
  quarter_ends = generate_quarter_ends(start_ts, end_ts)

  logger.info('Screening %d tickers over %d quarters', len(tickers),
              len(quarter_ends))
  logger.info('Period: %s to %s', start_date, end_date)
  logger.info('Criteria: hit>=%.1f%%, band>=%.1f%%, dev>=%.2f, tol<%dd',
              min_hit_rate * 100, min_inband_ratio * 100, dev_threshold,
              tolerance_day)

  lower_policies = create_policies(lower_config)
  upper_policies = create_policies(upper_config)

  panel_by_ticker = {t: g for t, g in panel.groupby('ticker')}
  prices_by_symbol = {
      s: g.sort_values('date') for s, g in prices.groupby('symbol')
  }

  results: list[ScreeningResult] = []

  def process_ticker(ticker: str) -> ScreeningResult:
    symbol = f'{ticker}.US'
    ticker_panel = panel_by_ticker.get(ticker, pd.DataFrame())
    ticker_prices = prices_by_symbol.get(symbol, pd.DataFrame())

    return screen_ticker(
        ticker=ticker,
        ticker_panel=ticker_panel,
        ticker_prices=ticker_prices,
        lower_policies=lower_policies,
        upper_policies=upper_policies,
        lower_n_years=lower_config.n_years,
        upper_n_years=upper_config.n_years,
        quarter_ends=quarter_ends,
        end_date=end_ts,
        tolerance_day=tolerance_day,
        min_hit_rate=min_hit_rate,
        min_inband_ratio=min_inband_ratio,
        dev_threshold=dev_threshold,
    )

  with ThreadPoolExecutor(max_workers=concurrency) as executor:
    futures = {executor.submit(process_ticker, t): t for t in tickers}

    for future in as_completed(futures):
      ticker = futures[future]
      try:
        result = future.result()
        results.append(result)

        if result.passed:
          logger.info(
              '✓ %s: hit=%.1f%%, in_band=%.1f%%, dev=%.2f, date=%s',
              ticker,
              (result.hit_rate or 0) * 100,
              (result.in_band_ratio or 0) * 100,
              result.deviation or 0,
              result.latest_date.date() if result.latest_date else 'N/A',
          )
        elif result.error:
          logger.debug('✗ %s: %s', ticker, result.error)
        else:
          logger.debug(
              '✗ %s: hit=%.1f%%, in_band=%.1f%%, dev=%.2f',
              ticker,
              (result.hit_rate or 0) * 100,
              (result.in_band_ratio or 0) * 100,
              result.deviation or 0,
          )
      except (ValueError, KeyError, IndexError) as e:
        logger.warning('Error processing %s: %s', ticker, e)
        results.append(
            ScreeningResult(
                ticker=ticker,
                passed=False,
                error=str(e),
            ))

  return results


def main() -> None:
  """CLI entrypoint for band screening."""
  parser = argparse.ArgumentParser(
      description='Screen stocks based on valuation band criteria',
      formatter_class=argparse.RawDescriptionHelpFormatter,
      epilog=__doc__,
  )

  parser.add_argument(
      '--tickers-file',
      type=Path,
      required=True,
      help='File with ticker symbols (one per line)',
  )
  parser.add_argument(
      '--lower-config',
      type=Path,
      required=True,
      help='Lower bound scenario JSON config path',
  )
  parser.add_argument(
      '--upper-config',
      type=Path,
      required=True,
      help='Upper bound scenario JSON config path',
  )
  parser.add_argument(
      '--start-date',
      type=str,
      required=True,
      help='Start date (YYYY-MM-DD)',
  )
  parser.add_argument(
      '--end-date',
      type=str,
      required=True,
      help='End date (YYYY-MM-DD)',
  )
  parser.add_argument(
      '--tolerance-day',
      type=int,
      required=True,
      help='Maximum days between latest data and end date',
  )
  parser.add_argument(
      '--min-hit-rate',
      type=float,
      required=True,
      help='Minimum ratio of quarters with valid IV data (0-1)',
  )
  parser.add_argument(
      '--min-inband-ratio',
      type=float,
      required=True,
      help='Minimum ratio of dates where price is within band (0-1)',
  )
  parser.add_argument(
      '--dev-threshold',
      type=float,
      required=True,
      help='Minimum (L(T)-P(T))/W(T) threshold for undervaluation',
  )
  parser.add_argument(
      '--concurrency',
      type=int,
      required=True,
      help='Number of parallel workers',
  )
  parser.add_argument(
      '--output',
      type=Path,
      required=True,
      help='Output file path for screened tickers',
  )
  parser.add_argument(
      '--gold-path',
      type=Path,
      default=Path('data/gold/out/backtest_panel.parquet'),
      help='Path to Gold panel parquet',
  )
  parser.add_argument(
      '--silver-dir',
      type=Path,
      default=Path('data/silver/out'),
      help='Path to Silver layer output directory',
  )
  parser.add_argument(
      '-v',
      '--verbose',
      action='store_true',
      help='Verbose output',
  )

  args = parser.parse_args()

  logging.basicConfig(
      level=logging.DEBUG if args.verbose else logging.INFO,
      format='%(asctime)s [%(levelname)s] %(message)s',
      datefmt='%Y-%m-%d %H:%M:%S',
  )

  tickers = load_tickers_from_file(args.tickers_file)
  logger.info('Loaded %d tickers from %s', len(tickers), args.tickers_file)

  lower_config = load_config_from_file(args.lower_config)
  upper_config = load_config_from_file(args.upper_config)
  logger.info('Lower bound scenario: %s', lower_config.name)
  logger.info('Upper bound scenario: %s', upper_config.name)

  results = run_screening(
      tickers=tickers,
      lower_config=lower_config,
      upper_config=upper_config,
      start_date=args.start_date,
      end_date=args.end_date,
      tolerance_day=args.tolerance_day,
      min_hit_rate=args.min_hit_rate,
      min_inband_ratio=args.min_inband_ratio,
      dev_threshold=args.dev_threshold,
      concurrency=args.concurrency,
      gold_path=args.gold_path,
      silver_dir=args.silver_dir,
  )

  passed_tickers = [r.ticker for r in results if r.passed]
  passed_tickers.sort()

  args.output.parent.mkdir(parents=True, exist_ok=True)
  with open(args.output, 'w', encoding='utf-8') as f:
    for ticker in passed_tickers:
      f.write(f'{ticker}\n')

  logger.info('')
  logger.info('=' * 70)
  logger.info('Screening Complete')
  logger.info('=' * 70)
  logger.info('Total tickers: %d', len(tickers))
  logger.info('Passed: %d', len(passed_tickers))
  logger.info('Failed: %d', len(tickers) - len(passed_tickers))
  logger.info('Output: %s', args.output)

  if passed_tickers:
    logger.info('Passed tickers: %s', ', '.join(passed_tickers))


if __name__ == '__main__':
  main()
