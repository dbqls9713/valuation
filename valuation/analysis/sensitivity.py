"""
Sensitivity analysis for DCF valuation.

This module provides tools to generate 2D sensitivity tables that show
how intrinsic value varies across different discount rates and initial
growth rates.

CLI Usage:
  python -m valuation.analysis.sensitivity \\
      --ticker GOOGL \\
      --as-of-date 2024-12-31 \\
      --discount-rates 0.08,0.10,0.12 \\
      --growth-rates 0.06,0.08,0.10,0.12
"""

import argparse
import logging
from pathlib import Path

import pandas as pd

from valuation.data_loader import ValuationDataLoader
from valuation.domain.types import FundamentalsSlice
from valuation.engine.dcf import compute_intrinsic_value
from valuation.scenarios.config import ScenarioConfig
from valuation.scenarios.registry import create_policies

logger = logging.getLogger(__name__)


class SensitivityTableBuilder:
  """
  Build 2D sensitivity tables for intrinsic value analysis.

  Varies discount rate and initial growth rate while keeping other
  parameters (CAPEX, shares, terminal growth) fixed based on the
  scenario configuration.
  """

  def __init__(
      self,
      fundamentals: FundamentalsSlice,
      base_config: ScenarioConfig,
  ):
    """
    Initialize sensitivity table builder.

    Args:
        fundamentals: Company fundamentals slice (PIT-safe)
        base_config: Base scenario configuration for policies
    """
    self.fundamentals = fundamentals
    self.base_config = base_config
    self.policies = create_policies(base_config)

    pre_maint_oe_result = self.policies['pre_maint_oe'].compute(fundamentals)
    maint_capex_result = self.policies['maint_capex'].compute(fundamentals)
    self.oe0 = pre_maint_oe_result.value - maint_capex_result.value

    shares_result = self.policies['shares'].compute(fundamentals)
    self.sh0 = fundamentals.latest_shares
    self.buyback_rate = shares_result.value

    terminal_result = self.policies['terminal'].compute()
    self.g_terminal = terminal_result.value

    logger.info('Initialized SensitivityTableBuilder')
    logger.info('  OE0: $%.2fB', self.oe0 / 1e9)
    logger.info('  Shares: %.2fB', self.sh0 / 1e9)
    logger.info('  Buyback rate: %.2f%%', self.buyback_rate * 100)
    logger.info('  Terminal growth: %.2f%%', self.g_terminal * 100)

  def build(
      self,
      discount_rates: list[float],
      initial_growth_rates: list[float],
  ) -> pd.DataFrame:
    """
    Build 2D sensitivity table.

    Args:
        discount_rates: List of discount rates (e.g., [0.08, 0.10, 0.12])
        initial_growth_rates: List of initial growth rates
                             (e.g., [0.06, 0.08, 0.10])

    Returns:
        DataFrame with discount rates as index, growth rates as columns,
        and intrinsic values per share as cell values
    """
    if not discount_rates:
      raise ValueError('discount_rates cannot be empty')
    if not initial_growth_rates:
      raise ValueError('initial_growth_rates cannot be empty')

    logger.info('Building sensitivity table: %d x %d', len(discount_rates),
                len(initial_growth_rates))

    data_rows = []

    for r in discount_rates:
      row_data = []
      for g0 in initial_growth_rates:
        fade_result = self.policies['fade'].compute(
            g0=g0,
            g_terminal=self.g_terminal,
            n_years=self.base_config.n_years,
        )

        iv, _, _ = compute_intrinsic_value(
            oe0=self.oe0,
            sh0=self.sh0,
            buyback_rate=self.buyback_rate,
            growth_path=fade_result.value,
            g_terminal=self.g_terminal,
            discount_rate=r,
        )
        row_data.append(iv)
      data_rows.append(row_data)

    r_labels = [f'{r:.1%}' for r in discount_rates]
    g_labels = [f'{g:.1%}' for g in initial_growth_rates]

    df = pd.DataFrame(data_rows, index=r_labels, columns=g_labels)
    df.index.name = 'Discount Rate'
    df.columns.name = 'Initial Growth'

    logger.info('Sensitivity table built successfully')
    return df


def _parse_float_list(s: str) -> list[float]:
  """Parse comma-separated float list."""
  return [float(x.strip()) for x in s.split(',')]


def _frange(start: float, stop: float, step: float) -> list[float]:
  """
  Inclusive float range with rounding.

  Args:
      start: Start value
      stop: Stop value (inclusive)
      step: Step size

  Returns:
      List of floats from start to stop (inclusive)
  """
  if step <= 0:
    raise ValueError('step must be > 0')
  n = int(round((stop - start) / step))
  if n < 0:
    return []
  return [round(start + k * step, 12) for k in range(n + 1)]


def main() -> None:
  """CLI entrypoint for sensitivity analysis."""
  parser = argparse.ArgumentParser(
      description='DCF Sensitivity Analysis',
      formatter_class=argparse.RawDescriptionHelpFormatter,
      epilog="""
Examples:
  # Basic usage with explicit rates
  python -m valuation.analysis.sensitivity \\
      --ticker GOOGL --as-of-date 2024-12-31 \\
      --discount-rates 0.08,0.10,0.12 \\
      --growth-rates 0.06,0.08,0.10,0.12

  # Using range specification
  python -m valuation.analysis.sensitivity \\
      --ticker AAPL --as-of-date 2024-12-31 \\
      --discount-min 0.08 --discount-max 0.12 --discount-step 0.01 \\
      --growth-min 0.05 --growth-max 0.15 --growth-step 0.01

  # With custom scenario
  python -m valuation.analysis.sensitivity \\
      --ticker META --as-of-date 2024-12-31 \\
      --scenario conservative \\
      --discount-rates 0.10,0.12,0.14 \\
      --growth-rates 0.04,0.06,0.08
      """)

  parser.add_argument('--ticker',
                      type=str,
                      required=True,
                      help='Ticker symbol (e.g., GOOGL, AAPL)')

  parser.add_argument('--as-of-date',
                      type=str,
                      required=True,
                      help='As-of date (YYYY-MM-DD)')

  parser.add_argument('--gold-path',
                      type=Path,
                      default=Path('data/gold/out/backtest_panel.parquet'),
                      help='Path to Gold panel')

  parser.add_argument('--silver-dir',
                      type=Path,
                      default=Path('data/silver/out'),
                      help='Path to Silver directory')

  parser.add_argument('--scenario',
                      type=str,
                      default='default',
                      help='Scenario config name')

  # Option 1: Explicit lists
  parser.add_argument('--discount-rates',
                      type=str,
                      help='Comma-separated discount rates (e.g., 0.08,0.10)')

  parser.add_argument('--growth-rates',
                      type=str,
                      help='Comma-separated growth rates (e.g., 0.06,0.08)')

  # Option 2: Range specification
  parser.add_argument('--discount-min',
                      type=float,
                      help='Minimum discount rate')
  parser.add_argument('--discount-max',
                      type=float,
                      help='Maximum discount rate')
  parser.add_argument('--discount-step',
                      type=float,
                      default=0.01,
                      help='Discount rate step (default: 0.01)')

  parser.add_argument('--growth-min', type=float, help='Minimum growth rate')
  parser.add_argument('--growth-max', type=float, help='Maximum growth rate')
  parser.add_argument('--growth-step',
                      type=float,
                      default=0.01,
                      help='Growth rate step (default: 0.01)')

  parser.add_argument('--output', type=Path, help='Output CSV path (optional)')

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

  logger.info('Loading data from: %s', args.gold_path)
  loader = ValuationDataLoader(gold_path=args.gold_path)
  panel = loader.load_panel()

  as_of_date = pd.Timestamp(args.as_of_date)
  logger.info('Constructing fundamentals: %s as of %s', args.ticker, as_of_date)

  fundamentals = FundamentalsSlice.from_panel(
      panel=panel,
      ticker=args.ticker,
      as_of_date=as_of_date,
  )

  if args.scenario == 'default':
    config = ScenarioConfig.default()
  else:
    raise ValueError(
        f'Unknown scenario: {args.scenario}. Available: default')

  logger.info('Using scenario: %s', args.scenario)

  # Parse discount rates
  if args.discount_rates:
    discount_rates = _parse_float_list(args.discount_rates)
  elif args.discount_min is not None and args.discount_max is not None:
    discount_rates = _frange(args.discount_min, args.discount_max,
                             args.discount_step)
  else:
    discount_rates = [0.08, 0.10, 0.12]
    logger.warning('No discount rates specified, using default: %s',
                   discount_rates)

  # Parse growth rates
  if args.growth_rates:
    growth_rates = _parse_float_list(args.growth_rates)
  elif args.growth_min is not None and args.growth_max is not None:
    growth_rates = _frange(args.growth_min, args.growth_max, args.growth_step)
  else:
    growth_rates = [0.06, 0.08, 0.10, 0.12]
    logger.warning('No growth rates specified, using default: %s', growth_rates)

  logger.info('Discount rates: %s', discount_rates)
  logger.info('Growth rates: %s', growth_rates)

  builder = SensitivityTableBuilder(fundamentals, config)

  logger.info('Building sensitivity table...')
  table = builder.build(
      discount_rates=discount_rates,
      initial_growth_rates=growth_rates,
  )

  print('\n' + '=' * 80)
  print(f'Sensitivity Analysis: {args.ticker} (as of {as_of_date.date()})')
  print('=' * 80)
  print(f'\nScenario: {args.scenario}')
  print(f'OE0: ${builder.oe0 / 1e9:.2f}B')
  print(f'Shares: {builder.sh0 / 1e9:.2f}B')
  print(f'Buyback: {builder.buyback_rate * 100:.2f}%/year')
  print(f'Terminal Growth: {builder.g_terminal * 100:.2f}%')
  print(f'Forecast Years: {config.n_years}')
  print('\n' + '=' * 80)
  print('Intrinsic Value per Share ($)')
  print('=' * 80)
  print(table.to_string(float_format=lambda x: f'${x:.2f}'))
  print('=' * 80 + '\n')

  if args.output:
    table.to_csv(args.output)
    logger.info('Saved to: %s', args.output)


if __name__ == '__main__':
  main()
