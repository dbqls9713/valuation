'''
Generate grid search scenario configs.

This script creates JSON config files for all combinations of parameters,
enabling systematic exploration of the parameter space.

Usage:
  # CAPEX × Discount grid
  python -m valuation.analysis.generate_grid_configs \
    --capex raw_ttm weighted_3y_123 intensity_clipped \
    --discount fixed_0p06 fixed_0p08 fixed_0p10 fixed_0p12 \
    --output-dir scenarios/grid_search

  # Full grid (CAPEX × Discount × N-Years)
  python -m valuation.analysis.generate_grid_configs \
    --capex raw_ttm weighted_3y_123 \
    --discount fixed_0p10 fixed_0p12 \
    --n-years 5 10 15 \
    --output-dir scenarios/grid_search_full

  # Multi-dimensional grid
  python -m valuation.analysis.generate_grid_configs \
    --capex raw_ttm weighted_3y_123 \
    --discount fixed_0p08 fixed_0p10 fixed_0p12 \
    --n-years 10 15 \
    --output-dir scenarios/grid_search_multi
'''

import argparse
import json
import logging
from itertools import product
from pathlib import Path
from typing import Dict, List

from valuation.scenarios.config import ScenarioConfig

logger = logging.getLogger(__name__)


def generate_grid_configs(
    capex_options: List[str],
    discount_options: List[str],
    growth_options: List[str],
    fade_options: List[str],
    shares_options: List[str],
    terminal_options: List[str],
    n_years_options: List[int],
) -> List[Dict]:
  '''
  Generate all combinations of policy parameters.

  Args:
    capex_options: List of CAPEX policy names
    discount_options: List of discount policy names
    growth_options: List of growth policy names
    fade_options: List of fade policy names
    shares_options: List of share policy names
    terminal_options: List of terminal policy names
    n_years_options: List of forecast year options

  Returns:
    List of config dictionaries
  '''
  configs = []

  combinations = product(
      capex_options,
      discount_options,
      growth_options,
      fade_options,
      shares_options,
      terminal_options,
      n_years_options,
  )

  for capex, discount, growth, fade, shares, terminal, n_years in combinations:
    parts = [capex, discount, growth, fade, shares, terminal, f'{n_years}y']
    name = '__'.join(parts)

    config = ScenarioConfig(
        name=name,
        capex=capex,
        growth=growth,
        fade=fade,
        shares=shares,
        terminal=terminal,
        discount=discount,
        n_years=n_years,
    )

    configs.append(config.to_dict())

  return configs


def save_configs(configs: List[Dict], output_dir: Path):
  '''
  Save configs to JSON files.

  Args:
    configs: List of config dictionaries
    output_dir: Output directory
  '''
  output_dir.mkdir(parents=True, exist_ok=True)

  for config in configs:
    config_name = config['name']
    filename = f'{config_name}.json'
    filepath = output_dir / filename

    with open(filepath, 'w', encoding='utf-8') as f:
      json.dump(config, f, indent=2)

    logger.info('Generated: %s', filename)


def main():
  '''CLI entrypoint for grid config generation.'''
  parser = argparse.ArgumentParser(
      description='Generate grid search scenario configs')

  parser.add_argument('--capex',
                      nargs='+',
                      default=['weighted_3y_123'],
                      help='CAPEX policy options')
  parser.add_argument('--discount',
                      nargs='+',
                      default=['fixed_0p10'],
                      help='Discount policy options')
  parser.add_argument('--growth',
                      nargs='+',
                      default=['cagr_3y_clip'],
                      help='Growth policy options')
  parser.add_argument('--fade',
                      nargs='+',
                      default=['linear'],
                      help='Fade policy options')
  parser.add_argument('--shares',
                      nargs='+',
                      default=['avg_5y'],
                      help='Share policy options')
  parser.add_argument('--terminal',
                      nargs='+',
                      default=['gordon'],
                      help='Terminal policy options')
  parser.add_argument('--n-years',
                      nargs='+',
                      type=int,
                      default=[10],
                      help='Forecast year options')
  parser.add_argument('--output-dir',
                      type=Path,
                      default=Path('scenarios/grid_search'),
                      help='Output directory')

  args = parser.parse_args()

  logger.info('Generating grid search configs...')
  logger.info('')
  logger.info('Parameters:')
  logger.info('  CAPEX: %s', args.capex)
  logger.info('  Discount: %s', args.discount)
  logger.info('  Growth: %s', args.growth)
  logger.info('  Fade: %s', args.fade)
  logger.info('  Shares: %s', args.shares)
  logger.info('  Terminal: %s', args.terminal)
  logger.info('  N Years: %s', args.n_years)
  logger.info('')

  configs = generate_grid_configs(
      capex_options=args.capex,
      discount_options=args.discount,
      growth_options=args.growth,
      fade_options=args.fade,
      shares_options=args.shares,
      terminal_options=args.terminal,
      n_years_options=args.n_years,
  )

  total = len(configs)
  logger.info('Total combinations: %d', total)
  logger.info('')

  save_configs(configs, args.output_dir)

  logger.info('')
  logger.info('✓ Generated %d config files in %s', total, args.output_dir)


if __name__ == '__main__':
  logging.basicConfig(
      level=logging.INFO,
      format='%(message)s',
  )
  main()
