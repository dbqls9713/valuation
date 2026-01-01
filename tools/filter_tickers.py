'''
Filters ticker list by excluding Financial, Utility, and REIT sectors.
Uses Wikipedia S&P 500 data for sector classification.
'''
import argparse
from io import StringIO
from pathlib import Path

import pandas as pd
import requests

EXCLUDED_SECTORS = ['Financials', 'Utilities', 'Real Estate']


def fetch_sector_mapping() -> dict[str, str]:
  '''Fetches ticker -> sector mapping from Wikipedia S&P 500 list.'''
  headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)'}
  url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
  response = requests.get(url, headers=headers, timeout=30)
  response.raise_for_status()
  tables = pd.read_html(StringIO(response.text))
  df = tables[0]
  df['Symbol'] = df['Symbol'].str.replace('.', '-', regex=False)
  return dict(zip(df['Symbol'], df['GICS Sector']))


def load_tickers(path: Path) -> list[str]:
  '''Loads tickers from a file (one per line).'''
  lines = path.read_text().splitlines()
  return [line.strip() for line in lines if line.strip()]


def main() -> None:
  parser = argparse.ArgumentParser(
      description='Filter tickers by excluding specific sectors')
  parser.add_argument('input',
                      type=Path,
                      help='Input ticker file (one ticker per line)')
  parser.add_argument('--output',
                      '-o',
                      type=Path,
                      help='Output file path (default: overwrite input)')
  parser.add_argument('--no-filter',
                      action='store_true',
                      help='Skip filtering (useful for validation only)')
  args = parser.parse_args()

  output_path = args.output or args.input

  print(f'Loading tickers from {args.input}...')
  tickers = load_tickers(args.input)
  print(f'Total tickers: {len(tickers)}')

  print('Fetching sector mapping from Wikipedia...')
  sector_map = fetch_sector_mapping()

  filtered = []
  excluded = []
  unknown = []

  for ticker in tickers:
    sector = sector_map.get(ticker)
    if sector is None:
      unknown.append(ticker)
      filtered.append(ticker)
    elif args.no_filter or sector not in EXCLUDED_SECTORS:
      filtered.append(ticker)
    else:
      excluded.append((ticker, sector))

  print()
  print(f'Excluded: {len(excluded)} tickers')
  if excluded:
    for ticker, sector in excluded[:10]:
      print(f'  {ticker}: {sector}')
    if len(excluded) > 10:
      print(f'  ... and {len(excluded) - 10} more')

  if unknown:
    print(f'Unknown sector (kept): {len(unknown)} tickers')
    print(f'  {unknown[:10]}')

  print()
  print(f'Remaining: {len(filtered)} tickers')

  output_path.parent.mkdir(parents=True, exist_ok=True)
  output_path.write_text('\n'.join(sorted(filtered)))
  print(f'Saved to {output_path}')


if __name__ == '__main__':
  main()
