"""
Convert Parquet files to CSV for data validation.

Usage:
  python tools/parquet_to_csv.py <parquet_file> [options]

Examples:
  python tools/parquet_to_csv.py data/gold/valuation_panel.parquet
  python tools/parquet_to_csv.py data/silver/sec/facts_long.parquet \\
    -o facts.csv
  python tools/parquet_to_csv.py data/silver/sec/metrics_quarterly.parquet \\
    -p 20
  python tools/parquet_to_csv.py data/gold/valuation_panel.parquet \\
    --filter "ticker == 'AAPL'"
  python tools/parquet_to_csv.py data/gold/valuation_panel.parquet \\
    --cols ticker,end,cfo_ttm
"""

import argparse
from pathlib import Path

import pandas as pd


def convert_parquet_to_csv(
    parquet_path: str,
    output_path: str | None = None,
    preview: int = 0,
    filter_expr: str | None = None,
    columns: str | None = None,
    sort_by: str | None = None,
) -> None:
  """
    Convert parquet file to CSV.

    Args:
        parquet_path: Path to input parquet file
        output_path: Path to output CSV file (optional)
        preview: Number of rows to preview (0 = no preview)
        filter_expr: Pandas query expression to filter rows
        columns: Comma-separated column names to include
        sort_by: Column name to sort by
    """
  parquet_file = Path(parquet_path)

  if not parquet_file.exists():
    print(f"Error: File not found: {parquet_file}")
    return

  print(f"Reading: {parquet_file}")
  df = pd.read_parquet(parquet_file)

  print(f"  Original shape: {df.shape[0]:,} rows × {df.shape[1]} columns")

  # Apply filter
  if filter_expr:
    print(f"  Applying filter: {filter_expr}")
    try:
      df = df.query(filter_expr)
      print(f"  After filter: {df.shape[0]:,} rows")
    except Exception as e:  # pylint: disable=broad-except
      print(f"  Filter error: {e}")
      return

  # Select columns
  if columns:
    col_list = [c.strip() for c in columns.split(',')]
    missing = [c for c in col_list if c not in df.columns]
    if missing:
      print(f"  Error: Columns not found: {missing}")
      print(f"  Available columns: {list(df.columns)}")
      return
    df = df[col_list]
    print(f"  Selected columns: {col_list}")

  # Sort
  if sort_by:
    if sort_by not in df.columns:
      print(f"  Error: Sort column not found: {sort_by}")
      return
    df = df.sort_values(sort_by)
    print(f"  Sorted by: {sort_by}")

  print(f"  Final shape: {df.shape[0]:,} rows × {df.shape[1]} columns")
  print(f"  Columns: {list(df.columns)}")
  print(f"  Memory: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")

  print('\nData types:')
  for col, dtype in df.dtypes.items():
    non_null = df[col].notna().sum()
    null_pct = (1 - non_null / len(df)) * 100
    msg = f'  {col}: {dtype} ({non_null:,} non-null, {null_pct:.1f}% null)'
    print(msg)

  if preview > 0:
    print(f'\nPreview (first {preview} rows):')
    print('=' * 80)
    with pd.option_context('display.max_columns', None, 'display.width', 1000):
      print(df.head(preview).to_string())
    print('=' * 80)

  if output_path is None:
    output_file: Path = parquet_file.with_suffix('.csv')
  else:
    output_file = Path(output_path)

  print(f"\nWriting CSV: {output_file}")
  df.to_csv(output_file, index=False)

  csv_size = output_file.stat().st_size / 1024**2
  parquet_size = parquet_file.stat().st_size / 1024**2

  print(f"  CSV size: {csv_size:.2f} MB")
  print(f"  Parquet size: {parquet_size:.2f} MB")
  print(f"  Compression ratio: {csv_size / parquet_size:.1f}x")
  print('✓ Done!')

def main() -> None:
  parser = argparse.ArgumentParser(
      description='Convert Parquet files to CSV for validation',
      formatter_class=argparse.RawDescriptionHelpFormatter,
      epilog=__doc__)

  parser.add_argument('parquet_file', type=str, help='Path to parquet file')

  parser.add_argument('--output',
                      '-o',
                      type=str,
                      default=None,
                      help='Output CSV path (default: same name as input)')

  parser.add_argument(
      '--preview',
      '-p',
      type=int,
      default=5,
      help='Number of rows to preview (default: 5, 0 = no preview)')

  parser.add_argument(
      '--filter',
      '-f',
      type=str,
      default=None,
      help='Pandas query expression (e.g., "ticker == \'AAPL\'")')

  parser.add_argument('--cols',
                      '-c',
                      type=str,
                      default=None,
                      help='Comma-separated column names to include')

  parser.add_argument('--sort',
                      '-s',
                      type=str,
                      default=None,
                      help='Column name to sort by')

  args = parser.parse_args()

  convert_parquet_to_csv(
      parquet_path=args.parquet_file,
      output_path=args.output,
      preview=args.preview,
      filter_expr=args.filter,
      columns=args.cols,
      sort_by=args.sort,
  )

if __name__ == '__main__':
  main()
