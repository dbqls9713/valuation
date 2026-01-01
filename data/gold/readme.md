# Gold Layer: Model-Ready Valuation Panels

Model-ready datasets joining all metrics, fundamentals, and prices.

## Outputs

### `valuation_panel.parquet`

Latest filed version only - for **current valuation**.

**Primary Key**: `(ticker, end)`

### `backtest_panel.parquet`

All filed versions - for **PIT backtesting**.

**Primary Key**: `(ticker, end, filed)`

**Shares Normalization**: All shares values are normalized to the latest filed
version for each `(ticker, end)` to ensure consistency with split-adjusted prices.

### Common Columns

| Column | Type | Description |
|--------|------|-------------|
| `ticker` | str | Stock ticker symbol |
| `end` | datetime | Period end date (quarter end) |
| `filed` | datetime | Filing date (when data became public) |
| `cfo_q` | float | Operating cash flow (quarterly discrete) |
| `cfo_ttm` | float | Operating cash flow (trailing 12 months) |
| `capex_q` | float | Capital expenditures (quarterly, positive) |
| `capex_ttm` | float | Capital expenditures (TTM, positive) |
| `shares_q` | float | Diluted shares outstanding |
| `date` | datetime | Price date (next trading day after filed) |
| `price` | float | Stock closing price on `date` |
| `market_cap` | float | Market capitalization (`shares_q * price`) |

## Build

```bash
# Build both panels
python -m data.gold.build

# Build specific panel
python -m data.gold.build --panel valuation
python -m data.gold.build --panel backtest
```

## Point-in-Time Logic

**Critical for backtesting:**

1. `filed` date determines when data was publicly available
2. Price is joined using `merge_asof` with `direction="forward"`
3. For Q1 2020 filed on 2020-04-30: uses first price on or after 2020-04-30

### Shares Normalization (backtest_panel only)

When stock splits occur, older SEC filings have pre-split share counts while
prices are split-adjusted. The `backtest_panel` normalizes all shares to the
latest filed version:

```text
Example: WMT 3:1 split in Feb 2024

Before normalization:
  2023-01-31 (filed 2023-03-17): shares = 2.7B (pre-split)
  2023-01-31 (filed 2025-03-14): shares = 8.2B (post-split, restated)

After normalization:
  2023-01-31 (filed 2023-03-17): shares = 8.2B (normalized)
  2023-01-31 (filed 2025-03-14): shares = 8.2B
```

This ensures OE per share calculations are consistent with split-adjusted prices.

## Usage

### Current Valuation

```python
import pandas as pd

panel = pd.read_parquet('data/gold/out/valuation_panel.parquet')
googl = panel[panel['ticker'] == 'GOOGL']
```

### Backtesting with PIT

```python
panel = pd.read_parquet('data/gold/out/backtest_panel.parquet')

# Filter to data available as of 2023-06-30
as_of = '2023-06-30'
pit_data = panel[panel['filed'] <= as_of]

# Get latest filed version for each (ticker, end)
latest = pit_data.sort_values('filed').groupby(['ticker', 'end']).tail(1)
```

## Validation

```bash
python -m data.gold.validate
```

Checks:

1. Schema compliance (columns, types)
2. Primary key uniqueness
3. `filed >= end` constraint
4. Owner Earnings positive ratio

## Data Quality Notes

### Missing Data

- Ticker may not have prices (not in Stooq)
- Quarter may not have all metrics (not reported or tag mismatch)
- Result: NaN in respective columns

### Market Cap Calculation

- Uses `shares_q` (point-in-time shares for that quarter)
- If shares missing → market_cap will be NaN
