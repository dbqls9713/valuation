# Gold Layer: Model-Ready Valuation Panel

Model-ready dataset joining all metrics, fundamentals, and prices.

## Output

### `valuation_panel.parquet`

Wide-format panel ready for ML/valuation models.

**Columns:**
- `ticker`: Stock ticker symbol
- `end`: Period end date (quarter end)
- `filed`: Filing date (when data became public)
- `date`: Price date (next trading day after filed)
- `cfo_q`: Operating cash flow (quarterly discrete)
- `cfo_ttm`: Operating cash flow (trailing 12 months)
- `capex_q`: Capital expenditures (quarterly discrete, positive)
- `capex_ttm`: Capital expenditures (TTM, positive)
- `shares_q`: Diluted shares outstanding (quarterly)
- `shares_ttm`: Diluted shares outstanding (TTM average)
- `price`: Stock closing price on `date`
- `market_cap`: Market capitalization (`shares_q * price`)

**Key characteristics:**
- One row per (ticker, quarter end)
- Point-in-time: price is first available after filing date
- All values in millions (USD) except price and shares
- NaN for missing metrics or unavailable prices

## Build

```bash
python -m data.gold.build
```

This joins:
1. Silver SEC metrics (pivoted to wide)
2. Silver company mappings (cik10 → ticker)
3. Silver prices (merge_asof for point-in-time)

## Point-in-Time Logic

**Critical:** Price is joined using `merge_asof` with `direction="forward"`:
- For Q1 2020 filed on 2020-04-30
- Uses first price available on or after 2020-04-30
- Ensures no look-ahead bias in backtesting

**Note:** Due to Silver layer using latest filed versions (see Silver README),
metrics may contain restated values. For strict PIT, Silver layer would need
to keep all filing versions.

## Usage Example

```python
import pandas as pd

panel = pd.read_parquet('data/gold/valuation_panel.parquet')

# Filter for specific ticker
googl = panel[panel['ticker'] == 'GOOGL']

# Calculate valuation multiples
panel['fcf_ttm'] = panel['cfo_ttm'] - panel['capex_ttm']
panel['fcf_yield'] = panel['fcf_ttm'] / panel['market_cap']
panel['price_to_fcf'] = panel['market_cap'] / panel['fcf_ttm']

# Time series for a ticker
googl = panel[panel['ticker'] == 'GOOGL'].set_index('date')
googl['fcf_yield'].plot()
```

## Data Quality Notes

### Missing Data
- Ticker may not have prices (not in Stooq)
- Quarter may not have all metrics (not reported or tag mismatch)
- Result: NaN in respective columns

### Market Cap Calculation
- Uses `shares_q` (point-in-time shares for that quarter)
- If shares missing → market_cap will be NaN
- Alternative: use shares_ttm (4-quarter average)

### Frequency
- Panel is quarterly (one row per quarter per ticker)
- Price is daily but subsampled to quarterly via point-in-time join
- For daily panel, would need to forward-fill metrics

## Validation

No automated validation yet. Manual checks:
1. Market cap should be reasonable (not 1000x or 0.001x expected)
2. FCF should be <= CFO (CAPEX always positive in our convention)
3. Filed date should be > end date
4. Price date should be >= filed date

## TODO

- [ ] Add automated validation checks
- [ ] Add shares_ttm average calculation
- [ ] Support daily frequency panel (forward-fill metrics)
- [ ] Add more metrics (revenue, earnings, etc.)
- [ ] Add derived ratios (P/E, EV/FCF, etc.)
