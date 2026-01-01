# Gold Layer: Model-Ready Valuation Panels

Model-ready datasets with quarterly/TTM values joined with prices.

## Responsibilities

Gold layer performs:

1. **YTD to Quarterly Conversion**: `Q2 = Q2_YTD - Q1_YTD`
2. **TTM Calculation**: Sum of last 4 quarters
3. **Metric Joining**: CFO, CAPEX, SHARES into wide format
4. **Price Joining**: PIT-safe price assignment

## Outputs

### `valuation_panel.parquet`

Latest filed version only - for **current valuation**.

**Primary Key**: `(ticker, end)`

### `backtest_panel.parquet`

All filed versions - for **PIT backtesting**.

**Primary Key**: `(ticker, end, filed)`

### Common Columns

| Column | Type | Description |
|--------|------|-------------|
| `ticker` | str | Stock ticker symbol |
| `end` | datetime | Period end date (quarter end) |
| `filed` | datetime | Filing date (when data became public) |
| `fiscal_year` | int | Calculated fiscal year |
| `fiscal_quarter` | str | Q1, Q2, Q3, Q4 |
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

## YTD to Quarterly Conversion

SEC cash flow statements report YTD cumulative values:

```text
Q1 filing: CFO_Q1_YTD = $100M → CFO_Q1 = $100M
Q2 filing: CFO_Q2_YTD = $250M → CFO_Q2 = $250M - $100M = $150M
Q3 filing: CFO_Q3_YTD = $400M → CFO_Q3 = $400M - $250M = $150M
FY filing: CFO_FY_YTD = $600M → CFO_Q4 = $600M - $400M = $200M
```

Uses PIT logic: for each filing, only uses previous quarter data that was filed
before the current filing date.

## TTM Calculation

Trailing Twelve Months = sum of most recent 4 quarters:

```text
CFO_TTM = CFO_Q1 + CFO_Q2 + CFO_Q3 + CFO_Q4
```

Requires 4 quarters of data; otherwise NULL.

## Point-in-Time Logic

**Critical for backtesting:**

1. `filed` date determines when data was publicly available
2. Price is joined using `merge_asof` with `direction="forward"`
3. For Q1 2020 filed on 2020-04-30: uses first price on or after 2020-04-30

## Validation

```bash
python -m data.gold.validate
```

Checks:

1. Schema compliance (columns, types)
2. Primary key uniqueness
3. `filed >= end` constraint
4. Owner Earnings positive ratio

## Relationship with Other Layers

```text
Silver → Gold → Valuation
         ^^^
         YTD→Q, TTM, Join
```

**Input from Silver:**

- `facts_long.parquet`: YTD values with all filed versions

**Output to Valuation:**

- `valuation_panel.parquet`: Latest version for current analysis
- `backtest_panel.parquet`: All versions for PIT backtesting
