# Valuation Analysis

Utilities for analyzing DCF valuations: batch processing, backtesting,
sensitivity analysis, and scenario comparisons.

## Batch Valuation

Run valuations for multiple tickers at a single point in time.

```bash
# From ticker file
python -m valuation.analysis.batch_valuation \
    --tickers-file example/tickers/dow30.txt \
    --as-of-date 2024-12-31 \
    --output results/dow30_valuation.csv

# Specific tickers
python -m valuation.analysis.batch_valuation \
    --tickers AAPL GOOGL MSFT META \
    --as-of-date 2024-09-30 \
    --output results/bigtech.csv

# With custom scenario
python -m valuation.analysis.batch_valuation \
    --tickers-file example/tickers/dow30.txt \
    --as-of-date 2024-12-31 \
    --scenario discount_6pct \
    --output results/dow30_6pct.csv
```

**Features:**

- Multiple tickers at once (Dow 30, S&P 500, etc.)
- Full diagnostics (CAPEX, growth rates, all policy outputs)
- Auto summary statistics (undervalued count, rankings)

## Sensitivity Analysis

Generate 2D tables showing how IV varies across discount and growth rates.

```bash
# Explicit rates
python -m valuation.analysis.sensitivity \
    --ticker GOOGL \
    --as-of-date 2024-09-30 \
    --discount-rates 0.08,0.10,0.12 \
    --growth-rates 0.06,0.08,0.10,0.12

# Range specification
python -m valuation.analysis.sensitivity \
    --ticker AAPL \
    --as-of-date 2024-09-30 \
    --discount-min 0.08 --discount-max 0.12 --discount-step 0.01 \
    --growth-min 0.05 --growth-max 0.15 --growth-step 0.01 \
    --output sensitivity.csv
```

**Features:**

- 2D sensitivity tables (discount vs growth)
- Efficient: pre-computes fixed inputs once
- CSV export for further analysis

## Scenario Comparison Charts

Plot IV over time for multiple scenarios vs market price.

```bash
# Multiple scenarios from config directory
python -m valuation.analysis.plot_prices \
    --tickers AAPL GOOGL META MSFT \
    --config-dir example/scenarios \
    --start-date 2020-01-01 \
    --output-dir output/analysis/price_charts

# From ticker file
python -m valuation.analysis.plot_prices \
    --tickers-file example/tickers/dow30.txt \
    --config-dir example/scenarios \
    --start-date 2020-01-01
```

**Features:**

- Compare multiple scenarios side-by-side
- Time series: IV trends vs market price
- Auto-generated legends (shows only differing policies)
- Batch processing for multiple tickers

## Backtesting

Run valuations across multiple quarters to test policy performance.

```bash
# Single ticker, multiple scenarios
python -m valuation.analysis.backtest_from_configs \
    --ticker AAPL \
    --start-date 2020-01-01 \
    --end-date 2024-12-31 \
    --config-dir scenarios/grid_search \
    --output results/backtest_aapl.csv

# Generate grid search configs first
python -m valuation.analysis.generate_grid_configs \
    --output-dir scenarios/grid_search \
    --discount-rates 0.07,0.09 \
    --n-years 3,5,10
```

**Features:**

- Test scenarios across historical data
- Grid search for optimal policy combinations
- Hit rate analysis (how often was IV > price)
