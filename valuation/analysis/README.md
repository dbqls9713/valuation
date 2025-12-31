# Valuation Analysis

Utilities for analyzing DCF valuations, including batch processing, sensitivity analysis, and CAPEX method comparisons.

## Batch Valuation

Run valuations for multiple tickers at a single point in time, comparing results across companies.

### Quick Start

```bash
# From ticker file
python -m valuation.analysis.batch_valuation \
    --tickers-file data/bronze/tickers_dow30.txt \
    --as-of-date 2024-12-31 \
    --output results/dow30_valuation.csv

# Specific tickers
python -m valuation.analysis.batch_valuation \
    --tickers AAPL GOOGL MSFT META \
    --as-of-date 2024-09-30 \
    --scenario default \
    --output results/bigtech.csv

# Different scenario
python -m valuation.analysis.batch_valuation \
    --tickers-file data/bronze/tickers_dow30.txt \
    --as-of-date 2024-12-31 \
    --scenario discount_6pct \
    --output results/dow30_6pct.csv \
    -v
```

### Python API

```python
# Recommended: Direct import
from valuation.analysis.batch_valuation import batch_valuation
from valuation.scenarios.config import ScenarioConfig

df = batch_valuation(
    tickers=['AAPL', 'GOOGL', 'MSFT'],
    as_of_date='2024-12-31',
    config=ScenarioConfig.default(),
    verbose=True,
)

# Analyze results
undervalued = df[df['margin_of_safety'] > 0]
print(f"Undervalued: {len(undervalued)}/{len(df)}")
print(undervalued[['ticker', 'iv_per_share', 'market_price', 'margin_of_safety']])

# Save to CSV
df.to_csv('results.csv', index=False)
```

### Features

- **Multiple Tickers**: Process entire lists (Dow 30, S&P 500, etc.) at once
- **Scenario Comparison**: Run different valuation scenarios
- **Full Diagnostics**: CSV includes all policy outputs (CAPEX, growth rates, etc.)
- **Summary Statistics**: Automatic summary with undervalued count, rankings
- **Error Handling**: Continues processing even if individual tickers fail

### Available Scenarios

- `default`: 10% discount, 3-year weighted CAPEX, CAGR growth
- `raw_capex`: Raw TTM CAPEX (no smoothing)
- `clipped_capex`: CAPEX intensity clipping
- `discount_6pct`: 6% discount rate

### Output CSV Columns

- **Core Results**: `ticker`, `as_of_date`, `scenario`, `iv_per_share`, `market_price`, `price_to_iv`, `margin_of_safety`
- **Policy Diagnostics**: All diagnostic outputs from CAPEX, growth, fade, shares, terminal, discount policies
  - CAPEX: `capex_capex_method`, `capex_weighted`, `capex_yearly_values`, etc.
  - Growth: `growth_raw_cagr`, `growth_clipped_cagr`, `growth_first_oeps`, etc.
  - Shares: `shares_buyback_rate`, `shares_sh_old`, `shares_sh_new`, etc.
  - Terminal: `terminal_g_terminal`
  - Discount: `discount_discount_rate`

### Example Output

```
2025-12-31 01:35:17 [INFO] Processing 3 tickers as of 2024-09-30
2025-12-31 01:35:17 [INFO] Using scenario: default
2025-12-31 01:35:17 [INFO]
2025-12-31 01:35:17 [INFO] [1/3] Processing AAPL...
2025-12-31 01:35:17 [INFO]   IV: $nan, Price: $0.00, MoS: 0.0%
2025-12-31 01:35:17 [INFO] [2/3] Processing GOOGL...
2025-12-31 01:35:17 [INFO]   IV: $nan, Price: $0.00, MoS: 0.0%
2025-12-31 01:35:17 [INFO] [3/3] Processing MSFT...
2025-12-31 01:35:17 [INFO]   IV: $231.24, Price: $416.76, MoS: -80.2%
2025-12-31 01:35:17 [INFO]
2025-12-31 01:35:17 [INFO] Saved 3 results to results.csv
2025-12-31 01:35:17 [INFO]
2025-12-31 01:35:17 [INFO] ======================================================================
2025-12-31 01:35:17 [INFO] Summary Statistics
2025-12-31 01:35:17 [INFO] ======================================================================
2025-12-31 01:35:17 [INFO] Total companies: 3
2025-12-31 01:35:17 [INFO] With market price: 1
2025-12-31 01:35:17 [INFO]
2025-12-31 01:35:17 [INFO] Intrinsic Value:
2025-12-31 01:35:17 [INFO]   Mean:   $231.24
2025-12-31 01:35:17 [INFO]   Median: $231.24
2025-12-31 01:35:17 [INFO]   Min:    $231.24 (MSFT)
2025-12-31 01:35:17 [INFO]   Max:    $231.24 (MSFT)
2025-12-31 01:35:17 [INFO]
2025-12-31 01:35:17 [INFO] Price/IV Ratio:
2025-12-31 01:35:17 [INFO]   Mean:   180.23%
2025-12-31 01:35:17 [INFO]   Median: 180.23%
2025-12-31 01:35:17 [INFO]
2025-12-31 01:35:17 [INFO] Margin of Safety:
2025-12-31 01:35:17 [INFO]   Mean:   -80.2%
2025-12-31 01:35:17 [INFO]   Median: -80.2%
2025-12-31 01:35:17 [INFO]
2025-12-31 01:35:17 [INFO] Undervalued (MoS > 0): 0 / 1 (0.0%)
2025-12-31 01:35:17 [INFO] ======================================================================
```

---

## Sensitivity Analysis

Generate 2D tables showing how intrinsic value varies across different discount rates and initial growth rates.

### Quick Start

```bash
# Basic usage with explicit rates
python -m valuation.analysis.sensitivity \
    --ticker GOOGL \
    --as-of-date 2024-09-30 \
    --discount-rates 0.08,0.10,0.12 \
    --growth-rates 0.06,0.08,0.10,0.12

# Using range specification
python -m valuation.analysis.sensitivity \
    --ticker AAPL \
    --as-of-date 2024-09-30 \
    --discount-min 0.08 \
    --discount-max 0.12 \
    --discount-step 0.01 \
    --growth-min 0.05 \
    --growth-max 0.15 \
    --growth-step 0.01

# Save to CSV
python -m valuation.analysis.sensitivity \
    --ticker META \
    --as-of-date 2024-09-30 \
    --discount-rates 0.10,0.12 \
    --growth-rates 0.06,0.08,0.10 \
    --output sensitivity_table.csv
```

### Python API

```python
from pathlib import Path
import pandas as pd
from valuation.run import load_gold_panel, adjust_for_splits
from valuation.domain import types
from valuation.analysis import SensitivityTableBuilder
from valuation.scenarios.config import ScenarioConfig

# Load data
panel = load_gold_panel(Path('data/gold/out/valuation_panel.parquet'))
panel = adjust_for_splits(panel)

# Construct fundamentals
fundamentals = types.FundamentalsSlice.from_panel(
    panel, 'GOOGL', pd.Timestamp('2024-09-30'))

# Build sensitivity table
config = ScenarioConfig.default()
builder = SensitivityTableBuilder(fundamentals, config)

table = builder.build(
    discount_rates=[0.08, 0.10, 0.12],
    initial_growth_rates=[0.06, 0.08, 0.10, 0.12],
)

print(table.to_string(float_format=lambda x: f'${x:.2f}'))
```

### Features

- **Flexible Rate Specification**: Use explicit lists or min/max/step ranges
- **Policy Integration**: Leverages the full policy system (CAPEX, growth, fade, etc.)
- **Multiple Scenarios**: Support for different scenario configurations
- **CSV Export**: Save results for further analysis
- **Verbose Mode**: See detailed diagnostics with `-v` flag

### Example Output

```
================================================================================
Sensitivity Analysis: GOOGL (as of 2024-09-30)
================================================================================

Scenario: default
OE0: $66.93B
Shares: 12.51B
Buyback: 1.75%/year
Terminal Growth: 3.00%
Forecast Years: 10

================================================================================
Intrinsic Value per Share ($)
================================================================================
Initial Growth    6.0%    8.0%   10.0%   12.0%
Discount Rate
8.0%           $150.57 $164.02 $178.52 $194.16
10.0%          $105.57 $114.64 $124.41 $134.92
12.0%           $80.75  $87.43  $94.62 $102.33
================================================================================
```

## Architecture

The sensitivity analysis module:
1. Pre-computes fixed inputs (OE0, shares, buyback rate) using policies
2. Varies discount rate and initial growth rate
3. Calls the pure DCF engine for each combination
4. Formats results as a 2D DataFrame

This design is efficient because:
- CAPEX, shares, terminal growth computed once
- Only fade policy and DCF engine called in the loop
- No data reloading or redundant calculations

---

## Scenario Comparison with

Plot prices and intrinsic values for a given ticker over time.

### Quick Start

```bash
# Basic usage
python -m valuation.analysis.plot_prices \
    --tickers AAPL GOOGL META MSFT

# From ticker file
python -m valuation.analysis.plot_prices \
    --tickers-file data/bronze/tickers_dow30.txt \
    --start-date 2020-01-01

# Custom output directory
python -m valuation.analysis.plot_prices \
    --tickers AAPL GOOGL \
    --output-dir charts/capex_analysis
```

### Features

- **Three CAPEX Methods**:
  - Raw TTM CAPEX
  - 3-Year Weighted Average (1:2:3)
  - Intensity Clipping (90th percentile cap)
- **Policy Integration**: Uses full valuation policy system
- **Time Series Charts**: IV trends vs market price
- **Statistics**: Average IV and Price/IV ratios

### Output

Creates PNG charts showing:
- IV from each CAPEX method
- Market price (filed+1 trading day)
- Average statistics for the period

Example: `AAPL_capex_comparison.png`
