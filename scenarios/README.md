# Scenario Configurations

JSON-based scenario configurations for systematic valuation experiments.

## Directory Structure

```text
scenarios/
├── base/                    # Core scenarios
│   ├── default.json        # Standard 10% discount, weighted CAPEX
│   ├── conservative.json   # 12% discount (higher hurdle)
│   └── aggressive.json     # 6% discount (lower hurdle)
│
├── capex_experiments/       # CAPEX method variations
│   ├── raw_ttm.json        # Raw TTM CAPEX
│   ├── weighted_3y.json    # 3-year weighted average
│   └── intensity_clipped.json  # Intensity-based clipping
│
├── discount_experiments/    # Discount rate sensitivity
│   ├── discount_6pct.json
│   ├── discount_8pct.json
│   ├── discount_10pct.json
│   └── discount_12pct.json
│
└── grid_search/            # Systematic parameter combinations
    └── (generated files)
```

## Quick Start

### 1. Run Single Scenario

```bash
python -m valuation.analysis.backtest_from_configs \
  --ticker AAPL \
  --start-date 2020-01-01 \
  --end-date 2024-12-31 \
  --configs scenarios/base/default.json \
  --output results/aapl_default.csv
```

### 2. Compare Multiple Scenarios

```bash
# Compare all CAPEX methods
python -m valuation.analysis.backtest_from_configs \
  --ticker AAPL \
  --start-date 2020-01-01 \
  --end-date 2024-12-31 \
  --config-dir scenarios/capex_experiments \
  --output results/aapl_capex_comparison.csv

# Compare discount rates
python -m valuation.analysis.backtest_from_configs \
  --ticker GOOGL \
  --start-date 2020-01-01 \
  --end-date 2024-12-31 \
  --config-dir scenarios/discount_experiments \
  --output results/googl_discount_sensitivity.csv
```

### 3. Generate Grid Search Configs

```bash
# CAPEX × Discount grid (3 × 4 = 12 combinations)
python -m valuation.analysis.generate_grid_configs \
  --capex raw_ttm weighted_3y_123 intensity_clipped \
  --discount fixed_0p06 fixed_0p08 fixed_0p10 fixed_0p12 \
  --output-dir scenarios/grid_search

# Run grid search
python -m valuation.analysis.backtest_from_configs \
  --ticker MSFT \
  --start-date 2020-01-01 \
  --end-date 2024-12-31 \
  --config-dir scenarios/grid_search \
  --output results/msft_grid_search.csv
```

## Config File Format

```json
{
  "name": "my_scenario",
  "capex": "weighted_3y_123",
  "growth": "cagr_3y_clip",
  "fade": "linear",
  "shares": "avg_5y",
  "terminal": "gordon",
  "discount": "fixed_0p10",
  "n_years": 10,
  "policy_params": {}
}
```

### Available Policy Options

See `valuation/scenarios/registry.py` for all available policies:

**CAPEX**:

- `raw_ttm`: Raw trailing twelve months
- `weighted_3y_123`: 3-year weighted (1:2:3)
- `weighted_5y_12345`: 5-year weighted (1:2:3:4:5)
- `intensity_clipped`: Intensity-based clipping

**Growth**:

- `cagr_3y_clip`: 3-year CAGR with 4% threshold, 0-18% clip
- `cagr_5y_clip`: 5-year CAGR with 4% threshold, 0-18% clip

**Fade**:

- `linear`: Linear fade (1% spread)
- `geometric`: Geometric/exponential fade
- `step_5y`: Step for 5 years, then fade

**Shares**:

- `avg_5y`: 5-year average buyback rate
- `zero_buyback`: No buyback assumed

**Terminal**:

- `gordon`: Gordon Growth at 3%

**Discount**:

- `fixed_0p06`: 6% discount rate
- `fixed_0p08`: 8% discount rate
- `fixed_0p10`: 10% discount rate
- `fixed_0p12`: 12% discount rate

## Creating Custom Scenarios

### Option 1: Edit JSON Directly

```bash
# Copy existing config
cp scenarios/base/default.json scenarios/my_experiments/my_scenario.json

# Edit the JSON file
vim scenarios/my_experiments/my_scenario.json
```

### Option 2: Generate Programmatically

```python
from valuation.scenarios.config import ScenarioConfig

config = ScenarioConfig(
    name='my_custom_scenario',
    capex='weighted_3y_123',
    growth='cagr_5y_clip',
    fade='geometric',
    shares='avg_5y',
    terminal='gordon',
    discount='fixed_0p08',
    n_years=15,
)

# Save to JSON
with open('scenarios/my_experiments/custom.json', 'w') as f:
    f.write(config.to_json())
```

## Batch Experiments

Process multiple tickers with multiple scenarios:

```bash
# Create ticker list
cat > tickers.txt << EOF
AAPL
GOOGL
MSFT
AMZN
EOF

# Run for each ticker (bash loop)
for ticker in $(cat tickers.txt); do
  python -m valuation.analysis.backtest_from_configs \
    --ticker $ticker \
    --start-date 2020-01-01 \
    --end-date 2024-12-31 \
    --config-dir scenarios/capex_experiments \
    --output results/${ticker}_capex_comparison.csv
done
```

## Analysis Tips

### 1. Compare Scenarios

```python
import pandas as pd

df = pd.read_csv('results/aapl_capex_comparison.csv')

# Average IV by scenario
df.groupby('scenario')['iv_per_share'].mean()

# Price/IV ratio distribution
df.groupby('scenario')['price_to_iv'].describe()
```

### 2. Find Best Scenario

```python
# Scenario with lowest average valuation error
df.groupby('scenario').apply(
    lambda x: (x['price_to_iv'] - 1).abs().mean()
).sort_values()
```

### 3. Visualize Results

```python
import matplotlib.pyplot as plt

pivot = df.pivot_table(
    index='as_of_date',
    columns='scenario',
    values='iv_per_share'
)

pivot.plot(figsize=(12, 6))
plt.title('IV per Share by Scenario')
plt.ylabel('Intrinsic Value')
plt.show()
```

## Version Control

Track your experiments with git:

```bash
# Commit scenario configs
git add scenarios/
git commit -m "Add CAPEX comparison scenarios"

# Tag important experiments
git tag -a experiment-capex-v1 -m "CAPEX method comparison"
```

## Best Practices

1. **Naming Convention**: Use descriptive names like `capex_weighted__discount_10`
2. **Organization**: Group related scenarios in subdirectories
3. **Documentation**: Add comments in JSON (though not standard, use README)
4. **Reproducibility**: Always commit configs with results
5. **Grid Search**: Start small (2×2) before full grid (3×4×2)
