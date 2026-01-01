# Scenario Configurations

JSON-based scenario configurations for valuation experiments.

## Directory Structure

```text
scenarios/
└── base/                    # Core scenarios
    ├── default.json        # Standard 10% discount, 10% growth
    ├── conservative.json   # 12% discount, 6% growth
    └── aggressive.json     # 6% discount, 12% growth
```

## Quick Start

### Run Single Scenario

```bash
python -m valuation.analysis.backtest_from_configs \
  --ticker AAPL \
  --start-date 2020-01-01 \
  --end-date 2024-12-31 \
  --configs scenarios/base/default.json \
  --output results/aapl_default.csv
```

### Compare Multiple Scenarios

```bash
python -m valuation.analysis.backtest_from_configs \
  --ticker AAPL \
  --start-date 2020-01-01 \
  --end-date 2024-12-31 \
  --config-dir scenarios/base \
  --output results/aapl_comparison.csv
```

## Config File Format

```json
{
  "name": "my_scenario",
  "pre_maint_oe": "ttm",
  "maint_capex": "ttm",
  "growth": "fixed_0p10",
  "fade": "linear",
  "shares": "avg_5y",
  "terminal": "gordon",
  "discount": "fixed_0p10",
  "n_years": 10
}
```

### Policy Naming

The policies are named to reflect what they approximate:

- **Pre-Maintenance OE**: Earnings before maintenance capital
  (approximated by CFO)
- **Maintenance CAPEX**: Capital required to maintain operations
  (approximated by CAPEX)
- **Owner Earnings (OE)** = Pre-Maintenance OE - Maintenance CAPEX

### Available Policy Options

See `valuation/scenarios/registry.py` for all available policies:

**Pre-Maintenance OE** (`pre_maint_oe`):

- `ttm`: TTM CFO (default)

**Maintenance CAPEX** (`maint_capex`):

- `ttm`: Raw trailing twelve months (default)

**Growth**:

- `fixed_0p06`: Fixed 6% growth rate
- `fixed_0p08`: Fixed 8% growth rate
- `fixed_0p10`: Fixed 10% growth rate (default)
- `fixed_0p12`: Fixed 12% growth rate
- `fixed_0p15`: Fixed 15% growth rate

**Fade**:

- `linear`: Linear fade to terminal rate (default)

**Shares**:

- `avg_5y`: 5-year average buyback rate (default)

**Terminal**:

- `gordon`: Gordon Growth at 3% (default)

**Discount**:

- `fixed_0p06`: 6% discount rate
- `fixed_0p08`: 8% discount rate
- `fixed_0p10`: 10% discount rate (default)
- `fixed_0p12`: 12% discount rate

## Creating Custom Scenarios

```bash
# Copy existing config
cp scenarios/base/default.json scenarios/base/my_scenario.json

# Edit the JSON file
vim scenarios/base/my_scenario.json
```

## Backward Compatibility

Legacy config files using `oe` and `capex` fields are automatically converted:

- `oe` → `pre_maint_oe`
- `capex` → `maint_capex`
