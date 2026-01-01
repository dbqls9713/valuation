# Silver Layer: Data Normalization

Normalized data from Bronze sources. YTD values preserved as-is.

## Architecture

```text
data/silver/
├── core/                    # Core abstractions
│   ├── pipeline.py          # Pipeline abstract class
│   └── dataset.py           # Dataset schema validation
│
├── sources/                 # Source-specific implementations
│   ├── sec/
│   │   ├── pipeline.py      # SECPipeline
│   │   ├── extractors.py    # companyfacts extraction
│   │   └── transforms.py    # SEC-specific transforms
│   │
│   └── stooq/
│       └── pipeline.py      # StooqPipeline
│
├── shared/                  # Shared utilities
│   ├── transforms.py        # Fiscal Year/Quarter calculations
│   └── validators.py        # Common validators
│
├── config/                  # Configuration
│   ├── metric_specs.py     # Metric definitions
│   └── schemas.py          # Data schemas
│
└── build.py                # CLI entry point
```

## Run

```bash
# Build Silver layer
python -m data.silver.build

# Validate
python -m data.silver.validate
```

## Output Files

### `sec/companies.parquet`

Company metadata with fiscal year end.

- Columns: `cik10`, `ticker`, `title`, `fye_mmdd`, `first_filing_date`

### `sec/facts_long.parquet`

Normalized SEC XBRL facts (YTD values preserved).

- Columns: `cik10`, `metric`, `fiscal_year`, `fiscal_quarter`, `filed`, `end`,
  `fy`, `fp`, `namespace`, `tag`, `unit`, `form`, `val`
- Primary Key: `[cik10, metric, fiscal_year, fiscal_quarter, filed]`
- YTD values stored in `val` (quarterly conversion in Gold layer)
- CAPEX normalized to absolute values
- SHARES normalized to actual count (not millions)

### `stooq/prices_daily.parquet`

Daily OHLCV prices.

- Columns: `symbol`, `date`, `open`, `high`, `low`, `close`, `volume`

## Key Design Decisions

### 1. YTD Values Preserved

Silver layer stores YTD (Year-to-Date) values as reported in SEC filings.
Quarterly conversion and TTM calculation are done in Gold layer.

This enables:

- Flexibility in aggregation methods (TTM, avg, median)
- Testing different approaches without rebuilding Silver

### 2. All Filed Versions Kept

For PIT (Point-in-Time) support, all filed versions are preserved:

```text
Q1 2020 filed 2020-04-30: $100M
Q1 2020 restated 2021-07-30: $110M
→ Both versions available for backtest at different dates
```

### 3. Fiscal Quarter Calculation

Uses company's FYE (Fiscal Year End) with ±7 day tolerance:

```python
Q1 end: FYE + 3 months
Q2 end: FYE + 6 months
Q3 end: FYE + 9 months
Q4 end: FYE (fiscal year end)
```

## Adding New Metrics

```python
# config/metric_specs.py
METRIC_SPECS = {
    'NEW_METRIC': {
        'namespace': 'us-gaap',
        'tags': ['XBRLTagName'],
        'unit': 'USD',
        'is_ytd': True,  # YTD cumulative
        'abs': False,    # Take absolute value
    },
}
```

## Relationship with Other Layers

```text
Bronze (raw) → Silver (normalized) → Gold (aggregated) → Valuation
              ^^^^^^^^^^^^^^^^^^^
              YTD values, all versions
```

**Silver responsibilities:**

- Normalize raw data (units, signs)
- Calculate fiscal year/quarter
- Deduplicate by (fiscal_year, fiscal_quarter, filed)
- Preserve all filed versions for PIT

**NOT Silver responsibilities (moved to Gold):**

- YTD to quarterly conversion
- TTM calculation
- Joining metrics into wide format
