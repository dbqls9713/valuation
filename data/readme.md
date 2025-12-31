# Data Pipeline (Bronze → Silver → Gold)

ETL pipeline for financial data processing with medallion architecture.

## Bronze Layer

**Purpose**: Raw data ingestion from external sources

**Sources**:
- SEC EDGAR API (companyfacts JSON)
- Stooq (historical stock prices CSV)

**Example**:
```bash
python -m data.bronze.update --tickers AAPL GOOGL
```
or
```bash
python -m data.bronze.update --tickers-file example/tickers/bigtech.txt \
  --sec-user-agent "StevenJobs valuation research (stevenjobs@gmail.com)"
```

**Output**: `data/bronze/out/sec/`, `data/bronze/out/stooq/`

## Silver Layer

**Purpose**: Normalize raw data into consistent quarterly metrics

**Key Features**:
1. **YTD → Quarterly conversion**: Cumulative to discrete quarters
2. **TTM calculation**: Rolling 4-quarter sum
3. **Fiscal year handling**: Company-specific fiscal year ends
4. **Shares normalization**: Actual count (not millions)
5. **PIT history**: All historical values for backtesting

**Outputs**:
- `companies.parquet`: Company metadata (ticker, cik, FYE)
- `metrics_quarterly.parquet`: Latest quarterly metrics (CFO, CAPEX, SHARES)
- `metrics_quarterly_history.parquet`: Historical (PIT) metrics
- `prices_daily.parquet`: Daily stock prices

**Metric Specifications** (`silver/config/metric_specs.py`):
```python
METRIC_SPECS = {
    'CFO': {...},      # Cash from operations
    'CAPEX': {...},    # Capital expenditures (6 tags for different industries)
    'SHARES': {...},   # Diluted shares outstanding
}
```

**See [silver/README.md](silver/README.md) for details.**

## Gold Layer

**Purpose**: Build analytical panels by joining Silver datasets

**Output**:
```
valuation_panel.parquet
Columns: [end, capex_q, capex_ttm, cfo_q, cfo_ttm, shares_q,
          filed, ticker, date, price, market_cap]
```

**Panel Construction**:
1. Load Silver datasets (metrics, companies, prices)
2. Pivot metrics to wide format (one row per company-quarter)
3. Join with stock prices (using `filed` date for PIT)
4. Validate schema and constraints

**See [gold/README.md](gold/README.md) for details.**

## Data Quality

### Silver Validations
1. Schema compliance (types, nullability)
2. Primary key uniqueness
3. YTD identity (Q1+Q2+Q3+Q4 ≈ Q4_ytd)
4. Fiscal year consistency

### Gold Validations
1. Schema compliance
2. Required fields (no NaN in critical columns)
3. Date alignment (filed ≤ date)

### Known Limitations
- **Restatements**: Silver includes all historical values; Gold uses latest filed
- **Financial services**: No CAPEX data (different valuation approach)
