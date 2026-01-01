# Silver Layer: Data Normalization and Transformation

Clean, normalized, analysis-ready tables with extensible and maintainable
structure.

## Architecture Overview

```text
data/silver/
├── core/                    # Core abstractions
│   ├── pipeline.py          # Pipeline abstract class
│   ├── dataset.py           # Dataset schema validation
│   └── validator.py         # Validator interface
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
│   ├── transforms.py        # TTM, Fiscal Year, etc.
│   ├── io.py               # Parquet I/O + metadata
│   └── validators.py        # Common validators
│
├── config/                  # Configuration
│   ├── metric_specs.py     # Metric definitions
│   └── schemas.py          # Data schemas
│
└── build.py                # CLI entry point
```

### Run validation

```bash
# Basic validation
python -m data.silver.validate

# Custom path
python -m data.silver.validate --silver-dir data/silver_out

# Include manual fixtures
python -m data.silver.validate --with-manual
```

## Output Files

### `sec/companies.parquet`

- Columns: `ticker`, `cik10`, `title`, `fye_mmdd`
- Fiscal year end from SEC submissions API

### `sec/facts_long.parquet`

- Minimal facts filtered by `metric_specs.py`
- Columns: `cik10`, `metric`, `end`, `filed`, `fy`, `fp`, `val`, etc.
- One row per (cik10, metric, fiscal_year, fiscal_quarter, filed)
- `fiscal_year`: calculated from company FYE (not SEC's `fy`)
- CAPEX stored as absolute values

### `sec/metrics_quarterly.parquet`

- Quarterly discrete (`q_val`) and TTM (`ttm_val`) values
- YTD metrics (CFO, CAPEX) converted to discrete via differencing
- Q4 derived as: FY - Q3

### `stooq/prices_daily.parquet`

- Daily OHLCV: `symbol`, `date`, `open`, `high`, `low`, `close`, `volume`

## Key Improvements

### 1. Clear Separation of Concerns

- **Pipeline**: Orchestrates entire ETL flow
- **Extractor**: Bronze → DataFrame conversion
- **Transformer**: Data normalization and transformation
- **Validator**: Data quality verification
- **Writer**: Parquet storage + metadata

### 2. Extensibility

Add new data sources by:

```python
from data.silver.core.pipeline import Pipeline

class NewSourcePipeline(Pipeline):
    def extract(self): ...
    def transform(self): ...
    def validate(self): ...
    def load(self): ...
```

### 3. Testability

Each component can be tested independently:

```python
extractor = SECCompanyFactsExtractor()
df = extractor.extract_companies(path, submissions)
assert len(df) > 0
```

### 4. Error Handling

- Per-file error isolation
- Detailed error messages
- Partial failure tolerance

### 5. Logging & Monitoring

```text
2025-12-30 08:32:22 - INFO - Running sec pipeline...
2025-12-30 08:32:22 - INFO - ✓ sec pipeline completed
2025-12-30 08:32:22 - INFO -   companies: (10507, 4)
2025-12-30 08:32:22 - INFO -   facts_long: (1043, 12)
```

## Data Quality Comparison

| Dataset | Old | New | Improvement |
|---------|-----|-----|-------------|
| Companies | 10,507 | 10,507 | ✓ |
| Facts Long | 929 | 1,043 | +12% |
| Metrics Quarterly | 902 | 1,028 | +14% |
| Prices Daily | 38,888 | 38,888 | ✓ |

**New architecture extracts more data!**

## Deduplication Strategy

SEC data contains duplicates from restatements, comparative periods,
multiple values per period, and mixed FY/Q labels.

**4-step process in `dedup_latest_filed()`:**

1. **Group by `fiscal_year`** (calculated, not SEC's `fy`)
2. **Select primary `fy`** (most common in group)
3. **Within primary `fy`, deduplicate:**
   - Same end with Q1/Q2/Q3 and FY → prefer quarterly (drop FY)
   - Same (end, fp) with multiple values → select max value
4. **Fill missing periods** from other `fy` values (exclude already covered ends)

Result: Consistent fiscal year data with correct YTD→Quarter conversion.

## Critical Notes

### Look-Ahead Bias Warning

**Current implementation uses LATEST FILED VERSION.**

Example:

```text
Q1 2020 originally filed 2020-04-30: $100M
Later restated in 2021-07-30: $110M

Current data: end=2020-03-31, filed=2021-07-30, val=$110M
→ Backtest at 2020-05-01 would use $110M (FUTURE INFORMATION)
```

**For true point-in-time:**

- Keep all versions in facts_long
- Filter by `filed <= backtest_date` at query time
- Then deduplicate

**Current workaround:** Use conservative lag (e.g., 45 days after quarter end).

### YTD vs Discrete

Cash flow metrics are YTD cumulative in SEC filings:

```python
Q1_discrete = Q1_ytd
Q2_discrete = Q2_ytd - Q1_ytd
Q3_discrete = Q3_ytd - Q2_ytd
Q4_discrete = FY_ytd - Q3_ytd
```

**Critical:** All YTD values must be from same filing version (ensured by
deduplication).

### Fiscal Year Calculation

```python
if end.strftime("%m%d") <= fye_mmdd:
    fiscal_year = end.year
else:
    fiscal_year = end.year + 1
```

Assumes FYE doesn't change. If company changes FYE, historical assignments
may be incorrect.

## Extension Examples

### Add new metric

```python
# config/metric_specs.py
METRIC_SPECS = {
    'REVENUE': {
        'namespace': 'us-gaap',
        'tags': ['RevenueFromContractWithCustomerExcludingAssessedTax'],
        'unit': 'USD',
        'is_ytd': True,
        'abs': False,
    },
}
```

### Add custom validator

```python
# shared/validators.py
class CustomValidator(Validator):
    def validate(self, name: str, df: pd.DataFrame) -> ValidationResult:
        errors = []
        # Custom validation logic
        return ValidationResult(is_valid=len(errors)==0, errors=errors)
```
