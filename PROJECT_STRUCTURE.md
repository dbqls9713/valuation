# Project Structure

```text
valuation/
│
├── README.md                 # Main project documentation
├── requirements.in           # Python dependencies
├── pyproject.toml           # Python project config (yapf, build)
├── mypy.ini                 # Type checking config
│
├── data/                    # ETL pipeline (Bronze → Silver → Gold)
│   ├── README.md           # Data pipeline documentation
│   ├── shared/             # Common I/O utilities
│   ├── bronze/             # Raw data ingestion
│   │   └── update.py       # Download from SEC, Stooq
│   ├── silver/             # Normalized quarterly metrics
│   │   ├── README.md       # Silver layer details
│   │   ├── build.py        # Build normalized datasets
│   │   ├── validate.py     # Data validation
│   │   ├── config/         # Metric specs, schemas
│   │   ├── core/           # Pipeline, dataset, validator
│   │   ├── sources/        # SEC, Stooq implementations
│   │   └── shared/         # Transforms, validators
│   └── gold/               # Analytical panels
│       ├── README.md       # Gold layer details
│       ├── build.py        # Build panels
│       ├── validate.py     # Panel validation
│       ├── config/         # Panel schemas
│       ├── panels/         # Panel builders
│       └── shared/         # Common transformations
│
├── valuation/              # Valuation framework
│   ├── README.md          # Framework documentation
│   ├── run.py             # Single valuation runner
│   ├── domain/            # Typed domain objects
│   │   └── types.py       # FundamentalsSlice, ValuationResult, etc.
│   ├── engine/            # Pure DCF math
│   │   └── dcf.py         # compute_dcf_iv()
│   ├── policies/          # Estimation policies
│   │   ├── capex.py       # CAPEX methods (raw, weighted, clipped)
│   │   ├── growth.py      # Growth rate (CAGR, threshold, clip)
│   │   ├── fade.py        # Fade strategies (linear, geometric)
│   │   ├── shares.py      # Share buyback rate
│   │   ├── terminal.py    # Terminal value (Gordon growth)
│   │   └── discount.py    # Discount rate
│   ├── scenarios/         # Scenario configurations
│   │   ├── config.py      # ScenarioConfig dataclass
│   │   └── registry.py    # Policy registry + pre-defined scenarios
│   └── analysis/          # Analysis tools
│       ├── README.md      # Analysis tools documentation
│       ├── batch_valuation.py    # Multi-company valuation
│       ├── sensitivity.py        # Sensitivity tables (r × g0)
│       └── compare_capex.py      # CAPEX method comparison charts
│
├── results/               # Output files (CSV, charts)
└── tools/                # Utilities
    └── parquet_to_csv.py # Convert parquet to CSV
```

## Key Entry Points

### Data Pipeline

- **Ingest**: `python -m data.bronze.update --tickers AAPL GOOGL`
- **Build Silver**: `python -m data.silver.build`
- **Build Gold**: `python -m data.gold.build`
- **Validate**: `python -m data.silver.validate`, `python -m data.gold.validate`

### Valuation

- **Single**: `python -m valuation.run --ticker AAPL --as-of 2024-09-30`
- **Batch**: `python -m valuation.analysis.batch_valuation --tickers-file tickers.txt`
- **Sensitivity**: `python -m valuation.analysis.sensitivity --ticker AAPL`
- **CAPEX Compare**: `python -m valuation.analysis.compare_capex --tickers AAPL GOOGL`

## Documentation Map

- **`README.md`**: Project overview, quick start, architecture
- **`data/README.md`**: ETL pipeline, Bronze/Silver/Gold layers
- **`data/silver/README.md`**: Silver layer implementation details
- **`data/gold/README.md`**: Gold panel specifications
- **`valuation/README.md`**: Valuation framework, policies, adding new policies
- **`valuation/analysis/README.md`**: Analysis tools usage
