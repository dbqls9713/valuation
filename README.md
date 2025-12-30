# DCF Valuation Framework

Production-grade DCF valuation framework with ETL pipeline, policy-based architecture, and backtesting capabilities.

## Quick Start

```bash
# 1. Data ingestion
python -m data.bronze.update --tickers AAPL GOOGL MSFT

# 2. Build pipeline
python -m data.silver.build && python -m data.gold.build

# 3. Run valuation
python -m valuation.run --ticker AAPL --as-of 2024-09-30

# 4. Batch valuation
python -m valuation.analysis.batch_valuation \
  --tickers-file data/bronze/tickers_dow30.txt \
  --as-of-date 2024-09-30 \
  --output results/dow30.csv -v
```

## Architecture

### Data Pipeline (Bronze → Silver → Gold)

```
Bronze (Raw)         Silver (Normalized)      Gold (Analytical)
├─ SEC filings    →  ├─ metrics_quarterly  →  └─ valuation_panel
└─ Stock prices      └─ companies
```

### Valuation Framework

```
Domain Types (FundamentalsSlice, MarketSlice)
       ↓
Policies (CAPEX, Growth, Fade, Shares, Terminal, Discount)
       ↓
DCF Engine (pure math)
       ↓
ValuationResult (IV, diagnostics)
```

## Project Structure

```
valuation/
├── README.md              # This file
├── PROJECT_STRUCTURE.md   # Detailed structure
│
├── data/                  # ETL pipeline
│   ├── README.md         # Data pipeline docs
│   ├── bronze/           # Raw data ingestion (SEC, Stooq)
│   ├── silver/           # Normalized metrics (quarterly)
│   │   └── README.md     # Silver layer details
│   └── gold/             # Analytical panels
│       └── README.md     # Gold layer details
│
├── valuation/            # Valuation framework
│   ├── README.md        # Framework docs
│   ├── run.py           # Single valuation
│   ├── domain/          # Typed domain objects
│   ├── engine/          # Pure DCF math
│   ├── policies/        # Estimation policies
│   ├── scenarios/       # Scenario configs
│   └── analysis/        # Analysis tools
│       ├── README.md    # Analysis docs
│       ├── batch_valuation.py    # Multi-company
│       ├── sensitivity.py        # Sensitivity tables
│       └── compare_capex.py      # CAPEX comparison
│
└── results/             # Output files (CSV, charts)
```

## Key Features

- **Policy-based architecture**: Easy experimentation with different methodologies
- **PIT (Point-in-Time)**: Only uses data available at backtest date
- **Multiple CAPEX methods**: Raw, 3yr weighted, intensity clipping
- **Batch processing**: Valuation for entire stock lists
- **Sensitivity analysis**: 2D tables (discount × growth rates)
- **Full diagnostics**: Every policy returns value + detailed diagnostics

## Data Coverage

**Supported**: 31 tickers across industries (Tech, Consumer, Industrial, Healthcare, Energy, Materials, Telecom, Retail)

**Not supported**: Financial services (Banks, Insurance) - different valuation approach needed

## Configuration

```python
from valuation.scenarios.config import ScenarioConfig

config = ScenarioConfig(
    name='custom',
    capex='weighted_3yr',     # or 'raw_ttm', 'intensity_clipped'
    growth='cagr',            # CAGR with 4% threshold, 0-18% clip
    fade='linear',            # Linear fade to terminal
    shares='avg_change',      # 5yr average share change
    terminal='perpetuity',    # Gordon growth at 3%
    discount=0.10,            # 10% discount rate
    n_years=10,               # 10-year explicit forecast
)
```

## Documentation

- **[PROJECT_STRUCTURE.md](PROJECT_STRUCTURE.md)**: Complete project structure and entry points
- **[data/README.md](data/README.md)**: ETL pipeline details (Bronze/Silver/Gold)
- **[valuation/README.md](valuation/README.md)**: Valuation framework and policies
- **[valuation/analysis/README.md](valuation/analysis/README.md)**: Analysis tools usage

## Development

### Adding New Policies

1. Create policy in `valuation/policies/`
2. Register in `valuation/scenarios/registry.py`
3. Use in `ScenarioConfig`

See [valuation/README.md](valuation/README.md) for details.

### Pre-commit Hooks

```bash
# Formatting and linting
yapf, pylint, mypy

# Run manually
pre-commit run --all-files
```

## Requirements

```bash
pip install pandas pyarrow pyyaml matplotlib requests
```

See `requirements.in` for exact versions.
