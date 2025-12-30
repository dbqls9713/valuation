# data/ — Valuation Data Lake (Bronze only)

This repo stores raw data snapshots used for valuation research.
Current scope: **Bronze layer only** (raw, source-truth).

## Layer: Bronze (raw snapshots)
**Purpose**
- Cache vendor/API responses to minimize re-fetching
- Keep source-truth for reproducibility and independent verification

**Rules**
- Do NOT manually edit files under `data/bronze/`
- Prefer append/refresh by pipeline; avoid ad-hoc downloads
- Every raw file should have a sidecar metadata file: `*.meta.json`

## Directory layout (current)

- `data/bronze/sec/`
  - `company_tickers.json` : SEC company ticker ↔ CIK mapping
  - `company_tickers.json.meta.json` : provenance for the file
  - `companyfacts/CIK##########.json` : SEC XBRL Company Facts (raw)
  - `companyfacts/CIK##########.json.meta.json`
  - `submissions/CIK##########.json` : SEC submissions feed (raw, optional)
  - `submissions/CIK##########.json.meta.json`

### SEC: companyfacts
Raw response from SEC XBRL **Company Facts** API.
Provides time-series fundamentals by XBRL tag (e.g., us-gaap CFO, revenue, capex, shares) with period end, filing date, form (10-Q/10-K), and value.

### SEC: submissions
Raw response from SEC **Submissions** API.
Provides the company’s filing index (forms, filing dates, accession numbers, report dates, document links). Useful to validate completeness and enforce point-in-time backtests.

- `data/bronze/stooq/daily/`
  - `<ticker>.us.csv` : daily OHLCV prices from Stooq (raw)
  - `<ticker>.us.csv.meta.json`

## Metadata sidecar (`*.meta.json`)
Minimum recommended fields:
- `source` (e.g., "sec", "stooq")
- `url`
- `fetched_at_utc`
- `status_code`
- `nbytes`

## Update workflow
- Use `data/bronze/update.py` to fetch/refresh Bronze data.
```
python data/bronze/update.py \
  --tickers GOOGL MSFT META \
  --refresh-days 30 \
  --include-submissions
```
or use a file containing the tickers
```
python data/bronze/update.py \
  --tickers-file tickers_example.txt \
  --refresh-days 30 \
  --include-submissions
```
- Add new sources only by extending the pipeline (not manual file drops).

## Layer: Silver (normalized tables)

**Purpose**
- Convert Bronze raw snapshots into analysis-ready tables (Parquet).
- Keep minimal fields for valuation and backtesting (includes `filed` for PIT).

**Current outputs**
- `data/silver/sec/companies.parquet`
  - Company mappings (ticker ↔ CIK) with fiscal year end (`fye_mmdd`) from submissions.
- `data/silver/sec/facts_long.parquet`
  - Minimal long-format facts extracted from SEC companyfacts (CFO, CAPEX, shares).
  - Includes `fiscal_year` calculated from company's FYE (not companyfacts `fy`).
- `data/silver/sec/metrics_quarterly.parquet`
  - Quarterly discrete values (`q_val`) derived from YTD facts + rolling TTM (`ttm_val`).
  - Uses `fiscal_year` (not `fy`) for grouping to avoid comparative period mixing.
  - Convention: CAPEX is stored as absolute value (>= 0).
- `data/silver/stooq/prices_daily.parquet`
  - Daily OHLCV prices normalized from Stooq CSV.

**Update workflow**
- Build/refresh: `python -m data.silver.build`
- Validate: `python -m data.silver.validate`
