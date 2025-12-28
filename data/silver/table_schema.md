# data/silver/table_schema.md
# Silver Table Schema (latest-snapshot, non-PIT)

Silver layer stores **normalized, analysis-ready tables** derived from Bronze raw snapshots.
Current scope: **simple valuation inputs** (no point-in-time/backtest guarantees).

## Global conventions
- `cik10`: SEC CIK, **10-digit zero-padded string** (e.g., `"0000789019"`)
- Date columns are stored as `datetime64[ns]` in parquet.
- “latest-snapshot” means:
  - if multiple facts exist for the same logical observation, keep the one with the **latest `filed`** (most recently filed).

## Directory layout (current)

- `data/silver/sec/`
  - `companies.parquet`
  - `facts_long.parquet`
  - `metrics_quarterly.parquet`
  - Each has sidecar: `*.parquet.meta.json`

- `data/silver/stooq/`
  - `prices_daily.parquet`
  - Sidecar: `prices_daily.parquet.meta.json`

---

# 1) SEC — `companies.parquet`

Source: `data/bronze/sec/company_tickers.json`, `data/bronze/sec/submissions/CIK*.json`

### Purpose
Company identifier mapping for joining:
- ticker ↔ CIK
- display name
- fiscal year end (for calculating fiscal_year from period end dates)

### Grain
One row per ticker/company mapping.

### Columns
| column | type | meaning |
|---|---:|---|
| `cik10` | string | 10-digit CIK (zero padded) |
| `ticker` | string | Stock ticker (e.g., `MSFT`) |
| `title` | string | SEC company title/name |
| `fye_mmdd` | string | Fiscal year end in "MMDD" format (e.g., "0630" for June 30) |

> Note: `fye_mmdd` is extracted from submissions JSON `fiscalYearEnd` field.
> Used to calculate `fiscal_year` from period `end` dates in facts_long.

---

# 2) SEC — `facts_long.parquet`

Source: `data/bronze/sec/companyfacts/CIK##########.json`

### Purpose
Normalized long-form facts table (minimal tags only; driven by `METRIC_SPECS`).

### Grain
A row is one XBRL fact observation for a company + metric/tag + period.

### Columns
| column | type | meaning |
|---|---:|---|
| `cik10` | string | 10-digit CIK |
| `metric` | string | Logical metric name (e.g., `CFO`, `CAPEX`) |
| `namespace` | string | XBRL namespace (e.g., `us-gaap`) |
| `tag` | string | XBRL tag selected (e.g., `NetCashProvidedByUsedInOperatingActivities`) |
| `unit` | string | Unit code (e.g., `USD`) |
| `end` | datetime | Period end date |
| `filed` | datetime | Filing date for this fact |
| `fy` | Int64 | Fiscal year label from companyfacts (report year, reference only) |
| `fp` | string | Fiscal period label (often `Q1/Q2/Q3/FY`) |
| `form` | string | SEC form type (e.g., `10-Q`, `10-K`) |
| `val` | float | Raw numeric value (sign depends on tag/source) |
| `fiscal_year` | Int64 | Calculated fiscal year based on company's FYE |

### Important Note on `fy` vs `fiscal_year`
**`fy`** from companyfacts is the **report year** and may include comparative period data, causing misalignment with the actual fiscal year of the period end date.

**`fiscal_year`** is calculated based on:
- Company's `fiscalYearEnd` (fye_mmdd) from submissions
- Period `end` date
- Formula: `fiscal_year = end.year if end_mmdd <= fye_mmdd else end.year + 1`

**Always use `fiscal_year` for grouping/aggregation** to avoid mixing comparative periods.

### Notes
- This table is “facts cache” for downstream transforms.
- CAPEX sign can vary by company/tag; downstream step may apply `abs` convention.

---

# 3) SEC — `metrics_quarterly.parquet`

Derived from `facts_long.parquet`

### Purpose
Convert YTD-style cashflow facts to discrete quarterly values and provide TTM series.

### Grain
One row per company + metric + quarter end.

### Columns (expected)
| column | type | meaning |
|---|---:|---|
| `cik10` | string | 10-digit CIK |
| `metric` | string | Logical metric name (e.g., `CFO`, `CAPEX`) |
| `end` | datetime | Quarter end date |
| `filed` | datetime | Filing date used for this quarter (latest snapshot) |
| `fy` | Int64 | Fiscal year label from source (report year, reference only) |
| `fp` | string | Quarter label: `Q1`, `Q2`, `Q3`, `Q4` |
| `fiscal_year` | Int64 | Calculated fiscal year based on company's FYE |
| `q_val` | float | Discrete quarterly value |
| `ttm_val` | float | Trailing Twelve Months (rolling 4Q sum of `q_val`) |
| `tag` | string | XBRL tag selected |

### Transformation rules (YTD → quarter)
For each **fiscal_year** group (by `fiscal_year`, not `fy`):
- `Q1 = YTD(Q1)`
- `Q2 = YTD(Q2) - YTD(Q1)`
- `Q3 = YTD(Q3) - YTD(Q2)`
- `Q4 = YTD(FY) - YTD(Q3)` (if FY and Q3 exist)

For metrics with `abs=True` (e.g., CAPEX):
- YTD values are made absolute before subtraction
- Quarterly values are also made absolute to prevent negatives

TTM:
- `ttm_val = rolling_sum_4q(q_val)` ordered by `end` per (`cik10`, `metric`)

---

# 4) STOOQ — `prices_daily.parquet`

Source: `data/bronze/stooq/daily/*.csv`

### Purpose
Daily OHLCV prices for joining with SEC-derived fundamentals.

### Grain
One row per symbol per trading date.

### Columns (recommended)
| column | type | meaning |
|---|---:|---|
| `symbol` | string | Stooq symbol (e.g., `GOOGL.US`) |
| `date` | datetime | Trading date |
| `open` | float | Open |
| `high` | float | High |
| `low` | float | Low |
| `close` | float | Close |
| `volume` | float/int | Volume |

> Note: Bronze CSV columns from Stooq are usually `Open, High, Low, Close, Volume`.
> In Silver we normalize to lowercase and add `symbol`.

---

## Dataset metadata sidecar (`*.parquet.meta.json`)
Each Silver dataset may have a metadata JSON capturing:
- `generated_at_utc`
- output path, row/col counts, column list
- inputs (paths/sizes/mtimes)
- (optional) metric specs / chosen tags, etc.

This is primarily for reproducibility and debugging; it is not used as an analysis table.
