# Silver Layer: Normalized Tables

Cleaned, analysis-ready tables derived from Bronze SEC and Stooq data.

## Output Files

### `sec/companies.parquet`
- `ticker`, `cik10`, `title`, `fye_mmdd`
- Fiscal year end from submissions API

### `sec/facts_long.parquet`
- Minimal facts filtered by `metric_specs.py`
- Columns: `cik10`, `metric`, `end`, `filed`, `fy`, `fp`, `val`, `fiscal_year`, etc.
- One row per (cik10, metric, end, fy, fp) after deduplication
- `fiscal_year`: calculated from company FYE (not SEC's `fy`)
- CAPEX stored as absolute values

### `sec/metrics_quarterly.parquet`
- Quarterly discrete (`q_val`) and TTM (`ttm_val`) values
- YTD metrics (CFO, CAPEX) converted to discrete via differencing
- Q4 derived as: FY - Q3

### `stooq/prices_daily.parquet`
- Daily OHLCV: `symbol`, `date`, `open`, `high`, `low`, `close`, `volume`

## Build & Validate

```bash
python -m data.silver.build
python -m data.silver.validate --tol 100
```

Validation checks: uniqueness, date consistency, YTD identity, TTM correctness, CAPEX sign, price sanity.

## Deduplication Strategy (`dedup_latest_filed()`)

SEC data has duplicates: restatements, comparative periods, multiple values per period, mixed FY/Q labels.

**4-step process:**

1. **Group by `fiscal_year`** (calculated, not SEC's `fy`)
2. **Select primary `fy`** (most common in group)
3. **Within primary `fy`, deduplicate:**
   - Same end with Q1/Q2/Q3 and FY → prefer quarterly (drop FY)
   - Same (end, fp) with multiple values → select max value
4. **Fill missing periods** from other `fy` values (exclude already covered ends)

Result: Consistent fiscal year data, correct YTD→Quarter conversion.

## Critical Notes

### ⚠️ Look-Ahead Bias Warning

**Current implementation uses LATEST FILED VERSION.**

Example:
```
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

**Critical:** All YTD values must be from same filing version (ensured by deduplication).

### Fiscal Year Calculation

```python
if end.strftime("%m%d") <= fye_mmdd:
    fiscal_year = end.year
else:
    fiscal_year = end.year + 1
```

Assumes FYE doesn't change. If company changes FYE, historical assignments may be incorrect.

### Adding Metrics

Edit `metric_specs.py`:
```python
"NEW_METRIC": {
    "tags": ["XBRLTag1", "XBRLTag2"],  # try in order
    "namespace": "us-gaap",
    "unit": "USD",
    "is_ytd": True,   # if cumulative (cash flow items)
    "abs": False      # if want absolute values
}
```

Then rebuild: `python -m data.silver.build`

## Troubleshooting

**ytd_identity validation fails:**
- Check if all quarters use same `fy` value
- Verify larger YTD values are selected
- Ensure Q labels preferred over FY for same end

**Negative CAPEX:**
- Means Q2_ytd < Q1_ytd (inconsistent versions)
- Check deduplication logic

**Debug specific case:**
```python
facts = pd.read_parquet('data/silver/sec/facts_long.parquet')
problem = facts[
    (facts['cik10'] == 'CIK') &
    (facts['metric'] == 'METRIC') &
    (facts['fiscal_year'] == YEAR)
].sort_values('end')
print(problem[['end', 'fp', 'fy', 'filed', 'val']])
```

## TODO

- [ ] Add independent fixture data (known-good company/period samples) for regression testing
  - Create `data/validation/sec_fixture.csv` with manual spot-checked values
  - Include diverse cases: calendar vs fiscal year, restatements, edge cases
  - Integrate into `validate.py` with `--with-fixture` flag
  - Prevents silent degradation from code changes
