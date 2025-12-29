# DCF Valuation Framework

## Goal
A reproducible, extensible DCF-based intrinsic value calculation framework with clean separation of concerns.

The framework is designed to:
- compute intrinsic value per share from simplified owner-earnings cashflows
- visualize sensitivity across assumptions (r × g0 table)
- keep the model small and hackable with clear interfaces
- separate data retrieval, preprocessing, calculation, and presentation

---

## Architecture

The framework uses four main components:

1. **AccountingRepository**: Interface for data retrieval
   - Provides TTM CFO, CAPEX, and yearly share counts
   - Implementations can fetch from APIs, databases, or in-memory

2. **DataPreprocessor**: Calculates base metrics
   - Owner Earnings (oe0) = CFO - CAPEX
   - Current shares (sh0)
   - Buyback rate (b) from historical share counts

3. **SimpleGoogleDcfModel**: DCF calculation engine
   - Initialized with fixed parameters (oe0, sh0, b, g_t, n_years)
   - Calculates intrinsic value by varying r and g0
   - Uses linear growth fade and share count adjustments

4. **SensitivityTableBuilder**: Generates 2D analysis tables
   - Varies discount rate and initial growth across ranges
   - Returns pandas DataFrame with intrinsic values

5. **Runner**: Orchestrates the workflow
   - Retrieves data → preprocesses → creates model → generates table

---

## Core Concept
**Intrinsic Value per Share** is the present value of future **Owner Earnings per Share** plus a **Terminal Value**, discounted by a required return.

### High-level formula
- PV = Σ [ OEPS_t / (1+r)^t ] + TV / (1+r)^N
- TV at year N uses Gordon Growth:
  - TV = OEPS_N × (1+g_t) / (r − g_t)
- Valid only when **r > g_t**

---

## Inputs

### 1) Accounting Data (from Repository)
- `yearly_shares_count`: Dict[year, shares] for historical share counts
TTM(Trailing Twelve Months) values:
- `cfo_ttm`: Cash Flow from Operations
- `capex_ttm`: Capital Expenditures (as absolute value)

### 2) Model Parameters
Fixed at model initialization:
- `terminal_growth_rate` (g_t): Terminal/perpetual growth rate
- `forecast_years` (n_years): Explicit forecast horizon (≥ 2)
- `g_end_spread`: Spread between terminal and fade-end growth (default: 0.01)

### 3) Sensitivity Analysis Parameters
Varied during analysis:
- `discount_rate` (r): Required return / discount rate
- `initial_growth_rate` (g0): Initial growth rate

---

## Cashflow Definition (Simplified Owner Earnings)
Base Owner Earnings calculated as:

- **oe0 = cfo_ttm − capex_ttm**

Note: Current implementation uses full CAPEX (no scaling factor).
This is intentionally simplified (no ΔNWC_required, no maintenance vs growth CAPEX separation).

---

## Share Count Adjustment (Buyback/Dilution)
Infer constant annual share change rate `b` from historical shares:

- b = 1 − (sh0 / sh_old)^(1/years_diff)

Interpretation:
- b > 0 → shares shrink (net buybacks), per-share value increases
- b < 0 → dilution dominates

Projected shares at year t:
- Shares_t = sh0 × (1 − b)^t

---

## Growth Assumption: Linear Fade
Linearly fading growth rate from initial to terminal-adjacent:

- `g0` = initial growth (variable parameter)
- `g_t` = terminal growth (fixed parameter)
- `g_end = g_t + g_end_spread` (e.g., 0.03 + 0.01 = 0.04)
- For t=1..n_years:
  - g_t = g0 + (g_end − g0) × (t−1)/(n_years−1)

Owner Earnings evolve:
- oe_t = oe_{t-1} × (1 + g_t)

---

## Acronyms (Glossary)
- **DCF**: Discounted Cash Flow
- **IV**: Intrinsic Value
- **OE**: Owner Earnings (variable: oe or oe0)
- **CFO**: Cash Flow from Operations
- **CAPEX**: Capital Expenditures
- **TTM**: Trailing Twelve Months
- **g0**: Initial Growth Rate (variable)
- **g_t**: Terminal Growth Rate (fixed)
- **g_end**: Terminal-adjacent growth (g_t + spread)
- **r**: Discount Rate / Required Return (variable)
- **n_years**: Forecast horizon in years (fixed)
- **b**: Annual share reduction rate (buyback rate)
- **sh0**: Current diluted shares
- **oe0**: Base owner earnings
- **OEPS**: Owner Earnings Per Share

---

## Known Limitations
- Full CAPEX used (no maintenance vs growth CAPEX separation)
- ΔNWC_required (working capital needs) ignored
- No enterprise→equity bridge (net cash/debt) yet
- No explicit margin/revenue drivers; growth applied directly to OE
- Single company example (Google) hardcoded in memory repository

---

## Extension Plan
1) Add real data sources:
   - fetch annual + quarterly from APIs (FMP, Alpha Vantage, etc.)
   - local caching (file/db) + TTL + retry/backoff
2) Improve OE calculation:
   - maintenance CAPEX ratio parameter
   - SBC / dilution consistency
3) Equity bridge:
   - net cash/debt adjustments
4) Multi-company support:
   - generic repository implementations
   - batch valuation & summary outputs
