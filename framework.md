# DCF Valuation Framework (Sheets → Python Port)

## Goal
A reproducible, extensible DCF-based intrinsic value calculation framework.

The framework is designed to:
- compute intrinsic value per share from simplified owner-earnings cashflows
- visualize sensitivity across assumptions (r × g0 table)
- keep the model small and hackable (easy to extend with caching/API/multi-ticker)

---

## Core Concept
**Intrinsic Value per Share** is the present value of future **Owner Earnings per Share** plus a **Terminal Value**, discounted by a required return.

### High-level formula
- PV = Σ [ OEPS_t / (1+r)^t ] + TV / (1+r)^N
- TV at year N uses Gordon Growth:
  - TV = OEPS_N × (1+gT) / (r − gT)
- Valid only when **r > gT**

---

## Inputs

### 1) Annual Core data (required)
A list of annual rows ordered **latest → oldest**:
- `date`
- `cfo` = **CFO (Cash Flow from Operations)**
- `capex` = **CAPEX (Capital Expenditures)** (sign may be negative; we use abs where relevant)
- `diluted_shares` = **Weighted Average Diluted Shares Outstanding**

Used for:
- latest share count `Sh0`
- buyback/dilution rate `b`
- CAPEX average baseline (optional smoothing)

### 2) TTM overrides (optional but recommended)
TTM = trailing twelve months (sum of last 4 quarters):
- `cfo_ttm`
- `capex_ttm`

Used to reflect "this year" even before annual statements exist.

**Important modeling choice:**
- CAPEX_for_OE = `capex_ttm × capex_ttm_scale`
- In the current implementation, `capex_ttm_scale = 0.5` (ported from your Apps Script: `capexTtm / 2`)
  - This is a deliberate simplification to reduce volatility / approximate maintenance CAPEX.
  - You can set scale to 1.0 if you want full TTM CAPEX.

### 3) Settings (required)
- `N` : explicit forecast horizon (years), integer >= 2
- `gT`: terminal growth rate

---

## Cashflow Definition (Simplified Owner Earnings)
We approximate base Owner Earnings as:

- **oe0 = cfo0 − CAPEX_for_OE**
- cfo0:
  - default = latest annual CFO
  - if `cfo_ttm` is provided and valid, override with TTM
- CAPEX_for_OE:
  - default = average abs(CAPEX) of latest `k` years (k=5)
  - if `capex_ttm` is provided and valid, override with `capex_ttm × scale`

This is intentionally simplified (no ΔNWC_required, no maintenance vs
growth CAPEX separation yet).

---

## Share Count Adjustment (Buyback/Dilution)
We infer a constant annual share change rate `b` from diluted shares
history:

- b = 1 − (sh0 / sh_old)^(1/years_diff)

Interpretation:
- b > 0 → shares shrink (net buybacks), per-share value increases
- b < 0 → dilution dominates

Projected shares at year t:
- Shares_t = sh0 × (1 − b)^t

---

## Growth Assumption: Fade
We model a linearly fading growth rate:

- `g0` = initial growth
- `g_t` = terminal growth
- `g_end = g_t + 0.01` (terminal-adjacent growth inside explicit horizon;
  simplification)
- For t=1..n:
  - g_t = g0 + (g_end − g0) × (t−1)/(n−1)

Owner Earnings evolve:
- oe_t = oe_{t-1} × (1 + g_t)

---

## Outputs
### 1) Intrinsic Value per Share
`iv_dcf(r, g0, settings, core)` → float

### 2) Sensitivity Table (r × g0)
`iv_table(r_min..r_max, g0_min..g0_max, step=1%)` → 2D array with headers.

---

## Acronyms (Glossary)
- **DCF**: Discounted Cash Flow
- **IV**: Intrinsic Value
- **OE**: Owner Earnings (variable: oe or oe0)
- **CFO**: Cash Flow from Operations (variable: cfo or cfo0)
- **CAPEX**: Capital Expenditures
- **TTM**: Trailing Twelve Months
- **g0**: Initial Growth Rate
- **g_t**: Terminal Growth Rate (setting key: "gT")
- **r**: Discount Rate / Required Return
- **n**: Forecast horizon in years (setting key: "N")
- **SBC**: Stock-Based Compensation (not yet modeled)
- **RSU**: Restricted Stock Units

---

## Known Limitations (Intentional for v0)
- Maintenance CAPEX vs Growth CAPEX not separated (we approximate via scaling).
- ΔNWC_required (working capital needs) ignored.
- No enterprise→equity bridge (net cash/debt) yet.
- No explicit margin/revenue drivers; growth is applied directly to OE.
- No API fetching/caching layer in Python yet (data-source agnostic by design).

---

## Extension Plan (next milestones)
1) Add a data layer:
   - fetch annual + quarterly from a provider (FMP or alternatives)
   - local caching (file/db) + TTL + retry/backoff
2) Improve OE:
   - maintenance CAPEX ratio parameter
   - SBC / dilution consistency
3) Equity bridge:
   - net cash/debt adjustments
4) Multi-ticker runner:
   - batch valuation & summary outputs
