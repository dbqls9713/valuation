"""
valuation_framework.py

A compact DCF framework ported from Google Sheets + Apps Script.

Core idea:
- Intrinsic Value per Share = PV of future Owner Earnings per share +
  Terminal Value
- oe0 is approximated as: cfo0 - CAPEX_for_OE
- Growth fades linearly from g0 to g_end (default: g_t + 1%p) over n_years
- Share count changes by a constant annual rate b estimated from
  diluted shares history

This module is intentionally data-source agnostic:
- You provide annual Core rows (Date, CFO, CAPEX, DilutedShares).
- Optionally provide TTM CFO/CAPEX to reflect the current year.

Next steps (not included here): attach API fetch + caching layer.
"""

from __future__ import annotations

from dataclasses import dataclass
from math import isfinite
from typing import Dict, List, Optional, Sequence

# ----------------------------
# Data models
# ----------------------------


@dataclass(frozen=True)
class CoreRow:
  """
    Annual core row.
    Convention: values should be numeric; CAPEX can be negative depending
    on data source. We will take abs(CAPEX) when needed.
    """
  date: str
  cfo: float
  capex: float
  diluted_shares: float


@dataclass(frozen=True)
class PreparedCore:
  """
    Prepared inputs needed for valuation.
    oe0 : base Owner Earnings (approx)
    sh0 : current diluted shares (latest)
    b   : annual share count reduction rate (buyback net of dilution).
          b>0 means shares shrink; b<0 means dilution dominates.
    """
  oe0: float
  sh0: float
  b: float


# ----------------------------
# Helpers
# ----------------------------


def _require_finite(x: float, name: str) -> float:
  if not isfinite(x):
    raise ValueError(f"{name} must be finite, got: {x}")
  return x


def get_setting(settings: Dict[str, float], name: str) -> float:
  if name not in settings:
    raise KeyError(f"Missing setting: {name}")
  v = settings[name]
  try:
    v = float(v)
  except Exception as e:
    raise ValueError(f"Invalid setting {name}: {settings[name]}") from e
  return _require_finite(v, f"settings[{name}]")


def _frange_inclusive(start: float, stop: float, step: float) -> List[float]:
  """
    Inclusive float range with rounding to reduce floating drift.
    Example: 0.05..0.12 step 0.01 -> [0.05, 0.06, ..., 0.12]
    """
  if step <= 0:
    raise ValueError("step must be > 0")
  # Protect against floating errors by computing number of steps
  n = int(round((stop - start) / step))
  if n < 0:
    return []
  out = []
  for k in range(n + 1):
    out.append(round(start + k * step, 12))
  return out


# ----------------------------
# Core preparation (ported from _prepareCore_)
# ----------------------------


def prepare_core(
    annual_rows: Sequence[CoreRow],
    *,
    # Optional TTM overrides (already computed externally; e.g., sum of
    # last 4 quarters)
    cfo_ttm: Optional[float] = None,
    capex_ttm: Optional[float] = None,
    # In your Apps Script you used: CapexForOE = capexTtm / 2
    # (i.e., a deliberate simplification / smoothing choice)
    capex_ttm_scale: float = 0.5,
    # Annual CAPEX average window (latest k years)
    capex_avg_years: int = 5,
) -> PreparedCore:
  """
    Port of _prepareCore_.

    Assumptions:
    - annual_rows are ordered from latest -> oldest (same as your Core
      sheet display).
    - Shares and CFO refer to annual.
    - CAPEX average uses abs(capex) over up to capex_avg_years.
    - b is inferred from latest vs oldest diluted shares over
      years_diff.
    - If TTM values are provided and finite/non-zero, cfo0 and
      CAPEX_for_OE are overridden.

    Returns PreparedCore(oe0, sh0, b).
    """
  if not annual_rows:
    raise ValueError("Core has no data (annual_rows is empty)")

  # Filter out obviously empty rows if caller passed them
  annual_filtered = [
      r for r in annual_rows
      if r.date and isfinite(r.cfo) and isfinite(r.diluted_shares)
  ]
  if not annual_filtered:
    raise ValueError("Core has no valid rows after filtering")

  latest = annual_filtered[0]
  sh0 = float(latest.diluted_shares)
  cfo_annual_latest = float(latest.cfo)

  # CAPEX average from latest years
  k = min(capex_avg_years, len(annual_filtered))
  capex_vals = [
      abs(float(r.capex)) for r in annual_filtered[:k] if isfinite(r.capex)
  ]
  if not capex_vals:
    raise ValueError("CAPEX values are missing/invalid in annual rows")
  capex_avg = sum(capex_vals) / len(capex_vals)

  # Buyback/dilution rate b from shares history
  oldest = annual_filtered[-1]
  sh_old = float(oldest.diluted_shares)
  years_diff = len(annual_filtered) - 1
  if years_diff <= 0 or sh0 <= 0 or sh_old <= 0:
    b = 0.0
  else:
    # b = 1 - (sh0/sh_old)^(1/years_diff)
    b = 1.0 - (sh0 / sh_old)**(1.0 / years_diff)

  # Defaults (annual-based)
  cfo0 = cfo_annual_latest
  capex_for_oe = capex_avg

  # Optional TTM overrides
  if cfo_ttm is not None:
    cfo_ttm = float(cfo_ttm)
    if isfinite(cfo_ttm) and cfo_ttm != 0:
      cfo0 = cfo_ttm

  if capex_ttm is not None:
    capex_ttm = abs(float(capex_ttm))
    if isfinite(capex_ttm) and capex_ttm != 0:
      capex_for_oe = capex_ttm * capex_ttm_scale

  oe0 = cfo0 - capex_for_oe
  return PreparedCore(oe0=oe0, sh0=sh0, b=b)


# ----------------------------
# Valuation (ported from IV_DCF / IV_TABLE / _ivCore_)
# ----------------------------


def iv_dcf(
    r: float,
    g0: float,
    *,
    settings: Dict[str, float],
    core: PreparedCore,
    # In Apps Script: gEnd = gT + 0.01
    g_end_spread: float = 0.01,
) -> float:
  """
    Equivalent of =IV_DCF(r, g0).

    Required settings:
    - n  : explicit forecast years (int >= 2)
    - g_t : terminal growth rate

    Constraints:
    - r > g_t (otherwise PV diverges)

    Returns:
    - Intrinsic Value per Share (float)
    """
  r = float(r)
  g0 = float(g0)
  _require_finite(r, "r")
  _require_finite(g0, "g0")

  n_years = int(round(get_setting(settings, "N")))
  g_t = get_setting(settings, "gT")

  if n_years < 2:
    raise ValueError("settings['N'] must be >= 2")
  if r <= g_t:
    return float("nan")

  g_end = g_t + g_end_spread

  pv = 0.0
  oe = float(core.oe0)

  for t in range(1, n_years + 1):
    # linear fade from g0 -> g_end over n_years years
    g = g0 + (g_end - g0) * ((t - 1) / (n_years - 1))
    oe *= (1.0 + g)

    shares = core.sh0 * ((1.0 - core.b)**t)
    if shares == 0:
      return float("nan")
    oeps = oe / shares

    pv += oeps / ((1.0 + r)**t)

    if t == n_years:
      tv = (oeps * (1.0 + g_t)) / (r - g_t)
      pv += tv / ((1.0 + r)**n_years)

  return pv


def iv_table(
    r_min: float,
    r_max: float,
    g0_min: float,
    g0_max: float,
    *,
    settings: Dict[str, float],
    core: PreparedCore,
    step: float = 0.01,
    g_end_spread: float = 0.01,
) -> List[List[float | str]]:
  """
    Equivalent of =IV_TABLE(rMin, rMax, g0Min, g0Max, step).

    Output format:
    - out[0][0] = 'r \\ g0'
    - first row headers are g0 values
    - first col headers are r values
    - body cells are intrinsic value per share
    """
  r_values = _frange_inclusive(float(r_min), float(r_max), float(step))
  g_values = _frange_inclusive(float(g0_min), float(g0_max), float(step))

  out: List[List[float | str]] = [[""
                                   for _ in range(len(g_values) + 1)]
                                  for rowidx in range(len(r_values) + 1)]
  out[0][0] = "r \\ g0"

  for j, g0 in enumerate(g_values, start=1):
    out[0][j] = g0
  for i, r in enumerate(r_values, start=1):
    out[i][0] = r

  for i, r in enumerate(r_values, start=1):
    for j, g0 in enumerate(g_values, start=1):
      out[i][j] = iv_dcf(r,
                         g0,
                         settings=settings,
                         core=core,
                         g_end_spread=g_end_spread)

  return out


# ----------------------------
# Minimal example (manual run)
# ----------------------------

if __name__ == "__main__":
  # Example usage (replace with real data)
  annual_data = [
      CoreRow("2024-12-31",
              cfo=125299000000,
              capex=-52535000000,
              diluted_shares=12447000000),
      CoreRow("2023-12-31",
              cfo=101746000000,
              capex=-32251000000,
              diluted_shares=12722000000),
      CoreRow("2022-12-31",
              cfo=91495000000,
              capex=-31485000000,
              diluted_shares=13159000000),
      CoreRow("2021-12-31",
              cfo=91652000000,
              capex=-24640000000,
              diluted_shares=13553480000),
      CoreRow("2020-12-31",
              cfo=65124000000,
              capex=-22281000000,
              diluted_shares=13740560000),
  ]

  settings_example = {"N": 5, "gT": 0.03}

  # Example TTM (sum of last 4 quarters); capex_ttm is absolute
  # (we abs() anyway)
  core_example = prepare_core(annual_data,
                              cfo_ttm=151_424_000_000,
                              capex_ttm=77_872_000_000,
                              capex_ttm_scale=1.0)

  print("PreparedCore:", core_example)
  print("IV (r=9%, g0=8%):",
        iv_dcf(0.09, 0.08, settings=settings_example, core=core_example))
  tbl = iv_table(0.08,
                 0.11,
                 0.05,
                 0.12,
                 settings=settings_example,
                 core=core_example,
                 step=0.01)
  print("Table size:", len(tbl), "x", len(tbl[0]))
  for row in tbl:
    print(row)
