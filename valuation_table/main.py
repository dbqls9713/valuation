"""
Simplified DCF valuation model and sensitivity table builder.
"""

import argparse
from abc import ABC, abstractmethod
from math import isfinite
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd


class AccountingRepository(ABC):
  """
    Interface for storing and retrieving accounting data.

    Responsibilities:
    - Store annual financial data
    - Retrieve annual data (latest -> oldest order)
    - Optionally provide TTM (trailing twelve months) data
    """

  @abstractmethod
  def get_ttm_cfo(self, company_code: str) -> float:
    """
        Get trailing twelve months cash flow from operations.

        Returns:
            TTM CFO value or None if not available
        """

  @abstractmethod
  def get_ttm_capex(self, company_code: str) -> float:
    """
        Get trailing twelve months capital expenditures.

        Returns:
            TTM CAPEX value or None if not available
        """

  @abstractmethod
  def get_yearly_shares_count(self, company_code: str) -> Dict[int, int]:
    """
        Get yearly shares count.

        Returns:
            Yearly shares count as a dictionary of year to shares count
        """


class MemoryGoogleAccountingRepository(AccountingRepository):
  """In-memory repository with hardcoded Google financial data."""

  def __init__(self):
    self.cfo_ttm = 151_424_000_000
    self.capex_ttm = 77_872_000_000
    self.yearly_shares_count = {
        2024: 12447000000,
        2023: 12722000000,
        2022: 13159000000,
        2021: 13553480000,
        2020: 13740560000,
    }

  def get_ttm_cfo(self, company_code: str) -> int:
    assert company_code == "GOOG", "Only Google is supported"
    return self.cfo_ttm

  def get_ttm_capex(self, company_code: str) -> int:
    assert company_code == "GOOG", "Only Google is supported"
    return self.capex_ttm

  def get_yearly_shares_count(self, company_code: str) -> Dict[int, int]:
    assert company_code == "GOOG", "Only Google is supported"
    return self.yearly_shares_count


class GoldAccountingRepository(AccountingRepository):
  """
    Repository that reads from Gold layer valuation panel.

    Loads data from data/gold/out/valuation_panel.parquet and provides
    latest TTM values and historical shares count by fiscal year.
    """

  def __init__(self,
               gold_panel_path: str = "data/gold/out/valuation_panel.parquet"):
    """
        Initialize repository with Gold panel data.

        Args:
            gold_panel_path: Path to valuation_panel.parquet file
        """
    self.panel_path = Path(gold_panel_path)
    if not self.panel_path.exists():
      raise FileNotFoundError(f"Gold panel not found: {self.panel_path}. "
                              f"Run 'python -m data.gold.build' first.")
    self.panel = pd.read_parquet(self.panel_path)
    self.panel["end"] = pd.to_datetime(self.panel["end"])

  def get_ttm_cfo(self, company_code: str) -> float:
    """
        Get latest TTM CFO for the company.

        Args:
            company_code: Ticker symbol (e.g., 'GOOGL', 'MSFT', 'META')

        Returns:
            Latest TTM CFO value

        Raises:
            ValueError: If ticker not found or no TTM data available
        """
    ticker_data = self.panel[self.panel["ticker"] == company_code]
    if ticker_data.empty:
      raise ValueError(f"Ticker {company_code} not found in Gold panel")

    ticker_data = ticker_data.sort_values("end", ascending=False)
    latest = ticker_data.iloc[0]

    if pd.isna(latest["cfo_ttm"]):
      raise ValueError(f"No TTM CFO data for {company_code}")

    return float(latest["cfo_ttm"])

  def get_ttm_capex(self, company_code: str) -> float:
    """
        Get latest TTM CAPEX for the company.

        Args:
            company_code: Ticker symbol (e.g., 'GOOGL', 'MSFT', 'META')

        Returns:
            Latest TTM CAPEX value (positive)

        Raises:
            ValueError: If ticker not found or no TTM data available
        """
    ticker_data = self.panel[self.panel["ticker"] == company_code]
    if ticker_data.empty:
      raise ValueError(f"Ticker {company_code} not found in Gold panel")

    ticker_data = ticker_data.sort_values("end", ascending=False)
    latest = ticker_data.iloc[0]

    if pd.isna(latest["capex_ttm"]):
      raise ValueError(f"No TTM CAPEX data for {company_code}")

    return float(latest["capex_ttm"])

  def get_yearly_shares_count(self, company_code: str) -> Dict[int, int]:
    """
        Get yearly shares count from quarterly data.

        Uses most recent shares_q for each fiscal year. Automatically adjusts
        for stock splits by detecting large changes in share count.

        Args:
            company_code: Ticker symbol (e.g., 'GOOGL', 'MSFT', 'META')

        Returns:
            Dictionary mapping fiscal year to diluted shares count
            (adjusted for all stock splits)

        Raises:
            ValueError: If ticker not found or no shares data available
        """
    ticker_data = self.panel[self.panel["ticker"] == company_code]
    if ticker_data.empty:
      raise ValueError(f"Ticker {company_code} not found in Gold panel")

    ticker_data = ticker_data[ticker_data["shares_q"].notna()].copy()
    if ticker_data.empty:
      raise ValueError(f"No shares data for {company_code}")

    ticker_data = ticker_data.sort_values("end")

    # Detect and adjust for ALL stock splits (iterate backwards)
    ticker_data["shares_ratio"] = (ticker_data["shares_q"] /
                                   ticker_data["shares_q"].shift(1))

    # Identify all splits: ratio > 2 or < 0.5
    splits = ticker_data[(ticker_data["shares_ratio"] > 2) |
                         (ticker_data["shares_ratio"] < 0.5)].copy()

    if not splits.empty:
      # Process splits from most recent to oldest
      for idx in splits.index[::-1]:
        split_date = ticker_data.loc[idx, "end"]
        split_ratio = ticker_data.loc[idx, "shares_ratio"]

        # Adjust all shares before this split
        mask = ticker_data["end"] < split_date
        ticker_data.loc[mask, "shares_q"] *= split_ratio

        # Recalculate ratios after adjustment
        ticker_data["shares_ratio"] = (ticker_data["shares_q"] /
                                       ticker_data["shares_q"].shift(1))

    ticker_data["fiscal_year"] = ticker_data["end"].dt.year

    yearly_shares = (ticker_data.sort_values("end").groupby("fiscal_year")
                     ["shares_q"].last().to_dict())

    return {int(year): int(shares) for year, shares in yearly_shares.items()}


class DataPreprocessor:
  """
    Preprocessor for accounting data.

    Calculates:
    - oe0: Owner earnings (CFO - CAPEX)
    - sh0: Current diluted shares
    - b: Annual share reduction rate from historical data
    """

  def prepare(
      self,
      cfo_ttm: float,
      capex_ttm: float,
      yearly_shares_count: Dict[int, int],
  ) -> Tuple[float, int, float]:
    """
        Prepare accounting data for valuation.

        Args:
            cfo_ttm: TTM cash flow from operations
            capex_ttm: TTM capital expenditures
            yearly_shares_count: Dictionary of year to diluted shares

        Returns:
          (oe0, sh0, b): Owner earnings, current shares, buyback rate
        """
    if not yearly_shares_count:
      raise ValueError("yearly_shares_count cannot be empty")

    descending_years = sorted(yearly_shares_count.keys(), reverse=True)

    sh0 = yearly_shares_count[descending_years[0]]

    capex_for_oe = abs(capex_ttm)
    oe0 = cfo_ttm - capex_for_oe

    # b = 1 - (sh0/sh_old)^(1/years_diff)
    if len(yearly_shares_count) == 1:
      return oe0, sh0, 0.0
    sh_old = yearly_shares_count[descending_years[-1]]
    years_diff = descending_years[0] - descending_years[-1]
    if years_diff > 0 and sh0 > 0 and sh_old > 0:
      b = 1.0 - (sh0 / sh_old)**(1.0 / years_diff)
      return oe0, sh0, b
    return oe0, sh0, 0.0


class SimpleGoogleDcfModel:
  """
    DCF model with linear growth fade and share count adjustments.

    Initialized with fixed parameters (oe0, sh0, b, g_t, n_years).
    Calculate intrinsic value by varying discount rate and initial growth.
    """

  def __init__(
      self,
      initial_owner_earnings: float,
      initial_total_shares: int,
      share_count_reduction_rate: float,
      terminal_growth_rate: float,
      forecast_years: int,
      g_end_spread: float = 0.01,
  ):
    """
        Initialize model with fixed parameters.

        Args:
            initial_owner_earnings: Base owner earnings (oe0)
            initial_total_shares: Current diluted shares (sh0)
            share_count_reduction_rate: Annual share reduction rate (b)
            terminal_growth_rate: Terminal growth rate (g_t)
            forecast_years: Forecast horizon (n_years)
            g_end_spread: Spread between terminal and end growth
                         (default: 0.01)
        """
    self.oe0 = float(initial_owner_earnings)
    self.sh0 = float(initial_total_shares)
    self.b = float(share_count_reduction_rate)
    self.g_t = float(terminal_growth_rate)
    self.n_years = int(forecast_years)
    self.g_end_spread = g_end_spread

    if not isfinite(self.g_t):
      raise ValueError("terminal_growth_rate must be finite")
    if self.n_years < 2:
      raise ValueError("forecast_years must be >= 2")

  def calculate_intrinsic_value(
      self,
      discount_rate: float,
      initial_growth_rate: float,
  ) -> float:
    """
        Calculate intrinsic value per share using DCF.

        Args:
            discount_rate: Required return (r)
            initial_growth_rate: Initial growth rate (g0)

        Returns:
            Intrinsic value per share
        """
    r = float(discount_rate)
    g0 = float(initial_growth_rate)

    if not isfinite(r) or not isfinite(g0):
      return float("nan")

    if r <= self.g_t:
      return float("nan")

    g_end = self.g_t + self.g_end_spread

    pv = 0.0
    oe = self.oe0

    for t in range(1, self.n_years + 1):
      g = g0 + (g_end - g0) * ((t - 1) / (self.n_years - 1))
      oe *= (1.0 + g)

      shares = self.sh0 * ((1.0 - self.b)**t)
      if shares == 0:
        return float("nan")

      oeps = oe / shares

      pv += oeps / ((1.0 + r)**t)

      if t == self.n_years:
        tv = (oeps * (1.0 + self.g_t)) / (r - self.g_t)
        pv += tv / ((1.0 + r)**self.n_years)

    return pv


class SensitivityTableBuilder:
  """
    Builds 2D sensitivity tables for intrinsic value.

    Varies discount rate (r) and initial growth rate (g0) across
    specified ranges to generate a DataFrame of intrinsic values.
    """

  def __init__(
      self,
      min_initial_growth_rate: float,
      max_initial_growth_rate: float,
      min_discount_rate: float,
      max_discount_rate: float,
      step: float = 0.01,
  ):
    self.min_initial_growth_rate = min_initial_growth_rate
    self.max_initial_growth_rate = max_initial_growth_rate
    self.min_discount_rate = min_discount_rate
    self.max_discount_rate = max_discount_rate
    self.step = step

  def _frange_inclusive(self, start: float, stop: float,
                        step: float) -> List[float]:
    """
        Inclusive float range with rounding to reduce floating drift.
        Example: 0.05..0.12 step 0.01 -> [0.05, 0.06, ..., 0.12]
        """
    if step <= 0:
      raise ValueError("step must be > 0")
    n = int(round((stop - start) / step))
    if n < 0:
      return []
    out = []
    for k in range(n + 1):
      out.append(round(start + k * step, 12))
    return out

  def build(
      self,
      valuation_model: SimpleGoogleDcfModel,
  ) -> pd.DataFrame:
    """
        Build a sensitivity table varying r and g0.

        Args:
            valuation_model: Model with prepared data already initialized

        Returns:
            Sensitivity table as a pandas DataFrame
        """
    r_values = self._frange_inclusive(self.min_discount_rate,
                                      self.max_discount_rate, self.step)
    g_values = self._frange_inclusive(self.min_initial_growth_rate,
                                      self.max_initial_growth_rate, self.step)

    data_rows = []
    for r in r_values:
      row_data = []
      for g0 in g_values:
        iv = valuation_model.calculate_intrinsic_value(
            discount_rate=r,
            initial_growth_rate=g0,
        )
        row_data.append(iv)
      data_rows.append(row_data)

    r_labels = [f"{r:.1%}" for r in r_values]
    g_labels = [f"{g:.1%}" for g in g_values]

    df = pd.DataFrame(data_rows, index=r_labels, columns=g_labels)
    df.index.name = "Discount Rate (r)"
    df.columns.name = "Initial Growth (g0)"

    return df


class Runner:
  """
    Orchestrates the full valuation workflow.

    Retrieves data from repository, preprocesses it, creates valuation
    model, builds sensitivity table, and displays results.
    """

  def __init__(
      self,
      company_code: str,
      accounting_repository: AccountingRepository,
      data_preprocessor: DataPreprocessor,
      sensitivity_table_builder: SensitivityTableBuilder,
      terminal_growth_rate: float = 0.03,
      forecast_years: int = 5,
  ):
    self.company_code = company_code
    self.repository = accounting_repository
    self.preprocessor = data_preprocessor
    self.sensitivity_table_builder = sensitivity_table_builder
    self.terminal_growth_rate = terminal_growth_rate
    self.forecast_years = forecast_years

  def run(self) -> None:
    """Run the full valuation analysis and display results."""
    print("=" * 70)
    print(f"DCF Valuation Analysis - {self.company_code}")
    print("=" * 70)

    cfo_ttm = self.repository.get_ttm_cfo(self.company_code)
    capex_ttm = self.repository.get_ttm_capex(self.company_code)
    shares_dict = self.repository.get_yearly_shares_count(self.company_code)

    print("\nInput Data:")
    print(f"  TTM CFO: ${cfo_ttm:,.0f}")
    print(f"  TTM CAPEX: ${capex_ttm:,.0f}")
    print(f"  Share count history ({list(shares_dict.keys())} years):")
    oe0, sh0, b = self.preprocessor.prepare(
        cfo_ttm=cfo_ttm,
        capex_ttm=capex_ttm,
        yearly_shares_count=shares_dict,
    )

    print("\nPrepared Data:")
    print(f"  Owner Earnings (OE0): ${oe0:,.0f}")
    print(f"  Current Shares (sh0): {sh0:,.0f}")
    print(f"  Buyback Rate (b): {b:.4f} ({b*100:.2f}%/year)")

    model = SimpleGoogleDcfModel(
        initial_owner_earnings=oe0,
        initial_total_shares=sh0,
        share_count_reduction_rate=b,
        terminal_growth_rate=self.terminal_growth_rate,
        forecast_years=self.forecast_years,
    )

    print("\n" + "=" * 70)
    print("Sensitivity Analysis Table (Intrinsic Value per Share)")
    print("=" * 70)

    df = self.sensitivity_table_builder.build(valuation_model=model)

    print("\n" + df.to_string(float_format=lambda x: f"${x:.2f}"))
    print("\n" + "=" * 70)


if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  parser.add_argument("--company-code", type=str, required=True)
  parser.add_argument(
      "--data-source",
      type=str,
      choices=["memory", "gold"],
      default="gold",
      help="Data source: 'memory' (hardcoded) or 'gold' (panel)")
  parser.add_argument("--gold-panel-path",
                      type=str,
                      default="data/gold/out/valuation_panel.parquet",
                      help="Path to Gold panel parquet file")
  parser.add_argument("--min-initial-growth-rate",
                      type=float,
                      required=True,
                      default=0.05)
  parser.add_argument("--max-initial-growth-rate",
                      type=float,
                      required=True,
                      default=0.12)
  parser.add_argument("--min-discount-rate",
                      type=float,
                      required=True,
                      default=0.08)
  parser.add_argument("--max-discount-rate",
                      type=float,
                      required=True,
                      default=0.12)
  parser.add_argument("--terminal-growth-rate", type=float, default=0.03)
  parser.add_argument("--forecast-years", type=int, default=10)
  args = parser.parse_args()

  if args.data_source == "memory":
    repository = MemoryGoogleAccountingRepository()
  else:
    repository = GoldAccountingRepository(args.gold_panel_path)

  preprocessor = DataPreprocessor()
  table_builder = SensitivityTableBuilder(
      args.min_initial_growth_rate,
      args.max_initial_growth_rate,
      args.min_discount_rate,
      args.max_discount_rate,
  )
  runner = Runner(
      args.company_code,
      repository,
      preprocessor,
      table_builder,
      terminal_growth_rate=args.terminal_growth_rate,
      forecast_years=args.forecast_years,
  )
  runner.run()
