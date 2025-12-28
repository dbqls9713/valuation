"""
stooq_silver.py

Build Silver prices table from Bronze Stooq CSV files.
Reads: data/bronze/stooq/daily/*.csv
Writes: data/silver/stooq/prices_daily.parquet
"""

from __future__ import annotations

from pathlib import Path
from typing import List

import pandas as pd

from data.silver.io import write_parquet_with_meta


def build_stooq_silver(*, bronze_dir: Path, silver_dir: Path) -> None:
  stooq_bronze = bronze_dir / "stooq" / "daily"
  out_dir = silver_dir / "stooq"
  out_dir.mkdir(parents=True, exist_ok=True)

  csv_files = sorted(stooq_bronze.glob("*.csv"))
  if not csv_files:
    raise FileNotFoundError(f"No stooq csv found under: {stooq_bronze}")

  parts: List[pd.DataFrame] = []
  for p in csv_files:
    # filename example: googl.us.csv
    sym = p.stem.upper()  # "GOOGL.US"
    df = pd.read_csv(p)
    # Stooq columns usually: Date, Open, High, Low, Close, Volume
    rename = {c: c.lower() for c in df.columns}
    df = df.rename(columns=rename)
    if "date" not in df.columns:
      continue
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])
    df["symbol"] = sym
    parts.append(
        df[["symbol", "date", "open", "high", "low", "close", "volume"]])

  prices = pd.concat(parts,
                     ignore_index=True).sort_values(["symbol", "date"
                                                    ]).reset_index(drop=True)

  write_parquet_with_meta(
      prices,
      out_dir / "prices_daily.parquet",
      inputs=csv_files,
      meta_extra={
          "layer": "silver",
          "source": "stooq",
          "dataset": "prices_daily"
      },
  )
