"""
sec_silver.py

Build Silver outputs from Bronze SEC data.
- Reads: data/bronze/sec/company_tickers.json,
  data/bronze/sec/companyfacts/CIK*.json
- Writes:
  - data/silver/sec/companies.parquet
  - data/silver/sec/facts_long.parquet  (minimal tags only)
  - data/silver/sec/metrics_quarterly.parquet
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

from data.silver.io import write_parquet_with_meta
from data.silver.metric_specs import METRIC_SPECS
from data.silver.transforms import (
    add_fiscal_year,
    companyfacts_to_minimal_facts_long,
    dedup_latest_filed,
    ytd_to_quarter,
)


def load_companies(company_tickers_path: Path,
                   submissions_dir: Path) -> pd.DataFrame:
  raw = json.loads(company_tickers_path.read_text(encoding="utf-8"))
  rows = []
  for _, v in raw.items():
    ticker = str(v.get("ticker", "")).upper().strip()
    cik = str(v.get("cik_str", "")).strip()
    title = str(v.get("title", "")).strip()
    if not ticker or not cik:
      continue
    cik10 = cik.zfill(10)

    fye_mmdd = None
    submission_path = submissions_dir / f"CIK{cik10}.json"
    if submission_path.exists():
      try:
        sub_data = json.loads(submission_path.read_text(encoding="utf-8"))
        fye_raw = sub_data.get("fiscalYearEnd")
        if fye_raw and len(str(fye_raw)) == 4:
          fye_mmdd = str(fye_raw)
      except (json.JSONDecodeError, OSError):
        pass

    rows.append({
        "ticker": ticker,
        "cik10": cik10,
        "title": title,
        "fye_mmdd": fye_mmdd
    })
  df = pd.DataFrame(rows).drop_duplicates(subset=["ticker"]).sort_values(
      ["ticker"])
  return df.reset_index(drop=True)


def build_metrics_quarterly(facts_long: pd.DataFrame) -> pd.DataFrame:
  """
    From minimal facts_long -> per-metric quarterly discrete values + TTM.
    Output columns:
      cik10, metric, end, filed, fy, fp, fiscal_year, q_val, ttm_val, tag
    """
  out_parts: List[pd.DataFrame] = []

  for metric, spec in METRIC_SPECS.items():
    df = facts_long[facts_long["metric"] == metric].copy()
    if df.empty:
      continue

    if bool(spec.get("abs", False)):
      df["val"] = df["val"].abs()

    parts = []
    for cik10, g in df.groupby("cik10"):
      if bool(spec.get("is_ytd", False)):
        qg = ytd_to_quarter(g,
                            value_col="val",
                            out_col="q_val",
                            group_by_fiscal_year=True)
        if bool(spec.get("abs", False)):
          qg["q_val"] = qg["q_val"].abs()
      else:
        qg = g.rename(columns={"val": "q_val"})[[
            "end", "filed", "fy", "fp", "fiscal_year", "q_val"
        ]].copy()

      qg["cik10"] = cik10
      qg["metric"] = metric
      qg["tag"] = str(g["tag"].iloc[0]) if "tag" in g.columns and len(
          g["tag"].unique()) == 1 else ""
      parts.append(qg)

    q_all = pd.concat(parts, ignore_index=True).sort_values(["cik10", "end"])

    q_all["ttm_val"] = (q_all.sort_values(["cik10", "metric", "end"]).groupby(
        ["cik10", "metric"])["q_val"].rolling(4).sum().reset_index(level=[0, 1],
                                                                   drop=True))

    out_parts.append(q_all[[
        "cik10", "metric", "end", "filed", "fy", "fp", "fiscal_year", "q_val",
        "ttm_val", "tag"
    ]])

  if not out_parts:
    return pd.DataFrame(columns=[
        "cik10", "metric", "end", "filed", "fy", "fp", "fiscal_year", "q_val",
        "ttm_val", "tag"
    ])

  out = pd.concat(out_parts, ignore_index=True)
  out = out.sort_values(["cik10", "metric", "end"]).reset_index(drop=True)
  return out


def build_sec_silver(*, bronze_dir: Path, silver_dir: Path) -> None:
  sec_bronze = bronze_dir / "sec"
  out_dir = silver_dir / "sec"
  out_dir.mkdir(parents=True, exist_ok=True)

  company_tickers_path = sec_bronze / "company_tickers.json"
  submissions_dir = sec_bronze / "submissions"
  companies = load_companies(company_tickers_path, submissions_dir)
  write_parquet_with_meta(
      companies,
      out_dir / "companies.parquet",
      inputs=[company_tickers_path],
      meta_extra={
          "layer": "silver",
          "source": "sec",
          "dataset": "companies"
      },
  )

  cf_dir = sec_bronze / "companyfacts"
  cf_files = sorted(
      p for p in cf_dir.glob("CIK*.json") if not p.name.endswith(".meta.json"))
  if not cf_files:
    raise FileNotFoundError(f"No companyfacts found under: {cf_dir}")

  facts_parts: List[pd.DataFrame] = []
  chosen_tags: List[Dict[str, Any]] = []

  for p in cf_files:
    cik10 = p.stem.replace("CIK", "")
    companyfacts = json.loads(p.read_text(encoding="utf-8"))
    df, chosen = companyfacts_to_minimal_facts_long(companyfacts,
                                                    cik10=cik10,
                                                    metric_specs=METRIC_SPECS)
    if not df.empty:
      facts_parts.append(df)
    chosen_tags.append(chosen)

  if facts_parts:
    facts_long = pd.concat(facts_parts, ignore_index=True)
    facts_long = add_fiscal_year(facts_long, companies)
    facts_long = dedup_latest_filed(facts_long)
  else:
    facts_long = pd.DataFrame(columns=[
        "cik10", "metric", "namespace", "tag", "unit", "end", "filed", "fy",
        "fp", "form", "val", "fiscal_year"
    ])

  write_parquet_with_meta(
      facts_long,
      out_dir / "facts_long.parquet",
      inputs=cf_files,
      meta_extra={
          "layer": "silver",
          "source": "sec",
          "dataset": "facts_long_minimal",
          "metric_specs": METRIC_SPECS,
          "chosen_tags": chosen_tags,
      },
  )

  metrics_q = build_metrics_quarterly(facts_long)
  write_parquet_with_meta(
      metrics_q,
      out_dir / "metrics_quarterly.parquet",
      inputs=[out_dir / "facts_long.parquet"],
      meta_extra={
          "layer": "silver",
          "source": "sec",
          "dataset": "metrics_quarterly"
      },
  )

  print(f"Companies: {companies.shape}, columns: {list(companies.columns)}")
  print(f"Facts long: {facts_long.shape}, columns: {list(facts_long.columns)}")
  print(f"Metrics quarterly: {metrics_q.shape}, "
        f"columns: {list(metrics_q.columns)}")
