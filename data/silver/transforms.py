"""
transforms.py

Transform helpers:
- companyfacts json -> minimal long facts table (only METRIC_SPECS)
- deduplicate by latest filed per period
- YTD -> discrete quarter
- rolling TTM
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import pandas as pd


def _as_float(x: Any) -> Optional[float]:
  if x is None:
    return None
  try:
    v = float(x)
  except (ValueError, TypeError):
    return None
  if v != v:
    return None
  return v


def pick_all_tags(
    companyfacts: Dict[str, Any],
    *,
    namespace: str,
    tags: List[str],
    unit: str,
) -> List[Tuple[str, List[Dict[str, Any]]]]:
  """
    Return list of (tag, items) for all matching tags that exist.
    Merges data from multiple tags to maximize coverage.
    """
  facts = companyfacts.get("facts", {})
  ns_obj = facts.get(namespace, {})
  result = []
  for tag in tags:
    tag_obj = ns_obj.get(tag, {})
    units = tag_obj.get("units", {})
    items = units.get(unit, [])
    if isinstance(items, list) and len(items) > 0:
      result.append((tag, items))
  return result


def companyfacts_to_minimal_facts_long(
    companyfacts: Dict[str, Any],
    *,
    cik10: str,
    metric_specs: Dict[str, Dict[str, Any]],
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
  """
    Build minimal facts_long for a single company (CIK) using metric_specs only.

    Output columns:
      cik10, metric, namespace, tag, unit, end, filed, fy, fp, form, val
    """
  rows: List[Dict[str, Any]] = []
  chosen: Dict[str, Any] = {"cik10": cik10, "chosen_tags": {}}

  for metric, spec in metric_specs.items():
    namespace = spec["namespace"]
    tags = list(spec["tags"])
    unit = spec["unit"]

    tag_items_pairs = pick_all_tags(companyfacts,
                                    namespace=namespace,
                                    tags=tags,
                                    unit=unit)

    if not tag_items_pairs:
      chosen["chosen_tags"][metric] = None
      continue

    chosen["chosen_tags"][metric] = [tag for tag, _ in tag_items_pairs]

    for tag, items in tag_items_pairs:
      for it in items:
        val = _as_float(it.get("val"))
        if val is None:
          continue

        row = {
            "cik10": cik10,
            "metric": metric,
            "namespace": namespace,
            "tag": tag,
            "unit": unit,
            "end": it.get("end"),
            "filed": it.get("filed"),
            "fy": it.get("fy"),
            "fp": it.get("fp"),
            "form": it.get("form"),
            "val": float(val),
        }
        rows.append(row)

  df = pd.DataFrame(rows)
  if df.empty:
    return df, chosen

  # Normalize types
  df["end"] = pd.to_datetime(df["end"], errors="coerce")
  df["filed"] = pd.to_datetime(df["filed"], errors="coerce")
  df["fy"] = pd.to_numeric(df["fy"], errors="coerce").astype("Int64")

  # Basic cleanup
  df = df.dropna(subset=["end", "filed", "fp", "fy"])
  df["fp"] = df["fp"].astype(str)

  return df, chosen


def dedup_latest_filed(df: pd.DataFrame) -> pd.DataFrame:
  """
    Keep only the latest filed value for each period.

    Strategy:
    - If fiscal_year is present: For each (cik10, metric, fiscal_year):
      1. Find the most common fy value across all periods
      2. Select all records with that fy value
      3. For periods missing in that fy, take the latest filed from other
         fy values
    - Otherwise: Use simple dedup by (cik10, metric, end, fy, fp)

    This ensures all quarters in a fiscal year come from the same filing
    version when possible, with fallback to latest filed for missing quarters.
    """
  if df.empty:
    return df

  if "fiscal_year" not in df.columns:
    group_cols = ["cik10", "metric", "end", "fy", "fp"]
    out = df.sort_values(group_cols + ["filed"])
    out = out.groupby(group_cols, as_index=False).tail(1)
    return out.reset_index(drop=True)

  out_parts: List[pd.DataFrame] = []

  for _, g in df.groupby(["cik10", "metric", "fiscal_year"], dropna=True):

    fy_counts = g["fy"].value_counts()
    if fy_counts.empty:
      simple_dedup = g.sort_values(["end", "fp", "filed", "val"],
                                   ascending=[True, True, True, True
                                             ]).groupby(["end", "fp"],
                                                        as_index=False).last()
      out_parts.append(simple_dedup)
      continue

    primary_fy = fy_counts.index[0]
    primary_records = g[g["fy"] == primary_fy].copy()

    # For same (end, fp), prefer higher val; but for same end with
    # different fp, prefer Q over FY
    def deduplicate_records(df: pd.DataFrame) -> pd.DataFrame:
      # First, for each (end), if both Q and FY exist, drop FY
      result_rows = []
      for _, end_group in df.groupby("end"):
        fps_present = set(end_group["fp"])
        quarterly_fps = {"Q1", "Q2", "Q3", "Q4"}
        has_quarterly = bool(quarterly_fps & fps_present)
        has_fy = "FY" in fps_present

        if has_quarterly and has_fy:
          # Keep only quarterly, drop FY
          filtered = end_group[end_group["fp"] != "FY"]
        else:
          filtered = end_group

        # Then for each (end, fp), take the one with max val
        # (latest filed as tiebreaker)
        for _, fp_group in filtered.groupby(["end", "fp"]):
          selected = fp_group.sort_values(["val", "filed"],
                                          ascending=[True, True]).iloc[-1]
          result_rows.append(selected)

      if result_rows:
        return pd.DataFrame(result_rows)
      return pd.DataFrame(columns=df.columns)

    primary_deduped = deduplicate_records(primary_records)

    # Track all end dates covered by primary
    # (regardless of fp that was dropped)
    primary_ends = set(primary_records["end"])

    # Missing records are from other fy values, but exclude ends already
    # covered by primary
    missing_records = g[(g["fy"] != primary_fy) &
                        (~g["end"].isin(primary_ends))].copy()

    if not missing_records.empty:
      missing_deduped = deduplicate_records(missing_records)
      out_parts.append(
          pd.concat([primary_deduped, missing_deduped], ignore_index=True))
    else:
      out_parts.append(primary_deduped)

  if not out_parts:
    return pd.DataFrame(columns=df.columns)

  out = pd.concat(out_parts, ignore_index=True)
  return out.reset_index(drop=True)


def ytd_to_quarter(
    df_ytd: pd.DataFrame,
    *,
    value_col: str = "val",
    out_col: str = "q_val",
    group_by_fiscal_year: bool = True,
) -> pd.DataFrame:
  """
    Convert YTD cumulative values into discrete quarterly values.

    Expected columns:
      end, filed, fp, value_col
      fiscal_year (if group_by_fiscal_year=True, required)
      fy (kept for reference but not used in grouping)

    Output:
      end, filed, fy, fp, out_col, fiscal_year

    Notes:
      - Groups by fiscal_year (not fy) to avoid comparative year mixing
      - Q4 is derived as FY - Q3 (when FY and Q3 exist)
    """
  if group_by_fiscal_year:
    required = {"end", "fp", "fiscal_year", value_col}
  else:
    required = {"end", "fy", "fp", value_col}
  missing = required - set(df_ytd.columns)
  if missing:
    raise ValueError(f"Missing columns: {sorted(missing)}")

  df = df_ytd.copy()
  df["end"] = pd.to_datetime(df["end"])
  df["filed"] = pd.to_datetime(df["filed"])
  df["fp"] = df["fp"].astype(str)

  if group_by_fiscal_year:
    df["fiscal_year"] = df["fiscal_year"].astype("Int64")
    group_col = "fiscal_year"
  else:
    df["fy"] = df["fy"].astype("Int64")
    group_col = "fy"

  out_rows: List[Dict[str, Any]] = []

  for group_val, g in df.groupby(group_col, dropna=True):
    g = g.sort_values(["end", "filed"])

    fp_latest = g.groupby("fp", as_index=False).tail(1)
    period_data = {row["fp"]: row for _, row in fp_latest.iterrows()}

    if "Q1" in period_data:
      row_dict = {
          "end": period_data["Q1"]["end"],
          "filed": period_data["Q1"]["filed"],
          "fp": "Q1",
          out_col: float(period_data["Q1"][value_col]),
      }
      if "fy" in period_data["Q1"]:
        row_dict["fy"] = period_data["Q1"]["fy"]
      if group_by_fiscal_year:
        row_dict["fiscal_year"] = group_val
      else:
        row_dict["fy"] = group_val
      out_rows.append(row_dict)

    if "Q2" in period_data and "Q1" in period_data:
      row_dict = {
          "end":
              period_data["Q2"]["end"],
          "filed":
              period_data["Q2"]["filed"],
          "fp":
              "Q2",
          out_col:
              float(period_data["Q2"][value_col] -
                    period_data["Q1"][value_col]),
      }
      if "fy" in period_data["Q2"]:
        row_dict["fy"] = period_data["Q2"]["fy"]
      if group_by_fiscal_year:
        row_dict["fiscal_year"] = group_val
      else:
        row_dict["fy"] = group_val
      out_rows.append(row_dict)

    if "Q3" in period_data and "Q2" in period_data:
      row_dict = {
          "end":
              period_data["Q3"]["end"],
          "filed":
              period_data["Q3"]["filed"],
          "fp":
              "Q3",
          out_col:
              float(period_data["Q3"][value_col] -
                    period_data["Q2"][value_col]),
      }
      if "fy" in period_data["Q3"]:
        row_dict["fy"] = period_data["Q3"]["fy"]
      if group_by_fiscal_year:
        row_dict["fiscal_year"] = group_val
      else:
        row_dict["fy"] = group_val
      out_rows.append(row_dict)

    if "FY" in period_data and "Q3" in period_data:
      row_dict = {
          "end":
              period_data["FY"]["end"],
          "filed":
              period_data["FY"]["filed"],
          "fp":
              "Q4",
          out_col:
              float(period_data["FY"][value_col] -
                    period_data["Q3"][value_col]),
      }
      if "fy" in period_data["FY"]:
        row_dict["fy"] = period_data["FY"]["fy"]
      if group_by_fiscal_year:
        row_dict["fiscal_year"] = group_val
      else:
        row_dict["fy"] = group_val
      out_rows.append(row_dict)

  out = pd.DataFrame(out_rows).sort_values("end").reset_index(drop=True)
  return out


def add_fiscal_year(df: pd.DataFrame,
                    companies_df: pd.DataFrame) -> pd.DataFrame:
  """
    Add fiscal_year column based on company"s fiscalYearEnd.

    Args:
        df: DataFrame with "end" (datetime) and "cik10" columns
        companies_df: DataFrame with "cik10" and "fye_mmdd" columns

    Returns:
        df with added "fiscal_year" column

    Logic:
        fiscal_year = end.year if end_mmdd <= fye_mmdd else end.year + 1
    """
  if df.empty:
    df["fiscal_year"] = pd.Series(dtype="Int64")
    return df

  df = df.copy()
  df["end"] = pd.to_datetime(df["end"])

  fye_map = companies_df.set_index("cik10")["fye_mmdd"].to_dict()

  def calc_fiscal_year(row):
    fye = fye_map.get(row["cik10"])
    if not fye or pd.isna(row["end"]):
      return pd.NA
    end_mmdd = row["end"].strftime("%m%d")
    return row["end"].year if end_mmdd <= fye else row["end"].year + 1

  df["fiscal_year"] = df.apply(calc_fiscal_year, axis=1)
  df["fiscal_year"] = df["fiscal_year"].astype("Int64")
  return df


def add_ttm(df: pd.DataFrame, *, value_col: str, out_col: str) -> pd.DataFrame:
  """
    Rolling 4-quarter sum by end date.
    Assumes df has one row per quarter in chronological order for each group.
    """
  if df.empty:
    return df
  out = df.sort_values("end").copy()
  out[out_col] = out[value_col].rolling(4).sum()
  return out
