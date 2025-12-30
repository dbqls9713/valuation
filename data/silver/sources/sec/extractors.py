"""
SEC data extractors.
"""
import json
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd

from data.silver.config.metric_specs import METRIC_SPECS


class SECCompanyFactsExtractor:
  """Extract data from SEC companyfacts JSON files."""

  def extract_companies(self, company_tickers_path: Path,
                        submissions_dir: Path) -> pd.DataFrame:
    """Extract companies list from company_tickers.json."""
    raw = json.loads(company_tickers_path.read_text(encoding='utf-8'))
    rows = []
    for _, v in raw.items():
      ticker = str(v.get('ticker', '')).upper().strip()
      cik = str(v.get('cik_str', '')).strip()
      title = str(v.get('title', '')).strip()
      if not ticker or not cik:
        continue
      cik10 = cik.zfill(10)

      fye_mmdd = None
      first_filing_date = None
      submission_path = submissions_dir / f'CIK{cik10}.json'
      if submission_path.exists():
        try:
          sub_data = json.loads(submission_path.read_text(encoding='utf-8'))
          fye_raw = sub_data.get('fiscalYearEnd')
          if fye_raw and len(str(fye_raw)) == 4:
            fye_mmdd = str(fye_raw)

          # Get first filing date from recent filings
          filings = sub_data.get('filings', {}).get('recent', {})
          filing_dates = filings.get('filingDate', [])
          if filing_dates:
            first_filing_date = min(filing_dates)
        except (json.JSONDecodeError, OSError):
          pass

      rows.append({
          'ticker': ticker,
          'cik10': cik10,
          'title': title,
          'fye_mmdd': fye_mmdd,
          'first_filing_date': first_filing_date
      })

    df = (pd.DataFrame(rows).drop_duplicates(subset=['ticker']).sort_values(
        ['ticker']))
    if 'first_filing_date' in df.columns:
      df['first_filing_date'] = pd.to_datetime(df['first_filing_date'],
                                               errors='coerce')
    return df.reset_index(drop=True)

  def extract_facts(self, companyfacts_path: Path) -> pd.DataFrame:
    """Extract facts from single companyfacts JSON file."""
    cik10 = companyfacts_path.stem.replace('CIK', '')
    companyfacts = json.loads(companyfacts_path.read_text(encoding='utf-8'))

    df, _ = self._companyfacts_to_minimal_facts_long(companyfacts,
                                                     cik10=cik10,
                                                     metric_specs=METRIC_SPECS)
    return df

  def _pick_all_tags(
      self,
      companyfacts: Dict[str, Any],
      *,
      namespace: str,
      tags: List[str],
      unit: str,
  ) -> List[Tuple[str, List[Dict[str, Any]]]]:
    """Return list of (tag, items) for all matching tags that exist."""
    facts = companyfacts.get('facts', {})
    ns_obj = facts.get(namespace, {})
    result = []
    for tag in tags:
      tag_obj = ns_obj.get(tag, {})
      units = tag_obj.get('units', {})
      items = units.get(unit, [])
      if isinstance(items, list) and len(items) > 0:
        result.append((tag, items))
    return result

  def _companyfacts_to_minimal_facts_long(
      self,
      companyfacts: Dict[str, Any],
      *,
      cik10: str,
      metric_specs: Dict[str, Dict[str, Any]],
  ) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """Build minimal facts_long for a single company."""
    rows: List[Dict[str, Any]] = []
    chosen: Dict[str, Any] = {'cik10': cik10, 'chosen_tags': {}}

    for metric, spec in metric_specs.items():
      namespace = spec['namespace']
      tags = list(spec['tags'])
      unit = spec['unit']

      tag_items_pairs = self._pick_all_tags(companyfacts,
                                            namespace=namespace,
                                            tags=tags,
                                            unit=unit)

      if not tag_items_pairs:
        chosen['chosen_tags'][metric] = None
        continue

      chosen['chosen_tags'][metric] = [tag for tag, _ in tag_items_pairs]

      for tag, items in tag_items_pairs:
        for it in items:
          val = self._as_float(it.get('val'))
          if val is None:
            continue

          rows.append({
              'cik10': cik10,
              'metric': metric,
              'namespace': namespace,
              'tag': tag,
              'unit': unit,
              'end': it.get('end'),
              'filed': it.get('filed'),
              'fy': it.get('fy'),
              'fp': it.get('fp'),
              'form': it.get('form'),
              'val': float(val),
          })

    df = pd.DataFrame(rows)
    if df.empty:
      return df, chosen

    df['end'] = pd.to_datetime(df['end'], errors='coerce')
    df['filed'] = pd.to_datetime(df['filed'], errors='coerce')
    df['fy'] = pd.to_numeric(df['fy'], errors='coerce').astype('Int64')

    df = df.dropna(subset=['end', 'filed', 'fp', 'fy'])
    df['fp'] = df['fp'].astype(str)

    return df, chosen

  @staticmethod
  def _as_float(x: Any) -> float:
    if x is None:
      return None
    try:
      v = float(x)
    except (ValueError, TypeError):
      return None
    if v != v:  # NaN check
      return None
    return v
