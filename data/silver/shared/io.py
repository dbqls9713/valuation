"""
I/O utilities for Silver layer.
"""
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd


def utc_now_iso() -> str:
  return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def sha256_file(path: Path) -> str:
  h = hashlib.sha256()
  with path.open('rb') as f:
    for chunk in iter(lambda: f.read(1024 * 1024), b''):
      h.update(chunk)
  return h.hexdigest()


def _file_mtime_iso(path: Path) -> str:
  ts = path.stat().st_mtime
  dt = datetime.fromtimestamp(ts, tz=timezone.utc)
  return dt.replace(microsecond=0).isoformat()


def build_inputs_info(paths: Iterable[Path],
                      *,
                      compute_sha256: bool = False) -> List[Dict[str, Any]]:
  out: List[Dict[str, Any]] = []
  for p in paths:
    out.append({
        'path': str(p),
        'size': int(p.stat().st_size),
        'mtime_utc': _file_mtime_iso(p),
        'sha256': sha256_file(p) if compute_sha256 else None,
    })
  return out


class ParquetWriter:
  """Write DataFrames to Parquet with metadata."""

  def write(
      self,
      df: pd.DataFrame,
      out_path: Path,
      *,
      inputs: Optional[Iterable[Path]] = None,
      metadata: Optional[Dict[str, Any]] = None,
      compute_sha256: bool = False,
      target_date: Optional[str] = None,
  ) -> None:
    """
    Write DataFrame to Parquet with sidecar metadata.

    Args:
        df: DataFrame to write
        out_path: Output path
        inputs: Input files (for provenance)
        metadata: Additional metadata
        compute_sha256: Whether to compute SHA256 of inputs
        target_date: Target date for data coverage (YYYY-MM-DD)
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_path, index=False)

    meta = {
        'generated_at_utc': utc_now_iso(),
        'output': str(out_path),
        'nrows': int(len(df)),
        'ncols': int(df.shape[1]),
        'columns': list(df.columns),
    }

    if target_date:
      meta['target_date'] = target_date

    if inputs:
      inputs_info = build_inputs_info(list(inputs),
                                      compute_sha256=compute_sha256)
      meta['inputs'] = inputs_info

    if metadata:
      meta.update(metadata)

    meta_path = out_path.with_suffix(out_path.suffix + '.meta.json')
    meta_json = json.dumps(meta, ensure_ascii=False, indent=2)
    meta_path.write_text(meta_json, encoding='utf-8')
