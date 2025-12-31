"""
I/O utilities for data layers.

Provides ParquetWriter for writing DataFrames with metadata sidecar files.
"""

from datetime import datetime
from datetime import timezone
import hashlib
import json
from pathlib import Path
from typing import Any, Iterable, Optional

import pandas as pd


def _utc_now_iso() -> str:
  """Return current UTC time in ISO format."""
  return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

def _sha256_file(path: Path) -> str:
  """Compute SHA256 hash of a file."""
  h = hashlib.sha256()
  with path.open('rb') as f:
    for chunk in iter(lambda: f.read(1024 * 1024), b''):
      h.update(chunk)
  return h.hexdigest()

def _file_mtime_iso(path: Path) -> str:
  """Return file modification time in ISO format."""
  ts = path.stat().st_mtime
  dt = datetime.fromtimestamp(ts, tz=timezone.utc)
  return dt.replace(microsecond=0).isoformat()

def _build_inputs_info(
    paths: Iterable[Path],
    *,
    compute_sha256: bool = False,
) -> list[dict[str, Any]]:
  """Build metadata info for input files."""
  out: list[dict[str, Any]] = []
  for p in paths:
    out.append({
        'path': str(p),
        'size': int(p.stat().st_size),
        'mtime_utc': _file_mtime_iso(p),
        'sha256': _sha256_file(p) if compute_sha256 else None,
    })
  return out

class ParquetWriter:
  """Write DataFrames to Parquet with metadata sidecar."""

  def write(
      self,
      df: pd.DataFrame,
      out_path: Path,
      *,
      inputs: Optional[Iterable[Path]] = None,
      metadata: Optional[dict[str, Any]] = None,
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
        'generated_at_utc': _utc_now_iso(),
        'output': str(out_path),
        'nrows': int(len(df)),
        'ncols': int(df.shape[1]),
        'columns': list(df.columns),
    }

    if target_date:
      meta['target_date'] = target_date

    if inputs:
      inputs_info = _build_inputs_info(list(inputs),
                                       compute_sha256=compute_sha256)
      meta['inputs'] = inputs_info

    if metadata:
      meta.update(metadata)

    meta_path = out_path.with_suffix(out_path.suffix + '.meta.json')
    meta_json = json.dumps(meta, ensure_ascii=False, indent=2)
    meta_path.write_text(meta_json, encoding='utf-8')
