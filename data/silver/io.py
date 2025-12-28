"""
io.py

Minimal IO helpers for Silver outputs:
- write parquet
- write sidecar meta json containing provenance
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd


def utc_now_iso() -> str:
  return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def sha256_file(path: Path) -> str:
  h = hashlib.sha256()
  with path.open("rb") as f:
    for chunk in iter(lambda: f.read(1024 * 1024), b""):
      h.update(chunk)
  return h.hexdigest()


@dataclass(frozen=True)
class InputFileInfo:
  path: str
  size: int
  mtime_utc: str
  sha256: Optional[str]


def _file_mtime_iso(path: Path) -> str:
  ts = path.stat().st_mtime
  return datetime.fromtimestamp(
      ts, tz=timezone.utc).replace(microsecond=0).isoformat()


def build_inputs_info(paths: Iterable[Path],
                      *,
                      compute_sha256: bool = False) -> List[Dict[str, Any]]:
  out: List[Dict[str, Any]] = []
  for p in paths:
    info = InputFileInfo(
        path=str(p),
        size=int(p.stat().st_size),
        mtime_utc=_file_mtime_iso(p),
        sha256=sha256_file(p) if compute_sha256 else None,
    )
    out.append({
        "path": info.path,
        "size": info.size,
        "mtime_utc": info.mtime_utc,
        "sha256": info.sha256,
    })
  return out


def write_parquet_with_meta(
    df: pd.DataFrame,
    out_path: Path,
    *,
    inputs: Iterable[Path],
    meta_extra: Optional[Dict[str, Any]] = None,
    compute_sha256: bool = False,
) -> None:
  out_path.parent.mkdir(parents=True, exist_ok=True)
  df.to_parquet(out_path, index=False)

  meta = {
      "generated_at_utc": utc_now_iso(),
      "output": str(out_path),
      "nrows": int(len(df)),
      "ncols": int(df.shape[1]),
      "columns": list(df.columns),
      "inputs": build_inputs_info(list(inputs), compute_sha256=compute_sha256),
  }
  if meta_extra:
    meta.update(meta_extra)

  meta_path = out_path.with_suffix(out_path.suffix + ".meta.json")
  meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2),
                       encoding="utf-8")
