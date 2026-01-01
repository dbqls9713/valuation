"""
Scenario configuration for valuation experiments.

ScenarioConfig is a serializable (YAML/JSON-friendly) configuration class
that specifies which policies to use for each component of the valuation.

Owner Earnings = Pre-Maintenance OE - Maintenance CAPEX
"""

from dataclasses import asdict
from dataclasses import dataclass
import json
from typing import Any


@dataclass
class ScenarioConfig:
  """
  Configuration for a valuation scenario.

  All fields are strings (policy names) that map to factories in the registry.
  """
  name: str = 'default'
  pre_maint_oe: str = 'ttm'
  maint_capex: str = 'ttm'
  growth: str = 'fixed_0p10'
  fade: str = 'linear'
  shares: str = 'avg_5y'
  terminal: str = 'gordon'
  discount: str = 'fixed_0p10'
  n_years: int = 10

  @classmethod
  def default(cls) -> 'ScenarioConfig':
    """Create default scenario configuration."""
    return cls()

  def to_dict(self) -> dict[str, Any]:
    """Convert to dictionary."""
    return asdict(self)

  def to_json(self) -> str:
    """Serialize to JSON string."""
    return json.dumps(self.to_dict(), indent=2)

  @classmethod
  def from_dict(cls, data: dict[str, Any]) -> 'ScenarioConfig':
    """
    Create from dictionary.

    Supports legacy field names for backward compatibility.
    """
    if 'oe' in data and 'pre_maint_oe' not in data:
      data['pre_maint_oe'] = data.pop('oe')
    if 'capex' in data and 'maint_capex' not in data:
      data['maint_capex'] = data.pop('capex')

    known_fields = {
        'name', 'pre_maint_oe', 'maint_capex', 'growth', 'fade', 'shares',
        'terminal', 'discount', 'n_years'
    }
    filtered = {k: v for k, v in data.items() if k in known_fields}
    return cls(**filtered)

  @classmethod
  def from_json(cls, json_str: str) -> 'ScenarioConfig':
    """Create from JSON string."""
    return cls.from_dict(json.loads(json_str))
