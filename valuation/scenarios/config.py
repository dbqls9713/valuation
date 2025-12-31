"""
Scenario configuration for valuation experiments.

ScenarioConfig is a serializable (YAML/JSON-friendly) configuration class
that specifies which policies to use for each component of the valuation.
"""

from dataclasses import asdict
from dataclasses import dataclass
from dataclasses import field
import json
from typing import Any


@dataclass
class ScenarioConfig:
  """
  Configuration for a valuation scenario.

  All fields are strings (policy names) that map to factories in the registry.
  This makes the config serializable to YAML/JSON for reproducibility.

  Attributes:
    name: Human-readable scenario name
    capex: CAPEX policy name (e.g., 'weighted_3y_123', 'raw_ttm')
    growth: Growth policy name (e.g., 'cagr_3y_clip')
    fade: Fade policy name (e.g., 'linear')
    shares: Share policy name (e.g., 'avg_5y')
    terminal: Terminal policy name (e.g., 'gordon')
    discount: Discount policy name (e.g., 'fixed_0p10', 'fixed_0p06')
    n_years: Number of explicit forecast years
    policy_params: Optional dict of policy-specific parameters
  """
  name: str = 'default'
  capex: str = 'weighted_3y_123'
  growth: str = 'cagr_3y_clip'
  fade: str = 'linear'
  shares: str = 'avg_5y'
  terminal: str = 'gordon'
  discount: str = 'fixed_0p10'
  n_years: int = 10
  policy_params: dict[str, Any] = field(default_factory=dict)

  @classmethod
  def default(cls) -> 'ScenarioConfig':
    """
    Create default scenario configuration.

    Uses:
      - 3-year weighted CAPEX (1:2:3)
      - CAGR growth with 4% threshold, 0-18% clip
      - Linear fade with 1% spread
      - 5-year average share change
      - Gordon terminal at 3%
      - Fixed 10% discount rate
      - 10-year forecast
    """
    return cls(
        name='default',
        capex='weighted_3y_123',
        growth='cagr_3y_clip',
        fade='linear',
        shares='avg_5y',
        terminal='gordon',
        discount='fixed_0p10',
        n_years=10,
    )

  @classmethod
  def raw_capex(cls) -> 'ScenarioConfig':
    """Scenario using raw TTM CAPEX."""
    return cls(
        name='raw_capex',
        capex='raw_ttm',
        growth='cagr_3y_clip',
        fade='linear',
        shares='avg_5y',
        terminal='gordon',
        discount='fixed_0p10',
        n_years=10,
    )

  @classmethod
  def clipped_capex(cls) -> 'ScenarioConfig':
    """Scenario using intensity-clipped CAPEX."""
    return cls(
        name='clipped_capex',
        capex='intensity_clipped',
        growth='cagr_3y_clip',
        fade='linear',
        shares='avg_5y',
        terminal='gordon',
        discount='fixed_0p10',
        n_years=10,
    )

  @classmethod
  def discount_6pct(cls) -> 'ScenarioConfig':
    """Scenario with 6% discount rate."""
    return cls(
        name='discount_6pct',
        capex='weighted_3y_123',
        growth='cagr_3y_clip',
        fade='linear',
        shares='avg_5y',
        terminal='gordon',
        discount='fixed_0p06',
        n_years=10,
    )

  def to_dict(self) -> dict[str, Any]:
    """Convert to dictionary."""
    return asdict(self)

  def to_json(self) -> str:
    """Serialize to JSON string."""
    return json.dumps(self.to_dict(), indent=2)

  @classmethod
  def from_dict(cls, data: dict[str, Any]) -> 'ScenarioConfig':
    """Create from dictionary."""
    return cls(**data)

  @classmethod
  def from_json(cls, json_str: str) -> 'ScenarioConfig':
    """Create from JSON string."""
    return cls.from_dict(json.loads(json_str))
