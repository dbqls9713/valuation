'''
Valuation framework with strategy/policy-based architecture.

This package provides a modular DCF valuation system where each component
(CAPEX calculation, growth estimation, fade schedule, etc.) is implemented
as an independent policy that can be swapped or compared.

Usage:
  from valuation.scenarios.config import ScenarioConfig
  from valuation.scenarios.registry import create_scenario
  from valuation.run import run_valuation

  config = ScenarioConfig.default()
  result = run_valuation(ticker='GOOGL', as_of_date='2024-12-31', config=config)
'''
