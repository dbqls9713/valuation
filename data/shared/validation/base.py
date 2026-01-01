"""Base classes and types for validation framework."""
from dataclasses import dataclass


@dataclass(frozen=True)
class CheckResult:
  """Result of a single validation check."""

  name: str
  ok: bool
  details: str

  def __str__(self) -> str:
    status = '✓' if self.ok else '✗'
    return f'{status} {self.name}: {self.details}'


def make_result(name: str, ok: bool, details: str) -> CheckResult:
  """Factory function for creating CheckResult."""
  return CheckResult(name=name, ok=ok, details=details)


def pass_result(name: str, details: str) -> CheckResult:
  """Create a passing CheckResult."""
  return CheckResult(name=name, ok=True, details=details)


def fail_result(name: str, details: str) -> CheckResult:
  """Create a failing CheckResult."""
  return CheckResult(name=name, ok=False, details=details)
